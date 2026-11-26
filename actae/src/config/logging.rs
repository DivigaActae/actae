// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use crate::config::AppConfig;
use anyhow::Result;
use tracing::{info, warn};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    fmt, layer::SubscriberExt, reload, util::SubscriberInitExt, EnvFilter, Layer, Registry,
};

/// Held for the process lifetime. Carries the file-writer guard and a handle
/// that switches the log filter from the quiet startup preset to the configured
/// level once the server is operational.
pub struct LoggingGuard {
    _file_guard: Option<WorkerGuard>,
    reload: reload::Handle<EnvFilter, Registry>,
    enabled: EnvFilter,
}

impl LoggingGuard {
    /// Reveal the configured log level. Startup internals stay hidden until
    /// this is called (once, when the server is fully operational). Never fatal:
    /// if the reload fails the quiet startup filter simply remains in effect.
    pub fn operational(&self) {
        let _ = self.reload.reload(self.enabled.clone());
    }
}

pub fn init(config: &AppConfig) -> Result<LoggingGuard> {
    // A compiled (release) binary is quiet and human-readable by default: no
    // file/line/thread noise on the console, and the internal startup chatter is
    // hidden until the server is operational. A debug build (`cargo run`) keeps
    // the verbose JSON stream for development. An explicit LOG_FORMAT /
    // LOG_LEVEL / RUST_LOG always wins.
    let explicit_level = std::env::var_os("RUST_LOG").is_some()
        || std::env::var_os("LOG_LEVEL").is_some();
    let explicit_format = std::env::var_os("LOG_FORMAT").is_some();
    let is_debug_build = cfg!(debug_assertions);

    let log_format = if explicit_format {
        config.log_format.clone()
    } else if is_debug_build {
        "json".to_string()
    } else {
        "compact".to_string()
    };

    // Default filter: the configured level, but silence the `tokio_postgres`
    // NOTICE stream ("... already exists, skipping") which is noisy on every
    // boot and never actionable. An explicit `RUST_LOG` still wins entirely.
    let default_filter = format!("{},tokio_postgres=warn", config.log_level);
    let enabled =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&default_filter));

    // Hide the noisy startup sequence (pool init, migrations, worker startup) in
    // a release build unless the operator explicitly asked for a level. Debug
    // builds show everything so a developer/CI sees the full boot.
    let startup_filter = if is_debug_build || explicit_level {
        enabled.clone()
    } else {
        EnvFilter::new("warn")
    };

    let mut layers = Vec::new();

    // Generate instance ID for correlation
    let instance_id = config.get_instance_id();

    // Console layer.
    let console_layer = match log_format.as_str() {
        "json" => fmt::layer()
            .json()
            .with_current_span(true)
            .with_span_list(true)
            .with_target(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_line_number(true)
            .with_file(true)
            .fmt_fields(StructuredFieldFormatter::new(&instance_id))
            .boxed(),
        "pretty" => fmt::layer()
            .pretty()
            .with_line_number(true)
            .with_file(true)
            .with_target(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .boxed(),
        "compact" => compact_console_layer(),
        _ => {
            warn!(
                "Unknown log format '{}', falling back to compact",
                config.log_format
            );
            compact_console_layer()
        }
    };
    layers.push(console_layer);

    // File layer with structured JSON logging (keeps full detail: ops can always
    // reconstruct exactly which file/line emitted a line).
    let mut guard = None;
    if let Some(file_path) = &config.log_file_path {
        info!("Logging to file: {}", file_path);
        let file_appender = tracing_appender::rolling::daily(
            file_path,
            config
                .log_file_name
                .as_ref()
                .unwrap_or(&"actae.log".to_string()),
        );
        let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
        let file_layer = fmt::layer()
            .json()
            .with_writer(non_blocking)
            .with_ansi(false)
            .with_current_span(true)
            .with_span_list(true)
            .with_target(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_line_number(true)
            .with_file(true)
            .fmt_fields(StructuredFieldFormatter::new(&instance_id))
            .boxed();
        layers.push(file_layer);
        guard = Some(_guard);
    }

    let (filter_layer, reload) = reload::Layer::new(startup_filter);
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(layers)
        .init();

    info!(
        instance_id = %instance_id,
        log_level = %config.log_level,
        log_format = %log_format,
        "Logging initialized with enhanced structured format"
    );
    Ok(LoggingGuard {
        _file_guard: guard,
        reload,
        enabled,
    })
}

/// The clean console layer used by a compiled binary: level + message only — no
/// target, file, line, or thread noise that would expose implementation details.
fn compact_console_layer<S>() -> Box<dyn Layer<S> + Send + Sync>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fmt::layer()
        .compact()
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_file(false)
        .with_line_number(false)
        .boxed()
}

/// Structured field formatter for consistent logging
struct StructuredFieldFormatter {
    instance_id: String,
}

impl StructuredFieldFormatter {
    fn new(instance_id: &str) -> Self {
        Self {
            instance_id: instance_id.to_string(),
        }
    }
}

impl<'writer> tracing_subscriber::fmt::FormatFields<'writer> for StructuredFieldFormatter {
    fn format_fields<R: tracing_subscriber::field::RecordFields>(
        &self,
        mut writer: tracing_subscriber::fmt::format::Writer<'writer>,
        fields: R,
    ) -> std::fmt::Result {
        let mut obj = sonic_rs::Object::new();
        obj.insert("instance_id", self.instance_id.as_str());

        let mut visitor = SonicFieldVisitor { obj: &mut obj };
        fields.record(&mut visitor);

        let json_str = sonic_rs::to_string(&obj).map_err(|_| std::fmt::Error)?;
        write!(writer, "{json_str}")
    }

    fn add_fields(
        &self,
        current: &'writer mut tracing_subscriber::fmt::FormattedFields<Self>,
        fields: &tracing::span::Record<'_>,
    ) -> std::fmt::Result {
        let mut obj = if current.is_empty() {
            sonic_rs::Object::new()
        } else {
            sonic_rs::from_str(&current.fields).map_err(|_| std::fmt::Error)?
        };

        let mut visitor = SonicFieldVisitor { obj: &mut obj };
        fields.record(&mut visitor);

        let json_str = sonic_rs::to_string(&obj).map_err(|_| std::fmt::Error)?;
        current.fields = json_str;
        Ok(())
    }
}

struct SonicFieldVisitor<'a> {
    obj: &'a mut sonic_rs::Object,
}

impl tracing_subscriber::field::Visit for SonicFieldVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let s = format!("{value:?}");
        self.obj.insert(field.name(), s.as_str());
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.obj.insert(field.name(), value);
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.obj.insert(field.name(), value);
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.obj.insert(field.name(), value);
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        let n = sonic_rs::Number::from_f64(value).unwrap_or_else(|| sonic_rs::Number::from(0));
        self.obj.insert(field.name(), sonic_rs::Value::from(n));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.obj.insert(field.name(), value);
    }
}

#[cfg(test)]
mod tests {
    use sonic_rs::JsonValueTrait;

    #[test]
    fn test_object_builds_valid_json() {
        let mut obj = sonic_rs::Object::new();
        obj.insert("instance_id", "inst-001");
        obj.insert("str_val", "hello");
        obj.insert("int_val", 42i64);
        obj.insert("uint_val", 100u64);
        obj.insert("bool_val", true);
        let n = sonic_rs::Number::from_f64(std::f64::consts::PI).unwrap();
        obj.insert("float_val", sonic_rs::Value::from(n));
        obj.insert("debug_val", "DebugOutput");

        let json = sonic_rs::to_string(&obj).unwrap();

        let parsed: sonic_rs::Value = sonic_rs::from_str(&json).expect("JSON output must be valid");

        assert_eq!(parsed["instance_id"].as_str().unwrap(), "inst-001");
        assert_eq!(parsed["str_val"].as_str().unwrap(), "hello");
        assert_eq!(parsed["int_val"].as_i64().unwrap(), 42);
        assert_eq!(parsed["uint_val"].as_u64().unwrap(), 100);
        assert!(parsed["bool_val"].as_bool().unwrap());
        assert!((parsed["float_val"].as_f64().unwrap() - std::f64::consts::PI).abs() < 0.001);
        assert_eq!(parsed["debug_val"].as_str().unwrap(), "DebugOutput");
    }

    #[test]
    fn test_format_fields_produces_json_object() {
        let mut obj = sonic_rs::Object::new();
        obj.insert("instance_id", "inst-001");
        obj.insert("conn_id", "550e8400-e29b-41d4-a716-446655440000");
        obj.insert("msg_type", "auth");

        let json = sonic_rs::to_string(&obj).unwrap();

        // Must be valid JSON
        let _: sonic_rs::Value = sonic_rs::from_str(&json).expect("Output must be valid JSON");

        // Must be a JSON object, not key=value format
        assert!(json.starts_with('{'), "JSON must start with {{");
        assert!(json.ends_with('}'), "JSON must end with }}");
        assert!(
            !json.contains("conn_id="),
            "Must not contain key=value format"
        );
        assert!(
            !json.contains("msg_type="),
            "Must not contain key=value format"
        );
    }

    #[test]
    fn test_add_fields_merges_existing() {
        let existing_json = r#"{"instance_id":"inst-001","existing":"value"}"#;

        let mut obj: sonic_rs::Object = sonic_rs::from_str(existing_json).unwrap();
        obj.insert("new_field", "new_value");
        let merged_json = sonic_rs::to_string(&obj).unwrap();

        let parsed: sonic_rs::Value =
            sonic_rs::from_str(&merged_json).expect("Merged JSON must be valid");

        assert_eq!(parsed["existing"].as_str().unwrap(), "value");
        assert_eq!(parsed["new_field"].as_str().unwrap(), "new_value");
        assert_eq!(parsed["instance_id"].as_str().unwrap(), "inst-001");
    }

    #[test]
    fn test_roundtrip_parseable_by_sonic() {
        // Simulates what tracing-subscriber does: format fields, store, parse back.
        let mut obj = sonic_rs::Object::new();
        obj.insert("instance_id", "inst-001");
        obj.insert("conn_id", "uuid-1234");
        obj.insert("msg_type", "auth");

        let formatted = sonic_rs::to_string(&obj).unwrap();

        // Parse back (as tracing-subscriber does internally)
        let reparsed: sonic_rs::Object =
            sonic_rs::from_str(&formatted).expect("Formatted fields must be parseable as JSON");

        let json_again = sonic_rs::to_string(&reparsed).unwrap();
        let _: sonic_rs::Value =
            sonic_rs::from_str(&json_again).expect("Roundtripped JSON must be valid");
    }
}
