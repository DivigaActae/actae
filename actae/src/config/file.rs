// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Typed `--config <file.toml>` support.
//!
//! Operators of a self-hosted instance can describe the deployment in a TOML
//! file instead of exporting dozens of environment variables:
//!
//! ```toml
//! [server]
//! host = "0.0.0.0"
//! port = 8002
//!
//! [database]
//! url = "postgresql://actae:secret@db.internal:5432/actae?sslmode=verify-full"
//!
//! [auth]
//! jwt-secret = "…"
//! api-key-hash-secret = "…"
//!
//! [tls]
//! enabled = true
//! cert-path = "/etc/actae/tls/cert.pem"
//! key-path  = "/etc/actae/tls/key.pem"
//! ```
//!
//! ## Precedence
//!
//! `CLI flag > config file > environment > built-in default`.
//!
//! The file is applied by materializing it into the process environment
//! ([`FileConfig::apply_to_env`]) **before** [`crate::config::AppConfig::load`]
//! runs, so `AppConfig` stays a single env-driven reader while the file becomes
//! the authoritative source when present. CLI flags are applied *after* the
//! file, so they always win. Docker deployments (which set `POSTGRES_*` and do
//! not mount a config file) are unaffected.
//!
//! ## Coverage
//!
//! The typed sections cover the fields an operator is likely to declare. The
//! `[env]` table is a raw escape hatch for everything else (and for future
//! keys): it maps environment-variable names to values and is applied first,
//! so typed sections win on conflict.

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;

/// Upper bound on the config file size (defends against a runaway/hostile
/// file being slurped into memory). Far above any realistic config.
const MAX_CONFIG_BYTES: u64 = 1024 * 1024;

/// The parsed `--config` file.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
pub struct FileConfig {
    pub server: ServerSection,
    pub database: DatabaseSection,
    #[serde(rename = "managed-storage")]
    pub managed_storage: ManagedStorageSection,
    pub auth: AuthSection,
    pub tls: TlsSection,
    pub retention: RetentionSection,
    pub websocket: WebsocketSection,
    pub logging: LoggingSection,
    pub metrics: MetricsSection,
    pub security: SecuritySection,
    #[serde(rename = "control-plane")]
    pub control_plane: ControlPlaneSection,
    /// Raw environment variables: `KEY = "value"` (or numbers/bools). Applied
    /// before the typed sections, so a typed field wins on conflict. This is
    /// the escape hatch that makes *every* `AppConfig` key reachable from the
    /// file.
    pub env: BTreeMap<String, toml::Value>,
}

impl FileConfig {
    /// Read and parse a TOML config file.
    pub fn load(path: &Path) -> Result<Self> {
        let metadata = std::fs::metadata(path)
            .with_context(|| format!("failed to inspect config file {}", path.display()))?;
        if !metadata.is_file() {
            return Err(anyhow!(
                "config path {} is not a regular file",
                path.display()
            ));
        }
        if metadata.len() > MAX_CONFIG_BYTES {
            return Err(anyhow!(
                "config file {} is too large ({} bytes; max {})",
                path.display(),
                metadata.len(),
                MAX_CONFIG_BYTES
            ));
        }
        // The file may hold JWT/API-key secrets; nudge operators toward 0600.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = metadata.permissions().mode() & 0o777;
            if mode & 0o077 != 0 {
                tracing::warn!(
                    "config file {} is accessible by group/other (mode {:o}); it may contain \
                     secrets — consider `chmod 600`",
                    path.display(),
                    mode
                );
            }
        }
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        toml::from_str(&raw).with_context(|| format!("invalid TOML config file {}", path.display()))
    }

    /// Materialize the file into the process environment.
    ///
    /// Existing variables are overwritten: the config file is authoritative
    /// over the environment (CLI flags are applied by the caller afterwards).
    pub fn apply_to_env(&self) -> Result<()> {
        // Escape hatch first, so typed sections win on conflict.
        for (key, value) in &self.env {
            let rendered = toml_value_to_string(value)
                .with_context(|| format!("config [env].{key} must be a scalar value"))?;
            set(key, rendered);
        }
        self.server.apply();
        self.database.apply();
        self.managed_storage.apply();
        self.auth.apply();
        self.tls.apply();
        self.retention.apply();
        self.websocket.apply();
        self.logging.apply();
        self.metrics.apply();
        self.security.apply();
        self.control_plane.apply();
        Ok(())
    }
}

fn set<K: AsRef<str>, V: std::fmt::Display>(key: K, value: V) {
    std::env::set_var(key.as_ref(), value.to_string());
}

fn set_opt<K: AsRef<str>, V: std::fmt::Display>(key: K, value: &Option<V>) {
    if let Some(value) = value {
        set(key, value);
    }
}

fn set_list<K: AsRef<str>>(key: K, value: &Option<Vec<String>>) {
    if let Some(list) = value {
        set(key, list.join(","));
    }
}

fn toml_value_to_string(value: &toml::Value) -> Result<String> {
    match value {
        toml::Value::String(s) => Ok(s.clone()),
        toml::Value::Integer(i) => Ok(i.to_string()),
        toml::Value::Float(f) => Ok(f.to_string()),
        toml::Value::Boolean(b) => Ok(b.to_string()),
        toml::Value::Datetime(d) => Ok(d.to_string()),
        toml::Value::Array(_) | toml::Value::Table(_) => {
            Err(anyhow::anyhow!("expected a scalar, got a TOML array/table"))
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
pub struct ServerSection {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub health_port: Option<u16>,
    pub https_port: Option<u16>,
    pub max_connections: Option<u32>,
    pub worker_count: Option<u32>,
    pub deployment_mode: Option<String>,
    /// `managed` (bundled PostgreSQL, the default) or `external`.
    pub storage: Option<String>,
    pub auto_bump: Option<bool>,
}

impl ServerSection {
    fn apply(&self) {
        set_opt("ACTAE_HOST", &self.host);
        set_opt("ACTAE_PORT", &self.port);
        set_opt("ACTAE_HEALTH_PORT", &self.health_port);
        set_opt("HTTPS_PORT", &self.https_port);
        set_opt("SERVER_MAX_CONNECTIONS", &self.max_connections);
        set_opt("WORKER_POOL_COUNT", &self.worker_count);
        set_opt("ACTAE_DEPLOYMENT_MODE", &self.deployment_mode);
        set_opt("ACTAE_STORAGE", &self.storage);
        set_opt("ACTAE_AUTO_BUMP", &self.auto_bump);
    }
}

/// Tuning for the bundled managed PostgreSQL (ignored when external storage
/// is selected).
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
pub struct ManagedStorageSection {
    pub data_dir: Option<String>,
    pub pg_port: Option<u16>,
    pub pg_password: Option<String>,
    pub pg_wipe: Option<bool>,
    pub pg_bin_dir: Option<String>,
    pub pg_no_host: Option<bool>,
}

impl ManagedStorageSection {
    fn apply(&self) {
        set_opt("ACTAE_DATA_DIR", &self.data_dir);
        set_opt("ACTAE_EMBEDDED_PG_PORT", &self.pg_port);
        set_opt("ACTAE_EMBEDDED_PG_PASSWORD", &self.pg_password);
        set_opt("ACTAE_EMBEDDED_PG_WIPE", &self.pg_wipe);
        set_opt("ACTAE_PG_BIN_DIR", &self.pg_bin_dir);
        set_opt("ACTAE_PG_NO_HOST", &self.pg_no_host);
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
pub struct DatabaseSection {
    /// Full main-database DSN. Sets both pools unless `client-url` is given.
    pub url: Option<String>,
    /// Full client-database DSN (defaults to `url`).
    pub client_url: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub name: Option<String>,
    pub ssl_mode: Option<String>,
    pub ssl_root_cert: Option<String>,
    pub client_user: Option<String>,
    pub client_password: Option<String>,
    pub client_host: Option<String>,
    pub client_port: Option<u16>,
    pub client_name: Option<String>,
    pub client_ssl_mode: Option<String>,
    pub max_connections: Option<u32>,
    pub min_connections: Option<u32>,
    pub connect_timeout: Option<u64>,
    pub idle_timeout: Option<u64>,
    pub pool_timeout: Option<u64>,
}

impl DatabaseSection {
    fn apply(&self) {
        const MAIN_DISCRETE: &[&str] = &[
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "POSTGRES_HOST",
            "POSTGRES_PORT",
            "POSTGRES_DB",
            "POSTGRES_SSL_MODE",
            "POSTGRES_SSL_ROOT_CERT",
        ];
        const CLIENT_DISCRETE: &[&str] = &[
            "CLIENT_POSTGRES_USER",
            "CLIENT_POSTGRES_PASSWORD",
            "CLIENT_POSTGRES_HOST",
            "CLIENT_POSTGRES_PORT",
            "CLIENT_POSTGRES_DB",
            "CLIENT_POSTGRES_SSL_MODE",
            "CLIENT_POSTGRES_SSL",
            "CLIENT_POSTGRES_SSL_ROOT_CERT",
        ];
        let has_main_discrete = self.user.is_some()
            || self.password.is_some()
            || self.host.is_some()
            || self.port.is_some()
            || self.name.is_some()
            || self.ssl_mode.is_some()
            || self.ssl_root_cert.is_some();
        let has_client_discrete = self.client_user.is_some()
            || self.client_password.is_some()
            || self.client_host.is_some()
            || self.client_port.is_some()
            || self.client_name.is_some()
            || self.client_ssl_mode.is_some();

        // A full URL is authoritative for its pool: drop discrete variables it
        // would otherwise be shadowed by. `url` alone also drives the client
        // pool (unless a client URL or client discrete fields are given),
        // matching `--database-url`.
        if self.url.is_some() {
            for key in MAIN_DISCRETE {
                std::env::remove_var(key);
            }
            if self.client_url.is_none() && !has_client_discrete {
                for key in CLIENT_DISCRETE {
                    std::env::remove_var(key);
                }
            }
        }
        if self.client_url.is_some() {
            for key in CLIENT_DISCRETE {
                std::env::remove_var(key);
            }
        }
        // Conversely, discrete fields must not be shadowed by an ambient DSN.
        if self.url.is_none() && has_main_discrete {
            std::env::remove_var("DATABASE_URL");
        }
        if self.client_url.is_none() && has_client_discrete {
            std::env::remove_var("CLIENT_DATABASE_URL");
        }

        set_opt("DATABASE_URL", &self.url);
        set_opt("CLIENT_DATABASE_URL", &self.client_url);
        set_opt("POSTGRES_USER", &self.user);
        set_opt("POSTGRES_PASSWORD", &self.password);
        set_opt("POSTGRES_HOST", &self.host);
        set_opt("POSTGRES_PORT", &self.port);
        set_opt("POSTGRES_DB", &self.name);
        set_opt("POSTGRES_SSL_MODE", &self.ssl_mode);
        set_opt("POSTGRES_SSL_ROOT_CERT", &self.ssl_root_cert);
        set_opt("CLIENT_POSTGRES_USER", &self.client_user);
        set_opt("CLIENT_POSTGRES_PASSWORD", &self.client_password);
        set_opt("CLIENT_POSTGRES_HOST", &self.client_host);
        set_opt("CLIENT_POSTGRES_PORT", &self.client_port);
        set_opt("CLIENT_POSTGRES_DB", &self.client_name);
        set_opt("CLIENT_POSTGRES_SSL_MODE", &self.client_ssl_mode);
        set_opt("DB_MAX_CONNECTIONS", &self.max_connections);
        set_opt("DB_MIN_CONNECTIONS", &self.min_connections);
        set_opt("DB_CONNECT_TIMEOUT", &self.connect_timeout);
        set_opt("DB_IDLE_TIMEOUT", &self.idle_timeout);
        set_opt("DB_POOL_TIMEOUT", &self.pool_timeout);
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
pub struct AuthSection {
    pub jwt_secret: Option<String>,
    pub api_key_hash_secret: Option<String>,
    pub enable_signup: Option<bool>,
    pub jwt_expiration: Option<u64>,
    pub jwt_issuer: Option<String>,
    pub jwt_audience: Option<String>,
    pub api_key_header: Option<String>,
    pub enable_anonymous: Option<bool>,
    pub require_auth_for_private: Option<bool>,
    pub initial_api_key: Option<String>,
    pub initial_admin_email: Option<String>,
    pub initial_admin_password: Option<String>,
}

impl AuthSection {
    fn apply(&self) {
        set_opt("JWT_SECRET", &self.jwt_secret);
        set_opt("API_KEY_HASH_SECRET", &self.api_key_hash_secret);
        set_opt("ENABLE_SIGNUP", &self.enable_signup);
        set_opt("JWT_EXPIRATION", &self.jwt_expiration);
        set_opt("JWT_ISSUER", &self.jwt_issuer);
        set_opt("JWT_AUDIENCE", &self.jwt_audience);
        set_opt("API_KEY_HEADER", &self.api_key_header);
        set_opt("ENABLE_ANONYMOUS", &self.enable_anonymous);
        set_opt("REQUIRE_AUTH_FOR_PRIVATE", &self.require_auth_for_private);
        set_opt("ACTAE_INITIAL_API_KEY", &self.initial_api_key);
        set_opt("ACTAE_INITIAL_ADMIN_EMAIL", &self.initial_admin_email);
        set_opt("ACTAE_INITIAL_ADMIN_PASSWORD", &self.initial_admin_password);
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
pub struct TlsSection {
    pub enabled: Option<bool>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub enable_mtls: Option<bool>,
    pub ca_cert_path: Option<String>,
    /// CA bundle for outbound control-plane HTTPS (also `ACTAE_CA_BUNDLE`).
    pub ca_bundle: Option<String>,
}

impl TlsSection {
    fn apply(&self) {
        set_opt("ENABLE_TLS", &self.enabled);
        set_opt("TLS_CERT_PATH", &self.cert_path);
        set_opt("TLS_KEY_PATH", &self.key_path);
        set_opt("ENABLE_MTLS", &self.enable_mtls);
        set_opt("CA_CERT_PATH", &self.ca_cert_path);
        set_opt("ACTAE_CA_BUNDLE", &self.ca_bundle);
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
pub struct RetentionSection {
    pub event_days: Option<u32>,
    pub archive_days: Option<u32>,
    pub state_days: Option<u32>,
    pub tool_execution_days: Option<u32>,
    pub check_interval_secs: Option<u64>,
}

impl RetentionSection {
    fn apply(&self) {
        set_opt("EVENT_RETENTION_DAYS", &self.event_days);
        set_opt("EVENT_ARCHIVE_RETENTION_DAYS", &self.archive_days);
        set_opt("STATE_RETENTION_DAYS", &self.state_days);
        set_opt("TOOL_EXECUTION_RETENTION_DAYS", &self.tool_execution_days);
        set_opt("RETENTION_CHECK_INTERVAL_SECS", &self.check_interval_secs);
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
pub struct WebsocketSection {
    pub max_connections: Option<u32>,
    pub connection_timeout: Option<u64>,
    pub ping_interval: Option<u64>,
    pub max_message_size: Option<u32>,
    pub buffer_size: Option<u32>,
    pub allowed_origins: Option<Vec<String>>,
    pub max_connections_per_ip: Option<u32>,
    pub require_auth: Option<bool>,
    pub enable_rate_limiting: Option<bool>,
    pub max_message_rate: Option<u32>,
}

impl WebsocketSection {
    fn apply(&self) {
        set_opt("WS_MAX_CONNECTIONS", &self.max_connections);
        set_opt("WS_CONNECTION_TIMEOUT", &self.connection_timeout);
        set_opt("WS_PING_INTERVAL", &self.ping_interval);
        set_opt("WS_MAX_MESSAGE_SIZE", &self.max_message_size);
        set_opt("MESSAGE_BUFFER_SIZE", &self.buffer_size);
        set_list("WEBSOCKET_ALLOWED_ORIGINS", &self.allowed_origins);
        set_opt(
            "WEBSOCKET_MAX_CONNECTIONS_PER_IP",
            &self.max_connections_per_ip,
        );
        set_opt("WEBSOCKET_REQUIRE_AUTH", &self.require_auth);
        set_opt("WEBSOCKET_ENABLE_RATE_LIMITING", &self.enable_rate_limiting);
        set_opt("WEBSOCKET_MAX_MESSAGE_RATE", &self.max_message_rate);
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
pub struct LoggingSection {
    pub level: Option<String>,
    pub format: Option<String>,
    pub file_path: Option<String>,
    pub file_name: Option<String>,
}

impl LoggingSection {
    fn apply(&self) {
        set_opt("LOG_LEVEL", &self.level);
        set_opt("LOG_FORMAT", &self.format);
        set_opt("LOG_FILE_PATH", &self.file_path);
        set_opt("LOG_FILE_NAME", &self.file_name);
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
pub struct MetricsSection {
    pub enabled: Option<bool>,
    pub detailed: Option<bool>,
    pub path: Option<String>,
    pub health_path: Option<String>,
    pub readiness_path: Option<String>,
}

impl MetricsSection {
    fn apply(&self) {
        set_opt("PROMETHEUS_EXPORTER_ENABLED", &self.enabled);
        set_opt("ENABLE_DETAILED_METRICS", &self.detailed);
        set_opt("METRICS_PATH", &self.path);
        set_opt("HEALTH_CHECK_PATH", &self.health_path);
        set_opt("READINESS_CHECK_PATH", &self.readiness_path);
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
pub struct SecuritySection {
    pub anonymous_session_timeout_seconds: Option<u64>,
    pub enable_ip_allowlist: Option<bool>,
    pub enable_security_monitoring: Option<bool>,
    pub auth_failure_lockout_threshold: Option<u32>,
    pub auth_failure_lockout_duration_seconds: Option<u64>,
    pub enable_connection_security_validation: Option<bool>,
    pub trusted_proxies: Option<Vec<String>>,
    pub http_allowed_origins: Option<Vec<String>>,
}

impl SecuritySection {
    fn apply(&self) {
        set_opt(
            "ANONYMOUS_SESSION_TIMEOUT_SECONDS",
            &self.anonymous_session_timeout_seconds,
        );
        set_opt("ENABLE_IP_ALLOWLIST", &self.enable_ip_allowlist);
        set_opt(
            "ENABLE_SECURITY_MONITORING",
            &self.enable_security_monitoring,
        );
        set_opt(
            "AUTH_FAILURE_LOCKOUT_THRESHOLD",
            &self.auth_failure_lockout_threshold,
        );
        set_opt(
            "AUTH_FAILURE_LOCKOUT_DURATION_SECONDS",
            &self.auth_failure_lockout_duration_seconds,
        );
        set_opt(
            "ENABLE_CONNECTION_SECURITY_VALIDATION",
            &self.enable_connection_security_validation,
        );
        set_list("ACTAE_TRUSTED_PROXIES", &self.trusted_proxies);
        set_list("HTTP_ALLOWED_ORIGINS", &self.http_allowed_origins);
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
pub struct ControlPlaneSection {
    pub url: Option<String>,
    pub install_id: Option<String>,
    pub enroll_secret: Option<String>,
    pub enroll_key: Option<String>,
    pub sync_interval_secs: Option<u64>,
    pub subscription_grace_secs: Option<i64>,
}

impl ControlPlaneSection {
    fn apply(&self) {
        set_opt("ACTAE_CONTROL_PLANE_URL", &self.url);
        set_opt("ACTAE_INSTALL_ID", &self.install_id);
        set_opt("ACTAE_ENROLL_SECRET", &self.enroll_secret);
        set_opt("ACTAE_ENROLL_KEY", &self.enroll_key);
        set_opt("ACTAE_SYNC_INTERVAL_SECS", &self.sync_interval_secs);
        set_opt(
            "ACTAE_SUBSCRIPTION_GRACE_SECS",
            &self.subscription_grace_secs,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn parses_a_full_file() {
        let toml = r#"
            [server]
            host = "0.0.0.0"
            port = 9000
            storage = "external"

            [database]
            url = "postgresql://u:p@h:5432/db?sslmode=disable"

            [auth]
            jwt-secret = "0123456789abcdef0123456789abcdef"
            enable-signup = true

            [tls]
            enabled = true
            cert-path = "/c.pem"

            [retention]
            event-days = 7

            [websocket]
            allowed-origins = ["https://a.example", "https://b.example"]

            [security]
            trusted-proxies = ["10.0.0.0/8"]

            [env]
            BATCH_SIZE = 50
            MEMORY_OPTIMIZER_ENABLED = false
        "#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.server.port, Some(9000));
        assert_eq!(cfg.server.storage.as_deref(), Some("external"));
        assert_eq!(cfg.auth.enable_signup, Some(true));
        assert_eq!(cfg.websocket.allowed_origins.as_ref().unwrap().len(), 2);
        assert_eq!(cfg.env.get("BATCH_SIZE").unwrap().as_integer(), Some(50));
    }

    #[test]
    fn rejects_unknown_fields() {
        let err = toml::from_str::<FileConfig>("[server]\nnot-a-field = 1\n").unwrap_err();
        assert!(err.to_string().contains("not-a-field") || err.to_string().contains("unknown"));
    }

    #[test]
    fn load_reads_a_file_and_rejects_bad_inputs() {
        let dir = tempfile::tempdir().unwrap();
        let good = dir.path().join("actae.toml");
        std::fs::write(&good, "[server]\nport = 8100\n").unwrap();
        assert_eq!(FileConfig::load(&good).unwrap().server.port, Some(8100));

        // A directory is not a config file.
        assert!(FileConfig::load(dir.path()).is_err());

        // Oversized files are refused before parsing.
        let big = dir.path().join("big.toml");
        std::fs::write(&big, vec![b'#'; (MAX_CONFIG_BYTES + 1) as usize]).unwrap();
        assert!(FileConfig::load(&big).is_err());

        // Missing files error with context.
        assert!(FileConfig::load(&dir.path().join("nope.toml")).is_err());
    }

    #[test]
    fn database_url_overrides_ambient_discrete_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("POSTGRES_HOST", "old-host");
        std::env::set_var("CLIENT_POSTGRES_HOST", "old-client");
        let cfg: FileConfig =
            toml::from_str("[database]\nurl = \"postgresql://u:p@new:5432/db\"\n").unwrap();
        cfg.apply_to_env().unwrap();
        assert_eq!(
            std::env::var("DATABASE_URL").unwrap(),
            "postgresql://u:p@new:5432/db"
        );
        // The URL is authoritative for both pools, so the stale discrete hosts
        // are gone (the client pool then reuses DATABASE_URL).
        assert!(std::env::var("POSTGRES_HOST").is_err());
        assert!(std::env::var("CLIENT_POSTGRES_HOST").is_err());
        for k in ["DATABASE_URL", "POSTGRES_HOST", "CLIENT_POSTGRES_HOST"] {
            std::env::remove_var(k);
        }
    }

    #[test]
    fn discrete_fields_override_ambient_dsn() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("DATABASE_URL", "postgresql://old");
        let cfg: FileConfig = toml::from_str(
            "[database]\nhost = \"h\"\nuser = \"u\"\npassword = \"p\"\nname = \"db\"\n",
        )
        .unwrap();
        cfg.apply_to_env().unwrap();
        assert!(std::env::var("DATABASE_URL").is_err());
        assert_eq!(std::env::var("POSTGRES_HOST").unwrap(), "h");
        for k in [
            "DATABASE_URL",
            "POSTGRES_HOST",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "POSTGRES_DB",
        ] {
            std::env::remove_var(k);
        }
    }

    #[test]
    fn applies_file_over_environment() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("ACTAE_PORT", "1111");
        std::env::set_var("BATCH_SIZE", "10");
        let cfg: FileConfig =
            toml::from_str("[server]\nport = 2222\n\n[env]\nBATCH_SIZE = 99\n").unwrap();
        cfg.apply_to_env().unwrap();
        assert_eq!(std::env::var("ACTAE_PORT").unwrap(), "2222");
        assert_eq!(std::env::var("BATCH_SIZE").unwrap(), "99");
        std::env::remove_var("ACTAE_PORT");
        std::env::remove_var("BATCH_SIZE");
    }
}
