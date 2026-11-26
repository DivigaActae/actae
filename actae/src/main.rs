#![allow(clippy::type_complexity, clippy::nonminimal_bool)]
// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0


use actae::auth::api_key::validate_api_key;
use actae::clean_json;
use actae::config::AppConfig;
use actae::database::json_compat::CleanJson;
use actae::database::repositories::Repository as _;
use actae::events::handler::{AuthContext, EventHandler};
use actae::https_server::HttpsServer;
use actae::security::SecurityManager;
use actae::websocket::server::WebSocketServer;
use anyhow::{Context, Result};
use axum::http::{HeaderName, HeaderValue};
use axum::{
    extract::DefaultBodyLimit,
    http::{HeaderMap, StatusCode},
    middleware,
    routing::{delete, get, patch, post, put},
    Json, Router,
};
#[cfg(feature = "rustls")]
use sonic_rs::json;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tower_http::set_header::SetResponseHeaderLayer;

use tokio::signal;
use tracing::{debug, error, info, warn};

mod handlers;

use actae::{
    api::AppServerState,
    config::logging,
    database::client_migrations::run_client_migrations,
    state::AppState,
    websocket::{connection_manager::ConnectionManager, WebSocketManager},
};

#[cfg(feature = "allocator")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    // Load a local `.env` for DEVELOPMENT convenience only. A downloaded
    // release/community binary must NOT be silently hijacked into
    // external-database mode by an unrelated `.env` sitting in the working
    // directory (or one of its parents), so dotenv is loaded only in debug
    // builds — or when explicitly opted in with `ACTAE_LOAD_DOTENV=1`.
    // Production/self-hosted operators pass real environment variables
    // (systemd `EnvironmentFile=` / compose `environment:`), which this does
    // not affect. This must run before the CLI dispatch below, which returns
    // early.
    if cfg!(debug_assertions) || std::env::var("ACTAE_LOAD_DOTENV").as_deref() == Ok("1") {
        dotenvy::dotenv().ok();
    }

    // CLI subcommands (actae auth ..., actae keys ..., actae doctor)
    // run in-process and exit; everything else starts the server.
    let cli_args: Vec<String> = std::env::args().skip(1).collect();
    if let Some(first) = cli_args.first() {
        if first == "auth"
            || first == "keys"
            || first == "doctor"
            || first == "enroll"
            || first == "upgrade"
            || first == "add"
            || first == "explain"
            || first == "passwd"
            || first == "login"
            || first == "logout"
        {
            return actae::cli::run(&cli_args).await;
        }
        if first == "--version" || first == "-V" || first == "version" {
            println!("{}", actae::version_string());
            return Ok(());
        }
    }

    // Top-level server arguments. `--help` exits here; unknown flags — including
    // the removed `--embedded` — fail loudly instead of being ignored.
    let server_args = actae::config::args::parse_server_args(&cli_args).map_err(|e| {
        eprintln!("{e}");
        e
    })?;
    if server_args.help {
        print_server_usage();
        return Ok(());
    }
    if server_args.version {
        println!("{}", actae::version_string());
        return Ok(());
    }

    // Install the ring crypto provider as the process default (the https
    // server and the control-plane/download HTTP clients use it). Only
    // meaningful when the `rustls` feature is on; the crate still compiles
    // without it (plain-HTTP deployments).
    #[cfg(feature = "rustls")]
    let _ = rustls::crypto::ring::default_provider().install_default();

    // The config file is applied to the environment first, then CLI flags, so
    // the effective precedence is CLI > file > environment > default. This runs
    // before logging init so a `[logging]` section is honored.
    if let Some(path) = server_args.config_file.as_deref() {
        let file = actae::config::file::FileConfig::load(path).map_err(|e| {
            eprintln!("Failed to load config file: {e:#}");
            e
        })?;
        file.apply_to_env().map_err(|e| {
            eprintln!("Failed to apply config file {}: {e:#}", path.display());
            e
        })?;
    }
    if let Some(host) = server_args.host.as_deref() {
        std::env::set_var("ACTAE_HOST", host);
    }
    if let Some(port) = server_args.port {
        std::env::set_var("ACTAE_PORT", port.to_string());
    }
    if let Some(port) = server_args.health_port {
        std::env::set_var("ACTAE_HEALTH_PORT", port.to_string());
    }
    if let Some(url) = server_args.database_url.as_deref() {
        std::env::set_var("DATABASE_URL", url);
        if server_args.client_database_url.is_none() {
            std::env::set_var("CLIENT_DATABASE_URL", url);
        }
        // An explicit external URL must not be shadowed by stale discrete
        // POSTGRES_* variables left in the environment.
        for key in [
            "POSTGRES_HOST",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "POSTGRES_DB",
            "CLIENT_POSTGRES_HOST",
            "CLIENT_POSTGRES_USER",
            "CLIENT_POSTGRES_PASSWORD",
            "CLIENT_POSTGRES_DB",
        ] {
            std::env::remove_var(key);
        }
    }
    if let Some(url) = server_args.client_database_url.as_deref() {
        std::env::set_var("CLIENT_DATABASE_URL", url);
    }

    // Initialize logging before the managed-PostgreSQL block so startup
    // warnings (port selection, orphan recovery, PG download) are visible.
    // Only the logging fields are loaded here — the managed block must start
    // its PostgreSQL before the full config load (it provides the POSTGRES_*
    // variables the server connects with).
    let log_config = AppConfig::load_logging_only().map_err(|e| {
        eprintln!("Failed to load logging configuration: {e}");
        e
    })?;
    let log_guard = logging::init(&log_config).map_err(|e| {
        eprintln!("Failed to initialize logging: {e}");
        e
    })?;

    // User-facing and always visible regardless of the log filter: the only
    // things a normal (release) run prints during startup are this line, the
    // "fully operational" line, and the banner below.
    println!("Actae is starting…");

    // Resolve the storage backend. Explicit `ACTAE_STORAGE` wins; otherwise an
    // external database means external, and a bare `actae` is self-contained
    // (bundled managed PostgreSQL).
    let managed_storage = match std::env::var("ACTAE_STORAGE")
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("managed") | Some("embedded") => true,
        Some("external") => {
            if !actae::config::external_database_configured() {
                eprintln!(
                    "ACTAE_STORAGE=external requires a database: set DATABASE_URL / POSTGRES_* \
                     or pass --database-url <dsn>"
                );
                std::process::exit(2);
            }
            false
        }
        Some(other) => {
            eprintln!("ACTAE_STORAGE must be `managed` or `external`, got `{other}`");
            std::process::exit(2);
        }
        None => !actae::config::external_database_configured(),
    };

    // Hosted tenants run the bundled managed PostgreSQL per workload today
    // (docs/CLOUD_HOSTED_TECHNICAL_PLAN.md); per-tenant external storage lands
    // with GA provisioning. Warn loudly instead of refusing to boot: the hosted
    // provisioner passes no DATABASE_URL/POSTGRES_* yet.
    let deployment_mode = std::env::var("ACTAE_DEPLOYMENT_MODE").unwrap_or_default();
    if deployment_mode == "hosted" && managed_storage {
        eprintln!(
            "WARNING: ACTAE_DEPLOYMENT_MODE=hosted is using the bundled managed \
             PostgreSQL (per-tenant external storage is not provisioned yet)"
        );
    }
    // Write the resolved backend back so AppConfig and health/metrics agree.
    std::env::set_var(
        "ACTAE_STORAGE",
        if managed_storage {
            "managed"
        } else {
            "external"
        },
    );

    // Make an implicitly-selected external database loud and actionable: a
    // stray `DATABASE_URL` / `POSTGRES_*` in the environment must never leave a
    // first-time user staring at connection retries with no explanation.
    if !managed_storage {
        let target = actae::config::redacted_database_target();
        eprintln!();
        eprintln!("Actae is using an EXTERNAL PostgreSQL database from the environment.");
        eprintln!("  Target: {target}");
        eprintln!("  DATABASE_URL / CLIENT_DATABASE_URL / POSTGRES_* select the external database");
        eprintln!("  and win over the bundled one. To run the self-contained community build");
        eprintln!("  instead, unset those variables (or set ACTAE_STORAGE=managed) and start");
        eprintln!("  Actae again.");
        eprintln!();
    }

    // Managed storage: run a bundled PostgreSQL child (the SAME code path for
    // development, CI and production). Only enabled when no external database
    // is configured — external always wins.
    let mut startup_banner: String = String::new();
    let mut embedded_pg: Option<actae::embedded_pg::EmbeddedPostgres> = None;
    let mut http_listener: Option<std::net::TcpListener> = None;
    // (http_port, pg_port, data_dir) of the managed instance, captured so the
    // SDK client-credential file (`~/.actae/client.json`) can record it.
    let mut managed_identity: Option<(u16, u16, std::path::PathBuf)> = None;
    if managed_storage {
        let started = actae::embedded_pg::start().await.map_err(|e| {
            eprintln!("Failed to start managed PostgreSQL: {e:#}");
            e
        })?;
        let http_port = started.http_port;
        let pg_port = started.pg_port;
        let data_dir = started.data_dir.clone();
        let password = started.password.clone();
        let bumped = started.bumped;
        let configured_http_port = started.configured_http_port;
        let password_was_generated = started.password_was_generated;
        http_listener = Some(started.http_listener);
        managed_identity = Some((http_port, pg_port, data_dir.clone()));

        // Point the server's pools at the embedded cluster. The password
        // is percent-encoded in the URL so an operator-supplied
        // ACTAE_EMBEDDED_PG_PASSWORD with `@`, `/`, `%`, spaces or quotes
        // can never corrupt the DSN (the POSTGRES_* vars carry the raw
        // value; the pool builders encode it themselves).
        let url_password =
            percent_encoding::utf8_percent_encode(&password, percent_encoding::NON_ALPHANUMERIC);
        std::env::set_var("ACTAE_PORT", http_port.to_string());
        std::env::set_var("POSTGRES_USER", "actae");
        std::env::set_var("POSTGRES_PASSWORD", &password);
        std::env::set_var("POSTGRES_HOST", "127.0.0.1");
        std::env::set_var("POSTGRES_PORT", pg_port.to_string());
        std::env::set_var("POSTGRES_DB", "actae");
        std::env::set_var("POSTGRES_SSL_MODE", "disable");
        std::env::set_var("CLIENT_POSTGRES_USER", "actae");
        std::env::set_var("CLIENT_POSTGRES_PASSWORD", &password);
        std::env::set_var("CLIENT_POSTGRES_HOST", "127.0.0.1");
        std::env::set_var("CLIENT_POSTGRES_PORT", pg_port.to_string());
        std::env::set_var("CLIENT_POSTGRES_DB", "actae");
        std::env::set_var("CLIENT_POSTGRES_SSL", "disable");
        std::env::set_var(
            "DATABASE_URL",
            format!("postgresql://actae:{url_password}@127.0.0.1:{pg_port}/actae"),
        );

        // Instance identity for humans and tooling (`actae doctor`).
        let instance_info = serde_json::json!({
            "url": format!("http://127.0.0.1:{http_port}"),
            "pg_port": pg_port,
            "data_dir": data_dir.display().to_string(),
        });
        let _ = tokio::fs::create_dir_all(&data_dir).await;
        if let Err(e) = tokio::fs::write(
            data_dir.join("url"),
            serde_json::to_string_pretty(&instance_info).unwrap_or_default(),
        )
        .await
        {
            warn!(
                "Failed to write instance identity file {}: {} — `actae doctor` will \
                     not list this instance",
                data_dir.join("url").display(),
                e
            );
        }

        let headline = if bumped {
            format!(
                "port {configured_http_port} was busy, listening on http://127.0.0.1:{http_port}"
            )
        } else {
            format!("listening on http://127.0.0.1:{http_port}")
        };
        let password_hint = if password_was_generated {
            format!(
                "\n  Postgres password: {password}  (shown once — set \
                     ACTAE_EMBEDDED_PG_PASSWORD to pin it)"
            )
        } else {
            String::new()
        };
        startup_banner = format!(
            "\nActae (managed storage) — {headline}\n  Data dir:   {}\n  Postgres:   \
                 postgresql://actae@127.0.0.1:{pg_port}/actae (scram auth){}\n  psql:       \
                 psql -h 127.0.0.1 -p {pg_port} -U actae -d actae\n",
            data_dir.display(),
            password_hint,
        );

        embedded_pg = Some(started.postgres);
    }

    let mut config = AppConfig::load().map_err(|e| {
        eprintln!("Failed to load configuration: {e}");
        e
    })?;

    // Resolve the signing secret (see src/secrets.rs). A fresh install gets a
    // generated, persisted random secret so it never runs on the built-in
    // default; the default is kept only when existing API keys were hashed with
    // it (an upgrade — rotating it would invalidate every key).
    {
        let existing_keys = actae::secrets::client_has_api_keys(&config).await;
        match actae::secrets::resolve_into_config(&mut config, existing_keys) {
            Ok(actae::secrets::SecretOutcome::Generated) => {
                info!("Generated and persisted a new JWT signing secret");
            }
            Ok(actae::secrets::SecretOutcome::Persisted) => {
                info!("Using the persisted JWT signing secret");
            }
            Ok(actae::secrets::SecretOutcome::Operator) => {}
            Ok(actae::secrets::SecretOutcome::LegacyDefault) => {
                // The banner warning below explains how to secure the instance.
            }
            Err(e) => {
                warn!(
                    "could not persist a JWT signing secret ({e:#}); using the current \
                     value — set JWT_SECRET to pin it explicitly"
                );
            }
        }
    }

    info!("Starting Actae - durable execution history for AI agents");
    info!("Log level: {}", config.log_level);
    info!("Server will bind to {}:{}", config.host, config.port);

    // A well-known default JWT signing secret means anyone who knows it can
    // forge a dashboard session. Fail loudly (but do not refuse to start, so an
    // existing install is not bricked) and tell the operator exactly how to fix
    // it. Set both secrets together: API keys are HMAC-hashed with
    // API_KEY_HASH_SECRET, which defaults to the JWT secret.
    if config.uses_default_jwt_secret() {
        eprintln!();
        eprintln!("SECURITY WARNING: this instance is using the built-in default JWT secret.");
        eprintln!("Anyone who knows it can forge dashboard sessions. Before exposing it, set:");
        eprintln!("  export JWT_SECRET=\"$(openssl rand -hex 32)\"");
        eprintln!("  export API_KEY_HASH_SECRET=\"$(openssl rand -hex 32)\"   # re-create existing API keys after changing this");
        eprintln!("Then restart Actae (this also invalidates existing dashboard sessions).");
        eprintln!();
    }


    // Security posture: without TLS, binding a non-loopback address sends
    // API keys and JWTs in clear text. The config default is loopback when
    // TLS is off — this fires only when an operator explicitly exposed it.
    if !config.enable_tls && !is_loopback_host(&config.host) {
        warn!(
            "Serving plain HTTP on {}:{} — no TLS. API keys and tokens travel unencrypted; \
             put Actae behind a TLS-terminating reverse proxy (Caddy/nginx) or set ENABLE_TLS=true.",
            config.host, config.port
        );
    }

    // Connection manager for WebSocket message delivery
    let connection_manager = Arc::new(ConnectionManager::new(
        config.ws_max_connections as usize,
        config.broadcaster_queue_size as usize,
    ));

    // Application state
    let app_state = Arc::new(
        AppState::new(Some(connection_manager.clone()), Arc::new(config.clone()))
            .await
            .map_err(|e| {
                error!("Failed to initialize application state: {}", e);
                e
            })?,
    );
    info!("Application state initialized");

    // Event store (Postgres-backed append-only agent events)
    // Security manager for rate limiting and threat detection
    let security_manager =
        Arc::new(SecurityManager::new(Arc::new(config.clone())).map_err(|e| {
            error!("Failed to create SecurityManager: {}", e);
            e
        })?);

    let event_store = Arc::new(actae::events::store::EventStore::new(
        app_state.db_manager.main_pool.clone(),
    ));
    event_store.initialize().await.map_err(|e| {
        error!("Failed to initialize event store: {}", e);
        anyhow::anyhow!("Event store initialization failed: {e}")
    })?;
    info!("Agent event store initialized");

    // Run client database migrations for users, sessions, api_keys, etc.
    if let Err(e) = run_client_migrations(&app_state.db_manager.client_pool).await {
        error!("Failed to run client migrations: {}", e);
        return Err(e);
    }
    info!("Client database migrations applied");

    // Provision local API keys (`source = 'local'`): the fixed dev key in
    // dev mode, an explicit `ACTAE_INITIAL_API_KEY` seed, or a generated
    // initial key on a standalone first boot. These are ordinary keys —
    // subject to subscription enforcement like any other.
    // Captured (not printed here) so every first-run credential is shown
    // together in the single "Next steps" banner at the end of startup.
    let provisioned_key: Option<String> = match provision_initial_keys(&app_state).await {
        Ok(initial_key) => initial_key,
        Err(e) => {
            error!("Failed to provision initial API keys: {}", e);
            return Err(e);
        }
    };

    // Ensure the standalone dashboard has a way in: create a first-boot
    // local admin account when the user table is empty. Enrolled instances
    // authenticate through the Actae portal instead (no local users).
    let initial_admin: Option<FirstBootAdmin> = match provision_initial_user(&app_state).await {
        Ok(boot) => boot,
        Err(e) => {
            error!("Failed to provision the initial dashboard user: {}", e);
            return Err(e);
        }
    };

    // Restore persisted subscription state (a restart must stay locked if
    // the subscription was inactive) and start the control plane sync loop.
    if let Err(e) = app_state.control_plane.load_persisted_state().await {
        error!("Failed to load persisted subscription state: {}", e);
        return Err(e);
    }
    app_state.control_plane.start();

    connection_manager
        .set_event_store(event_store.clone())
        .await;

    // Delivery tracker: at-least-once broadcast delivery with retry + DLQ
    let delivery_config = actae::broadcast::delivery_tracker::DeliveryConfig {
        max_retries: 5,
        base_backoff_ms: 100,
        max_backoff_ms: 30_000,
        retry_interval_ms: 500,
        dead_letter_enabled: true,
    };
    let delivery_tracker = Arc::new(actae::broadcast::delivery_tracker::DeliveryTracker::new(
        delivery_config,
    ));
    info!("Delivery tracker initialized");

    // Dead letter queue (PostgreSQL-backed)
    let dlq = Arc::new(actae::broadcast::dead_letter_queue::DeadLetterQueue::new(
        Arc::new(app_state.db_manager.main_pool.clone()),
    ));
    dlq.init().await.map_err(|e| {
        error!("Failed to initialize dead letter queue: {}", e);
        anyhow::anyhow!("DLQ init failed: {e}")
    })?;
    info!("Dead letter queue initialized");

    // Wire DLQ into delivery tracker
    {
        use actae::broadcast::delivery_tracker::{DeadLetterSink, PendingDelivery};
        struct DlqSink(Arc<actae::broadcast::dead_letter_queue::DeadLetterQueue>);
        impl DeadLetterSink for DlqSink {
            fn enqueue(&self, delivery: &PendingDelivery) {
                let dlq = self.0.clone();
                let delivery = delivery.clone();
                tokio::spawn(async move {
                    if let Err(e) = dlq.enqueue(&delivery).await {
                        tracing::error!(error = %e, "Failed to persist dead letter");
                    }
                });
            }
        }
        delivery_tracker
            .set_dlq(Arc::new(DlqSink(dlq.clone())))
            .await;
    }

    // Event handler (record/subscribe/replay/query)
    let event_handler = Arc::new(
        EventHandler::new(
            event_store.clone(),
            connection_manager.clone(),
            delivery_tracker.clone(),
            app_state.control_plane.clone(),
        )
        .with_anonymous(config.enable_anonymous),
    );
    info!("Event handler initialized");

    // Start retry worker for pending deliveries
    let retry_tracker = delivery_tracker.clone();
    let retry_cm = connection_manager.clone();
    let retry_send_fn: Arc<dyn Fn(uuid::Uuid, &str) -> Result<(), String> + Send + Sync> =
        Arc::new(move |conn_id, payload| {
            let arc_payload = std::sync::Arc::new(payload.to_string());
            retry_cm
                .try_send_to_connection(
                    &conn_id,
                    actae::websocket::connection::OutgoingMessage::Serialized(arc_payload),
                )
                .map_err(|e| e.to_string())
        });
    tokio::spawn(async move {
        retry_tracker.run_retry_worker(retry_send_fn).await;
    });
    info!("Delivery retry worker started");

    // WebSocket manager for CDC subscription distribution
    let _websocket_manager = Arc::new(
        WebSocketManager::new(&config, (*app_state).clone())
            .await
            .map_err(|e| {
                error!("Failed to create WebSocket manager: {}", e);
                e
            })?,
    );
    info!("WebSocket manager created");

    // WebSocket server
    let ws_server = Arc::new(
        WebSocketServer::new(
            &config,
            connection_manager.clone(),
            app_state.as_ref().clone(),
        )
        .await?,
    );
    ws_server.start_background_with_usage_tracking().await?;

    ws_server.set_event_handler(event_handler.clone()).await;

    // Task scheduler for periodic background work
    let (scheduler_shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(16);
    let mut scheduler = actae::scheduler::TaskScheduler::new(&scheduler_shutdown_tx);

    // Immediate socket reauthorization on control-plane revocations: wake
    // whenever a sync revoked >=1 key and de-authenticate affected sockets.
    {
        let connection_manager = connection_manager.clone();
        let app_state = app_state.clone();
        let notifier = app_state.control_plane.revoked_notifier();
        tokio::spawn(async move {
            loop {
                notifier.notified().await;
                let revoked = connection_manager.reauthorize_all(&app_state).await;
                tracing::info!(
                    "control-plane revoke signal: re-authorized, de-authenticated {} connection(s)",
                    revoked
                );
            }
        });
    }

    // Register: event retention purge
    {
        let event_store = event_store.clone();
        let retention_days = config.retention_days;
        scheduler.register(
            "event_retention_purge",
            Duration::from_secs(config.retention_check_interval_secs),
            Duration::from_secs(60),
            move || {
                let event_store = event_store.clone();
                async move {
                    let cutoff = chrono::Utc::now() - chrono::Duration::days(retention_days as i64);
                    event_store
                        .purge_events_older_than(cutoff)
                        .await
                        .map(|_| ())
                        .map_err(|e| actae::scheduler::TaskError::Transient(e.to_string()))
                }
            },
        );
    }

    // Archive erasure is a second, explicit lifecycle stage. A zero value
    // preserves historical self-hosted behavior; hosted plans set a window.
    if config.archive_retention_days > 0 {
        let event_store = event_store.clone();
        let archive_days = config.archive_retention_days;
        scheduler.register(
            "event_archive_erasure",
            Duration::from_secs(config.retention_check_interval_secs),
            Duration::from_secs(60),
            move || {
                let event_store = event_store.clone();
                async move {
                    let cutoff = chrono::Utc::now() - chrono::Duration::days(archive_days as i64);
                    event_store
                        .purge_archive_older_than(cutoff)
                        .await
                        .map(|_| ())
                        .map_err(|e| actae::scheduler::TaskError::Transient(e.to_string()))
                }
            },
        );
    }

    // State-snapshot retention: age-based purge of versioned state, subject to
    // provenance anchors. This closes the unbounded-storage path through
    // state versions that the event-only retention sweep could not touch.
    if config.state_retention_days > 0 {
        let event_store = event_store.clone();
        let state_days = config.state_retention_days;
        scheduler.register(
            "state_retention_purge",
            Duration::from_secs(config.retention_check_interval_secs),
            Duration::from_secs(60),
            move || {
                let event_store = event_store.clone();
                async move {
                    let cutoff = chrono::Utc::now() - chrono::Duration::days(state_days as i64);
                    event_store
                        .purge_states_older_than(cutoff)
                        .await
                        .map(|_| ())
                        .map_err(|e| actae::scheduler::TaskError::Transient(e.to_string()))
                }
            },
        );
    }

    // Tool-execution retention: tombstone completed/failed executions whose
    // replay window expired. Consumer offsets are NEVER age-purged — they are
    // live coordination state removed only by group/channel deletion or
    // lifecycle cleanup of a vanished owning resource.
    if config.tool_execution_retention_days > 0 {
        let event_store = event_store.clone();
        let tool_days = config.tool_execution_retention_days;
        scheduler.register(
            "tool_execution_tombstone",
            Duration::from_secs(config.retention_check_interval_secs),
            Duration::from_secs(60),
            move || {
                let event_store = event_store.clone();
                async move {
                    let cutoff = chrono::Utc::now() - chrono::Duration::days(tool_days as i64);
                    event_store
                        .tombstone_expired_executions(cutoff)
                        .await
                        .map(|_| ())
                        .map_err(|e| actae::scheduler::TaskError::Transient(e.to_string()))
                }
            },
        );
    }


    // Register: credential reauthorizer — periodically re-checks every
    // authenticated WebSocket connection's API key status against the DB and
    // de-authenticates sockets whose key was revoked or expired (audit item
    // 11). A CLI `actae keys revoke` in another process cannot invalidate the
    // in-process auth cache, so this sweep bounds the window (default 30s).
    {
        let connection_manager = connection_manager.clone();
        let app_state = app_state.clone();
        scheduler.register_infallible(
            "credential_reauthorizer",
            Duration::from_secs(30),
            move || {
                let connection_manager = connection_manager.clone();
                let app_state = app_state.clone();
                async move {
                    let changed = connection_manager.reauthorize_all(&app_state).await;
                    if changed > 0 {
                        tracing::info!(
                            "credential_reauthorizer: refreshed principal on {} connection(s)",
                            changed
                        );
                    }
                }
            },
        );
    }

    // Register: server checkpoint persistence
    {
        let pool = app_state.db_manager.main_pool.clone();
        scheduler.register_infallible("server_checkpoint", Duration::from_secs(30), move || {
            let pool = pool.clone();
            async move {
                use actae::scheduler::checkpoint::ServerCheckpoint;
                let cp = ServerCheckpoint::new(None, std::collections::HashMap::new(), false);
                let _ = cp.save(&pool).await;
            }
        });
    }

    // Register: consumer lease reaper — expires lapsed leases so claimed
    // work becomes reclaimable by other consumers.
    {
        let event_store = event_store.clone();
        scheduler.register(
            "consumer_lease_reaper",
            Duration::from_secs(30),
            Duration::from_secs(60),
            move || {
                let event_store = event_store.clone();
                async move {
                    let expired = event_store
                        .expire_leases()
                        .await
                        .map_err(|e| actae::scheduler::TaskError::Transient(e.to_string()))?;
                    if expired > 0 {
                        tracing::info!(
                            "consumer_lease_reaper: expired {} consumer leases",
                            expired
                        );
                    }
                    Ok(())
                }
            },
        );
    }

    // Register: wake-up dispatcher — fires due `scheduler.wakeup` events
    // and broadcasts them live to subscribers.
    {
        let event_handler = event_handler.clone();
        scheduler.register(
            "wakeup_dispatcher",
            Duration::from_secs(5),
            Duration::from_secs(60),
            move || {
                let event_handler = event_handler.clone();
                async move {
                    let dispatched = event_handler
                        .dispatch_due_wakeups(50)
                        .await
                        .map_err(|e| actae::scheduler::TaskError::Transient(e.to_string()))?;
                    if dispatched > 0 {
                        tracing::info!("wakeup_dispatcher: dispatched {} wake-ups", dispatched);
                    }
                    Ok(())
                }
            },
        );
    }

    // Start all scheduled tasks
    scheduler.start();

    // Insert global server state reference
    let server_state = AppServerState {
        app_state: app_state.as_ref().clone(),
        connection_manager: connection_manager.clone(),
        event_handler: event_handler.clone(),
        security_manager: security_manager.clone(),
        ws_server: ws_server.clone(),
        start_time: Instant::now(),
    };
    // Store in global for Router<()> handlers
    actae::api::set_global_state(server_state.clone()).map_err(|e| anyhow::anyhow!("{e}"))?;
    let api_routes = actae::api::create_api_routes();

    // ── Build Main Router (HTTPS, config.port) ──
    // Serves: API + Dashboard static files + WebSocket + Health + Metrics
    let log_all_access = std::env::var("ACTAE_ACCESS_LOG_ALL")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false);
    let cors_layer = match &config.http_allowed_origins {
        Some(origins) => {
            let values = origins
                .iter()
                .map(|origin| {
                    HeaderValue::from_str(origin)
                        .with_context(|| format!("invalid HTTP_ALLOWED_ORIGINS entry: {origin}"))
                })
                .collect::<Result<Vec<_>>>()?;
            CorsLayer::new()
                .allow_origin(AllowOrigin::list(values))
                .allow_methods(Any)
                .allow_headers(Any)
                .expose_headers(Any)
        }
        None => CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any)
            .expose_headers(Any),
    };

    let main_router = Router::new()
        // Health & metrics (always available on main port too)
        .route(
            &app_state.config.health_check_path,
            get(health_check_handler),
        )
        .route(
            &app_state.config.readiness_check_path,
            get(readiness_check_handler),
        )
        .route(&app_state.config.metrics_path, get(metrics_handler))
        .route("/metrics.json", get(metrics_json_handler))
        // Event API: record a new agent event
        .route("/api/v1/events/record", post(handlers::events::record_event_handler))
        // Event API: atomic event+state transition
        .route("/api/v1/events/transition", post(handlers::events::transition_handler))
        // Event API: replay events from a channel
        .route("/api/v1/events/replay/{channel_id}", get(handlers::events::replay_events_handler))
        // Event API: query events across channels
        .route("/api/v1/events/query", post(handlers::events::query_events_handler))
        // Event API: get latest cursor for resume
        .route(
            "/api/v1/events/cursor/{channel_id}",
            get(handlers::events::latest_cursor_handler),
        )
        // Event API: verify the tamper-evident hash chain (audit selling point 3)
        .route(
            "/api/v1/events/verify/{channel_id}",
            get(handlers::events::verify_chain_handler),
        )
        .route(
            "/api/v1/events/{event_id}/causal-graph",
            get(handlers::events::causal_graph_handler),
        )
        // Event API: list all active channels
        .route("/api/v1/channels", get(handlers::events::list_channels_handler))
        // State API: save cursor-aligned context snapshot
        .route("/api/v1/state/{channel_id}", post(handlers::state::save_state_handler))
        // State API: load latest context snapshot for resume
        .route("/api/v1/state/{channel_id}", get(handlers::state::load_state_handler))
        // State API: list version history for a channel
        .route("/api/v1/state/{channel_id}/versions", get(handlers::state::list_states_handler))
        // State API: load a specific version
        .route("/api/v1/state/{channel_id}/version/{version}", get(handlers::state::load_state_version_handler))
        // State API: delete a specific version
        .route("/api/v1/state/{channel_id}/version/{version}", delete(handlers::state::delete_state_version_handler))
        // Fork API: fork a channel at a cursor into a new experiment fork
        .route("/api/v1/channels/fork", post(handlers::channels::fork_channel_handler))
        // Structural state diff between two channels' latest saved states.
        .route("/api/v1/channels/diff", get(handlers::channels::diff_channels_handler))
        .route("/api/v1/channels/compare", get(handlers::channels::compare_channels_handler))
        .route("/api/v1/channels/{channel_id}/outcome", patch(handlers::channels::set_outcome_handler))
        .route("/api/v1/channels/{channel_id}/promote", post(handlers::channels::promote_channel_handler))
        .route("/api/v1/channels/{channel_id}", delete(handlers::channels::delete_channel_handler))
        // Channel metadata
        .route("/api/v1/channels/{channel_id}/metadata", get(handlers::channels::channel_metadata_handler))
        // Server-owned step index resolution
        .route("/api/v1/channels/{channel_id}/steps", get(handlers::channels::latest_step_handler))
        .route("/api/v1/channels/{channel_id}/steps/{step_number}", get(handlers::channels::resolve_step_handler))
        // Fork listing (direct children)
        .route("/api/v1/channels/{channel_id}/forks", get(handlers::channels::list_forks_handler))
        // Execution tree (recursive)
        .route("/api/v1/channels/{channel_id}/fork-tree", get(handlers::channels::fork_tree_handler))
        // Decision trail (lineage chain + boundary state + tool-execution ledger)
        .route("/api/v1/channels/{channel_id}/trail", get(handlers::channels::decision_trail_handler))
        // Immutable fork receipt (provenance record)
        .route("/api/v1/channels/{channel_id}/receipt", get(handlers::channels::fork_receipt_handler))
        // Lifecycle: close a channel (stamps closed_at on the registry)
        .route("/api/v1/channels/{channel_id}/close", post(handlers::channels::close_channel_handler))
        // Metadata update
        .route("/api/v1/channels/{channel_id}/metadata", put(handlers::channels::update_metadata_handler))
        // Consumer groups API: durable workers with leases and offsets
        .route("/api/v1/groups", post(handlers::groups::create_group_handler))
        .route("/api/v1/groups", get(handlers::groups::list_groups_handler))
        .route("/api/v1/groups/{group_id}", delete(handlers::groups::delete_group_handler))
        .route("/api/v1/groups/{group_id}/join", post(handlers::groups::join_group_handler))
        .route("/api/v1/groups/{group_id}/work", post(handlers::groups::claim_work_handler))
        .route("/api/v1/groups/{group_id}/ack", post(handlers::groups::ack_handler))
        .route("/api/v1/groups/{group_id}/heartbeat", post(handlers::groups::heartbeat_handler))
        .route("/api/v1/groups/{group_id}/offsets", get(handlers::groups::offsets_handler))
        // Execution groups: durable coordination across independently-running agents.
        .route("/api/v1/execution-groups", post(handlers::execution_groups::create))
        .route("/api/v1/execution-groups", get(handlers::execution_groups::list))
        .route("/api/v1/execution-groups/{group_id}/members", post(handlers::execution_groups::add_member))
        .route("/api/v1/execution-groups/{group_id}/members", get(handlers::execution_groups::members))
        .route("/api/v1/execution-groups/{group_id}/members/{member_id}/claim", post(handlers::execution_groups::claim_member))
        .route("/api/v1/execution-groups/{group_id}/members/{member_id}/heartbeat", post(handlers::execution_groups::heartbeat_member))
        .route("/api/v1/execution-groups/{group_id}/members/{member_id}/release", post(handlers::execution_groups::release_member))
        .route("/api/v1/execution-groups/{group_id}/messages", post(handlers::execution_groups::send))
        .route("/api/v1/execution-groups/{group_id}/members/{member_id}/messages", get(handlers::execution_groups::messages))
        .route("/api/v1/execution-group-messages/{message_id}/ack", post(handlers::execution_groups::acknowledge))
        .route("/api/v1/execution-group-forks", post(handlers::execution_groups::create_fork))
        .route("/api/v1/execution-group-forks/{fork_group_id}", get(handlers::execution_groups::get_fork))
        .route("/api/v1/execution-group-forks/{fork_group_id}/members/{member_id}/promote", post(handlers::execution_groups::promote_fork_member))
        // Experiment platform (groups, ranking)
        .route("/api/v1/experiments", post(handlers::experiments::create_experiment_handler))
        .route("/api/v1/experiments", get(handlers::experiments::list_experiments_handler))
        .route("/api/v1/experiments/{group_id}", get(handlers::experiments::get_experiment_handler))
        .route("/api/v1/experiments/{group_id}/members", post(handlers::experiments::add_experiment_member_handler))
        .route("/api/v1/experiments/{group_id}/rank", get(handlers::experiments::rank_experiment_handler))
        // Scheduler API: persisted wake-ups
        .route("/api/v1/scheduler/wakeups", post(handlers::wakeups::create_wakeup_handler))
        .route("/api/v1/scheduler/wakeups", get(handlers::wakeups::list_wakeups_handler))
        .route("/api/v1/scheduler/wakeups/{id}", get(handlers::wakeups::get_wakeup_handler))
        .route("/api/v1/scheduler/wakeups/{id}", delete(handlers::wakeups::cancel_wakeup_handler))
        // Idempotent tool executions API
        .route("/api/v1/executions/claim", post(handlers::executions::claim_execution_handler))
        .route("/api/v1/executions", get(handlers::executions::list_executions_handler))
        .route("/api/v1/executions/{id}", get(handlers::executions::get_execution_handler))
        .route("/api/v1/executions/{id}", delete(handlers::executions::delete_execution_handler))
        .route("/api/v1/executions/{id}/complete", post(handlers::executions::complete_execution_handler))
        .route("/api/v1/executions/{id}/fail", post(handlers::executions::fail_execution_handler))
        .route("/api/v1/executions/{id}/heartbeat", post(handlers::executions::heartbeat_execution_handler))
        .route("/api/v1/executions/{id}/cancel", post(handlers::executions::cancel_execution_handler))
        // Auth API: signup, login, me, logout
        // Admin API: stats, users, api-keys
        .merge(api_routes)
        // WebSocket upgrade endpoint
        .route("/ws", get(ws_upgrade_handler))
        // Security headers (applied to all responses)
        .layer(SetResponseHeaderLayer::overriding(
            HeaderName::from_static("x-content-type-options"),
            HeaderValue::from_static("nosniff"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            HeaderName::from_static("x-frame-options"),
            HeaderValue::from_static("DENY"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            HeaderName::from_static("referrer-policy"),
            HeaderValue::from_static("strict-origin-when-cross-origin"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            HeaderName::from_static("content-security-policy"),
            HeaderValue::from_static(
                "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self' wss:; img-src 'self' data:; font-src 'self'"
            ),
        ))
        .layer(cors_layer)
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
        // Peer-IP resolution: derives the real client IP from the socket peer
        // (and trusted proxies only) and records it under x-actae-client-ip so
        // rate limits/auth logging are never spoofable (audit item 6).
        .layer(middleware::from_fn(resolve_peer_ip_middleware))
        // Access log: one line per API/WS request (method, path, status,
        // duration) so client connections are visible in the logs. Health
        // probes, metrics scraping and dashboard static files are excluded
        // unless ACTAE_ACCESS_LOG_ALL=1 (also logs /healthz, /readyz, /metrics).
        .layer(middleware::from_fn(move |req, next| {
            access_log(req, next, log_all_access)
        }))
        // Dashboard static files (FALLBACK — lowest priority, any non-API
        // path). Embedded in the binary (production), from ACTAE_DASHBOARD_DIR
        // or the dev checkout otherwise.
        .fallback_service(actae::dashboard::fallback_router());

    // ── Start Main Server (HTTPS or HTTP on config.port) ──
    // Health, metrics, API, dashboard, and WebSocket all served on the same
    // port. In managed-storage mode the socket was reserved at startup
    // (`http_listener`) and is served from directly — eliminating the port
    // race while PostgreSQL initializes. External-PG / plain deployments
    // bind normally.
    let main_addr = SocketAddr::from((config.host.parse::<std::net::IpAddr>()?, config.port));

    // A bind failure must never leave the embedded postgres orphaned, so the
    // server task reports fatal errors through this channel instead of
    // `process::exit` (which skips destructors and the graceful shutdown).
    let (server_failure_tx, mut server_failure_rx) =
        tokio::sync::oneshot::channel::<anyhow::Error>();

    let main_server_task = if config.enable_tls {
        let https_server = match http_listener {
            Some(listener) => {
                HttpsServer::new_with_listener(Arc::new(config.clone()), main_router, listener)
            }
            None => HttpsServer::new(Arc::new(config.clone()), main_router),
        };
        if let Err(e) = https_server.validate_tls_files().await {
            error!("HTTPS server TLS validation failed: {}", e);
            if let Some(pg) = embedded_pg.as_mut() {
                pg.stop().await;
            }
            return Err(e);
        }
        tokio::spawn(async move {
            if let Err(e) = https_server.start().await {
                error!("HTTPS server error: {}", e);
                let _ = server_failure_tx.send(anyhow::anyhow!("HTTPS server failed: {e:#}"));
            }
        })
    } else {
        info!(
            " Starting HTTP server on http://{}:{}",
            config.host, config.port
        );
        let server = match http_listener {
            Some(listener) => axum_server::from_tcp(listener),
            None => axum_server::bind(main_addr),
        };
        tokio::spawn(async move {
            if let Err(e) = server
                .serve(main_router.into_make_service_with_connect_info::<std::net::SocketAddr>())
                .await
            {
                error!("HTTP server error: {}", e);
                let _ = server_failure_tx.send(anyhow::anyhow!("HTTP server failed: {e:#}"));
            }
        })
    };

    // Now that startup is done, reveal the configured log level (a compiled
    // binary hid the internal startup chatter until here) and print the single
    // user-facing readiness line.
    log_guard.operational();
    println!("Actae is fully operational");
    if !startup_banner.is_empty() {
        print!("{startup_banner}");
    }

    // The last thing in the log: exactly what a first-time self-hosted user
    // needs next. Shown on every boot (harmless on restarts, where the API key
    // line falls back to the `actae keys create` hint because this process did
    // not mint a key). The community binary is fully usable without any of
    // this; the SDK line is the one that turns "a running server" into a
    // working agent integration.
    {
        let scheme = if config.enable_tls { "https" } else { "http" };
        let display_host = if config.host == "0.0.0.0" || config.host == "::" {
            "127.0.0.1"
        } else {
            config.host.as_str()
        };

        // Publish local SDK client credentials (`~/.actae/client.json`) on the
        // boot that minted a key: the "run it and your SDKs are configured"
        // path. Best-effort — any failure degrades to the banner (the key is
        // still printed below) and never aborts the boot.
        let mut client_creds_note: Option<String> = None;
        if let Some(key) = provisioned_key.as_deref() {
            if config.control_plane_url.is_none()
                && actae::client_credentials::auto_write_enabled()
            {
                let endpoint = format!("{scheme}://{display_host}:{}", config.port);
                let mut creds =
                    actae::client_credentials::ClientCredentials::new(endpoint, key);
                if let Some((http_port, pg_port, data_dir)) = managed_identity.as_ref() {
                    creds = creds.with_instance(actae::client_credentials::InstanceInfo {
                        http_port: *http_port,
                        pg_port: Some(*pg_port),
                        data_dir: data_dir.display().to_string(),
                        pid: std::process::id(),
                    });
                }
                // Do not clobber a sibling instance that is still live.
                let existing_live = match actae::client_credentials::ClientCredentials::load() {
                    Some(existing) => endpoint_is_live(&existing.endpoint).await,
                    None => false,
                };
                let default_path = actae::client_credentials::ClientCredentials::file_path();
                match actae::client_credentials::publish_at_with_liveness(
                    &default_path,
                    &creds,
                    false,
                    Some(existing_live),
                ) {
                    Ok(outcome) if in_container() => {
                        client_creds_note = Some(format!(
                            "saved inside the container to {} — copy the key above for SDKs on \
                             your host",
                            outcome.path.display()
                        ));
                    }
                    Ok(outcome) if outcome.primary => {
                        client_creds_note = Some(format!(
                            "saved to {} — Python/Go/TS SDKs authenticate automatically, \
                             no setup needed",
                            outcome.path.display()
                        ));
                    }
                    Ok(outcome) => {
                        client_creds_note = Some(format!(
                            "another Actae instance is live; saved to {} — point SDKs with \
                             ACTAE_URL or `actae login --url`",
                            outcome.path.display()
                        ));
                    }
                    Err(e) => {
                        warn!("Could not save SDK client credentials: {e:#}");
                        client_creds_note = Some(format!(
                            "not saved ({e}) — run `actae login` or set ACTAE_API_KEY"
                        ));
                    }
                }
            }
        }

        println!();
        println!("Next steps");
        println!("  Dashboard:   {scheme}://{display_host}:{}", config.port);
        match provisioned_key.as_deref() {
            Some(key) => {
                println!("  API key:     {key}  (shown once — store it now)");
                if let Some(note) = client_creds_note.as_deref() {
                    println!("  SDK setup:   {note}");
                }
            }
            None if app_state.control_plane.is_enrolled() => println!(
                "  API key:     synced from your control plane; mint a local one with \
                 `actae keys create <name>`"
            ),
            None => println!(
                "  API key:     existing keys — run `actae keys list`; mint a new one with \
                 `actae keys create <name>`"
            ),
        }
        if let Some(admin) = initial_admin.as_ref() {
            match (admin.password.as_deref(), admin.setup_token.as_deref()) {
                (Some(password), _) => println!(
                    "  Admin login: {}  password: {password}  (shown once — reset with \
                     `actae passwd`)",
                    admin.email
                ),
                (None, Some(token)) => {
                    if actae::auth::local_dashboard_auto_login_enabled() {
                        // A same-machine browser is signed in automatically, so
                        // do not tell the user to "set the password at the
                        // dashboard" — the token is only for remote/LAN use.
                        println!(
                            "  Admin login: {}  (opens automatically on this machine — no sign-in)",
                            admin.email
                        );
                        println!(
                            "    from another machine: set a password with `actae passwd` \
                             (or use the setup token below)"
                        );
                        println!(
                            "    setup token: {token}  (valid 24h, for the dashboard sign-in form)"
                        );
                    } else {
                        println!(
                            "  Admin login: {}  (first run — set the password at the dashboard)",
                            admin.email
                        );
                        println!(
                            "    setup token: {token}  (valid 24h, shown once — regenerate with \
                             `actae passwd`)"
                        );
                    }
                }
                (None, None) => {}
            }
        }
        println!("  Install an SDK:");
        println!("    Python       pip install actae-client");
        println!("    Go           go get github.com/actae-dev/actae/sdks/go");
        println!("    TypeScript   npm install @actae/sdk");
        println!("  Or detect your stack:  actae add");
        println!("  Quickstart:            https://actae.dev/docs/quickstart/");
        println!("  Docs:                  https://actae.dev/docs/");
        println!();
    }

    // Self-hosted outbound relay (Phase 7): when enabled, the instance dials
    // OUT to the fleet gateway's relay listener and executes relayed canonical
    // commands against its own canonical API.
    {
        let relay_cfg = actae::relay::RelayConfig::from_env()?;
        let local_base = format!("http://{}:{}", config.host, config.port);
        actae::relay::spawn_relay_client(relay_cfg, local_base);
    }

    debug!("Waiting for shutdown signal (Ctrl+C)...");

    // Handle Ctrl+C (SIGINT), `kill` (SIGTERM) and terminal close (SIGHUP)
    // so any of them exits gracefully (stops embedded postgres, no
    // orphaned processes).
    let mut server_error: Option<anyhow::Error> = None;
    let shutdown_result: std::io::Result<()> = {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate())?;
            let mut sighup = signal(SignalKind::hangup())?;
            tokio::select! {
                result = signal::ctrl_c() => result,
                _ = sigterm.recv() => Ok(()),
                _ = sighup.recv() => Ok(()),
                failure = &mut server_failure_rx => {
                    server_error = failure.ok();
                    Ok(())
                }
            }
        }
        #[cfg(not(unix))]
        {
            tokio::select! {
                result = signal::ctrl_c() => result,
                failure = &mut server_failure_rx => {
                    server_error = failure.ok();
                    Ok(())
                }
            }
        }
    };
    shutdown_result?;
    if server_error.is_some() {
        error!("Fatal server failure — initiating graceful shutdown");
    } else {
        warn!("Received shutdown signal, initiating graceful shutdown");
    }

    let shutdown_timeout = Duration::from_secs(30);
    let shutdown_start = Instant::now();

    // Signal scheduler tasks to stop
    let _ = scheduler_shutdown_tx.send(());

    if let Err(e) = tokio::time::timeout(Duration::from_secs(10), ws_server.stop()).await {
        error!("WebSocket server shutdown timed out: {}", e);
    }

    let active_connections = connection_manager.cleanup_all_connections().await;
    info!("Cleaned up {} active connections", active_connections);

    main_server_task.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Mark clean shutdown in checkpoint
    {
        let pool = app_state.db_manager.main_pool.clone();
        let _ = actae::scheduler::checkpoint::ServerCheckpoint::mark_clean_shutdown(&pool).await;
    }

    let shutdown_duration = shutdown_start.elapsed();
    info!(
        "Actae shutdown complete (took {}ms)",
        shutdown_duration.as_millis()
    );

    if shutdown_duration > shutdown_timeout {
        warn!("Shutdown took longer than expected");
    }

    // Close the DB pools BEFORE stopping postgres, so deadpool does not log
    // "connection error / could not be recycled" warnings as the server
    // disappears underneath live connections.
    app_state.db_manager.main_pool.close();
    app_state.db_manager.client_pool.close();

    // Stop our embedded postgres child (graceful `pg_ctl stop`, then a hard
    // kill). Ownership is unambiguous — we only ever target the child we
    // spawned.
    if let Some(pg) = embedded_pg.as_mut() {
        pg.stop().await;
    }

    if let Some(e) = server_error {
        return Err(e);
    }

    Ok(())
}

/// Resolve the caller IP for rate limiting / audit logging.
///
/// Consumes ONLY the `x-actae-client-ip` value recorded by the peer-IP
/// middleware, which derives it from the actual socket peer and (only when
/// the peer is a configured trusted proxy) the `X-Forwarded-For` chain. A
/// client-supplied `X-Forwarded-For` is never read here, and there is NO
/// fallback to it: if the middleware did not run, callers have no IP rather
/// than a spoofed one (audit: auth rate limiting trusts raw IP headers).
fn client_ip(headers: &HeaderMap) -> Option<std::net::IpAddr> {
    actae::auth::authorization::client_ip(headers)
}

/// Check rate limit for an incoming HTTP request.
/// Returns 429 response with Retry-After if limit exceeded, None otherwise.
fn check_rate_limit(
    security_manager: &SecurityManager,
    headers: &HeaderMap,
) -> Option<(StatusCode, Json<CleanJson>)> {
    let ip = client_ip(headers);

    if let Some(ip) = ip {
        match security_manager
            .rate_limiter()
            .check_message_limit(Some(ip), None)
        {
            actae::security::rate_limit::RateLimitResult::Limited(info) => {
                return Some((
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(clean_json!({
                        "error": "Rate limit exceeded",
                        "retry_after_seconds": info.retry_after_seconds,
                    })),
                ));
            }
            actae::security::rate_limit::RateLimitResult::Allowed => {}
        }
    }
    None
}

/// Consume one token from the per-IP authentication-abuse bucket. Returns a
/// 429 response when the bucket is exhausted at consume time.
///
/// Called ONLY on authentication FAILURE paths, so the bucket counts failed
/// attempts (brute-force/abuse) and a valid key burst is never throttled as
/// abuse. Successful authentication never touches this bucket.
fn consume_auth_rate_limit(
    security_manager: &SecurityManager,
    ip: Option<std::net::IpAddr>,
) -> Option<(StatusCode, Json<CleanJson>)> {
    let ip = ip?;
    match security_manager.rate_limiter().check_auth_limit(ip) {
        actae::security::rate_limit::RateLimitResult::Limited(info) => Some((
            StatusCode::TOO_MANY_REQUESTS,
            Json(clean_json!({
                "status": "rate_limited",
                "error": "Too many failed authentication attempts",
                "retry_after_seconds": info.retry_after_seconds,
            })),
        )),
        actae::security::rate_limit::RateLimitResult::Allowed => None,
    }
}

/// Resolve the real client IP for every request and record it under the
/// internal `x-actae-client-ip` header so downstream rate limiting, auth
/// audit logging, and WebSocket identity can never be spoofed through a
/// client-supplied `X-Forwarded-For` (audit item 6).
async fn resolve_peer_ip_middleware(
    mut req: axum::extract::Request,
    next: middleware::Next,
) -> axum::response::Response {
    let Some(connect) = req
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .copied()
    else {
        return next.run(req).await;
    };
    let peer = connect.0.ip();
    let trusted = actae::api::global_state()
        .app_state
        .config
        .trusted_proxies
        .clone();
    let xff = req
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok());
    let resolved =
        actae::auth::authorization::resolve_client_addr(Some(peer), xff, &trusted).unwrap_or(peer);
    if let Ok(value) = HeaderValue::from_str(&resolved.to_string()) {
        req.headers_mut().insert("x-actae-client-ip", value);
    }
    next.run(req).await
}

/// One-line access log for API and WebSocket requests, so client
/// connections (SDK calls, WS upgrades) are visible in the server logs.
/// Health probes, metrics scraping and dashboard static files are excluded
/// by default (probe noise); `ACTAE_ACCESS_LOG_ALL=1` also logs
/// /healthz, /readyz and /metrics.
async fn access_log(
    req: axum::extract::Request,
    next: middleware::Next,
    log_all: bool,
) -> axum::response::Response {
    let path = req.uri().path().to_string();
    let is_api = path.starts_with("/api/") || path == "/ws";
    let is_probe =
        path.starts_with("/healthz") || path.starts_with("/readyz") || path.starts_with("/metrics");
    if !is_api && !(log_all && is_probe) {
        return next.run(req).await;
    }
    let method = req.method().clone();
    let start = Instant::now();
    let resp = next.run(req).await;
    let status = resp.status().as_u16();
    let duration_ms = start.elapsed().as_millis() as u64;
    // Per-request access log at DEBUG: the dashboard polls every few seconds,
    // so INFO here floods the console. Set RUST_LOG=debug (or =info,actae=debug)
    // to see it.
    debug!(
        method = %method,
        path = %path,
        status = status,
        duration_ms = duration_ms,
        "HTTP request",
    );
    resp
}

/// Subscription enforcement: block API-key traffic and anonymous access
/// when the control-plane lock is active. Returns `None` when the request
/// may proceed.
fn subscription_block(
    app_state: &AppState,
    api_key_auth: bool,
) -> Option<(StatusCode, Json<CleanJson>)> {
    let reason = if api_key_auth {
        app_state.control_plane.blocks_key()
    } else {
        app_state.control_plane.blocks_anonymous()
    }?;
    let (status, code) = if reason.http_status == 503 {
        (StatusCode::SERVICE_UNAVAILABLE, "subscription_unverifiable")
    } else {
        (StatusCode::PAYMENT_REQUIRED, "subscription_required")
    };
    Some((
        status,
        Json(clean_json!({
            "status": code,
            "error": reason.message,
        })),
    ))
}

/// Resolve the caller's AuthContext from the request headers.
/// Returns an AuthContext or an error response.
///
/// Integrates SecurityManager auth rate limiting with the same lockout,
/// progressive delay, and threat detection used by the WebSocket authenticator.
/// This brings HTTP security to parity with WS.
///
/// `enforce_subscription` controls whether the control-plane subscription
/// lock is applied to the resolved principal. It is `true` for customer
/// traffic and `false` for the metrics surface: a locked instance must stay
/// observable so operators/monitoring can read the subscription state that
/// explains the lock.
async fn resolve_auth_context(
    headers: &HeaderMap,
    app_state: &AppState,
    security_manager: &SecurityManager,
    enforce_subscription: bool,
) -> Result<AuthContext, (StatusCode, Json<CleanJson>)> {
    let ip = client_ip(headers);

    // Try X-API-Key header
    if let Some(key_val) = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
    {
        let key_user_id = key_val[..8.min(key_val.len())].to_string();

        // Pre-auth lockout check (same as WS SecurityManager.validate_auth_attempt,
        // minus the per-attempt bucket consumption). Pass key prefix as user_id so
        // lockout tracks per-key even without IP.
        let pre_auth = security_manager
            .validate_auth_lockout(
                ip,
                Some(&key_user_id),
                actae::auth::websocket_auth::AuthMethod::ApiKey,
            )
            .await;
        match pre_auth {
            Ok(actae::security::SecurityValidationResult::Blocked(reason)) => {
                security_manager.record_auth_failure(
                    ip,
                    Some(&key_user_id),
                    actae::auth::websocket_auth::AuthMethod::ApiKey,
                    reason.clone(),
                );
                return Err((
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(clean_json!({"status": "blocked", "error": reason,
                        "retry_after_seconds": 300})),
                ));
            }
            Ok(actae::security::SecurityValidationResult::RateLimited(info)) => {
                security_manager.record_auth_failure(
                    ip,
                    Some(&key_user_id),
                    actae::auth::websocket_auth::AuthMethod::ApiKey,
                    format!("Rate limit exceeded: {}", info.limit_type),
                );
                return Err((
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(
                        clean_json!({"status": "rate_limited", "error": "Too many attempts",
                        "retry_after_seconds": info.retry_after_seconds}),
                    ),
                ));
            }
            _ => {} // Allowed or RequiresAdditionalAuth — proceed
        }

        match validate_api_key(&key_val, app_state).await {
            Ok(Some(api_key)) => {
                // Subscription lock: every API key is blocked while the
                // instance is locked; only the dashboard's JWT auth is
                // exempt. The metrics surface skips the lock so a locked
                // instance stays observable.
                if enforce_subscription {
                    if let Some(block) = subscription_block(app_state, true) {
                        return Err(block);
                    }
                }
                let key_id = api_key.id.clone();
                let permissions = actae::auth::parse_api_key_permissions(&api_key.permissions);

                security_manager.record_auth_success(
                    ip,
                    key_id.clone(),
                    actae::auth::websocket_auth::AuthMethod::ApiKey,
                );
                return Ok(AuthContext::ApiKey {
                    key_id,
                    key_source: api_key.source.clone(),
                    expires_at: api_key.expires_at,
                    permissions,
                });
            }
            Ok(None) => {
                if app_state.config.enable_anonymous {
                    if enforce_subscription {
                        if let Some(block) = subscription_block(app_state, false) {
                            return Err(block);
                        }
                    }
                    // Don't record auth failure — just proceed as anonymous
                } else {
                    let limited = consume_auth_rate_limit(security_manager, ip);
                    security_manager.record_auth_failure(
                        ip,
                        Some(&key_user_id),
                        actae::auth::websocket_auth::AuthMethod::ApiKey,
                        "Invalid API key".to_string(),
                    );
                    // A burst of failed attempts gets throttled as abuse; a
                    // single bad key still surfaces the plain 401.
                    if let Some(resp) = limited {
                        return Err(resp);
                    }
                    return Err((
                        StatusCode::UNAUTHORIZED,
                        Json(clean_json!({"status": "unauthorized", "error": "Invalid API key"})),
                    ));
                }
            }
            Err(e) => {
                // Never leak backend/database detail to the client (audit
                // item 15): log it server-side and return a stable code.
                let request_id = uuid::Uuid::new_v4().to_string();
                error!(
                    request_id = %request_id,
                    "API-key auth backend error for prefix {}: {:#}",
                    key_user_id, e
                );
                let limited = consume_auth_rate_limit(security_manager, ip);
                security_manager.record_auth_failure(
                    ip,
                    Some(&key_user_id),
                    actae::auth::websocket_auth::AuthMethod::ApiKey,
                    format!("Auth error: {e}"),
                );
                if let Some(resp) = limited {
                    return Err(resp);
                }
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(clean_json!({
                        "status": "authentication_backend_error",
                        "request_id": request_id,
                    })),
                ));
            }
        }
    }

    // Try Bearer JWT (dashboard session minted by /api/v1/auth/login or
    // /api/v1/auth/api-key-login). Mirrors the WebSocket authenticator:
    // api_key-method sessions carry a SaaS subject with no local
    // users-table row, so only the token signature/expiry is checked.
    // Phase 4: a Bearer that is not a session JWT is then tried as a
    // purpose-bound federation token (observation/command).
    if let Some(auth_header) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        if let Some(token) = auth_header
            .strip_prefix("Bearer ")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            let session_claims = actae::auth::jwt::validate_token(token, &app_state.config);
            let session_error = session_claims.as_ref().err().map(|e| e.to_string());
            if let Ok(claims) = session_claims {
                // Server-side revocation: a session JWT is only accepted while
                // its `jti` has a live row in `dashboard_sessions` (logout,
                // "sign out everywhere" and password resets revoke). A token
                // without a `jti` is not revocable and is rejected.
                let session_ok = match claims.jti.as_deref() {
                    Some(jti) => {
                        actae::auth::session_store::is_active(
                            &app_state.db_manager.client_pool,
                            jti,
                        )
                        .await
                    }
                    None => false,
                };
                if session_ok {
                    security_manager.record_auth_success(
                        ip,
                        claims.sub.clone(),
                        actae::auth::websocket_auth::AuthMethod::JWT,
                    );
                    // Dashboard sessions grant full access — the dashboard
                    // needs replay, fork and metadata operations after an
                    // API-key login.
                    return Ok(AuthContext::Authenticated {
                        user_id: claims.sub,
                        permissions: actae::auth::authorization::ApiKeyPermissions::full(),
                    });
                }
                security_manager.record_auth_failure(
                    ip,
                    None,
                    actae::auth::websocket_auth::AuthMethod::JWT,
                    "Session revoked, expired or unknown".to_string(),
                );
                let limited = consume_auth_rate_limit(security_manager, ip);
                if let Some(resp) = limited {
                    return Err(resp);
                }
                return Err((
                    StatusCode::UNAUTHORIZED,
                    Json(clean_json!({
                        "status": "unauthorized",
                        "error": "Session expired or signed out — sign in again",
                    })),
                ));
            }

            // Federation token (Phase 4): purpose-bound, instance-scoped,
            // fail-closed. On success the principal is read-only (observation)
            // or a single-operation grant (command); it can never impersonate
            // dashboard/JWT authority.
            let fed_config = app_state.control_plane.federation_config();
            let instance_id = app_state.config.install_id.clone().unwrap_or_default();
            match actae::auth::federation::verify(&fed_config, token, &instance_id) {
                Ok(fed_claims) => {
                    security_manager.record_auth_success(
                        ip,
                        fed_claims.principal.clone(),
                        actae::auth::websocket_auth::AuthMethod::Federation,
                    );
                    return Ok(actae::auth::federation::to_auth_context(&fed_claims));
                }
                Err(fed_err) => {
                    tracing::debug!(
                        federation = true,
                        error = %fed_err,
                        enabled = fed_config.enabled,
                        keys = fed_config.verification_keys.len(),
                        "federation token rejected"
                    );
                }
            }

            let limited = consume_auth_rate_limit(security_manager, ip);
            security_manager.record_auth_failure(
                ip,
                None,
                actae::auth::websocket_auth::AuthMethod::JWT,
                format!(
                    "Invalid session token: {}",
                    session_error.unwrap_or_else(|| "unknown".into())
                ),
            );
            if let Some(resp) = limited {
                return Err(resp);
            }
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(clean_json!({
                    "status": "unauthorized",
                    "error": "Invalid or expired session token",
                })),
            ));
        }
    }

    // No credentials — the subscription lock takes precedence over the
    // anonymous policy so locked instances explain why instead of 401.
    if enforce_subscription {
        if let Some(block) = subscription_block(app_state, false) {
            return Err(block);
        }
    }
    if !app_state.config.enable_anonymous {
        let limited = consume_auth_rate_limit(security_manager, ip);
        security_manager.record_auth_failure(
            ip,
            None,
            actae::auth::websocket_auth::AuthMethod::ApiKey,
            "No credentials provided, anonymous access disabled".to_string(),
        );
        if let Some(resp) = limited {
            return Err(resp);
        }
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(clean_json!({
                "status": "unauthorized",
                "error": "Authentication required — no valid API key or token provided"
            })),
        ));
    }
    Ok(AuthContext::Anonymous)
}

// ---- WebSocket Upgrade Handler ----

async fn ws_upgrade_handler(
    ws: axum::extract::ws::WebSocketUpgrade,
    headers: HeaderMap,
    connect_info: axum::extract::ConnectInfo<std::net::SocketAddr>,
) -> impl axum::response::IntoResponse {
    let state = actae::api::global_state().clone();
    ws.on_upgrade(move |socket| handle_ws_connection(socket, headers.clone(), state, connect_info))
}

async fn handle_ws_connection(
    socket: axum::extract::ws::WebSocket,
    headers: HeaderMap,
    state: AppServerState,
    connect_info: axum::extract::ConnectInfo<std::net::SocketAddr>,
) {
    // Use the real socket peer for the WS connection identity, honoring
    // X-Forwarded-For only when the immediate peer is a configured trusted
    // proxy (audit item 6). Previously the client-controlled first XFF value
    // was used directly, so a caller could spoof any IP for rate limits and
    // audit logging.
    let trusted = state.app_state.config.trusted_proxies.clone();
    let xff = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok());
    let peer = connect_info.0.ip();
    let addr = actae::auth::authorization::resolve_client_addr(Some(peer), xff, &trusted)
        .map(|ip| std::net::SocketAddr::new(ip, connect_info.0.port()))
        .unwrap_or(connect_info.0);

    if let Err(e) = state
        .ws_server
        .handle_axum_connection(socket, addr, &headers)
        .await
    {
        error!("WebSocket connection error from {}: {}", addr, e);
    }
}

// ---- Legacy Health/Metrics Handlers ----

async fn health_check_handler() -> (StatusCode, Json<CleanJson>) {
    let server_state = actae::api::global_state();
    let start_time = Instant::now();
    let mut health_status = "ok";
    let mut status_code = StatusCode::OK;

    // Check database health
    let db_health = match server_state.app_state.db_manager.health_check().await {
        Ok(health) => {
            if !health.is_healthy() {
                health_status = "unhealthy";
                status_code = StatusCode::SERVICE_UNAVAILABLE;
            }
            json!({
                "status": if health.is_healthy() { "ok" } else { "error" },
                "main_pool_healthy": health.main_pool_healthy,
                "client_pool_healthy": health.client_pool_healthy,
                "checked_at": chrono::Utc::now().to_rfc3339()
            })
        }
        Err(e) => {
            health_status = "error";
            status_code = StatusCode::SERVICE_UNAVAILABLE;
            json!({
                "status": "error",
                "error": format!("Database health check failed: {}", e),
                "checked_at": chrono::Utc::now().to_rfc3339()
            })
        }
    };

    // Check connection manager health
    let connection_count = server_state.connection_manager.connection_count().await;
    let memory_stats = server_state.connection_manager.get_memory_stats().await;
    let connection_health = json!({
        "status": "ok",
        "active_connections": connection_count,
        "utilization_percent": memory_stats.utilization_percent,
        "max_connections": memory_stats.max_connections,
        "memory_leak_detected": memory_stats.connection_count != memory_stats.sender_count,
        "checked_at": chrono::Utc::now().to_rfc3339()
    });

    // Check system resources
    let system_health = json!({
        "status": "ok",
        "uptime_seconds": server_state.start_time.elapsed().as_secs(),
        "process_id": std::process::id(),
        "threads": std::thread::available_parallelism().map(|p| p.get()).unwrap_or(1),
        "checked_at": chrono::Utc::now().to_rfc3339()
    });

    let health_details = json!({
        "service": "actae",
        "version": env!("CARGO_PKG_VERSION"),
        "status": health_status,
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "instance_id": server_state.app_state.config.get_instance_id(),
        "mode": if server_state.app_state.config.managed_storage { "embedded" } else { "production" },
        "embedded_pg": server_state.app_state.config.managed_storage,
        "check_duration_ms": start_time.elapsed().as_millis(),
        "components": {
            "database": db_health,
            "connections": connection_health,
            "system": system_health
        }
    });

    (status_code, Json(CleanJson(health_details)))
}

async fn readiness_check_handler() -> (StatusCode, Json<CleanJson>) {
    let server_state = actae::api::global_state();
    let start_time = Instant::now();
    let mut is_ready = true;

    // Check if service is ready to accept traffic
    let connection_count = server_state.connection_manager.connection_count().await;
    let memory_stats = server_state.connection_manager.get_memory_stats().await;

    // Consider service not ready if at high capacity
    if memory_stats.utilization_percent > 95.0 {
        is_ready = false;
    }

    // Check database readiness (different from health)
    let db_ready = match server_state.app_state.db_manager.health_check().await {
        Ok(health) => health.is_healthy(),
        Err(_) => false,
    };

    if !db_ready {
        is_ready = false;
    }

    // Check if we have excessive resource usage
    let uptime = server_state.start_time.elapsed().as_secs();

    // Control plane enforcement: the instance must not take traffic once the
    // subscription lock is active or imminent (k8s won't route to it,
    // docker healthchecks alert).
    let control_plane = &server_state.app_state.control_plane;
    if control_plane.lock_imminent() {
        is_ready = false;
    }
    let ready_status = if is_ready { "ready" } else { "not_ready" };
    let subscription_json = control_plane.status_json();

    let readiness_details = json!({
        "status": ready_status,
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "instance_id": server_state.app_state.config.get_instance_id(),
        "check_duration_ms": start_time.elapsed().as_millis(),
        "readiness_checks": {
            "database_ready": db_ready,
            "capacity_available": memory_stats.utilization_percent <= 95.0,
            "uptime_seconds": uptime,
            "connection_utilization": memory_stats.utilization_percent,
            "active_connections": connection_count,
            "max_connections": memory_stats.max_connections,
            "subscription_enforcement": subscription_json
        }
    });

    let status_code = if is_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status_code, Json(CleanJson(readiness_details)))
}

/// Whether a bind host is loopback (safe without TLS).
fn is_loopback_host(host: &str) -> bool {
    host == "localhost" || host == "::1" || host.starts_with("127.")
}

/// Print top-level server usage (shown for `actae --help`).
fn print_server_usage() {
    println!(
        "Actae — durable execution history for AI agents\n\n\
         usage: actae [OPTIONS]\n\n\
         With no options, Actae runs self-contained with a managed PostgreSQL \
         (bundled; no external database required).\n\n\
         options:\n\
           --database-url <DSN>        Use an existing PostgreSQL instead of the bundled one\n\
           --client-database-url <DSN> Separate database for the client/auth pool\n\
           --config <FILE>             Load settings from a TOML config file\n\
           --host <ADDR>               Bind address (default 127.0.0.1)\n\
           --port <PORT>               HTTP port (default 8002)\n\
           --health-port <PORT>        Health/metrics port (defaults to --port)\n\
           -h, --help                  Print this help\n\
           -V, --version               Print the version\n\n\
         subcommands:\n\
           actae keys create|list|revoke\n\
           actae login [--url <url>] [--api-key <key>]\n\
           actae logout [--all]\n\
           actae passwd [email] [--password <password>]\n\
           actae upgrade [--check]\n\
           actae add | explain integration\n\n\
         environment: DATABASE_URL / POSTGRES_* select an external database; \
         ACTAE_STORAGE=managed|external forces the backend."
    );
}

/// Probe an endpoint's `/healthz` with a short timeout. Used to avoid
/// clobbering a live sibling instance's SDK credentials.
async fn endpoint_is_live(endpoint: &str) -> bool {
    let Ok(client) = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
    else {
        return false;
    };
    match client.get(format!("{endpoint}/healthz")).send().await {
        Ok(resp) => resp.status().is_success(),
        Err(_) => false,
    }
}

/// Whether the process looks like it runs inside a container. Only affects the
/// banner wording: an SDK on the host cannot read a file inside the container,
/// so the key must be copied out.
fn in_container() -> bool {
    std::path::Path::new("/.dockerenv").exists()
        || std::env::var("ACTAE_IN_CONTAINER")
            .map(|v| {
                let v = v.trim().to_ascii_lowercase();
                v == "1" || v == "true"
            })
            .unwrap_or(false)
}

/// Count API keys in the client database (used by first-boot provisioning).
async fn count_api_keys(pool: &deadpool_postgres::Pool) -> Result<i64> {
    let client = pool
        .get()
        .await
        .context("failed to get client database connection")?;
    let row = client
        .query_one("SELECT COUNT(*) FROM public.api_keys", &[])
        .await
        .context("failed to count API keys")?;
    Ok(row.get::<_, i64>(0))
}

/// Provision local API keys on first boot (`source = 'local'`):
///
/// - **`ACTAE_INITIAL_API_KEY` set**: that exact key — the deterministic
///   seed used by CI/compose so a well-known key exists on a fresh stack.
/// - **standalone first boot with no keys**: a generated `sk-…` key, printed
///   once in the banner so a fresh install is immediately usable.
///
/// Every path is idempotent (only runs when the `api_keys` table is empty)
/// and all keys are ordinary `source='local'` keys — no enforcement
/// exemption. Returns the raw key to print (when one was provisioned).
async fn provision_initial_keys(app_state: &AppState) -> Result<Option<String>> {
    let config = &app_state.config;
    let pool = &app_state.db_manager.client_pool;
    let empty = count_api_keys(pool).await? == 0;

    if let Some(seed) = config.initial_api_key.as_deref() {
        if empty {
            actae::auth::local_keys::insert_api_key(pool, config, seed, "admin", "Initial API Key")
                .await?;
            return Ok(Some(seed.to_string()));
        }
        return Ok(None);
    }

    // Standalone first boot with no keys: generate one and print it once.
    if config.control_plane_url.is_none() && empty {
        let raw = format!(
            "sk-{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        actae::auth::local_keys::insert_api_key(pool, config, &raw, "admin", "Initial API Key")
            .await?;
        return Ok(Some(raw));
    }

    Ok(None)
}

async fn count_users(pool: &deadpool_postgres::Pool) -> Result<i64> {
    let client = pool
        .get()
        .await
        .context("failed to get client database connection")?;
    let row = client
        .query_one("SELECT COUNT(*) FROM public.user", &[])
        .await
        .context("failed to count users")?;
    Ok(row.get::<_, i64>(0))
}

/// Create the first-boot dashboard admin on a standalone instance.
///
/// Standalone instances have no control plane to authenticate against, so
/// the dashboard needs at least one local password account. Runs only when
/// the `public.user` table is empty and the instance is not enrolled, so a
/// restart (or an enrolled instance) never re-provisions. Returns the
/// credentials to print once; the password is generated unless
/// `ACTAE_INITIAL_ADMIN_PASSWORD` is set.
/// What a standalone first boot provisioned, for the startup banner.
struct FirstBootAdmin {
    email: String,
    /// Set when `ACTAE_INITIAL_ADMIN_PASSWORD` pinned a password.
    password: Option<String>,
    /// Set when no password was preset — the operator redeems this one-time
    /// token on the dashboard's setup page to choose the password.
    setup_token: Option<String>,
}

async fn provision_initial_user(app_state: &AppState) -> Result<Option<FirstBootAdmin>> {
    let config = &app_state.config;
    if config.control_plane_url.is_some() {
        return Ok(None);
    }
    if count_users(&app_state.db_manager.client_pool).await? != 0 {
        return Ok(None);
    }
    let email = config
        .initial_admin_email
        .clone()
        .unwrap_or_else(|| "admin@actae.local".to_string());
    let user_id = format!("user_{}", uuid::Uuid::new_v4().simple());
    let now = chrono::Utc::now();

    let (password_hash, password, setup_token) = match config.initial_admin_password.clone() {
        Some(password) => {
            let hash = app_state
                .password_hasher
                .hash_password(&password, &user_id)
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "ACTAE_INITIAL_ADMIN_PASSWORD is not acceptable: {e}. Choose a \
                         stronger password (8–128 chars, mixed case + digit + symbol, no \
                         common words/sequences), or unset ACTAE_INITIAL_ADMIN_PASSWORD to \
                         get a one-time setup token on the dashboard instead."
                    )
                })?;
            // A preset password means setup is already done.
            let _ = actae::auth::setup::mark_complete(&app_state.db_manager.client_pool).await;
            (Some(hash), Some(password), None)
        }
        None => {
            // No preset password: create the admin without one and mint a
            // one-time setup token the operator redeems at the dashboard.
            let token = actae::secrets::generate();
            actae::auth::setup::begin(
                &app_state.db_manager.client_pool,
                &token,
                chrono::Duration::hours(24),
            )
            .await
            .context("failed to record the first-run setup token")?;
            (None, None, Some(token))
        }
    };

    let user = actae::database::models::User {
        id: user_id,
        email: email.clone(),
        email_verified: true,
        name: "Administrator".to_string(),
        image: None,
        password_hash,
        role: "admin".to_string(),
        created_at: now,
        updated_at: now,
    };
    app_state
        .user_repo
        .create(&user)
        .await
        .context("failed to create the initial dashboard user")?;
    Ok(Some(FirstBootAdmin {
        email,
        password,
        setup_token,
    }))
}

/// Require a valid credential — an API key (`X-API-Key` header) or a
/// dashboard session token (`Authorization: Bearer <jwt>`) — for sensitive
/// endpoints (metrics). Replaces the mTLS client certificate that previously
/// guarded these endpoints in the default tier.
async fn require_api_key(headers: &HeaderMap) -> Result<(), (StatusCode, Json<CleanJson>)> {
    let server_state = actae::api::try_global_state().ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(clean_json!({"error": "Server not initialized"})),
        )
    })?;
    match resolve_auth_context(
        headers,
        &server_state.app_state,
        &server_state.security_manager,
        false, // metrics stay readable under the subscription lock
    )
    .await?
    {
        // Anonymous (no credentials, ENABLE_ANONYMOUS=1) is not enough for
        // metrics — require an authenticated principal.
        AuthContext::Anonymous => Err((
            StatusCode::UNAUTHORIZED,
            Json(clean_json!({
                "error": "Unauthorized — a valid API key (X-API-Key) or session token (Bearer) is required",
            })),
        )),
        _ => Ok(()),
    }
}

async fn metrics_handler(headers: HeaderMap) -> (StatusCode, String) {
    if let Err((status, body)) = require_api_key(&headers).await {
        return (status, sonic_rs::to_string(&body.0).unwrap_or_default());
    }
    let server_state = actae::api::global_state();
    use actae::metrics::gather_metrics;

    // Update real-time metrics from server state
    let connection_count = server_state.connection_manager.connection_count().await;
    actae::metrics::CONNECTED_CLIENTS.set(connection_count as i64);

    // Update service status
    actae::metrics::SERVICE_STATUS
        .with_label_values(&["actae-core"])
        .set(1);

    // Gather all Prometheus metrics
    let metrics_text = gather_metrics();

    (StatusCode::OK, metrics_text)
}

// Legacy JSON metrics handler for backward compatibility
async fn metrics_json_handler(headers: HeaderMap) -> (StatusCode, Json<CleanJson>) {
    if let Err(e) = require_api_key(&headers).await {
        return e;
    }
    let server_state = actae::api::global_state();
    let connection_count = server_state.connection_manager.connection_count().await;
    let uptime_seconds = server_state.start_time.elapsed().as_secs();

    // Get WebSocket metrics from connection manager
    let topic_stats = server_state.connection_manager.get_topic_stats().await;
    let back_pressure_stats = server_state
        .connection_manager
        .get_back_pressure_stats()
        .await;

    // DB-backed counters for the dashboard overview (total events, channels,
    // state snapshots). Failures degrade to zeros — metrics must never block.
    let event_stats = server_state.event_handler.event_stats().await;
    let events_section = match event_stats {
        Ok(stats) => json!({
            "total": stats.total_events,
            "channels": stats.channel_count,
            "state_snapshots": stats.state_snapshots,
        }),
        Err(e) => json!({
            "total": 0,
            "channels": 0,
            "state_snapshots": 0,
            "error": e.to_string(),
        }),
    };

    // Calculate message processing metrics from Prometheus metrics
    let messages_sent = actae::metrics::MESSAGES_SENT.get();
    let messages_received = actae::metrics::MESSAGES_RECEIVED.get();
    let total_messages = messages_sent + messages_received;

    // Local fork/recovery counters (the differentiated primitives). Always
    // available on the instance with no phone-home; delivered to a control
    // plane as the `fork_operations` / `recovery_operations` usage meters only
    // when the instance is linked.
    let forks_total = actae::metrics::basic_metrics::FORKS_TOTAL.get();
    let recoveries_total = actae::metrics::basic_metrics::RECOVERIES_TOTAL.get();

    // Return comprehensive metrics
    (
        StatusCode::OK,
        Json(clean_json!({
            "status": "ok",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "uptime_seconds": uptime_seconds,
            "mode": if server_state.app_state.config.managed_storage { "embedded" } else { "production" },
            "embedded_pg": server_state.app_state.config.managed_storage,
            "subscription": server_state.app_state.control_plane.status_json(),
            "events": events_section,
            "forks": { "total": forks_total },
            "recoveries": { "total": recoveries_total },
            "websocket": {
                "connections": connection_count,
                "topics": topic_stats.len(),
                "topic_stats": topic_stats,
                "messages_sent": messages_sent,
                "messages_received": messages_received,
                "total_messages": total_messages,
                "back_pressure": {
                    "total_consumers": back_pressure_stats.total_consumers,
                    "slow_consumers": back_pressure_stats.slow_consumers,
                    "total_queued_messages": back_pressure_stats.total_queued_messages,
                    "total_dropped_messages": back_pressure_stats.total_dropped_messages,
                    "available_permits": back_pressure_stats.available_permits,
                    "max_queue_size": back_pressure_stats.max_queue_size,
                }
            }
        })),
    )
}

// BUILD_MARKER: 1784617117
