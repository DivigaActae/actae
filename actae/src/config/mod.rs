// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use std::env;

pub mod args;
pub mod file;
pub mod logging;

/// The built-in JWT signing secret used when `JWT_SECRET` is unset. It is a
/// well-known constant, so anyone who knows it can forge dashboard sessions;
/// self-hosted instances must set a real `JWT_SECRET` before being exposed.
pub const DEFAULT_JWT_SECRET: &str = "dev-jwt-secret-key-please-change-in-production-min-256-bits";

/// Main application configuration loaded from environment variables
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// True when the server manages its own bundled PostgreSQL child
    /// (the default when no external database is configured). Exposed via
    /// health/metrics as `"mode": "embedded"` / `embedded_pg` so deployments
    /// stay identifiable.
    pub managed_storage: bool,
    /// Explicit operating profile. `hosted` enables fail-closed checks that
    /// must never silently inherit self-hosted convenience behavior.
    pub deployment_mode: String,

    // Server
    pub host: String,
    pub port: u16,
    pub health_port: u16,
    pub max_connections: u32,
    pub worker_count: Option<u32>,

    // Database
    pub main_database_url: String,
    pub client_database_url: String,
    pub db_max_connections: u32,
    pub db_min_connections: u32,
    pub db_connect_timeout: u64,
    pub db_idle_timeout: u64,
    pub db_pool_timeout: u64,

    // Retention
    pub retention_days: u32,
    /// Permanent-erasure window for payloads already moved to
    /// `event_archive`. Zero preserves archives indefinitely.
    pub archive_retention_days: u32,
    /// Age-based purge window for versioned state snapshots. Old versions are
    /// removed unless they are the latest state, referenced by a live fork
    /// boundary, or required for provenance. Zero disables state purge.
    pub state_retention_days: u32,
    /// Window after which a completed/failed tool execution's stored bodies
    /// (params/result/error) are tombstoned: the bodies are nulled and the row
    /// keeps only identity + status + timestamps, so a retried idempotency key
    /// fails loudly with `result_expired` instead of silently executing again.
    /// Zero preserves bodies indefinitely.
    pub tool_execution_retention_days: u32,
    pub retention_check_interval_secs: u64,

    // WebSocket
    pub ws_max_connections: u32,
    pub ws_connection_timeout: u64,
    pub ws_ping_interval: u64,
    pub ws_max_message_size: u32,
    pub ws_buffer_size: u32,

    // Auth
    pub jwt_secret: String,
    /// Secret used to HMAC-hash API keys at rest. Independent from
    /// `jwt_secret` so a leak/rotation of one does not cascade into the
    /// other. Defaults to the JWT secret for backward compatibility with
    /// existing key hashes.
    pub api_key_hash_secret: String,
    /// Whether email/password auth (signup + login) is enabled. Off by
    /// default — the SaaS-issued API key is the primary credential.
    pub enable_signup: bool,
    pub jwt_expiration: u64,
    pub jwt_issuer: Option<String>,
    pub jwt_audience: Option<String>,
    pub api_key_header: String,
    pub enable_anonymous: bool,
    pub require_auth_for_private: bool,

    // Performance
    pub enable_compression: bool,
    pub compression_level: u32,
    pub message_batch_size: u32,
    pub message_batch_timeout_ms: u64,
    pub worker_queue_size: u32,
    pub broadcaster_queue_size: u32,
    pub enable_memory_optimization: bool,

    // Features
    pub enable_persistence: bool,
    pub enable_metrics: bool,

    // WebSocket Security
    pub websocket_allowed_origins: Option<Vec<String>>,
    /// Browser origins allowed for HTTP CORS. `None` preserves the existing
    /// self-hosted wildcard behavior; hosted mode requires an explicit list.
    pub http_allowed_origins: Option<Vec<String>>,
    pub websocket_max_connections_per_ip: Option<u32>,
    pub websocket_connection_timeout: Option<u64>,
    pub websocket_require_auth: bool,
    pub websocket_enable_rate_limiting: bool,
    pub websocket_max_message_rate: Option<u32>,

    /// CIDRs/IPs of trusted reverse proxies. When set, `X-Forwarded-For` is
    /// only honored from immediate peers in this list (rightmost-untrusted
    /// chain semantics). Defaults to empty = never trust forwarding headers
    /// (audit item 6).
    pub trusted_proxies: Vec<ipnet::IpNet>,

    // Enhanced Security Configuration
    pub anonymous_session_timeout_seconds: u64,
    pub enable_ip_allowlist: bool,
    pub enable_security_monitoring: bool,
    pub auth_failure_lockout_threshold: u32,
    pub auth_failure_lockout_duration_seconds: u64,
    pub enable_connection_security_validation: bool,

    // TLS Configuration
    pub enable_tls: bool,
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
    /// mTLS (client-certificate validation) — opt-in hardening tier,
    /// disabled by default. Requires `ENABLE_TLS=true` and a CA.
    pub enable_mtls: bool,
    /// CA bundle used to verify client certificates (mTLS only).
    pub ca_cert_path: Option<String>,

    // HTTPS server port (replaces HTTP server)
    pub https_port: Option<u16>,

    // Logging
    pub log_level: String,
    pub log_format: String,
    pub log_file_path: Option<String>,
    pub log_file_name: Option<String>,

    // Monitoring
    pub enable_prometheus: bool,
    pub enable_detailed_metrics: bool,
    pub metrics_path: String,
    pub health_check_path: String,
    pub readiness_check_path: String,

    // Phase 2: Performance-optimized security configuration
    pub enable_batch_processing: Option<bool>,
    pub enable_advanced_caching: Option<bool>,

    // Phase 3: Advanced hardening configuration
    pub enable_crypto_security: Option<bool>,
    pub enable_threat_detection: Option<bool>,

    // Control plane (SaaS enrollment + subscription enforcement)
    pub control_plane_url: Option<String>,
    pub install_id: Option<String>,
    pub enroll_secret: Option<String>,
    /// Paste-to-enroll one-shot onboarding: a portal-issued key exchanged
    /// for enrollment credentials at boot. Idempotent — skipped when an
    /// enrollment already exists (env credentials or the enrollment file).
    pub enroll_key: Option<String>,
    /// One-shot first-boot key: when set and the `api_keys` table is empty
    /// at boot, this exact key is provisioned (source='local'). The
    /// deterministic seed for CI/compose; standalone installs get a random
    /// generated key printed once in the banner when this is unset.
    pub initial_api_key: Option<String>,
    /// First-boot dashboard admin (standalone only). When the `public.user`
    /// table is empty at boot, a local password account is created so the
    /// dashboard is reachable without an API key. Defaults to a generated
    /// password printed once in the banner; `ACTAE_INITIAL_ADMIN_EMAIL` /
    /// `ACTAE_INITIAL_ADMIN_PASSWORD` make it deterministic for CI.
    pub initial_admin_email: Option<String>,
    pub initial_admin_password: Option<String>,
    pub sync_interval_secs: u64,
    pub subscription_grace_secs: i64,
    pub ca_bundle_path: Option<String>,

    // Federation (Phase 4): purpose-bound fleet/connector credentials and the
    // source policy applied to federated reads. Local operator config; the
    // control-plane sync can replace/extend verification keys and policy (a
    // merged effective config is computed at authentication time).
    pub federation: crate::auth::federation::FederationConfig,
    pub federation_source_policy: crate::auth::source_policy::FederationSourcePolicy,
}

impl AppConfig {
    /// Load just the logging configuration from the environment, without
    /// requiring any database settings. Used to initialize tracing before the
    /// managed-PostgreSQL block starts (it must run before the full
    /// `AppConfig::load()` because that block provides the `POSTGRES_*`
    /// variables the server connects with).
    pub fn load_logging_only() -> Result<Self> {
        Ok(Self {
            log_level: env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string()),
            log_format: env::var("LOG_FORMAT").unwrap_or_else(|_| "json".to_string()),
            log_file_path: env::var("LOG_FILE_PATH").ok(),
            log_file_name: env::var("LOG_FILE_NAME")
                .ok()
                .or_else(|| Some("actae.log".to_string())),
            ..Self::default()
        })
    }

    /// Load configuration from environment variables with proper validation
    pub fn load() -> Result<Self> {
        // Server configuration with validation
        // Security posture: bind the loopback interface by default when no
        // TLS is configured, so a fresh binary install is not accidentally
        // exposed on the network. An explicit ACTAE_HOST always wins; TLS
        // deployments (e.g. the docker stack) default to 0.0.0.0.
        let enable_tls = env::var("ENABLE_TLS")
            .unwrap_or_else(|_| "false".to_string())
            .parse()
            .unwrap_or(false);
        let host = env::var("ACTAE_HOST").unwrap_or_else(|_| {
            if enable_tls {
                "0.0.0.0".to_string()
            } else {
                "127.0.0.1".to_string()
            }
        });
        if host.is_empty() {
            return Err(anyhow::anyhow!("ACTAE_HOST cannot be empty"));
        }

        // Managed storage (bundled PostgreSQL) is the default when no external
        // database is configured and `ACTAE_STORAGE` does not force external.
        // The startup path resolves this before `load()` and writes it back as
        // `ACTAE_STORAGE=managed|external`; this fallback keeps standalone
        // callers (e.g. `actae keys`) consistent.
        let managed_storage = storage_is_managed();

        // Main port: ACTAE_PORT (with HTTPS_PORT fallback for backward compatibility).
        // Parsed before the origin derivation below: managed mode serves the
        // dashboard from this same origin, so the loopback allowlist must know
        // the effective port (ACTAE_PORT is already the post-auto-bump port —
        // the startup writes it back before config builds).
        let port = env::var("ACTAE_PORT")
            .or_else(|_| env::var("HTTPS_PORT"))
            .unwrap_or_else(|_| "8002".to_string())
            .parse::<u16>()
            .context("ACTAE_PORT must be a valid port number (1-65535)")?;

        // An operator-supplied list wins; otherwise managed loopback mode
        // auto-allows the Next.js dev server (:3000) AND the Actae server's
        // own origin (`http://localhost:<port>` / `http://127.0.0.1:<port>`)
        // so the dashboard served from the managed instance can upgrade its
        // WebSocket. With no list at all the policy falls back to same-origin
        // (Origin == Host), which already covers the docker/production
        // topology.
        let websocket_allowed_origins = env::var("WEBSOCKET_ALLOWED_ORIGINS")
            .ok()
            .map(|origins| origins.split(',').map(|s| s.trim().to_string()).collect())
            .or_else(|| {
                managed_loopback_dashboard_origins(managed_storage, enable_tls, &host, port)
            });

        if port < 1024 && port != 0 {
            return Err(anyhow::anyhow!(
                "ACTAE_PORT should be >= 1024 for non-root users"
            ));
        }

        // Health port: ACTAE_HEALTH_PORT (defaults to main port for single-port mode)
        let health_port = env::var("ACTAE_HEALTH_PORT")
            .unwrap_or_else(|_| port.to_string())
            .parse::<u16>()
            .context("ACTAE_HEALTH_PORT must be a valid port number (1-65535)")?;

        // JWT secret is validated once and reused as the default for the
        // API-key hash secret (backward compatibility: existing key hashes
        // were minted with the JWT secret).
        let jwt_secret = Self::validate_jwt_secret()?;
        let api_key_hash_secret = Self::resolve_api_key_hash_secret(&jwt_secret)?;

        let connection_pool_fallback = |key: &str, default: &str| {
            env::var(key).unwrap_or_else(|_| {
                env::var("CONNECTION_POOL_SIZE").unwrap_or_else(|_| default.to_string())
            })
        };

        let max_connections = connection_pool_fallback("SERVER_MAX_CONNECTIONS", "1000")
            .parse::<u32>()
            .context("SERVER_MAX_CONNECTIONS must be a valid positive number")?;

        if max_connections == 0 {
            return Err(anyhow::anyhow!(
                "CONNECTION_POOL_SIZE must be greater than 0"
            ));
        }

        let worker_count = env::var("WORKER_POOL_COUNT")
            .ok()
            .map(|w| {
                w.parse::<u32>()
                    .context("WORKER_POOL_COUNT must be a valid positive number")
            })
            .transpose()?;

        let config = Self {
            managed_storage,
            deployment_mode: env::var("ACTAE_DEPLOYMENT_MODE")
                .unwrap_or_else(|_| "self_hosted".to_string()),

            // Server configuration
            host,
            port,
            health_port,
            max_connections,
            worker_count,

            // Database configuration
            main_database_url: Self::build_main_db_url()?,
            client_database_url: Self::build_client_db_url()?,
            db_max_connections: connection_pool_fallback("DB_MAX_CONNECTIONS", "10")
                .parse()
                .context("DB_MAX_CONNECTIONS must be a valid positive number")?,
            db_min_connections: env::var("DB_MIN_CONNECTIONS")
                .unwrap_or_else(|_| "2".to_string())
                .parse()
                .context("DB_MIN_CONNECTIONS must be a valid positive number")?,
            db_connect_timeout: env::var("DB_CONNECT_TIMEOUT")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .context("DB_CONNECT_TIMEOUT must be a valid positive number")?,
            db_idle_timeout: env::var("DB_IDLE_TIMEOUT")
                .unwrap_or_else(|_| "300".to_string())
                .parse()
                .context("DB_IDLE_TIMEOUT must be a valid positive number")?,
            db_pool_timeout: env::var("DB_POOL_TIMEOUT")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .context("DB_POOL_TIMEOUT must be a valid positive number")?,

            // Retention configuration
            retention_days: env::var("EVENT_RETENTION_DAYS")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
            archive_retention_days: env::var("EVENT_ARCHIVE_RETENTION_DAYS")
                .unwrap_or_else(|_| "0".to_string())
                .parse()
                .context("EVENT_ARCHIVE_RETENTION_DAYS must be a non-negative integer")?,
            state_retention_days: env::var("STATE_RETENTION_DAYS")
                .unwrap_or_else(|_| "0".to_string())
                .parse()
                .context("STATE_RETENTION_DAYS must be a non-negative integer")?,
            tool_execution_retention_days: env::var("TOOL_EXECUTION_RETENTION_DAYS")
                .unwrap_or_else(|_| "0".to_string())
                .parse()
                .context("TOOL_EXECUTION_RETENTION_DAYS must be a non-negative integer")?,
            retention_check_interval_secs: env::var("RETENTION_CHECK_INTERVAL_SECS")
                .unwrap_or_else(|_| "3600".to_string())
                .parse()
                .unwrap_or(3600),

            // WebSocket configuration
            ws_max_connections: connection_pool_fallback("WS_MAX_CONNECTIONS", "1000")
                .parse()
                .unwrap_or(1000),
            ws_connection_timeout: env::var("WS_CONNECTION_TIMEOUT")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
            ws_ping_interval: env::var("WS_PING_INTERVAL")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),
            ws_max_message_size: env::var("WS_MAX_MESSAGE_SIZE")
                .unwrap_or_else(|_| "1048576".to_string()) // 1MB
                .parse()
                .unwrap_or(1048576),
            ws_buffer_size: env::var("MESSAGE_BUFFER_SIZE")
                .unwrap_or_else(|_| "10000".to_string())
                .parse()
                .unwrap_or(10000),

            // Auth configuration with secure JWT secret validation
            jwt_secret: jwt_secret.clone(),
            api_key_hash_secret,
            enable_signup: env::var("ENABLE_SIGNUP")
                .unwrap_or_else(|_| "false".to_string()) // SECURE DEFAULT: disabled
                .parse()
                .context("ENABLE_SIGNUP must be true or false")?,
            jwt_expiration: env::var("JWT_EXPIRATION")
                .unwrap_or_else(|_| "86400".to_string()) // 24 hours
                .parse()
                .context("JWT_EXPIRATION must be a valid positive number")?,
            jwt_issuer: env::var("JWT_ISSUER").ok(),
            jwt_audience: env::var("JWT_AUDIENCE").ok(),
            api_key_header: env::var("API_KEY_HEADER").unwrap_or_else(|_| "X-API-Key".to_string()),
            enable_anonymous: env::var("ENABLE_ANONYMOUS")
                .unwrap_or_else(|_| "false".to_string()) // SECURE DEFAULT: disabled
                .parse()
                .context("ENABLE_ANONYMOUS must be true or false")?,
            require_auth_for_private: env::var("REQUIRE_AUTH_FOR_PRIVATE")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .context("REQUIRE_AUTH_FOR_PRIVATE must be true or false")?,

            // Performance configuration
            enable_compression: env::var("ENABLE_COMPRESSION")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            compression_level: env::var("BINARY_COMPRESSION_LEVEL")
                .unwrap_or_else(|_| "6".to_string())
                .parse()
                .unwrap_or(6),
            message_batch_size: env::var("BATCH_SIZE")
                .unwrap_or_else(|_| "100".to_string())
                .parse()
                .unwrap_or(100),
            message_batch_timeout_ms: env::var("BATCH_TIMEOUT_MS")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10),
            worker_queue_size: env::var("WORKER_QUEUE_SIZE")
                .unwrap_or_else(|_| "10000".to_string())
                .parse()
                .unwrap_or(10000),
            broadcaster_queue_size: env::var("BROADCASTER_QUEUE_SIZE")
                .unwrap_or_else(|_| {
                    env::var("WORKER_QUEUE_SIZE").unwrap_or_else(|_| "10000".to_string())
                })
                .parse()
                .unwrap_or(10000),
            enable_memory_optimization: env::var("MEMORY_OPTIMIZER_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),

            // Feature flags
            enable_persistence: env::var("MESSAGE_PERSISTENCE_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            enable_metrics: env::var("PROMETHEUS_EXPORTER_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),

            // Logging configuration
            log_level: env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string()),
            log_format: env::var("LOG_FORMAT").unwrap_or_else(|_| "json".to_string()),
            log_file_path: env::var("LOG_FILE_PATH").ok(),
            log_file_name: env::var("LOG_FILE_NAME")
                .ok()
                .or_else(|| Some("actae.log".to_string())),

            // Monitoring
            enable_prometheus: env::var("PROMETHEUS_EXPORTER_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            enable_detailed_metrics: env::var("ENABLE_DETAILED_METRICS")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            metrics_path: env::var("METRICS_PATH").unwrap_or_else(|_| "/metrics".to_string()),
            health_check_path: env::var("HEALTH_CHECK_PATH")
                .unwrap_or_else(|_| "/healthz".to_string()),
            readiness_check_path: env::var("READINESS_CHECK_PATH")
                .unwrap_or_else(|_| "/readyz".to_string()),

            // Phase 2: Performance-optimized security
            enable_batch_processing: env::var("ENABLE_BATCH_PROCESSING")
                .ok()
                .and_then(|s| s.parse().ok()),
            enable_advanced_caching: env::var("ENABLE_ADVANCED_CACHING")
                .ok()
                .and_then(|s| s.parse().ok()),

            // Phase 3: Advanced hardening
            enable_crypto_security: env::var("ENABLE_CRYPTO_SECURITY")
                .ok()
                .and_then(|s| s.parse().ok()),
            enable_threat_detection: env::var("ENABLE_THREAT_DETECTION")
                .ok()
                .and_then(|s| s.parse().ok()),

            // WebSocket Security
            // The local Next.js dashboard runs on :3000 while an embedded
            // Actae instance listens on :8002. Permit only those loopback
            // dashboard origins when no explicit policy was supplied; every
            // network-reachable and production topology retains the strict
            // same-origin default below.
            websocket_allowed_origins,
            http_allowed_origins: env::var("HTTP_ALLOWED_ORIGINS").ok().map(|origins| {
                origins
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            }),
            websocket_max_connections_per_ip: env::var("WEBSOCKET_MAX_CONNECTIONS_PER_IP")
                .ok()
                .and_then(|s| s.parse().ok()),
            websocket_connection_timeout: env::var("WEBSOCKET_CONNECTION_TIMEOUT")
                .ok()
                .and_then(|s| s.parse().ok()),
            websocket_require_auth: env::var("WEBSOCKET_REQUIRE_AUTH")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            websocket_enable_rate_limiting: env::var("WEBSOCKET_ENABLE_RATE_LIMITING")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            websocket_max_message_rate: env::var("WEBSOCKET_MAX_MESSAGE_RATE")
                .ok()
                .and_then(|s| s.parse().ok()),
            trusted_proxies: env::var("ACTAE_TRUSTED_PROXIES")
                .unwrap_or_default()
                .split(',')
                .filter_map(|s| s.trim().parse::<ipnet::IpNet>().ok())
                .collect(),

            // Enhanced Security Configuration
            anonymous_session_timeout_seconds: env::var("ANONYMOUS_SESSION_TIMEOUT_SECONDS")
                .unwrap_or_else(|_| "900".to_string()) // 15 minutes default
                .parse()
                .context("ANONYMOUS_SESSION_TIMEOUT_SECONDS must be a valid number")?,
            enable_ip_allowlist: env::var("ENABLE_IP_ALLOWLIST")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .context("ENABLE_IP_ALLOWLIST must be true or false")?,
            enable_security_monitoring: env::var("ENABLE_SECURITY_MONITORING")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .context("ENABLE_SECURITY_MONITORING must be true or false")?,
            auth_failure_lockout_threshold: env::var("AUTH_FAILURE_LOCKOUT_THRESHOLD")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .context("AUTH_FAILURE_LOCKOUT_THRESHOLD must be a valid number")?,
            auth_failure_lockout_duration_seconds: env::var(
                "AUTH_FAILURE_LOCKOUT_DURATION_SECONDS",
            )
            .unwrap_or_else(|_| "300".to_string()) // 5 minutes default
            .parse()
            .context("AUTH_FAILURE_LOCKOUT_DURATION_SECONDS must be a valid number")?,
            enable_connection_security_validation: env::var(
                "ENABLE_CONNECTION_SECURITY_VALIDATION",
            )
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .context("ENABLE_CONNECTION_SECURITY_VALIDATION must be true or false")?,

            // TLS Configuration
            enable_tls,
            tls_cert_path: env::var("TLS_CERT_PATH").ok(),
            tls_key_path: env::var("TLS_KEY_PATH").ok(),
            // mTLS is an opt-in hardening tier: default OFF.
            enable_mtls: env::var("ENABLE_MTLS")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
            ca_cert_path: env::var("CA_CERT_PATH")
                .ok()
                .filter(|s| !s.trim().is_empty()),

            // HTTPS server port configuration
            https_port: env::var("HTTPS_PORT").ok().and_then(|s| s.parse().ok()),

            // Control plane configuration
            control_plane_url: env::var("ACTAE_CONTROL_PLANE_URL")
                .ok()
                .filter(|s| !s.trim().is_empty()),
            install_id: env::var("ACTAE_INSTALL_ID")
                .ok()
                .filter(|s| !s.trim().is_empty()),
            enroll_secret: Self::secret_from_env_or_file("ACTAE_ENROLL_SECRET")?,
            enroll_key: Self::secret_from_env_or_file("ACTAE_ENROLL_KEY")?,
            sync_interval_secs: env::var("ACTAE_SYNC_INTERVAL_SECS")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .context("ACTAE_SYNC_INTERVAL_SECS must be a valid positive number")?,
            subscription_grace_secs: env::var("ACTAE_SUBSCRIPTION_GRACE_SECS")
                .unwrap_or_else(|_| "259200".to_string()) // 72 hours default
                .parse()
                .context("ACTAE_SUBSCRIPTION_GRACE_SECS must be a valid number")?,
            ca_bundle_path: env::var("ACTAE_CA_BUNDLE")
                .ok()
                .filter(|s| !s.trim().is_empty()),
            initial_api_key: env::var("ACTAE_INITIAL_API_KEY")
                .ok()
                .filter(|s| !s.trim().is_empty()),
            initial_admin_email: env::var("ACTAE_INITIAL_ADMIN_EMAIL")
                .ok()
                .filter(|s| !s.trim().is_empty()),
            initial_admin_password: env::var("ACTAE_INITIAL_ADMIN_PASSWORD")
                .ok()
                .filter(|s| !s.trim().is_empty()),
            federation: crate::auth::federation::FederationConfig::from_env()
                .context("invalid ACTAE_FEDERATION_* configuration")?,
            federation_source_policy: crate::auth::source_policy::FederationSourcePolicy::from_env(
            ),
        };
        config.validate()?;
        Ok(config)
    }

    /// Percent-encode a URL userinfo component (user/password) so embedded
    /// or operator-supplied credentials containing `@`, `/`, `%`, spaces or
    /// quotes can never corrupt the connection URL.
    fn pg_url_escape(s: &str) -> String {
        percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
    }

    /// Build main database URL.
    ///
    /// An explicit `DATABASE_URL` (or the `--database-url` CLI flag, which the
    /// startup path writes into it) wins verbatim. Otherwise the URL is
    /// assembled from the discrete `POSTGRES_*` variables.
    fn build_main_db_url() -> Result<String> {
        if let Some(url) = nonempty_env("DATABASE_URL") {
            return Ok(url);
        }
        let user = env::var("POSTGRES_USER")
            .map_err(|_| anyhow::anyhow!("POSTGRES_USER environment variable is required"))?;
        let password = env::var("POSTGRES_PASSWORD")
            .map_err(|_| anyhow::anyhow!("POSTGRES_PASSWORD environment variable is required"))?;
        let host = env::var("POSTGRES_HOST")
            .map_err(|_| anyhow::anyhow!("POSTGRES_HOST environment variable is required"))?;
        let port = env::var("POSTGRES_PORT").unwrap_or_else(|_| "5432".to_string());
        let db = env::var("POSTGRES_DB")
            .map_err(|_| anyhow::anyhow!("POSTGRES_DB environment variable is required"))?;
        let ssl_mode = env::var("POSTGRES_SSL_MODE").unwrap_or_else(|_| "require".to_string());
        let ssl_root = env::var("POSTGRES_SSL_ROOT_CERT").ok();

        let mut url = format!(
            "postgresql://{}:{}@{}:{}/{}?sslmode={}",
            Self::pg_url_escape(&user),
            Self::pg_url_escape(&password),
            host,
            port,
            db,
            ssl_mode
        );
        if let Some(path) = ssl_root {
            url.push_str("&sslrootcert=");
            url.push_str(
                &percent_encoding::utf8_percent_encode(&path, percent_encoding::NON_ALPHANUMERIC)
                    .to_string(),
            );
        }
        Ok(url)
    }

    /// Build client database URL.
    ///
    /// `CLIENT_DATABASE_URL` wins verbatim. Otherwise the discrete
    /// `CLIENT_POSTGRES_*` variables are used; when those are absent but a
    /// main `DATABASE_URL` is present, the same database is reused for the
    /// client pool (one external instance, two pools).
    fn build_client_db_url() -> Result<String> {
        if let Some(url) = nonempty_env("CLIENT_DATABASE_URL") {
            return Ok(url);
        }
        if env::var("CLIENT_POSTGRES_HOST")
            .map(|v| v.trim().is_empty())
            .unwrap_or(true)
        {
            if let Some(url) = nonempty_env("DATABASE_URL") {
                return Ok(url);
            }
        }
        let user = env::var("CLIENT_POSTGRES_USER").map_err(|_| {
            anyhow::anyhow!("CLIENT_POSTGRES_USER environment variable is required")
        })?;
        let password = env::var("CLIENT_POSTGRES_PASSWORD").map_err(|_| {
            anyhow::anyhow!("CLIENT_POSTGRES_PASSWORD environment variable is required")
        })?;
        let host = env::var("CLIENT_POSTGRES_HOST").map_err(|_| {
            anyhow::anyhow!("CLIENT_POSTGRES_HOST environment variable is required")
        })?;
        let port = env::var("CLIENT_POSTGRES_PORT").unwrap_or_else(|_| "5432".to_string());
        let db = env::var("CLIENT_POSTGRES_DB")
            .map_err(|_| anyhow::anyhow!("CLIENT_POSTGRES_DB environment variable is required"))?;
        let ssl_mode = env::var("CLIENT_POSTGRES_SSL_MODE")
            .or_else(|_| env::var("CLIENT_POSTGRES_SSL"))
            .unwrap_or_else(|_| "require".to_string());
        let ssl_root = env::var("CLIENT_POSTGRES_SSL_ROOT_CERT").ok();

        let mut url = format!(
            "postgresql://{}:{}@{}:{}/{}?sslmode={}",
            Self::pg_url_escape(&user),
            Self::pg_url_escape(&password),
            host,
            port,
            db,
            ssl_mode
        );
        if let Some(path) = ssl_root {
            url.push_str("&sslrootcert=");
            url.push_str(
                &percent_encoding::utf8_percent_encode(&path, percent_encoding::NON_ALPHANUMERIC)
                    .to_string(),
            );
        }
        Ok(url)
    }

    /// Get database URLs (for backward compatibility)
    pub fn get_database_urls(&self) -> (String, String) {
        (
            self.main_database_url.clone(),
            self.client_database_url.clone(),
        )
    }

    /// Get primary database URL (for backward compatibility)
    pub fn get_database_url(&self) -> String {
        self.main_database_url.clone()
    }

    /// Get JWT secret (for backward compatibility)
    pub fn get_jwt_secret(&self) -> String {
        self.jwt_secret.clone()
    }

    /// True when the instance is still using the built-in default JWT secret
    /// (a known constant). `doctor` and the startup banner flag this so an
    /// operator sets a real `JWT_SECRET` before exposing the instance.
    pub fn uses_default_jwt_secret(&self) -> bool {
        self.jwt_secret == DEFAULT_JWT_SECRET
    }

    /// Validate JWT secret meets security requirements
    fn validate_jwt_secret() -> Result<String> {
        let secret = Self::secret_from_env_or_file("JWT_SECRET")?
            .unwrap_or_else(|| DEFAULT_JWT_SECRET.to_string());

        // Validate JWT secret length (minimum 32 characters for security)
        if secret.len() < 32 {
            return Err(anyhow::anyhow!(
                "JWT_SECRET must be at least 32 characters long for security. Current length: {}",
                secret.len()
            ));
        }

        // A generated-and-persisted secret is resolved later (see
        // `crate::secrets`); the startup banner / `actae doctor` report the
        // final state, so no warning is emitted here.

        // Validate entropy (basic check for repeated characters)
        let unique_chars: std::collections::HashSet<char> = secret.chars().collect();
        if unique_chars.len() < 8 {
            return Err(anyhow::anyhow!(
                "JWT_SECRET appears to have low entropy (too few unique characters). Use a cryptographically secure random string."
            ));
        }

        Ok(secret)
    }

    /// Resolve the API-key hash secret. Explicitly set via
    /// `API_KEY_HASH_SECRET` (validated like the JWT secret) or inherited
    /// from the JWT secret for backward compatibility with existing key
    /// hashes. Changing it invalidates previously minted key hashes — that
    /// is why the default keeps it identical to `JWT_SECRET`.
    fn resolve_api_key_hash_secret(jwt_secret: &str) -> Result<String> {
        let secret = match Self::secret_from_env_or_file("API_KEY_HASH_SECRET")? {
            Some(v) => v,
            None => return Ok(jwt_secret.to_string()),
        };
        let trimmed = secret.trim();
        if trimmed.is_empty() {
            return Err(anyhow::anyhow!("API_KEY_HASH_SECRET must not be empty"));
        }
        if trimmed.len() < 32 {
            return Err(anyhow::anyhow!(
                "API_KEY_HASH_SECRET must be at least 32 characters long for security. Current length: {}",
                trimmed.len()
            ));
        }
        let unique_chars: std::collections::HashSet<char> = trimmed.chars().collect();
        if unique_chars.len() < 8 {
            return Err(anyhow::anyhow!(
                "API_KEY_HASH_SECRET appears to have low entropy (too few unique characters). Use a cryptographically secure random string."
            ));
        }
        Ok(trimmed.to_string())
    }

    /// Resolve sensitive configuration without requiring secret material in
    /// process/container metadata. `NAME_FILE` and `NAME` are mutually
    /// exclusive so a stale environment value can never silently override a
    /// mounted secret. Files are bounded and must contain a non-empty value.
    fn secret_from_env_or_file(name: &str) -> Result<Option<String>> {
        let file_name = format!("{name}_FILE");
        let direct = env::var(name).ok();
        let path = env::var(&file_name).ok();
        if direct.is_some() && path.is_some() {
            return Err(anyhow::anyhow!(
                "{name} and {file_name} are mutually exclusive"
            ));
        }
        let value = match (direct, path) {
            (Some(value), None) => value,
            (None, Some(path)) => {
                let metadata = std::fs::metadata(&path)
                    .with_context(|| format!("failed to inspect {file_name} at {path}"))?;
                if !metadata.is_file() || metadata.len() > 1024 * 1024 {
                    return Err(anyhow::anyhow!(
                        "{file_name} must point to a regular file no larger than 1 MiB"
                    ));
                }
                std::fs::read_to_string(&path)
                    .with_context(|| format!("failed to read {file_name} at {path}"))?
            }
            (None, None) => return Ok(None),
            (Some(_), Some(_)) => unreachable!(),
        };
        let trimmed = value.trim();
        if trimmed.is_empty() || trimmed.contains('\0') {
            return Err(anyhow::anyhow!("{name} must not be empty or contain NUL"));
        }
        Ok(Some(trimmed.to_string()))
    }

    /// Validate configuration settings
    pub fn validate(&self) -> Result<()> {
        // Validate database URLs
        if self.main_database_url.is_empty() {
            return Err(anyhow::anyhow!("Main database URL cannot be empty"));
        }
        if self.client_database_url.is_empty() {
            return Err(anyhow::anyhow!("Client database URL cannot be empty"));
        }

        // Validate JWT secret
        if self.jwt_secret.is_empty() {
            return Err(anyhow::anyhow!("JWT secret cannot be empty"));
        }
        if self.jwt_secret.len() < 32 {
            return Err(anyhow::anyhow!("JWT secret must be at least 32 characters"));
        }
        if self.api_key_hash_secret.is_empty() {
            return Err(anyhow::anyhow!("API key hash secret cannot be empty"));
        }
        if self.api_key_hash_secret.len() < 32 {
            return Err(anyhow::anyhow!(
                "API key hash secret must be at least 32 characters"
            ));
        }

        if self.deployment_mode != "self_hosted" && self.deployment_mode != "hosted" {
            return Err(anyhow::anyhow!(
                "ACTAE_DEPLOYMENT_MODE must be self_hosted or hosted"
            ));
        }
        if self.deployment_mode == "hosted" {
            // Hosted tenants run the bundled managed PostgreSQL per workload
            // (docs/CLOUD_HOSTED_TECHNICAL_PLAN.md); per-tenant external
            // storage is a GA target and lands with its provisioning, so do
            // not reject managed storage here — the provisioner passes no DB.
            if self.enable_signup || self.enable_anonymous {
                return Err(anyhow::anyhow!(
                    "hosted mode forbids ENABLE_SIGNUP and ENABLE_ANONYMOUS"
                ));
            }
            if self.control_plane_url.is_none()
                || self.install_id.is_none()
                || self.enroll_secret.is_none()
            {
                return Err(anyhow::anyhow!(
                    "hosted mode requires ACTAE_CONTROL_PLANE_URL, ACTAE_INSTALL_ID and ACTAE_ENROLL_SECRET"
                ));
            }
            if self.jwt_secret == DEFAULT_JWT_SECRET || self.jwt_secret == self.api_key_hash_secret
            {
                return Err(anyhow::anyhow!(
                    "hosted mode requires independent production JWT_SECRET and API_KEY_HASH_SECRET values"
                ));
            }
            if self.initial_api_key.is_some() {
                return Err(anyhow::anyhow!(
                    "hosted mode forbids ACTAE_INITIAL_API_KEY; keys must arrive from the control plane"
                ));
            }
            if self
                .http_allowed_origins
                .as_ref()
                .map_or(true, Vec::is_empty)
            {
                return Err(anyhow::anyhow!(
                    "hosted mode requires an explicit HTTP_ALLOWED_ORIGINS list"
                ));
            }
            if self
                .websocket_allowed_origins
                .as_ref()
                .map_or(true, Vec::is_empty)
            {
                return Err(anyhow::anyhow!(
                    "hosted mode requires an explicit WEBSOCKET_ALLOWED_ORIGINS list"
                ));
            }
        }

        // mTLS is meaningless without TLS, and requires a CA to validate
        // client certificates against. Fail fast instead of silently
        // downgrading to plain TLS.
        if self.enable_mtls && !self.enable_tls {
            return Err(anyhow::anyhow!(
                "ENABLE_MTLS requires ENABLE_TLS=true (mTLS cannot be used over plain HTTP)"
            ));
        }
        if self.enable_mtls && self.ca_cert_path.is_none() {
            return Err(anyhow::anyhow!(
                "ENABLE_MTLS requires CA_CERT_PATH (the CA that signs client certificates)"
            ));
        }

        // Validate server configuration
        if self.port == 0 {
            return Err(anyhow::anyhow!("Server port cannot be 0"));
        }

        // Validate performance settings
        if self.message_batch_size == 0 {
            return Err(anyhow::anyhow!("Message batch size cannot be 0"));
        }
        if self.worker_queue_size == 0 {
            return Err(anyhow::anyhow!("Worker queue size cannot be 0"));
        }

        // Validate WebSocket settings
        if self.ws_max_connections == 0 {
            return Err(anyhow::anyhow!("Max WebSocket connections cannot be 0"));
        }
        if self.ws_max_message_size == 0 {
            return Err(anyhow::anyhow!("Max WebSocket message size cannot be 0"));
        }

        // Validate control plane configuration: enrollment credentials are
        // all-or-nothing. `ACTAE_CONTROL_PLANE_URL` alone is allowed (the
        // enrollment file supplies the credentials), but a partial set of
        // credentials is ambiguous — fail fast instead.
        let install_set = self.install_id.is_some();
        let secret_set = self.enroll_secret.is_some();
        if install_set != secret_set {
            return Err(anyhow::anyhow!(
                "Control plane misconfiguration: ACTAE_INSTALL_ID and ACTAE_ENROLL_SECRET must be set together (or not at all)"
            ));
        }
        if self.sync_interval_secs == 0 {
            return Err(anyhow::anyhow!(
                "ACTAE_SYNC_INTERVAL_SECS must be greater than 0"
            ));
        }
        if self.subscription_grace_secs < 0 {
            return Err(anyhow::anyhow!(
                "ACTAE_SUBSCRIPTION_GRACE_SECS must be >= 0"
            ));
        }

        Ok(())
    }

    /// Get unique instance ID (using process ID + hostname)
    pub fn get_instance_id(&self) -> String {
        use std::process;
        let pid = process::id();
        let hostname = env::var("HOSTNAME")
            .or_else(|_| env::var("COMPUTERNAME"))
            .unwrap_or_else(|_| "unknown".to_string());
        format!("{hostname}_{pid}")
    }

    /// Convert to JSON string for debugging
    pub fn to_json(&self) -> Result<String> {
        Ok(format!(
            "AppConfig {{ host: {}, port: {}, database: {} }}",
            self.host,
            self.port,
            self.redact_db_url(&self.main_database_url)
        ))
    }

    /// Redact password from a database URL for safe logging
    fn redact_db_url(&self, url: &str) -> String {
        let re =
            regex::Regex::new(r"(://[^:]+):([^@]+)@").expect("hardcoded DB URL regex must compile");
        re.replace(url, "$1:***@").to_string()
    }
}

/// Whether an Actae bind host is reachable only from the local machine.
/// Kept here (rather than in `main`) because configuration determines whether
/// the managed-development dashboard convenience policy is safe to enable.
fn is_loopback_host(host: &str) -> bool {
    matches!(host.trim(), "localhost" | "127.0.0.1" | "::1" | "[::1]")
}

/// Read an environment variable, treating empty/whitespace as unset.
fn nonempty_env(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// True when the bundled (managed) PostgreSQL is in effect.
///
/// `ACTAE_STORAGE` is the explicit selector written by the startup path
/// (`managed`/`embedded` vs `external`). When unset, storage is managed unless
/// an external database is configured via env — so a bare `actae` is
/// self-contained, while Docker (`POSTGRES_*`) and `--database-url` stay
/// external.
pub fn storage_is_managed() -> bool {
    match env::var("ACTAE_STORAGE")
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("managed") | Some("embedded") => true,
        Some("external") => false,
        _ => !external_database_configured(),
    }
}

/// A credential-free description of the external database selected by the
/// environment, for startup notices. Never includes a password.
pub fn redacted_database_target() -> String {
    if let Some(url) = nonempty_env("DATABASE_URL") {
        let re =
            regex::Regex::new(r"(://[^:]+):([^@]+)@").expect("hardcoded DB URL regex must compile");
        return re.replace(&url, "$1:***@").to_string();
    }
    let host = nonempty_env("POSTGRES_HOST").unwrap_or_else(|| "localhost".to_string());
    let port = nonempty_env("POSTGRES_PORT").unwrap_or_else(|| "5432".to_string());
    let db = nonempty_env("POSTGRES_DB").unwrap_or_else(|| "actae".to_string());
    format!("postgresql://{host}:{port}/{db}")
}

/// True when the operator configured an external PostgreSQL through the
/// environment. Kept in lock-step with the startup resolver in `main`.
pub fn external_database_configured() -> bool {
    nonempty_env("DATABASE_URL").is_some()
        || nonempty_env("CLIENT_DATABASE_URL").is_some()
        || nonempty_env("POSTGRES_HOST").is_some()
        || nonempty_env("CLIENT_POSTGRES_HOST").is_some()
}

fn managed_loopback_dashboard_origins(
    managed_storage: bool,
    enable_tls: bool,
    host: &str,
    port: u16,
) -> Option<Vec<String>> {
    if managed_storage && !enable_tls && is_loopback_host(host) {
        let mut origins = vec![
            // Next.js dev server (dashboard on :3000 talking to managed Actae).
            "http://localhost:3000".to_string(),
            "http://127.0.0.1:3000".to_string(),
        ];
        // The managed instance also SERVES the dashboard from its own
        // origin (`http://localhost:<port>`), so that origin must be allowed
        // for the browser's WS upgrade — otherwise the LiveToggle shows
        // "Offline" even though a stack is running. Include both loopback
        // spellings plus the configured host (may be a custom loopback name).
        let own = format!("http://{}:{port}", host.trim());
        for candidate in ["localhost", "127.0.0.1"] {
            let o = format!("http://{candidate}:{port}");
            if !origins.contains(&o) {
                origins.push(o);
            }
        }
        if !origins.contains(&own) {
            origins.push(own);
        }
        Some(origins)
    } else {
        None
    }
}

/// Default implementation for testing and fallback scenarios
impl Default for AppConfig {
    fn default() -> Self {
        Self {
            managed_storage: false,
            deployment_mode: "self_hosted".to_string(),
            host: "0.0.0.0".to_string(),
            port: 8002,
            health_port: 8002,
            max_connections: 1000,
            worker_count: Some(num_cpus::get() as u32),
            main_database_url: "postgresql://localhost:5432/actae".to_string(),
            client_database_url: "postgresql://localhost:5433/actae".to_string(),
            db_max_connections: 10,
            db_min_connections: 2,
            db_connect_timeout: 30,
            db_idle_timeout: 300,
            db_pool_timeout: 30,
            retention_days: 30,
            archive_retention_days: 0,
            state_retention_days: 0,
            tool_execution_retention_days: 0,
            retention_check_interval_secs: 3600,
            ws_max_connections: 1000,
            ws_connection_timeout: 30,
            ws_ping_interval: 30,
            ws_max_message_size: 1048576, // 1MB
            ws_buffer_size: 10000,
            jwt_secret: "dev-jwt-secret-key-please-change-in-production-min-256-bits".to_string(),
            api_key_hash_secret: "dev-jwt-secret-key-please-change-in-production-min-256-bits"
                .to_string(),
            enable_signup: false,
            jwt_expiration: 86400, // 24 hours
            jwt_issuer: Some("actae".to_string()),
            jwt_audience: Some("actae-clients".to_string()),
            api_key_header: "X-API-Key".to_string(),
            enable_anonymous: false, // SECURE DEFAULT: anonymous access disabled
            require_auth_for_private: true,
            enable_compression: true,
            compression_level: 6,
            message_batch_size: 100,
            message_batch_timeout_ms: 10,
            worker_queue_size: 10000,
            broadcaster_queue_size: 10000,
            enable_memory_optimization: true,
            enable_persistence: true,
            enable_metrics: true,
            log_level: "info".to_string(),
            log_format: "json".to_string(),
            log_file_path: None,
            log_file_name: Some("actae.log".to_string()),
            enable_prometheus: true,
            enable_detailed_metrics: true,
            metrics_path: "/metrics".to_string(),
            health_check_path: "/healthz".to_string(),
            readiness_check_path: "/readyz".to_string(),

            // Phase 2: Performance-optimized security defaults
            enable_batch_processing: Some(true), // Enable by default for performance
            enable_advanced_caching: Some(true), // Enable by default for performance

            // Phase 3: Advanced hardening defaults
            enable_crypto_security: Some(true), // Enable by default for security
            enable_threat_detection: Some(true), // Enable by default for security

            // WebSocket Security defaults (HARDENED)
            websocket_allowed_origins: None, // No wildcard by default - must be explicitly configured
            http_allowed_origins: None,
            websocket_max_connections_per_ip: Some(50), // Reduced limit
            websocket_connection_timeout: Some(60),     // Shorter timeout
            websocket_require_auth: true,
            websocket_enable_rate_limiting: true, // ENABLED by default
            websocket_max_message_rate: Some(100), // Much lower default limit
            trusted_proxies: Vec::new(),          // never trust X-Forwarded-For by default

            // Enhanced Security defaults
            anonymous_session_timeout_seconds: 900, // 15 minutes
            enable_ip_allowlist: false,
            enable_security_monitoring: true,
            auth_failure_lockout_threshold: 5,
            auth_failure_lockout_duration_seconds: 300, // 5 minutes
            enable_connection_security_validation: true,

            // TLS defaults
            enable_tls: false,
            tls_cert_path: None,
            tls_key_path: None,
            enable_mtls: false,
            ca_cert_path: None,

            // HTTPS defaults
            https_port: Some(8002),

            // Control plane defaults: standalone (no enforcement)
            control_plane_url: None,
            install_id: None,
            enroll_secret: None,
            enroll_key: None,
            sync_interval_secs: 60,
            subscription_grace_secs: 259200, // 72 hours
            ca_bundle_path: None,
            initial_api_key: None,
            initial_admin_email: None,
            initial_admin_password: None,
            // Federation disabled by default (opt-in per instance).
            federation: Default::default(),
            federation_source_policy:
                crate::auth::source_policy::FederationSourcePolicy::no_narrow_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes tests that mutate the process environment (parallel tests
    /// otherwise race on shared env vars).
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn db_urls_prefer_full_dsn() {
        let _guard = ENV_LOCK.lock().unwrap();
        let keys = [
            "DATABASE_URL",
            "CLIENT_DATABASE_URL",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "POSTGRES_HOST",
            "POSTGRES_DB",
            "CLIENT_POSTGRES_HOST",
        ];
        let saved: Vec<(&str, Option<String>)> =
            keys.iter().map(|k| (*k, env::var(k).ok())).collect();
        for k in keys {
            env::remove_var(k);
        }
        // No DSN, no discrete vars → clear error.
        assert!(AppConfig::build_main_db_url().is_err());
        // A full DATABASE_URL is honored verbatim (no POSTGRES_* needed).
        env::set_var("DATABASE_URL", "postgresql://u:p@h:5432/db?sslmode=disable");
        assert_eq!(
            AppConfig::build_main_db_url().unwrap(),
            "postgresql://u:p@h:5432/db?sslmode=disable"
        );
        // The client pool reuses DATABASE_URL when CLIENT_POSTGRES_HOST is unset.
        assert_eq!(
            AppConfig::build_client_db_url().unwrap(),
            "postgresql://u:p@h:5432/db?sslmode=disable"
        );
        // An explicit CLIENT_DATABASE_URL wins for the client pool.
        env::set_var("CLIENT_DATABASE_URL", "postgresql://u:p@h2:5432/db2");
        assert_eq!(
            AppConfig::build_client_db_url().unwrap(),
            "postgresql://u:p@h2:5432/db2"
        );
        for (k, v) in saved {
            match v {
                Some(v) => env::set_var(k, v),
                None => env::remove_var(k),
            }
        }
    }

    #[test]
    fn storage_backend_resolution() {
        let _guard = ENV_LOCK.lock().unwrap();
        let keys = [
            "ACTAE_STORAGE",
            "DATABASE_URL",
            "CLIENT_DATABASE_URL",
            "POSTGRES_HOST",
            "CLIENT_POSTGRES_HOST",
        ];
        let saved: Vec<(&str, Option<String>)> =
            keys.iter().map(|k| (*k, env::var(k).ok())).collect();
        for k in keys {
            env::remove_var(k);
        }
        // Nothing configured → managed (the self-contained default).
        assert!(storage_is_managed());
        assert!(!external_database_configured());
        // A DATABASE_URL flips to external.
        env::set_var("DATABASE_URL", "postgresql://u:p@h/db");
        assert!(!storage_is_managed());
        assert!(external_database_configured());
        // Explicit ACTAE_STORAGE wins over inference.
        env::set_var("ACTAE_STORAGE", "managed");
        assert!(storage_is_managed());
        env::set_var("ACTAE_STORAGE", "external");
        assert!(!storage_is_managed());
        // Restore.
        for (k, v) in saved {
            match v {
                Some(v) => env::set_var(k, v),
                None => env::remove_var(k),
            }
        }
    }

    #[test]
    fn test_default_config() {
        let config = AppConfig::default();
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.deployment_mode, "self_hosted");
        assert_eq!(config.port, 8002);
        assert_eq!(config.health_port, 8002);
        assert_eq!(config.ws_max_connections, 1000);
        assert_eq!(config.jwt_expiration, 86400);
        assert_eq!(config.api_key_header, "X-API-Key");
        assert!(!config.enable_anonymous);
        assert!(config.require_auth_for_private);
        assert!(config.websocket_require_auth);
        assert_eq!(config.health_check_path, "/healthz");
        assert_eq!(config.readiness_check_path, "/readyz");
    }

    #[test]
    fn test_hosted_mode_is_fail_closed() {
        let mut config = AppConfig::default();
        config.deployment_mode = "hosted".to_string();
        assert!(config.validate().is_err());
        config.control_plane_url = Some("https://saas.example.com".to_string());
        config.install_id = Some("ins_hosted".to_string());
        config.enroll_secret = Some("es_secret".to_string());
        config.jwt_secret = "hosted-jwt-secret-with-enough-entropy-12345".to_string();
        config.api_key_hash_secret = "independent-api-key-hash-secret-67890".to_string();
        config.http_allowed_origins = Some(vec!["https://tenant.example.com".to_string()]);
        config.websocket_allowed_origins = Some(vec!["https://tenant.example.com".to_string()]);
        assert!(config.validate().is_ok());
        config.enable_anonymous = true;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_default_security_settings() {
        let config = AppConfig::default();
        assert_eq!(config.websocket_max_connections_per_ip, Some(50));
        assert_eq!(config.websocket_max_message_rate, Some(100));
        assert!(config.websocket_enable_rate_limiting);
        assert!(config.enable_crypto_security.unwrap_or(false));
        assert!(config.enable_threat_detection.unwrap_or(false));
    }

    #[test]
    fn test_managed_loopback_dashboard_origins_are_local_only() {
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("::1"));
        assert!(!is_loopback_host("0.0.0.0"));
        assert!(!is_loopback_host("192.168.1.10"));
        // Embedded loopback mode allows the Next dev server (:3000) AND the
        // server's own origin so the dashboard served from the embedded
        // instance can upgrade its WebSocket (same-origin Host matching is
        // disabled once an explicit list exists).
        assert_eq!(
            managed_loopback_dashboard_origins(true, false, "127.0.0.1", 8010),
            Some(vec![
                "http://localhost:3000".to_string(),
                "http://127.0.0.1:3000".to_string(),
                "http://localhost:8010".to_string(),
                "http://127.0.0.1:8010".to_string(),
            ])
        );
        // A custom loopback host is added alongside the standard spellings.
        assert_eq!(
            managed_loopback_dashboard_origins(true, false, "localhost", 8002),
            Some(vec![
                "http://localhost:3000".to_string(),
                "http://127.0.0.1:3000".to_string(),
                "http://localhost:8002".to_string(),
                "http://127.0.0.1:8002".to_string(),
            ])
        );
        assert_eq!(
            managed_loopback_dashboard_origins(false, false, "127.0.0.1", 8002),
            None
        );
        assert_eq!(
            managed_loopback_dashboard_origins(true, true, "127.0.0.1", 8002),
            None
        );
        assert_eq!(
            managed_loopback_dashboard_origins(true, false, "0.0.0.0", 8002),
            None
        );
    }

    #[test]
    fn test_default_feature_flags() {
        let config = AppConfig::default();
        assert!(config.enable_metrics);
    }

    #[test]
    fn test_default_tls_settings() {
        let config = AppConfig::default();
        assert!(!config.enable_tls);
        assert!(config.tls_cert_path.is_none());
        assert_eq!(config.https_port, Some(8002));
    }

    #[test]
    fn test_default_performance_settings() {
        let config = AppConfig::default();
        assert!(config.enable_compression);
        assert_eq!(config.compression_level, 6);
        assert_eq!(config.message_batch_size, 100);
        assert!(config.enable_memory_optimization);
    }

    #[test]
    fn test_get_instance_id_format() {
        let config = AppConfig::default();
        let id = config.get_instance_id();
        assert!(!id.is_empty());
        assert!(id.contains('_'));
    }

    #[test]
    fn test_to_json_contains_fields() {
        let config = AppConfig::default();
        let json = config.to_json().unwrap();
        assert!(json.contains("0.0.0.0"));
        assert!(json.contains("8002"));
    }

    #[test]
    fn test_jwt_secret_default_length() {
        let config = AppConfig::default();
        assert!(config.jwt_secret.len() >= 32);
    }

    #[test]
    fn uses_default_jwt_secret_detects_the_builtin() {
        let mut config = AppConfig::default();
        assert_eq!(config.jwt_secret, DEFAULT_JWT_SECRET);
        assert!(config.uses_default_jwt_secret());
        config.jwt_secret = "a-real-secret-with-enough-entropy-1234567890".to_string();
        assert!(!config.uses_default_jwt_secret());
    }

    #[test]
    fn test_database_defaults() {
        let config = AppConfig::default();
        assert_eq!(config.db_max_connections, 10);
        assert_eq!(config.db_min_connections, 2);
        assert_eq!(config.db_connect_timeout, 30);
    }

    #[test]
    fn test_logging_defaults() {
        let config = AppConfig::default();
        assert_eq!(config.log_level, "info");
        assert_eq!(config.log_format, "json");
    }

    #[test]
    fn test_default_control_plane_standalone() {
        let config = AppConfig::default();
        assert!(config.control_plane_url.is_none());
        assert!(config.install_id.is_none());
        assert!(config.enroll_secret.is_none());
        assert_eq!(config.sync_interval_secs, 60);
        assert_eq!(config.subscription_grace_secs, 259200);
        assert!(config.ca_bundle_path.is_none());
        assert!(config.initial_api_key.is_none());
    }

    #[test]
    fn test_validate_standalone_passes() {
        let config = AppConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_default_security_posture() {
        let config = AppConfig::default();
        assert!(
            !config.enable_mtls,
            "mTLS must default to off (opt-in tier)"
        );
        assert!(config.ca_cert_path.is_none());
        assert!(!config.enable_signup, "password signup must default to off");
        assert_eq!(config.api_key_hash_secret, config.jwt_secret);
    }

    #[test]
    fn test_mtls_requires_tls() {
        let mut config = AppConfig::default();
        config.enable_mtls = true;
        config.ca_cert_path = Some("/tmp/ca.crt".to_string());
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string().contains("ENABLE_MTLS requires ENABLE_TLS"),
            "got: {err}"
        );
    }

    #[test]
    fn test_mtls_requires_ca() {
        let mut config = AppConfig::default();
        config.enable_tls = true;
        config.enable_mtls = true;
        config.ca_cert_path = None;
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string().contains("requires CA_CERT_PATH"),
            "got: {err}"
        );
    }

    #[test]
    fn test_mtls_valid_config_passes() {
        let mut config = AppConfig::default();
        config.enable_tls = true;
        config.enable_mtls = true;
        config.ca_cert_path = Some("/tmp/ca.crt".to_string());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_api_key_hash_secret_defaults_to_jwt_secret() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("API_KEY_HASH_SECRET");
        let jwt = "some-valid-jwt-secret-at-least-32-chars-long!!".to_string();
        assert_eq!(
            AppConfig::resolve_api_key_hash_secret(&jwt).unwrap(),
            jwt,
            "unset API_KEY_HASH_SECRET must inherit the JWT secret (backward compat)"
        );
        std::env::remove_var("API_KEY_HASH_SECRET");
    }

    #[test]
    fn test_api_key_hash_secret_explicit_value() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var(
            "API_KEY_HASH_SECRET",
            "independent-key-hash-secret-32-chars!!",
        );
        let jwt = "some-valid-jwt-secret-at-least-32-chars-long!!".to_string();
        assert_eq!(
            AppConfig::resolve_api_key_hash_secret(&jwt).unwrap(),
            "independent-key-hash-secret-32-chars!!"
        );
        std::env::remove_var("API_KEY_HASH_SECRET");
    }

    #[test]
    fn test_api_key_hash_secret_too_short_rejected() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("API_KEY_HASH_SECRET", "short");
        let err = AppConfig::resolve_api_key_hash_secret("jwt-secret-long-enough-for-testing!!")
            .unwrap_err();
        assert!(
            err.to_string().contains("at least 32 characters"),
            "got: {err}"
        );
        std::env::remove_var("API_KEY_HASH_SECRET");
    }

    #[test]
    fn test_api_key_hash_secret_low_entropy_rejected() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("API_KEY_HASH_SECRET", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let err = AppConfig::resolve_api_key_hash_secret("jwt-secret-long-enough-for-testing!!")
            .unwrap_err();
        assert!(err.to_string().contains("low entropy"), "got: {err}");
        std::env::remove_var("API_KEY_HASH_SECRET");
    }

    #[test]
    fn test_secret_file_is_read_and_direct_value_is_mutually_exclusive() {
        let _guard = ENV_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        std::fs::write(&path, "mounted-secret-value\n").unwrap();
        std::env::remove_var("ACTAE_TEST_SECRET");
        std::env::set_var("ACTAE_TEST_SECRET_FILE", &path);
        assert_eq!(
            AppConfig::secret_from_env_or_file("ACTAE_TEST_SECRET").unwrap(),
            Some("mounted-secret-value".to_string())
        );
        std::env::set_var("ACTAE_TEST_SECRET", "metadata-secret");
        let error = AppConfig::secret_from_env_or_file("ACTAE_TEST_SECRET").unwrap_err();
        assert!(error.to_string().contains("mutually exclusive"));
        std::env::remove_var("ACTAE_TEST_SECRET");
        std::env::remove_var("ACTAE_TEST_SECRET_FILE");
    }

    #[test]
    fn test_validate_partial_control_plane_rejected() {
        let mut config = AppConfig::default();
        config.control_plane_url = Some("https://saas.example.com".to_string());
        assert!(
            config.validate().is_ok(),
            "URL alone is valid — credentials may come from the enrollment file"
        );
        config.install_id = Some("ins_1".to_string());
        assert!(
            config.validate().is_err(),
            "install without secret must fail"
        );
        config.enroll_secret = Some("es_1".to_string());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_zero_sync_interval_rejected() {
        let mut config = AppConfig::default();
        config.control_plane_url = Some("https://saas.example.com".to_string());
        config.install_id = Some("ins_1".to_string());
        config.enroll_secret = Some("es_1".to_string());
        config.sync_interval_secs = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_negative_grace_rejected() {
        let mut config = AppConfig::default();
        config.control_plane_url = Some("https://saas.example.com".to_string());
        config.install_id = Some("ins_1".to_string());
        config.enroll_secret = Some("es_1".to_string());
        config.subscription_grace_secs = -1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_redact_db_url_masks_passwords() {
        let config = AppConfig::default();
        assert_eq!(
            config.redact_db_url("postgresql://actae:s3cret@localhost:5432/actae"),
            "postgresql://actae:***@localhost:5432/actae"
        );
        assert_eq!(
            config.redact_db_url("postgresql://user:p%40ss@db.example.com:5432/db"),
            "postgresql://user:***@db.example.com:5432/db"
        );
    }

    #[test]
    fn test_redact_db_url_leaves_credential_free_urls_unchanged() {
        let config = AppConfig::default();
        assert_eq!(
            config.redact_db_url("postgresql://localhost:5432/actae"),
            "postgresql://localhost:5432/actae"
        );
        assert_eq!(config.redact_db_url(""), "");
    }

    #[test]
    fn test_is_loopback_host_matrix() {
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("::1"));
        assert!(is_loopback_host("[::1]"));
        assert!(is_loopback_host(" localhost "));
        assert!(!is_loopback_host("0.0.0.0"));
        assert!(!is_loopback_host("192.168.1.10"));
        assert!(!is_loopback_host("actae.dev"));
        assert!(!is_loopback_host(""));
    }

    #[test]
    fn test_embedded_dashboard_origins_matrix() {
        // Embedded + loopback + no TLS → local dev dashboard + own origin.
        let origins = managed_loopback_dashboard_origins(true, false, "127.0.0.1", 8002).unwrap();
        assert!(origins.contains(&"http://localhost:3000".to_string()));
        assert!(origins.contains(&"http://127.0.0.1:3000".to_string()));
        assert!(origins.contains(&"http://localhost:8002".to_string()));
        // TLS enabled → no convenience origins (browser same-origin only).
        assert!(managed_loopback_dashboard_origins(true, true, "127.0.0.1", 8002).is_none());
        // Non-loopback bind → no convenience origins.
        assert!(managed_loopback_dashboard_origins(true, false, "0.0.0.0", 8002).is_none());
        // Non-managed → no convenience origins.
        assert!(managed_loopback_dashboard_origins(false, false, "127.0.0.1", 8002).is_none());
    }

    #[test]
    fn test_validate_requires_db_urls() {
        let mut config = AppConfig::default();
        config.main_database_url = "".into();
        assert!(config.validate().is_err());
        let mut config = AppConfig::default();
        config.client_database_url = "".into();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_short_secrets() {
        let mut config = AppConfig::default();
        config.jwt_secret = "short".into();
        assert!(config.validate().is_err());
        let mut config = AppConfig::default();
        config.api_key_hash_secret = "short".into();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_invalid_deployment_mode() {
        let mut config = AppConfig::default();
        config.deployment_mode = "edge".into();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_zero_port() {
        let mut config = AppConfig::default();
        config.port = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_zero_performance_settings() {
        let mut config = AppConfig::default();
        config.message_batch_size = 0;
        assert!(config.validate().is_err());
        let mut config = AppConfig::default();
        config.worker_queue_size = 0;
        assert!(config.validate().is_err());
        let mut config = AppConfig::default();
        config.ws_max_connections = 0;
        assert!(config.validate().is_err());
        let mut config = AppConfig::default();
        config.ws_max_message_size = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_rejects_partial_install_credentials() {
        let mut config = AppConfig::default();
        config.install_id = Some("ins_1".into());
        assert!(
            config.validate().is_err(),
            "install without secret must fail"
        );
        let mut config = AppConfig::default();
        config.enroll_secret = Some("es_1".into());
        assert!(
            config.validate().is_err(),
            "secret without install must fail"
        );
    }

    #[test]
    fn test_validate_hosted_forbids_initial_api_key_and_dev_jwt() {
        let mut hosted = AppConfig::default();
        hosted.deployment_mode = "hosted".into();
        hosted.control_plane_url = Some("https://saas.example.com".into());
        hosted.install_id = Some("ins_1".into());
        hosted.enroll_secret = Some("es_1".into());
        hosted.jwt_secret = "independent-production-jwt-secret-12345".into();
        hosted.api_key_hash_secret = "independent-api-key-hash-secret-67890".into();
        hosted.http_allowed_origins = Some(vec!["https://tenant.example.com".into()]);
        hosted.websocket_allowed_origins = Some(vec!["https://tenant.example.com".into()]);
        assert!(hosted.validate().is_ok());
        hosted.initial_api_key = Some("sk-bad".into());
        assert!(
            hosted.validate().is_err(),
            "hosted must forbid ACTAE_INITIAL_API_KEY"
        );
        hosted.initial_api_key = None;
        hosted.jwt_secret = "dev-jwt-secret-key-please-change-in-production-min-256-bits".into();
        assert!(
            hosted.validate().is_err(),
            "hosted must reject the dev JWT secret"
        );
    }

    #[test]
    fn test_validate_hosted_requires_origins() {
        let mut hosted = AppConfig::default();
        hosted.deployment_mode = "hosted".into();
        hosted.control_plane_url = Some("https://saas.example.com".into());
        hosted.install_id = Some("ins_1".into());
        hosted.enroll_secret = Some("es_1".into());
        hosted.jwt_secret = "independent-production-jwt-secret-12345".into();
        hosted.api_key_hash_secret = "independent-api-key-hash-secret-67890".into();
        assert!(hosted.validate().is_err(), "hosted requires HTTP origins");
        hosted.http_allowed_origins = Some(vec!["https://tenant.example.com".into()]);
        assert!(hosted.validate().is_err(), "hosted requires WS origins");
        hosted.websocket_allowed_origins = Some(vec!["https://tenant.example.com".into()]);
        assert!(hosted.validate().is_ok());
    }

    #[test]
    fn test_validate_rejects_duplicate_jwt_and_hash_secret_in_hosted() {
        let mut hosted = AppConfig::default();
        hosted.deployment_mode = "hosted".into();
        hosted.control_plane_url = Some("https://saas.example.com".into());
        hosted.install_id = Some("ins_1".into());
        hosted.enroll_secret = Some("es_1".into());
        hosted.jwt_secret = "the-same-secret-for-both-the-jwt-and-the-hash-123456".into();
        hosted.api_key_hash_secret = hosted.jwt_secret.clone();
        hosted.http_allowed_origins = Some(vec!["https://tenant.example.com".into()]);
        hosted.websocket_allowed_origins = Some(vec!["https://tenant.example.com".into()]);
        assert!(
            hosted.validate().is_err(),
            "hosted requires distinct secrets"
        );
    }
}
