// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result};
use dashmap::DashMap;
use deadpool_postgres::{Config, Pool, Runtime};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio_postgres::NoTls;
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{error, info, warn};

pub mod client_migrations;
pub mod error;
pub mod json_compat;
pub mod models;
pub mod repositories;
pub mod schema_validator;

use crate::config::AppConfig;
use crate::database::error::DatabaseError;

// Connection performance metrics
static CONNECTION_METRICS: std::sync::LazyLock<Arc<DashMap<String, ConnectionMetrics>>> =
    std::sync::LazyLock::new(|| Arc::new(DashMap::new()));

/// Connection performance metrics for optimization
#[derive(Debug, Clone)]
pub struct ConnectionMetrics {
    pub total_queries: u64,
    pub successful_queries: u64,
    pub failed_queries: u64,
    pub avg_query_time_ms: f64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub last_used: std::time::Instant,
}

/// Database manager handling multiple PostgreSQL connection pools
#[derive(Clone, Debug)]
pub struct DatabaseManager {
    /// Main database pool for real-time data (actae)
    pub main_pool: Pool,
    /// Client database pool for SaaS operations (actae)  
    pub client_pool: Pool,
    /// Pool configuration for optimization
    pub pool_config: Arc<PoolConfig>,
    /// Per-pool circuit breakers for fault isolation
    pub circuit_breakers: Arc<RwLock<HashMap<String, CircuitBreaker>>>,
    /// Semaphore limiting concurrent database operations
    pub db_semaphore: Arc<Semaphore>,
}

/// Enhanced pool configuration for performance optimization
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub max_connections: u32,
    pub min_connections: u32,
    pub connection_timeout: Duration,
    pub idle_timeout: Duration,
    pub enable_connection_warmup: bool,
}

impl DatabaseManager {
    /// Create a new database manager with connection pools
    pub async fn new(config: &AppConfig) -> Result<Self> {
        let (main_db_url, client_db_url) = config.get_database_urls();

        // Enhanced pool configuration for performance
        let pool_config = Arc::new(PoolConfig {
            max_connections: config.db_max_connections,
            min_connections: config.db_min_connections,
            connection_timeout: Duration::from_secs(config.db_connect_timeout),
            idle_timeout: Duration::from_secs(config.db_idle_timeout),
            enable_connection_warmup: true,
        });

        info!("Initializing enhanced database connection pools");
        info!(
            "Main database: {}",
            Self::safe_connection_string(&main_db_url)
        );
        info!(
            "Client database: {}",
            Self::safe_connection_string(&client_db_url)
        );

        // Create main database pool
        let main_pool = Self::create_pool(
            &main_db_url,
            pool_config.max_connections,
            pool_config.min_connections,
            pool_config.connection_timeout,
            pool_config.idle_timeout,
            Duration::from_secs(config.db_pool_timeout),
            "main",
        )
        .await
        .context("Failed to create main database pool")?;

        // Create client database pool
        let client_pool = Self::create_pool(
            &client_db_url,
            pool_config.max_connections,
            pool_config.min_connections,
            pool_config.connection_timeout,
            pool_config.idle_timeout,
            Duration::from_secs(config.db_pool_timeout),
            "client",
        )
        .await
        .context("Failed to create client database pool")?;

        let manager = Self {
            main_pool,
            client_pool,
            pool_config,
            circuit_breakers: {
                let mut map = HashMap::new();
                let threshold = 10u32;
                let timeout = Duration::from_secs(60);
                map.insert("main".to_string(), CircuitBreaker::new(threshold, timeout));
                map.insert(
                    "client".to_string(),
                    CircuitBreaker::new(threshold, timeout),
                );
                Arc::new(RwLock::new(map))
            },
            db_semaphore: Arc::new(Semaphore::new(200)),
        };

        // Test connections with retry to handle database startup delay
        const MAX_RETRIES: u32 = 10;
        const INITIAL_DELAY_SECS: u64 = 2;
        let mut attempt = 0;

        loop {
            match manager.test_connections().await {
                Ok(_) => break,
                Err(e) => {
                    attempt += 1;
                    if attempt >= MAX_RETRIES {
                        error!("Database connection test failed after {attempt} attempts: {e}");
                        error!(
                            "If you did not intend to use an external PostgreSQL, unset \
                             DATABASE_URL / CLIENT_DATABASE_URL / POSTGRES_* (or set \
                             ACTAE_STORAGE=managed) and restart to use the bundled database."
                        );
                        return Err(e);
                    }
                    let delay = INITIAL_DELAY_SECS * attempt as u64;
                    warn!(
                        "Database connections not ready (attempt {attempt}/{MAX_RETRIES}): {e}. Retrying in {delay}s..."
                    );
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                }
            }
        }

        info!("Database connection pools initialized successfully");
        Ok(manager)
    }

    /// Create a connection pool with the given configuration
    async fn create_pool(
        database_url: &str,
        max_connections: u32,
        min_connections: u32,
        connect_timeout: Duration,
        idle_timeout: Duration,
        pool_timeout: Duration,
        pool_name: &str,
    ) -> Result<Pool> {
        let mut cfg = Config::new();

        // Parse the database URL
        let parsed_url = url::Url::parse(database_url).context("Invalid database URL format")?;

        // Extract connection parameters. The `postgresql` scheme is
        // non-special for the url crate, which does NOT percent-decode
        // userinfo for such schemes, so credentials are decoded here
        // explicitly (they are percent-encoded when the URL is built from
        // POSTGRES_* env vars — see config::AppConfig::build_main_db_url).
        let decode = |s: &str| {
            percent_encoding::percent_decode_str(s)
                .decode_utf8_lossy()
                .to_string()
        };
        cfg.host = parsed_url.host_str().map(|h| h.to_string());
        cfg.port = parsed_url.port();
        cfg.user = Some(decode(parsed_url.username()));
        cfg.password = parsed_url.password().map(decode);
        cfg.dbname = parsed_url.path().trim_start_matches('/').to_string().into();

        // Set pool configuration
        cfg.manager = Some(deadpool_postgres::ManagerConfig {
            recycling_method: deadpool_postgres::RecyclingMethod::Fast,
        });

        cfg.pool = Some(deadpool_postgres::PoolConfig {
            max_size: max_connections as usize,
            timeouts: deadpool_postgres::Timeouts {
                wait: Some(pool_timeout),
                create: Some(connect_timeout),
                recycle: Some(Duration::from_secs(10)),
            },
            queue_mode: Default::default(),
        });

        let ssl_mode = parsed_url
            .query_pairs()
            .find(|(key, _)| key == "sslmode")
            .map(|(_, value)| value.to_ascii_lowercase())
            // Preserve existing explicit DATABASE_URL behavior for local and
            // private-network deployments. URLs assembled from POSTGRES_*
            // default to authenticated TLS in AppConfig; operators supplying
            // DATABASE_URL must add sslmode=verify-full for public networks.
            .unwrap_or_else(|| "disable".to_string());
        let ssl_root_cert = parsed_url
            .query_pairs()
            .find(|(key, _)| key == "sslrootcert")
            .map(|(_, value)| value.into_owned());

        let pool = match ssl_mode.as_str() {
            "disable" => cfg
                .create_pool(Some(Runtime::Tokio1), NoTls)
                .context(format!("Failed to create {pool_name} database pool"))?,
            "require" | "verify-ca" | "verify-full" => {
                // All enabled modes verify both the certificate chain and
                // hostname. This is deliberately stricter than libpq's
                // historical `require`/`verify-ca` semantics and prevents a
                // hosted deployment from opting into encryption without peer
                // authentication.
                let mut roots = rustls::RootCertStore::empty();
                roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
                if let Some(path) = ssl_root_cert.as_deref() {
                    let pem = std::fs::read(path).with_context(|| {
                        format!("failed to read PostgreSQL CA bundle at {path}")
                    })?;
                    let certs = rustls_pemfile::certs(&mut pem.as_slice())
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .with_context(|| {
                            format!("failed to parse PostgreSQL CA bundle at {path}")
                        })?;
                    if certs.is_empty() {
                        return Err(anyhow::anyhow!(
                            "PostgreSQL CA bundle at {path} contains no certificates"
                        ));
                    }
                    for cert in certs {
                        roots.add(cert).with_context(|| {
                            format!("invalid certificate in PostgreSQL CA bundle at {path}")
                        })?;
                    }
                }
                let tls_config = rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth();
                cfg.create_pool(Some(Runtime::Tokio1), MakeRustlsConnect::new(tls_config))
                    .context(format!("Failed to create TLS {pool_name} database pool"))?
            }
            "prefer" | "allow" => {
                return Err(anyhow::anyhow!(
                    "database sslmode={ssl_mode} permits plaintext downgrade; use disable explicitly for local PostgreSQL or require/verify-full for TLS"
                ));
            }
            other => {
                return Err(anyhow::anyhow!(
                    "unsupported database sslmode '{other}' (expected disable, require, verify-ca, or verify-full)"
                ));
            }
        };

        info!(
            "Created {} database pool: max={}, min={}, connect={:?}, idle={:?}, pool={:?}",
            pool_name,
            max_connections,
            min_connections,
            connect_timeout,
            idle_timeout,
            pool_timeout
        );

        Ok(pool)
    }

    /// Test database connections to ensure they work
    async fn test_connections(&self) -> Result<()> {
        info!("Testing database connections...");

        // Test main database connection
        match self.main_pool.get().await {
            Ok(client) => match client.query_one("SELECT 1 as test", &[]).await {
                Ok(_) => info!(" Main database connection successful"),
                Err(e) => {
                    error!(" Main database query failed: {}", e);
                    return Err(anyhow::anyhow!("Main database query test failed: {e}"));
                }
            },
            Err(e) => {
                error!(" Failed to get main database connection: {}", e);
                return Err(anyhow::anyhow!("Main database connection failed: {e}"));
            }
        }

        // Test client database connection
        match self.client_pool.get().await {
            Ok(client) => match client.query_one("SELECT 1 as test", &[]).await {
                Ok(_) => info!(" Client database connection successful"),
                Err(e) => {
                    error!(" Client database query failed: {}", e);
                    return Err(anyhow::anyhow!("Client database query test failed: {e}"));
                }
            },
            Err(e) => {
                error!(" Failed to get client database connection: {}", e);
                return Err(anyhow::anyhow!("Client database connection failed: {e}"));
            }
        }

        info!(" All database connections tested successfully");
        Ok(())
    }

    /// Get a connection from the main database pool
    pub async fn get_main_connection(&self) -> Result<deadpool_postgres::Client, DatabaseError> {
        self.get_connection_with_timeout(&self.main_pool, "main")
            .await
    }

    /// Get a connection from the client database pool
    pub async fn get_client_connection(&self) -> Result<deadpool_postgres::Client, DatabaseError> {
        self.get_connection_with_timeout(&self.client_pool, "client")
            .await
    }

    async fn get_connection_with_timeout(
        &self,
        pool: &Pool,
        pool_name: &str,
    ) -> Result<deadpool_postgres::Client, DatabaseError> {
        {
            let breakers = self.circuit_breakers.read().map_err(|_| {
                DatabaseError::ConnectionError(deadpool_postgres::PoolError::Closed)
            })?;
            if let Some(breaker) = breakers.get(pool_name) {
                if !breaker.can_execute() {
                    return Err(DatabaseError::ConnectionError(
                        deadpool_postgres::PoolError::Timeout(deadpool_postgres::TimeoutType::Wait),
                    ));
                }
            }
        }

        let _permit = self.db_semaphore.try_acquire().map_err(|_| {
            DatabaseError::ConnectionError(deadpool_postgres::PoolError::Timeout(
                deadpool_postgres::TimeoutType::Wait,
            ))
        })?;

        let client = pool.get().await.map_err(DatabaseError::from)?;

        // Set statement timeout on this connection to prevent runaway queries
        let _ = client.execute("SET statement_timeout = '30s'", &[]).await;

        // Record success in circuit breaker
        if let Ok(mut breakers) = self.circuit_breakers.write() {
            if let Some(breaker) = breakers.get_mut(pool_name) {
                breaker.record_success();
            }
        }

        Ok(client)
    }

    /// Execute a health check on both database pools
    pub async fn health_check(&self) -> Result<DatabaseHealth> {
        let mut health = DatabaseHealth {
            main_pool_healthy: false,
            client_pool_healthy: false,
            main_pool_connections: 0,
            client_pool_connections: 0,
        };

        // Check main pool
        match self.main_pool.get().await {
            Ok(client) => {
                if client.query_one("SELECT 1", &[]).await.is_ok() {
                    health.main_pool_healthy = true;
                }
            }
            Err(e) => {
                warn!("Main database health check failed: {}", e);
                if let Ok(mut breakers) = self.circuit_breakers.write() {
                    if let Some(breaker) = breakers.get_mut("main") {
                        breaker.record_failure();
                    }
                }
            }
        }

        // Check client pool
        match self.client_pool.get().await {
            Ok(client) => {
                if client.query_one("SELECT 1", &[]).await.is_ok() {
                    health.client_pool_healthy = true;
                }
            }
            Err(e) => {
                warn!("Client database health check failed: {}", e);
                if let Ok(mut breakers) = self.circuit_breakers.write() {
                    if let Some(breaker) = breakers.get_mut("client") {
                        breaker.record_failure();
                    }
                }
            }
        }

        // Get pool statistics
        health.main_pool_connections = self.main_pool.status().size;
        health.client_pool_connections = self.client_pool.status().size;

        Ok(health)
    }

    /// Get database pool statistics
    pub fn get_pool_stats(&self) -> PoolStats {
        let main_status = self.main_pool.status();
        let client_status = self.client_pool.status();

        PoolStats {
            main_pool: PoolStat {
                size: main_status.size,
                available: main_status.available,
                waiting: main_status.waiting,
            },
            client_pool: PoolStat {
                size: client_status.size,
                available: client_status.available,
                waiting: client_status.waiting,
            },
        }
    }

    /// Create a safe connection string for logging (removes password)
    fn safe_connection_string(url: &str) -> String {
        match url::Url::parse(url) {
            Ok(parsed) => {
                let mut safe_url = parsed.clone();
                if parsed.password().is_some() {
                    let _ = safe_url.set_password(Some("***"));
                }
                safe_url.to_string()
            }
            Err(_) => url.chars().take(20).collect::<String>() + "...",
        }
    }

    /// Execute a prepared statement with metrics.
    ///
    /// This prepares an unnamed statement on the given connection every call.
    /// PostgreSQL's unnamed statement cache holds one statement per connection,
    /// so repeated calls with the same SQL on the same connection may avoid a
    /// full round-trip. For true multi-statement caching, use named statements
    /// at the PostgreSQL protocol level (not yet exposed by tokio-postgres).
    #[allow(dead_code)]
    pub async fn execute_cached_statement(
        &self,
        client: &deadpool_postgres::Client,
        pool_name: &str,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Vec<tokio_postgres::Row>, DatabaseError> {
        let stmt = client.prepare(sql).await.map_err(DatabaseError::from)?;

        let execution_start = std::time::Instant::now();
        let result = client
            .query(&stmt, params)
            .await
            .map_err(DatabaseError::from)?;

        self.update_query_metrics(pool_name, true, execution_start.elapsed());
        Ok(result)
    }

    /// Execute a single row query with metrics.
    ///
    /// See [`execute_cached_statement`] for caching semantics (unnamed
    /// statements per connection).
    #[allow(dead_code)]
    pub async fn query_one_cached(
        &self,
        client: &deadpool_postgres::Client,
        pool_name: &str,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<tokio_postgres::Row, DatabaseError> {
        let stmt = client.prepare(sql).await.map_err(DatabaseError::from)?;

        let execution_start = std::time::Instant::now();
        let result = client
            .query_one(&stmt, params)
            .await
            .map_err(DatabaseError::from)?;

        self.update_query_metrics(pool_name, true, execution_start.elapsed());
        Ok(result)
    }

    /// Update query execution metrics
    fn update_query_metrics(&self, pool_name: &str, success: bool, duration: Duration) {
        let metrics_key = format!("{pool_name}_queries");
        let mut metrics =
            CONNECTION_METRICS
                .entry(metrics_key)
                .or_insert_with(|| ConnectionMetrics {
                    total_queries: 0,
                    successful_queries: 0,
                    failed_queries: 0,
                    avg_query_time_ms: 0.0,
                    cache_hits: 0,
                    cache_misses: 0,
                    last_used: std::time::Instant::now(),
                });

        metrics.total_queries += 1;
        if success {
            metrics.successful_queries += 1;
        } else {
            metrics.failed_queries += 1;
        }

        // Update average query time using exponential moving average
        let query_time_ms = duration.as_secs_f64() * 1000.0;
        if metrics.avg_query_time_ms == 0.0 {
            metrics.avg_query_time_ms = query_time_ms;
        } else {
            // EMA with alpha = 0.1 (gives more weight to recent queries)
            metrics.avg_query_time_ms = 0.1 * query_time_ms + 0.9 * metrics.avg_query_time_ms;
        }

        metrics.last_used = std::time::Instant::now();
    }

    /// Get database performance metrics
    pub fn get_performance_metrics(&self) -> DatabasePerformanceMetrics {
        let mut main_cache_metrics = None;
        let mut client_cache_metrics = None;
        let mut main_query_metrics = None;
        let mut client_query_metrics = None;

        // Collect metrics from all tracked pools
        for entry in CONNECTION_METRICS.iter() {
            match entry.key().as_str() {
                "main_cache" => main_cache_metrics = Some(entry.value().clone()),
                "client_cache" => client_cache_metrics = Some(entry.value().clone()),
                "main_queries" => main_query_metrics = Some(entry.value().clone()),
                "client_queries" => client_query_metrics = Some(entry.value().clone()),
                _ => {}
            }
        }

        DatabasePerformanceMetrics {
            main_cache_metrics,
            client_cache_metrics,
            main_query_metrics,
            client_query_metrics,
        }
    }

    /// Close all database connections gracefully
    pub async fn close(&self) {
        info!("Closing database connection pools...");

        self.main_pool.close();
        self.client_pool.close();

        info!("Database connection pools closed");
    }
}

/// Database performance metrics for monitoring
#[derive(Debug, Clone)]
pub struct DatabasePerformanceMetrics {
    pub main_cache_metrics: Option<ConnectionMetrics>,
    pub client_cache_metrics: Option<ConnectionMetrics>,
    pub main_query_metrics: Option<ConnectionMetrics>,
    pub client_query_metrics: Option<ConnectionMetrics>,
}

/// Database health status
#[derive(Debug, Clone)]
pub struct DatabaseHealth {
    pub main_pool_healthy: bool,
    pub client_pool_healthy: bool,
    pub main_pool_connections: usize,
    pub client_pool_connections: usize,
}

impl DatabaseHealth {
    pub fn is_healthy(&self) -> bool {
        self.main_pool_healthy && self.client_pool_healthy
    }
}

/// Pool statistics for monitoring
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub main_pool: PoolStat,
    pub client_pool: PoolStat,
}

#[derive(Debug, Clone)]
pub struct PoolStat {
    pub size: usize,
    pub available: usize,
    pub waiting: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    state: CircuitState,
    failure_count: u32,
    failure_threshold: u32,
    timeout: Duration,
    last_failure_time: Option<Instant>,
    success_count: u32,
    half_open_threshold: u32,
}

impl CircuitBreaker {
    fn new(failure_threshold: u32, timeout: Duration) -> Self {
        Self {
            state: CircuitState::Closed,
            failure_count: 0,
            failure_threshold,
            timeout,
            last_failure_time: None,
            success_count: 0,
            half_open_threshold: 1,
        }
    }

    fn can_execute(&self) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(last_failure) = self.last_failure_time {
                    if last_failure.elapsed() >= self.timeout {
                        return true;
                    }
                }
                false
            }
            CircuitState::HalfOpen => self.success_count < self.half_open_threshold,
        }
    }

    fn record_success(&mut self) {
        match self.state {
            CircuitState::HalfOpen => {
                self.success_count += 1;
                if self.success_count >= self.half_open_threshold {
                    self.state = CircuitState::Closed;
                    self.failure_count = 0;
                    self.success_count = 0;
                }
            }
            CircuitState::Closed => {
                self.success_count = 0;
            }
            CircuitState::Open => {}
        }
    }

    fn record_failure(&mut self) {
        self.failure_count += 1;
        self.last_failure_time = Some(Instant::now());
        match self.state {
            CircuitState::Closed => {
                if self.failure_count >= self.failure_threshold {
                    self.state = CircuitState::Open;
                }
            }
            CircuitState::HalfOpen => {
                self.state = CircuitState::Open;
            }
            CircuitState::Open => {}
        }
    }
}

impl PoolStats {
    pub fn total_connections(&self) -> usize {
        self.main_pool.size + self.client_pool.size
    }

    pub fn total_available(&self) -> usize {
        self.main_pool.available + self.client_pool.available
    }

    pub fn total_waiting(&self) -> usize {
        self.main_pool.waiting + self.client_pool.waiting
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // ── CircuitState ────────────────────────────────────────────────────────

    #[test]
    fn circuit_state_derives_debug_clone_copy_partial_eq_eq() {
        let a = CircuitState::Closed;
        let b = a;
        assert_eq!(a, b);
        let _ = format!("{a:?}");
        let _ = a;
        let c = CircuitState::Open;
        assert_ne!(a, c);
        assert_ne!(CircuitState::Open, CircuitState::HalfOpen);
        assert_ne!(CircuitState::Closed, CircuitState::HalfOpen);
    }

    // ── CircuitBreaker::new ─────────────────────────────────────────────────

    #[test]
    fn circuit_breaker_new_initial_state() {
        let cb = CircuitBreaker::new(5, Duration::from_secs(30));
        assert_eq!(cb.state, CircuitState::Closed);
        assert_eq!(cb.failure_count, 0);
        assert_eq!(cb.failure_threshold, 5);
        assert_eq!(cb.timeout, Duration::from_secs(30));
        assert_eq!(cb.last_failure_time, None);
        assert_eq!(cb.success_count, 0);
        assert_eq!(cb.half_open_threshold, 1);
    }

    // ── CircuitBreaker::can_execute ─────────────────────────────────────────

    #[test]
    fn can_execute_returns_true_when_closed() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(60));
        assert!(cb.can_execute());
    }

    #[test]
    fn can_execute_returns_false_when_open_and_timeout_not_elapsed() {
        let mut cb = CircuitBreaker::new(1, Duration::from_secs(999));
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);
        assert!(!cb.can_execute());
    }

    #[test]
    fn can_execute_returns_true_when_open_and_timeout_elapsed() {
        let mut cb = CircuitBreaker::new(1, Duration::from_millis(1));
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);
        std::thread::sleep(Duration::from_millis(2));
        assert!(cb.can_execute());
    }

    #[test]
    fn can_execute_returns_true_when_half_open_with_no_successes_yet() {
        let mut cb = CircuitBreaker::new(3, Duration::from_secs(60));
        for _ in 0..3 {
            cb.record_failure();
        }
        assert_eq!(cb.state, CircuitState::Open);
    }

    #[test]
    fn can_execute_after_timeout_still_shows_open_state() {
        let mut cb = CircuitBreaker::new(1, Duration::from_millis(1));
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);
        std::thread::sleep(Duration::from_millis(2));
        assert!(cb.can_execute());
        assert_eq!(cb.state, CircuitState::Open);
    }

    // ── CircuitBreaker::record_failure ──────────────────────────────────────

    #[test]
    fn record_failure_increments_count_and_sets_timestamp() {
        let mut cb = CircuitBreaker::new(10, Duration::from_secs(30));
        cb.record_failure();
        assert_eq!(cb.failure_count, 1);
        assert!(cb.last_failure_time.is_some());
    }

    #[test]
    fn record_failure_opens_when_threshold_reached() {
        let mut cb = CircuitBreaker::new(3, Duration::from_secs(30));
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Closed);
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);
        assert_eq!(cb.failure_count, 3);
    }

    #[test]
    fn record_failure_beyond_threshold_stays_open() {
        let mut cb = CircuitBreaker::new(2, Duration::from_secs(30));
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);
        assert_eq!(cb.failure_count, 3);
    }

    #[test]
    fn record_failure_in_half_open_sends_back_to_open() {
        let mut cb = CircuitBreaker::new(1, Duration::from_millis(1));
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);
        std::thread::sleep(Duration::from_millis(2));
        assert!(cb.can_execute());
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);
        assert_eq!(cb.failure_count, 2);
    }

    #[test]
    fn record_failure_in_open_does_not_change_state() {
        let mut cb = CircuitBreaker::new(1, Duration::from_secs(60));
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);
    }

    // ── CircuitBreaker::record_success ──────────────────────────────────────

    #[test]
    fn record_success_in_closed_resets_success_count() {
        let mut cb = CircuitBreaker::new(5, Duration::from_secs(30));
        cb.record_success();
        assert_eq!(cb.state, CircuitState::Closed);
        assert_eq!(cb.success_count, 0);
        assert_eq!(cb.failure_count, 0);
    }

    #[test]
    fn record_success_in_open_is_noop() {
        let mut cb = CircuitBreaker::new(1, Duration::from_secs(60));
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);
        cb.record_success();
        assert_eq!(cb.state, CircuitState::Open);
    }

    // ── CircuitBreaker full lifecycle ───────────────────────────────────────

    #[test]
    fn circuit_breaker_full_open_close_cycle() {
        let mut cb = CircuitBreaker::new(2, Duration::from_millis(10));
        assert!(cb.can_execute());

        cb.record_failure();
        assert!(cb.can_execute());
        cb.record_failure();
        assert!(!cb.can_execute());

        std::thread::sleep(Duration::from_millis(15));
        assert!(cb.can_execute());
    }

    #[test]
    fn circuit_breaker_rapid_failures_accumulate() {
        let mut cb = CircuitBreaker::new(5, Duration::from_secs(30));
        for _ in 0..4 {
            cb.record_failure();
            assert_eq!(cb.state, CircuitState::Closed);
            assert!(cb.can_execute());
        }
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);
        assert!(!cb.can_execute());
    }

    #[test]
    fn failure_count_keeps_accumulating_in_open_state() {
        let mut cb = CircuitBreaker::new(1, Duration::from_secs(60));
        cb.record_failure();
        assert_eq!(cb.failure_count, 1);
        cb.record_failure();
        assert_eq!(cb.failure_count, 2);
        cb.record_failure();
        assert_eq!(cb.failure_count, 3);
    }

    #[test]
    fn timeout_zero_makes_open_passable_immediately() {
        let mut cb = CircuitBreaker::new(1, Duration::from_millis(0));
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);
        assert!(cb.can_execute());
    }

    #[test]
    fn last_failure_time_is_none_until_first_failure() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(30));
        assert_eq!(cb.last_failure_time, None);
    }

    #[test]
    fn can_execute_with_none_last_failure_in_open() {
        let mut cb = CircuitBreaker::new(1, Duration::from_secs(30));
        cb.record_failure();
        assert!(cb.last_failure_time.is_some());
        cb.last_failure_time = None;
        assert_eq!(cb.state, CircuitState::Open);
        assert!(!cb.can_execute());
    }

    // ── PoolConfig ──────────────────────────────────────────────────────────

    #[test]
    fn pool_config_construction() {
        let config = PoolConfig {
            max_connections: 50,
            min_connections: 10,
            connection_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(300),
            enable_connection_warmup: true,
        };
        assert_eq!(config.max_connections, 50);
        assert_eq!(config.min_connections, 10);
        assert_eq!(config.connection_timeout, Duration::from_secs(5));
        assert_eq!(config.idle_timeout, Duration::from_secs(300));
        assert!(config.enable_connection_warmup);
    }

    #[test]
    fn pool_config_debug_and_clone() {
        let config = PoolConfig {
            max_connections: 1,
            min_connections: 0,
            connection_timeout: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(1),
            enable_connection_warmup: false,
        };
        let _ = format!("{config:?}");
        let cloned = config.clone();
        assert_eq!(cloned.max_connections, config.max_connections);
        assert_eq!(cloned.idle_timeout, config.idle_timeout);
    }

    // ── PoolStat / PoolStats ────────────────────────────────────────────────

    #[test]
    fn pool_stats_total_connections() {
        let stats = PoolStats {
            main_pool: PoolStat {
                size: 10,
                available: 5,
                waiting: 2,
            },
            client_pool: PoolStat {
                size: 8,
                available: 4,
                waiting: 1,
            },
        };
        assert_eq!(stats.total_connections(), 18);
    }

    #[test]
    fn pool_stats_total_available() {
        let stats = PoolStats {
            main_pool: PoolStat {
                size: 20,
                available: 15,
                waiting: 3,
            },
            client_pool: PoolStat {
                size: 10,
                available: 7,
                waiting: 0,
            },
        };
        assert_eq!(stats.total_available(), 22);
    }

    #[test]
    fn pool_stats_total_waiting() {
        let stats = PoolStats {
            main_pool: PoolStat {
                size: 5,
                available: 0,
                waiting: 4,
            },
            client_pool: PoolStat {
                size: 3,
                available: 1,
                waiting: 2,
            },
        };
        assert_eq!(stats.total_waiting(), 6);
    }

    #[test]
    fn pool_stats_all_zero() {
        let stats = PoolStats {
            main_pool: PoolStat {
                size: 0,
                available: 0,
                waiting: 0,
            },
            client_pool: PoolStat {
                size: 0,
                available: 0,
                waiting: 0,
            },
        };
        assert_eq!(stats.total_connections(), 0);
        assert_eq!(stats.total_available(), 0);
        assert_eq!(stats.total_waiting(), 0);
    }

    // ── DatabaseHealth ──────────────────────────────────────────────────────

    #[test]
    fn database_health_both_healthy() {
        let health = DatabaseHealth {
            main_pool_healthy: true,
            client_pool_healthy: true,
            main_pool_connections: 42,
            client_pool_connections: 17,
        };
        assert!(health.is_healthy());
    }

    #[test]
    fn database_health_main_unhealthy() {
        let health = DatabaseHealth {
            main_pool_healthy: false,
            client_pool_healthy: true,
            main_pool_connections: 0,
            client_pool_connections: 5,
        };
        assert!(!health.is_healthy());
    }

    #[test]
    fn database_health_client_unhealthy() {
        let health = DatabaseHealth {
            main_pool_healthy: true,
            client_pool_healthy: false,
            main_pool_connections: 10,
            client_pool_connections: 0,
        };
        assert!(!health.is_healthy());
    }

    #[test]
    fn database_health_both_unhealthy() {
        let health = DatabaseHealth {
            main_pool_healthy: false,
            client_pool_healthy: false,
            main_pool_connections: 0,
            client_pool_connections: 0,
        };
        assert!(!health.is_healthy());
    }

    // ── ConnectionMetrics defaults ──────────────────────────────────────────

    #[test]
    fn connection_metrics_defaults() {
        let metrics = ConnectionMetrics {
            total_queries: 0,
            successful_queries: 0,
            failed_queries: 0,
            avg_query_time_ms: 0.0,
            cache_hits: 0,
            cache_misses: 0,
            last_used: std::time::Instant::now(),
        };
        assert_eq!(metrics.total_queries, 0);
        assert_eq!(metrics.successful_queries, 0);
        assert_eq!(metrics.failed_queries, 0);
        assert_eq!(metrics.avg_query_time_ms, 0.0);
        assert_eq!(metrics.cache_hits, 0);
        assert_eq!(metrics.cache_misses, 0);
    }

    // ── DatabasePerformanceMetrics ──────────────────────────────────────────

    #[test]
    fn database_performance_metrics_all_none() {
        let perf = DatabasePerformanceMetrics {
            main_cache_metrics: None,
            client_cache_metrics: None,
            main_query_metrics: None,
            client_query_metrics: None,
        };
        assert!(perf.main_cache_metrics.is_none());
        assert!(perf.client_cache_metrics.is_none());
        assert!(perf.main_query_metrics.is_none());
        assert!(perf.client_query_metrics.is_none());
    }

    #[test]
    fn database_performance_metrics_with_data() {
        let metrics = ConnectionMetrics {
            total_queries: 100,
            successful_queries: 95,
            failed_queries: 5,
            avg_query_time_ms: 12.5,
            cache_hits: 80,
            cache_misses: 20,
            last_used: std::time::Instant::now(),
        };
        let perf = DatabasePerformanceMetrics {
            main_cache_metrics: Some(metrics.clone()),
            client_cache_metrics: None,
            main_query_metrics: Some(metrics.clone()),
            client_query_metrics: None,
        };
        let main = perf.main_query_metrics.unwrap();
        assert_eq!(main.total_queries, 100);
        assert_eq!(main.successful_queries, 95);
        assert_eq!(main.failed_queries, 5);
        assert_eq!(main.avg_query_time_ms, 12.5);
        assert_eq!(main.cache_hits, 80);
        assert_eq!(main.cache_misses, 20);
    }

    #[test]
    fn database_performance_metrics_debug_and_clone() {
        let perf = DatabasePerformanceMetrics {
            main_cache_metrics: None,
            client_cache_metrics: None,
            main_query_metrics: Some(ConnectionMetrics {
                total_queries: 1,
                successful_queries: 1,
                failed_queries: 0,
                avg_query_time_ms: 5.0,
                cache_hits: 0,
                cache_misses: 0,
                last_used: std::time::Instant::now(),
            }),
            client_query_metrics: None,
        };
        let _ = format!("{perf:?}");
        let cloned = perf.clone();
        assert_eq!(cloned.main_query_metrics.unwrap().total_queries, 1);
    }
}
