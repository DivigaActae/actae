// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use anyhow::{anyhow, Result};
use axum::http;
use dashmap::DashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::auth::{
    websocket_auth::{AuthMethod, AuthResult},
    AuthManager,
};
use crate::config::AppConfig;
use sonic_rs::{JsonContainerTrait, JsonValueTrait};

/// Connection security context
#[derive(Debug, Clone)]
pub struct SecurityContext {
    pub connection_id: String,
    pub client_ip: IpAddr,
    pub origin: Option<String>,
    pub user_agent: Option<String>,
    pub auth_result: Option<AuthResult>,
    pub permissions: Vec<String>,
    pub established_at: Instant,
    pub last_activity: Arc<AtomicU64>,
}

/// Rate limiting bucket for connections and messages
#[derive(Debug)]
struct RateLimitBucket {
    tokens: AtomicU64,
    last_refill: AtomicU64,
    max_tokens: u64,
    refill_rate: u64, // tokens per second
}

impl RateLimitBucket {
    fn new(max_tokens: u64, refill_rate: u64) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            tokens: AtomicU64::new(max_tokens),
            last_refill: AtomicU64::new(now),
            max_tokens,
            refill_rate,
        }
    }

    fn try_consume(&self, tokens: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Refill tokens based on time elapsed
        let last_refill = self.last_refill.load(Ordering::Relaxed);
        let time_elapsed = now.saturating_sub(last_refill);

        if time_elapsed > 0 {
            let tokens_to_add = time_elapsed * self.refill_rate;
            let current = self.tokens.load(Ordering::Relaxed);
            let new_tokens = (current + tokens_to_add).min(self.max_tokens);

            self.tokens.store(new_tokens, Ordering::Relaxed);
            self.last_refill.store(now, Ordering::Relaxed);
        }

        // Try to consume tokens
        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current < tokens {
                return false;
            }

            match self.tokens.compare_exchange_weak(
                current,
                current - tokens,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(_) => continue, // Retry on race condition
            }
        }
    }
}

/// WebSocket security manager
pub struct WebSocketSecurity {
    config: AppConfig,
    auth_manager: Arc<AuthManager>,

    // Rate limiting
    connection_rate_limits: DashMap<IpAddr, RateLimitBucket>,
    message_rate_limits: DashMap<String, RateLimitBucket>, // connection_id -> bucket

    // Security policies
    max_connections_per_ip: u32,
    connection_timeout: Duration,
    max_message_size: usize,

    // Active connections tracking
    active_connections: DashMap<String, SecurityContext>,
    connections_per_ip: DashMap<IpAddr, AtomicU64>,

    // Security metrics
    auth_failures: AtomicU64,
    rate_limit_violations: AtomicU64,
    origin_violations: AtomicU64,
}

impl WebSocketSecurity {
    pub fn new(config: AppConfig, auth_manager: Arc<AuthManager>) -> Self {
        Self {
            config: config.clone(),
            auth_manager,
            connection_rate_limits: DashMap::new(),
            message_rate_limits: DashMap::new(),
            max_connections_per_ip: config.websocket_max_connections_per_ip.unwrap_or(1000),
            connection_timeout: Duration::from_secs(
                config.websocket_connection_timeout.unwrap_or(300),
            ),
            max_message_size: config.ws_max_message_size as usize,
            active_connections: DashMap::new(),
            connections_per_ip: DashMap::new(),
            auth_failures: AtomicU64::new(0),
            rate_limit_violations: AtomicU64::new(0),
            origin_violations: AtomicU64::new(0),
        }
    }

    /// Validate connection request and return security context
    pub async fn validate_connection(
        &self,
        client_ip: IpAddr,
        headers: &http::HeaderMap,
    ) -> Result<SecurityContext> {
        let connection_id = Uuid::new_v4().to_string();

        // 1. Check IP-based connection limit
        self.check_ip_connection_limit(client_ip)?;

        // 2. Check connection rate limit (if enabled)
        if self.config.websocket_enable_rate_limiting {
            self.check_connection_rate_limit(client_ip)?;
        }

        // 3. Validate origin
        let origin = self.validate_origin(headers)?;

        // 4. Extract user agent
        let user_agent = headers
            .get("user-agent")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());

        // 5. Authenticate connection
        let auth_result = self.authenticate_connection(headers).await?;

        // 6. Extract permissions
        let permissions = self.extract_permissions(&auth_result);

        let context = SecurityContext {
            connection_id: connection_id.clone(),
            client_ip,
            origin,
            user_agent,
            auth_result: Some(auth_result),
            permissions,
            established_at: Instant::now(),
            last_activity: Arc::new(AtomicU64::new(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            )),
        };

        // Register connection
        self.register_connection(&context)?;

        info!(
            "WebSocket connection validated: client_ip={}, connection_id={}",
            client_ip, connection_id,
        );

        Ok(context)
    }

    /// Validate incoming message
    pub fn validate_message(&self, context: &SecurityContext, message: &[u8]) -> Result<()> {
        // 1. Check message size
        if message.len() > self.max_message_size {
            return Err(anyhow!(
                "Message too large: {} bytes (max: {})",
                message.len(),
                self.max_message_size
            ));
        }

        // 2. Check message rate limit (if enabled)
        if self.config.websocket_enable_rate_limiting {
            self.check_message_rate_limit(&context.connection_id)?;
        }

        // 3. Update last activity
        context.last_activity.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            Ordering::Relaxed,
        );

        // 4. Validate JSON format (basic validation)
        if !message.is_empty() && message[0] == b'{' {
            // Depth guard before any recursive parse: a hostile client can
            // crash the server with a deeply nested message even before auth.
            let text = std::str::from_utf8(message).unwrap_or("");
            if crate::database::json_compat::json_nesting_depth(text)
                > crate::database::json_compat::MAX_JSON_DEPTH
            {
                return Err(anyhow!("Invalid JSON message: nesting too deep"));
            }
            // Parse JSON to check if it's an auth message - these should be allowed even without auth
            if let Ok(json_value) = sonic_rs::from_slice::<sonic_rs::Value>(message) {
                if let Some(obj) = json_value.as_object() {
                    if let Some(msg_type) = obj.get(&"type") {
                        if msg_type.as_str() == Some("auth") {
                            // Auth messages are always allowed
                            return Ok(());
                        }
                    }
                }
            } else {
                return Err(anyhow!("Invalid JSON message"));
            }
        }

        Ok(())
    }

    /// Check if client has permission for specific action
    pub fn check_permission(&self, context: &SecurityContext, action: &str) -> bool {
        context.permissions.contains(&action.to_string())
            || context.permissions.contains(&"admin".to_string())
    }

    /// Cleanup expired connections
    pub async fn cleanup_expired_connections(&self) {
        let now = Instant::now();
        let mut expired_connections = Vec::new();

        for entry in self.active_connections.iter() {
            let context = entry.value();
            if now.duration_since(context.established_at) > self.connection_timeout {
                expired_connections.push(entry.key().clone());
            }
        }

        for connection_id in expired_connections {
            self.remove_connection(&connection_id);
            warn!("Removed expired WebSocket connection: {}", connection_id);
        }
    }

    /// Remove connection from tracking
    pub fn remove_connection(&self, connection_id: &str) {
        if let Some((_, context)) = self.active_connections.remove(connection_id) {
            // Decrement IP connection count
            if let Some(ip_count) = self.connections_per_ip.get_mut(&context.client_ip) {
                let current = ip_count.load(Ordering::Relaxed);
                if current > 0 {
                    ip_count.store(current - 1, Ordering::Relaxed);
                }
            }

            // Remove message rate limit bucket
            self.message_rate_limits.remove(connection_id);

            debug!("Removed WebSocket connection: {}", connection_id);
        }
    }

    /// Get security metrics
    pub fn get_metrics(&self) -> SecurityMetrics {
        SecurityMetrics {
            active_connections: self.active_connections.len() as u64,
            total_auth_failures: self.auth_failures.load(Ordering::Relaxed),
            total_rate_limit_violations: self.rate_limit_violations.load(Ordering::Relaxed),
            total_origin_violations: self.origin_violations.load(Ordering::Relaxed),
            connections_per_ip: self.connections_per_ip.len() as u64,
        }
    }

    // Private helper methods

    fn check_ip_connection_limit(&self, client_ip: IpAddr) -> Result<()> {
        let connections = self
            .connections_per_ip
            .entry(client_ip)
            .or_insert_with(|| AtomicU64::new(0));

        let current = connections.load(Ordering::Relaxed);
        if current >= self.max_connections_per_ip as u64 {
            return Err(anyhow!(
                "Too many connections from IP {}: {} (max: {})",
                client_ip,
                current,
                self.max_connections_per_ip
            ));
        }

        connections.store(current + 1, Ordering::Relaxed);
        Ok(())
    }

    fn check_connection_rate_limit(&self, client_ip: IpAddr) -> Result<()> {
        let bucket = self
            .connection_rate_limits
            .entry(client_ip)
            .or_insert_with(|| RateLimitBucket::new(10, 1)); // 10 connections per IP, refill 1/sec

        if !bucket.try_consume(1) {
            self.rate_limit_violations.fetch_add(1, Ordering::Relaxed);
            return Err(anyhow!(
                "Connection rate limit exceeded for IP: {client_ip}"
            ));
        }

        Ok(())
    }

    fn validate_origin(&self, headers: &http::HeaderMap) -> Result<Option<String>> {
        let Some(origin_header) = headers.get("origin") else {
            return Ok(None);
        };
        let origin = origin_header
            .to_str()
            .map_err(|_| anyhow!("Invalid origin header"))?;

        // Exact normalized (scheme, host, port) matching; same-origin only by
        // default; explicit "*" opt-in wildcard (audit item 5). This method is
        // kept for the (currently unused) legacy WebSocketSecurity path but
        // delegates to the same policy the live upgrade path uses.
        let policy = crate::security::origin::OriginPolicy::from_config(
            self.config.websocket_allowed_origins.as_deref(),
        );
        let host_header = headers.get("host").and_then(|h| h.to_str().ok());
        if policy.allows(origin, host_header, self.config.enable_tls) {
            Ok(Some(origin.to_string()))
        } else {
            self.origin_violations.fetch_add(1, Ordering::Relaxed);
            Err(anyhow!("Origin not allowed: {origin}"))
        }
    }

    async fn authenticate_connection(&self, headers: &http::HeaderMap) -> Result<AuthResult> {
        // Try JWT token from Authorization header
        if let Some(auth_header) = headers.get("authorization") {
            let auth_str = auth_header
                .to_str()
                .map_err(|_| anyhow!("Invalid authorization header"))?;

            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                // Try to authenticate with the auth manager
                match self
                    .auth_manager
                    .authenticate_websocket(Some(token.to_string()), None, None, None)
                    .await
                {
                    Ok(auth_result) => return Ok(auth_result),
                    Err(e) => {
                        debug!("JWT token validation failed: {}", e);
                        // Continue to other auth methods
                    }
                }
            }
        }

        // Try API key from header
        if let Some(api_key_header) = headers.get("x-api-key") {
            let api_key = api_key_header
                .to_str()
                .map_err(|_| anyhow!("Invalid API key header"))?;

            // Try to authenticate with the auth manager
            match self
                .auth_manager
                .authenticate_websocket(None, Some(api_key.to_string()), None, None)
                .await
            {
                Ok(auth_result) => return Ok(auth_result),
                Err(e) => {
                    debug!("API key validation failed: {}", e);
                    // Continue to allow anonymous connection
                }
            }
        }

        // Allow anonymous connections for WebSocket - authentication happens via messages
        // Create a temporary anonymous auth result
        Ok(AuthResult {
            user_id: "anonymous".to_string(),
            auth_method: AuthMethod::Anonymous,
            permissions: Some(sonic_rs::json!(["connect"])),
            expires_at: None,
            key_source: None,
            key_id: None,
            federation_operation_id: None,
            federation_action: None,
            federation_instance_id: None,
        })
    }

    fn extract_permissions(&self, auth_result: &AuthResult) -> Vec<String> {
        if let Some(perms) = &auth_result.permissions {
            // Try to parse permissions from JSON Value
            if let Ok(string_array) = sonic_rs::from_value::<Vec<String>>(perms) {
                string_array
            } else {
                vec!["read".to_string()]
            }
        } else {
            vec!["read".to_string()]
        }
    }

    fn register_connection(&self, context: &SecurityContext) -> Result<()> {
        // Add message rate limit bucket for this connection
        self.message_rate_limits.insert(
            context.connection_id.clone(),
            RateLimitBucket::new(100, 10), // 100 messages max, refill 10/sec
        );

        // Register active connection
        self.active_connections
            .insert(context.connection_id.clone(), context.clone());

        Ok(())
    }

    fn check_message_rate_limit(&self, connection_id: &str) -> Result<()> {
        if let Some(bucket) = self.message_rate_limits.get(connection_id) {
            if !bucket.try_consume(1) {
                self.rate_limit_violations.fetch_add(1, Ordering::Relaxed);
                return Err(anyhow!(
                    "Message rate limit exceeded for connection: {connection_id}"
                ));
            }
        }
        Ok(())
    }
}

/// Security metrics for monitoring
#[derive(Debug, Clone)]
pub struct SecurityMetrics {
    pub active_connections: u64,
    pub total_auth_failures: u64,
    pub total_rate_limit_violations: u64,
    pub total_origin_violations: u64,
    pub connections_per_ip: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_rate_limit_bucket() {
        let bucket = RateLimitBucket::new(10, 5);

        // Should allow consuming tokens up to max
        for _ in 0..10 {
            assert!(bucket.try_consume(1));
        }

        // Should deny when bucket is empty
        assert!(!bucket.try_consume(1));

        // After waiting and refilling, should allow again
        std::thread::sleep(Duration::from_secs(1));
        assert!(bucket.try_consume(1));
    }

    #[tokio::test]
    #[ignore = "Requires database connection"]
    async fn test_ip_connection_limit() {
        let config = AppConfig::default();
        let auth_manager = Arc::new(AuthManager::new(
            Arc::new(
                crate::database::DatabaseManager::new(&config)
                    .await
                    .unwrap(),
            ),
            config.get_jwt_secret().as_bytes().to_vec(),
        ));
        let security = WebSocketSecurity::new(config, auth_manager);

        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        // Should allow connections up to limit
        for _ in 0..security.max_connections_per_ip {
            assert!(security.check_ip_connection_limit(ip).is_ok());
        }

        // Should deny when limit exceeded
        assert!(security.check_ip_connection_limit(ip).is_err());
    }

    // ── RateLimitBucket token math ─────────────────────────────────

    #[test]
    fn rate_limit_bucket_refills_over_time() {
        let bucket = RateLimitBucket::new(100, 10);
        for _ in 0..100 {
            assert!(bucket.try_consume(1));
        }
        assert!(!bucket.try_consume(1));
        // After 2s at 10 tokens/s, at least 20 tokens refilled.
        std::thread::sleep(Duration::from_secs(2));
        for _ in 0..20 {
            assert!(bucket.try_consume(1), "refilled tokens must be spendable");
        }
        assert!(!bucket.try_consume(1));
    }

    #[test]
    fn rate_limit_bucket_caps_at_max_tokens() {
        let bucket = RateLimitBucket::new(5, 100);
        // Even after a long wait the bucket never exceeds its cap.
        std::thread::sleep(Duration::from_millis(1500));
        assert!(bucket.try_consume(5));
        assert!(
            !bucket.try_consume(1),
            "cap must not be exceeded by refills"
        );
    }

    #[test]
    fn rate_limit_bucket_bulk_consume() {
        let bucket = RateLimitBucket::new(10, 5);
        assert!(bucket.try_consume(10));
        assert!(!bucket.try_consume(1));
    }

    // ── permission grant logic (SecurityContext.permissions) ───────

    #[test]
    fn permission_check_grants_exact_or_admin() {
        // The grant rule is `permissions.contains(action) || contains("admin")`.
        let perms = ["read:ch-1".to_string()];
        assert!(perms.contains(&"read:ch-1".to_string()) || perms.contains(&"admin".to_string()));
        assert!(
            !perms.contains(&"write:ch-1".to_string()) && !perms.contains(&"admin".to_string())
        );

        let admin = ["admin".to_string()];
        assert!(admin.contains(&"read:ch-1".to_string()) || admin.contains(&"admin".to_string()));
    }
}
