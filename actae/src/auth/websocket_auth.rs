// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use crate::auth::{
    jwt,
    rate_limiter::{AuthRateLimiter, RateLimitConfig},
};
use crate::database::repositories::Repository;
use crate::state::AppState;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sonic_rs::Value;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Authentication result containing user and context information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthResult {
    pub user_id: String,
    pub auth_method: AuthMethod,
    pub permissions: Option<Value>,
    pub expires_at: Option<DateTime<Utc>>,
    /// Key origin for `ApiKey` auth: `control_plane` or `local`.
    /// `None` for every other auth method.
    pub key_source: Option<String>,
    /// The `api_keys.id` of the authenticating key (`ApiKey` auth only).
    /// Used for revocation: cache invalidation and socket reauthorization
    /// key on this id, NOT `user_id` (which is the SaaS customer id and may
    /// be shared by many keys — audit item 11).
    pub key_id: Option<String>,
    /// The canonical operation id a federation `instance_command` token
    /// authorizes (`Federation` auth only; `None` for observation tokens).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation_operation_id: Option<String>,
    /// The exact action a federation principal may perform (`Federation` auth
    /// only): `read` for observation, the granted action for command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation_action: Option<String>,
    /// The instance id the federation token was issued for (`Federation` auth
    /// only) — used for audit and diagnostics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation_instance_id: Option<String>,
}

impl AuthResult {
    /// Convert to the shared `AuthContext` consumed by handlers, carrying the
    /// parsed resource-scoped permissions (audit items 2 and 3). Anonymous
    /// handshakes become `AuthContext::Anonymous`; API keys keep their real
    /// scopes + source + expiry; JWT/dashboard principals get full access;
    /// federation principals become read-only observation or single-operation
    /// command contexts (Phase 4).
    pub fn to_auth_context(&self) -> crate::auth::types::AuthContext {
        use crate::auth::types::AuthContext;
        match self.auth_method {
            AuthMethod::ApiKey => {
                let permissions = self
                    .permissions
                    .as_ref()
                    .map(crate::auth::parse_api_key_permissions)
                    .unwrap_or_default();
                AuthContext::ApiKey {
                    key_id: self.key_id.clone().unwrap_or_else(|| self.user_id.clone()),
                    key_source: self
                        .key_source
                        .clone()
                        .unwrap_or_else(|| "control_plane".to_string()),
                    expires_at: self.expires_at,
                    permissions,
                }
            }
            AuthMethod::Anonymous => AuthContext::Anonymous,
            AuthMethod::Federation => {
                let permissions = self
                    .permissions
                    .as_ref()
                    .map(crate::auth::parse_api_key_permissions)
                    .unwrap_or_default();
                let instance_id = self.federation_instance_id.clone().unwrap_or_default();
                match &self.federation_operation_id {
                    Some(op) => AuthContext::FederatedCommand {
                        principal: self.user_id.clone(),
                        instance_id,
                        operation_id: op.clone(),
                        action: self.federation_action.clone().unwrap_or_default(),
                        permissions,
                        request_digest: None,
                    },
                    None => AuthContext::FederatedObservation {
                        principal: self.user_id.clone(),
                        instance_id,
                        permissions,
                    },
                }
            }
            _ => AuthContext::Authenticated {
                user_id: self.user_id.clone(),
                permissions: crate::auth::authorization::ApiKeyPermissions::full(),
            },
        }
    }
}

/// The control-plane subscription lock rejected an authentication attempt.
/// Carries the customer-facing reason (e.g. "Subscription is not active…"),
/// which is safe to send to the client — it is the same message HTTP 402
/// returns, not an internal error detail.
#[derive(Debug)]
pub struct SubscriptionBlocked(pub String);

impl std::fmt::Display for SubscriptionBlocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SubscriptionBlocked {}

/// Authentication method used
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuthMethod {
    #[serde(rename = "jwt")]
    JWT,
    #[serde(rename = "api_key")]
    ApiKey,
    #[serde(rename = "session")]
    Session,
    /// Federated purpose-bound credential (`instance_observation` /
    /// `instance_command`), Phase 4.
    #[serde(rename = "federation")]
    Federation,
    #[serde(rename = "anonymous")]
    Anonymous,
}

impl std::fmt::Display for AuthMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::JWT => write!(f, "jwt"),
            Self::ApiKey => write!(f, "api_key"),
            Self::Session => write!(f, "session"),
            Self::Federation => write!(f, "federation"),
            Self::Anonymous => write!(f, "anonymous"),
        }
    }
}

/// Cached authentication result for performance
#[derive(Debug, Clone)]
struct CachedAuthResult {
    result: AuthResult,
    cached_at: Instant,
    ttl: Duration,
}

impl CachedAuthResult {
    fn new(result: AuthResult, ttl: Duration) -> Self {
        Self {
            result,
            cached_at: Instant::now(),
            ttl,
        }
    }

    fn is_expired(&self) -> bool {
        self.cached_at.elapsed() > self.ttl
    }
}

/// A credential whose `expires_at` is in the past is treated as expired even
/// when its cache entry TTL has not elapsed (audit item 11).
fn credential_expired(result: &AuthResult) -> bool {
    matches!(result.expires_at, Some(exp) if exp <= chrono::Utc::now())
}

/// High-performance WebSocket authenticator with caching and rate limiting
pub struct WebSocketAuthenticator {
    app_state: AppState,
    auth_cache: Arc<RwLock<HashMap<String, CachedAuthResult>>>,
    /// Secondary index `api_keys.id → cache keys` so a revoked key can evict
    /// its cached auth results immediately (audit item 11).
    auth_cache_index: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    cache_ttl: Duration,
    max_cache_size: usize,
    rate_limiter: Arc<AuthRateLimiter>,
}

impl WebSocketAuthenticator {
    /// Create a new WebSocket authenticator
    pub fn new(app_state: AppState) -> Self {
        let rate_limiter = Arc::new(AuthRateLimiter::new(RateLimitConfig::default()));

        Self {
            app_state,
            auth_cache: Arc::new(RwLock::new(HashMap::new())),
            auth_cache_index: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl: Duration::from_secs(300), // 5 minutes cache
            max_cache_size: 10000,
            rate_limiter,
        }
    }

    /// Create authenticator with custom rate limit config
    pub fn with_rate_limiting(app_state: AppState, rate_config: RateLimitConfig) -> Self {
        let rate_limiter = Arc::new(AuthRateLimiter::new(rate_config));

        Self {
            app_state,
            auth_cache: Arc::new(RwLock::new(HashMap::new())),
            auth_cache_index: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl: Duration::from_secs(300),
            max_cache_size: 10000,
            rate_limiter,
        }
    }

    /// Start the authenticator (initializes rate limiter)
    pub async fn start(&mut self) -> Result<()> {
        Arc::get_mut(&mut self.rate_limiter)
            .context("Failed to get mutable reference to rate limiter")?
            .start()
            .await
            .context("Failed to start rate limiter")?;

        info!("WebSocket authenticator started with rate limiting");
        Ok(())
    }

    /// Authenticate a WebSocket connection with JWT token, API key, or session token
    pub async fn authenticate(
        &self,
        token: Option<String>,
        api_key: Option<String>,
        ip_address: Option<IpAddr>,
        user_agent: Option<String>,
    ) -> Result<AuthResult> {
        let mut auth_success = false;
        let mut auth_result: Option<AuthResult> = None;
        let mut user_id_for_rate_limit: Option<String> = None;

        // Try JWT token first
        if let Some(jwt_token) = token {
            match self.authenticate_jwt(&jwt_token).await {
                Ok(result) => {
                    auth_success = true;
                    user_id_for_rate_limit = Some(result.user_id.clone());
                    auth_result = Some(result);
                }
                Err(e) => {
                    debug!("JWT authentication failed: {}", e);
                    // Purpose-bound federation tokens are JWTs too (PR-004);
                    // they MUST be resolved as federation BEFORE the session
                    // path, so an observation/command token never falls into
                    // a lenient "session" stub and loses its federated
                    // identity (source-policy transform would be skipped and
                    // the token could gain unintended permissions).
                    match self.authenticate_federation(&jwt_token).await {
                        Ok(result) => {
                            auth_success = true;
                            user_id_for_rate_limit = Some(result.user_id.clone());
                            auth_result = Some(result);
                        }
                        Err(e) => {
                            debug!("Federation authentication failed: {}", e);
                            // Try a real session token (validated against the
                            // session store — never an unconditional grant).
                            match self.authenticate_session(&jwt_token).await {
                                Ok(result) => {
                                    auth_success = true;
                                    user_id_for_rate_limit = Some(result.user_id.clone());
                                    auth_result = Some(result);
                                }
                                Err(e) => {
                                    debug!("Session authentication failed: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Try API key if no token auth succeeded
        if auth_result.is_none() {
            if let Some(key) = api_key {
                match self.authenticate_api_key(&key).await {
                    Ok(result) => {
                        auth_success = true;
                        user_id_for_rate_limit = Some(result.user_id.clone());
                        auth_result = Some(result);
                    }
                    Err(e) => {
                        debug!("API key authentication failed: {}", e);
                    }
                }
            }
        }

        // Check rate limiting before proceeding
        let rate_limit_result = self
            .rate_limiter
            .check_auth_attempt(
                ip_address,
                user_id_for_rate_limit.clone(),
                auth_success,
                user_agent,
            )
            .await
            .context("Rate limit check failed")?;

        if rate_limit_result.is_blocked() {
            return Err(anyhow!(
                "Rate limit exceeded. Too many authentication attempts."
            ));
        }

        // Apply progressive delay if configured
        if let Some(delay) = rate_limit_result.get_delay_secs().checked_sub(0) {
            if delay > 0 {
                warn!("Applying authentication delay of {} seconds", delay);
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
            }
        }

        // Return auth result or handle anonymous access
        if let Some(result) = auth_result.as_ref() {
            // Enforce the control-plane subscription lock for API-key and
            // anonymous authentication. JWT (dashboard) is never blocked.
            if let Some(reason) = self.enforcement_block(result) {
                return Err(anyhow!(SubscriptionBlocked(reason)));
            }
            Ok((*result).clone())
        } else if !self.app_state.config.websocket_require_auth
            || self.app_state.config.enable_anonymous
        {
            // Check rate limiting for anonymous access too
            let anon_rate_result = self
                .rate_limiter
                .check_auth_attempt(ip_address, None, true, None)
                .await
                .context("Anonymous rate limit check failed")?;

            if anon_rate_result.is_blocked() {
                return Err(anyhow!("Rate limit exceeded for anonymous access"));
            }

            debug!("Creating anonymous authentication for WebSocket connection");
            let anon = self.create_anonymous_auth();
            if let Some(reason) = self.enforcement_block(&anon) {
                return Err(anyhow!(SubscriptionBlocked(reason)));
            }
            Ok(anon)
        } else {
            debug!("Authentication failed: no credentials provided and WebSocket authentication required");
            Err(anyhow!(
                "Authentication required: no valid credentials provided"
            ))
        }
    }

    /// Authenticate using JWT token
    async fn authenticate_jwt(&self, token: &str) -> Result<AuthResult> {
        // Validate JWT token
        let claims = jwt::validate_token(token, &self.app_state.config)
            .map_err(|e| anyhow!("Invalid JWT token: {e}"))?;

        // Server-side revocation: the session must have a live ledger row.
        // JWT auth happens once per connection and is not cached, so a revoked
        // session is rejected immediately on reconnect.
        let session_ok = match claims.jti.as_deref() {
            Some(jti) => {
                crate::auth::session_store::is_active(
                    &self.app_state.db_manager.client_pool,
                    jti,
                )
                .await
            }
            None => false,
        };
        if !session_ok {
            return Err(anyhow!("Session expired or signed out"));
        }

        // API-key sessions (dashboard "log in with your key") carry a SaaS
        // subject with no local users-table row — skip the lookup for them.
        if claims.auth_method.as_deref() == Some("api_key") {
            let auth_result = AuthResult {
                user_id: claims.sub,
                auth_method: AuthMethod::JWT,
                federation_action: None,
                federation_operation_id: None,
                federation_instance_id: None,
                permissions: None,
                expires_at: Some(
                    DateTime::from_timestamp(claims.exp as i64, 0).unwrap_or(Utc::now()),
                ),
                key_source: None,
                key_id: None,
            };
            info!("JWT (API-key session) authentication successful");
            return Ok(auth_result);
        }

        // Verify user exists in database
        let user = self
            .app_state
            .user_repo
            .get_by_id(&claims.sub)
            .await
            .map_err(|e| anyhow!("Database error: {e}"))?
            .ok_or_else(|| anyhow!("User not found"))?;

        let user_id = user.id.clone();
        let auth_result = AuthResult {
            user_id: user.id,
            auth_method: AuthMethod::JWT,
            federation_action: None,
            federation_operation_id: None,
            federation_instance_id: None,
            permissions: None,
            expires_at: Some(DateTime::from_timestamp(claims.exp as i64, 0).unwrap_or(Utc::now())),
            key_source: None,
            key_id: None,
        };

        // JWT auth is not cached (see above): the session ledger check must run
        // on every connection.
        info!("JWT authentication successful for user: {}", user_id);
        Ok(auth_result)
    }

    /// Authenticate using API key
    async fn authenticate_api_key(&self, key: &str) -> Result<AuthResult> {
        // Check cache first
        let cache_key = format!("api_key:{}", self.hash_token(key));
        if let Some(cached) = self.get_cached_auth(&cache_key).await {
            // Re-verify the key is still active/unexpired on every cached hit
            // so a revoked key fails fast (audit item 11). The cache avoids
            // the HMAC + full-row fetch + permissions parse; the status check
            // is one indexed SELECT. On failure, evict and re-resolve so the
            // cache cannot mask a revocation.
            if let Some(ref key_id) = cached.result.key_id {
                if self.key_is_active(key_id).await {
                    return Ok(cached.result);
                }
                self.invalidate_key(key_id).await;
            } else {
                return Ok(cached.result);
            }
        }

        // Resolve via the shared service (single hash/fallback implementation
        // shared with HTTP — audit item 4). It tries HMAC-SHA256 with
        // `api_key_hash_secret`, then raw SHA-256 for legacy keys, and only
        // returns active, unexpired keys.
        let Some(api_key) = crate::auth::authorization::resolve_api_key(key, &self.app_state)
            .await
            .map_err(|e| anyhow!("Database error: {e}"))?
        else {
            return Err(anyhow!("Invalid API key"));
        };

        // Update last used timestamp (fire and forget)
        let api_key_repo = self.app_state.api_key_repo.clone();
        let api_key_id = api_key.id.clone();
        tokio::spawn(async move {
            if let Err(e) = api_key_repo.update_last_used(&api_key_id).await {
                warn!("Failed to update API key last used timestamp: {}", e);
            }
        });

        // API keys are standalone identities: the key's user_id is the
        // SaaS customer id (or legacy provisioning id) and is used as the
        // authenticated identity directly — no Actae user row is required.
        let auth_result = AuthResult {
            user_id: api_key.user_id,
            auth_method: AuthMethod::ApiKey,
            federation_action: None,
            federation_operation_id: None,
            federation_instance_id: None,
            permissions: Some(sonic_rs::to_value(&api_key.permissions).unwrap_or_default()),
            expires_at: api_key.expires_at,
            key_source: Some(api_key.source),
            key_id: Some(api_key.id),
        };

        // Cache the result
        self.cache_auth_result(&cache_key, &auth_result).await;

        info!(
            "API key authentication successful for user: {}",
            auth_result.user_id
        );
        Ok(auth_result)
    }

    /// Authenticate using a purpose-bound federation token (Phase 4).
    /// `instance_observation` → read-only principal; `instance_command` →
    /// single-operation principal. Fails closed when federation is disabled,
    /// the token is for another instance, or verification fails.
    async fn authenticate_federation(&self, token: &str) -> Result<AuthResult> {
        let cache_key = format!("fed:{}", self.hash_token(token));
        if let Some(cached) = self.get_cached_auth(&cache_key).await {
            return Ok(cached.result);
        }

        let cfg = self.app_state.control_plane.federation_config();
        let instance_id = self.app_state.config.install_id.clone().unwrap_or_default();
        let claims = crate::auth::federation::verify(&cfg, token, &instance_id)
            .map_err(|e| anyhow!("invalid federation token: {e}"))?;

        let (permissions, action, operation_id) = match claims.purpose {
            crate::auth::federation::FederationPurpose::Observation => (
                sonic_rs::json!({"read": claims.channels, "write": [], "delete": [], "admin": []}),
                Some("read".to_string()),
                None,
            ),
            crate::auth::federation::FederationPurpose::Command => {
                let action = claims.action.clone().unwrap_or_default();
                let permissions = match action.as_str() {
                    "read" => sonic_rs::json!({"read": claims.resources}),
                    "write" => sonic_rs::json!({"write": claims.resources}),
                    _ => sonic_rs::json!({"delete": claims.resources}),
                };
                (permissions, Some(action), claims.operation_id.clone())
            }
            crate::auth::federation::FederationPurpose::FleetAccess => {
                // Canonical instances never accept customer-backend
                // fleet_access tokens (the gateway verifies those).
                return Err(anyhow!("fleet_access tokens are not accepted by instances"));
            }
        };

        let auth_result = AuthResult {
            user_id: claims.principal.clone(),
            auth_method: AuthMethod::Federation,
            permissions: Some(permissions),
            expires_at: None,
            key_source: None,
            key_id: None,
            federation_operation_id: operation_id,
            federation_action: action,
            federation_instance_id: Some(claims.instance.clone()),
        };
        self.cache_auth_result(&cache_key, &auth_result).await;
        info!(
            "Federation authentication successful for principal: {}",
            auth_result.user_id
        );
        Ok(auth_result)
    }

    /// Cheap indexed status check used to validate cached API-key auth.
    /// Fails closed on any DB error.
    async fn key_is_active(&self, key_id: &str) -> bool {
        let Ok(client) = self.app_state.db_manager.get_client_connection().await else {
            return false;
        };
        let row = client
            .query_opt(
                "SELECT 1 FROM public.api_keys
                 WHERE id = $1 AND status = 'active'
                   AND (expires_at IS NULL OR expires_at > NOW())",
                &[&key_id],
            )
            .await
            .unwrap_or(None);
        row.is_some()
    }

    /// Authenticate using session token
    async fn authenticate_session(&self, token: &str) -> Result<AuthResult> {
        // Check cache first
        let cache_key = format!("session:{}", self.hash_token(token));
        if let Some(cached) = self.get_cached_auth(&cache_key).await {
            return Ok(cached.result);
        }

        // Validate the session against the session store. A token that is not
        // a valid, unexpired session is REJECTED — never granted a catch-all
        // "session" identity (a federation/observation token or an arbitrary
        // string must not silently become a full-access session).
        let found = self
            .app_state
            .session_repo
            .find_by_token(token)
            .await
            .map_err(|e| anyhow!("session lookup failed: {e}"))?;
        let Some((user, session)) = found else {
            return Err(anyhow!("invalid or unknown session token"));
        };
        if session.expires_at <= Utc::now() {
            return Err(anyhow!("session token expired"));
        }

        // Dashboard sessions grant full access (they authenticate a real
        // logged-in user; consistent with the HTTP JWT path).
        let auth_result = AuthResult {
            user_id: user.id,
            auth_method: AuthMethod::Session,
            federation_action: None,
            federation_operation_id: None,
            federation_instance_id: None,
            permissions: None,
            expires_at: Some(session.expires_at),
            key_source: None,
            key_id: None,
        };

        // Cache the result
        self.cache_auth_result(&cache_key, &auth_result).await;

        info!(
            "Session authentication successful for user: {}",
            auth_result.user_id
        );
        Ok(auth_result)
    }

    /// Create anonymous authentication result
    fn create_anonymous_auth(&self) -> AuthResult {
        let anon_id = format!("anon_{}", Uuid::new_v4());
        AuthResult {
            user_id: anon_id,
            auth_method: AuthMethod::Anonymous,
            federation_action: None,
            federation_operation_id: None,
            federation_instance_id: None,
            permissions: None,
            expires_at: None,
            key_source: None,
            key_id: None,
        }
    }

    /// Enforce the control-plane subscription lock for API-key and
    /// anonymous authentication. Returns the block reason when the request
    /// must be rejected; `None` when allowed (standalone mode, active
    /// subscription, or JWT/session auth).
    fn enforcement_block(&self, result: &AuthResult) -> Option<String> {
        match result.auth_method {
            AuthMethod::ApiKey | AuthMethod::Federation => {
                // Federation credentials are data-plane credentials, not a
                // dashboard exemption: a lapsed subscription locks them like
                // API keys (only dashboard JWT/session stays exempt).
                self.app_state.control_plane.blocks_key().map(|r| r.message)
            }
            AuthMethod::Anonymous => self
                .app_state
                .control_plane
                .blocks_anonymous()
                .map(|r| r.message),
            _ => None,
        }
    }
    /// Hash token for cache key (shorter hash for cache)
    fn hash_token(&self, token: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(token.as_bytes());
        format!("{:x}", hasher.finalize())[..16].to_string()
    }

    /// Get cached authentication result.
    /// Enforces both the TTL and the credential `expires_at` (a cached
    /// ApiKey whose expiry has passed is treated as a miss).
    async fn get_cached_auth(&self, cache_key: &str) -> Option<CachedAuthResult> {
        let cache = self.auth_cache.read().await;
        if let Some(cached) = cache.get(cache_key) {
            if !cached.is_expired() && !credential_expired(&cached.result) {
                debug!("Cache hit for auth key: {}", cache_key);
                return Some(cached.clone());
            }
        }
        None
    }

    /// Cache authentication result
    async fn cache_auth_result(&self, cache_key: &str, auth_result: &AuthResult) {
        let mut cache = self.auth_cache.write().await;

        // Evict old entries if cache is full
        if cache.len() >= self.max_cache_size {
            // Remove expired entries first
            cache.retain(|_, cached| !cached.is_expired());

            // If still full, remove oldest entries
            if cache.len() >= self.max_cache_size {
                let keys_to_remove: Vec<String> =
                    cache.keys().take(cache.len() / 4).cloned().collect();
                for key in keys_to_remove {
                    cache.remove(&key);
                }
            }
        }

        cache.insert(
            cache_key.to_string(),
            CachedAuthResult::new(auth_result.clone(), self.cache_ttl),
        );

        // Maintain the key_id → cache-keys index so a revocation can evict
        // every cached auth for that key (audit item 11).
        if let Some(ref key_id) = auth_result.key_id {
            let mut index = self.auth_cache_index.write().await;
            index
                .entry(key_id.clone())
                .or_default()
                .insert(cache_key.to_string());
        }

        debug!("Cached auth result for key: {}", cache_key);
    }

    /// Evict every cached auth result for a revoked key (audit item 11).
    pub async fn invalidate_key(&self, key_id: &str) {
        let cache_keys: Vec<String> = {
            let mut index = self.auth_cache_index.write().await;
            let keys = index.remove(key_id).unwrap_or_default();
            keys.into_iter().collect()
        };
        if cache_keys.is_empty() {
            return;
        }
        let mut cache = self.auth_cache.write().await;
        for key in &cache_keys {
            cache.remove(key);
        }
        info!(
            "Invalidated {} cached auth entr{} for revoked key {}",
            cache_keys.len(),
            if cache_keys.len() == 1 { "y" } else { "ies" },
            key_id
        );
    }

    /// Clear authentication cache
    pub async fn clear_cache(&self) {
        let mut cache = self.auth_cache.write().await;
        cache.clear();
        let mut index = self.auth_cache_index.write().await;
        index.clear();
        info!("Authentication cache cleared");
    }

    /// Get cache statistics
    pub async fn get_cache_stats(&self) -> AuthCacheStats {
        let cache = self.auth_cache.read().await;
        let total_entries = cache.len();
        let expired_entries = cache.values().filter(|cached| cached.is_expired()).count();

        AuthCacheStats {
            total_entries,
            active_entries: total_entries - expired_entries,
            expired_entries,
            max_cache_size: self.max_cache_size,
            cache_ttl_seconds: self.cache_ttl.as_secs(),
        }
    }

    /// Get rate limiter statistics
    pub async fn get_rate_limiter_stats(
        &self,
    ) -> Result<crate::auth::rate_limiter::RateLimiterStats> {
        Ok(self.rate_limiter.get_stats().await)
    }

    /// Manually unblock an IP address
    pub async fn unblock_ip(&self, ip: IpAddr) -> Result<bool> {
        self.rate_limiter.unblock_ip(ip).await
    }

    /// Manually unblock a user
    pub async fn unblock_user(&self, user_id: &str) -> Result<bool> {
        self.rate_limiter.unblock_user(user_id).await
    }

    /// Get currently blocked IPs
    pub async fn get_blocked_ips(&self) -> Vec<IpAddr> {
        self.rate_limiter.get_blocked_ips().await
    }

    /// Get currently blocked users
    pub async fn get_blocked_users(&self) -> Vec<String> {
        self.rate_limiter.get_blocked_users().await
    }

    /// Shutdown the authenticator
    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(rate_limiter) = Arc::get_mut(&mut self.rate_limiter) {
            rate_limiter.shutdown().await?;
        }
        info!("WebSocket authenticator shutdown complete");
        Ok(())
    }
}

/// Authentication cache statistics
#[derive(Debug, Clone, Serialize)]
pub struct AuthCacheStats {
    pub total_entries: usize,
    pub active_entries: usize,
    pub expired_entries: usize,
    pub max_cache_size: usize,
    pub cache_ttl_seconds: u64,
}

/// Create a global authenticator instance
pub fn create_authenticator(app_state: AppState) -> WebSocketAuthenticator {
    WebSocketAuthenticator::new(app_state)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- AuthMethod Display ---

    #[test]
    fn test_auth_method_display_jwt() {
        assert_eq!(format!("{}", AuthMethod::JWT), "jwt");
    }

    #[test]
    fn test_auth_method_display_api_key() {
        assert_eq!(format!("{}", AuthMethod::ApiKey), "api_key");
    }

    #[test]
    fn test_auth_method_display_session() {
        assert_eq!(format!("{}", AuthMethod::Session), "session");
    }

    #[test]
    fn test_auth_method_display_anonymous() {
        assert_eq!(format!("{}", AuthMethod::Anonymous), "anonymous");
    }

    // --- AuthMethod serde round-trip ---

    #[test]
    fn test_auth_method_serde_roundtrip() {
        for method in &[
            AuthMethod::JWT,
            AuthMethod::ApiKey,
            AuthMethod::Session,
            AuthMethod::Anonymous,
        ] {
            let json = sonic_rs::to_string(method).unwrap();
            let deserialized: AuthMethod = sonic_rs::from_str(&json).unwrap();
            assert_eq!(format!("{deserialized}"), format!("{}", method));
        }
    }

    // --- AuthResult construction ---

    #[test]
    fn test_auth_result_construction() {
        let result = AuthResult {
            user_id: "user-42".to_string(),
            auth_method: AuthMethod::JWT,
            federation_action: None,
            federation_operation_id: None,
            federation_instance_id: None,
            permissions: None,
            expires_at: Some(Utc::now() + chrono::Duration::try_hours(1).unwrap()),
            key_source: None,
            key_id: None,
        };
        assert_eq!(result.user_id, "user-42");
    }

    #[test]
    fn test_auth_result_api_key_variant() {
        let result = AuthResult {
            user_id: "user-1".to_string(),
            auth_method: AuthMethod::ApiKey,
            federation_action: None,
            federation_operation_id: None,
            federation_instance_id: None,
            permissions: None,
            expires_at: None,
            key_source: Some("control_plane".to_string()),
            key_id: Some("k_1".to_string()),
        };
        assert!(result.expires_at.is_none());
        assert_eq!(result.key_source.as_deref(), Some("control_plane"));
        assert_eq!(result.key_id.as_deref(), Some("k_1"));
    }

    // --- CachedAuthResult ---

    #[test]
    fn test_cached_auth_result_not_expired_initially() {
        let auth_result = AuthResult {
            user_id: "u".to_string(),
            auth_method: AuthMethod::Anonymous,
            federation_action: None,
            federation_operation_id: None,
            federation_instance_id: None,
            permissions: None,
            expires_at: None,
            key_source: None,
            key_id: None,
        };
        let cached = CachedAuthResult::new(auth_result, Duration::from_secs(60));
        assert!(!cached.is_expired());
    }

    #[test]
    fn test_cached_auth_result_expired() {
        let auth_result = AuthResult {
            user_id: "u".to_string(),
            auth_method: AuthMethod::Anonymous,
            federation_action: None,
            federation_operation_id: None,
            federation_instance_id: None,
            permissions: None,
            expires_at: None,
            key_source: None,
            key_id: None,
        };
        let cached = CachedAuthResult::new(auth_result, Duration::from_millis(0));
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(cached.is_expired());
    }

    // --- AuthCacheStats ---

    #[test]
    fn test_auth_cache_stats_defaults() {
        let stats = AuthCacheStats {
            total_entries: 0,
            active_entries: 0,
            expired_entries: 0,
            max_cache_size: 10000,
            cache_ttl_seconds: 300,
        };
        assert_eq!(stats.total_entries, 0);
        assert_eq!(stats.max_cache_size, 10000);
    }

    // ── API key hash algorithm consistency ──
    // Keys created by EnhancedApiKeyManager use HMAC-SHA256.
    // Legacy Docker init.d keys use raw SHA-256.
    // The WS authenticator must try HMAC first, then SHA-256.
    // These tests verify the two hash methods produce DIFFERENT results
    // (proving both paths are distinct) and that the HTTP validate_api_key
    // function correctly tries both.

    #[test]
    fn test_hmac_and_sha256_produce_different_hashes() {
        let key = "diviga_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let secret = b"this-is-a-32-char-minimum-jwt-secret!!";

        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        let sha256_hash = format!("{:x}", hasher.finalize());

        let hmac_hash = crate::auth::hmac_hash_api_key(key, secret);

        assert_ne!(
            sha256_hash, hmac_hash,
            "HMAC-SHA256 and raw SHA-256 must produce different hashes for the same key"
        );
        assert_eq!(
            sha256_hash.len(),
            64,
            "SHA-256 should produce 64-char hex string"
        );
        assert_eq!(
            hmac_hash.len(),
            64,
            "HMAC-SHA256 should produce 64-char hex string"
        );
    }

    #[test]
    fn test_hmac_hash_is_deterministic() {
        let key = "actae_test_key_1234";
        let secret = b"this-is-a-32-char-minimum-jwt-secret!!";
        let hash1 = crate::auth::hmac_hash_api_key(key, secret);
        let hash2 = crate::auth::hmac_hash_api_key(key, secret);
        assert_eq!(
            hash1, hash2,
            "HMAC must produce the same hash for identical inputs"
        );
    }

    #[test]
    fn test_hmac_hash_depends_on_secret() {
        let key = "actae_test_key_1234";
        let secret1 = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let secret2 = b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let hash1 = crate::auth::hmac_hash_api_key(key, secret1);
        let hash2 = crate::auth::hmac_hash_api_key(key, secret2);
        assert_ne!(
            hash1, hash2,
            "HMAC with different secrets must produce different hashes"
        );
    }

    // ── Credential expiry ──

    #[test]
    fn test_credential_expired_rejects_past_expiry() {
        let result = AuthResult {
            user_id: "u".into(),
            auth_method: AuthMethod::ApiKey,
            federation_action: None,
            federation_operation_id: None,
            federation_instance_id: None,
            permissions: None,
            expires_at: Some(Utc::now() - chrono::Duration::seconds(1)),
            key_source: None,
            key_id: Some("k1".into()),
        };
        assert!(credential_expired(&result));
    }

    #[test]
    fn test_credential_expired_allows_future_or_none() {
        let future = AuthResult {
            user_id: "u".into(),
            auth_method: AuthMethod::ApiKey,
            federation_action: None,
            federation_operation_id: None,
            federation_instance_id: None,
            permissions: None,
            expires_at: Some(Utc::now() + chrono::Duration::hours(1)),
            key_source: None,
            key_id: Some("k1".into()),
        };
        assert!(!credential_expired(&future));

        let none = AuthResult {
            user_id: "u".into(),
            auth_method: AuthMethod::ApiKey,
            federation_action: None,
            federation_operation_id: None,
            federation_instance_id: None,
            permissions: None,
            expires_at: None,
            key_source: None,
            key_id: Some("k1".into()),
        };
        assert!(!credential_expired(&none));
    }

    // ── Cache invalidation on revocation (audit item 11) ──

    /// A dead AppState whose DB pools never connect — sufficient for the
    /// in-memory auth-cache tests below (no DB is touched).
    async fn dead_app_state() -> AppState {
        use crate::database::repositories::{ApiKeyRepository, SessionRepository, UserRepository};
        use deadpool_postgres::{Config, Runtime};
        use std::time::Duration;
        use tokio::sync::Semaphore;
        use tokio_postgres::NoTls;

        let app_config = crate::config::AppConfig::default();
        let mut cfg = Config::new();
        cfg.host = Some("localhost".to_string());
        cfg.port = Some(5432);
        cfg.user = Some("postgres".to_string());
        cfg.password = Some("postgres".to_string());
        cfg.dbname = Some("actae".to_string());
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();

        let db_manager = Arc::new(crate::database::DatabaseManager {
            main_pool: pool.clone(),
            client_pool: pool,
            pool_config: Arc::new(crate::database::PoolConfig {
                max_connections: 2,
                min_connections: 1,
                connection_timeout: Duration::from_secs(5),
                idle_timeout: Duration::from_secs(300),
                enable_connection_warmup: false,
            }),
            circuit_breakers: Arc::new(std::sync::RwLock::new(HashMap::new())),
            db_semaphore: Arc::new(Semaphore::new(200)),
        });

        let batch_broadcaster = Arc::new(
            crate::websocket::batch_broadcaster::BatchBroadcaster::new(
                crate::websocket::batch_broadcaster::BatchBroadcastConfig::default(),
            )
            .await
            .unwrap(),
        );

        AppState {
            config: Arc::new(app_config),
            db_manager: db_manager.clone(),
            user_repo: Arc::new(UserRepository::new(db_manager.clone())),
            api_key_repo: Arc::new(ApiKeyRepository::new(db_manager.clone())),
            session_repo: Arc::new(SessionRepository::new(db_manager.clone())),
            batch_broadcaster,
            password_hasher: Arc::new(
                crate::auth::password_hash::SecurePasswordHasher::with_secure_defaults(),
            ),
            control_plane: Arc::new(
                crate::control_plane::ControlPlaneManager::new(
                    Arc::new(crate::config::AppConfig::default()),
                    db_manager.client_pool.clone(),
                )
                .unwrap(),
            ),
            auth_manager: Arc::new(crate::auth::AuthManager::new(db_manager, vec![])),
        }
    }

    #[tokio::test]
    async fn test_invalidate_key_evicts_cached_auth() {
        let authenticator = WebSocketAuthenticator::new(dead_app_state().await);

        let api_key_result = AuthResult {
            user_id: "cus_1".into(),
            auth_method: AuthMethod::ApiKey,
            federation_action: None,
            federation_operation_id: None,
            federation_instance_id: None,
            permissions: Some(sonic_rs::json!({"read": ["*"]})),
            expires_at: None,
            key_source: Some("control_plane".into()),
            key_id: Some("k_revoked".into()),
        };
        let other_key_result = AuthResult {
            user_id: "cus_1".into(),
            auth_method: AuthMethod::ApiKey,
            federation_action: None,
            federation_operation_id: None,
            federation_instance_id: None,
            permissions: Some(sonic_rs::json!({"read": ["*"]})),
            expires_at: None,
            key_source: Some("control_plane".into()),
            key_id: Some("k_other".into()),
        };

        authenticator
            .cache_auth_result("api_key:hash_revoked", &api_key_result)
            .await;
        authenticator
            .cache_auth_result("api_key:hash_other", &other_key_result)
            .await;

        // Revoking k_revoked evicts only its own cache entry.
        authenticator.invalidate_key("k_revoked").await;
        assert!(authenticator
            .get_cached_auth("api_key:hash_revoked")
            .await
            .is_none());
        assert!(
            authenticator
                .get_cached_auth("api_key:hash_other")
                .await
                .is_some(),
            "other key's cached auth must survive"
        );
        // The index no longer tracks the revoked key.
        assert!(!authenticator
            .auth_cache_index
            .read()
            .await
            .contains_key("k_revoked"));
    }
}
