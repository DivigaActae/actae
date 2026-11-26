// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

// Authentication & authorization
pub mod advanced_cache;
pub mod api_key;
pub mod audit_logger;
pub mod authorization;
pub mod federation;
pub mod jwt;
pub mod local_keys;
pub mod password_hash;
pub mod rate_limiter;
pub mod session;
pub mod session_store;
pub mod setup;
pub mod source_policy;
pub mod types;
pub mod websocket_auth;

pub use crate::database::models::User;
use crate::database::DatabaseManager;
use anyhow::Result;
pub use authorization::{parse_api_key_permissions, ApiKeyPermissions};
use chrono::{DateTime, Utc};
use sha2::Sha256;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

/// Whether loopback dashboard auto-login is enabled. On by default; an operator
/// who wants the dashboard to require a sign-in even locally sets
/// `ACTAE_LOCAL_AUTO_LOGIN=0`. Used by the auto-login endpoint and the startup
/// banner (which tailors the admin-login line accordingly).
pub fn local_dashboard_auto_login_enabled() -> bool {
    !matches!(
        std::env::var("ACTAE_LOCAL_AUTO_LOGIN")
            .ok()
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref(),
        Some("0") | Some("false") | Some("no") | Some("off")
    )
}

/// Hash an API key using HMAC-SHA256 with the given secret.
/// This replaces bare SHA-256 to defeat rainbow-table attacks.
/// The secret should be a high-entropy value (e.g. the JWT secret).
/// Returns a 64-character hex string.
pub fn hmac_hash_api_key(key: &str, secret: &[u8]) -> String {
    use hmac::{Hmac, Mac};
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(key.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Authentication error types
#[derive(Debug, Clone)]
pub enum AuthError {
    DatabaseError(String),
    InvalidCredentials,
    TokenExpired,
    InvalidToken,
    InsufficientPermissions,
    RateLimitExceeded,
    ValidationError(String),
    InvalidProvider(String),
    InvalidState,
    ExpiredState,
    TokenExchangeFailed(String),
    UserInfoFailed(String),
    InvalidUserData,
}

/// Authentication result wrapper
pub type AuthResult<T> = Result<T, AuthError>;

impl std::error::Error for AuthError {}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::DatabaseError(msg) => write!(f, "Database error: {msg}"),
            AuthError::InvalidCredentials => write!(f, "Invalid credentials"),
            AuthError::TokenExpired => write!(f, "Token expired"),
            AuthError::InvalidToken => write!(f, "Invalid token"),
            AuthError::InsufficientPermissions => write!(f, "Insufficient permissions"),
            AuthError::RateLimitExceeded => write!(f, "Rate limit exceeded"),
            AuthError::ValidationError(msg) => write!(f, "Validation error: {msg}"),
            AuthError::InvalidProvider(msg) => write!(f, "Invalid provider: {msg}"),
            AuthError::InvalidState => write!(f, "Invalid state"),
            AuthError::ExpiredState => write!(f, "Expired state"),
            AuthError::TokenExchangeFailed(msg) => write!(f, "Token exchange failed: {msg}"),
            AuthError::UserInfoFailed(msg) => write!(f, "User info failed: {msg}"),
            AuthError::InvalidUserData => write!(f, "Invalid user data"),
        }
    }
}

impl From<crate::database::error::DatabaseError> for AuthError {
    fn from(err: crate::database::error::DatabaseError) -> Self {
        AuthError::DatabaseError(err.to_string())
    }
}

impl From<tokio_postgres::Error> for AuthError {
    fn from(err: tokio_postgres::Error) -> Self {
        AuthError::DatabaseError(err.to_string())
    }
}

impl From<sonic_rs::Error> for AuthError {
    fn from(err: sonic_rs::Error) -> Self {
        AuthError::ValidationError(err.to_string())
    }
}

/// Main authentication manager
pub struct AuthManager {
    api_key_manager: Arc<EnhancedApiKeyManager>,
    session_manager: Arc<session::SessionManager>,
}

impl AuthManager {
    pub fn new(db_manager: Arc<DatabaseManager>, hmac_secret: Vec<u8>) -> Self {
        Self {
            api_key_manager: Arc::new(EnhancedApiKeyManager::new(
                Arc::clone(&db_manager),
                hmac_secret,
            )),
            session_manager: Arc::new(session::SessionManager::new(
                chrono::Duration::try_hours(1).unwrap_or_else(chrono::Duration::zero), // 1 hour default session duration
                10, // max 10 sessions per user
            )),
        }
    }

    pub fn api_key_manager(&self) -> &Arc<EnhancedApiKeyManager> {
        &self.api_key_manager
    }

    pub fn session_manager(&self) -> &Arc<session::SessionManager> {
        &self.session_manager
    }

    pub async fn validate_session(&self, token: &str) -> Result<Option<session::Session>> {
        self.session_manager.validate_session(token).await
    }

    /// Authenticate WebSocket connection using token or API key
    pub async fn authenticate_websocket(
        &self,
        token: Option<String>,
        api_key: Option<String>,
        _ip_address: Option<std::net::IpAddr>,
        _user_agent: Option<String>,
    ) -> Result<websocket_auth::AuthResult, AuthError> {
        // Try JWT token first
        if let Some(jwt_token) = token {
            match jwt::validate_token(&jwt_token, &Default::default()) {
                Ok(claims) => {
                    return Ok(websocket_auth::AuthResult {
                        user_id: claims.sub,
                        auth_method: websocket_auth::AuthMethod::JWT,
                        permissions: None,
                        expires_at: Some(
                            DateTime::from_timestamp(claims.exp, 0).unwrap_or(Utc::now()),
                        ),
                        key_source: None,
                        key_id: None,
                        federation_operation_id: None,
                        federation_action: None,
                        federation_instance_id: None,
                    });
                }
                Err(_) => {
                    // Token validation failed, continue to API key
                }
            }
        }

        // Try API key
        if let Some(key) = api_key {
            let validation = self
                .api_key_manager
                .validate_api_key(&key, "read", "*")
                .await?;

            if validation.is_valid {
                return Ok(websocket_auth::AuthResult {
                    user_id: validation.user_id.unwrap_or_default(),
                    auth_method: websocket_auth::AuthMethod::ApiKey,
                    permissions: validation
                        .permissions
                        .map(|p| sonic_rs::to_value(&p).unwrap_or_default()),
                    expires_at: None,
                    key_source: validation.source,
                    key_id: None,
                    federation_operation_id: None,
                    federation_action: None,
                    federation_instance_id: None,
                });
            }
        }

        Err(AuthError::InvalidCredentials)
    }
}

/// Enhanced API key manager with scoped permissions and rate limiting
pub struct EnhancedApiKeyManager {
    db_manager: Arc<DatabaseManager>,
    rate_limiter: Arc<ApiKeyRateLimiter>,
    hmac_secret: Vec<u8>,
}

impl EnhancedApiKeyManager {
    pub fn new(db_manager: Arc<DatabaseManager>, hmac_secret: Vec<u8>) -> Self {
        Self {
            db_manager,
            rate_limiter: Arc::new(ApiKeyRateLimiter::new()),
            hmac_secret,
        }
    }

    /// Validate API key with rate limiting and permission checking
    pub async fn validate_api_key(
        &self,
        key: &str,
        requested_permission: &str,
        resource: &str,
    ) -> Result<ApiKeyValidation, AuthError> {
        // Check rate limiting first
        if !self.rate_limiter.check_rate_limit(key).await? {
            return Ok(ApiKeyValidation {
                is_valid: false,
                user_id: None,
                permissions: None,
                error: Some("Rate limit exceeded".to_string()),
                source: None,
            });
        }

        // Get API key details
        let api_key = match self.get_api_key_by_hash(&self.hash_api_key(key)).await? {
            Some(key) => key,
            None => {
                return Ok(ApiKeyValidation {
                    is_valid: false,
                    user_id: None,
                    permissions: None,
                    error: Some("Invalid API key".to_string()),
                    source: None,
                });
            }
        };

        // Check if key is active
        if api_key.status != "active" {
            return Ok(ApiKeyValidation {
                is_valid: false,
                user_id: Some(api_key.user_id),
                permissions: None,
                error: Some("API key is not active".to_string()),
                source: Some(api_key.source),
            });
        }

        // Check if key is expired
        if let Some(expires_at) = api_key.expires_at {
            if expires_at <= Utc::now() {
                return Ok(ApiKeyValidation {
                    is_valid: false,
                    user_id: Some(api_key.user_id),
                    permissions: None,
                    error: Some("API key has expired".to_string()),
                    source: Some(api_key.source),
                });
            }
        }

        // Check permissions
        let has_permission = self
            .check_permission(&api_key.permissions, requested_permission, resource)
            .await?;

        if !has_permission {
            return Ok(ApiKeyValidation {
                is_valid: false,
                user_id: Some(api_key.user_id),
                permissions: Some(api_key.permissions),
                error: Some("Insufficient permissions".to_string()),
                source: Some(api_key.source),
            });
        }

        // Update last used timestamp
        self.update_last_used(&api_key.id).await?;

        Ok(ApiKeyValidation {
            is_valid: true,
            user_id: Some(api_key.user_id),
            permissions: Some(api_key.permissions),
            error: None,
            source: Some(api_key.source),
        })
    }

    /// Check if API key has specific permission for resource.
    /// Delegates to the shared authorization service (audit item 2/9/12).
    async fn check_permission(
        &self,
        permissions: &ApiKeyPermissions,
        requested_permission: &str,
        resource: &str,
    ) -> Result<bool, AuthError> {
        Ok(permissions.check(requested_permission, resource))
    }

    /// Hash API key for storage using HMAC-SHA256.
    fn hash_api_key(&self, key: &str) -> String {
        hmac_hash_api_key(key, &self.hmac_secret)
    }

    /// Get API key by hash
    async fn get_api_key_by_hash(&self, key_hash: &str) -> Result<Option<ApiKey>, AuthError> {
        let client = self.db_manager.get_client_connection().await?;

        let rows = client
            .query(
                "SELECT id, user_id, name, key_hash, key_prefix, permissions, 
             last_used_at, expires_at, status, created_at, updated_at, source
             FROM api_keys 
             WHERE key_hash = $1",
                &[&key_hash],
            )
            .await?;

        if let Some(row) = rows.first() {
            let permissions_str: String = row.get("permissions");
            let permissions: ApiKeyPermissions = sonic_rs::from_str(&permissions_str)
                .map_err(|e| AuthError::DatabaseError(e.to_string()))?;

            Ok(Some(ApiKey {
                id: row.get("id"),
                user_id: row.get("user_id"),
                name: row.get("name"),
                key_hash: row.get("key_hash"),
                key_prefix: row.get("key_prefix"),
                permissions,
                last_used_at: row.get("last_used_at"),
                expires_at: row.get("expires_at"),
                status: row.get("status"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
                source: row.get("source"),
            }))
        } else {
            Ok(None)
        }
    }

    /// Update last used timestamp
    async fn update_last_used(&self, key_id: &str) -> Result<(), AuthError> {
        let client = self.db_manager.get_client_connection().await?;

        client
            .execute(
                "UPDATE api_keys SET last_used_at = NOW() WHERE id = $1",
                &[&key_id],
            )
            .await?;

        Ok(())
    }
}

/// API key rate limiter
pub struct ApiKeyRateLimiter {
    limits: Arc<RwLock<HashMap<String, RateLimitState>>>,
}

impl Default for ApiKeyRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl ApiKeyRateLimiter {
    pub fn new() -> Self {
        Self {
            limits: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check if API key is within rate limits
    pub async fn check_rate_limit(&self, key: &str) -> Result<bool, AuthError> {
        let now = Utc::now();
        let mut limits = self.limits.write().await;

        let state = limits
            .entry(key.to_string())
            .or_insert_with(|| RateLimitState {
                requests: 0,
                window_start: now,
                last_request: now,
            });

        // Reset window if needed (1 minute windows)
        if now - state.window_start > chrono::Duration::minutes(1) {
            state.requests = 0;
            state.window_start = now;
        }

        // Check limit (100 requests per minute)
        if state.requests >= 100 {
            return Ok(false);
        }

        // Update state
        state.requests += 1;
        state.last_request = now;

        Ok(true)
    }
}

/// Rate limit state for API keys
#[derive(Debug, Clone)]
struct RateLimitState {
    requests: u32,
    window_start: DateTime<Utc>,
    last_request: DateTime<Utc>,
}

// `ApiKeyPermissions` lives in `authorization.rs` and is re-exported here
// (`pub use authorization::{ApiKeyPermissions, parse_api_key_permissions}`)
// so existing imports keep working. It is the single source of truth for
// permission parsing and pattern matching used by every transport.

/// API key validation result
#[derive(Debug, Clone)]
pub struct ApiKeyValidation {
    pub is_valid: bool,
    pub user_id: Option<String>,
    pub permissions: Option<ApiKeyPermissions>,
    pub error: Option<String>,
    /// Key origin: `control_plane` or `local`. `None` when validation
    /// failed before the key could be resolved.
    pub source: Option<String>,
}

/// API key data structure
#[derive(Debug, Clone)]
pub struct ApiKey {
    pub id: String,
    pub user_id: String,
    pub name: String,
    pub key_hash: String,
    pub key_prefix: String,
    pub permissions: ApiKeyPermissions,
    pub last_used_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Key origin: `control_plane` (subscription-enforced) or `local`
    /// (created on the host; also subject to enforcement).
    pub source: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- hmac_hash_api_key ---

    #[test]
    fn test_hmac_hash_deterministic() {
        let key = "my-api-key-123";
        let secret = b"my-secret-key-at-least-32-bytes-long-for-hmac!!";
        let hash1 = hmac_hash_api_key(key, secret);
        let hash2 = hmac_hash_api_key(key, secret);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hmac_hash_different_keys_different_hashes() {
        let secret = b"my-secret-key-at-least-32-bytes-long-for-hmac!!";
        let hash1 = hmac_hash_api_key("key-alpha", secret);
        let hash2 = hmac_hash_api_key("key-beta", secret);
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_hmac_hash_different_secrets_different_hashes() {
        let key = "same-api-key";
        let hash1 = hmac_hash_api_key(key, b"secret-one-that-is-long-enough-for-test!!");
        let hash2 = hmac_hash_api_key(key, b"secret-two-that-is-long-enough-for-test!!");
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_hmac_hash_output_is_64_char_hex() {
        let key = "test-key";
        let secret = b"my-secret-key-at-least-32-bytes-long-for-hmac!!";
        let hash = hmac_hash_api_key(key, secret);
        assert_eq!(hash.len(), 64);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_hmac_hash_empty_key_produces_consistent_hash() {
        let secret = b"my-secret-key-at-least-32-bytes-long-for-hmac!!";
        let hash = hmac_hash_api_key("", secret);
        assert_eq!(hash.len(), 64);
    }

    #[test]
    fn test_hmac_hash_empty_secret_succeeds() {
        let hash = hmac_hash_api_key("test-key", b"");
        assert_eq!(hash.len(), 64);
    }

    // --- AuthError Display ---

    #[test]
    fn test_auth_error_display_database_error() {
        let err = AuthError::DatabaseError("connection refused".to_string());
        assert_eq!(format!("{err}"), "Database error: connection refused");
    }

    #[test]
    fn test_auth_error_display_invalid_credentials() {
        let err = AuthError::InvalidCredentials;
        assert_eq!(format!("{err}"), "Invalid credentials");
    }

    #[test]
    fn test_auth_error_display_token_expired() {
        let err = AuthError::TokenExpired;
        assert_eq!(format!("{err}"), "Token expired");
    }

    #[test]
    fn test_auth_error_display_invalid_token() {
        let err = AuthError::InvalidToken;
        assert_eq!(format!("{err}"), "Invalid token");
    }

    #[test]
    fn test_auth_error_display_insufficient_permissions() {
        let err = AuthError::InsufficientPermissions;
        assert_eq!(format!("{err}"), "Insufficient permissions");
    }

    #[test]
    fn test_auth_error_display_rate_limit_exceeded() {
        let err = AuthError::RateLimitExceeded;
        assert_eq!(format!("{err}"), "Rate limit exceeded");
    }

    #[test]
    fn test_auth_error_display_validation_error() {
        let err = AuthError::ValidationError("bad input".to_string());
        assert_eq!(format!("{err}"), "Validation error: bad input");
    }

    #[test]
    fn test_auth_error_display_invalid_provider() {
        let err = AuthError::InvalidProvider("google".to_string());
        assert_eq!(format!("{err}"), "Invalid provider: google");
    }

    #[test]
    fn test_auth_error_display_invalid_state() {
        let err = AuthError::InvalidState;
        assert_eq!(format!("{err}"), "Invalid state");
    }

    #[test]
    fn test_auth_error_display_expired_state() {
        let err = AuthError::ExpiredState;
        assert_eq!(format!("{err}"), "Expired state");
    }

    #[test]
    fn test_auth_error_display_token_exchange_failed() {
        let err = AuthError::TokenExchangeFailed("timeout".to_string());
        assert_eq!(format!("{err}"), "Token exchange failed: timeout");
    }

    #[test]
    fn test_auth_error_display_user_info_failed() {
        let err = AuthError::UserInfoFailed("network error".to_string());
        assert_eq!(format!("{err}"), "User info failed: network error");
    }

    #[test]
    fn test_auth_error_display_invalid_user_data() {
        let err = AuthError::InvalidUserData;
        assert_eq!(format!("{err}"), "Invalid user data");
    }

    // --- AuthError is std::error::Error ---

    #[test]
    fn test_auth_error_implements_std_error() {
        fn assert_error<E: std::error::Error>() {}
        assert_error::<AuthError>();
    }

    // --- AuthResult type alias ---

    #[test]
    fn test_auth_result_type_alias() {
        let ok_result: AuthResult<i32> = Ok(42);
        assert_eq!(ok_result.unwrap(), 42);

        let err_result: AuthResult<i32> = Err(AuthError::InvalidCredentials);
        assert!(err_result.is_err());
    }

    // --- ApiKeyPermissions ---

    #[test]
    fn test_api_key_permissions_default() {
        // Fail-closed default: no grants.
        let perms = ApiKeyPermissions::default();
        assert!(perms.read.is_empty());
        assert!(perms.write.is_empty());
        assert!(perms.delete.is_empty());
        assert!(perms.admin.is_empty());
        assert!(!perms.check("read", "anything"));
    }

    #[test]
    fn test_api_key_permissions_full_access() {
        let perms = ApiKeyPermissions::full();
        assert!(perms.check("read", "any"));
        assert!(perms.check("write", "any"));
        assert!(perms.check("admin", "any"));
    }

    // --- ApiKeyRateLimiter ---

    #[tokio::test]
    async fn test_api_key_rate_limiter_allows_first_request() {
        let limiter = ApiKeyRateLimiter::new();
        let result = limiter.check_rate_limit("test-key").await.unwrap();
        assert!(result);
    }

    #[tokio::test]
    async fn test_api_key_rate_limiter_multiple_requests() {
        let limiter = ApiKeyRateLimiter::new();
        for _ in 0..5 {
            assert!(limiter.check_rate_limit("burst-key").await.unwrap());
        }
    }

    #[tokio::test]
    async fn test_api_key_rate_limiter_different_keys_independent() {
        let limiter = ApiKeyRateLimiter::new();
        for _ in 0..100 {
            assert!(limiter.check_rate_limit("key-a").await.unwrap());
        }
        assert!(limiter.check_rate_limit("key-b").await.unwrap());
    }
}
