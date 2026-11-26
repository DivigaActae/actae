// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

pub mod cache;
pub mod monitor;
pub mod origin;
pub mod rate_limit;
pub mod validator;

// Phase 2: Performance-optimized security enhancements
pub mod advanced_cache;
pub mod batch_processor;

// Phase 3: Advanced hardening measures
pub mod crypto_security;
pub mod threat_detection;

use crate::auth::websocket_auth::{AuthMethod, AuthResult as WebSocketAuthResult};
use crate::config::AppConfig;
use anyhow::Result;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub use cache::{SecurityCache, SecurityCacheStats};
pub use monitor::{
    AuthFailureEvent, AuthSuccessEvent, ConnectionSecurityEvent, ConnectionSecurityType,
    IpBlockedEvent, PermissionDeniedEvent, RateLimitAction, RateLimitEvent, RateLimitType,
    SecurityEvent, SecurityMonitor, SecuritySeverity, SuspiciousActivityEvent,
    SuspiciousActivityType,
};
pub use rate_limit::{
    MultiRateLimiter, RateLimitConfig, RateLimitInfo, RateLimitResult, RateLimitStats,
    TokenBucketRateLimit,
};

/// Read an env var as u32, falling back to `default` on absence/parse error.
fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
pub use validator::{AnonymousAccessPolicy, ConnectionSecurityValidator, SecurityValidationResult};

// Phase 2 exports
pub use advanced_cache::{
    AdvancedCacheConfig, AdvancedSecurityCache, CacheMetrics as AdvancedCacheMetrics, CachedData,
};
pub use batch_processor::{
    AuthBatchEvent, BatchProcessorConfig, BatchProcessorMetrics, ConnectionBatchEvent,
    RateLimitBatchEvent, SecurityBatchProcessor,
};

// Phase 3 exports
pub use crypto_security::{
    CryptoAuditEvent, CryptoConfig, CryptoEventType, CryptoSecurityManager, CryptoStats,
    EncryptedData, SecureToken, TokenScope,
};
pub use threat_detection::{
    BehavioralViolation, DetectedAnomaly, DetectedAttack, ThreatAnalysisResult,
    ThreatDetectionConfig, ThreatDetectionMetrics, ThreatDetectionSystem, ThreatSeverity,
    ThreatSummary, ThreatType,
};

/// Comprehensive security manager that coordinates all security components
/// Now includes Phase 2 and Phase 3 enhancements
pub struct SecurityManager {
    // Phase 1: Core security components
    cache: Arc<SecurityCache>,
    monitor: Arc<SecurityMonitor>,
    rate_limiter: Arc<MultiRateLimiter>,
    validator: Arc<ConnectionSecurityValidator>,

    // Phase 2: Performance-optimized components
    batch_processor: Option<Arc<SecurityBatchProcessor>>,
    advanced_cache: Option<Arc<AdvancedSecurityCache>>,

    // Phase 3: Advanced hardening components
    crypto_manager: Option<Arc<CryptoSecurityManager>>,
    threat_detector: Option<Arc<ThreatDetectionSystem>>,

    config: Arc<AppConfig>,
}

impl SecurityManager {
    pub fn new(config: Arc<AppConfig>) -> Result<Self> {
        let cache = Arc::new(SecurityCache::new(
            2000,  // auth cache size
            10000, // permission cache size
            300,   // 5 minute TTL
        ));

        let monitor = Arc::new(SecurityMonitor::new()?);

        let rate_limit_config = RateLimitConfig {
            ip_connection_rate: config.websocket_max_connections_per_ip.unwrap_or(10),
            ip_connection_capacity: config.websocket_max_connections_per_ip.unwrap_or(50),
            ip_message_rate: config.websocket_max_message_rate.unwrap_or(100),
            ip_message_capacity: config.websocket_max_message_rate.unwrap_or(1000),
            ip_auth_rate: env_u32("ACTAE_IP_AUTH_RATE", 5),
            ip_auth_capacity: env_u32("ACTAE_IP_AUTH_CAPACITY", 20),
            user_message_rate: config.websocket_max_message_rate.unwrap_or(200),
            user_message_capacity: config.websocket_max_message_rate.unwrap_or(2000),
            user_subscription_rate: 10,
            user_subscription_capacity: 100,
            global_connection_rate: config.max_connections * 2, // Allow burst
            global_connection_capacity: config.max_connections * 5,
            global_message_rate: config.websocket_max_message_rate.unwrap_or(10000),
            global_message_capacity: config.websocket_max_message_rate.unwrap_or(100000),
            cleanup_interval_seconds: 300,
        };

        let rate_limiter = Arc::new(MultiRateLimiter::new(rate_limit_config));

        let validator = Arc::new(ConnectionSecurityValidator::new(config.clone()));

        // Phase 2: Performance-optimized components (optional)
        let batch_processor = if config.enable_batch_processing.unwrap_or(true) {
            Some(Arc::new(SecurityBatchProcessor::new(
                BatchProcessorConfig::default(),
                monitor.clone(),
            )?))
        } else {
            None
        };

        let advanced_cache = if config.enable_advanced_caching.unwrap_or(true) {
            Some(Arc::new(AdvancedSecurityCache::new(
                AdvancedCacheConfig::default(),
            )))
        } else {
            None
        };

        // Phase 3: Advanced hardening components (optional)
        let crypto_manager = if config.enable_crypto_security.unwrap_or(true) {
            Some(Arc::new(CryptoSecurityManager::new(
                CryptoConfig::default(),
            )?))
        } else {
            None
        };

        let threat_detector = if config.enable_threat_detection.unwrap_or(true) {
            Some(Arc::new(ThreatDetectionSystem::new(
                ThreatDetectionConfig::default(),
            )?))
        } else {
            None
        };

        Ok(Self {
            cache,
            monitor,
            rate_limiter,
            validator,
            batch_processor,
            advanced_cache,
            crypto_manager,
            threat_detector,
            config,
        })
    }

    /// Validate a new connection with comprehensive security checks
    pub async fn validate_connection(
        &self,
        connection_id: Uuid,
        ip: Option<IpAddr>,
        user_agent: Option<String>,
        origin: Option<String>,
        host_header: Option<String>,
    ) -> Result<SecurityValidationResult> {
        // Check IP blocklist/allowlist
        if let Some(ip) = ip {
            if self.cache.is_ip_blocked(ip) {
                let event = ConnectionSecurityEvent {
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    connection_id,
                    ip: Some(ip),
                    event_type: ConnectionSecurityType::ConnectionFromBlockedIp,
                    details: "Connection attempt from blocked IP".to_string(),
                };
                self.monitor.log_connection_security(event);
                return Ok(SecurityValidationResult::Blocked(
                    "IP address is blocked".to_string(),
                ));
            }

            if !self.cache.is_ip_allowed(ip) && self.config.enable_ip_allowlist {
                return Ok(SecurityValidationResult::Blocked(
                    "IP address not in allowlist".to_string(),
                ));
            }
        }

        // Check rate limits
        if let Some(ip) = ip {
            match self.rate_limiter.check_connection_limit(ip) {
                RateLimitResult::Limited(info) => {
                    let event = RateLimitEvent {
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                        ip: Some(ip),
                        user_id: None,
                        connection_id: Some(connection_id),
                        limit_type: RateLimitType::ConnectionRate,
                        current_rate: info.current_rate,
                        limit_threshold: info.limit_threshold,
                        action_taken: RateLimitAction::ConnectionDropped,
                    };
                    self.monitor.log_rate_limit_violation(event);
                    return Ok(SecurityValidationResult::RateLimited(info));
                }
                RateLimitResult::Allowed => {}
            }
        }

        // Validate connection security parameters
        self.validator
            .validate_connection_security(connection_id, ip, user_agent, origin, host_header)
            .await
    }

    /// Validate message with rate limiting and security checks
    pub async fn validate_message(
        &self,
        connection_id: Uuid,
        ip: Option<IpAddr>,
        user_id: Option<&str>,
        message_size: usize,
    ) -> Result<SecurityValidationResult> {
        // Check message rate limits
        match self.rate_limiter.check_message_limit(ip, user_id) {
            RateLimitResult::Limited(info) => {
                let event = RateLimitEvent {
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    ip,
                    user_id: user_id.map(|s| s.to_string()),
                    connection_id: Some(connection_id),
                    limit_type: RateLimitType::MessageRate,
                    current_rate: info.current_rate,
                    limit_threshold: info.limit_threshold,
                    action_taken: RateLimitAction::ConnectionThrottled,
                };
                self.monitor.log_rate_limit_violation(event);
                return Ok(SecurityValidationResult::RateLimited(info));
            }
            RateLimitResult::Allowed => {}
        }

        // Check for suspicious message patterns
        if message_size > 1024 * 1024 {
            // 1MB
            let event = SuspiciousActivityEvent {
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                connection_id,
                ip,
                user_id: user_id.map(|s| s.to_string()),
                activity_type: SuspiciousActivityType::ExcessiveMessageRate,
                details: format!("Large message size: {message_size} bytes"),
                severity: SecuritySeverity::Medium,
            };
            self.monitor.log_suspicious_activity(event);
        }

        Ok(SecurityValidationResult::Allowed)
    }

    /// Validate authentication attempt
    /// Pre-auth validation: lockout checks ONLY, without consuming the
    /// per-IP authentication rate-limit bucket.
    ///
    /// This is the HTTP request path (see `resolve_auth_context` in main.rs):
    /// the auth bucket must count only FAILED attempts, so a valid API key
    /// bursting above the failure threshold is never throttled as abuse. The
    /// bucket is consumed by the caller on the failure paths instead.
    ///
    /// Connection-based auth (WebSocket) keeps using `validate_auth_attempt`,
    /// which additionally consumes the bucket per attempt: every connection
    /// is a fresh authentication attempt, and a connection flood must be
    /// throttled regardless of outcome.
    pub async fn validate_auth_lockout(
        &self,
        ip: Option<IpAddr>,
        user_id: Option<&str>,
        auth_method: AuthMethod,
    ) -> Result<SecurityValidationResult> {
        // Check if IP is locked out
        if let Some(ip) = ip {
            let ip_key = format!("ip:{ip}");
            if self.cache.is_locked_out(&ip_key) {
                let event = AuthFailureEvent {
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    ip: Some(ip),
                    user_id: user_id.map(|s| s.to_string()),
                    connection_id: None,
                    auth_method,
                    reason: "IP address is locked out".to_string(),
                    user_agent: None,
                };
                self.monitor.log_auth_failure(event);
                return Ok(SecurityValidationResult::Blocked(
                    "IP address is temporarily locked".to_string(),
                ));
            }
        }

        // Check if user is locked out
        if let Some(user_id) = user_id {
            let user_key = format!("user:{user_id}");
            if self.cache.is_locked_out(&user_key) {
                let event = AuthFailureEvent {
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    ip,
                    user_id: Some(user_id.to_string()),
                    connection_id: None,
                    auth_method,
                    reason: "User account is locked out".to_string(),
                    user_agent: None,
                };
                self.monitor.log_auth_failure(event);
                return Ok(SecurityValidationResult::Blocked(
                    "User account is temporarily locked".to_string(),
                ));
            }
        }

        Ok(SecurityValidationResult::Allowed)
    }

    /// Validate an authentication attempt with lockout checks AND per-attempt
    /// consumption of the per-IP authentication rate-limit bucket.
    ///
    /// Used by connection-based auth (WebSocket), where each connection is
    /// one attempt: a connection flood must be throttled per IP regardless of
    /// whether the credentials are valid. HTTP request auth uses
    /// `validate_auth_lockout` instead — the bucket there counts failures
    /// only, so valid traffic is never throttled as abuse.
    pub async fn validate_auth_attempt(
        &self,
        ip: Option<IpAddr>,
        user_id: Option<&str>,
        auth_method: AuthMethod,
    ) -> Result<SecurityValidationResult> {
        if let SecurityValidationResult::Blocked(reason) =
            self.validate_auth_lockout(ip, user_id, auth_method).await?
        {
            return Ok(SecurityValidationResult::Blocked(reason));
        }

        // Check auth rate limits
        if let Some(ip) = ip {
            match self.rate_limiter.check_auth_limit(ip) {
                RateLimitResult::Limited(info) => {
                    return Ok(SecurityValidationResult::RateLimited(info));
                }
                RateLimitResult::Allowed => {}
            }
        }

        Ok(SecurityValidationResult::Allowed)
    }

    /// Record authentication failure
    pub fn record_auth_failure(
        &self,
        ip: Option<IpAddr>,
        user_id: Option<&str>,
        auth_method: AuthMethod,
        reason: String,
    ) {
        let lockout_threshold = self.config.auth_failure_lockout_threshold;
        let lockout_duration =
            Duration::from_secs(self.config.auth_failure_lockout_duration_seconds);

        // Track IP-based failures
        if let Some(ip) = ip {
            let ip_key = format!("ip:{ip}");
            let is_locked =
                self.cache
                    .record_auth_failure(&ip_key, lockout_threshold, lockout_duration);

            if is_locked {
                self.cache
                    .block_ip(ip, reason.clone(), Some(lockout_duration));
            }
        }

        // Track user-based failures
        if let Some(user_id) = user_id {
            let user_key = format!("user:{user_id}");
            self.cache
                .record_auth_failure(&user_key, lockout_threshold, lockout_duration);
        }

        // Log the failure
        let event = AuthFailureEvent {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            ip,
            user_id: user_id.map(|s| s.to_string()),
            connection_id: None,
            auth_method,
            reason,
            user_agent: None,
        };
        self.monitor.log_auth_failure(event);
    }

    /// Record successful authentication
    pub fn record_auth_success(
        &self,
        ip: Option<IpAddr>,
        user_id: String,
        auth_method: AuthMethod,
    ) {
        // Clear auth failures on successful login
        if let Some(ip) = ip {
            let ip_key = format!("ip:{ip}");
            self.cache.clear_auth_failures(&ip_key);
        }

        let user_key = format!("user:{user_id}");
        self.cache.clear_auth_failures(&user_key);

        // Log the success
        let event = AuthSuccessEvent {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            ip,
            user_id,
            connection_id: None,
            auth_method,
            user_agent: None,
            session_duration: None,
        };
        self.monitor.log_auth_success(event);
    }

    /// Get cached authentication result
    pub fn get_cached_auth(&self, token: &str) -> Option<WebSocketAuthResult> {
        self.cache.get_cached_auth(token)
    }

    /// Cache authentication result
    pub fn cache_auth(&self, token: &str, auth_result: WebSocketAuthResult) {
        self.cache.cache_auth(token, auth_result);
    }

    /// Get cached permission result
    pub fn get_cached_permission(
        &self,
        user_id: &str,
        resource: &str,
        action: &str,
    ) -> Option<bool> {
        self.cache.get_cached_permission(user_id, resource, action)
    }

    /// Cache permission result
    pub fn cache_permission(&self, user_id: &str, resource: &str, action: &str, allowed: bool) {
        self.cache
            .cache_permission(user_id, resource, action, allowed);
    }

    /// Cleanup old entries (call periodically)
    pub fn cleanup(&self) {
        self.cache.cleanup_expired();
        self.rate_limiter.cleanup_old_entries();
    }

    /// Get comprehensive security statistics (Enhanced with Phase 2 & 3)
    pub fn get_security_stats(&self) -> EnhancedSecurityStats {
        EnhancedSecurityStats {
            // Phase 1 stats
            cache_stats: self.cache.get_stats(),
            rate_limit_stats: self.rate_limiter.get_stats(),

            // Phase 2 stats
            batch_processor_stats: self.batch_processor.as_ref().map(|bp| bp.get_metrics()),
            advanced_cache_stats: self.advanced_cache.as_ref().map(|ac| ac.get_metrics()),

            // Phase 3 stats - sync versions for now
            crypto_stats: self
                .crypto_manager
                .as_ref()
                .map(|cm| cm.get_crypto_stats_sync()),
            threat_detection_stats: self.threat_detector.as_ref().map(|td| td.get_metrics()),
        }
    }

    /// Get comprehensive security statistics with async crypto stats
    pub async fn get_security_stats_async(&self) -> EnhancedSecurityStats {
        let crypto_stats = if let Some(ref crypto_manager) = self.crypto_manager {
            Some(crypto_manager.get_crypto_stats().await)
        } else {
            None
        };

        EnhancedSecurityStats {
            // Phase 1 stats
            cache_stats: self.cache.get_stats(),
            rate_limit_stats: self.rate_limiter.get_stats(),

            // Phase 2 stats
            batch_processor_stats: self.batch_processor.as_ref().map(|bp| bp.get_metrics()),
            advanced_cache_stats: self.advanced_cache.as_ref().map(|ac| ac.get_metrics()),

            // Phase 3 stats - async version
            crypto_stats,
            threat_detection_stats: self.threat_detector.as_ref().map(|td| td.get_metrics()),
        }
    }

    /// Enhanced authentication with advanced caching and threat detection
    pub async fn enhanced_authenticate(
        &self,
        token: &str,
        _context: &str,
    ) -> Result<Option<WebSocketAuthResult>> {
        // Try advanced cache first if available
        if let Some(ref advanced_cache) = self.advanced_cache {
            if let Some(CachedData::Auth(auth_result)) =
                advanced_cache.get(&format!("auth:{token}")).await
            {
                return Ok(Some(auth_result));
            }
        }

        // Fallback to standard cache
        let result = self.cache.get_cached_auth(token);

        // Cache in advanced cache if available and result found
        if let (Some(advanced_cache), Some(auth_result)) = (&self.advanced_cache, &result) {
            let _ = advanced_cache
                .set(
                    &format!("auth:{token}"),
                    CachedData::Auth(auth_result.clone()),
                    Some(std::time::Duration::from_secs(300)),
                )
                .await;
        }

        Ok(result)
    }

    /// Encrypt sensitive data using Phase 3 crypto manager
    pub async fn encrypt_sensitive_data(
        &self,
        data: &[u8],
        context: &str,
    ) -> Result<Option<EncryptedData>> {
        if let Some(ref crypto_manager) = self.crypto_manager {
            Ok(Some(crypto_manager.encrypt_data(data, context).await?))
        } else {
            Ok(None)
        }
    }

    /// Decrypt sensitive data using Phase 3 crypto manager
    pub async fn decrypt_sensitive_data(
        &self,
        encrypted_data: &EncryptedData,
    ) -> Result<Option<Vec<u8>>> {
        if let Some(ref crypto_manager) = self.crypto_manager {
            Ok(Some(crypto_manager.decrypt_data(encrypted_data).await?))
        } else {
            Ok(None)
        }
    }

    /// Create secure token using Phase 3 crypto manager
    pub async fn create_secure_token(
        &self,
        payload: &[u8],
        scope: TokenScope,
        ttl: std::time::Duration,
        permissions: Vec<String>,
    ) -> Result<Option<SecureToken>> {
        if let Some(ref crypto_manager) = self.crypto_manager {
            Ok(Some(
                crypto_manager
                    .create_secure_token(payload, scope, ttl, permissions)
                    .await?,
            ))
        } else {
            Ok(None)
        }
    }

    /// Analyze security event for threats using Phase 3 threat detection
    pub async fn analyze_threat(
        &self,
        event: &SecurityEvent,
    ) -> Result<Option<ThreatAnalysisResult>> {
        if let Some(ref threat_detector) = self.threat_detector {
            Ok(Some(threat_detector.analyze_security_event(event).await?))
        } else {
            Ok(None)
        }
    }

    /// Get active threats using Phase 3 threat detection
    pub async fn get_active_threats(&self) -> Result<Vec<ThreatSummary>> {
        if let Some(ref threat_detector) = self.threat_detector {
            Ok(threat_detector.get_active_threats().await)
        } else {
            Ok(Vec::new())
        }
    }

    /// Submit security event for batch processing (Phase 2)
    pub async fn submit_batch_auth_event(&self, event: AuthBatchEvent) -> Result<()> {
        if let Some(ref batch_processor) = self.batch_processor {
            batch_processor.submit_auth_event(event).await
        } else {
            Ok(())
        }
    }

    /// Submit rate limit event for batch processing (Phase 2)
    pub async fn submit_batch_rate_limit_event(&self, event: RateLimitBatchEvent) -> Result<()> {
        if let Some(ref batch_processor) = self.batch_processor {
            batch_processor.submit_rate_limit_event(event).await
        } else {
            Ok(())
        }
    }

    /// Get advanced cache hit ratios (Phase 2)
    pub fn get_advanced_cache_hit_ratios(&self) -> Option<(f64, f64)> {
        self.advanced_cache.as_ref().map(|ac| ac.get_hit_ratios())
    }

    /// Get crypto audit log (Phase 3)
    pub async fn get_crypto_audit_log(&self, limit: Option<usize>) -> Vec<CryptoAuditEvent> {
        if let Some(ref crypto_manager) = self.crypto_manager {
            crypto_manager.get_audit_log(limit).await
        } else {
            Vec::new()
        }
    }

    /// Get references to individual components for advanced usage
    pub fn cache(&self) -> &SecurityCache {
        &self.cache
    }

    pub fn monitor(&self) -> &SecurityMonitor {
        &self.monitor
    }

    pub fn rate_limiter(&self) -> &MultiRateLimiter {
        &self.rate_limiter
    }

    pub fn validator(&self) -> &ConnectionSecurityValidator {
        &self.validator
    }
}

#[derive(Debug, Clone)]
pub struct SecurityStats {
    pub cache_stats: SecurityCacheStats,
    pub rate_limit_stats: RateLimitStats,
}

#[derive(Debug, Clone)]
pub struct EnhancedSecurityStats {
    // Phase 1 statistics
    pub cache_stats: SecurityCacheStats,
    pub rate_limit_stats: RateLimitStats,

    // Phase 2 statistics
    pub batch_processor_stats: Option<BatchProcessorMetrics>,
    pub advanced_cache_stats: Option<AdvancedCacheMetrics>,

    // Phase 3 statistics
    pub crypto_stats: Option<CryptoStats>,
    pub threat_detection_stats: Option<ThreatDetectionMetrics>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn dummy_cache_stats() -> SecurityCacheStats {
        SecurityCacheStats {
            auth_cache_size: 10,
            auth_cache_capacity: 100,
            permission_cache_size: 20,
            permission_cache_capacity: 200,
            auth_failures_tracked: 5,
            ip_allowlist_size: 3,
            ip_blocklist_size: 2,
        }
    }

    fn dummy_rate_limit_stats() -> RateLimitStats {
        RateLimitStats {
            ip_connection_limiters: 5,
            ip_message_limiters: 5,
            ip_auth_limiters: 5,
            user_message_limiters: 3,
            user_subscription_limiters: 3,
            global_connection_tokens: 100,
            global_message_tokens: 500,
        }
    }

    #[test]
    fn test_security_stats_debug() {
        let stats = SecurityStats {
            cache_stats: dummy_cache_stats(),
            rate_limit_stats: dummy_rate_limit_stats(),
        };
        let _ = format!("{stats:?}");
    }

    #[test]
    fn test_enhanced_security_stats_all_none() {
        let stats = EnhancedSecurityStats {
            cache_stats: dummy_cache_stats(),
            rate_limit_stats: dummy_rate_limit_stats(),
            batch_processor_stats: None,
            advanced_cache_stats: None,
            crypto_stats: None,
            threat_detection_stats: None,
        };
        assert!(stats.batch_processor_stats.is_none());
        assert!(stats.crypto_stats.is_none());
    }

    #[test]
    fn test_enhanced_security_stats_with_data() {
        let stats = EnhancedSecurityStats {
            cache_stats: dummy_cache_stats(),
            rate_limit_stats: dummy_rate_limit_stats(),
            batch_processor_stats: Some(BatchProcessorMetrics::default()),
            advanced_cache_stats: Some(AdvancedCacheMetrics::default()),
            crypto_stats: Some(CryptoStats {
                active_contexts: 2,
                total_encryptions: 50,
                total_decryptions: 30,
                security_violations: 0,
                master_key_age_hours: 6,
                last_key_rotation: std::time::SystemTime::now(),
            }),
            threat_detection_stats: Some(ThreatDetectionMetrics::default()),
        };
        assert!(stats.batch_processor_stats.is_some());
        assert!(stats.crypto_stats.is_some());
        assert_eq!(stats.crypto_stats.as_ref().unwrap().active_contexts, 2);
    }

    #[test]
    fn test_enhanced_security_stats_clone() {
        let stats = EnhancedSecurityStats {
            cache_stats: dummy_cache_stats(),
            rate_limit_stats: dummy_rate_limit_stats(),
            batch_processor_stats: None,
            advanced_cache_stats: None,
            crypto_stats: None,
            threat_detection_stats: None,
        };
        let cloned = stats.clone();
        assert_eq!(
            cloned.cache_stats.auth_cache_size,
            stats.cache_stats.auth_cache_size
        );
    }

    /// A SecurityManager whose auth bucket is configured deliberately:
    /// rate 0 tokens/s (never refills) and capacity 2 — fully deterministic
    /// regardless of any ACTAE_IP_AUTH_* env vars in the test environment.
    fn manager_with_fixed_auth_bucket() -> SecurityManager {
        let mut manager =
            SecurityManager::new(Arc::new(crate::config::AppConfig::default())).unwrap();
        let config = RateLimitConfig {
            ip_auth_rate: 0,
            ip_auth_capacity: 2,
            ..RateLimitConfig::default()
        };
        manager.rate_limiter = Arc::new(MultiRateLimiter::new(config));
        manager
    }

    #[tokio::test]
    async fn test_validate_auth_lockout_does_not_consume_auth_bucket() {
        let manager = manager_with_fixed_auth_bucket();
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 10));

        // 30 lockout-only pre-auth checks: way above the bucket's 2-token
        // capacity. If any of them consumed a token, the bucket would be
        // empty afterwards.
        for i in 0..30 {
            let result = manager
                .validate_auth_lockout(Some(ip), Some("key-prefix"), AuthMethod::ApiKey)
                .await
                .unwrap();
            assert!(
                matches!(result, SecurityValidationResult::Allowed),
                "lockout check {i} must allow (no bucket consumption)"
            );
        }

        // The bucket must still be untouched: both tokens are still available.
        assert!(matches!(
            manager.rate_limiter().check_auth_limit(ip),
            RateLimitResult::Allowed
        ));
        assert!(matches!(
            manager.rate_limiter().check_auth_limit(ip),
            RateLimitResult::Allowed
        ));
        assert!(matches!(
            manager.rate_limiter().check_auth_limit(ip),
            RateLimitResult::Limited(_)
        ));
    }

    #[tokio::test]
    async fn test_validate_auth_attempt_consumes_auth_bucket() {
        let manager = manager_with_fixed_auth_bucket();
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 11));

        // Per-attempt consumption (WebSocket semantics): with capacity 2 the
        // first two attempts pass, the third is rate-limited.
        for _ in 0..2 {
            let result = manager
                .validate_auth_attempt(Some(ip), Some("key-prefix"), AuthMethod::ApiKey)
                .await
                .unwrap();
            assert!(matches!(result, SecurityValidationResult::Allowed));
        }
        assert!(matches!(
            manager
                .validate_auth_attempt(Some(ip), Some("key-prefix"), AuthMethod::ApiKey)
                .await
                .unwrap(),
            SecurityValidationResult::RateLimited(_)
        ));
    }
}
