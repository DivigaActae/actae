// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use regex::Regex;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;
use uuid::Uuid;

use crate::auth::websocket_auth::{AuthMethod, AuthResult as WebSocketAuthResult};
use crate::config::AppConfig;

/// Connection security validator with comprehensive checks
pub struct ConnectionSecurityValidator {
    config: Arc<AppConfig>,
    // Precompiled regex patterns for performance
    user_agent_patterns: Vec<Regex>,
    suspicious_patterns: Vec<Regex>,
}

#[derive(Debug, Clone)]
pub enum SecurityValidationResult {
    Allowed,
    Blocked(String),
    RateLimited(crate::security::RateLimitInfo),
    RequiresAdditionalAuth,
}

#[derive(Debug, Clone)]
pub struct AnonymousAccessPolicy {
    pub enabled: bool,
    pub allowed_channels: Vec<String>,
    pub session_timeout_seconds: u64,
    pub max_concurrent_sessions: u32,
}

impl ConnectionSecurityValidator {
    pub fn new(config: Arc<AppConfig>) -> Self {
        // Precompile regex patterns for better performance
        let user_agent_patterns = vec![
            Regex::new(r"(?i)bot|crawler|spider|scraper")
                .expect("hardcoded bot regex must compile"),
            Regex::new(r"^$").expect("hardcoded empty-ua regex must compile"), // Empty user agent
        ];

        let suspicious_patterns = vec![
            Regex::new(r"(?i)injection|union|select|drop|delete")
                .expect("hardcoded injection regex must compile"),
            Regex::new(r#"[<>"'&]"#).expect("hardcoded XSS regex must compile"), // XSS patterns
            Regex::new(r"\.\./").expect("hardcoded path-traversal regex must compile"), // Path traversal
        ];

        Self {
            config,
            user_agent_patterns,
            suspicious_patterns,
        }
    }

    /// Comprehensive connection security validation
    pub async fn validate_connection_security(
        &self,
        connection_id: Uuid,
        _ip: Option<IpAddr>,
        user_agent: Option<String>,
        origin: Option<String>,
        host_header: Option<String>,
    ) -> Result<SecurityValidationResult> {
        // Validate user agent
        if let Some(ref ua) = user_agent {
            if self.is_suspicious_user_agent(ua) {
                warn!("Suspicious user agent detected: {}", ua);
                return Ok(SecurityValidationResult::Blocked(
                    "Suspicious user agent".to_string(),
                ));
            }
        }

        // Validate origin if a browser supplied one (audit item 5).
        if let Some(ref origin) = origin {
            if !self.validate_origin(origin, host_header.as_deref()) {
                return Ok(SecurityValidationResult::Blocked(format!(
                    "Origin not allowed: {} (host: {})",
                    origin,
                    host_header.as_deref().unwrap_or("<none>")
                )));
            }
        }

        // Check for suspicious patterns in connection parameters
        if self.has_suspicious_patterns(&connection_id.to_string()) {
            return Ok(SecurityValidationResult::Blocked(
                "Suspicious connection parameters".to_string(),
            ));
        }

        Ok(SecurityValidationResult::Allowed)
    }

    /// Validate anonymous access with strict controls
    pub async fn validate_anonymous_access(
        &self,
        channel: Option<&str>,
        ip: Option<IpAddr>,
    ) -> Result<SecurityValidationResult> {
        // Check if anonymous access is globally enabled
        if !self.config.enable_anonymous {
            return Ok(SecurityValidationResult::Blocked(
                "Anonymous access is disabled".to_string(),
            ));
        }

        // Get anonymous access policy
        let policy = self.get_anonymous_access_policy().await?;

        if !policy.enabled {
            return Ok(SecurityValidationResult::Blocked(
                "Anonymous access not enabled".to_string(),
            ));
        }

        // Check if the requested channel is allowed for anonymous access
        if let Some(channel) = channel {
            if !self.is_channel_allowed_for_anonymous(&policy, channel) {
                return Ok(SecurityValidationResult::Blocked(
                    "Channel not accessible for anonymous users".to_string(),
                ));
            }
        }

        // Check concurrent anonymous sessions limit
        if let Some(ip) = ip {
            let current_sessions = self.count_anonymous_sessions_for_ip(ip).await?;
            if current_sessions >= policy.max_concurrent_sessions {
                return Ok(SecurityValidationResult::RateLimited(
                    crate::security::RateLimitInfo {
                        limit_type: "anonymous_sessions".to_string(),
                        current_rate: current_sessions,
                        limit_threshold: policy.max_concurrent_sessions,
                        retry_after_seconds: 60,
                    },
                ));
            }
        }

        Ok(SecurityValidationResult::Allowed)
    }

    /// Create secure anonymous authentication result
    pub fn create_secure_anonymous_auth(&self, _ip: Option<IpAddr>) -> Result<WebSocketAuthResult> {
        // Generate unique anonymous user ID
        let anon_user_id = format!("anon_{}", Uuid::new_v4());

        // Calculate session expiration
        let expires_at = chrono::Utc::now()
            + chrono::Duration::seconds(self.config.anonymous_session_timeout_seconds as i64);

        // Create restricted permissions for anonymous users
        let permissions = sonic_rs::json!(["read:public.*", "connect"]);

        Ok(WebSocketAuthResult {
            user_id: anon_user_id,
            auth_method: AuthMethod::Anonymous,
            permissions: Some(permissions),
            expires_at: Some(expires_at),
            key_source: None,
            key_id: None,
            federation_operation_id: None,
            federation_action: None,
            federation_instance_id: None,
        })
    }

    /// Check if user agent is suspicious
    fn is_suspicious_user_agent(&self, user_agent: &str) -> bool {
        // Check against known bot/crawler patterns
        for pattern in &self.user_agent_patterns {
            if pattern.is_match(user_agent) {
                return true;
            }
        }

        // Check for extremely long user agents (potential buffer overflow attempts)
        if user_agent.len() > 512 {
            return true;
        }

        // Check for binary or encoded data in user agent
        if user_agent.chars().any(|c| c.is_control() && c != '\t') {
            return true;
        }

        false
    }

    /// Decide whether a browser-supplied `Origin` header is allowed.
    ///
    /// Delegates to `OriginPolicy` (exact normalized scheme/host/port
    /// matching, same-origin default when nothing is configured, explicit
    /// `*` opt-in wildcard). A missing origin header is always allowed (CLI
    /// and server SDK clients do not send one).
    fn validate_origin(&self, origin: &str, host_header: Option<&str>) -> bool {
        let policy = crate::security::origin::OriginPolicy::from_config(
            self.config.websocket_allowed_origins.as_deref(),
        );
        policy.allows(origin, host_header, self.config.enable_tls)
    }

    /// Check for suspicious patterns in strings
    fn has_suspicious_patterns(&self, input: &str) -> bool {
        for pattern in &self.suspicious_patterns {
            if pattern.is_match(input) {
                return true;
            }
        }
        false
    }

    /// Get anonymous access policy
    async fn get_anonymous_access_policy(&self) -> Result<AnonymousAccessPolicy> {
        // This would typically be loaded from database/configuration
        // For now, return a secure default policy

        Ok(AnonymousAccessPolicy {
            enabled: self.config.enable_anonymous,
            allowed_channels: vec!["public.*".to_string(), "announcements.*".to_string()],
            session_timeout_seconds: self.config.anonymous_session_timeout_seconds,
            max_concurrent_sessions: 3, // Conservative limit
        })
    }

    /// Check if channel is allowed for anonymous access
    fn is_channel_allowed_for_anonymous(
        &self,
        policy: &AnonymousAccessPolicy,
        channel: &str,
    ) -> bool {
        for allowed_pattern in &policy.allowed_channels {
            if self.matches_channel_pattern(channel, allowed_pattern) {
                return true;
            }
        }
        false
    }

    /// Simple pattern matching for channels
    fn matches_channel_pattern(&self, channel: &str, pattern: &str) -> bool {
        if let Some(prefix) = pattern.strip_suffix("*") {
            return channel.starts_with(prefix);
        }
        channel == pattern
    }

    /// Count anonymous sessions for IP (simplified)
    async fn count_anonymous_sessions_for_ip(&self, _ip: IpAddr) -> Result<u32> {
        // This would typically query active connection manager
        // For now, return a placeholder
        Ok(1) // Conservative estimate
    }

    /// Validate connection timing (detect replay attacks)
    pub fn validate_connection_timing(&self, timestamp: Option<u64>) -> bool {
        if let Some(ts) = timestamp {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            // Allow 5 minute window for clock skew
            let time_diff = now.abs_diff(ts);
            time_diff <= 300 // 5 minutes
        } else {
            true // No timestamp provided, allow
        }
    }

    /// Validate message size and content
    pub fn validate_message_content(&self, content: &str, max_size: usize) -> Result<bool> {
        // Check size limits
        if content.len() > max_size {
            return Ok(false);
        }

        // Check for suspicious patterns
        if self.has_suspicious_patterns(content) {
            warn!("Suspicious content detected in message");
            return Ok(false);
        }

        // Check for potential DoS patterns (excessive repetition)
        if self.has_excessive_repetition(content) {
            return Ok(false);
        }

        Ok(true)
    }

    /// Check for excessive character repetition (potential DoS)
    fn has_excessive_repetition(&self, content: &str) -> bool {
        if content.is_empty() {
            return false;
        }

        let mut current_char = content.chars().next().unwrap();
        let mut current_count = 1;
        let mut max_repetition = 1;

        for c in content.chars().skip(1) {
            if c == current_char {
                current_count += 1;
                max_repetition = max_repetition.max(current_count);
            } else {
                current_char = c;
                current_count = 1;
            }
        }

        // Flag as suspicious if any character repeats more than 100 times
        max_repetition > 100
    }
}

impl Default for AnonymousAccessPolicy {
    fn default() -> Self {
        Self {
            enabled: false, // Secure default: disabled
            allowed_channels: vec!["public.*".to_string()],
            session_timeout_seconds: 900, // 15 minutes
            max_concurrent_sessions: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use std::sync::Arc;

    fn make_test_validator() -> ConnectionSecurityValidator {
        let config = Arc::new(AppConfig {
            websocket_allowed_origins: Some(vec![
                "https://example.com".to_string(),
                "*.example.com".to_string(),
            ]),
            enable_anonymous: true,
            ..AppConfig::default()
        });
        ConnectionSecurityValidator::new(config)
    }

    // --- ConnectionSecurityValidator tests ---

    #[test]
    fn test_new_creates_valid_instance() {
        let validator = make_test_validator();
        assert_eq!(validator.user_agent_patterns.len(), 2);
        assert_eq!(validator.suspicious_patterns.len(), 3);
    }

    #[test]
    fn test_is_suspicious_user_agent_matches_bots() {
        let validator = make_test_validator();
        assert!(validator.is_suspicious_user_agent("Googlebot"));
        assert!(validator.is_suspicious_user_agent("Mozilla/5.0 (compatible; bingbot/2.0)"));
        assert!(validator.is_suspicious_user_agent("crawler"));
        assert!(validator.is_suspicious_user_agent("spider"));
        assert!(validator.is_suspicious_user_agent("scraper"));
        assert!(validator.is_suspicious_user_agent(""));
    }

    #[test]
    fn test_is_suspicious_user_agent_allows_normal() {
        let validator = make_test_validator();
        assert!(!validator.is_suspicious_user_agent(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        ));
        assert!(!validator.is_suspicious_user_agent(
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
        ));
        assert!(!validator.is_suspicious_user_agent(
            "Mozilla/5.0 (X11; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0"
        ));
    }

    #[test]
    fn test_is_suspicious_user_agent_long_ua() {
        let validator = make_test_validator();
        let long_ua = "Normal/1.0".to_string() + &"a".repeat(510);
        assert!(validator.is_suspicious_user_agent(&long_ua));
    }

    #[test]
    fn test_is_suspicious_user_agent_control_chars() {
        let validator = make_test_validator();
        assert!(validator.is_suspicious_user_agent("Mozilla\x00Firefox"));
        assert!(validator.is_suspicious_user_agent("User\rAgent"));
    }

    #[test]
    fn test_validate_origin_exact_match() {
        let validator = make_test_validator();
        // Explicitly configured origin matches exactly.
        assert!(validator.validate_origin("https://example.com", Some("example.com")));
        // Substring lookalikes must be rejected (old `contains()` bug).
        assert!(
            !validator.validate_origin("https://example.com.attacker.test", Some("example.com"))
        );
        assert!(!validator.validate_origin("https://evil.com", Some("example.com")));
        assert!(!validator.validate_origin("http://example.org", Some("example.com")));
    }

    #[test]
    fn test_validate_origin_same_origin_default() {
        // No origins configured → same-origin only: Origin must match Host
        // on scheme (derived from enable_tls) + host + port.
        let config = Arc::new(AppConfig {
            websocket_allowed_origins: None,
            enable_tls: true,
            ..AppConfig::default()
        });
        let validator = ConnectionSecurityValidator::new(config);
        assert!(validator.validate_origin("https://example.com", Some("example.com")));
        assert!(!validator.validate_origin("https://evil.com", Some("example.com")));
        assert!(!validator.validate_origin("http://example.com", Some("example.com")));

        let config = Arc::new(AppConfig {
            websocket_allowed_origins: None,
            enable_tls: false,
            ..AppConfig::default()
        });
        let validator = ConnectionSecurityValidator::new(config);
        assert!(validator.validate_origin("http://example.com", Some("example.com")));
        assert!(!validator.validate_origin("https://example.com", Some("example.com")));
    }

    #[test]
    fn test_validate_origin_wildcard_opt_in() {
        let config = Arc::new(AppConfig {
            websocket_allowed_origins: Some(vec!["*".to_string()]),
            ..AppConfig::default()
        });
        let validator = ConnectionSecurityValidator::new(config);
        assert!(validator.validate_origin("https://anything.com", None));
    }

    #[test]
    fn test_has_suspicious_patterns_catches_injection() {
        let validator = make_test_validator();
        assert!(validator.has_suspicious_patterns("SELECT * FROM users"));
        assert!(validator.has_suspicious_patterns("drop table users"));
        assert!(validator.has_suspicious_patterns("DELETE FROM sessions"));
        assert!(validator.has_suspicious_patterns("UNION SELECT username,password"));
        assert!(validator.has_suspicious_patterns("injection attempt"));
    }

    #[test]
    fn test_has_suspicious_patterns_catches_xss() {
        let validator = make_test_validator();
        assert!(validator.has_suspicious_patterns("<script>alert('xss')</script>"));
        assert!(validator.has_suspicious_patterns("<img src=x onerror=alert(1)>"));
        assert!(validator.has_suspicious_patterns("\" onmouseover=\"alert(1)\""));
        assert!(validator.has_suspicious_patterns("' OR '1'='1"));
    }

    #[test]
    fn test_has_suspicious_patterns_catches_path_traversal() {
        let validator = make_test_validator();
        assert!(validator.has_suspicious_patterns("../../../etc/passwd"));
    }

    #[test]
    fn test_has_suspicious_patterns_catches_path_traversal_forward() {
        let validator = make_test_validator();
        assert!(validator.has_suspicious_patterns("../../../etc/passwd"));
    }

    #[test]
    fn test_has_suspicious_patterns_allows_normal() {
        let validator = make_test_validator();
        assert!(!validator.has_suspicious_patterns("Hello, this is a normal message"));
        assert!(!validator.has_suspicious_patterns("user123-tenant-abc"));
        assert!(!validator.has_suspicious_patterns("normal-connection-id"));
        assert!(!validator.has_suspicious_patterns("just some regular text"));
    }

    #[test]
    fn test_validate_connection_timing() {
        let validator = make_test_validator();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        assert!(validator.validate_connection_timing(Some(now)));
        assert!(validator.validate_connection_timing(Some(now - 240)));
        assert!(!validator.validate_connection_timing(Some(now - 301)));
        assert!(validator.validate_connection_timing(None));
    }

    #[test]
    fn test_validate_message_content() {
        let validator = make_test_validator();
        assert!(validator
            .validate_message_content("Hello world", 1000)
            .unwrap());
        assert!(!validator
            .validate_message_content("Hello world", 5)
            .unwrap());
        assert!(!validator
            .validate_message_content("SELECT * FROM users", 1000)
            .unwrap());
        assert!(!validator
            .validate_message_content("<script>attack</script>", 1000)
            .unwrap());
        assert!(!validator
            .validate_message_content("../../../etc/passwd", 1000)
            .unwrap());
    }

    #[test]
    fn test_has_excessive_repetition() {
        let validator = make_test_validator();
        assert!(!validator.has_excessive_repetition("normal text"));
        assert!(validator.has_excessive_repetition(&"a".repeat(200)));
        assert!(!validator.has_excessive_repetition(&"ab".repeat(50)));
        assert!(!validator.has_excessive_repetition(""));
    }

    #[test]
    fn test_matches_origin_pattern() {
        // Removed: substring/glob origin matching is gone — OriginPolicy uses
        // exact normalized (scheme, host, port) comparison (audit item 5).
    }

    #[test]
    fn test_matches_channel_pattern() {
        let validator = make_test_validator();
        assert!(validator.matches_channel_pattern("public.room1", "public.*"));
        assert!(validator.matches_channel_pattern("public.room1.sub", "public.*"));
        assert!(!validator.matches_channel_pattern("private.room1", "public.*"));
        assert!(validator.matches_channel_pattern("exact", "exact"));
        assert!(!validator.matches_channel_pattern("different", "exact"));
    }

    #[test]
    fn test_is_channel_allowed_for_anonymous() {
        let policy = AnonymousAccessPolicy {
            enabled: true,
            allowed_channels: vec!["public.*".to_string(), "announcements.*".to_string()],
            session_timeout_seconds: 900,
            max_concurrent_sessions: 3,
        };
        let validator = make_test_validator();
        assert!(validator.is_channel_allowed_for_anonymous(&policy, "public.general"));
        assert!(validator.is_channel_allowed_for_anonymous(&policy, "announcements.system"));
        assert!(!validator.is_channel_allowed_for_anonymous(&policy, "private.chat"));
    }

    // --- SecurityValidationResult tests ---

    #[test]
    fn test_security_validation_result_debug_clone() {
        let variants: Vec<SecurityValidationResult> = vec![
            SecurityValidationResult::Allowed,
            SecurityValidationResult::Blocked("blocked".to_string()),
            SecurityValidationResult::RateLimited(crate::security::RateLimitInfo {
                limit_type: "test".to_string(),
                current_rate: 0,
                limit_threshold: 100,
                retry_after_seconds: 60,
            }),
            SecurityValidationResult::RequiresAdditionalAuth,
        ];
        for v in &variants {
            let _ = format!("{v:?}");
            let _ = v.clone();
        }
    }

    // --- AnonymousAccessPolicy tests ---

    #[test]
    fn test_anonymous_access_policy_default() {
        let policy = AnonymousAccessPolicy::default();
        assert!(!policy.enabled);
        assert_eq!(policy.allowed_channels, vec!["public.*"]);
        assert_eq!(policy.session_timeout_seconds, 900);
        assert_eq!(policy.max_concurrent_sessions, 1);
    }

    #[test]
    fn test_anonymous_access_policy_custom() {
        let policy = AnonymousAccessPolicy {
            enabled: true,
            allowed_channels: vec!["public.*".to_string(), "chat.*".to_string()],
            session_timeout_seconds: 1800,
            max_concurrent_sessions: 5,
        };
        assert!(policy.enabled);
        assert_eq!(policy.allowed_channels.len(), 2);
        assert_eq!(policy.session_timeout_seconds, 1800);
        assert_eq!(policy.max_concurrent_sessions, 5);
    }

    // ── validate_message_content / connection timing edge cases ──

    #[test]
    fn test_validate_message_content_edge_cases() {
        let validator = make_test_validator();
        // Empty content is allowed.
        assert!(validator.validate_message_content("", 1000).unwrap());
        // Exactly-at-limit passes; one over fails. (Must avoid consecutive
        // repetition — 100+ identical chars trips the DoS marker.)
        let exact = "abcdefghij".repeat(100); // 1000 chars, no repetition
        assert_eq!(exact.len(), 1000);
        assert!(validator.validate_message_content(&exact, 1000).unwrap());
        assert!(!validator.validate_message_content(&exact, 999).unwrap());
        // Excessive repetition (DoS marker) is rejected.
        assert!(!validator
            .validate_message_content(&"a".repeat(500), 5000)
            .unwrap());
        // Benign text still passes at a generous limit.
        assert!(validator
            .validate_message_content("a normal message from a user", 1000)
            .unwrap());
    }

    #[test]
    fn test_validate_connection_timing_window() {
        let validator = make_test_validator();
        // No timestamp → allowed.
        assert!(validator.validate_connection_timing(None));
        // A timestamp roughly "now" is within the 5-minute skew window.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(validator.validate_connection_timing(Some(now)));
        assert!(validator.validate_connection_timing(Some(now - 120))); // 2 min ago
        assert!(validator.validate_connection_timing(Some(now + 120))); // 2 min in future
                                                                        // Beyond the 5-minute skew window either way.
        assert!(!validator.validate_connection_timing(Some(now - 10_000)));
        assert!(!validator.validate_connection_timing(Some(now + 10_000)));
    }
}
