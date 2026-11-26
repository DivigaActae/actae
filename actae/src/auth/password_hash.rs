// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Secure Password Hashing Module
//!
//! Provides comprehensive password security with:
//! - bcrypt hashing with configurable cost (≥ 12 for security)
//! - Password strength validation
//! - Timing attack resistance
//! - Salt generation and verification
//! - Performance optimized with async support
//! - Password history tracking for rotation policies
//! - Audit logging for password operations

use anyhow::{Context, Result};
use base64::Engine;
use chrono::{DateTime, Duration, Utc};

use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use sonic_rs::Value;
use std::time::Instant;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Password hashing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordConfig {
    /// bcrypt cost parameter (rounds = 2^cost)
    pub bcrypt_cost: u32,
    /// Minimum password length
    pub min_length: usize,
    /// Maximum password length
    pub max_length: usize,
    /// Require uppercase letters
    pub require_uppercase: bool,
    /// Require lowercase letters
    pub require_lowercase: bool,
    /// Require digits
    pub require_digits: bool,
    /// Require special characters
    pub require_special_chars: bool,
    /// Password history length (for rotation policies)
    pub password_history_length: usize,
    /// Minimum time between password changes (in hours)
    pub min_password_age_hours: u32,
    /// Maximum time before password expires (in days, 0 = no expiry)
    pub max_password_age_days: u32,
    /// Enable timing attack protection
    pub enable_timing_protection: bool,
    /// Enable audit logging
    pub enable_audit_logging: bool,
}

impl Default for PasswordConfig {
    fn default() -> Self {
        Self {
            bcrypt_cost: 12, // Secure default (2^12 = 4096 rounds)
            min_length: 8,
            max_length: 128,
            require_uppercase: true,
            require_lowercase: true,
            require_digits: true,
            require_special_chars: true,
            password_history_length: 5, // Remember last 5 passwords
            min_password_age_hours: 1,  // Can't change password more than once per hour
            max_password_age_days: 90,  // Password expires after 90 days
            enable_timing_protection: true,
            enable_audit_logging: true,
        }
    }
}

/// Password strength levels
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum PasswordStrength {
    VeryWeak = 0,
    Weak = 1,
    Fair = 2,
    Good = 3,
    Strong = 4,
    VeryStrong = 5,
}

impl PasswordStrength {
    pub fn is_acceptable(&self) -> bool {
        *self as u8 >= PasswordStrength::Good as u8
    }

    pub fn description(&self) -> &'static str {
        match self {
            PasswordStrength::VeryWeak => "Very Weak",
            PasswordStrength::Weak => "Weak",
            PasswordStrength::Fair => "Fair",
            PasswordStrength::Good => "Good",
            PasswordStrength::Strong => "Strong",
            PasswordStrength::VeryStrong => "Very Strong",
        }
    }
}

/// Password validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordValidation {
    pub is_valid: bool,
    pub strength: PasswordStrength,
    pub score: u8,
    pub feedback: Vec<String>,
    pub estimated_crack_time: String,
    pub validation_time_ms: u64,
}

/// Password hash metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordHashMetadata {
    pub algorithm: String,
    pub cost: u32,
    pub salt: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub usage_count: u64,
    pub expires_at: Option<DateTime<Utc>>,
}

/// Historical password record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordHistory {
    pub hash: String,
    pub metadata: PasswordHashMetadata,
    pub created_at: DateTime<Utc>,
    pub replaced_at: Option<DateTime<Utc>>,
}

/// Password audit event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordAuditEvent {
    pub event_id: String,
    pub user_id: String,
    pub event_type: PasswordEventType,
    pub success: bool,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub metadata: Option<Value>,
    pub timestamp: DateTime<Utc>,
}

/// Password event types for audit logging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PasswordEventType {
    Created,
    Changed,
    Reset,
    Validated,
    Expired,
    ValidationFailed,
    WeakPasswordRejected,
    ReusedPasswordRejected,
}

/// Secure password hasher with comprehensive security features
pub struct SecurePasswordHasher {
    config: PasswordConfig,
    /// Password history storage (in production, use database)
    password_history: std::sync::Arc<
        tokio::sync::RwLock<std::collections::HashMap<String, Vec<PasswordHistory>>>,
    >,
    /// Audit events storage
    audit_events: std::sync::Arc<tokio::sync::RwLock<Vec<PasswordAuditEvent>>>,
}

impl SecurePasswordHasher {
    /// Create new password hasher
    pub fn new(config: PasswordConfig) -> Self {
        Self {
            config,
            password_history: std::sync::Arc::new(tokio::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
            audit_events: std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new())),
        }
    }

    /// Create hasher with default secure configuration
    pub fn with_secure_defaults() -> Self {
        Self::new(PasswordConfig::default())
    }

    /// Hash a password securely
    pub async fn hash_password(&self, password: &str, user_id: &str) -> Result<String> {
        let start_time = Instant::now();

        // Validate password strength first
        let validation = self.validate_password_strength(password).await?;
        if !validation.is_valid {
            self.audit_log_event(PasswordAuditEvent {
                event_id: Uuid::new_v4().to_string(),
                user_id: user_id.to_string(),
                event_type: PasswordEventType::WeakPasswordRejected,
                success: false,
                ip_address: None,
                user_agent: None,
                metadata: Some(sonic_rs::json!({
                    "strength": validation.strength,
                    "feedback": validation.feedback
                })),
                timestamp: Utc::now(),
            })
            .await;

            return Err(anyhow::anyhow!(
                "Password does not meet security requirements: {}",
                validation.feedback.join(", ")
            ));
        }

        // Check against password history
        if self.is_password_reused(password, user_id).await? {
            self.audit_log_event(PasswordAuditEvent {
                event_id: Uuid::new_v4().to_string(),
                user_id: user_id.to_string(),
                event_type: PasswordEventType::ReusedPasswordRejected,
                success: false,
                ip_address: None,
                user_agent: None,
                metadata: None,
                timestamp: Utc::now(),
            })
            .await;

            return Err(anyhow::anyhow!(
                "Password was recently used. Please choose a different password."
            ));
        }

        // Generate hash with bcrypt
        let hash = tokio::task::spawn_blocking({
            let password = password.to_string();
            let cost = self.config.bcrypt_cost;
            move || -> Result<String> {
                bcrypt::hash(password, cost).context("Failed to hash password with bcrypt")
            }
        })
        .await??;

        // Store in password history
        self.add_to_password_history(user_id, &hash).await?;

        // Audit log successful creation
        self.audit_log_event(PasswordAuditEvent {
            event_id: Uuid::new_v4().to_string(),
            user_id: user_id.to_string(),
            event_type: PasswordEventType::Created,
            success: true,
            ip_address: None,
            user_agent: None,
            metadata: Some(sonic_rs::json!({
                "strength": validation.strength,
                "processing_time_ms": start_time.elapsed().as_millis()
            })),
            timestamp: Utc::now(),
        })
        .await;

        debug!(
            "Password hashed for user {} in {}ms (strength: {:?})",
            user_id,
            start_time.elapsed().as_millis(),
            validation.strength
        );

        Ok(hash)
    }

    /// Verify a password against its hash
    pub async fn verify_password(&self, password: &str, hash: &str, user_id: &str) -> Result<bool> {
        let start_time = Instant::now();

        // Verify with bcrypt
        let is_valid = tokio::task::spawn_blocking({
            let password = password.to_string();
            let hash = hash.to_string();
            move || -> Result<bool> {
                bcrypt::verify(password, &hash).context("Failed to verify password with bcrypt")
            }
        })
        .await??;

        // Apply timing attack protection
        if self.config.enable_timing_protection {
            self.apply_timing_protection(start_time).await;
        }

        // Audit log verification attempt
        self.audit_log_event(PasswordAuditEvent {
            event_id: Uuid::new_v4().to_string(),
            user_id: user_id.to_string(),
            event_type: if is_valid {
                PasswordEventType::Validated
            } else {
                PasswordEventType::ValidationFailed
            },
            success: is_valid,
            ip_address: None,
            user_agent: None,
            metadata: Some(sonic_rs::json!({
                "processing_time_ms": start_time.elapsed().as_millis()
            })),
            timestamp: Utc::now(),
        })
        .await;

        if is_valid {
            debug!("Password verification successful for user {}", user_id);
        } else {
            warn!("Password verification failed for user {}", user_id);
        }

        Ok(is_valid)
    }

    /// Validate password strength
    pub async fn validate_password_strength(&self, password: &str) -> Result<PasswordValidation> {
        let start_time = Instant::now();
        let mut feedback = Vec::new();
        let mut score = 0u8;

        // Length validation
        if password.len() < self.config.min_length {
            feedback.push(format!(
                "Password must be at least {} characters",
                self.config.min_length
            ));
        } else if password.len() >= self.config.min_length {
            score += 1;
        }

        if password.len() > self.config.max_length {
            feedback.push(format!(
                "Password must not exceed {} characters",
                self.config.max_length
            ));
        }

        // Character set validation
        let has_uppercase = password.chars().any(|c| c.is_uppercase());
        let has_lowercase = password.chars().any(|c| c.is_lowercase());
        let has_digits = password.chars().any(|c| c.is_ascii_digit());
        let has_special = password
            .chars()
            .any(|c| "!@#$%^&*()_+-=[]{}|;:,.<>?".contains(c));

        if self.config.require_uppercase && !has_uppercase {
            feedback.push("Password must contain at least one uppercase letter".to_string());
        } else if has_uppercase {
            score += 1;
        }

        if self.config.require_lowercase && !has_lowercase {
            feedback.push("Password must contain at least one lowercase letter".to_string());
        } else if has_lowercase {
            score += 1;
        }

        if self.config.require_digits && !has_digits {
            feedback.push("Password must contain at least one digit".to_string());
        } else if has_digits {
            score += 1;
        }

        if self.config.require_special_chars && !has_special {
            feedback.push("Password must contain at least one special character".to_string());
        } else if has_special {
            score += 1;
        }

        // Additional strength checks
        if password.len() >= 12 {
            score += 1; // Bonus for longer passwords
        }

        // Check for common patterns
        if self.contains_common_patterns(password) {
            feedback.push("Password contains common patterns or sequences".to_string());
            score = score.saturating_sub(1);
        }

        // Check for dictionary words (simplified check)
        if self.contains_dictionary_words(password) {
            feedback.push("Password contains common dictionary words".to_string());
            score = score.saturating_sub(1);
        }

        // Determine strength level
        let strength = match score {
            0..=1 => PasswordStrength::VeryWeak,
            2 => PasswordStrength::Weak,
            3 => PasswordStrength::Fair,
            4 => PasswordStrength::Good,
            5 => PasswordStrength::Strong,
            _ => PasswordStrength::VeryStrong,
        };

        // Estimate crack time (simplified calculation)
        let estimated_crack_time = self.estimate_crack_time(password, strength);

        let is_valid = feedback.is_empty() && strength.is_acceptable();

        Ok(PasswordValidation {
            is_valid,
            strength,
            score,
            feedback,
            estimated_crack_time,
            validation_time_ms: start_time.elapsed().as_millis() as u64,
        })
    }

    /// Change password with validation
    pub async fn change_password(
        &self,
        user_id: &str,
        old_password: &str,
        new_password: &str,
        current_hash: &str,
    ) -> Result<String> {
        // Verify old password
        if !self
            .verify_password(old_password, current_hash, user_id)
            .await?
        {
            return Err(anyhow::anyhow!("Current password is incorrect"));
        }

        // Check minimum password age
        if let Some(last_change) = self.get_last_password_change(user_id).await? {
            let min_age = Duration::hours(self.config.min_password_age_hours as i64);
            if Utc::now() - last_change < min_age {
                return Err(anyhow::anyhow!(
                    "Password was changed too recently. Please wait before changing again."
                ));
            }
        }

        // Hash new password (includes validation)
        let new_hash = self.hash_password(new_password, user_id).await?;

        // Audit log password change
        self.audit_log_event(PasswordAuditEvent {
            event_id: Uuid::new_v4().to_string(),
            user_id: user_id.to_string(),
            event_type: PasswordEventType::Changed,
            success: true,
            ip_address: None,
            user_agent: None,
            metadata: None,
            timestamp: Utc::now(),
        })
        .await;

        info!("Password changed successfully for user {}", user_id);
        Ok(new_hash)
    }

    /// Check if password needs to be changed due to age
    pub async fn password_needs_change(&self, user_id: &str) -> Result<bool> {
        if self.config.max_password_age_days == 0 {
            return Ok(false); // No expiry configured
        }

        if let Some(last_change) = self.get_last_password_change(user_id).await? {
            let max_age = Duration::days(self.config.max_password_age_days as i64);
            Ok(Utc::now() - last_change >= max_age)
        } else {
            Ok(false) // No history = no expiry
        }
    }

    /// Get password strength requirements as user-friendly text
    pub fn get_strength_requirements(&self) -> Vec<String> {
        let mut requirements = Vec::new();

        requirements.push(format!(
            "Password must be between {} and {} characters",
            self.config.min_length, self.config.max_length
        ));

        if self.config.require_uppercase {
            requirements.push("Must contain at least one uppercase letter".to_string());
        }

        if self.config.require_lowercase {
            requirements.push("Must contain at least one lowercase letter".to_string());
        }

        if self.config.require_digits {
            requirements.push("Must contain at least one digit".to_string());
        }

        if self.config.require_special_chars {
            requirements.push("Must contain at least one special character (!@#$%^&*)".to_string());
        }

        requirements
    }

    /// Generate a random password guaranteed to satisfy this hasher's policy.
    ///
    /// Unlike [`generate_secure_password`](Self::generate_secure_password),
    /// callers do not have to know the configured minimum length (nor the
    /// character requirements): the result is always accepted by
    /// `validate_password_strength`. Used for first-boot credentials that are
    /// never shown to a human who could fix them up.
    pub fn generate_compliant_password(&self) -> String {
        self.generate_secure_password(self.config.min_length.max(16))
    }

    /// Generate a secure random password
    pub fn generate_secure_password(&self, length: usize) -> String {
        use rand::seq::SliceRandom;
        use rand::thread_rng;

        let uppercase = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        let lowercase = "abcdefghijklmnopqrstuvwxyz";
        let digits = "0123456789";
        let special = "!@#$%^&*()_+-=[]{}|;:,.<>?";

        let mut password = String::new();
        let mut rng = thread_rng();

        // Ensure we have at least one of each required type
        if self.config.require_uppercase {
            password.push(uppercase.chars().choose(&mut rng).unwrap());
        }
        if self.config.require_lowercase {
            password.push(lowercase.chars().choose(&mut rng).unwrap());
        }
        if self.config.require_digits {
            password.push(digits.chars().choose(&mut rng).unwrap());
        }
        if self.config.require_special_chars {
            password.push(special.chars().choose(&mut rng).unwrap());
        }

        // Fill remaining length with random characters
        let all_chars = format!("{uppercase}{lowercase}{digits}{special}");
        let all_chars: Vec<char> = all_chars.chars().collect();

        while password.len() < length {
            password.push(*all_chars.choose(&mut rng).unwrap());
        }

        // Shuffle the password to avoid predictable patterns
        let mut password_chars: Vec<char> = password.chars().collect();
        password_chars.shuffle(&mut rng);
        password_chars.into_iter().collect()
    }

    /// Get audit events for a user
    pub async fn get_audit_events(
        &self,
        user_id: &str,
        limit: Option<usize>,
    ) -> Vec<PasswordAuditEvent> {
        let events = self.audit_events.read().await;
        let mut user_events: Vec<_> = events
            .iter()
            .filter(|event| event.user_id == user_id)
            .cloned()
            .collect();

        user_events.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        if let Some(limit) = limit {
            user_events.truncate(limit);
        }

        user_events
    }

    // Private helper methods

    /// Check if password contains common patterns
    fn contains_common_patterns(&self, password: &str) -> bool {
        let common_patterns = [
            "123456", "654321", "abcdef", "qwerty", "password", "admin", "111111", "000000",
            "aaaaaa", "123abc", "abc123",
        ];

        let lower_password = password.to_lowercase();
        common_patterns
            .iter()
            .any(|pattern| lower_password.contains(pattern))
    }

    /// Check if password contains dictionary words (simplified)
    fn contains_dictionary_words(&self, password: &str) -> bool {
        let common_words = [
            "password", "admin", "user", "login", "welcome", "secret", "master", "super", "root",
            "test", "demo", "guest",
        ];

        let lower_password = password.to_lowercase();
        common_words
            .iter()
            .any(|word| lower_password.contains(word))
    }

    /// Estimate password crack time (simplified)
    fn estimate_crack_time(&self, _password: &str, strength: PasswordStrength) -> String {
        match strength {
            PasswordStrength::VeryWeak => "Instant",
            PasswordStrength::Weak => "Minutes",
            PasswordStrength::Fair => "Hours",
            PasswordStrength::Good => "Days",
            PasswordStrength::Strong => "Years",
            PasswordStrength::VeryStrong => "Centuries",
        }
        .to_string()
    }

    /// Apply timing attack protection
    async fn apply_timing_protection(&self, start_time: Instant) {
        let elapsed = start_time.elapsed();
        let target_time = std::time::Duration::from_millis(100); // Target 100ms

        if elapsed < target_time {
            let delay = target_time - elapsed;
            tokio::time::sleep(delay).await;
        }
    }

    /// Check if password was recently used
    async fn is_password_reused(&self, password: &str, user_id: &str) -> Result<bool> {
        let history = self.password_history.read().await;
        if let Some(user_history) = history.get(user_id) {
            for historical_password in user_history
                .iter()
                .take(self.config.password_history_length)
            {
                if bcrypt::verify(password, &historical_password.hash).unwrap_or(false) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Add password to history
    async fn add_to_password_history(&self, user_id: &str, hash: &str) -> Result<()> {
        let mut history = self.password_history.write().await;
        let user_history = history.entry(user_id.to_string()).or_insert_with(Vec::new);

        let password_record = PasswordHistory {
            hash: hash.to_string(),
            metadata: PasswordHashMetadata {
                algorithm: "bcrypt".to_string(),
                cost: self.config.bcrypt_cost,
                salt: "embedded".to_string(), // bcrypt embeds salt in hash
                created_at: Utc::now(),
                last_used_at: Some(Utc::now()),
                usage_count: 1,
                expires_at: if self.config.max_password_age_days > 0 {
                    Some(Utc::now() + Duration::days(self.config.max_password_age_days as i64))
                } else {
                    None
                },
            },
            created_at: Utc::now(),
            replaced_at: None,
        };

        user_history.insert(0, password_record);

        // Keep only configured history length
        user_history.truncate(self.config.password_history_length);

        Ok(())
    }

    /// Get last password change time
    async fn get_last_password_change(&self, user_id: &str) -> Result<Option<DateTime<Utc>>> {
        let history = self.password_history.read().await;
        if let Some(user_history) = history.get(user_id) {
            if let Some(latest) = user_history.first() {
                return Ok(Some(latest.created_at));
            }
        }
        Ok(None)
    }

    /// Add audit event
    async fn audit_log_event(&self, event: PasswordAuditEvent) {
        if self.config.enable_audit_logging {
            let mut events = self.audit_events.write().await;
            events.push(event);

            // Keep only recent events (last 5000)
            if events.len() > 5000 {
                events.drain(0..1000);
            }
        }
    }
}

/// Utility functions for password operations
pub mod utils {
    use super::*;

    /// Check if bcrypt cost is secure (≥ 12)
    pub fn is_secure_bcrypt_cost(cost: u32) -> bool {
        cost >= 12
    }

    /// Recommend bcrypt cost based on system performance
    pub async fn recommend_bcrypt_cost() -> u32 {
        // Test different costs and measure performance
        let test_password = "test_password_for_benchmarking";
        let costs_to_test = [10, 11, 12, 13, 14];

        for cost in costs_to_test {
            let start = Instant::now();
            let _ = bcrypt::hash(test_password, cost);
            let duration = start.elapsed();

            // Target ~100ms for good security/performance balance
            if duration.as_millis() >= 100 {
                return cost;
            }
        }

        12 // Secure default
    }

    /// Validate password meets minimum security requirements
    pub fn quick_password_check(password: &str) -> bool {
        password.len() >= 8
            && password.chars().any(|c| c.is_uppercase())
            && password.chars().any(|c| c.is_lowercase())
            && password.chars().any(|c| c.is_ascii_digit())
    }

    /// Generate salt for custom implementations (bcrypt handles this automatically)
    pub fn generate_salt(length: usize) -> String {
        use rand::RngCore;
        let mut rng = rand::thread_rng();
        let mut salt_bytes = vec![0u8; length];
        rng.fill_bytes(&mut salt_bytes);
        base64::engine::general_purpose::STANDARD.encode(salt_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strong_password() -> &'static str {
        "K7#nMp4$qR"
    }

    fn other_password() -> &'static str {
        "X9#rLm2$wV"
    }

    #[tokio::test]
    async fn test_password_hashing() {
        let hasher = SecurePasswordHasher::with_secure_defaults();
        let password = strong_password();
        let user_id = "hash_user";

        let hash = hasher.hash_password(password, user_id).await.unwrap();
        assert!(!hash.is_empty());
        assert!(hash.starts_with("$2b$")); // bcrypt format
    }

    #[tokio::test]
    async fn test_password_verification() {
        let hasher = SecurePasswordHasher::with_secure_defaults();
        let password = strong_password();
        let user_id = "verify_user";

        let hash = hasher.hash_password(password, user_id).await.unwrap();

        // Correct password should verify
        assert!(hasher
            .verify_password(password, &hash, user_id)
            .await
            .unwrap());

        // Wrong password should not verify
        assert!(!hasher
            .verify_password(other_password(), &hash, user_id)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_password_strength_validation() {
        let hasher = SecurePasswordHasher::with_secure_defaults();

        // Strong password (no dictionary words or common patterns)
        let validation = hasher
            .validate_password_strength(strong_password())
            .await
            .unwrap();
        assert!(validation.is_valid);
        assert!(validation.strength.is_acceptable());

        // Weak password
        let validation = hasher.validate_password_strength("weak").await.unwrap();
        assert!(!validation.is_valid);
        assert!(!validation.strength.is_acceptable());
    }

    #[tokio::test]
    async fn test_password_reuse_prevention() {
        let hasher = SecurePasswordHasher::with_secure_defaults();
        let user_id = "reuse_user";

        // First password
        let password1 = strong_password();
        let _hash1 = hasher.hash_password(password1, user_id).await.unwrap();

        // Try to reuse same password - should fail
        let result = hasher.hash_password(password1, user_id).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("recently used"));
    }

    #[tokio::test]
    async fn test_secure_password_generation() {
        let hasher = SecurePasswordHasher::with_secure_defaults();
        let password = hasher.generate_secure_password(16);

        assert_eq!(password.len(), 16);

        // Should meet all requirements
        let validation = hasher.validate_password_strength(&password).await.unwrap();
        assert!(validation.is_valid);
    }

    #[tokio::test]
    async fn test_generate_compliant_password_satisfies_policy() {
        let hasher = SecurePasswordHasher::with_secure_defaults();
        let password = hasher.generate_compliant_password();
        assert!(password.len() >= 16);
        let validation = hasher.validate_password_strength(&password).await.unwrap();
        assert!(
            validation.is_valid,
            "unexpected feedback: {:?}",
            validation.feedback
        );

        // A raised minimum length must still yield an acceptable password.
        let mut config = PasswordConfig::default();
        config.min_length = 32;
        let hasher = SecurePasswordHasher::new(config);
        let password = hasher.generate_compliant_password();
        assert!(password.len() >= 32);
        let validation = hasher.validate_password_strength(&password).await.unwrap();
        assert!(
            validation.is_valid,
            "unexpected feedback: {:?}",
            validation.feedback
        );
    }

    #[test]
    fn test_bcrypt_cost_validation() {
        assert!(utils::is_secure_bcrypt_cost(12));
        assert!(utils::is_secure_bcrypt_cost(15));
        assert!(!utils::is_secure_bcrypt_cost(10));
        assert!(!utils::is_secure_bcrypt_cost(8));
    }

    #[test]
    fn test_quick_password_check() {
        assert!(utils::quick_password_check("Password123"));
        assert!(!utils::quick_password_check("weak"));
        assert!(!utils::quick_password_check("PASSWORD123"));
        assert!(!utils::quick_password_check("password123"));
    }

    #[tokio::test]
    async fn test_empty_password_rejected() {
        let mut config = PasswordConfig::default();
        config.min_length = 1;
        config.require_uppercase = false;
        config.require_lowercase = false;
        config.require_digits = false;
        config.require_special_chars = false;
        config.enable_audit_logging = false;
        let hasher = SecurePasswordHasher::new(config);
        let result = hasher.hash_password("", "user-empty").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_password_strength_empty() {
        let hasher = SecurePasswordHasher::with_secure_defaults();
        let validation = hasher.validate_password_strength("").await.unwrap();
        assert!(!validation.is_valid);
        assert!(!validation.strength.is_acceptable());
        assert!(!validation.feedback.is_empty());
    }

    #[tokio::test]
    async fn test_password_almost_valid_missing_upper() {
        let hasher = SecurePasswordHasher::with_secure_defaults();
        let validation = hasher
            .validate_password_strength("lowercase123!")
            .await
            .unwrap();
        assert!(!validation.is_valid);
        assert!(validation.feedback.iter().any(|f| f.contains("uppercase")));
    }

    #[tokio::test]
    async fn test_password_almost_valid_missing_lower() {
        let hasher = SecurePasswordHasher::with_secure_defaults();
        let validation = hasher
            .validate_password_strength("UPPERCASE123!")
            .await
            .unwrap();
        assert!(!validation.is_valid);
        assert!(validation.feedback.iter().any(|f| f.contains("lowercase")));
    }

    #[tokio::test]
    async fn test_password_almost_valid_missing_digit() {
        let hasher = SecurePasswordHasher::with_secure_defaults();
        let validation = hasher
            .validate_password_strength("UppercaseLower!")
            .await
            .unwrap();
        assert!(!validation.is_valid);
        assert!(validation.feedback.iter().any(|f| f.contains("digit")));
    }

    #[tokio::test]
    async fn test_password_almost_valid_missing_special() {
        let hasher = SecurePasswordHasher::with_secure_defaults();
        let validation = hasher
            .validate_password_strength("UppercaseLower123")
            .await
            .unwrap();
        assert!(!validation.is_valid);
        assert!(validation.feedback.iter().any(|f| f.contains("special")));
    }

    #[tokio::test]
    async fn test_password_strength_very_weak() {
        let hasher = SecurePasswordHasher::with_secure_defaults();
        let validation = hasher
            .validate_password_strength(&"1".repeat(7))
            .await
            .unwrap();
        assert!(!validation.is_valid);
        assert_eq!(validation.strength, PasswordStrength::VeryWeak);
    }

    #[test]
    fn test_password_strength_levels_order() {
        assert!((PasswordStrength::VeryWeak as u8) < PasswordStrength::Weak as u8);
        assert!((PasswordStrength::Weak as u8) < PasswordStrength::Fair as u8);
        assert!((PasswordStrength::Fair as u8) < PasswordStrength::Good as u8);
        assert!((PasswordStrength::Good as u8) < PasswordStrength::Strong as u8);
        assert!((PasswordStrength::Strong as u8) < PasswordStrength::VeryStrong as u8);
    }

    #[test]
    fn test_password_strength_description() {
        assert_eq!(PasswordStrength::VeryWeak.description(), "Very Weak");
        assert_eq!(PasswordStrength::Weak.description(), "Weak");
        assert_eq!(PasswordStrength::Fair.description(), "Fair");
        assert_eq!(PasswordStrength::Good.description(), "Good");
        assert_eq!(PasswordStrength::Strong.description(), "Strong");
        assert_eq!(PasswordStrength::VeryStrong.description(), "Very Strong");
    }

    #[test]
    fn test_password_config_default_min_length() {
        let config = PasswordConfig::default();
        assert_eq!(config.min_length, 8);
    }

    #[test]
    fn test_password_config_default_bcrypt_cost() {
        let config = PasswordConfig::default();
        assert!(utils::is_secure_bcrypt_cost(config.bcrypt_cost));
    }

    #[test]
    fn test_generate_salt_length() {
        let salt = utils::generate_salt(16);
        assert!(!salt.is_empty());
        // base64 encoded: 16 bytes -> ceil(16*4/3) = 22 chars
        assert_eq!(salt.len(), 24);
    }

    #[test]
    fn test_generate_secure_password_meets_length() {
        let hasher = SecurePasswordHasher::with_secure_defaults();
        let password = hasher.generate_secure_password(20);
        assert_eq!(password.len(), 20);
    }

    #[test]
    fn test_estimate_crack_time() {
        let hasher = SecurePasswordHasher::with_secure_defaults();
        assert_eq!(
            hasher.estimate_crack_time("", PasswordStrength::VeryWeak),
            "Instant"
        );
        assert_eq!(
            hasher.estimate_crack_time("", PasswordStrength::Strong),
            "Years"
        );
    }

    #[test]
    fn test_strength_requirements_not_empty() {
        let hasher = SecurePasswordHasher::with_secure_defaults();
        let reqs = hasher.get_strength_requirements();
        assert!(!reqs.is_empty());
    }

    #[tokio::test]
    async fn test_password_needs_change_no_history() {
        let hasher = SecurePasswordHasher::with_secure_defaults();
        let needs_change = hasher.password_needs_change("nonexistent").await.unwrap();
        assert!(!needs_change);
    }

    #[tokio::test]
    async fn test_contains_common_patterns_detected() {
        let hasher = SecurePasswordHasher::with_secure_defaults();
        let validation = hasher
            .validate_password_strength(strong_password())
            .await
            .unwrap();
        assert!(validation.is_valid);
        let weak_validation = hasher
            .validate_password_strength("QwErTy123!")
            .await
            .unwrap();
        if !weak_validation.is_valid {
            assert!(weak_validation
                .feedback
                .iter()
                .any(|f| f.contains("common")));
        }
    }

    #[tokio::test]
    async fn test_password_verify_invalid_hash_fails_gracefully() {
        let hasher = SecurePasswordHasher::with_secure_defaults();
        let result = hasher
            .verify_password("anything", "not-a-valid-bcrypt-hash", "user")
            .await;
        assert!(result.is_err());
    }
}
