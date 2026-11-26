// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! JWT Authentication Module
//!
//! Provides comprehensive JWT token validation with security hardening:
//! - Format validation (proper JWT structure)
//! - Signature verification with proper key management
//! - Expiration validation with configurable tolerance
//! - Issuer and audience validation
//! - Algorithm validation (prevent algorithm confusion attacks)
//! - Rate limiting integration
//! - Audit logging for security monitoring

use crate::config::AppConfig;
use anyhow::{Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{
    decode,
    errors::{Error as JwtError, ErrorKind},
    Algorithm, DecodingKey, Header, TokenData, Validation,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tracing::{debug, error, warn};

/// Enhanced JWT claims with additional security fields
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    /// Subject (user ID)
    pub sub: String,
    /// Expiration time (Unix timestamp)
    pub exp: i64,
    /// Issued at time (Unix timestamp)
    pub iat: i64,
    /// Not before time (Unix timestamp)
    pub nbf: Option<i64>,
    /// Issuer
    pub iss: Option<String>,
    /// Audience
    pub aud: Option<String>,
    /// JWT ID for tracking
    pub jti: Option<String>,
    /// Custom permissions
    pub permissions: Option<Vec<String>>,
    /// Session ID
    pub session_id: Option<String>,
    /// Client IP (for security validation)
    pub client_ip: Option<String>,
    /// Device fingerprint
    pub device_id: Option<String>,
    /// Additional metadata (serde_json so the claim round-trips through
    /// the jsonwebtoken crate — sonic_rs::Value breaks serde_json's
    /// deserializer with its arbitrary-precision number representation)
    pub metadata: Option<serde_json::Value>,
    /// How this token was issued: `None` = password user session,
    /// `Some("api_key")` = dashboard session minted from an API key login.
    /// API-key sessions carry a subject that may not exist in the local
    /// users table (SaaS-issued keys), so consumers must skip the user
    /// lookup for them.
    #[serde(default)]
    pub auth_method: Option<String>,
}

impl Claims {
    /// Validate claims for security requirements
    pub fn validate_security(&self, config: &AppConfig) -> Result<()> {
        let now = Utc::now().timestamp();

        // Check expiration with tolerance
        if self.exp <= now {
            return Err(anyhow::anyhow!("Token has expired"));
        }

        // Check not-before time
        if let Some(nbf) = self.nbf {
            if nbf > now {
                return Err(anyhow::anyhow!("Token not yet valid"));
            }
        }

        // Validate issuer if configured
        if let Some(expected_issuer) = &config.jwt_issuer {
            match &self.iss {
                Some(iss) if iss == expected_issuer => {}
                _ => return Err(anyhow::anyhow!("Invalid or missing issuer")),
            }
        }

        // Validate audience if configured
        if let Some(expected_audience) = &config.jwt_audience {
            match &self.aud {
                Some(aud) if aud == expected_audience => {}
                _ => return Err(anyhow::anyhow!("Invalid or missing audience")),
            }
        }

        // Validate subject format
        if self.sub.trim().is_empty() {
            return Err(anyhow::anyhow!("Subject cannot be empty"));
        }

        Ok(())
    }

    /// Check if token expires within a given duration
    pub fn expires_within(&self, duration: Duration) -> bool {
        let expiry_time = DateTime::from_timestamp(self.exp, 0).unwrap_or_else(Utc::now);
        let threshold = Utc::now() + duration;
        expiry_time <= threshold
    }

    /// Get remaining token lifetime
    pub fn time_to_expiry(&self) -> Duration {
        let expiry_time = DateTime::from_timestamp(self.exp, 0).unwrap_or_else(Utc::now);
        let now = Utc::now();

        if expiry_time > now {
            expiry_time - now
        } else {
            Duration::zero()
        }
    }
}

/// JWT validation result with additional context
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub claims: Claims,
    pub header: Header,
    pub is_refresh_needed: bool,
    pub time_to_expiry: Duration,
    pub validation_timestamp: DateTime<Utc>,
}

/// JWT validation configuration
#[derive(Debug, Clone)]
pub struct JwtConfig {
    pub secret: String,
    pub algorithm: Algorithm,
    pub issuer: Option<String>,
    pub audience: Option<String>,
    pub leeway: Duration,
    pub require_exp: bool,
    pub require_iat: bool,
    pub require_nbf: bool,
    pub allowed_algorithms: HashSet<Algorithm>,
}

impl Default for JwtConfig {
    fn default() -> Self {
        let mut allowed_algorithms = HashSet::new();
        allowed_algorithms.insert(Algorithm::HS256);
        allowed_algorithms.insert(Algorithm::HS384);
        allowed_algorithms.insert(Algorithm::HS512);

        Self {
            secret: String::new(),
            algorithm: Algorithm::HS256,
            issuer: None,
            audience: None,
            leeway: Duration::seconds(30),
            require_exp: true,
            require_iat: true,
            require_nbf: false,
            allowed_algorithms,
        }
    }
}

/// Enhanced JWT validator with security hardening
pub struct JwtValidator {
    config: JwtConfig,
}

impl JwtValidator {
    /// Create new JWT validator
    pub fn new(config: JwtConfig) -> Self {
        Self { config }
    }

    /// Create validator from app config
    pub fn from_app_config(app_config: &AppConfig) -> Result<Self> {
        let secret = app_config.get_jwt_secret();
        if secret.len() < 32 {
            return Err(anyhow::anyhow!(
                "JWT secret must be at least 32 characters long for security"
            ));
        }

        let mut allowed_algorithms = HashSet::new();
        allowed_algorithms.insert(Algorithm::HS256);

        let jwt_config = JwtConfig {
            secret,
            algorithm: Algorithm::HS256,
            issuer: app_config.jwt_issuer.clone(),
            audience: app_config.jwt_audience.clone(),
            leeway: Duration::seconds(30),
            require_exp: true,
            require_iat: true,
            require_nbf: false,
            allowed_algorithms,
        };

        Ok(Self::new(jwt_config))
    }

    /// Validate JWT token with comprehensive security checks
    pub fn validate_token(&self, token: &str) -> Result<ValidationResult> {
        let validation_start = Utc::now();

        // Step 1: Basic format validation
        self.validate_format(token)
            .context("Token format validation failed")?;

        // Step 2: Parse header without verification for algorithm check
        let header = self.parse_header(token).context("Header parsing failed")?;

        // Step 3: Algorithm validation (prevent algorithm confusion attacks)
        self.validate_algorithm(&header)
            .context("Algorithm validation failed")?;

        // Step 4: JWT verification with proper validation settings
        let token_data = self
            .verify_token(token)
            .context("Token verification failed")?;

        // Step 5: Enhanced claims validation
        token_data
            .claims
            .validate_security(&self.get_app_config_mock())
            .context("Claims security validation failed")?;

        let time_to_expiry = token_data.claims.time_to_expiry();
        let is_refresh_needed = token_data.claims.expires_within(Duration::minutes(30));

        // Step 6: Audit logging
        self.audit_log_validation(&token_data.claims, true, None);

        Ok(ValidationResult {
            claims: token_data.claims,
            header: token_data.header,
            is_refresh_needed,
            time_to_expiry,
            validation_timestamp: validation_start,
        })
    }

    /// Validate token format (basic JWT structure)
    fn validate_format(&self, token: &str) -> Result<()> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(anyhow::anyhow!(
                "Invalid JWT format: expected 3 parts separated by dots"
            ));
        }

        // Basic base64 validation
        for (i, part) in parts.iter().enumerate() {
            if part.is_empty() {
                return Err(anyhow::anyhow!("JWT part {i} is empty"));
            }

            // Validate base64url format
            if !part
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            {
                return Err(anyhow::anyhow!("JWT part {i} contains invalid characters"));
            }
        }

        Ok(())
    }

    /// Parse JWT header without verification
    fn parse_header(&self, token: &str) -> Result<Header> {
        let parts: Vec<&str> = token.split('.').collect();
        let header_part = parts[0];

        // Decode header using manual base64 implementation
        let header_bytes = self
            .decode_base64url(header_part)
            .context("Failed to decode JWT header")?;

        let header: Header =
            sonic_rs::from_slice(&header_bytes).context("Failed to parse JWT header")?;

        Ok(header)
    }

    /// Validate algorithm to prevent algorithm confusion attacks
    fn validate_algorithm(&self, header: &Header) -> Result<()> {
        if !self.config.allowed_algorithms.contains(&header.alg) {
            return Err(anyhow::anyhow!(
                "Algorithm {:?} is not allowed. Allowed algorithms: {:?}",
                header.alg,
                self.config.allowed_algorithms
            ));
        }

        Ok(())
    }

    /// Verify JWT token with proper validation settings
    fn verify_token(&self, token: &str) -> Result<TokenData<Claims>> {
        let decoding_key = DecodingKey::from_secret(self.config.secret.as_ref());

        let mut validation = Validation::new(self.config.algorithm);
        validation.validate_exp = self.config.require_exp;
        validation.validate_nbf = self.config.require_nbf;
        validation.leeway = self.config.leeway.num_seconds() as u64;
        validation.algorithms = vec![self.config.algorithm];

        // Disable audience validation if no audience is configured
        // (otherwise jsonwebtoken defaults validate_aud=true and fails
        //  on tokens that include an "aud" claim)
        if let Some(ref audience) = self.config.audience {
            validation.aud = Some(HashSet::from([audience.clone()]));
        } else {
            validation.validate_aud = false;
        }

        if let Some(ref issuer) = self.config.issuer {
            validation.iss = Some(HashSet::from([issuer.clone()]));
        }

        decode::<Claims>(token, &decoding_key, &validation).map_err(|e| {
            self.audit_log_validation_error(&e);
            anyhow::anyhow!("JWT verification failed: {e}")
        })
    }

    /// Decode a base64url-encoded string to bytes using the `base64` crate.
    fn decode_base64url(&self, input: &str) -> Result<Vec<u8>> {
        URL_SAFE_NO_PAD
            .decode(input.as_bytes())
            .map_err(|e| anyhow::anyhow!("Failed to decode base64url segment: {e}"))
    }

    /// Audit log for validation attempts
    fn audit_log_validation(&self, claims: &Claims, success: bool, error: Option<&str>) {
        if success {
            debug!(
                "JWT validation successful: user_id={}, expires_in={}",
                claims.sub,
                claims.time_to_expiry().num_seconds()
            );
        } else {
            warn!(
                "JWT validation failed: user_id={}, error={}",
                claims.sub,
                error.unwrap_or("unknown")
            );
        }
    }

    /// Audit log for validation errors
    fn audit_log_validation_error(&self, error: &JwtError) {
        match error.kind() {
            ErrorKind::ExpiredSignature => {
                warn!("JWT validation failed: Token expired");
            }
            ErrorKind::InvalidSignature => {
                error!("JWT validation failed: Invalid signature - possible tampering");
            }
            ErrorKind::InvalidToken => {
                error!("JWT validation failed: Invalid token format");
            }
            ErrorKind::InvalidIssuer => {
                warn!("JWT validation failed: Invalid issuer");
            }
            ErrorKind::InvalidAudience => {
                warn!("JWT validation failed: Invalid audience");
            }
            _ => {
                error!("JWT validation failed: {}", error);
            }
        }
    }

    /// Mock app config for validation (in real implementation, this would be injected)
    fn get_app_config_mock(&self) -> AppConfig {
        let mut config = AppConfig::default();
        config.jwt_issuer = self.config.issuer.clone();
        config.jwt_audience = self.config.audience.clone();
        config
    }
}

/// Simplified validation function for backward compatibility
pub fn validate_token(token: &str, config: &AppConfig) -> Result<Claims, JwtError> {
    match JwtValidator::from_app_config(config) {
        Ok(validator) => match validator.validate_token(token) {
            Ok(result) => Ok(result.claims),
            Err(e) => {
                debug!("JWT validation error: {}", e);
                Err(JwtError::from(
                    jsonwebtoken::errors::ErrorKind::InvalidToken,
                ))
            }
        },
        Err(e) => {
            error!("Failed to create JWT validator: {}", e);
            Err(JwtError::from(
                jsonwebtoken::errors::ErrorKind::InvalidToken,
            ))
        }
    }
}

/// Enhanced validation function with detailed result
pub fn validate_token_enhanced(token: &str, config: &AppConfig) -> Result<ValidationResult> {
    let validator =
        JwtValidator::from_app_config(config).context("Failed to create JWT validator")?;

    validator
        .validate_token(token)
        .context("Token validation failed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;

    fn test_config() -> AppConfig {
        let mut c = AppConfig::default();
        c.jwt_secret = "test-jwt-secret-for-testing-at-least-32-chars!".to_string();
        c.jwt_issuer = None;
        c.jwt_audience = None;
        c
    }

    fn make_claims(exp_offset: i64) -> Claims {
        Claims {
            sub: "u1".into(),
            exp: Utc::now().timestamp() + exp_offset,
            iat: Utc::now().timestamp(),
            nbf: None,
            iss: None,
            aud: None,
            jti: None,
            permissions: None,
            session_id: None,
            client_ip: None,
            device_id: None,
            metadata: None,
            auth_method: None,
        }
    }

    fn make_token_config() -> AppConfig {
        let mut c = AppConfig::default();
        c.jwt_secret = "test-jwt-secret-for-testing-at-least-32-chars!".to_string();
        c.jwt_issuer = None;
        c.jwt_audience = None;
        c
    }

    fn create_test_token(claims: &Claims, secret: &str) -> String {
        let header = jsonwebtoken::Header::new(Algorithm::HS256);
        jsonwebtoken::encode(
            &header,
            claims,
            &jsonwebtoken::EncodingKey::from_secret(secret.as_ref()),
        )
        .unwrap()
    }

    // --- JwtValidator construction ---

    #[test]
    fn test_jwt_validator_from_config() {
        assert!(JwtValidator::from_app_config(&test_config()).is_ok());
    }

    #[test]
    fn test_jwt_validator_weak_secret() {
        let mut config = test_config();
        config.jwt_secret = "short".to_string();
        assert!(JwtValidator::from_app_config(&config).is_err());
    }

    // --- Claims::validate_security ---

    #[test]
    fn test_claims_valid() {
        assert!(make_claims(3600).validate_security(&test_config()).is_ok());
    }

    #[test]
    fn test_claims_expired() {
        assert!(make_claims(-3600)
            .validate_security(&test_config())
            .is_err());
    }

    #[test]
    fn test_claims_nbf_future_fails() {
        let mut claims = make_claims(3600);
        claims.nbf = Some(Utc::now().timestamp() + 3600);
        assert!(claims.validate_security(&test_config()).is_err());
    }

    #[test]
    fn test_claims_nbf_past_succeeds() {
        let mut claims = make_claims(3600);
        claims.nbf = Some(Utc::now().timestamp() - 3600);
        assert!(claims.validate_security(&test_config()).is_ok());
    }

    #[test]
    fn test_claims_issuer_mismatch_fails() {
        let mut config = test_config();
        config.jwt_issuer = Some("expected-issuer".to_string());
        let claims = Claims {
            iss: Some("wrong-issuer".to_string()),
            ..make_claims(3600)
        };
        assert!(claims.validate_security(&config).is_err());
    }

    #[test]
    fn test_claims_issuer_match_succeeds() {
        let mut config = test_config();
        config.jwt_issuer = Some("expected-issuer".to_string());
        let claims = Claims {
            iss: Some("expected-issuer".to_string()),
            ..make_claims(3600)
        };
        assert!(claims.validate_security(&config).is_ok());
    }

    #[test]
    fn test_claims_issuer_missing_when_configured_fails() {
        let mut config = test_config();
        config.jwt_issuer = Some("required-issuer".to_string());
        let claims = Claims {
            iss: None,
            ..make_claims(3600)
        };
        assert!(claims.validate_security(&config).is_err());
    }

    #[test]
    fn test_claims_audience_mismatch_fails() {
        let mut config = test_config();
        config.jwt_audience = Some("expected-audience".to_string());
        let claims = Claims {
            aud: Some("wrong-audience".to_string()),
            ..make_claims(3600)
        };
        assert!(claims.validate_security(&config).is_err());
    }

    #[test]
    fn test_claims_audience_match_succeeds() {
        let mut config = test_config();
        config.jwt_audience = Some("expected-audience".to_string());
        let claims = Claims {
            aud: Some("expected-audience".to_string()),
            ..make_claims(3600)
        };
        assert!(claims.validate_security(&config).is_ok());
    }

    #[test]
    fn test_claims_audience_missing_when_configured_fails() {
        let mut config = test_config();
        config.jwt_audience = Some("required-audience".to_string());
        let claims = Claims {
            aud: None,
            ..make_claims(3600)
        };
        assert!(claims.validate_security(&config).is_err());
    }

    #[test]
    fn test_claims_jti_tracked() {
        let claims = Claims {
            jti: Some("unique-jti-123".to_string()),
            ..make_claims(3600)
        };
        assert_eq!(claims.jti.as_deref(), Some("unique-jti-123"));
        assert!(claims.validate_security(&test_config()).is_ok());
    }

    #[test]
    fn test_claims_empty_subject_fails() {
        let mut claims = make_claims(3600);
        claims.sub = "   ".to_string();
        assert!(claims.validate_security(&test_config()).is_err());
    }

    // --- Token creation / validation round-trips ---

    #[test]
    fn test_token_roundtrip_hs256() {
        let config = make_token_config();
        let claims = make_claims(3600);
        let token = create_test_token(&claims, &config.jwt_secret);
        let validator = JwtValidator::from_app_config(&config).unwrap();
        let result = validator.validate_token(&token).unwrap();
        assert_eq!(result.claims.sub, "u1");
        assert!(result.claims.exp > Utc::now().timestamp());
    }

    #[test]
    fn test_modified_token_fails() {
        let config = make_token_config();
        let claims = make_claims(3600);
        let token = create_test_token(&claims, &config.jwt_secret);
        let parts: Vec<&str> = token.split('.').collect();
        let modified_payload = format!("{}x", &parts[1][..parts[1].len() - 1]);
        let modified = format!("{}.{}.{}", parts[0], modified_payload, parts[2]);
        let validator = JwtValidator::from_app_config(&config).unwrap();
        assert!(validator.validate_token(&modified).is_err());
    }

    #[test]
    fn test_expired_token_validation_fails() {
        let config = make_token_config();
        let claims = make_claims(-3600);
        let token = create_test_token(&claims, &config.jwt_secret);
        let validator = JwtValidator::from_app_config(&config).unwrap();
        assert!(validator.validate_token(&token).is_err());
    }

    #[test]
    fn test_token_wrong_key_fails() {
        let config = make_token_config();
        let claims = make_claims(3600);
        let token = create_test_token(&claims, "different-secret-that-is-also-32-chars-long!!");
        let validator = JwtValidator::from_app_config(&config).unwrap();
        assert!(validator.validate_token(&token).is_err());
    }

    // --- Algorithm confusion / security ---

    #[test]
    fn test_alg_none_token_rejected() {
        let config = make_token_config();
        let header_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            b"{\"alg\":\"none\",\"typ\":\"JWT\"}",
        );
        let payload_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            b"{\"sub\":\"u1\",\"exp\":9999999999,\"iat\":1000000000}",
        );
        let token = format!("{header_b64}.{payload_b64}.");
        let validator = JwtValidator::from_app_config(&config).unwrap();
        assert!(validator.validate_token(&token).is_err());
    }

    #[test]
    fn test_alg_hs512_token_rejected_when_only_hs256_allowed() {
        let config = make_token_config();
        let claims = make_claims(3600);
        let header = jsonwebtoken::Header::new(Algorithm::HS512);
        let token = jsonwebtoken::encode(
            &header,
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(config.jwt_secret.as_ref()),
        )
        .unwrap();
        let validator = JwtValidator::from_app_config(&config).unwrap();
        assert!(validator.validate_token(&token).is_err());
    }

    #[test]
    fn test_validate_algorithm_rejects_wrong_alg() {
        let config = test_config();
        let validator = JwtValidator::from_app_config(&config).unwrap();
        let header = jsonwebtoken::Header::new(Algorithm::HS512);
        assert!(validator.validate_algorithm(&header).is_err());
    }

    // --- Token with full claims ---

    #[test]
    fn test_token_with_issuer_and_audience_roundtrip() {
        let mut config = make_token_config();
        config.jwt_issuer = Some("test-issuer".to_string());
        config.jwt_audience = Some("test-audience".to_string());
        let claims = Claims {
            sub: "u1".into(),
            exp: Utc::now().timestamp() + 3600,
            iat: Utc::now().timestamp(),
            nbf: None,
            iss: Some("test-issuer".into()),
            aud: Some("test-audience".into()),
            jti: Some("token-1".into()),
            permissions: None,
            session_id: None,
            client_ip: None,
            device_id: None,
            metadata: None,
            auth_method: None,
        };
        let token = create_test_token(&claims, &config.jwt_secret);
        let validator = JwtValidator::from_app_config(&config).unwrap();
        let result = validator.validate_token(&token).unwrap();
        assert_eq!(result.claims.iss.as_deref(), Some("test-issuer"));
        assert_eq!(result.claims.aud.as_deref(), Some("test-audience"));
        assert_eq!(result.claims.jti.as_deref(), Some("token-1"));
    }

    // --- Backward-compat functions ---

    #[test]
    fn test_validate_token_backward_compat_ok() {
        let config = make_token_config();
        let claims = make_claims(3600);
        let token = create_test_token(&claims, &config.jwt_secret);
        let result = validate_token(&token, &config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_token_backward_compat_invalid() {
        let config = make_token_config();
        let result = validate_token("invalid.token.here", &config);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_token_enhanced_ok() {
        let config = make_token_config();
        let claims = make_claims(3600);
        let token = create_test_token(&claims, &config.jwt_secret);
        let result = validate_token_enhanced(&token, &config);
        assert!(result.is_ok());
        let vr = result.unwrap();
        assert!(!vr.is_refresh_needed);
    }

    #[test]
    fn test_validate_token_enhanced_rejects_bad_token() {
        let config = make_token_config();
        let result = validate_token_enhanced("bad.token.here", &config);
        assert!(result.is_err());
    }

    // --- Edge cases ---

    #[test]
    fn test_time_to_expiry_expired() {
        let claims = make_claims(-3600);
        let tte = claims.time_to_expiry();
        assert_eq!(tte, chrono::Duration::zero());
    }

    #[test]
    fn test_expires_within_expired_token() {
        assert!(make_claims(-10).expires_within(chrono::Duration::minutes(1)));
    }

    #[test]
    fn test_expires_within_fresh_token() {
        assert!(!make_claims(7200).expires_within(chrono::Duration::hours(1)));
    }

    // --- Format validation edge cases ---

    #[test]
    fn test_validate_format() {
        let v = JwtValidator::from_app_config(&test_config()).unwrap();
        assert!(v.validate_format("a.b.c").is_ok());
        assert!(v.validate_format("a.b").is_err());
        assert!(v.validate_format("").is_err());
    }

    #[test]
    fn test_validate_format_empty_parts() {
        let v = JwtValidator::from_app_config(&test_config()).unwrap();
        assert!(v.validate_format("a..c").is_err());
        assert!(v.validate_format(".b.c").is_err());
        assert!(v.validate_format("a.b.").is_err());
    }

    #[test]
    fn test_validate_format_invalid_chars() {
        let v = JwtValidator::from_app_config(&test_config()).unwrap();
        assert!(v.validate_format("a!b.c").is_err());
    }

    #[test]
    fn test_decode_base64url() {
        let data = "test data";
        let enc = base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            data.as_bytes(),
        );
        let v = JwtValidator::from_app_config(&test_config()).unwrap();
        assert_eq!(v.decode_base64url(&enc).unwrap(), data.as_bytes());
    }

    #[test]
    fn test_decode_base64url_invalid() {
        let v = JwtValidator::from_app_config(&test_config()).unwrap();
        assert!(v.decode_base64url("!!!invalid!!!").is_err());
    }

    // --- ValidationResult structure ---

    #[test]
    fn test_validation_result_structure() {
        let config = make_token_config();
        let claims = make_claims(3600);
        let token = create_test_token(&claims, &config.jwt_secret);
        let validator = JwtValidator::from_app_config(&config).unwrap();
        let result = validator.validate_token(&token).unwrap();
        assert_eq!(result.claims.sub, "u1");
        assert!(result.time_to_expiry > chrono::Duration::zero());
        assert!(!result.is_refresh_needed);
    }

    #[test]
    fn test_validation_result_near_expiry_triggers_refresh() {
        let config = make_token_config();
        let claims = make_claims(1200);
        let token = create_test_token(&claims, &config.jwt_secret);
        let validator = JwtValidator::from_app_config(&config).unwrap();
        let result = validator.validate_token(&token).unwrap();
        assert!(result.is_refresh_needed);
    }
}
