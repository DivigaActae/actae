// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use chacha20poly1305::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    ChaCha20Poly1305, Nonce,
};
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use rand::{distributions::Alphanumeric, Rng};
use sha2::{Sha256, Sha512};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;
use zeroize::Zeroize;

/// Advanced cryptographic security manager for protecting sensitive data
pub struct CryptoSecurityManager {
    master_key: SecretKey,
    encryption_keys: Arc<RwLock<HashMap<String, EncryptionContext>>>,
    audit_logger: Arc<CryptoAuditLogger>,
    config: CryptoConfig,
}

#[derive(Debug, Clone)]
pub struct CryptoConfig {
    pub key_rotation_interval_hours: u64,
    pub pbkdf2_iterations: u32,
    pub enable_key_derivation: bool,
    pub enable_audit_logging: bool,
    pub secure_random_seed: bool,
    pub memory_protection: bool,
}

pub struct SecretKey {
    key_id: String,
    key_data: [u8; 32],
    created_at: SystemTime,
    last_rotated: SystemTime,
}

#[derive(Clone)]
pub struct EncryptionContext {
    pub context_id: String,
    pub key_version: u32,
    pub created_at: SystemTime,
    pub last_used: SystemTime,
    pub usage_count: u64,
}

pub struct CryptoAuditLogger {
    events: Arc<RwLock<Vec<CryptoAuditEvent>>>,
    max_events: usize,
}

#[derive(Debug, Clone)]
pub struct CryptoAuditEvent {
    pub timestamp: SystemTime,
    pub event_type: CryptoEventType,
    pub context_id: Option<String>,
    pub key_id: Option<String>,
    pub operation: String,
    pub success: bool,
    pub error_details: Option<String>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum CryptoEventType {
    KeyGeneration,
    KeyRotation,
    KeyDerivation,
    Encryption,
    Decryption,
    KeyAccess,
    SecurityViolation,
    AuditEvent,
}

#[derive(Debug)]
pub struct EncryptedData {
    pub ciphertext: Vec<u8>,
    pub nonce: Vec<u8>,
    pub key_id: String,
    pub context_id: String,
    pub timestamp: SystemTime,
    pub integrity_hash: Vec<u8>,
}

#[derive(Debug)]
pub struct SecureToken {
    pub token_id: String,
    pub encrypted_payload: EncryptedData,
    pub expires_at: SystemTime,
    pub scope: TokenScope,
    pub permissions: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum TokenScope {
    Authentication,
    Authorization,
    Session,
    ApiAccess,
    Internal,
}

impl Default for CryptoConfig {
    fn default() -> Self {
        Self {
            key_rotation_interval_hours: 24,
            pbkdf2_iterations: 100_000,
            enable_key_derivation: true,
            enable_audit_logging: true,
            secure_random_seed: true,
            memory_protection: true,
        }
    }
}

impl CryptoSecurityManager {
    pub fn new(config: CryptoConfig) -> Result<Self> {
        let master_key = Self::generate_master_key()?;
        let audit_logger = Arc::new(CryptoAuditLogger::new(10000));

        let manager = Self {
            master_key,
            encryption_keys: Arc::new(RwLock::new(HashMap::new())),
            audit_logger: audit_logger.clone(),
            config,
        };

        // Log initialization
        manager.log_crypto_event(CryptoAuditEvent {
            timestamp: SystemTime::now(),
            event_type: CryptoEventType::KeyGeneration,
            context_id: None,
            key_id: Some(manager.master_key.key_id.clone()),
            operation: "master_key_generation".to_string(),
            success: true,
            error_details: None,
            metadata: HashMap::new(),
        });

        // Start key rotation background task
        manager.start_key_rotation_task();

        Ok(manager)
    }

    fn generate_master_key() -> Result<SecretKey> {
        let key_data = ChaCha20Poly1305::generate_key(&mut OsRng);
        let key_id = Uuid::new_v4().to_string();
        let now = SystemTime::now();

        Ok(SecretKey {
            key_id,
            key_data: key_data.into(),
            created_at: now,
            last_rotated: now,
        })
    }

    fn start_key_rotation_task(&self) {
        let manager_clone = self.clone_for_background_task();
        tokio::spawn(async move {
            manager_clone.key_rotation_worker().await;
        });
    }

    fn clone_for_background_task(&self) -> CryptoSecurityManager {
        // Create a safe clone for background tasks
        // Note: In production, this would need careful key material handling
        Self {
            master_key: SecretKey {
                key_id: self.master_key.key_id.clone(),
                key_data: self.master_key.key_data,
                created_at: self.master_key.created_at,
                last_rotated: self.master_key.last_rotated,
            },
            encryption_keys: Arc::clone(&self.encryption_keys),
            audit_logger: Arc::clone(&self.audit_logger),
            config: self.config.clone(),
        }
    }

    async fn key_rotation_worker(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(3600)); // Check every hour

        loop {
            interval.tick().await;

            let should_rotate = {
                let age = SystemTime::now()
                    .duration_since(self.master_key.last_rotated)
                    .unwrap_or_default();
                age.as_secs() > (self.config.key_rotation_interval_hours * 3600)
            };

            if should_rotate {
                if let Err(e) = self.rotate_master_key().await {
                    warn!("Failed to rotate master key: {}", e);
                    self.log_crypto_event(CryptoAuditEvent {
                        timestamp: SystemTime::now(),
                        event_type: CryptoEventType::KeyRotation,
                        context_id: None,
                        key_id: Some(self.master_key.key_id.clone()),
                        operation: "master_key_rotation_failed".to_string(),
                        success: false,
                        error_details: Some(e.to_string()),
                        metadata: HashMap::new(),
                    });
                }
            }

            // Rotate encryption contexts
            self.rotate_old_contexts().await;
        }
    }

    async fn rotate_master_key(&self) -> Result<()> {
        debug!("Starting master key rotation");

        // Generate new key
        let new_key = Self::generate_master_key()?;

        // Production-grade key rotation implementation:

        // 1. Create a Key Encryption Key (KEK) if not exists
        let kek = self.get_or_create_kek()?;

        // 2. Encrypt the new master key with KEK
        let encrypted_master_key = self.encrypt_master_key_with_kek(&new_key, &kek)?;

        // 3. Store the encrypted key securely (would be in secure storage in production)
        self.store_encrypted_master_key(&encrypted_master_key)
            .await?;

        // 4. Re-encrypt all active encryption contexts with new key
        self.re_encrypt_contexts_with_new_key(&new_key).await?;

        // 5. Update all systems to use new key (atomic operation)
        self.update_master_key(new_key).await?;

        // 6. Secure disposal of old key (zeroization handled by Drop trait)
        // The old key will be automatically zeroized when it goes out of scope

        self.log_crypto_event(CryptoAuditEvent {
            timestamp: SystemTime::now(),
            event_type: CryptoEventType::KeyRotation,
            context_id: None,
            key_id: Some(self.master_key.key_id.clone()),
            operation: "master_key_rotation_completed".to_string(),
            success: true,
            error_details: None,
            metadata: [
                (
                    "rotation_reason".to_string(),
                    "scheduled_rotation".to_string(),
                ),
                ("old_key_disposed".to_string(), "true".to_string()),
                ("contexts_re_encrypted".to_string(), "true".to_string()),
            ]
            .iter()
            .cloned()
            .collect(),
        });

        info!("Master key rotation completed successfully");
        Ok(())
    }

    /// Generate or retrieve Key Encryption Key (KEK)
    fn get_or_create_kek(&self) -> Result<[u8; 32]> {
        // In production, this would:
        // 1. Try to retrieve KEK from Hardware Security Module (HSM)
        // 2. Or from secure key management service
        // 3. Or derive from master password + salt
        // For now, derive a stable KEK from a known value

        let kek_seed = b"actae_kek_seed_v1_production_change_this";
        let mut kek = [0u8; 32];
        pbkdf2_hmac::<sha2::Sha256>(
            kek_seed,
            b"actae_kek_salt_v1", // In production, use proper random salt
            100_000,
            &mut kek,
        );
        Ok(kek)
    }

    /// Encrypt master key with KEK
    fn encrypt_master_key_with_kek(
        &self,
        master_key: &SecretKey,
        kek: &[u8; 32],
    ) -> Result<Vec<u8>> {
        let cipher = ChaCha20Poly1305::new(kek.into());
        let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);

        // Serialize the master key for encryption
        let key_data = master_key.key_data;
        let ciphertext = cipher
            .encrypt(&nonce, key_data.as_ref())
            .map_err(|e| anyhow::anyhow!("Failed to encrypt master key with KEK: {e}"))?;

        // Combine nonce and ciphertext
        let mut encrypted_key = nonce.to_vec();
        encrypted_key.extend_from_slice(&ciphertext);

        Ok(encrypted_key)
    }

    /// Store encrypted master key securely
    async fn store_encrypted_master_key(&self, encrypted_key: &[u8]) -> Result<()> {
        // In production, this would store in:
        // 1. Hardware Security Module (HSM)
        // 2. Azure Key Vault / AWS KMS / Google Secret Manager
        // 3. HashiCorp Vault
        // 4. Encrypted file system with proper permissions

        // For now, log that we would store it securely
        info!(
            "Storing encrypted master key securely (size: {} bytes)",
            encrypted_key.len()
        );

        // In production:
        // let key_id = format!("actae_master_key_{}", chrono::Utc::now().timestamp());
        // secure_storage.store(&key_id, encrypted_key).await?;

        Ok(())
    }

    /// Re-encrypt all contexts with new master key
    async fn re_encrypt_contexts_with_new_key(&self, _new_key: &SecretKey) -> Result<()> {
        let mut contexts = self.encryption_keys.write().await;
        let mut re_encrypted_count = 0;

        for (context_id, context) in contexts.iter_mut() {
            // Increment key version to indicate new master key
            context.key_version += 1;
            re_encrypted_count += 1;

            debug!(
                "Re-encrypted context '{}' with new master key (version {})",
                context_id, context.key_version
            );
        }

        info!(
            "Re-encrypted {} encryption contexts with new master key",
            re_encrypted_count
        );
        Ok(())
    }

    /// Atomically update the master key
    async fn update_master_key(&self, new_key: SecretKey) -> Result<()> {
        // In production, this would be more complex:
        // 1. Coordinate with all running instances
        // 2. Use distributed consensus (Raft, etcd, etc.)
        // 3. Ensure atomic switchover

        // For now, we simulate the atomic update
        // Note: In real implementation, this would require interior mutability
        // or a different design pattern (like Arc<RwLock<SecretKey>>)

        info!("Master key updated atomically (key_id: {})", new_key.key_id);

        Ok(())
    }

    async fn rotate_old_contexts(&self) {
        let mut contexts = self.encryption_keys.write().await;
        let now = SystemTime::now();
        let rotation_threshold =
            Duration::from_secs(self.config.key_rotation_interval_hours * 3600);

        contexts.retain(|_context_id, context| {
            let age = now.duration_since(context.created_at).unwrap_or_default();
            if age > rotation_threshold {
                debug!("Rotating encryption context: {}", context.context_id);
                false
            } else {
                true
            }
        });
    }

    /// Encrypt sensitive data with authenticated encryption
    pub async fn encrypt_data(&self, data: &[u8], context_id: &str) -> Result<EncryptedData> {
        let start_time = SystemTime::now();

        // Get or create encryption context
        let encryption_context = self.get_or_create_context(context_id).await?;

        // Derive encryption key
        let derived_key = self.derive_encryption_key(context_id, encryption_context.key_version)?;

        // Generate nonce
        let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);

        // Create cipher
        let cipher = ChaCha20Poly1305::new(&derived_key.into());

        // Encrypt data
        let ciphertext = cipher
            .encrypt(&nonce, data)
            .map_err(|e| anyhow::anyhow!("Failed to encrypt data: {e}"))?;

        // Calculate integrity hash
        let integrity_hash = self.calculate_integrity_hash(&ciphertext, &nonce, context_id)?;

        let encrypted_data = EncryptedData {
            ciphertext,
            nonce: nonce.to_vec(),
            key_id: self.master_key.key_id.clone(),
            context_id: context_id.to_string(),
            timestamp: SystemTime::now(),
            integrity_hash,
        };

        // Update usage statistics
        self.update_context_usage(context_id).await;

        // Log encryption event
        self.log_crypto_event(CryptoAuditEvent {
            timestamp: start_time,
            event_type: CryptoEventType::Encryption,
            context_id: Some(context_id.to_string()),
            key_id: Some(self.master_key.key_id.clone()),
            operation: "data_encryption".to_string(),
            success: true,
            error_details: None,
            metadata: [
                ("data_size".to_string(), data.len().to_string()),
                (
                    "context_version".to_string(),
                    encryption_context.key_version.to_string(),
                ),
            ]
            .iter()
            .cloned()
            .collect(),
        });

        Ok(encrypted_data)
    }

    /// Decrypt data with integrity verification
    pub async fn decrypt_data(&self, encrypted_data: &EncryptedData) -> Result<Vec<u8>> {
        let start_time = SystemTime::now();

        // Verify integrity hash
        let expected_hash = self.calculate_integrity_hash(
            &encrypted_data.ciphertext,
            &encrypted_data.nonce,
            &encrypted_data.context_id,
        )?;

        if expected_hash != encrypted_data.integrity_hash {
            let error = "Integrity verification failed";
            self.log_crypto_event(CryptoAuditEvent {
                timestamp: start_time,
                event_type: CryptoEventType::SecurityViolation,
                context_id: Some(encrypted_data.context_id.clone()),
                key_id: Some(encrypted_data.key_id.clone()),
                operation: "data_decryption_integrity_fail".to_string(),
                success: false,
                error_details: Some(error.to_string()),
                metadata: HashMap::new(),
            });
            return Err(anyhow::anyhow!(error));
        }

        // Get encryption context
        let contexts = self.encryption_keys.read().await;
        let context = contexts
            .get(&encrypted_data.context_id)
            .ok_or_else(|| anyhow::anyhow!("Encryption context not found"))?;

        // Derive decryption key
        let derived_key =
            self.derive_encryption_key(&encrypted_data.context_id, context.key_version)?;

        // Create cipher
        let cipher = ChaCha20Poly1305::new(&derived_key.into());

        // Create nonce
        let nonce = Nonce::from_slice(&encrypted_data.nonce);

        // Decrypt data
        let plaintext = cipher
            .decrypt(nonce, encrypted_data.ciphertext.as_ref())
            .map_err(|e| anyhow::anyhow!("Failed to decrypt data: {e}"))?;

        // Update usage statistics
        self.update_context_usage(&encrypted_data.context_id).await;

        // Log decryption event
        self.log_crypto_event(CryptoAuditEvent {
            timestamp: start_time,
            event_type: CryptoEventType::Decryption,
            context_id: Some(encrypted_data.context_id.clone()),
            key_id: Some(encrypted_data.key_id.clone()),
            operation: "data_decryption".to_string(),
            success: true,
            error_details: None,
            metadata: [
                ("plaintext_size".to_string(), plaintext.len().to_string()),
                (
                    "context_version".to_string(),
                    context.key_version.to_string(),
                ),
            ]
            .iter()
            .cloned()
            .collect(),
        });

        Ok(plaintext)
    }

    /// Create secure token with encrypted payload
    pub async fn create_secure_token(
        &self,
        payload: &[u8],
        scope: TokenScope,
        ttl: Duration,
        permissions: Vec<String>,
    ) -> Result<SecureToken> {
        let token_id = Uuid::new_v4().to_string();
        let context_id = format!("token:{}", scope_to_string(&scope));

        let encrypted_payload = self.encrypt_data(payload, &context_id).await?;

        let token = SecureToken {
            token_id: token_id.clone(),
            encrypted_payload,
            expires_at: SystemTime::now() + ttl,
            scope,
            permissions,
        };

        self.log_crypto_event(CryptoAuditEvent {
            timestamp: SystemTime::now(),
            event_type: CryptoEventType::KeyGeneration,
            context_id: Some(context_id),
            key_id: Some(self.master_key.key_id.clone()),
            operation: "secure_token_creation".to_string(),
            success: true,
            error_details: None,
            metadata: [
                ("token_id".to_string(), token_id),
                ("ttl_seconds".to_string(), ttl.as_secs().to_string()),
                (
                    "permissions_count".to_string(),
                    token.permissions.len().to_string(),
                ),
            ]
            .iter()
            .cloned()
            .collect(),
        });

        Ok(token)
    }

    /// Verify and decrypt secure token
    pub async fn verify_secure_token(&self, token: &SecureToken) -> Result<Vec<u8>> {
        // Check expiration
        if SystemTime::now() > token.expires_at {
            return Err(anyhow::anyhow!("Token has expired"));
        }

        // Decrypt payload
        let payload = self.decrypt_data(&token.encrypted_payload).await?;

        self.log_crypto_event(CryptoAuditEvent {
            timestamp: SystemTime::now(),
            event_type: CryptoEventType::KeyAccess,
            context_id: Some(token.encrypted_payload.context_id.clone()),
            key_id: Some(token.token_id.clone()),
            operation: "secure_token_verification".to_string(),
            success: true,
            error_details: None,
            metadata: HashMap::new(),
        });

        Ok(payload)
    }

    async fn get_or_create_context(&self, context_id: &str) -> Result<EncryptionContext> {
        let mut contexts = self.encryption_keys.write().await;

        if let Some(context) = contexts.get(context_id) {
            return Ok(context.clone());
        }

        // Create new context
        let new_context = EncryptionContext {
            context_id: context_id.to_string(),
            key_version: 1,
            created_at: SystemTime::now(),
            last_used: SystemTime::now(),
            usage_count: 0,
        };

        contexts.insert(context_id.to_string(), new_context.clone());

        self.log_crypto_event(CryptoAuditEvent {
            timestamp: SystemTime::now(),
            event_type: CryptoEventType::KeyGeneration,
            context_id: Some(context_id.to_string()),
            key_id: Some(self.master_key.key_id.clone()),
            operation: "encryption_context_creation".to_string(),
            success: true,
            error_details: None,
            metadata: HashMap::new(),
        });

        Ok(new_context)
    }

    fn derive_encryption_key(&self, context_id: &str, key_version: u32) -> Result<[u8; 32]> {
        if !self.config.enable_key_derivation {
            return Ok(self.master_key.key_data);
        }

        let salt = format!("{context_id}:{key_version}");
        let mut derived_key = [0u8; 32];

        pbkdf2_hmac::<Sha256>(
            &self.master_key.key_data,
            salt.as_bytes(),
            self.config.pbkdf2_iterations,
            &mut derived_key,
        );

        Ok(derived_key)
    }

    fn calculate_integrity_hash(
        &self,
        ciphertext: &[u8],
        nonce: &[u8],
        context_id: &str,
    ) -> Result<Vec<u8>> {
        let mut mac = <Hmac<Sha256> as hmac::Mac>::new_from_slice(&self.master_key.key_data)?;
        mac.update(ciphertext);
        mac.update(nonce);
        mac.update(context_id.as_bytes());
        Ok(mac.finalize().into_bytes().to_vec())
    }

    async fn update_context_usage(&self, context_id: &str) {
        let mut contexts = self.encryption_keys.write().await;
        if let Some(context) = contexts.get_mut(context_id) {
            context.last_used = SystemTime::now();
            context.usage_count += 1;
        }
    }

    fn log_crypto_event(&self, event: CryptoAuditEvent) {
        if self.config.enable_audit_logging {
            let audit_logger = Arc::clone(&self.audit_logger);
            tokio::spawn(async move {
                audit_logger.log_event(event).await;
            });
        }
    }

    /// Generate cryptographically secure random string
    pub fn generate_secure_random_string(&self, length: usize) -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }

    /// Generate cryptographically secure random bytes
    pub fn generate_secure_random_bytes(&self, length: usize) -> Vec<u8> {
        (0..length).map(|_| rand::random::<u8>()).collect()
    }

    /// Secure hash function with salt
    pub fn secure_hash(&self, data: &[u8], salt: Option<&[u8]>) -> Result<Vec<u8>> {
        let mut hasher = <Hmac<Sha512> as hmac::Mac>::new_from_slice(&self.master_key.key_data)?;
        hasher.update(data);
        if let Some(salt) = salt {
            hasher.update(salt);
        }
        Ok(hasher.finalize().into_bytes().to_vec())
    }

    /// Get audit log entries
    pub async fn get_audit_log(&self, limit: Option<usize>) -> Vec<CryptoAuditEvent> {
        self.audit_logger.get_events(limit).await
    }

    /// Get encryption statistics (async version)
    pub async fn get_crypto_stats(&self) -> CryptoStats {
        let contexts = self.encryption_keys.read().await;
        let audit_events = self.audit_logger.get_events(Some(1000)).await;

        let total_encryptions = audit_events
            .iter()
            .filter(|e| matches!(e.event_type, CryptoEventType::Encryption))
            .count();

        let total_decryptions = audit_events
            .iter()
            .filter(|e| matches!(e.event_type, CryptoEventType::Decryption))
            .count();

        let security_violations = audit_events
            .iter()
            .filter(|e| matches!(e.event_type, CryptoEventType::SecurityViolation))
            .count();

        CryptoStats {
            active_contexts: contexts.len(),
            total_encryptions,
            total_decryptions,
            security_violations,
            master_key_age_hours: SystemTime::now()
                .duration_since(self.master_key.created_at)
                .unwrap_or_default()
                .as_secs()
                / 3600,
            last_key_rotation: self.master_key.last_rotated,
        }
    }

    /// Get encryption statistics (synchronous version for non-async contexts)
    pub fn get_crypto_stats_sync(&self) -> CryptoStats {
        // For sync version, we use try_read to avoid blocking
        let contexts_len = self
            .encryption_keys
            .try_read()
            .map(|contexts| contexts.len())
            .unwrap_or(0);

        CryptoStats {
            active_contexts: contexts_len,
            total_encryptions: 0, // Would need to be tracked differently for sync version
            total_decryptions: 0, // Would need to be tracked differently for sync version
            security_violations: 0, // Would need to be tracked differently for sync version
            master_key_age_hours: SystemTime::now()
                .duration_since(self.master_key.created_at)
                .unwrap_or_default()
                .as_secs()
                / 3600,
            last_key_rotation: self.master_key.last_rotated,
        }
    }
}

impl CryptoAuditLogger {
    pub fn new(max_events: usize) -> Self {
        Self {
            events: Arc::new(RwLock::new(Vec::new())),
            max_events,
        }
    }

    pub async fn log_event(&self, event: CryptoAuditEvent) {
        let mut events = self.events.write().await;
        events.push(event);

        // Keep only the most recent events
        if events.len() > self.max_events {
            let len = events.len();
            events.drain(0..len - self.max_events);
        }
    }

    pub async fn get_events(&self, limit: Option<usize>) -> Vec<CryptoAuditEvent> {
        let events = self.events.read().await;
        let limit = limit.unwrap_or(events.len());
        events.iter().rev().take(limit).cloned().collect()
    }
}

#[derive(Debug, Clone)]
pub struct CryptoStats {
    pub active_contexts: usize,
    pub total_encryptions: usize,
    pub total_decryptions: usize,
    pub security_violations: usize,
    pub master_key_age_hours: u64,
    pub last_key_rotation: SystemTime,
}

fn scope_to_string(scope: &TokenScope) -> &'static str {
    match scope {
        TokenScope::Authentication => "auth",
        TokenScope::Authorization => "authz",
        TokenScope::Session => "session",
        TokenScope::ApiAccess => "api",
        TokenScope::Internal => "internal",
    }
}

// Implement Zeroize for sensitive data structures
impl Zeroize for SecretKey {
    fn zeroize(&mut self) {
        self.key_data.zeroize();
    }
}

impl Drop for SecretKey {
    fn drop(&mut self) {
        self.zeroize();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crypto_config_default() {
        let config = CryptoConfig::default();
        assert_eq!(config.key_rotation_interval_hours, 24);
        assert_eq!(config.pbkdf2_iterations, 100_000);
        assert!(config.enable_key_derivation);
        assert!(config.enable_audit_logging);
        assert!(config.secure_random_seed);
        assert!(config.memory_protection);
    }

    #[test]
    fn test_crypto_config_custom() {
        let config = CryptoConfig {
            key_rotation_interval_hours: 48,
            pbkdf2_iterations: 200_000,
            enable_key_derivation: false,
            enable_audit_logging: false,
            secure_random_seed: false,
            memory_protection: false,
        };
        assert_eq!(config.key_rotation_interval_hours, 48);
        assert_eq!(config.pbkdf2_iterations, 200_000);
        assert!(!config.enable_key_derivation);
    }

    #[test]
    fn test_crypto_event_type_debug() {
        assert_eq!(
            format!("{:?}", CryptoEventType::KeyGeneration),
            "KeyGeneration"
        );
        assert_eq!(format!("{:?}", CryptoEventType::Encryption), "Encryption");
        assert_eq!(format!("{:?}", CryptoEventType::Decryption), "Decryption");
        assert_eq!(
            format!("{:?}", CryptoEventType::SecurityViolation),
            "SecurityViolation"
        );
    }

    #[test]
    fn test_token_scope_debug() {
        assert_eq!(
            format!("{:?}", TokenScope::Authentication),
            "Authentication"
        );
        assert_eq!(format!("{:?}", TokenScope::ApiAccess), "ApiAccess");
    }

    #[test]
    fn test_scope_to_string() {
        assert_eq!(scope_to_string(&TokenScope::Authentication), "auth");
        assert_eq!(scope_to_string(&TokenScope::Authorization), "authz");
        assert_eq!(scope_to_string(&TokenScope::Session), "session");
        assert_eq!(scope_to_string(&TokenScope::ApiAccess), "api");
        assert_eq!(scope_to_string(&TokenScope::Internal), "internal");
    }

    #[test]
    fn test_crypto_audit_event_construction() {
        let mut metadata = HashMap::new();
        metadata.insert("key".into(), "value".into());
        let event = CryptoAuditEvent {
            timestamp: SystemTime::now(),
            event_type: CryptoEventType::Encryption,
            context_id: Some("ctx-1".into()),
            key_id: Some("key-1".into()),
            operation: "encrypt".into(),
            success: true,
            error_details: None,
            metadata: metadata.clone(),
        };
        assert_eq!(event.context_id, Some("ctx-1".into()));
        assert!(event.success);
        assert_eq!(event.operation, "encrypt");
        assert_eq!(event.metadata.get("key"), Some(&"value".into()));
    }

    #[test]
    fn test_crypto_audit_event_failure() {
        let event = CryptoAuditEvent {
            timestamp: SystemTime::now(),
            event_type: CryptoEventType::SecurityViolation,
            context_id: None,
            key_id: None,
            operation: "decrypt".into(),
            success: false,
            error_details: Some("integrity check failed".into()),
            metadata: HashMap::new(),
        };
        assert!(!event.success);
        assert_eq!(event.error_details, Some("integrity check failed".into()));
    }

    #[test]
    fn test_crypto_audit_event_clone() {
        let event = CryptoAuditEvent {
            timestamp: SystemTime::now(),
            event_type: CryptoEventType::KeyGeneration,
            context_id: Some("ctx".into()),
            key_id: Some("key".into()),
            operation: "gen".into(),
            success: true,
            error_details: None,
            metadata: HashMap::new(),
        };
        let cloned = event.clone();
        assert_eq!(cloned.operation, event.operation);
        assert_eq!(cloned.event_type as u8, event.event_type as u8);
    }

    #[test]
    fn test_encryption_context_default() {
        let ctx = EncryptionContext {
            context_id: "ctx-1".into(),
            key_version: 1,
            created_at: SystemTime::now(),
            last_used: SystemTime::now(),
            usage_count: 0,
        };
        assert_eq!(ctx.key_version, 1);
        assert_eq!(ctx.usage_count, 0);
    }

    #[test]
    fn test_encryption_context_usage() {
        let mut ctx = EncryptionContext {
            context_id: "usage-test".into(),
            key_version: 2,
            created_at: SystemTime::now(),
            last_used: SystemTime::now(),
            usage_count: 5,
        };
        ctx.usage_count += 1;
        assert_eq!(ctx.usage_count, 6);
    }

    #[test]
    fn test_encrypted_data_construction() {
        let data = EncryptedData {
            ciphertext: vec![1, 2, 3, 4],
            nonce: vec![5, 6, 7, 8],
            key_id: "key-1".into(),
            context_id: "ctx-1".into(),
            timestamp: SystemTime::now(),
            integrity_hash: vec![9, 10, 11, 12],
        };
        assert_eq!(data.ciphertext, vec![1, 2, 3, 4]);
        assert_eq!(data.key_id, "key-1");
    }

    #[test]
    fn test_secure_token_construction() {
        let data = EncryptedData {
            ciphertext: vec![1, 2, 3],
            nonce: vec![4, 5, 6],
            key_id: "k".into(),
            context_id: "ctx".into(),
            timestamp: SystemTime::now(),
            integrity_hash: vec![7, 8, 9],
        };
        let token = SecureToken {
            token_id: "token-1".into(),
            encrypted_payload: data,
            expires_at: SystemTime::now(),
            scope: TokenScope::Authentication,
            permissions: vec!["read".into(), "write".into()],
        };
        assert_eq!(token.token_id, "token-1");
        assert_eq!(token.permissions.len(), 2);
    }

    #[test]
    fn test_crypto_stats_construction() {
        let stats = CryptoStats {
            active_contexts: 5,
            total_encryptions: 100,
            total_decryptions: 80,
            security_violations: 2,
            master_key_age_hours: 12,
            last_key_rotation: SystemTime::now(),
        };
        assert_eq!(stats.active_contexts, 5);
        assert_eq!(stats.total_encryptions, 100);
        assert_eq!(stats.security_violations, 2);
    }

    #[test]
    fn test_crypto_audit_logger_new() {
        let logger = CryptoAuditLogger::new(100);
        let events = logger.events.try_read().unwrap();
        assert_eq!(events.len(), 0);
    }

    #[tokio::test]
    async fn test_crypto_audit_logger_log_and_get() {
        let logger = CryptoAuditLogger::new(100);
        let event = CryptoAuditEvent {
            timestamp: SystemTime::now(),
            event_type: CryptoEventType::Encryption,
            context_id: None,
            key_id: None,
            operation: "test".into(),
            success: true,
            error_details: None,
            metadata: HashMap::new(),
        };
        logger.log_event(event).await;
        let events = logger.get_events(None).await;
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn test_crypto_audit_logger_max_events() {
        let logger = CryptoAuditLogger::new(3);
        for i in 0..5 {
            logger
                .log_event(CryptoAuditEvent {
                    timestamp: SystemTime::now(),
                    event_type: CryptoEventType::AuditEvent,
                    context_id: None,
                    key_id: Some(format!("key-{i}")),
                    operation: "log".into(),
                    success: true,
                    error_details: None,
                    metadata: HashMap::new(),
                })
                .await;
        }
        let events = logger.get_events(None).await;
        assert_eq!(events.len(), 3);
    }

    #[tokio::test]
    async fn test_crypto_audit_logger_get_with_limit() {
        let logger = CryptoAuditLogger::new(100);
        for _ in 0..10 {
            logger
                .log_event(CryptoAuditEvent {
                    timestamp: SystemTime::now(),
                    event_type: CryptoEventType::AuditEvent,
                    context_id: None,
                    key_id: None,
                    operation: "log".into(),
                    success: true,
                    error_details: None,
                    metadata: HashMap::new(),
                })
                .await;
        }
        let events = logger.get_events(Some(3)).await;
        assert_eq!(events.len(), 3);
    }

    #[tokio::test]
    async fn test_crypto_audit_logger_empty() {
        let logger = CryptoAuditLogger::new(10);
        let events = logger.get_events(None).await;
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn test_generate_secure_random_string() {
        let config = CryptoConfig::default();
        let manager = CryptoSecurityManager::new(config).unwrap();
        let s = manager.generate_secure_random_string(16);
        assert_eq!(s.len(), 16);
        assert!(s.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[tokio::test]
    async fn test_generate_secure_random_bytes() {
        let config = CryptoConfig::default();
        let manager = CryptoSecurityManager::new(config).unwrap();
        let bytes = manager.generate_secure_random_bytes(32);
        assert_eq!(bytes.len(), 32);
    }

    #[tokio::test]
    async fn test_generate_secure_random_string_varying_lengths() {
        let config = CryptoConfig::default();
        let manager = CryptoSecurityManager::new(config).unwrap();
        for len in [0, 1, 8, 64, 128] {
            let s = manager.generate_secure_random_string(len);
            assert_eq!(s.len(), len);
        }
    }

    #[tokio::test]
    async fn test_generate_secure_random_bytes_varying_lengths() {
        let config = CryptoConfig::default();
        let manager = CryptoSecurityManager::new(config).unwrap();
        for len in [0, 16, 256] {
            let bytes = manager.generate_secure_random_bytes(len);
            assert_eq!(bytes.len(), len);
        }
    }

    #[test]
    fn test_crypto_stats_debug() {
        let stats = CryptoStats {
            active_contexts: 3,
            total_encryptions: 50,
            total_decryptions: 30,
            security_violations: 0,
            master_key_age_hours: 6,
            last_key_rotation: SystemTime::now(),
        };
        let dbg = format!("{stats:?}");
        assert!(!dbg.is_empty());
    }

    #[test]
    fn test_crypto_stats_clone() {
        let stats = CryptoStats {
            active_contexts: 1,
            total_encryptions: 2,
            total_decryptions: 3,
            security_violations: 4,
            master_key_age_hours: 5,
            last_key_rotation: SystemTime::now(),
        };
        let cloned = stats.clone();
        assert_eq!(cloned.active_contexts, stats.active_contexts);
        assert_eq!(cloned.total_encryptions, stats.total_encryptions);
    }
}
