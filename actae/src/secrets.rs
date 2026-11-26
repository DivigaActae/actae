// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! First-boot signing-secret management.
//!
//! A self-hosted instance must not run on a well-known JWT signing secret —
//! anyone who knows it can forge dashboard sessions. When the operator has not
//! configured `JWT_SECRET`, this module resolves the effective secret in order:
//!
//! 1. a previously generated secret persisted under the config dir
//!    (`<ACTAE_CONFIG_DIR>/jwt.secret`, mode 0600);
//! 2. the built-in [`DEFAULT_JWT_SECRET`], **only** when the instance already
//!    has API keys hashed with it (an upgrade — rotating it would invalidate
//!    every key); or
//! 3. a freshly generated random secret, persisted for subsequent boots.
//!
//! The caller supplies `existing_keys` (a database probe) because only it can
//! reach the database. The API-key hash secret defaults to the JWT secret
//! (preserving existing key hashes) unless the operator set it independently.

use anyhow::{Context, Result};
use rand::RngCore;
use std::path::{Path, PathBuf};

pub use crate::config::DEFAULT_JWT_SECRET;

const SECRET_FILE: &str = "jwt.secret";

/// Where the generated secret is persisted (same dir as `enrollment.json`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretOutcome {
    /// The operator set `JWT_SECRET`/`JWT_SECRET_FILE`.
    Operator,
    /// Reused a previously persisted generated secret.
    Persisted,
    /// Generated and persisted a new random secret (fresh install).
    Generated,
    /// Kept the built-in default because existing API keys depend on it.
    LegacyDefault,
}

/// A cryptographically random secret (96 hex chars / 384 bits).
pub fn generate() -> String {
    let mut bytes = [0u8; 48];
    let mut rng = rand::rngs::OsRng;
    rng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

fn is_valid(secret: &str) -> bool {
    secret.len() >= 32 && !secret.contains('\0')
}

/// Read a previously persisted secret, if any and valid.
pub fn load_persisted(dir: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(dir.join(SECRET_FILE)).ok()?;
    let trimmed = raw.trim();
    is_valid(trimmed).then(|| trimmed.to_string())
}

/// Persist `secret` to `<dir>/jwt.secret`, mode 0600, atomically (temp + rename).
pub fn persist(dir: &Path, secret: &str) -> Result<PathBuf> {
    use std::io::Write;

    std::fs::create_dir_all(dir)
        .with_context(|| format!("failed to create secret dir {}", dir.display()))?;
    let path = dir.join(SECRET_FILE);
    let tmp = dir.join(format!(
        ".{SECRET_FILE}.tmp.{}.{}",
        std::process::id(),
        rand::random::<u32>()
    ));

    let mut opts = std::fs::OpenOptions::new();
    opts.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts
        .open(&tmp)
        .with_context(|| format!("failed to create {}", tmp.display()))?;
    let write = (|| -> std::io::Result<()> {
        file.write_all(secret.as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()
    })();
    if let Err(e) = write {
        let _ = std::fs::remove_file(&tmp);
        return Err(e).with_context(|| format!("failed to write {}", tmp.display()));
    }
    drop(file);

    if let Err(e) = std::fs::rename(&tmp, &path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e).with_context(|| format!("failed to publish {}", path.display()));
    }
    // The file is created 0600, but an existing file replaced by rename keeps
    // its own mode — enforce it explicitly (best effort).
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(path)
}

/// Resolve a secret from an operator value, a persisted value, or generation.
pub fn resolve(
    operator_secret: Option<String>,
    dir: &Path,
    existing_keys: bool,
) -> Result<(String, SecretOutcome)> {
    if let Some(secret) = operator_secret {
        return Ok((secret, SecretOutcome::Operator));
    }
    if let Some(persisted) = load_persisted(dir) {
        return Ok((persisted, SecretOutcome::Persisted));
    }
    if existing_keys {
        return Ok((DEFAULT_JWT_SECRET.to_string(), SecretOutcome::LegacyDefault));
    }
    let generated = generate();
    persist(dir, &generated)?;
    Ok((generated, SecretOutcome::Generated))
}

fn operator_set(name: &str) -> bool {
    std::env::var(name)
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false)
        || std::env::var(format!("{name}_FILE")).is_ok()
}

/// Apply the resolution to `config` in place and derive the API-key hash
/// secret. Returns the JWT-secret outcome for logging.
pub fn resolve_into_config(
    config: &mut crate::config::AppConfig,
    existing_keys: bool,
) -> Result<SecretOutcome> {
    let dir = crate::control_plane::enrollment::Enrollment::config_dir();
    let operator_secret = if operator_set("JWT_SECRET") {
        Some(config.jwt_secret.clone())
    } else {
        None
    };
    let (secret, outcome) = resolve(operator_secret, &dir, existing_keys)?;
    config.jwt_secret = secret;
    // The API-key hash secret defaults to the JWT secret (existing key hashes
    // were minted that way); an operator-set value stays authoritative.
    if !operator_set("API_KEY_HASH_SECRET") {
        config.api_key_hash_secret = config.jwt_secret.clone();
    }
    Ok(outcome)
}

/// Best-effort probe: does the instance already have API keys? On any error
/// (fresh DB without the table, unreachable) assume `true`, so a secret is
/// never rotated out from under existing key hashes.
pub async fn client_has_api_keys(config: &crate::config::AppConfig) -> bool {
    match crate::database::DatabaseManager::new(config).await {
        Ok(manager) => pool_has_api_keys(&manager.client_pool).await,
        Err(_) => true,
    }
}

/// Same probe against an already-built client pool.
pub async fn pool_has_api_keys(pool: &deadpool_postgres::Pool) -> bool {
    match pool.get().await {
        Ok(client) => match client
            .query_one("SELECT COUNT(*) FROM public.api_keys", &[])
            .await
        {
            Ok(row) => row.get::<_, i64>(0) > 0,
            Err(_) => false, // table absent => fresh install
        },
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_secret_is_strong_and_valid() {
        let s = generate();
        assert_eq!(s.len(), 96);
        assert!(is_valid(&s));
        assert_ne!(s, generate());
    }

    #[test]
    fn resolve_generates_and_persists_on_a_fresh_install() {
        let dir = std::env::temp_dir().join(format!("actae-secrets-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (secret, outcome) = resolve(None, &dir, false).unwrap();
        assert_eq!(outcome, SecretOutcome::Generated);
        assert_eq!(load_persisted(&dir).as_deref(), Some(secret.as_str()));
        // Second resolution reuses the persisted value.
        let (again, outcome) = resolve(None, &dir, false).unwrap();
        assert_eq!(outcome, SecretOutcome::Persisted);
        assert_eq!(again, secret);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_keeps_the_default_when_keys_already_exist() {
        let dir =
            std::env::temp_dir().join(format!("actae-secrets-test-ke-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (secret, outcome) = resolve(None, &dir, true).unwrap();
        assert_eq!(outcome, SecretOutcome::LegacyDefault);
        assert_eq!(secret, DEFAULT_JWT_SECRET);
        assert!(
            load_persisted(&dir).is_none(),
            "must not persist the default"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_prefers_the_operator_secret() {
        let dir =
            std::env::temp_dir().join(format!("actae-secrets-test-op-{}", std::process::id()));
        let (secret, outcome) = resolve(
            Some("operator-secret-1234567890abcdefghij".into()),
            &dir,
            true,
        )
        .unwrap();
        assert_eq!(outcome, SecretOutcome::Operator);
        assert_eq!(secret, "operator-secret-1234567890abcdefghij");
    }
}
