// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! First-run admin setup state.
//!
//! When a standalone instance creates its initial admin without a preset
//! password, it stores a one-time **setup token** (hashed) here. The operator
//! sets the admin password from the dashboard's setup page using that token;
//! `setup_complete` then flips true and the token is cleared. This keeps the
//! first-run flow a real "set your password" page without leaving an
//! unauthenticated account-claim endpoint open.

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use deadpool_postgres::Pool;
use sha2::{Digest, Sha256};

/// Constant-time byte comparison (avoids leaking the stored token through
/// timing). Both operands here are fixed-length 64-char hex digests.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// SHA-256 hash of a setup token (hex).
pub fn hash_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

#[derive(Debug, Clone)]
pub struct SetupState {
    pub setup_complete: bool,
    pub token_hash: Option<String>,
    pub token_expires_at: Option<DateTime<Utc>>,
}

/// Outcome of an atomic setup attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetupOutcome {
    /// Token valid, password written, setup completed.
    Completed {
        user_id: String,
        email: String,
        name: String,
    },
    /// Setup was already completed (by this or a concurrent request).
    AlreadyComplete,
    /// No setup state row (setup not applicable to this instance).
    Unavailable,
    /// Token missing, wrong, or expired.
    InvalidToken,
    /// No passwordless admin account is awaiting setup.
    NoAdmin,
}

/// Load the singleton setup state, if the table exists.
pub async fn load(pool: &Pool) -> Option<SetupState> {
    let client = pool.get().await.ok()?;
    let row = client
        .query_opt(
            "SELECT setup_complete, token_hash, token_expires_at
             FROM public.instance_setup WHERE singleton = TRUE",
            &[],
        )
        .await
        .ok()??;
    Some(SetupState {
        setup_complete: row.get("setup_complete"),
        token_hash: row.get("token_hash"),
        token_expires_at: row.get("token_expires_at"),
    })
}

/// Is first-run setup still required? Only when the singleton is incomplete,
/// a live setup token was actually minted, **and** an admin account is awaiting
/// a password. The token + passwordless checks keep an upgraded install (users
/// already exist) or an enrolled instance (passwordless portal users) from ever
/// showing the setup page.
pub async fn is_required(pool: &Pool) -> bool {
    match load(pool).await {
        Some(state) if !state.setup_complete && state.token_hash.is_some() => {
            has_passwordless_user(pool).await
        }
        _ => false,
    }
}

/// Is there an account with no password yet (the first-boot admin)?
async fn has_passwordless_user(pool: &Pool) -> bool {
    let Ok(client) = pool.get().await else {
        return false;
    };
    client
        .query_opt(
            "SELECT 1 FROM public.user WHERE password_hash IS NULL LIMIT 1",
            &[],
        )
        .await
        .ok()
        .flatten()
        .is_some()
}

/// Store a fresh one-time setup token.
pub async fn begin(pool: &Pool, token: &str, ttl: Duration) -> Result<()> {
    let client = pool.get().await.context("setup: no db connection")?;
    client
        .execute(
            "UPDATE public.instance_setup
             SET setup_complete = FALSE, token_hash = $1, token_expires_at = $2, updated_at = NOW()
             WHERE singleton = TRUE",
            &[&hash_token(token), &(Utc::now() + ttl)],
        )
        .await
        .context("failed to record the setup token")?;
    Ok(())
}

/// Mark setup complete and clear the token. Returns `true` when this call was
/// the one that completed it. Used by the CLI recovery path (`actae passwd`),
/// which sets a password and thereby satisfies first-run setup.
pub async fn mark_complete(pool: &Pool) -> Result<bool> {
    let client = pool.get().await.context("setup: no db connection")?;
    let updated = client
        .execute(
            "UPDATE public.instance_setup
             SET setup_complete = TRUE, token_hash = NULL, token_expires_at = NULL, updated_at = NOW()
             WHERE singleton = TRUE AND setup_complete = FALSE",
            &[],
        )
        .await
        .context("failed to complete setup")?;
    Ok(updated > 0)
}

/// Does `token` match the stored, unexpired setup token?
pub fn token_matches(state: &SetupState, token: &str) -> bool {
    let Some(stored) = state.token_hash.as_deref() else {
        return false;
    };
    let expired = state
        .token_expires_at
        .map(|e| e <= Utc::now())
        .unwrap_or(true);
    !expired && constant_time_eq(stored.as_bytes(), hash_token(token).as_bytes())
}

/// Atomically verify the token, write the already-hashed password to the
/// passwordless admin, and mark setup complete — all in one transaction that
/// locks the singleton and the target row. This makes a concurrent duplicate
/// submit safe: exactly one caller completes, the other observes
/// `AlreadyComplete`/`NoAdmin` and never overwrites the winner's password.
///
/// `password_hash` must be produced by the caller (after validating the
/// password policy) so no slow hashing happens while row locks are held.
pub async fn complete(
    pool: &Pool,
    token: &str,
    user_id: &str,
    password_hash: &str,
    name: Option<&str>,
) -> Result<SetupOutcome> {
    let mut client = pool.get().await.context("setup: no db connection")?;
    let tx = client.transaction().await.context("setup: begin tx")?;

    // Lock the singleton row for the duration of the transition.
    let row = tx
        .query_opt(
            "SELECT setup_complete, token_hash, token_expires_at
             FROM public.instance_setup WHERE singleton = TRUE FOR UPDATE",
            &[],
        )
        .await
        .context("setup: load state")?;
    let Some(row) = row else {
        return Ok(SetupOutcome::Unavailable);
    };
    if row.get::<_, bool>("setup_complete") {
        return Ok(SetupOutcome::AlreadyComplete);
    }
    let stored: Option<String> = row.get("token_hash");
    let expires_at: Option<DateTime<Utc>> = row.get("token_expires_at");
    let expired = expires_at.map(|e| e <= Utc::now()).unwrap_or(true);
    match stored {
        Some(stored)
            if !expired
                && constant_time_eq(stored.as_bytes(), hash_token(token).as_bytes()) => {}
        _ => return Ok(SetupOutcome::InvalidToken),
    }

    // Fetch (and lock) the passwordless admin's display fields.
    let user = tx
        .query_opt(
            "SELECT email, name FROM public.user
             WHERE id = $1 AND password_hash IS NULL FOR UPDATE",
            &[&user_id],
        )
        .await
        .context("setup: load admin")?;
    let Some(user) = user else {
        return Ok(SetupOutcome::NoAdmin);
    };
    let email: String = user.get("email");
    let final_name: String = name
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| user.get("name"));

    // Guarded write: only applies while the account is still passwordless, so a
    // concurrent winner cannot be overwritten.
    let updated = tx
        .execute(
            "UPDATE public.user
             SET password_hash = $2, name = $3, \"updatedAt\" = NOW()
             WHERE id = $1 AND password_hash IS NULL",
            &[&user_id, &password_hash, &final_name],
        )
        .await
        .context("setup: set password")?;
    if updated == 0 {
        return Ok(SetupOutcome::NoAdmin);
    }
    tx.execute(
        "UPDATE public.instance_setup
         SET setup_complete = TRUE, token_hash = NULL, token_expires_at = NULL, updated_at = NOW()
         WHERE singleton = TRUE",
        &[],
    )
    .await
    .context("setup: mark complete")?;
    tx.commit().await.context("setup: commit")?;

    Ok(SetupOutcome::Completed {
        user_id: user_id.to_string(),
        email,
        name: final_name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(hash: Option<String>, exp: Option<DateTime<Utc>>, complete: bool) -> SetupState {
        SetupState {
            setup_complete: complete,
            token_hash: hash,
            token_expires_at: exp,
        }
    }

    #[test]
    fn constant_time_eq_matches_semantics() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(!constant_time_eq(b"", b"a"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn token_matches_only_the_stored_unexpired_value() {
        let token = "setup-token-abc";
        let good = state(
            Some(hash_token(token)),
            Some(Utc::now() + Duration::hours(1)),
            false,
        );
        assert!(token_matches(&good, token));
        assert!(!token_matches(&good, "wrong"));

        let expired = state(
            Some(hash_token(token)),
            Some(Utc::now() - Duration::seconds(1)),
            false,
        );
        assert!(!token_matches(&expired, token));

        let no_token = state(None, None, false);
        assert!(!token_matches(&no_token, token));
    }
}
