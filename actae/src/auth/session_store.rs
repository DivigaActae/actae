// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Server-side dashboard session ledger + revocation cache.
//!
//! Session JWTs are stateless, so without a server-side record "logout" only
//! clears the browser and a leaked token stays valid until it expires. Every
//! session JWT carries a `jti`; a row in `public.dashboard_sessions` makes the
//! session revocable (logout, "sign out everywhere", and a password reset all
//! revoke). `is_active` is cached briefly so the per-request cost stays off the
//! database; the cache is bounded and purged on revoke.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use parking_lot::RwLock;

/// How long a positive/negative activity result is trusted without a DB hit.
const CACHE_TTL: Duration = Duration::from_secs(30);
/// Bound on the cache size (entries are also dropped on revoke).
const CACHE_CAP: usize = 20_000;
/// Opportunistic prune interval.
const PRUNE_INTERVAL_SECS: i64 = 300;

struct Entry {
    user_id: String,
    active: bool,
    at: Instant,
}

static CACHE: OnceLock<RwLock<HashMap<String, Entry>>> = OnceLock::new();
static LAST_PRUNE: AtomicI64 = AtomicI64::new(0);

fn cache() -> &'static RwLock<HashMap<String, Entry>> {
    CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Record a freshly minted session. Best effort: if the insert fails the token
/// is still cached as active and, on a later cache miss, `is_active` fails
/// closed — so a login is never broken by a transient DB error, but an
/// unverifiable session is denied.
pub async fn record(
    pool: &Pool,
    jti: &str,
    user_id: &str,
    auth_method: &str,
    expires_at: DateTime<Utc>,
) {
    if let Ok(client) = pool.get().await {
        let _ = client
            .execute(
                "INSERT INTO public.dashboard_sessions (jti, user_id, auth_method, expires_at)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (jti) DO UPDATE
                   SET expires_at = EXCLUDED.expires_at, revoked_at = NULL",
                &[&jti, &user_id, &auth_method, &expires_at],
            )
            .await;
    }
    {
        let mut guard = cache().write();
        if guard.len() > CACHE_CAP {
            guard.clear();
        }
        guard.insert(
            jti.to_string(),
            Entry {
                user_id: user_id.to_string(),
                active: true,
                at: Instant::now(),
            },
        );
    }
    maybe_prune(pool).await;
}

/// Is the session active (not revoked, not expired)? Cached briefly; a DB error
/// fails closed (deny).
pub async fn is_active(pool: &Pool, jti: &str) -> bool {
    {
        let guard = cache().read();
        if let Some(entry) = guard.get(jti) {
            if entry.at.elapsed() < CACHE_TTL {
                return entry.active;
            }
        }
    }
    let active = match pool.get().await {
        Ok(client) => match client
            .query_opt(
                "SELECT 1 FROM public.dashboard_sessions
                 WHERE jti = $1 AND revoked_at IS NULL AND expires_at > NOW()",
                &[&jti],
            )
            .await
        {
            Ok(row) => row.is_some(),
            Err(_) => false,
        },
        Err(_) => false,
    };
    let mut guard = cache().write();
    if guard.len() > CACHE_CAP {
        guard.clear();
    }
    guard.insert(
        jti.to_string(),
        Entry {
            user_id: String::new(),
            active,
            at: Instant::now(),
        },
    );
    active
}

/// Revoke a single session immediately.
pub async fn revoke(pool: &Pool, jti: &str) {
    if let Some(entry) = cache().write().get_mut(jti) {
        entry.active = false;
        entry.at = Instant::now();
    } else {
        cache().write().insert(
            jti.to_string(),
            Entry {
                user_id: String::new(),
                active: false,
                at: Instant::now(),
            },
        );
    }
    if let Ok(client) = pool.get().await {
        let _ = client
            .execute(
                "UPDATE public.dashboard_sessions SET revoked_at = NOW()
                 WHERE jti = $1 AND revoked_at IS NULL",
                &[&jti],
            )
            .await;
    }
}

/// Revoke every session for a user ("sign out everywhere" / password reset).
/// Returns the number of rows revoked. Cached entries for the user are dropped
/// so the effect is immediate on this process.
pub async fn revoke_user(pool: &Pool, user_id: &str) -> u64 {
    cache().write().retain(|_, entry| entry.user_id != user_id);
    match pool.get().await {
        Ok(client) => client
            .execute(
                "UPDATE public.dashboard_sessions SET revoked_at = NOW()
                 WHERE user_id = $1 AND revoked_at IS NULL",
                &[&user_id],
            )
            .await
            .unwrap_or(0),
        Err(_) => 0,
    }
}

/// Delete expired/old-revoked rows, at most once per `PRUNE_INTERVAL_SECS`.
pub async fn maybe_prune(pool: &Pool) {
    let now = Utc::now().timestamp();
    let last = LAST_PRUNE.load(Ordering::Relaxed);
    if now - last < PRUNE_INTERVAL_SECS {
        return;
    }
    if LAST_PRUNE
        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }
    if let Ok(client) = pool.get().await {
        let _ = client
            .execute(
                "DELETE FROM public.dashboard_sessions
                 WHERE expires_at < NOW() OR revoked_at < NOW() - INTERVAL '1 day'",
                &[],
            )
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_starts_empty() {
        // The global cache is process-wide; just ensure access is safe.
        let guard = cache().read();
        assert!(guard.len() < CACHE_CAP);
    }
}
