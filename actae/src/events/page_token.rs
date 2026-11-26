// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Opaque, authenticated keyset page tokens for cross-channel event queries
//! (`POST /api/v1/events/query`).
//!
//! A page token is:
//! * **opaque** — base64url payload + HMAC signature; it exposes no SQL value
//!   (the continuation `global_cursor` is an authenticated claim, never a SQL
//!   fragment);
//! * **versioned** — `v1` in the token; a format change mints a new version;
//! * **bound to normalized filters** — the filter fingerprint is re-derived
//!   from the follow-up request and must match, so a token cannot be reused
//!   with changed filters;
//! * **bound to the caller scope** — the principal identity + read grants are
//!   fingerprinted, so a token minted by one principal cannot be replayed by
//!   another;
//! * **short-lived** — 15 minutes (D-011, section 2.2);
//! * **retention-aware** — the handler returns a typed `410` when the bound
//!   `global_cursor` has been purged by retention and is no longer satisfiable.
//!
//! This module is part of the canonical `events` domain (`actae/src/events/`),
//! the single registered owner for event logic (RUST-002).

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use sonic_rs::{JsonContainerTrait, JsonValueTrait};

pub const PAGE_TOKEN_TTL_SECS: i64 = 900;
const TOKEN_VERSION: u8 = 1;
const DOMAIN_SEPARATOR: &[u8] = b"actae:page-token:v1";

/// Decoded, verified page-token claims.
#[derive(Debug, Clone, PartialEq)]
pub struct PageTokenClaims {
    pub expires_at_unix: i64,
    /// Continuation bound: continue with `global_cursor < this`.
    pub global_cursor: i64,
    /// SHA-256 hex of the normalized filters the token was minted for.
    pub filter_hash: String,
    /// SHA-256 hex of the principal scope (identity + read grants).
    pub scope_hash: String,
}

/// Why a page token was rejected.
#[derive(Debug, Clone, PartialEq)]
pub enum PageTokenError {
    /// Malformed, wrong version, bad signature, filter/scope mismatch.
    Invalid(String),
    /// Token is beyond its 15-minute lifetime.
    Expired,
}

impl std::fmt::Display for PageTokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PageTokenError::Invalid(msg) => write!(f, "invalid page token: {msg}"),
            PageTokenError::Expired => write!(f, "page token expired"),
        }
    }
}

/// Canonical, order-independent fingerprint of the query filters that a token
/// binds to. The client does not compute this — the server derives it both at
/// mint time and at verify time, so any change to the filters invalidates the
/// token (filter mismatch).
pub fn filter_fingerprint(request: &crate::events::schema::QueryRequest) -> String {
    let mut hasher = Sha256::new();
    let mut channel_ids: Vec<&String> = request.channel_ids.iter().flatten().collect();
    channel_ids.sort();
    for id in &channel_ids {
        hasher.update(b"c:");
        hasher.update(id.as_bytes());
        hasher.update(b";");
    }
    if let Some(et) = &request.event_type {
        hasher.update(b"t:");
        hasher.update(et.as_bytes());
        hasher.update(b";");
    }
    if let Some(actor) = &request.actor {
        hasher.update(b"a:");
        hasher.update(actor.as_bytes());
        hasher.update(b";");
    }
    // Use the canonical cursor fields; the deprecated aliases resolve to the
    // same values, so the fingerprint is identical for both spellings.
    let c_start = request
        .cursor_start
        .or(request.channel_cursor_start)
        .map(|v| v.to_string())
        .unwrap_or_default();
    let c_end = request
        .cursor_end
        .or(request.channel_cursor_end)
        .map(|v| v.to_string())
        .unwrap_or_default();
    hasher.update(b"s:");
    hasher.update(c_start.as_bytes());
    hasher.update(b";e:");
    hasher.update(c_end.as_bytes());
    hasher.update(b";");
    if let Some(from) = &request.from {
        hasher.update(b"f:");
        hasher.update(from.as_bytes());
        hasher.update(b";");
    }
    if let Some(to) = &request.to {
        hasher.update(b"o:");
        hasher.update(to.as_bytes());
        hasher.update(b";");
    }
    hex::encode(hasher.finalize())
}

/// Fingerprint of the principal's query scope: identity + sorted read grants.
/// Binds a token to the caller so it cannot be replayed across principals.
pub fn principal_scope_fingerprint(auth: &crate::auth::types::AuthContext) -> String {
    let mut hasher = Sha256::new();
    let identity = auth.user_id().unwrap_or("anonymous");
    hasher.update(b"i:");
    hasher.update(identity.as_bytes());
    hasher.update(b";");
    let mut read_patterns: Vec<&String> = Vec::new();
    if let Some(perms) = auth.permissions() {
        read_patterns.extend(perms.read.iter());
        read_patterns.extend(perms.admin.iter());
    }
    read_patterns.sort();
    for p in &read_patterns {
        hasher.update(b"r:");
        hasher.update(p.as_bytes());
        hasher.update(b";");
    }
    hex::encode(hasher.finalize())
}

/// Mint a signed page token bound to `global_cursor` (the last row of the
/// previous page; the next page continues with `global_cursor < this`).
pub fn mint(
    secret: &[u8],
    filter_hash: &str,
    scope_hash: &str,
    global_cursor: i64,
) -> Result<String, PageTokenError> {
    let now = chrono::Utc::now().timestamp();
    let claims = sonic_rs::json!({
        "v": TOKEN_VERSION,
        "e": now + PAGE_TOKEN_TTL_SECS,
        "g": global_cursor,
        "f": filter_hash,
        "s": scope_hash,
    });
    let payload = sonic_rs::to_string(&claims).map_err(|e| {
        PageTokenError::Invalid(format!("failed to serialize page-token claims: {e}"))
    })?;
    let sig = sign(secret, payload.as_bytes())?;
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    Ok(format!(
        "pt_v1.{}.{}",
        engine.encode(payload.as_bytes()),
        engine.encode(sig)
    ))
}

/// Verify and decode a page token against the current request's filters and
/// caller scope. Returns `PageTokenError::Expired` for expired tokens so the
/// caller can answer with a typed `410`.
pub fn verify(
    secret: &[u8],
    token: &str,
    filter_hash: &str,
    scope_hash: &str,
) -> Result<PageTokenClaims, PageTokenError> {
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let parts: Vec<&str> = token.splitn(3, '.').collect();
    if parts.len() != 3 || parts[0] != "pt_v1" {
        return Err(PageTokenError::Invalid("unrecognized token format".into()));
    }
    let payload_bytes = engine
        .decode(parts[1])
        .map_err(|_| PageTokenError::Invalid("malformed payload".into()))?;
    let sig_bytes = engine
        .decode(parts[2])
        .map_err(|_| PageTokenError::Invalid("malformed signature".into()))?;
    if !constant_time_eq(&sig_bytes, &sign(secret, &payload_bytes)?) {
        return Err(PageTokenError::Invalid("signature mismatch".into()));
    }
    let claims: sonic_rs::Value = sonic_rs::from_slice(&payload_bytes)
        .map_err(|_| PageTokenError::Invalid("payload is not JSON".into()))?;
    let claims_obj = claims
        .as_object()
        .ok_or_else(|| PageTokenError::Invalid("claims are not a JSON object".into()))?;
    let version = claims_obj.get(&"v").and_then(|v| v.as_u64());
    if version != Some(TOKEN_VERSION as u64) {
        return Err(PageTokenError::Invalid("unsupported token version".into()));
    }
    let expires_at_unix = claims_obj
        .get(&"e")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| PageTokenError::Invalid("missing expiry".into()))?;
    if chrono::Utc::now().timestamp() > expires_at_unix {
        return Err(PageTokenError::Expired);
    }
    let global_cursor = claims_obj
        .get(&"g")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| PageTokenError::Invalid("missing cursor bound".into()))?;
    let token_filter = claims_obj
        .get(&"f")
        .and_then(|v| v.as_str())
        .ok_or_else(|| PageTokenError::Invalid("missing filter hash".into()))?;
    let token_scope = claims_obj
        .get(&"s")
        .and_then(|v| v.as_str())
        .ok_or_else(|| PageTokenError::Invalid("missing scope hash".into()))?;
    if token_filter != filter_hash {
        return Err(PageTokenError::Invalid(
            "page token was issued for different filters".into(),
        ));
    }
    if token_scope != scope_hash {
        return Err(PageTokenError::Invalid(
            "page token was issued for a different caller scope".into(),
        ));
    }
    Ok(PageTokenClaims {
        expires_at_unix,
        global_cursor,
        filter_hash: token_filter.to_string(),
        scope_hash: token_scope.to_string(),
    })
}

fn sign(secret: &[u8], payload: &[u8]) -> Result<Vec<u8>, PageTokenError> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret)
        .map_err(|_| PageTokenError::Invalid("HMAC accepts any key length".into()))?;
    mac.update(DOMAIN_SEPARATOR);
    mac.update(payload);
    Ok(mac.finalize().into_bytes().to_vec())
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::schema::QueryRequest;

    const SECRET: &[u8] = b"test-page-token-secret-0123456789abcdef";

    fn req() -> QueryRequest {
        QueryRequest {
            channel_ids: Some(vec!["a".into(), "b".into()]),
            event_type: Some("tool.completed".into()),
            actor: Some("agent".into()),
            cursor_start: Some(10),
            cursor_end: None,
            channel_cursor_start: None,
            channel_cursor_end: None,
            from: None,
            to: None,
            limit: Some(50),
            offset: None,
            page_token: None,
            pagination: None,
        }
    }

    #[test]
    fn filter_fingerprint_is_order_independent() {
        let mut a = req();
        let mut b = req();
        a.channel_ids = Some(vec!["b".into(), "a".into()]);
        b.channel_ids = Some(vec!["a".into(), "b".into()]);
        assert_eq!(filter_fingerprint(&a), filter_fingerprint(&b));
    }

    #[test]
    fn filter_fingerprint_sensitive_to_filters() {
        let a = req();
        let mut b = req();
        b.actor = Some("other".into());
        assert_ne!(filter_fingerprint(&a), filter_fingerprint(&b));
    }

    #[test]
    fn deprecated_cursor_alias_maps_to_same_fingerprint() {
        let mut a = req();
        let mut b = req();
        a.cursor_start = Some(10);
        a.channel_cursor_start = None;
        b.cursor_start = None;
        b.channel_cursor_start = Some(10);
        assert_eq!(filter_fingerprint(&a), filter_fingerprint(&b));
    }

    #[test]
    fn mint_and_verify_roundtrip() {
        let fh = filter_fingerprint(&req());
        let sh = "scope-hash";
        let token = mint(SECRET, &fh, sh, 42).unwrap();
        let claims = verify(SECRET, &token, &fh, sh).unwrap();
        assert_eq!(claims.global_cursor, 42);
        assert!(claims.expires_at_unix > chrono::Utc::now().timestamp());
    }

    #[test]
    fn verify_rejects_changed_filters() {
        let fh = filter_fingerprint(&req());
        let token = mint(SECRET, &fh, "scope-hash", 42).unwrap();
        assert!(matches!(
            verify(SECRET, &token, "different-filter", "scope-hash"),
            Err(PageTokenError::Invalid(_))
        ));
    }

    #[test]
    fn verify_rejects_changed_scope() {
        let fh = filter_fingerprint(&req());
        let token = mint(SECRET, &fh, "scope-a", 42).unwrap();
        assert!(matches!(
            verify(SECRET, &token, &fh, "scope-b"),
            Err(PageTokenError::Invalid(_))
        ));
    }

    #[test]
    fn verify_rejects_tampered_token() {
        let fh = filter_fingerprint(&req());
        let token = mint(SECRET, &fh, "scope-hash", 42).unwrap();
        let tampered = format!("{token}x");
        assert!(matches!(
            verify(SECRET, &tampered, &fh, "scope-hash"),
            Err(PageTokenError::Invalid(_))
        ));
    }

    #[test]
    fn verify_rejects_expired_token() {
        let fh = filter_fingerprint(&req());
        // Mint a token with an already-past expiry by crafting claims directly.
        let claims = sonic_rs::json!({
            "v": TOKEN_VERSION,
            "e": chrono::Utc::now().timestamp() - 1,
            "g": 1,
            "f": fh,
            "s": "scope-hash",
        });
        let payload = sonic_rs::to_string(&claims).unwrap();
        let sig = sign(SECRET, payload.as_bytes()).unwrap();
        let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let token = format!(
            "pt_v1.{}.{}",
            engine.encode(payload.as_bytes()),
            engine.encode(sig)
        );
        assert_eq!(
            verify(SECRET, &token, &fh, "scope-hash"),
            Err(PageTokenError::Expired)
        );
    }

    #[test]
    fn fingerprint_binds_principal() {
        let full = crate::auth::authorization::ApiKeyPermissions::full();
        let auth = crate::auth::types::AuthContext::ApiKey {
            key_id: "k1".into(),
            key_source: "local".into(),
            expires_at: None,
            permissions: full.clone(),
        };
        let auth2 = crate::auth::types::AuthContext::ApiKey {
            key_id: "k2".into(),
            key_source: "local".into(),
            expires_at: None,
            permissions: full,
        };
        assert_ne!(
            principal_scope_fingerprint(&auth),
            principal_scope_fingerprint(&auth2)
        );
    }
}
