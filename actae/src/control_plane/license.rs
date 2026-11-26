// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Ed25519 license tokens for control-plane responses.
//!
//! Every control-plane response that can influence subscription
//! enforcement carries a `license` field: a compact token signed by the
//! Diviga SaaS with its Ed25519 private key. Actae verifies the token
//! against an embedded public key before trusting the response, so a fake
//! or substituted control plane cannot mint a `subscription: active` state
//! — doing so would require the SaaS's Ed25519 private key (or a patched
//! binary).
//!
//! Token format: `base64url(payload_json) "." base64url(ed25519_sig)`.
//! The signature covers the exact payload bytes as sent, and Actae
//! verifies over the received bytes — so there is no cross-language
//! canonicalization of the token itself.
//!
//! Payload (canonical JSON, keys sorted):
//!
//! ```json
//! {
//!   "v": 1, "kid": "k1", "install_id": "ins_...",
//!   "subscription": "active", "rev": 5, "keys_sha256": "<hex>",
//!   "enrollment_secret_sha256": "<hex>",
//!   "iat": 1750000000, "exp": 1750086400
//! }
//! ```
//!
//! - Enrollment responses (`/v1/installations/enroll`,
//!   `/v1/device_codes/poll` when linked) carry `enrollment_secret_sha256`
//!   binding the token to the delivered credentials.
//! - Keys responses (`/v1/installations/{id}/keys`) carry `subscription`,
//!   `rev` and `keys_sha256` (a hash of the canonical key list) so the
//!   signed payload is bound to the response body.
//!
//! Safety rule: only a *valid* signature is authoritative. An invalid,
//! expired or unknown-`kid` token makes the response unverifiable — the
//! caller routes that through the existing offline-grace / 503 path. It
//! never hard-locks an instance (a signing rollout bug must not brick every
//! customer). A *valid* token declaring `suspended`/`cancelled` is what
//! produces the hard 402.

use crate::control_plane::client::RemoteKey;
use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::Utc;
use ed25519_dalek::{Signature, VerifyingKey};
use serde::Deserialize;

/// Current license schema version.
const LICENSE_VERSION: u64 = 1;
/// `kid` of the initial license signing key.
const DEFAULT_KID: &str = "k1";
/// Clock-skew allowance when checking `iat`/`exp` (seconds).
const CLOCK_SKEW_SECS: i64 = 300;
/// Hard cap on the token TTL (`exp - iat`). Rejects a signer accidentally
/// issuing multi-year tokens that would gut replay protection.
const MAX_TOKEN_TTL_SECS: i64 = 30 * 24 * 3600;

/// Public key embedded when `ACTAE_LICENSE_PUBKEY` is not set at build time.
/// The public half of the shared test keypair used by the conformance stub
/// and the SaaS test-driver mode (seed
/// `f0734c774db9b525109add0b896c137a57cf8f7f456f6f4bb4e00a81b8189ec7`).
const TEST_PUBKEY_B64: &str = "Zve_tDxUYX8mvCePbIGY26-7zzJVMy5N-3GsQxV-eds";

/// Verified claims extracted from a license token.
#[derive(Debug, Clone, Deserialize)]
pub struct LicenseClaims {
    pub v: u64,
    #[serde(default = "default_kid")]
    pub kid: String,
    pub install_id: String,
    #[serde(default)]
    pub subscription: Option<String>,
    #[serde(default)]
    pub rev: Option<i64>,
    #[serde(default)]
    pub keys_sha256: Option<String>,
    /// Hash of the canonical federation configuration (verification keys,
    /// revoked key ids, source policy) delivered with a keys response. Binds
    /// the distributed federation trust anchors to the signed license, so a
    /// MITM cannot swap them. Additive: absent on pre-federation licenses.
    #[serde(default)]
    pub federation_sha256: Option<String>,
    #[serde(default)]
    pub enrollment_secret_sha256: Option<String>,
    pub iat: i64,
    pub exp: i64,
}

fn default_kid() -> String {
    DEFAULT_KID.to_string()
}

/// Parse and verify a license token. Returns the claims on success; any
/// failure is an `Err` describing why the response cannot be trusted.
pub fn verify_license(token: &str) -> Result<LicenseClaims> {
    let claims = verify_license_signature(token)?;

    let now = Utc::now().timestamp();
    if claims.iat > now + CLOCK_SKEW_SECS {
        return Err(anyhow!("license is not yet valid (iat is in the future)"));
    }
    if claims.exp < now - CLOCK_SKEW_SECS {
        return Err(anyhow!("license expired at {}", claims.exp));
    }
    if claims.exp - claims.iat > MAX_TOKEN_TTL_SECS {
        return Err(anyhow!("license TTL exceeds the allowed maximum"));
    }
    Ok(claims)
}

/// Verify the signature, version, `kid` and `install_id` of a license token
/// without enforcing `iat`/`exp`. Used for informational diagnostics (`actae
/// auth status`), where an expired enrollment license is not a failure —
/// enforcement relies on the per-sync tokens, not the stored enrollment one.
pub fn verify_license_signature(token: &str) -> Result<LicenseClaims> {
    let payload_bytes = verify_token_signature(token)?;
    let claims: LicenseClaims =
        serde_json::from_slice(&payload_bytes).context("license payload is not valid JSON")?;

    if claims.v != LICENSE_VERSION {
        return Err(anyhow!("unsupported license version: {}", claims.v));
    }
    if claims.install_id.trim().is_empty() {
        return Err(anyhow!("license is missing install_id"));
    }
    Ok(claims)
}

/// Decode a license token and verify its Ed25519 signature against the
/// embedded public key for its `kid`. Returns the signed payload bytes —
/// callers parse and validate the type-specific claims.
fn verify_token_signature(token: &str) -> Result<Vec<u8>> {
    let (payload_b64, sig_b64) = token
        .split_once('.')
        .ok_or_else(|| anyhow!("malformed license token: missing '.' separator"))?;
    let payload_bytes = decode_b64url(payload_b64, "license payload")?;
    let sig_bytes = decode_b64url(sig_b64, "license signature")?;
    let sig: [u8; 64] = sig_bytes
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("license signature must be 64 bytes"))?;

    // The `kid` lives inside the signed payload; read it minimally to pick
    // the verification key.
    let minimal: serde_json::Value =
        serde_json::from_slice(&payload_bytes).context("license payload is not valid JSON")?;
    let kid = minimal
        .get("kid")
        .and_then(|k| k.as_str())
        .unwrap_or(DEFAULT_KID)
        .to_string();

    let pubkey = embedded_pubkey(&kid)
        .ok_or_else(|| anyhow!("license signed with unknown key id '{kid}'"))?;
    let verifying_key =
        VerifyingKey::from_bytes(&pubkey).context("embedded license public key is invalid")?;
    verifying_key
        .verify_strict(&payload_bytes, &Signature::from_bytes(&sig))
        .map_err(|_| anyhow!("license signature verification failed (tampered or forged token)"))?;

    Ok(payload_bytes)
}

/// The license `scope` marker carried by signed upgrade-manifest tokens.
pub const MANIFEST_SCOPE: &str = "upgrade-manifest";

/// The license `scope` marker carried by signed dashboard SSO handoff
/// tokens (`POST /v1/installations/{id}/dashboard-token`). These are signed
/// by the same control-plane Ed25519 key as a license but are short-lived
/// and single-use; the scope keeps them from being accepted as a license.
pub const DASHBOARD_SSO_SCOPE: &str = "dashboard-sso";

/// Hard cap on a dashboard SSO token's lifetime. The SaaS mints ~2-minute
/// tokens; anything beyond this is treated as forged/misconfigured.
const MAX_DASHBOARD_TOKEN_TTL_SECS: i64 = 15 * 60;

/// Verified claims of a signed dashboard SSO handoff token.
#[derive(Debug, Clone, Deserialize)]
pub struct DashboardClaims {
    pub v: u64,
    #[serde(default = "default_kid")]
    pub kid: String,
    #[serde(default)]
    pub scope: String,
    pub install_id: String,
    /// Portal customer id (`cus_...`) — becomes the dashboard user id.
    pub sub: String,
    #[serde(default)]
    pub email: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub role: String,
    /// Single-use nonce; recorded by the instance on exchange.
    pub jti: String,
    pub iat: i64,
    pub exp: i64,
}

/// Verify a dashboard SSO handoff token: signature, `scope`, `jti`
/// freshness and the lifetime cap. Returns the claims on success.
pub fn verify_dashboard_token(token: &str) -> Result<DashboardClaims> {
    let payload_bytes = verify_token_signature(token)?;
    let claims: DashboardClaims = serde_json::from_slice(&payload_bytes)
        .context("dashboard token payload is not valid JSON")?;
    if claims.v != LICENSE_VERSION {
        return Err(anyhow!("unsupported dashboard token version: {}", claims.v));
    }
    if claims.scope != DASHBOARD_SSO_SCOPE {
        return Err(anyhow!(
            "token scope '{}' is not a dashboard handoff",
            claims.scope
        ));
    }
    if claims.install_id.trim().is_empty() {
        return Err(anyhow!("dashboard token is missing install_id"));
    }
    if claims.sub.trim().is_empty() {
        return Err(anyhow!("dashboard token is missing a subject"));
    }
    if claims.jti.trim().is_empty() {
        return Err(anyhow!("dashboard token is missing a jti"));
    }
    let now = Utc::now().timestamp();
    if claims.iat > now + CLOCK_SKEW_SECS {
        return Err(anyhow!(
            "dashboard token is not yet valid (iat in the future)"
        ));
    }
    if claims.exp < now - CLOCK_SKEW_SECS {
        return Err(anyhow!("dashboard token expired at {}", claims.exp));
    }
    if claims.exp - claims.iat > MAX_DASHBOARD_TOKEN_TTL_SECS {
        return Err(anyhow!("dashboard token TTL exceeds the allowed maximum"));
    }
    Ok(claims)
}

/// Claims of a signed upgrade-manifest token
/// (`GET /v1/downloads/manifest.json`). `install_id` is intentionally absent
/// — the manifest is global, not per-install.
#[derive(Debug, Clone, Deserialize)]
pub struct ManifestClaims {
    pub v: u64,
    #[serde(default = "default_kid")]
    pub kid: String,
    #[serde(default)]
    pub scope: String,
    pub manifest_sha256: String,
    pub iat: i64,
    pub exp: i64,
}

/// Verify an upgrade-manifest license token: signature, `scope`, freshness
/// and the manifest-hash binding.
pub fn verify_manifest(token: &str) -> Result<ManifestClaims> {
    let payload_bytes = verify_token_signature(token)?;
    let claims: ManifestClaims =
        serde_json::from_slice(&payload_bytes).context("license payload is not valid JSON")?;
    if claims.v != LICENSE_VERSION {
        return Err(anyhow!("unsupported license version: {}", claims.v));
    }
    if claims.scope != MANIFEST_SCOPE {
        return Err(anyhow!(
            "license scope '{}' is not an upgrade manifest",
            claims.scope
        ));
    }
    if claims.manifest_sha256.trim().is_empty() {
        return Err(anyhow!("license is missing manifest_sha256"));
    }
    let now = Utc::now().timestamp();
    if claims.iat > now + CLOCK_SKEW_SECS {
        return Err(anyhow!("license is not yet valid (iat is in the future)"));
    }
    if claims.exp < now - CLOCK_SKEW_SECS {
        return Err(anyhow!("license expired at {}", claims.exp));
    }
    if claims.exp - claims.iat > MAX_TOKEN_TTL_SECS {
        return Err(anyhow!("license TTL exceeds the allowed maximum"));
    }
    Ok(claims)
}

/// Look up the embedded Ed25519 public key for `kid`.
///
/// The public key is injected at build time via `ACTAE_LICENSE_PUBKEY`
/// (see `build.rs`); release builds must set it to the production key.
/// When it is unset (dev/test builds) the shared test public key is used.
fn embedded_pubkey(kid: &str) -> Option<[u8; 32]> {
    if let Some(b64) = option_env!("ACTAE_LICENSE_PUBKEY_B64") {
        if let Ok(bytes) = decode_pubkey(b64) {
            if kid == DEFAULT_KID {
                return Some(bytes);
            }
        }
    }
    if kid == DEFAULT_KID {
        decode_pubkey(TEST_PUBKEY_B64).ok()
    } else {
        None
    }
}

/// Decode a base64url public key (padding tolerated) into 32 raw bytes.
fn decode_pubkey(b64: &str) -> Result<[u8; 32]> {
    let bytes = decode_b64url(b64, "public key")?;
    bytes
        .as_slice()
        .try_into()
        .map_err(|_| anyhow!("license public key must be 32 bytes"))
}

/// Base64url (RFC 4648, no padding) decoder that tolerates trailing `=` so
/// both padded and unpadded inputs work.
fn decode_b64url(input: &str, what: &str) -> Result<Vec<u8>> {
    URL_SAFE_NO_PAD
        .decode(input.trim().trim_end_matches('='))
        .with_context(|| format!("{what} is not valid base64url"))
}

/// SHA-256 hex digest of `data`.
pub fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Canonical serialization of a control-plane keys array for `keys_sha256`.
///
/// Sorted keys, compact, no spaces, explicit `null`s for absent optional
/// fields — byte-identical to the SaaS side
/// (`json.dumps(..., sort_keys=True, separators=(",", ":"), ensure_ascii=False)`).
/// Both sides normalize every key entry to exactly the six known fields so
/// unknown/legacy fields can never cause a hash mismatch.
pub fn canonical_keys(keys: &[RemoteKey]) -> String {
    let arr: Vec<serde_json::Value> = keys.iter().map(canonical_key).collect();
    serde_json::to_string(&serde_json::Value::Array(arr)).unwrap_or_default()
}

fn canonical_key(key: &RemoteKey) -> serde_json::Value {
    serde_json::json!({
        "id": key.id,
        "key": key.key,
        "user_id": key.user_id,
        "name": key.name,
        "permissions": key.permissions,
        "expires_at": key.expires_at,
    })
}

/// Canonical serialization of the distributed federation configuration for
/// `federation_sha256` — verification keys (sorted by kid), revoked key ids
/// (sorted), and the optional source policy, compact, no spaces, sorted keys.
/// Byte-identical to the SaaS side's canonical JSON (`sort_keys=True`,
/// `separators=(",", ":")`).
pub fn canonical_federation(
    keys: &[crate::control_plane::client::FederationKeyWire],
    revoked_key_ids: &[String],
    source_policy: Option<&serde_json::Value>,
) -> String {
    let mut sorted_keys: Vec<serde_json::Value> = keys
        .iter()
        .map(|k| serde_json::json!({"kid": k.kid, "public_key": k.public_key}))
        .collect();
    sorted_keys.sort_by(|a, b| {
        let ak = a.get("kid").and_then(|v| v.as_str()).unwrap_or("");
        let bk = b.get("kid").and_then(|v| v.as_str()).unwrap_or("");
        ak.cmp(bk)
    });
    let mut sorted_revoked: Vec<&String> = revoked_key_ids.iter().collect();
    sorted_revoked.sort();
    serde_json::json!({
        "federation_keys": sorted_keys,
        "revoked_key_ids": sorted_revoked,
        "source_policy": source_policy,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use std::time::{SystemTime, UNIX_EPOCH};

    /// The shared test keypair (seed + public key). Matches the stub and the
    /// SaaS test-driver constants so a token signed here verifies against
    /// the embedded test public key.
    const TEST_SEED_HEX: &str = "f0734c774db9b525109add0b896c137a57cf8f7f456f6f4bb4e00a81b8189ec7";
    /// A second deterministic seed used to forge a token with the wrong key.
    const OTHER_SEED_HEX: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn test_signing_key() -> SigningKey {
        let seed = hex::decode(TEST_SEED_HEX).unwrap();
        SigningKey::from_bytes(&seed.as_slice().try_into().unwrap())
    }

    fn other_signing_key() -> SigningKey {
        let seed = hex::decode(OTHER_SEED_HEX).unwrap();
        SigningKey::from_bytes(&seed.as_slice().try_into().unwrap())
    }

    fn now() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    fn base64url(data: &[u8]) -> String {
        URL_SAFE_NO_PAD.encode(data)
    }

    fn build_token(payload: serde_json::Value, key: &SigningKey) -> String {
        let bytes = serde_json::to_vec(&payload).unwrap();
        let sig = key.sign(&bytes);
        format!("{}.{}", base64url(&bytes), base64url(&sig.to_bytes()))
    }

    fn enroll_payload(now: i64, install: &str, secret: &str, exp_offset: i64) -> serde_json::Value {
        serde_json::json!({
            "v": 1, "kid": "k1", "install_id": install,
            "enrollment_secret_sha256": sha256_hex(secret.as_bytes()),
            "iat": now, "exp": now + exp_offset,
        })
    }

    #[test]
    fn test_verify_valid_enrollment_token() {
        let key = test_signing_key();
        let now = now();
        let token = build_token(enroll_payload(now, "ins_1", "es_secret", 3600), &key);
        let claims = verify_license(&token).unwrap();
        assert_eq!(claims.install_id, "ins_1");
        assert_eq!(
            claims.enrollment_secret_sha256.as_deref(),
            Some(sha256_hex(b"es_secret").as_str())
        );
    }

    #[test]
    fn test_verify_rejects_tampered_payload() {
        let key = test_signing_key();
        let now = now();
        let token = build_token(enroll_payload(now, "ins_1", "es_secret", 3600), &key);
        // Flip a byte inside the sha256 string value: the JSON stays valid
        // but the signed content changes, so signature verification fails.
        let (payload_b64, sig) = token.split_once('.').unwrap();
        let mut bytes = URL_SAFE_NO_PAD.decode(payload_b64).unwrap();
        bytes[2] = if bytes[2] == b'0' { b'1' } else { b'0' };
        let tampered = format!("{}.{}", base64url(&bytes), sig);
        let err = verify_license(&tampered).unwrap_err();
        assert!(err.to_string().contains("signature verification failed"));
    }

    #[test]
    fn test_verify_rejects_wrong_key() {
        let other = other_signing_key();
        let now = now();
        let token = build_token(enroll_payload(now, "ins_1", "es_secret", 3600), &other);
        assert!(verify_license(&token).is_err());
    }

    #[test]
    fn test_verify_rejects_unknown_kid() {
        let key = test_signing_key();
        let now = now();
        let mut payload = enroll_payload(now, "ins_1", "es_secret", 3600);
        payload["kid"] = serde_json::json!("k9");
        let token = build_token(payload, &key);
        let err = verify_license(&token).unwrap_err();
        assert!(err.to_string().contains("unknown key id"));
    }

    #[test]
    fn test_verify_rejects_expired_token() {
        let key = test_signing_key();
        let now = now();
        let token = build_token(enroll_payload(now - 7200, "ins_1", "es_secret", 3600), &key);
        let err = verify_license(&token).unwrap_err();
        assert!(err.to_string().contains("expired"));
    }

    #[test]
    fn test_verify_rejects_token_from_future() {
        let key = test_signing_key();
        let now = now();
        let token = build_token(enroll_payload(now + 3600, "ins_1", "es_secret", 3600), &key);
        let err = verify_license(&token).unwrap_err();
        assert!(err.to_string().contains("future"));
    }

    #[test]
    fn test_verify_rejects_unsupported_version() {
        let key = test_signing_key();
        let now = now();
        let mut payload = enroll_payload(now, "ins_1", "es_secret", 3600);
        payload["v"] = serde_json::json!(2);
        let token = build_token(payload, &key);
        let err = verify_license(&token).unwrap_err();
        assert!(err.to_string().contains("unsupported license version"));
    }

    #[test]
    fn test_verify_rejects_missing_install_id() {
        let key = test_signing_key();
        let now = now();
        let mut payload = enroll_payload(now, "ins_1", "es_secret", 3600);
        payload["install_id"] = serde_json::json!("  ");
        let token = build_token(payload, &key);
        assert!(verify_license(&token).is_err());
    }

    #[test]
    fn test_verify_rejects_malformed_token() {
        assert!(verify_license("not-a-token").is_err());
        assert!(verify_license("abc.def.ghi").is_err());
        assert!(verify_license("@@@.@@@").is_err());
    }

    fn dashboard_payload(now: i64, scope: &str, jti: &str, ttl: i64) -> serde_json::Value {
        serde_json::json!({
            "v": 1, "kid": "k1", "scope": scope, "install_id": "ins_1",
            "sub": "cus_1", "email": "a@b.com", "name": "Alice", "role": "owner",
            "jti": jti, "iat": now, "exp": now + ttl,
        })
    }

    #[test]
    fn test_verify_valid_dashboard_token() {
        let key = test_signing_key();
        let now = now();
        let token = build_token(
            dashboard_payload(now, DASHBOARD_SSO_SCOPE, "jti_1", 120),
            &key,
        );
        let claims = verify_dashboard_token(&token).unwrap();
        assert_eq!(claims.install_id, "ins_1");
        assert_eq!(claims.sub, "cus_1");
        assert_eq!(claims.email, "a@b.com");
        assert_eq!(claims.jti, "jti_1");
    }

    #[test]
    fn test_verify_dashboard_token_rejects_wrong_scope() {
        let key = test_signing_key();
        let now = now();
        let token = build_token(
            dashboard_payload(now, "upgrade-manifest", "jti_1", 120),
            &key,
        );
        assert!(verify_dashboard_token(&token).is_err());
    }

    #[test]
    fn test_verify_dashboard_token_rejects_expired() {
        let key = test_signing_key();
        let now = now();
        let token = build_token(
            dashboard_payload(now - 3600, DASHBOARD_SSO_SCOPE, "jti_1", 120),
            &key,
        );
        assert!(verify_dashboard_token(&token).is_err());
    }

    #[test]
    fn test_verify_dashboard_token_rejects_long_ttl() {
        let key = test_signing_key();
        let now = now();
        let token = build_token(
            dashboard_payload(
                now,
                DASHBOARD_SSO_SCOPE,
                "jti_1",
                MAX_DASHBOARD_TOKEN_TTL_SECS + 1,
            ),
            &key,
        );
        assert!(verify_dashboard_token(&token).is_err());
    }

    #[test]
    fn test_verify_dashboard_token_rejects_missing_jti() {
        let key = test_signing_key();
        let now = now();
        let token = build_token(dashboard_payload(now, DASHBOARD_SSO_SCOPE, "  ", 120), &key);
        assert!(verify_dashboard_token(&token).is_err());
    }

    #[test]
    fn test_verify_dashboard_token_rejects_license() {
        // A real license token must not be accepted as a dashboard handoff.
        let key = test_signing_key();
        let now = now();
        let token = build_token(enroll_payload(now, "ins_1", "es_secret", 3600), &key);
        assert!(verify_dashboard_token(&token).is_err());
    }

    #[test]
    fn test_canonical_keys_matches_python_serialization() {
        // Structurally identical to what the SaaS canonicalizes. The Rust
        // side sorts keys like Python's sort_keys=True and uses explicit
        // nulls, so the hash of this string must equal the hash computed
        // over the equivalent Python json.dumps output.
        let keys = vec![
            RemoteKey {
                id: "k_2".to_string(),
                key: None,
                user_id: "cus_1".to_string(),
                name: "prod".to_string(),
                permissions: Some(serde_json::json!({"write": ["*"], "read": ["*"]})),
                expires_at: Some("2027-01-01T00:00:00Z".to_string()),
            },
            RemoteKey {
                id: "k_1".to_string(),
                key: Some("diviga_abc".to_string()),
                user_id: "cus_2".to_string(),
                name: "ci".to_string(),
                permissions: None,
                expires_at: None,
            },
        ];
        let canonical = canonical_keys(&keys);
        // Python: json.dumps([...], sort_keys=True, separators=(",",":"), ensure_ascii=False)
        // Expected: array order preserved (k_2 first), per-key keys sorted,
        // nulls explicit, nested permissions sorted.
        assert_eq!(
            canonical,
            concat!(
                r#"[{"expires_at":"2027-01-01T00:00:00Z","id":"k_2","key":null,"name":"prod","permissions":{"read":["*"],"write":["*"]},"user_id":"cus_1"},"#,
                r#"{"expires_at":null,"id":"k_1","key":"diviga_abc","name":"ci","permissions":null,"user_id":"cus_2"}]"#
            )
        );
    }

    #[test]
    fn test_sha256_hex_known_vector() {
        // sha256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_canonical_keys_non_ascii_matches_python() {
        // The canonical serialization must be byte-identical to the Python
        // side (`json.dumps(..., ensure_ascii=False)`) for non-ASCII names:
        // Python writes raw UTF-8, and serde_json must too — otherwise a key
        // with a non-ASCII name would break `keys_sha256` and lock every
        // customer with such a key out.
        let key = RemoteKey {
            id: "k_1".to_string(),
            key: Some("diviga_x".to_string()),
            user_id: "cus".to_string(),
            name: "café ☕".to_string(),
            permissions: None,
            expires_at: None,
        };
        let canonical = canonical_keys(std::slice::from_ref(&key));
        assert_eq!(
            canonical.as_bytes(),
            &[
                b'[', b'{', b'"', b'e', b'x', b'p', b'i', b'r', b'e', b's', b'_', b'a', b't', b'"',
                b':', b'n', b'u', b'l', b'l', b',', b'"', b'i', b'd', b'"', b':', b'"', b'k', b'_',
                b'1', b'"', b',', b'"', b'k', b'e', b'y', b'"', b':', b'"', b'd', b'i', b'v', b'i',
                b'g', b'a', b'_', b'x', b'"', b',', b'"', b'n', b'a', b'm', b'e', b'"', b':', b'"',
                b'c', b'a', b'f', 0xC3, 0xA9, b' ', 0xE2, 0x98, 0x95, b'"', b',', b'"', b'p', b'e',
                b'r', b'm', b'i', b's', b's', b'i', b'o', b'n', b's', b'"', b':', b'n', b'u', b'l',
                b'l', b',', b'"', b'u', b's', b'e', b'r', b'_', b'i', b'd', b'"', b':', b'"', b'c',
                b'u', b's', b'"', b'}', b']',
            ]
        );
    }
}
