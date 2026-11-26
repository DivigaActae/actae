// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Purpose-bound federation token verification (Phase 4).
//!
//! The fleet gateway / SaaS control plane issues short-lived Ed25519 tokens to
//! gateways and connectors. This module verifies them in the canonical server:
//!
//! * **purpose-bound** — `instance_observation` (canonical read/stream only)
//!   or `instance_command` (exactly one manifest operation / action /
//!   resource grant);
//! * **keyed** — `kid` selects a verification key so keys can rotate; revoked
//!   key ids are rejected;
//! * **clock-safe** — `exp`/`nbf` validated with a configurable leeway;
//! * **scoped** — `iss`/`aud`/`instance` must match the server's configuration
//!   and this instance's install id, so a token cannot be replayed across
//!   instances;
//! * **command-validated** — a command token's `op` must exist in the canonical
//!   manifest (`spec/api-parity/v1/manifest.json`, embedded at build time) and
//!   its `action` must match the manifest's declared action for that operation,
//!   so a token cannot claim a broader action than the operation permits.
//!
//! Federation secrets/keys are never the license, session, API-key-hash, or
//! instance-JWT secrets (section 4.1). Verification keys come from the local
//! operator config (env) and the control-plane sync payload; both are merged by
//! the effective-config helper, and local policy can only narrow.

use base64::Engine;
use ed25519_dalek::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};
use sonic_rs::JsonContainerTrait;
use sonic_rs::JsonValueTrait;

pub const PURPOSE_OBSERVATION: &str = "instance_observation";
pub const PURPOSE_COMMAND: &str = "instance_command";
pub const PURPOSE_FLEET_ACCESS: &str = "fleet_access";

/// Canonical operation manifest, embedded at build time so the verifier and the
/// published contract can never drift apart.
const MANIFEST: &str = include_str!("../../../spec/api-parity/v1/manifest.json");

/// A federation purpose (`instance_observation` | `instance_command` |
/// `fleet_access`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FederationPurpose {
    Observation,
    Command,
    /// Customer-backend credential verified by the fleet gateway (Phase 5),
    /// not by canonical instances.
    FleetAccess,
}

impl FederationPurpose {
    pub fn as_str(&self) -> &'static str {
        match self {
            FederationPurpose::Observation => PURPOSE_OBSERVATION,
            FederationPurpose::Command => PURPOSE_COMMAND,
            FederationPurpose::FleetAccess => PURPOSE_FLEET_ACCESS,
        }
    }
}

/// One Ed25519 verification key.
#[derive(Debug, Clone)]
pub struct VerificationKey {
    pub kid: String,
    pub key: VerifyingKey,
}

/// How a federation token was rejected.
#[derive(Debug, Clone, PartialEq)]
pub enum FederationError {
    /// Not a federation token (wrong shape / alg / signature).
    NotAFederationToken(String),
    Malformed(String),
    UnknownKeyId(String),
    RevokedKeyId(String),
    SignatureInvalid,
    Expired,
    NotYetValid,
    AudienceMismatch(String),
    IssuerMismatch(String),
    InstanceMismatch {
        expected: String,
        actual: String,
    },
    UnknownPurpose(String),
    CommandOperationUnknown(String),
    CommandActionMismatch {
        op: String,
        claimed: String,
        manifest: String,
    },
    CommandMissingResources,
    MalformedClaims(String),
}

impl std::fmt::Display for FederationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FederationError::NotAFederationToken(m) => write!(f, "not a federation token: {m}"),
            FederationError::Malformed(m) => write!(f, "malformed federation token: {m}"),
            FederationError::UnknownKeyId(k) => write!(f, "unknown federation key id: {k}"),
            FederationError::RevokedKeyId(k) => write!(f, "revoked federation key id: {k}"),
            FederationError::SignatureInvalid => write!(f, "federation token signature invalid"),
            FederationError::Expired => write!(f, "federation token expired"),
            FederationError::NotYetValid => write!(f, "federation token not yet valid"),
            FederationError::AudienceMismatch(a) => write!(f, "federation audience mismatch: {a}"),
            FederationError::IssuerMismatch(i) => write!(f, "federation issuer mismatch: {i}"),
            FederationError::InstanceMismatch { expected, actual } => write!(
                f,
                "federation token instance '{actual}' does not match this instance '{expected}'"
            ),
            FederationError::UnknownPurpose(p) => write!(f, "unknown federation purpose: {p}"),
            FederationError::CommandOperationUnknown(op) => {
                write!(f, "command operation not in manifest: {op}")
            }
            FederationError::CommandActionMismatch { op, claimed, manifest } => write!(
                f,
                "command token claims action '{claimed}' for {op} but the manifest declares '{manifest}'"
            ),
            FederationError::CommandMissingResources => {
                write!(f, "command token missing resource patterns")
            }
            FederationError::MalformedClaims(m) => write!(f, "malformed claims: {m}"),
        }
    }
}

impl std::error::Error for FederationError {}

/// Verified federation claims.
#[derive(Debug, Clone, PartialEq)]
pub struct FederationClaims {
    pub purpose: FederationPurpose,
    pub issuer: String,
    pub audience: String,
    /// Principal (organization/backend identity) that obtained the token.
    pub principal: String,
    /// Target instance id the token is scoped to.
    pub instance: String,
    pub issued_at: i64,
    pub expires_at: i64,
    pub jti: String,
    /// Observation: channel read patterns (empty = deny all, fail closed).
    pub channels: Vec<String>,
    /// Observation: optional event-type allow patterns.
    pub types: Vec<String>,
    /// Command: the one canonical operation id.
    pub operation_id: Option<String>,
    /// Command: the one action ("read"/"write"/"delete").
    pub action: Option<String>,
    /// Command: resource glob patterns the action applies to.
    pub resources: Vec<String>,
    /// Command: optional request digest binding (idempotency/anti-replay).
    pub request_digest: Option<String>,
    /// fleet_access: the effective scope set granted to this token (PR-001).
    /// An empty list on a fleet_access token is rejected at the gateway (fail
    /// closed); it is never interpreted as legacy full access.
    pub scopes: Vec<String>,
}

/// The effective federation configuration: local (env) trust anchors merged
/// with whatever the control-plane sync delivered.
#[derive(Debug, Clone, Default)]
pub struct FederationConfig {
    pub enabled: bool,
    pub issuer: String,
    pub audience: String,
    pub verification_keys: Vec<VerificationKey>,
    pub revoked_key_ids: Vec<String>,
    pub clock_skew_secs: i64,
}

impl FederationConfig {
    pub fn is_enabled(&self) -> bool {
        self.enabled && !self.verification_keys.is_empty()
    }

    /// Load the local operator federation configuration from the environment.
    /// Verification keys are `kid:base64url(ed25519-public-key)` entries,
    /// semicolon-separated. `ACTAE_FEDERATION_ENABLED` must be truthy AND at
    /// least one key present for federation to be active (fails closed).
    pub fn from_env() -> anyhow::Result<Self> {
        use std::env;
        let enabled = env::var("ACTAE_FEDERATION_ENABLED")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let issuer =
            env::var("ACTAE_FEDERATION_ISSUER").unwrap_or_else(|_| "actae-fleet".to_string());
        let audience =
            env::var("ACTAE_FEDERATION_AUDIENCE").unwrap_or_else(|_| "actae-instances".to_string());
        let clock_skew_secs = env::var("ACTAE_FEDERATION_CLOCK_SKEW_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60);

        let mut verification_keys = Vec::new();
        if let Ok(spec) = env::var("ACTAE_FEDERATION_VERIFICATION_KEYS") {
            for entry in spec.split(';') {
                let entry = entry.trim();
                if entry.is_empty() {
                    continue;
                }
                verification_keys.push(parse_verification_key(entry).map_err(anyhow::Error::msg)?);
            }
        }

        let revoked_key_ids: Vec<String> = env::var("ACTAE_FEDERATION_REVOKED_KEY_IDS")
            .map(|s| {
                s.split(',')
                    .map(|k| k.trim().to_string())
                    .filter(|k| !k.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        Ok(FederationConfig {
            enabled,
            issuer,
            audience,
            verification_keys,
            revoked_key_ids,
            clock_skew_secs,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClaimsWire {
    purpose: String,
    #[serde(default)]
    iss: Option<String>,
    #[serde(default)]
    aud: Option<String>,
    #[serde(default)]
    sub: Option<String>,
    #[serde(default)]
    instance: Option<String>,
    #[serde(default)]
    iat: Option<i64>,
    #[serde(default)]
    exp: Option<i64>,
    #[serde(default)]
    nbf: Option<i64>,
    #[serde(default)]
    jti: Option<String>,
    #[serde(default)]
    channels: Option<Vec<String>>,
    #[serde(default)]
    types: Option<Vec<String>>,
    #[serde(default)]
    op: Option<String>,
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    resources: Option<Vec<String>>,
    #[serde(default)]
    request_digest: Option<String>,
    #[serde(default)]
    scopes: Option<Vec<String>>,
}

/// Verify a federation token against the effective config and this instance's
/// id. Fails closed: an unrecognized token is `NotAFederationToken`, which the
/// caller treats as "not a federation credential" (session-JWT path continues).
pub fn verify(
    config: &FederationConfig,
    token: &str,
    instance_id: &str,
) -> Result<FederationClaims, FederationError> {
    verify_for(
        config,
        token,
        instance_id,
        &[FederationPurpose::Observation, FederationPurpose::Command],
    )
}

/// Verify a federation token against the effective config, this instance's
/// id, and an explicit allow-list of purposes. The fleet gateway verifies
/// `fleet_access` tokens; canonical instances verify `instance_observation` /
/// `instance_command` only. Fails closed: an unrecognized token is
/// `NotAFederationToken`, which the caller treats as "not a federation
/// credential".
pub fn verify_for(
    config: &FederationConfig,
    token: &str,
    instance_id: &str,
    allowed_purposes: &[FederationPurpose],
) -> Result<FederationClaims, FederationError> {
    if !config.is_enabled() {
        return Err(FederationError::NotAFederationToken(
            "federation disabled".into(),
        ));
    }

    let parts: Vec<&str> = token.splitn(3, '.').collect();
    if parts.len() != 3 {
        return Err(FederationError::NotAFederationToken(
            "compact JWT expected (header.payload.signature)".into(),
        ));
    }
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let header_bytes = engine
        .decode(parts[0])
        .map_err(|_| FederationError::Malformed("header".into()))?;
    let payload_bytes = engine
        .decode(parts[1])
        .map_err(|_| FederationError::Malformed("payload".into()))?;
    let sig_bytes: [u8; 64] = engine
        .decode(parts[2])
        .map_err(|_| FederationError::Malformed("signature".into()))?
        .try_into()
        .map_err(|_| FederationError::Malformed("signature length".into()))?;

    let header: sonic_rs::Value = sonic_rs::from_slice(&header_bytes)
        .map_err(|_| FederationError::Malformed("header not JSON".into()))?;
    let header_obj = header
        .as_object()
        .ok_or_else(|| FederationError::Malformed("header not an object".into()))?;
    let alg = header_obj
        .get(&"alg")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if alg != "EdDSA" {
        return Err(FederationError::NotAFederationToken(format!(
            "unexpected alg '{alg}'"
        )));
    }
    let kid = header_obj
        .get(&"kid")
        .and_then(|v| v.as_str())
        .ok_or_else(|| FederationError::Malformed("missing kid".into()))?;

    if config.revoked_key_ids.iter().any(|k| k == kid) {
        return Err(FederationError::RevokedKeyId(kid.to_string()));
    }
    let vkey = config
        .verification_keys
        .iter()
        .find(|k| k.kid == kid)
        .ok_or_else(|| FederationError::UnknownKeyId(kid.to_string()))?;

    let message = format!("{}.{}", parts[0], parts[1]);
    let signature = Signature::from_bytes(&sig_bytes);
    vkey.key
        .verify_strict(message.as_bytes(), &signature)
        .map_err(|_| FederationError::SignatureInvalid)?;

    let wire: ClaimsWire = serde_json::from_slice(&payload_bytes)
        .map_err(|_| FederationError::MalformedClaims("payload not JSON".into()))?;

    let now = chrono::Utc::now().timestamp();
    let leeway = config.clock_skew_secs.max(0);

    let exp = wire
        .exp
        .ok_or_else(|| FederationError::MalformedClaims("missing exp".into()))?;
    if now > exp + leeway {
        return Err(FederationError::Expired);
    }
    if let Some(nbf) = wire.nbf {
        if now + leeway < nbf {
            return Err(FederationError::NotYetValid);
        }
    }

    if let Some(aud) = &wire.aud {
        if !config.audience.is_empty() && aud != &config.audience {
            return Err(FederationError::AudienceMismatch(aud.clone()));
        }
    }
    if let Some(iss) = &wire.iss {
        if !config.issuer.is_empty() && iss != &config.issuer {
            return Err(FederationError::IssuerMismatch(iss.clone()));
        }
    }

    let instance = wire
        .instance
        .ok_or_else(|| FederationError::MalformedClaims("missing instance".into()))?;
    if instance != instance_id {
        return Err(FederationError::InstanceMismatch {
            expected: instance_id.to_string(),
            actual: instance,
        });
    }

    let purpose = match wire.purpose.as_str() {
        PURPOSE_OBSERVATION => FederationPurpose::Observation,
        PURPOSE_COMMAND => FederationPurpose::Command,
        PURPOSE_FLEET_ACCESS => FederationPurpose::FleetAccess,
        other => return Err(FederationError::UnknownPurpose(other.to_string())),
    };
    if !allowed_purposes.contains(&purpose) {
        return Err(FederationError::UnknownPurpose(wire.purpose.clone()));
    }

    let claims = FederationClaims {
        purpose,
        issuer: wire.iss.clone().unwrap_or_default(),
        audience: wire.aud.clone().unwrap_or_default(),
        principal: wire.sub.clone().unwrap_or_default(),
        instance,
        issued_at: wire.iat.unwrap_or(now),
        expires_at: exp,
        jti: wire.jti.unwrap_or_default(),
        channels: wire.channels.unwrap_or_default(),
        types: wire.types.unwrap_or_default(),
        operation_id: wire.op,
        action: wire.action,
        resources: wire.resources.unwrap_or_default(),
        request_digest: wire.request_digest,
        scopes: wire.scopes.unwrap_or_default(),
    };

    match claims.purpose {
        FederationPurpose::Observation => {
            // Fail closed: an observation token without channel patterns can
            // read nothing.
            if claims.channels.is_empty() {
                return Err(FederationError::MalformedClaims(
                    "observation token must carry channel patterns".into(),
                ));
            }
        }
        FederationPurpose::Command => {
            let op = claims
                .operation_id
                .clone()
                .ok_or_else(|| FederationError::MalformedClaims("missing op".into()))?;
            let action = claims
                .action
                .clone()
                .ok_or_else(|| FederationError::MalformedClaims("missing action".into()))?;
            if claims.resources.is_empty() {
                return Err(FederationError::CommandMissingResources);
            }
            validate_command_op(&op, &action)?;
        }
        FederationPurpose::FleetAccess => {
            // No channel/op requirements; verified by the fleet gateway.
        }
    }

    Ok(claims)
}

/// A command token's operation must exist in the canonical manifest and its
/// action must equal the manifest's declared action for that operation.
fn validate_command_op(op: &str, action: &str) -> Result<(), FederationError> {
    let manifest: sonic_rs::Value = sonic_rs::from_str(MANIFEST)
        .map_err(|_| FederationError::MalformedClaims("manifest embedded".into()))?;
    let ops = manifest
        .as_object()
        .and_then(|o| o.get(&"operations"))
        .and_then(|v| v.as_array())
        .ok_or_else(|| FederationError::MalformedClaims("manifest operations".into()))?;
    let entry = ops
        .iter()
        .find(|o| o.get("id").and_then(|i| i.as_str()) == Some(op))
        .ok_or_else(|| FederationError::CommandOperationUnknown(op.to_string()))?;
    let manifest_action = entry
        .get("action")
        .and_then(|a| a.as_str())
        .unwrap_or_default();
    if manifest_action != action {
        return Err(FederationError::CommandActionMismatch {
            op: op.to_string(),
            claimed: action.to_string(),
            manifest: manifest_action.to_string(),
        });
    }
    Ok(())
}

/// Build the shared `AuthContext` for a verified federation principal.
///
/// * Observation → read-only `FederatedObservation` (channel patterns only).
/// * Command → `FederatedCommand` carrying exactly one operation/action and
///   the resource grant; it cannot impersonate dashboard/JWT authority and the
///   `AuthContext` permission checks deny every other action/channel.
pub fn to_auth_context(claims: &FederationClaims) -> crate::auth::types::AuthContext {
    use crate::auth::authorization::ApiKeyPermissions;
    use crate::auth::types::AuthContext;
    match claims.purpose {
        FederationPurpose::Observation => AuthContext::FederatedObservation {
            principal: claims.principal.clone(),
            instance_id: claims.instance.clone(),
            permissions: ApiKeyPermissions {
                read: claims.channels.clone(),
                write: Vec::new(),
                delete: Vec::new(),
                admin: Vec::new(),
            },
        },
        FederationPurpose::Command => {
            let action = claims.action.clone().unwrap_or_default();
            let mut read = Vec::new();
            let mut write = Vec::new();
            let mut delete = Vec::new();
            match action.as_str() {
                "read" => read = claims.resources.clone(),
                "write" => write = claims.resources.clone(),
                "delete" => delete = claims.resources.clone(),
                _ => {}
            }
            AuthContext::FederatedCommand {
                principal: claims.principal.clone(),
                instance_id: claims.instance.clone(),
                operation_id: claims.operation_id.clone().unwrap_or_default(),
                action,
                permissions: ApiKeyPermissions {
                    read,
                    write,
                    delete,
                    admin: Vec::new(),
                },
                request_digest: claims.request_digest.clone(),
            }
        }
        // fleet_access tokens are never accepted by canonical instances; the
        // fleet gateway verifies them itself.
        FederationPurpose::FleetAccess => AuthContext::Anonymous,
    }
}

/// Verify a command token's `request_digest` claim against the ACTUAL request
/// body the canonical server received (PR-014). The digest is computed over
/// the same canonical inputs the gateway used: the serialized body, the
/// idempotency key and the confirmation target. Method/path are already bound
/// to the token by the `op` gate (the manifest maps one op to exactly one
/// method/path) and by the resource patterns, so the digest binds the mutable
/// CONTENT — the anti-replay property. A mismatch means the token was minted
/// for different content — reject before handler execution.
pub fn verify_request_digest(
    expected: &str,
    body: &[u8],
    idempotency_key: Option<&str>,
    confirm_target: Option<&str>,
) -> bool {
    let digest = compute_request_digest(body, idempotency_key, confirm_target);
    constant_time_eq(expected, &digest)
}

/// Canonical request digest (mirrors the gateway's `dispatch::request_digest`).
/// SHA-256 over NUL-separated canonical inputs: body, idempotency key and
/// confirmation target.
pub fn compute_request_digest(
    body: &[u8],
    idempotency_key: Option<&str>,
    confirm_target: Option<&str>,
) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(body);
    hasher.update([0u8]);
    if let Some(key) = idempotency_key {
        hasher.update(b"idem:");
        hasher.update(key.as_bytes());
    }
    hasher.update([0u8]);
    if let Some(target) = confirm_target {
        hasher.update(b"confirm:");
        hasher.update(target.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

/// Constant-time string equality for digest comparison.
fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.as_bytes().iter().zip(b.as_bytes()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Parse an env-encoded verification-key entry `kid:base64url(32-byte key)`.
pub fn parse_verification_key(spec: &str) -> Result<VerificationKey, String> {
    let (kid, b64) = spec
        .split_once(':')
        .ok_or_else(|| format!("expected kid:base64url, got '{spec}'"))?;
    if kid.trim().is_empty() {
        return Err("empty key id".into());
    }
    let raw = base64::engine::general_purpose::STANDARD
        .decode(b64.trim())
        .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(b64.trim()))
        .map_err(|e| format!("invalid base64 public key for {kid}: {e}"))?;
    let bytes: [u8; 32] = raw
        .try_into()
        .map_err(|_| format!("public key for {kid} must be exactly 32 bytes"))?;
    let key = VerifyingKey::from_bytes(&bytes)
        .map_err(|e| format!("invalid Ed25519 public key for {kid}: {e}"))?;
    Ok(VerificationKey {
        kid: kid.to_string(),
        key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey, Verifier};

    /// Insert a claim into the test token's claims object.
    fn set(claims: &mut sonic_rs::Value, key: &str, val: sonic_rs::Value) {
        use sonic_rs::JsonValueMutTrait;
        if let Some(obj) = claims.as_object_mut() {
            obj.insert(key, val);
        }
    }

    struct TestKeys {
        signing: SigningKey,
        kid: String,
    }

    impl TestKeys {
        fn new(kid: &str) -> Self {
            let signing = SigningKey::from_bytes(&[0x42u8; 32]);
            Self {
                signing,
                kid: kid.to_string(),
            }
        }

        fn verifying(&self) -> VerificationKey {
            VerificationKey {
                kid: self.kid.clone(),
                key: VerifyingKey::from(&self.signing),
            }
        }

        fn sign(&self, claims: sonic_rs::Value) -> String {
            let header = sonic_rs::json!({"alg": "EdDSA", "kid": self.kid, "typ": "JWT"});
            let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
            let h = engine.encode(sonic_rs::to_string(&header).unwrap().as_bytes());
            let p = engine.encode(sonic_rs::to_string(&claims).unwrap().as_bytes());
            let msg = format!("{h}.{p}");
            let sig = self.signing.sign(msg.as_bytes());
            format!("{msg}.{}", engine.encode(sig.to_bytes()))
        }
    }

    fn config(keys: Vec<VerificationKey>) -> FederationConfig {
        FederationConfig {
            enabled: true,
            issuer: "actae-fleet".into(),
            audience: "actae-instances".into(),
            verification_keys: keys,
            revoked_key_ids: Vec::new(),
            clock_skew_secs: 30,
        }
    }

    fn observation_claims(_k: &TestKeys) -> sonic_rs::Value {
        sonic_rs::json!({
            "purpose": "instance_observation",
            "iss": "actae-fleet",
            "aud": "actae-instances",
            "sub": "org:123",
            "instance": "ins_test",
            "iat": chrono::Utc::now().timestamp(),
            "exp": chrono::Utc::now().timestamp() + 300,
            "jti": "jti-1",
            "channels": ["public.*"],
            "types": ["tool.*"]
        })
    }

    fn command_claims(_k: &TestKeys) -> sonic_rs::Value {
        sonic_rs::json!({
            "purpose": "instance_command",
            "iss": "actae-fleet",
            "aud": "actae-instances",
            "sub": "org:123",
            "instance": "ins_test",
            "iat": chrono::Utc::now().timestamp(),
            "exp": chrono::Utc::now().timestamp() + 60,
            "jti": "jti-2",
            "op": "actae.api.v1.events.record",
            "action": "write",
            "resources": ["public.*"],
            "request_digest": "deadbeef"
        })
    }

    #[test]
    fn observation_token_verifies() {
        let k = TestKeys::new("k1");
        let cfg = config(vec![k.verifying()]);
        let token = k.sign(observation_claims(&k));
        let claims = verify(&cfg, &token, "ins_test").unwrap();
        assert_eq!(claims.purpose, FederationPurpose::Observation);
        assert_eq!(claims.channels, vec!["public.*"]);
        assert_eq!(claims.principal, "org:123");
    }

    #[test]
    fn command_token_verifies_and_action_matches_manifest() {
        let k = TestKeys::new("k1");
        let cfg = config(vec![k.verifying()]);
        let token = k.sign(command_claims(&k));
        let claims = verify(&cfg, &token, "ins_test").unwrap();
        assert_eq!(claims.purpose, FederationPurpose::Command);
        assert_eq!(
            claims.operation_id.as_deref(),
            Some("actae.api.v1.events.record")
        );
        assert_eq!(claims.action.as_deref(), Some("write"));
    }

    #[test]
    fn command_action_must_match_manifest() {
        let k = TestKeys::new("k1");
        let cfg = config(vec![k.verifying()]);
        let mut claims = command_claims(&k);
        set(&mut claims, "action", sonic_rs::json!("delete")); // events.record is write
        let token = k.sign(claims);
        assert!(matches!(
            verify(&cfg, &token, "ins_test"),
            Err(FederationError::CommandActionMismatch { .. })
        ));
    }

    #[test]
    fn unknown_operation_rejected() {
        let k = TestKeys::new("k1");
        let cfg = config(vec![k.verifying()]);
        let mut claims = command_claims(&k);
        set(
            &mut claims,
            "op",
            sonic_rs::json!("actae.api.v1.not.a.real.op"),
        );
        let token = k.sign(claims);
        assert!(matches!(
            verify(&cfg, &token, "ins_test"),
            Err(FederationError::CommandOperationUnknown(_))
        ));
    }

    #[test]
    fn wrong_instance_rejected() {
        let k = TestKeys::new("k1");
        let cfg = config(vec![k.verifying()]);
        let token = k.sign(observation_claims(&k));
        assert!(matches!(
            verify(&cfg, &token, "ins_other"),
            Err(FederationError::InstanceMismatch { .. })
        ));
    }

    #[test]
    fn expired_token_rejected() {
        let k = TestKeys::new("k1");
        let cfg = config(vec![k.verifying()]);
        let mut claims = observation_claims(&k);
        set(
            &mut claims,
            "exp",
            sonic_rs::json!(chrono::Utc::now().timestamp() - 100),
        );
        let token = k.sign(claims);
        assert_eq!(
            verify(&cfg, &token, "ins_test"),
            Err(FederationError::Expired)
        );
    }

    #[test]
    fn nbf_rejects_future_tokens() {
        let k = TestKeys::new("k1");
        let cfg = config(vec![k.verifying()]);
        let mut claims = observation_claims(&k);
        set(
            &mut claims,
            "nbf",
            sonic_rs::json!(chrono::Utc::now().timestamp() + 3600),
        );
        let token = k.sign(claims);
        assert_eq!(
            verify(&cfg, &token, "ins_test"),
            Err(FederationError::NotYetValid)
        );
    }

    #[test]
    fn audience_mismatch_rejected() {
        let k = TestKeys::new("k1");
        let cfg = config(vec![k.verifying()]);
        let mut claims = observation_claims(&k);
        set(&mut claims, "aud", sonic_rs::json!("other-audience"));
        let token = k.sign(claims);
        assert!(matches!(
            verify(&cfg, &token, "ins_test"),
            Err(FederationError::AudienceMismatch(_))
        ));
    }

    #[test]
    fn issuer_mismatch_rejected() {
        let k = TestKeys::new("k1");
        let cfg = config(vec![k.verifying()]);
        let mut claims = observation_claims(&k);
        set(&mut claims, "iss", sonic_rs::json!("evil-issuer"));
        let token = k.sign(claims);
        assert!(matches!(
            verify(&cfg, &token, "ins_test"),
            Err(FederationError::IssuerMismatch(_))
        ));
    }

    #[test]
    fn unknown_kid_rejected() {
        let k = TestKeys::new("k1");
        let other = TestKeys::new("k2");
        let cfg = config(vec![k.verifying()]);
        let token = other.sign(observation_claims(&other));
        assert!(matches!(
            verify(&cfg, &token, "ins_test"),
            Err(FederationError::UnknownKeyId(_))
        ));
    }

    #[test]
    fn revoked_kid_rejected() {
        let k = TestKeys::new("k1");
        let mut cfg = config(vec![k.verifying()]);
        cfg.revoked_key_ids.push("k1".into());
        let token = k.sign(observation_claims(&k));
        assert!(matches!(
            verify(&cfg, &token, "ins_test"),
            Err(FederationError::RevokedKeyId(_))
        ));
    }

    #[test]
    fn rotated_keys_accepted() {
        let k1 = TestKeys::new("k1");
        let k2 = TestKeys::new("k2");
        let cfg = config(vec![k1.verifying(), k2.verifying()]);
        let token = k2.sign(observation_claims(&k2));
        let claims = verify(&cfg, &token, "ins_test").unwrap();
        assert_eq!(claims.purpose, FederationPurpose::Observation);
    }

    #[test]
    fn tampered_signature_rejected() {
        let k = TestKeys::new("k1");
        let cfg = config(vec![k.verifying()]);
        let token = k.sign(observation_claims(&k));
        // Flip a payload byte (same length) so the signature no longer matches.
        let mut parts: Vec<&str> = token.split('.').collect();
        let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let mut payload = engine.decode(parts[1]).unwrap();
        payload[0] ^= 0x01;
        parts[1] = Box::leak(engine.encode(&payload).into_boxed_str());
        let tampered = format!("{}.{}.{}", parts[0], parts[1], parts[2]);
        assert_eq!(
            verify(&cfg, &tampered, "ins_test"),
            Err(FederationError::SignatureInvalid)
        );
    }

    #[test]
    fn observation_without_channels_fails_closed() {
        let k = TestKeys::new("k1");
        let cfg = config(vec![k.verifying()]);
        let mut claims = observation_claims(&k);
        set(&mut claims, "channels", sonic_rs::json!([]));
        let token = k.sign(claims);
        assert!(matches!(
            verify(&cfg, &token, "ins_test"),
            Err(FederationError::MalformedClaims(_))
        ));
    }

    #[test]
    fn disabled_config_rejects() {
        let k = TestKeys::new("k1");
        let mut cfg = config(vec![k.verifying()]);
        cfg.enabled = false;
        let token = k.sign(observation_claims(&k));
        assert!(matches!(
            verify(&cfg, &token, "ins_test"),
            Err(FederationError::NotAFederationToken(_))
        ));
    }

    #[test]
    fn parse_verification_key_roundtrip() {
        let signing = SigningKey::from_bytes(&[0x42u8; 32]);
        let pub_bytes = VerifyingKey::from(&signing).to_bytes();
        let b64 = base64::engine::general_purpose::STANDARD.encode(pub_bytes);
        let vk = parse_verification_key(&format!("kid-a:{b64}")).unwrap();
        assert_eq!(vk.kid, "kid-a");
        assert_eq!(vk.key.to_bytes(), pub_bytes);
    }

    #[test]
    fn malformed_key_spec_rejected() {
        assert!(parse_verification_key("no-colon").is_err());
        assert!(parse_verification_key("kid:###not-base64###").is_err());
    }

    #[test]
    fn command_missing_resources_rejected() {
        let k = TestKeys::new("k1");
        let cfg = config(vec![k.verifying()]);
        let mut claims = command_claims(&k);
        set(&mut claims, "resources", sonic_rs::json!([]));
        let token = k.sign(claims);
        assert_eq!(
            verify(&cfg, &token, "ins_test"),
            Err(FederationError::CommandMissingResources)
        );
    }

    #[test]
    fn valid_token_verifies_with_verifier_trait() {
        // Ensure the generated keys actually verify via ed25519-dalek.
        let k = TestKeys::new("k1");
        let msg = b"hello";
        let sig = k.signing.sign(msg);
        assert!(VerifyingKey::from(&k.signing).verify(msg, &sig).is_ok());
    }

    #[test]
    fn request_digest_binds_content() {
        // PR-014: the digest over (body, idem key, confirm target) must match
        // only the exact content.
        let d1 = compute_request_digest(b"{\"a\":1}", Some("key-1"), Some("ch"));
        let d2 = compute_request_digest(b"{\"a\":1}", Some("key-1"), Some("ch"));
        assert_eq!(d1, d2);
        // Changed body / key / confirm all change the digest.
        assert_ne!(
            d1,
            compute_request_digest(b"{\"a\":2}", Some("key-1"), Some("ch"))
        );
        assert_ne!(
            d1,
            compute_request_digest(b"{\"a\":1}", Some("key-2"), Some("ch"))
        );
        assert_ne!(
            d1,
            compute_request_digest(b"{\"a\":1}", Some("key-1"), None)
        );
        // verify_request_digest accepts the exact match and rejects a change.
        assert!(verify_request_digest(
            &d1,
            b"{\"a\":1}",
            Some("key-1"),
            Some("ch")
        ));
        assert!(!verify_request_digest(
            &d1,
            b"{\"a\":1}",
            Some("key-1"),
            None
        ));
        assert!(!verify_request_digest(
            &d1,
            b"{\"a\":9}",
            Some("key-1"),
            Some("ch")
        ));
    }
}
