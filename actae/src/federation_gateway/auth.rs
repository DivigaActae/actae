// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Gateway authentication (Phase 5).
//!
//! The customer backend presents a short-lived `fleet_access` token minted by
//! the SaaS (`POST /api/v1/fleet/token`). The gateway verifies it with the
//! same purpose-bound Ed25519 machinery as the canonical instances
//! (`auth::federation`) and binds the path organization to the token's
//! `sub` (`org:<id>`). Fleet credentials are never the license, session,
//! API-key-hash, or instance-JWT secrets.

use crate::auth::federation::{verify_for, FederationConfig, FederationPurpose};
use anyhow::{anyhow, Result};

/// Verified fleet-access principal.
#[derive(Debug, Clone)]
pub struct FleetPrincipal {
    pub organization_id: String,
    pub principal: String,
    pub expires_at: i64,
    /// The effective scope set granted to this token (PR-001). The gateway
    /// enforces operation-specific grants from this list.
    pub scopes: Vec<String>,
}

/// Verify a `fleet_access` bearer token and bind it to the path organization.
/// Fails closed: a token for another org, a wrong purpose, an unknown/revoked
/// key, an expired token, a bad signature, or a **missing/empty scope list** is
/// rejected. A missing scope claim is never interpreted as legacy full access.
pub fn verify_fleet_access(
    cfg: &FederationConfig,
    bearer: &str,
    organization_id: &str,
) -> Result<FleetPrincipal> {
    let token = bearer
        .strip_prefix("Bearer ")
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing Bearer token"))?;

    // fleet_access tokens are instance wildcards (`instance: "*"`).
    let claims = verify_for(cfg, token, "*", &[FederationPurpose::FleetAccess])
        .map_err(|e| anyhow!("invalid fleet token: {e}"))?;
    if claims.audience != cfg.audience {
        return Err(anyhow!("fleet token audience mismatch"));
    }
    let org = claims
        .principal
        .strip_prefix("org:")
        .ok_or_else(|| anyhow!("fleet token principal is not org-scoped"))?;
    if org != organization_id {
        return Err(anyhow!("fleet token is for a different organization"));
    }
    if claims.scopes.is_empty() {
        return Err(anyhow!(
            "fleet token carries no scopes (fail closed; empty scopes deny all)"
        ));
    }
    Ok(FleetPrincipal {
        organization_id: organization_id.to_string(),
        principal: claims.principal,
        expires_at: claims.expires_at,
        scopes: claims.scopes,
    })
}

/// Whether the caller holds a given fleet scope. Unknown/empty scope grants
/// never match, so they deny by construction (fail closed).
pub fn has_scope(p: &FleetPrincipal, required: &str) -> bool {
    if required.is_empty() {
        return false;
    }
    p.scopes.iter().any(|s| s == required)
}

/// The fleet scope required to invoke a canonical operation id. This is the
/// central operation_id → scope mapping (PR-001), derived from the embedded
/// canonical manifest — never duplicated in SaaS and Rust. Returns `None`
/// when the operation is not fleet-invokable (unknown or auth/dashboard
/// surface).
pub fn required_scope_for_op(op_id: &str) -> Option<String> {
    crate::federation_gateway::dispatch::fleet_supported_scope(op_id)
}

/// Whether the caller may invoke a single-instance command/read for the given
/// canonical operation id. Returns `Err` with a stable, instance-agnostic
/// message when the operation is unsupported or the scope is missing — so a
/// foreign/insufficient caller cannot learn whether an instance exists.
pub fn authorize_op(p: &FleetPrincipal, op_id: &str) -> Result<()> {
    let scope = required_scope_for_op(op_id)
        .ok_or_else(|| anyhow!("operation {op_id} is not available through the fleet API"))?;
    if has_scope(p, &scope) {
        Ok(())
    } else {
        Err(anyhow!(
            "fleet credential lacks the required scope '{scope}' for this operation"
        ))
    }
}

/// Require one exact scope. Gateway routes must use this (or
/// [`authorize_op`]) instead of treating any read-like grant as permission for
/// an unrelated resource family.
pub fn authorize_scope(p: &FleetPrincipal, required: &str) -> Result<()> {
    if has_scope(p, required) {
        Ok(())
    } else {
        Err(anyhow!(
            "fleet credential lacks the required scope '{required}'"
        ))
    }
}

/// Whether the caller may perform a generic fleet mutation. Individual
/// operations still gate on their manifest scope via [`authorize_op`]; this is
/// a coarse pre-check so a read-only token fails fast before routing lookup.
pub fn allow_mutate(p: &FleetPrincipal) -> bool {
    const WRITE_SCOPES: &[&str] = &[
        "events:write",
        "state:write",
        "state:delete",
        "channels:write",
        "channels:delete",
        "forks:create",
        "forks:promote",
        "groups:manage",
        "execution_groups:manage",
        "experiments:manage",
        "scheduler:manage",
        "executions:manage",
    ];
    WRITE_SCOPES.iter().any(|s| has_scope(p, s))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::federation::VerificationKey;
    use base64::Engine;
    use ed25519_dalek::{Signer, SigningKey};
    use sonic_rs::JsonValueMutTrait;

    fn keypair() -> (SigningKey, VerificationKey) {
        let signing = SigningKey::from_bytes(&[0x42u8; 32]);
        let pub_bytes = ed25519_dalek::VerifyingKey::from(&signing).to_bytes();
        let vk = VerificationKey {
            kid: "fk1".into(),
            key: ed25519_dalek::VerifyingKey::from_bytes(&pub_bytes).unwrap(),
        };
        (signing, vk)
    }

    fn b64(b: &[u8]) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b)
    }

    fn mint(signing: &SigningKey, claims: sonic_rs::Value) -> String {
        let header = sonic_rs::json!({"alg": "EdDSA", "kid": "fk1", "typ": "JWT"});
        let h = b64(sonic_rs::to_string(&header).unwrap().as_bytes());
        let p = b64(sonic_rs::to_string(&claims).unwrap().as_bytes());
        let msg = format!("{h}.{p}");
        let sig = signing.sign(msg.as_bytes());
        format!("{msg}.{}", b64(&sig.to_bytes()))
    }

    fn cfg(key: VerificationKey) -> FederationConfig {
        FederationConfig {
            enabled: true,
            issuer: "actae-fleet".into(),
            audience: "actae-fleet-gateway".into(),
            verification_keys: vec![key],
            revoked_key_ids: Vec::new(),
            clock_skew_secs: 60,
        }
    }

    fn fleet_claims() -> sonic_rs::Value {
        sonic_rs::json!({
            "purpose": "fleet_access",
            "iss": "actae-fleet",
            "aud": "actae-fleet-gateway",
            "sub": "org:org_1",
            "instance": "*",
            "iat": chrono::Utc::now().timestamp(),
            "exp": chrono::Utc::now().timestamp() + 300,
            "jti": "jti-fleet",
            "scopes": ["events:read", "events:write"]
        })
    }

    #[test]
    fn fleet_access_token_accepted() {
        let (signing, vk) = keypair();
        let cfg = cfg(vk);
        let token = mint(&signing, fleet_claims());
        let p = verify_fleet_access(&cfg, &format!("Bearer {token}"), "org_1").unwrap();
        assert_eq!(p.organization_id, "org_1");
        assert!(p.scopes.contains(&"events:read".to_string()));
    }

    #[test]
    fn fleet_token_without_scopes_rejected() {
        let (signing, vk) = keypair();
        let cfg = cfg(vk);
        let mut claims = fleet_claims();
        if let Some(obj) = claims.as_object_mut() {
            obj.remove(&"scopes".to_string());
        }
        let token = mint(&signing, claims);
        // Missing scopes fails closed — never treated as legacy full access.
        assert!(verify_fleet_access(&cfg, &format!("Bearer {token}"), "org_1").is_err());
    }

    #[test]
    fn fleet_token_with_empty_scopes_rejected() {
        let (signing, vk) = keypair();
        let cfg = cfg(vk);
        let mut claims = fleet_claims();
        if let Some(obj) = claims.as_object_mut() {
            obj.insert("scopes", sonic_rs::json!([]));
        }
        let token = mint(&signing, claims);
        assert!(verify_fleet_access(&cfg, &format!("Bearer {token}"), "org_1").is_err());
    }

    #[test]
    fn scope_enforcement_grants_and_denies() {
        let (signing, vk) = keypair();
        let cfg = cfg(vk);
        let token = mint(&signing, fleet_claims());
        let p = verify_fleet_access(&cfg, &format!("Bearer {token}"), "org_1").unwrap();
        // events:write granted -> record allowed.
        assert!(authorize_op(&p, "actae.api.v1.events.record").is_ok());
        // no state:write -> state save denied (fail closed).
        assert!(authorize_op(&p, "actae.api.v1.state.save").is_err());
        // auth/dashboard op is not fleet-invokable.
        assert!(authorize_op(&p, "actae.api.v1.auth.login").is_err());
        // read granted, mutation scope absent.
        assert!(allow_mutate(&p)); // has events:write
    }

    #[test]
    fn read_only_token_cannot_mutate() {
        let (signing, vk) = keypair();
        let cfg = cfg(vk);
        let mut claims = fleet_claims();
        if let Some(obj) = claims.as_object_mut() {
            obj.insert("scopes", sonic_rs::json!(["events:read", "state:read"]));
        }
        let token = mint(&signing, claims);
        let p = verify_fleet_access(&cfg, &format!("Bearer {token}"), "org_1").unwrap();
        assert!(!allow_mutate(&p));
        // Every mutation op is denied.
        assert!(authorize_op(&p, "actae.api.v1.events.record").is_err());
        assert!(authorize_op(&p, "actae.api.v1.channels.delete").is_err());
    }

    #[test]
    fn wrong_org_rejected() {
        let (signing, vk) = keypair();
        let cfg = cfg(vk);
        let token = mint(&signing, fleet_claims());
        assert!(verify_fleet_access(&cfg, &format!("Bearer {token}"), "org_2").is_err());
    }

    #[test]
    fn wrong_audience_rejected() {
        let (signing, vk) = keypair();
        let cfg = cfg(vk);
        let mut claims = fleet_claims();
        if let Some(obj) = claims.as_object_mut() {
            obj.insert("aud", sonic_rs::json!("other"));
        }
        let token = mint(&signing, claims);
        assert!(verify_fleet_access(&cfg, &format!("Bearer {token}"), "org_1").is_err());
    }

    #[test]
    fn expired_rejected() {
        let (signing, vk) = keypair();
        let cfg = cfg(vk);
        let mut claims = fleet_claims();
        if let Some(obj) = claims.as_object_mut() {
            obj.insert("exp", sonic_rs::json!(chrono::Utc::now().timestamp() - 100));
        }
        let token = mint(&signing, claims);
        assert!(verify_fleet_access(&cfg, &format!("Bearer {token}"), "org_1").is_err());
    }

    #[test]
    fn missing_bearer_rejected() {
        let (_s, vk) = keypair();
        let cfg = cfg(vk);
        assert!(verify_fleet_access(&cfg, "", "org_1").is_err());
        assert!(verify_fleet_access(&cfg, "Basic abc", "org_1").is_err());
    }

    /// Least-privilege negative authorization matrix over the ENTIRE manifest
    /// (PR-001 acceptance). For every fleet-invokable operation:
    ///   * a token holding exactly its required scope is allowed;
    ///   * a token holding ANY other scope family is denied;
    ///   * a token with no scopes is denied;
    /// and every non-fleet (auth/dashboard/public) operation is denied even by
    /// a fully-scoped token. Also covers exact direct-scope checks.
    #[test]
    fn full_manifest_scope_negative_matrix() {
        use std::collections::BTreeMap;
        use std::collections::BTreeSet;

        // op_id -> required scope (fleet-invokable only).
        let mut required: BTreeMap<String, String> = BTreeMap::new();
        // scope family -> ops that require it.
        let mut families: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for op_id in crate::federation_gateway::dispatch::fleet_supported_operation_ids() {
            if let Some(scope) = required_scope_for_op(&op_id) {
                required.insert(op_id.clone(), scope.clone());
                families.entry(scope).or_default().push(op_id);
            }
        }
        assert!(
            !required.is_empty(),
            "manifest must expose fleet operations"
        );

        fn principal(scopes: Vec<String>) -> FleetPrincipal {
            FleetPrincipal {
                organization_id: "org_1".into(),
                principal: "org:org_1".into(),
                expires_at: chrono::Utc::now().timestamp() + 300,
                scopes,
            }
        }

        // A principal with every data scope — still must be DENIED every
        // non-fleet (public/auth/dashboard) operation.
        let full_scopes: BTreeSet<&str> = [
            "events:read",
            "events:write",
            "state:read",
            "state:write",
            "state:delete",
            "channels:read",
            "channels:write",
            "channels:delete",
            "forks:create",
            "forks:promote",
            "groups:manage",
            "execution_groups:manage",
            "experiments:manage",
            "scheduler:manage",
            "executions:manage",
            "streams:read",
            "payloads:read",
        ]
        .into_iter()
        .collect();
        let full = principal(full_scopes.iter().map(|s| s.to_string()).collect());
        for op_id in ["actae.api.v1.auth.login", "actae.api.v1.auth.signup"] {
            assert!(
                authorize_op(&full, op_id).is_err(),
                "{op_id} is not fleet-invokable and must be denied"
            );
        }

        // Per-operation: exact-scope allowed; every OTHER family denied.
        for (op_id, scope) in &required {
            // Exact scope -> allowed.
            let exact = principal(vec![scope.clone()]);
            assert!(
                authorize_op(&exact, op_id).is_ok(),
                "{op_id} must be allowed with exactly scope {scope}"
            );
            // Empty scopes -> denied (fail closed).
            let none = principal(Vec::new());
            assert!(
                authorize_op(&none, op_id).is_err(),
                "{op_id} must be denied with no scopes"
            );
            // Any other scope family -> denied (no cross-family privilege).
            for other_scope in families.keys() {
                if other_scope == scope {
                    continue;
                }
                let other = principal(vec![other_scope.clone()]);
                assert!(
                    authorize_op(&other, op_id).is_err(),
                    "{op_id} (scope {scope}) must be denied for a token holding only {other_scope}"
                );
            }
        }

        // Exact direct checks: a scope never grants a sibling resource family.
        let read_only = principal(vec!["events:read".into()]);
        assert!(!allow_mutate(&read_only));
        assert!(authorize_scope(&read_only, "events:read").is_ok());
        assert!(authorize_scope(&read_only, "state:read").is_err());
        let write_only = principal(vec!["events:write".into()]);
        assert!(allow_mutate(&write_only));
        let empty = principal(Vec::new());
        assert!(authorize_scope(&empty, "events:read").is_err());
        assert!(!allow_mutate(&empty));
    }

    #[test]
    fn unknown_key_rejected() {
        let (signing, vk) = keypair();
        let mut cfg = cfg(vk);
        cfg.revoked_key_ids.push("fk1".into());
        let token = mint(&signing, fleet_claims());
        assert!(verify_fleet_access(&cfg, &format!("Bearer {token}"), "org_1").is_err());
    }

    #[test]
    fn federation_error_maps() {
        // The underlying verifier's errors surface as useful messages.
        let (signing, _vk) = keypair();
        let other: VerificationKey = {
            let s = SigningKey::from_bytes(&[0x24u8; 32]);
            VerificationKey {
                kid: "fk1".into(),
                key: ed25519_dalek::VerifyingKey::from(&s),
            }
        };
        let cfg = cfg(other);
        let token = mint(&signing, fleet_claims());
        let err = verify_fleet_access(&cfg, &format!("Bearer {token}"), "org_1").unwrap_err();
        assert!(err.to_string().contains("signature") || err.to_string().contains("invalid"));
    }
}
