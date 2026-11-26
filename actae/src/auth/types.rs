// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Shared authentication context for all subsystems.
//!
//! This single definition is consumed by `EventHandler`, HTTP handlers, and
//! the WebSocket message handler. The `ApiKey` variant carries the parsed
//! resource-scoped `ApiKeyPermissions` (not a flat capability list), the key
//! source for control-plane enforcement, and the credential expiry so long
//! living sockets can be reauthorized (audit items 2, 3, 9, 11).

use crate::auth::authorization::ApiKeyPermissions;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq)]
pub enum AuthContext {
    Authenticated {
        user_id: String,
        permissions: ApiKeyPermissions,
    },
    ApiKey {
        key_id: String,
        key_source: String,
        expires_at: Option<DateTime<Utc>>,
        permissions: ApiKeyPermissions,
    },
    /// Federated read-only principal (`instance_observation` purpose): may read
    /// and stream channels matching its patterns, never mutate (Phase 4).
    FederatedObservation {
        principal: String,
        instance_id: String,
        permissions: ApiKeyPermissions,
    },
    /// Federated one-operation command principal (`instance_command` purpose):
    /// authorized for exactly one canonical operation, action, and resource
    /// grant. Cannot impersonate dashboard/JWT authority; cannot exceed the
    /// action/resource carried in the token.
    FederatedCommand {
        principal: String,
        instance_id: String,
        operation_id: String,
        action: String,
        permissions: ApiKeyPermissions,
        request_digest: Option<String>,
    },
    Anonymous,
}

impl AuthContext {
    /// Authorize `action` ("read"/"write"/"delete"/"admin") against a
    /// resource (channel id), applying the key's resource patterns.
    /// Anonymous principals are always denied.
    pub fn has_permission(&self, action: &str, resource: &str) -> bool {
        match self {
            Self::Authenticated { permissions, .. } | Self::ApiKey { permissions, .. } => {
                permissions.check(action, resource)
            }
            Self::FederatedObservation { permissions, .. } => {
                // Observation is strictly read-only: even a matching "write"
                // pattern on the token cannot authorize a mutation.
                action == "read" && permissions.check("read", resource)
            }
            Self::FederatedCommand {
                action: granted_action,
                permissions,
                ..
            } => {
                // The token authorizes exactly one action; anything else is
                // denied regardless of patterns (one-action token cannot
                // access another action/channel/instance).
                if action == granted_action {
                    return permissions.check(action, resource);
                }
                // Engine preconditions the single-action grant implicitly
                // covers on the SAME resource:
                //  * write → read   (fork's source-boundary read: the gateway
                //    mints a write command token for forks.create and the
                //    engine checks source read + child write);
                //  * delete → write (the engine's channel delete is
                //    write-permissioned: a delete command token minted for
                //    channels.delete must pass the write gate on its channel).
                // This never authorizes a different action on a different
                // channel.
                match granted_action.as_str() {
                    "write" if action == "read" => permissions.check("write", resource),
                    "delete" if action == "write" => permissions.check("delete", resource),
                    _ => false,
                }
            }
            Self::Anonymous => false,
        }
    }

    pub fn can_read(&self, resource: &str) -> bool {
        self.has_permission("read", resource)
    }

    pub fn can_write(&self, resource: &str) -> bool {
        self.has_permission("write", resource)
    }

    pub fn permissions(&self) -> Option<&ApiKeyPermissions> {
        match self {
            Self::Authenticated { permissions, .. }
            | Self::ApiKey { permissions, .. }
            | Self::FederatedObservation { permissions, .. }
            | Self::FederatedCommand { permissions, .. } => Some(permissions),
            Self::Anonymous => None,
        }
    }

    /// True for both federation principal kinds (observation + command).
    pub fn is_federated(&self) -> bool {
        matches!(
            self,
            Self::FederatedObservation { .. } | Self::FederatedCommand { .. }
        )
    }

    /// The canonical operation id a command token authorizes (None otherwise).
    pub fn federation_operation_id(&self) -> Option<&str> {
        match self {
            Self::FederatedCommand { operation_id, .. } => Some(operation_id),
            _ => None,
        }
    }

    /// The exact action a federation principal may perform.
    pub fn federation_action(&self) -> Option<&str> {
        match self {
            Self::FederatedCommand { action, .. } => Some(action),
            Self::FederatedObservation { .. } => Some("read"),
            _ => None,
        }
    }

    /// Whether a federated command token is allowed to perform the given
    /// canonical operation. Command tokens authorize exactly ONE manifest
    /// operation (Phase 4): a token minted for `events.record` must not reach
    /// `channels.delete` even though the engine authorizes deletes via the
    /// write action. Non-command principals are always allowed past this gate
    /// (their ordinary read/write/delete checks apply).
    pub fn federation_allows_operation(&self, operation_id: &str) -> bool {
        match self {
            Self::FederatedCommand {
                operation_id: granted,
                ..
            } => granted == operation_id,
            _ => true,
        }
    }

    /// The canonical request digest a command token is bound to (PR-014).
    /// `None` for non-command principals and for command tokens minted without
    /// a digest binding.
    pub fn federation_request_digest(&self) -> Option<&str> {
        match self {
            Self::FederatedCommand { request_digest, .. } => request_digest.as_deref(),
            _ => None,
        }
    }

    pub fn expires_at(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::ApiKey { expires_at, .. } => *expires_at,
            _ => None,
        }
    }

    pub fn key_id(&self) -> Option<&str> {
        match self {
            Self::ApiKey { key_id, .. } => Some(key_id),
            _ => None,
        }
    }

    pub fn key_source(&self) -> Option<&str> {
        match self {
            Self::ApiKey { key_source, .. } => Some(key_source),
            _ => None,
        }
    }

    pub fn user_id(&self) -> Option<&str> {
        match self {
            Self::Authenticated { user_id, .. } => Some(user_id),
            Self::ApiKey { key_id, .. } => Some(key_id),
            Self::FederatedObservation { principal, .. }
            | Self::FederatedCommand { principal, .. } => Some(principal),
            Self::Anonymous => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_rs::json;

    fn perms(v: &sonic_rs::Value) -> ApiKeyPermissions {
        crate::auth::parse_api_key_permissions(v)
    }

    #[test]
    fn test_anonymous_denies() {
        assert!(!AuthContext::Anonymous.can_read("public.x"));
        assert!(!AuthContext::Anonymous.can_write("public.x"));
        assert!(AuthContext::Anonymous.permissions().is_none());
    }

    #[test]
    fn test_authenticated_full_access() {
        let ctx = AuthContext::Authenticated {
            user_id: "u1".into(),
            permissions: ApiKeyPermissions::full(),
        };
        assert!(ctx.can_read("anything"));
        assert!(ctx.can_write("anything"));
    }

    #[test]
    fn test_api_key_resource_scoping() {
        let ctx = AuthContext::ApiKey {
            key_id: "k1".into(),
            key_source: "control_plane".into(),
            expires_at: None,
            permissions: perms(&json!({"read": ["project-a.*"]})),
        };
        assert!(ctx.can_read("project-a.ch"));
        assert!(!ctx.can_read("project-b.ch"));
        assert!(!ctx.can_write("project-a.ch"));
        assert_eq!(ctx.key_id(), Some("k1"));
        assert_eq!(ctx.key_source(), Some("control_plane"));
    }

    #[test]
    fn test_auth_context_debug() {
        let ctx = AuthContext::Authenticated {
            user_id: "u1".into(),
            permissions: ApiKeyPermissions::full(),
        };
        let debug = format!("{ctx:?}");
        assert!(debug.contains("Authenticated"));
        assert!(debug.contains("u1"));
    }

    #[test]
    fn test_api_key_accessors_and_identity() {
        let expires = Utc::now() + chrono::Duration::hours(1);
        let ctx = AuthContext::ApiKey {
            key_id: "k9".into(),
            key_source: "local".into(),
            expires_at: Some(expires),
            permissions: perms(&json!({"read": ["*"], "write": ["*"]})),
        };
        assert_eq!(ctx.key_id(), Some("k9"));
        assert_eq!(ctx.key_source(), Some("local"));
        assert_eq!(ctx.expires_at(), Some(expires));
        assert_eq!(ctx.user_id(), Some("k9"));
        assert!(ctx.can_read("anything"));
        assert!(ctx.can_write("anything"));
        assert_eq!(ctx.permissions().map(|p| p.read.len()), Some(1));
    }

    #[test]
    fn test_anonymous_identity_and_accessors() {
        let ctx = AuthContext::Anonymous;
        assert_eq!(ctx.user_id(), None);
        assert_eq!(ctx.key_id(), None);
        assert_eq!(ctx.key_source(), None);
        assert_eq!(ctx.expires_at(), None);
        assert_eq!(ctx.permissions(), None);
        assert!(!ctx.can_read("x"));
        assert!(!ctx.can_write("x"));
    }

    #[test]
    fn test_authenticated_user_id_and_permissions() {
        let ctx = AuthContext::Authenticated {
            user_id: "alice".into(),
            permissions: perms(&json!({"read": ["team.*"]})),
        };
        assert_eq!(ctx.user_id(), Some("alice"));
        assert_eq!(ctx.key_id(), None);
        assert!(ctx.can_read("team.ch"));
        assert!(!ctx.can_write("team.ch"));
    }

    #[test]
    fn test_federated_observation_is_read_only() {
        // A token may carry read patterns; it must never authorize a write
        // even if the resource matches — observation is strictly read-only.
        let ctx = AuthContext::FederatedObservation {
            principal: "org:1".into(),
            instance_id: "ins_x".into(),
            permissions: perms(&json!({"read": ["public.*"]})),
        };
        assert!(ctx.is_federated());
        assert_eq!(ctx.federation_action(), Some("read"));
        assert!(ctx.can_read("public.news"));
        assert!(!ctx.can_read("secret.x"));
        assert!(!ctx.can_write("public.news"));
        assert!(!ctx.has_permission("delete", "public.news"));
        assert_eq!(ctx.user_id(), Some("org:1"));
    }

    #[test]
    fn test_federated_command_is_single_action() {
        let ctx = AuthContext::FederatedCommand {
            principal: "org:1".into(),
            instance_id: "ins_x".into(),
            operation_id: "actae.api.v1.events.record".into(),
            action: "write".into(),
            permissions: perms(&json!({"write": ["public.*"]})),
            request_digest: Some("d1".into()),
        };
        assert!(ctx.is_federated());
        assert_eq!(
            ctx.federation_operation_id(),
            Some("actae.api.v1.events.record")
        );
        assert_eq!(ctx.federation_action(), Some("write"));
        // Write allowed on matching resource.
        assert!(ctx.can_write("public.news"));
        // A write grant implicitly covers the read precondition on the SAME
        // channel (fork source-boundary read), but never another channel.
        assert!(ctx.can_read("public.news"));
        assert!(!ctx.can_read("secret.x"));
        // One-action token cannot delete, even on a matching channel.
        assert!(!ctx.has_permission("delete", "public.news"));
        // A delete command token passes the engine's write gate on its
        // delete-granted channel (channel delete is write-permissioned).
        let del = AuthContext::FederatedCommand {
            principal: "org:1".into(),
            instance_id: "ins_x".into(),
            operation_id: "actae.api.v1.channels.delete".into(),
            action: "delete".into(),
            permissions: perms(&json!({"delete": ["public.*"]})),
            request_digest: None,
        };
        assert!(del.can_write("public.news"));
        assert!(del.has_permission("delete", "public.news"));
        assert!(!del.can_write("secret.x"));
        assert!(!del.can_read("public.news"));
        // Matching action but non-matching channel is denied.
        assert!(!ctx.can_write("secret.x"));
        // Cannot impersonate JWT authority: it is not Authenticated and has no
        // admin grants from the dashboard path.
        assert!(!ctx.has_permission("admin", "*"));
    }

    #[test]
    fn test_federation_accessors() {
        let obs = AuthContext::FederatedObservation {
            principal: "p".into(),
            instance_id: "i".into(),
            permissions: perms(&json!({"read": ["*"]})),
        };
        let cmd = AuthContext::FederatedCommand {
            principal: "p".into(),
            instance_id: "i".into(),
            operation_id: "op".into(),
            action: "read".into(),
            permissions: perms(&json!({"read": ["*"]})),
            request_digest: None,
        };
        assert!(obs.is_federated() && cmd.is_federated());
        assert_eq!(obs.federation_operation_id(), None);
        assert_eq!(cmd.federation_operation_id(), Some("op"));
        assert_eq!(obs.expires_at(), None);
        assert_eq!(obs.key_id(), None);
        assert_eq!(obs.key_source(), None);
        // A read-action command CAN read its resources (the one action).
        assert!(cmd.can_read("any"));
    }
}
