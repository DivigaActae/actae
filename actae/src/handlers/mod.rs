// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use axum::http::StatusCode;

pub mod channels;
pub mod events;
pub mod execution_groups;
pub mod executions;
pub mod experiments;
pub mod groups;
pub mod state;
pub mod wakeups;

/// Convert EventError to HTTP status code and status string
pub fn err_to_status(e: &actae::events::handler::EventError) -> (StatusCode, &'static str) {
    use actae::events::handler::EventError;
    match e {
        EventError::Unauthenticated(_) => (StatusCode::UNAUTHORIZED, "unauthorized"),
        EventError::Forbidden(_) => (StatusCode::FORBIDDEN, "forbidden"),
        EventError::InvalidRequest(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
        EventError::NotFound(_) => (StatusCode::NOT_FOUND, "not_found"),
        EventError::SnapshotBoundaryNotFound { .. } => {
            (StatusCode::CONFLICT, "snapshot_boundary_required")
        }
        EventError::VersionConflict { .. } => (StatusCode::CONFLICT, "version_conflict"),
        EventError::GroupNotFound(_) => (StatusCode::NOT_FOUND, "group_not_found"),
        EventError::ConsumerNotFound(_) => (StatusCode::CONFLICT, "consumer_not_found"),
        EventError::ExecutionGroupNotFound(_) => {
            (StatusCode::NOT_FOUND, "execution_group_not_found")
        }
        EventError::ExecutionGroupMemberNotFound { .. } => {
            (StatusCode::NOT_FOUND, "execution_group_member_not_found")
        }
        EventError::ExecutionGroupMemberClaimed { .. } => (StatusCode::CONFLICT, "member_claimed"),
        EventError::ExecutionGroupMemberFenced { .. } => (StatusCode::CONFLICT, "member_fenced"),
        EventError::CounterfactualBlocked { .. } => {
            (StatusCode::CONFLICT, "counterfactual_blocked")
        }
        EventError::ForkToolBlocked { .. } => (StatusCode::CONFLICT, "fork_tool_blocked"),
        EventError::WakeupNotFound(_) => (StatusCode::NOT_FOUND, "wakeup_not_found"),
        EventError::ExecutionKeyMismatch { .. } => {
            (StatusCode::CONFLICT, "idempotency_key_mismatch")
        }
        EventError::ExecutionNotFound(_) => (StatusCode::NOT_FOUND, "execution_not_found"),
        EventError::ExecutionNotOwned(_) => (StatusCode::CONFLICT, "execution_not_owned"),
        EventError::IdempotencyConflict { .. } => (StatusCode::CONFLICT, "idempotency_conflict"),
        EventError::ChannelConflict { .. } => (StatusCode::CONFLICT, "channel_conflict"),
        EventError::ChannelDeleted(_) => (StatusCode::GONE, "gone"),
        EventError::ChannelHasChildren { .. } => (StatusCode::CONFLICT, "channel_has_children"),
        EventError::QuotaExceeded => (StatusCode::TOO_MANY_REQUESTS, "quota_exceeded"),
        EventError::ExecutionResultExpired { .. } => (StatusCode::CONFLICT, "result_expired"),
        EventError::PageTokenInvalid(_) => (StatusCode::BAD_REQUEST, "invalid_page_token"),
        EventError::PageTokenExpired => (StatusCode::GONE, "page_token_expired"),
        EventError::PageTokenUnsatisfiable { .. } => (StatusCode::GONE, "page_token_unsatisfiable"),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "error"),
    }
}

/// Client-safe message for an `EventError` (audit item 15): structured,
/// user-facing errors keep their message; store/backend/internal errors —
/// which can embed database or configuration detail — are masked to a
/// stable string. The status code (from [`err_to_status`]) carries the
/// error category.
pub fn err_to_client_message(e: &actae::events::handler::EventError) -> String {
    use actae::events::handler::EventError;
    match e {
        EventError::Store(_) | EventError::Internal(_) => {
            // Operators need the real cause; clients get a masked message.
            tracing::error!(error = ?e, "request failed with an internal error");
            "Internal server error".to_string()
        }
        other => other.to_string(),
    }
}

/// Operation-level gate for federated command tokens (Phase 4): a command
/// token authorizes exactly ONE manifest operation. Every mutating (and
/// per-operation) handler calls this with its canonical operation id so a
/// write/read token minted for one operation can never reach another — even
/// when the engine authorizes both via the same action. Non-command
/// principals pass through (their read/write/delete checks still apply).
pub(crate) fn federation_op_guard(
    auth: &actae::auth::types::AuthContext,
    operation_id: &str,
) -> Result<
    (),
    (
        axum::http::StatusCode,
        axum::Json<actae::database::json_compat::CleanJson>,
    ),
> {
    if auth.is_federated() && !auth.federation_allows_operation(operation_id) {
        return Err((
            axum::http::StatusCode::FORBIDDEN,
            axum::Json(actae::clean_json!({
                "status": "forbidden",
                "error": "Operation not authorized by this federation command token"
            })),
        ));
    }
    Ok(())
}

/// Request-digest guard for federation command tokens (PR-014). When the
/// command token carries a `request_digest`, the canonical server verifies it
/// against the ACTUAL request body + forwarded idempotency and confirmation
/// headers before handler execution. A mismatch means the token was minted for
/// different content — reject with a stable 403.
///
/// Handlers that receive a mutable federation command call this instead of
/// [`federation_op_guard`]; read/observation and local principals are
/// unaffected. `body` is the received request body; the headers supply
/// `Idempotency-Key` and `X-Actae-Confirm-Target` (forwarded by the gateway).
pub(crate) fn federation_op_guard_body(
    auth: &actae::auth::types::AuthContext,
    operation_id: &str,
    body: &[u8],
    headers: &axum::http::HeaderMap,
) -> Result<
    (),
    (
        axum::http::StatusCode,
        axum::Json<actae::database::json_compat::CleanJson>,
    ),
> {
    federation_op_guard(auth, operation_id)?;
    if let Some(expected) = auth.federation_request_digest() {
        let idem = headers.get("idempotency-key").and_then(|v| v.to_str().ok());
        let confirm = headers
            .get("x-actae-confirm-target")
            .and_then(|v| v.to_str().ok());
        let ok = actae::auth::federation::verify_request_digest(expected, body, idem, confirm);
        if !ok {
            return Err((
                axum::http::StatusCode::FORBIDDEN,
                axum::Json(actae::clean_json!({
                    "status": "forbidden",
                    "error": "Request content does not match the command token binding"
                })),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use actae::events::handler::EventError;

    #[test]
    fn test_err_to_status_unauthenticated() {
        let err = EventError::Unauthenticated("bad token".into());
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(msg, "unauthorized");
    }

    #[test]
    fn test_err_to_status_forbidden() {
        let err = EventError::Forbidden("no access".into());
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(msg, "forbidden");
    }

    #[test]
    fn test_err_to_status_invalid_request() {
        let err = EventError::InvalidRequest("bad payload".into());
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(msg, "invalid_request");
    }

    #[test]
    fn test_err_to_status_not_found() {
        let err = EventError::NotFound("channel missing".into());
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(msg, "not_found");
    }

    #[test]
    fn test_err_to_status_internal_error() {
        let err = EventError::Internal("db connection failed".into());
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(msg, "error");
    }

    #[test]
    fn test_err_to_status_snapshot_boundary() {
        let err = EventError::SnapshotBoundaryNotFound {
            channel_id: "ch".into(),
            at_cursor: 42,
        };
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(msg, "snapshot_boundary_required");
    }

    #[test]
    fn test_err_to_status_version_conflict() {
        let err = EventError::VersionConflict {
            channel_id: "ch".into(),
            expected_version: Some(3),
            actual_version: Some(5),
            expected_cursor: None,
            actual_cursor: None,
        };
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(msg, "version_conflict");
    }

    #[test]
    fn test_err_to_status_group_errors() {
        let err = EventError::GroupNotFound("g1".into());
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(msg, "group_not_found");

        let err = EventError::ConsumerNotFound("c1".into());
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(msg, "consumer_not_found");

        let err = EventError::WakeupNotFound("w1".into());
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(msg, "wakeup_not_found");
    }

    #[test]
    fn test_err_to_status_execution_errors() {
        let err = EventError::ExecutionKeyMismatch {
            channel_id: "ch".into(),
            key_name: "k".into(),
        };
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(msg, "idempotency_key_mismatch");

        let err = EventError::ExecutionNotFound("e1".into());
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(msg, "execution_not_found");

        let err = EventError::ExecutionNotOwned("e1".into());
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(msg, "execution_not_owned");
    }

    #[test]
    fn test_err_to_status_execution_group_errors() {
        let err = EventError::ExecutionGroupNotFound("g1".into());
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(msg, "execution_group_not_found");

        let err = EventError::ExecutionGroupMemberNotFound {
            group_id: "g1".into(),
            member_id: "m1".into(),
        };
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(msg, "execution_group_member_not_found");

        let err = EventError::ExecutionGroupMemberClaimed {
            group_id: "g1".into(),
            member_id: "m1".into(),
        };
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(msg, "member_claimed");

        let err = EventError::ExecutionGroupMemberFenced {
            group_id: "g1".into(),
            member_id: "m1".into(),
        };
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(msg, "member_fenced");

        let err = EventError::CounterfactualBlocked {
            fork_group_id: "g1".into(),
            boundary: "member:m1:send".into(),
        };
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(msg, "counterfactual_blocked");
    }

    #[test]
    fn test_err_to_status_idempotency_channel_and_quota_errors() {
        let err = EventError::IdempotencyConflict {
            operation_id: "op-1".into(),
        };
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(msg, "idempotency_conflict");

        let err = EventError::ChannelConflict {
            channel_id: "ch".into(),
        };
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(msg, "channel_conflict");

        let err = EventError::ChannelDeleted("ch".into());
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::GONE);
        assert_eq!(msg, "gone");

        let err = EventError::ChannelHasChildren {
            channel_id: "ch".into(),
            count: 3,
        };
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(msg, "channel_has_children");

        let err = EventError::QuotaExceeded;
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(msg, "quota_exceeded");

        let err = EventError::ExecutionResultExpired {
            channel_id: "ch".into(),
            key_name: "k".into(),
            execution_id: uuid::Uuid::new_v4(),
        };
        let (status, msg) = err_to_status(&err);
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(msg, "result_expired");
    }
}

#[cfg(test)]
mod error_message_tests {
    use super::*;
    use actae::events::handler::EventError;

    #[test]
    fn user_facing_errors_keep_their_message() {
        let e = EventError::Forbidden("Read permission denied on channel: x".into());
        assert!(err_to_client_message(&e).contains("Read permission denied on channel: x"));
        let e = EventError::NotFound("channel not found".into());
        assert!(err_to_client_message(&e).contains("channel not found"));
        // The message must not contain any backend/db marker.
        assert!(!err_to_client_message(&e).contains("postgres"));
    }

    #[test]
    fn backend_errors_are_masked() {
        // Store and Internal variants can embed database/configuration
        // detail — they must never reach the client (audit item 15).
        // (Store is constructed in the same match arm as Internal.)
        let e = EventError::Internal("sqlx error: could not connect to postgres".into());
        assert_eq!(err_to_client_message(&e), "Internal server error");
    }
}

/// Instance-layer negative authorization matrix (PR-001 acceptance).
#[cfg(test)]
mod federation_op_guard_tests {
    use super::*;
    use actae::auth::authorization::ApiKeyPermissions;
    use actae::auth::types::AuthContext;

    fn command(operation_id: &str, action: &str, resources: Vec<String>) -> AuthContext {
        AuthContext::FederatedCommand {
            principal: "org:org_1".into(),
            instance_id: "ins_1".into(),
            operation_id: operation_id.to_string(),
            action: action.to_string(),
            request_digest: None,
            permissions: ApiKeyPermissions {
                read: Vec::new(),
                write: resources.clone(),
                delete: resources,
                admin: Vec::new(),
            },
        }
    }

    #[test]
    fn command_token_cannot_invoke_a_different_operation() {
        // A command token minted for events.record must be rejected for
        // state.save (no cross-operation privilege).
        let auth = command("actae.api.v1.events.record", "write", vec!["ch_1".into()]);
        assert!(federation_op_guard(&auth, "actae.api.v1.events.record").is_ok());
        let err = federation_op_guard(&auth, "actae.api.v1.state.save").unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }

    #[test]
    fn command_token_request_digest_mismatch_rejected() {
        // PR-014: a command token bound to one request body is rejected when
        // the actual body differs (mismatched digest -> 403 before execution).
        let mut auth = command("actae.api.v1.events.record", "write", vec!["ch_1".into()]);
        if let AuthContext::FederatedCommand { request_digest, .. } = &mut auth {
            *request_digest =
                Some("ced5c0da0646dd52de8537aa51f3cff96daee6cacd565f13d8c382fddfee5a3d".into());
            // sha256("expected-body" + NUL + NUL: body + idem/confirm sections)
        }
        let headers = axum::http::HeaderMap::new();
        let mismatch = federation_op_guard_body(
            &auth,
            "actae.api.v1.events.record",
            b"{\"actual\":\"different\"}",
            &headers,
        )
        .unwrap_err();
        assert_eq!(mismatch.0, StatusCode::FORBIDDEN);
        // Matching body + headers passes.
        let ok = federation_op_guard_body(
            &auth,
            "actae.api.v1.events.record",
            b"expected-body",
            &headers,
        );
        assert!(ok.is_ok());
    }

    #[test]
    fn observation_token_write_permissions_are_empty() {
        // An observation token has NO write/delete/admin grants, so the
        // data-plane permission check denies every mutation (validate_auth
        // Permission::Write on an empty write list).
        let auth = AuthContext::FederatedObservation {
            principal: "org:org_1".into(),
            instance_id: "ins_1".into(),
            permissions: ApiKeyPermissions {
                read: vec!["ch_1".into()],
                write: Vec::new(),
                delete: Vec::new(),
                admin: Vec::new(),
            },
        };
        assert!(auth.is_federated());
        if let AuthContext::FederatedObservation { permissions, .. } = &auth {
            assert!(permissions.write.is_empty());
            assert!(permissions.delete.is_empty());
            assert!(permissions.admin.is_empty());
            assert!(permissions.read.contains(&"ch_1".to_string()));
        }
    }
}
