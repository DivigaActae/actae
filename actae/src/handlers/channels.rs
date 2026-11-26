// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use actae::clean_json;
use actae::database::json_compat::{
    clean_json_from, clean_sonic_value, parse_json_body, CleanJson,
};
use actae::events::schema::{
    ForkInfo, ForkRequest, SetOutcomeRequest, StateDiffRequest, UpdateMetadataRequest,
};
use axum::{
    extract::{Path, Query},
    http::{HeaderMap, StatusCode},
    Json,
};
use sonic_rs::json;

use crate::handlers::{err_to_client_message, err_to_status};
use crate::{check_rate_limit, resolve_auth_context};

pub async fn fork_channel_handler(
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }

    let request: ForkRequest = match parse_json_body(body.as_ref()) {
        Ok(r) => r,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error": "Invalid request body"})),
            )
        }
    };
    if request.at_cursor < 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(clean_json!({"error": "at_cursor must be >= 0"})),
        );
    }
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(e) => return e,
    };
    if let Err(resp) = crate::handlers::federation_op_guard_body(
        &auth,
        "actae.api.v1.forks.create",
        body.as_ref(),
        &headers,
    ) {
        return resp;
    }

    match state.event_handler.fork(&request, &auth).await {
        Ok((metadata, replayed)) => {
            // Local fork counter (always available, no phone-home). The
            // linked-control-plane usage meter (`fork_operations`) is written
            // transactionally inside `store.fork_channel`; this Prometheus
            // counter is the in-instance view and must not double-count
            // idempotent replays.
            if !replayed {
                actae::metrics::basic_metrics::FORKS_TOTAL.inc();
            }
            (
                StatusCode::CREATED,
                Json(clean_json!({
                    "status": "forked",
                    "fork_id": metadata.channel_id,
                    "child_channel_id": metadata.channel_id,
                    "channel_id": metadata.channel_id,
                    "source_channel_id": metadata.parent_channel_id,
                    "parent_channel_id": metadata.parent_channel_id,
                    "origin_run_id": metadata.origin_run_id,
                    "forked_at_cursor": metadata.forked_at_cursor,
                    "forked_at": metadata.forked_at.to_rfc3339(),
                    "display_name": metadata.display_name,
                    "reason": metadata.reason,
                    "requested_at_cursor": metadata.requested_at_cursor,
                    "requested_cursor": metadata.requested_at_cursor,
                    "resolved_cursor": metadata.resolved_state_cursor,
                    "resolved_state_cursor": metadata.resolved_state_cursor,
                    "resolved_event_id": metadata.resolved_event_id,
                    "source_state_version": metadata.source_state_version,
                    "source_state_sha256": metadata.source_state_sha256,
                    "restorable": metadata.restorable,
                    "reproducibility": metadata.reproducibility,
                    "replayed": replayed,
                    "manifest": metadata.manifest.as_ref().map(clean_sonic_value),
                    "tool_policies": metadata.tool_policies.as_ref().map(clean_sonic_value),
                })),
            )
        }
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

/// GET /api/v1/channels/{channel_id}/receipt
///
/// The immutable fork receipt for a channel: the provenance record returned
/// at fork creation (source identity, requested/resolved boundary, source
/// state version + fingerprint, restorable, reproducibility). `replayed` is
/// informational here (a receipt is immutable; the replay flag only matters
/// at creation time).
pub async fn fork_receipt_handler(
    headers: HeaderMap,
    Path(channel_id): Path<String>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(e) => return e,
    };

    match state
        .event_handler
        .get_channel_metadata(&channel_id, &auth)
        .await
    {
        Ok(Some(m)) => {
            let receipt = clean_json!({
                "fork_id": m.channel_id,
                "source_channel_id": m.parent_channel_id,
                "child_channel_id": m.channel_id,
                "requested_cursor": m.requested_at_cursor,
                "resolved_cursor": m.resolved_state_cursor,
                "resolved_event_id": m.resolved_event_id,
                "source_state_version": m.source_state_version,
                "source_state_sha256": m.source_state_sha256,
                "restorable": m.restorable,
                "replayed": false,
                "manifest": m.manifest.as_ref().map(clean_sonic_value),
                "reproducibility": m.reproducibility,
                "tool_policies": m.tool_policies.as_ref().map(clean_sonic_value),
            });
            (StatusCode::OK, Json(receipt))
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(clean_json!({"status": "not_found", "error": "Channel not found"})),
        ),
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

/// GET /api/v1/channels/{channel_id}/steps/{step_number}
///
/// Resolve a step number to the channel that OWNS it and its cursor via the
/// server-owned step index. Walks the fork lineage for inherited steps.
/// 404 when the step is not indexed (e.g. channels created before the step
/// index existed — SDKs fall back to their replay-based resolution).
pub async fn resolve_step_handler(
    headers: HeaderMap,
    Path((channel_id, step_number)): Path<(String, i64)>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(e) => return e,
    };

    match state
        .event_handler
        .resolve_step(&channel_id, step_number, &auth)
        .await
    {
        Ok(Some((owner, cursor, event_id))) => (
            StatusCode::OK,
            Json(clean_json!({
                "channel_id": owner,
                "step_number": step_number,
                "cursor": cursor,
                "event_id": event_id,
            })),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(clean_json!({"status": "not_found", "error": "Step not found"})),
        ),
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

/// GET /api/v1/channels/{channel_id}/steps/latest
///
/// The highest step number recorded on the channel (0 when none). SDK crash
/// recovery uses this O(1) answer instead of paging the event log.
pub async fn latest_step_handler(
    headers: HeaderMap,
    Path(channel_id): Path<String>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(e) => return e,
    };

    match state
        .event_handler
        .latest_step_number(&channel_id, &auth)
        .await
    {
        Ok(last_step) => (
            StatusCode::OK,
            Json(clean_json!({
                "channel_id": channel_id,
                "last_step_number": last_step,
            })),
        ),
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

pub async fn channel_metadata_handler(
    headers: HeaderMap,
    Path(channel_id): Path<String>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(e) => return e,
    };

    match state
        .event_handler
        .get_channel_metadata(&channel_id, &auth)
        .await
    {
        Ok(Some(m)) => {
            let exp_val = m.experiment_metadata.as_ref().map(clean_sonic_value);
            (
                StatusCode::OK,
                Json(clean_json!({
                    "channel_id": m.channel_id,
                    "parent_channel_id": m.parent_channel_id,
                    "origin_run_id": m.origin_run_id,
                    "forked_at_cursor": m.forked_at_cursor,
                    "forked_at": m.forked_at.to_rfc3339(),
                    "display_name": m.display_name,
                    "reason": m.reason,
                    "experiment_metadata": exp_val,
                    "requested_at_cursor": m.requested_at_cursor,
                    "resolved_cursor": m.resolved_state_cursor,
                    "resolved_event_id": m.resolved_event_id,
                    "source_state_version": m.source_state_version,
                    "source_state_sha256": m.source_state_sha256,
                    "restorable": m.restorable,
                    "manifest": m.manifest.as_ref().map(clean_sonic_value),
                    "reproducibility": m.reproducibility,
                    "tool_policies": m.tool_policies.as_ref().map(clean_sonic_value),
                    "outcome": m.outcome,
                    "result_score": m.result_score,
                })),
            )
        }
        Ok(None) => {
            // No channel_metadata row. Distinguish a genuinely-unknown
            // channel (404) from a real channel that simply has no fork
            // metadata (synthesize a default so non-forked channels still
            // expose a consistent shape).
            match state.event_handler.channel_exists(&channel_id, &auth).await {
                Ok(true) => (
                    StatusCode::OK,
                    Json(clean_json!({
                        "channel_id": channel_id,
                        "parent_channel_id": null,
                        "origin_run_id": channel_id,
                        "forked_at_cursor": 0,
                        "forked_at": null,
                        "display_name": null,
                        "reason": null,
                        "experiment_metadata": null,
                        "requested_at_cursor": 0,
                        "resolved_cursor": 0,
                        "resolved_event_id": null,
                        "source_state_version": 0,
                        "source_state_sha256": null,
                        "restorable": false,
                        "manifest": null,
                        "reproducibility": null,
                        "tool_policies": null,
                        "outcome": null,
                        "result_score": null,
                    })),
                ),
                Ok(false) => (
                    StatusCode::NOT_FOUND,
                    Json(clean_json!({"status": "not_found", "error": "Channel not found"})),
                ),
                Err(e) => {
                    let (code, status) = err_to_status(&e);
                    (
                        code,
                        Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
                    )
                }
            }
        }
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

pub async fn list_forks_handler(
    headers: HeaderMap,
    Path(channel_id): Path<String>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(e) => return e,
    };

    match state.event_handler.list_forks(&channel_id, &auth).await {
        Ok(forks) => {
            let list: Vec<sonic_rs::Value> = forks
                .iter()
                .map(|b| {
                    let exp_val = b.experiment_metadata.as_ref().map(clean_sonic_value);
                    json!({
                        "channel_id": b.channel_id,
                        "parent_channel_id": b.parent_channel_id,
                        "origin_run_id": b.origin_run_id,
                        "display_name": b.display_name,
                        "reason": b.reason,
                        "forked_at_cursor": b.forked_at_cursor,
                        "forked_at": b.forked_at.to_rfc3339(),
                        "experiment_metadata": exp_val,
                        "requested_at_cursor": b.requested_at_cursor,
                        "resolved_cursor": b.resolved_state_cursor,
                        "resolved_event_id": b.resolved_event_id,
                        "source_state_version": b.source_state_version,
                        "source_state_sha256": b.source_state_sha256,
                        "restorable": b.restorable,
                        "reproducibility": b.reproducibility,
                        "tool_policies": b.tool_policies.as_ref().map(clean_sonic_value),
                        "outcome": b.outcome,
                        "result_score": b.result_score,
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(clean_json!({"forks": list, "count": list.len()})),
            )
        }
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

pub async fn fork_tree_handler(
    headers: HeaderMap,
    Path(channel_id): Path<String>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(e) => return e,
    };

    match state.event_handler.get_fork_tree(&channel_id, &auth).await {
        Ok((tree, truncated)) => {
            fn fork_to_json(b: &ForkInfo) -> CleanJson {
                clean_json!({
                    "channel_id": b.metadata.channel_id,
                    "display_name": b.metadata.display_name,
                    "reason": b.metadata.reason,
                    "forked_at_cursor": b.metadata.forked_at_cursor,
                    "event_count": b.event_count,
                    "latest_cursor": b.latest_cursor,
                    "children": b.children.iter().map(fork_to_json).collect::<Vec<_>>(),
                })
            }
            (
                StatusCode::OK,
                Json(clean_json!({
                    "channel_id": tree.metadata.channel_id,
                    "display_name": tree.metadata.display_name,
                    "reason": tree.metadata.reason,
                    "forked_at_cursor": tree.metadata.forked_at_cursor,
                    "event_count": tree.event_count,
                    "latest_cursor": tree.latest_cursor,
                    "truncated": truncated,
                    "max_depth": 10,
                    "children": tree.children.iter().map(fork_to_json).collect::<Vec<_>>(),
                })),
            )
        }
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

pub async fn update_metadata_handler(
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: UpdateMetadataRequest = match parse_json_body(body.as_ref()) {
        Ok(r) => r,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error": "Invalid request body"})),
            )
        }
    };
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(e) => return e,
    };
    if let Err(resp) = crate::handlers::federation_op_guard_body(
        &auth,
        "actae.api.v1.channels.metadata.put",
        body.as_ref(),
        &headers,
    ) {
        return resp;
    }

    match state.event_handler.update_metadata(&request, &auth).await {
        Ok(metadata) => {
            let exp_val = metadata.experiment_metadata.as_ref().map(clean_sonic_value);
            (
                StatusCode::OK,
                Json(clean_json!({
                    "channel_id": metadata.channel_id,
                    "parent_channel_id": metadata.parent_channel_id,
                    "origin_run_id": metadata.origin_run_id,
                    "forked_at_cursor": metadata.forked_at_cursor,
                    "forked_at": metadata.forked_at.to_rfc3339(),
                    "display_name": metadata.display_name,
                    "reason": metadata.reason,
                    "experiment_metadata": exp_val,
                    "requested_at_cursor": metadata.requested_at_cursor,
                    "resolved_cursor": metadata.resolved_state_cursor,
                    "resolved_event_id": metadata.resolved_event_id,
                    "source_state_version": metadata.source_state_version,
                    "source_state_sha256": metadata.source_state_sha256,
                    "restorable": metadata.restorable,
                    "reproducibility": metadata.reproducibility,
                    "tool_policies": metadata.tool_policies.as_ref().map(clean_sonic_value),
                    "outcome": metadata.outcome,
                    "result_score": metadata.result_score,
                })),
            )
        }
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

/// GET /api/v1/channels/diff?left={channel}&right={channel}
///
/// Structural diff of two channels' latest saved states.
pub async fn diff_channels_handler(
    headers: HeaderMap,
    Query(request): Query<StateDiffRequest>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    if request.left.is_empty() || request.right.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(clean_json!({"error": "left and right query params are required"})),
        );
    }
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(e) => return e,
    };

    match state
        .event_handler
        .diff_channels(&request.left, &request.right, &auth)
        .await
    {
        Ok(diff) => (StatusCode::OK, Json(clean_json_from(&diff))),
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

/// GET /api/v1/channels/{channel_id}/trail
///
/// The full decision trail for a channel: lineage chain to the root run,
/// the fork boundary state, and the channel's tool-execution ledger.
pub async fn decision_trail_handler(
    headers: HeaderMap,
    Path(channel_id): Path<String>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(e) => return e,
    };

    match state
        .event_handler
        .get_decision_trail(&channel_id, &auth)
        .await
    {
        Ok(trail) => (StatusCode::OK, Json(clean_json_from(&trail))),
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

/// GET /api/v1/channels/compare?left={channel}&right={channel}
///
/// "What changed?" between two forks: state diff + manifest diff +
/// tool-call diff + output diff + metrics delta.
pub async fn compare_channels_handler(
    headers: HeaderMap,
    Query(request): Query<StateDiffRequest>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    if request.left.is_empty() || request.right.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(clean_json!({"error": "left and right query params are required"})),
        );
    }
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(e) => return e,
    };
    match state
        .event_handler
        .compare_channels(&request.left, &request.right, &auth)
        .await
    {
        Ok(comparison) => (StatusCode::OK, Json(clean_json_from(&comparison))),
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

/// PATCH /api/v1/channels/{channel_id}/outcome
///
/// Record a fork outcome (promoted | rejected | inconclusive | crashed)
/// and an optional numeric result score.
pub async fn set_outcome_handler(
    headers: HeaderMap,
    Path(channel_id): Path<String>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: SetOutcomeRequest = match parse_json_body(body.as_ref()) {
        Ok(r) => r,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error": "Invalid request body"})),
            )
        }
    };
    if !matches!(
        request.outcome.as_str(),
        "promoted" | "rejected" | "inconclusive" | "crashed"
    ) {
        return (
            StatusCode::BAD_REQUEST,
            Json(clean_json!({"error": "outcome must be promoted|rejected|inconclusive|crashed"})),
        );
    }
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(e) => return e,
    };
    if let Err(resp) = crate::handlers::federation_op_guard_body(
        &auth,
        "actae.api.v1.channels.outcome.set",
        b"",
        &headers,
    ) {
        return resp;
    }
    match state
        .event_handler
        .set_channel_outcome(&channel_id, &request, &auth)
        .await
    {
        Ok(metadata) => (
            StatusCode::OK,
            Json(clean_json!({
                "channel_id": metadata.channel_id,
                "outcome": metadata.outcome,
                "result_score": metadata.result_score,
            })),
        ),
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

/// POST /api/v1/channels/{channel_id}/promote
///
/// Promote a winning fork into its parent (append-only merge): the child's
/// latest state is copied onto the parent at its next cursor, a `run.promoted`
/// event is recorded on the parent, and the child is marked promoted.
pub async fn promote_channel_handler(
    headers: HeaderMap,
    Path(channel_id): Path<String>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(e) => return e,
    };
    if let Err(resp) = crate::handlers::federation_op_guard_body(
        &auth,
        "actae.api.v1.forks.promote",
        b"",
        &headers,
    ) {
        return resp;
    }
    match state
        .event_handler
        .promote_channel(&channel_id, &auth)
        .await
    {
        Ok((parent, cursor)) => (
            StatusCode::OK,
            Json(clean_json!({
                "status": "promoted",
                "child_channel_id": channel_id,
                "parent_channel_id": parent,
                "parent_cursor": cursor,
            })),
        ),
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

/// DELETE /api/v1/channels/{channel_id}?recursive=true|false
///
/// Soft-delete a channel (optionally the whole subtree) and purge its
/// events/states/cursors/executions/step rows. Non-recursive deletes of a
/// channel with children are rejected (409).
pub async fn delete_channel_handler(
    headers: HeaderMap,
    Path(channel_id): Path<String>,
    Query(request): Query<DeleteChannelQuery>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(e) => return e,
    };
    if let Err(resp) = crate::handlers::federation_op_guard_body(
        &auth,
        "actae.api.v1.channels.delete",
        b"",
        &headers,
    ) {
        return resp;
    }
    match state
        .event_handler
        .delete_channel(&channel_id, request.recursive.unwrap_or(false), &auth)
        .await
    {
        Ok(()) => (
            StatusCode::OK,
            Json(clean_json!({"status": "deleted", "channel_id": channel_id})),
        ),
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct DeleteChannelQuery {
    pub recursive: Option<bool>,
}

/// POST /api/v1/channels/{channel_id}/close — stamp `closed_at` on the
/// channel's registry row (audit item 14). Write-authorized; does not delete
/// data, it records the lifecycle transition.
pub async fn close_channel_handler(
    headers: HeaderMap,
    axum::extract::Path(channel_id): axum::extract::Path<String>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(e) => return e,
    };
    if let Err(resp) = crate::handlers::federation_op_guard_body(
        &auth,
        "actae.api.v1.channels.close",
        b"",
        &headers,
    ) {
        return resp;
    }
    match state.event_handler.close_channel(&channel_id, &auth).await {
        Ok(()) => (
            StatusCode::OK,
            Json(clean_json!({
                "status": "closed",
                "channel_id": channel_id,
                "closed_at": chrono::Utc::now().to_rfc3339(),
            })),
        ),
        Err(e) => {
            let (code, error_status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": error_status, "error": err_to_client_message(&e)})),
            )
        }
    }
}
