// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use actae::clean_json;
use actae::database::json_compat::{parse_json_body, CleanJson};
use actae::events::schema::{
    AckRequest, ClaimWorkRequest, CreateGroupRequest, HeartbeatRequest, JoinGroupRequest,
};
use axum::{
    extract::{Path, Query},
    http::{HeaderMap, StatusCode},
    Json,
};

use crate::handlers::{err_to_client_message, err_to_status};
use crate::{check_rate_limit, resolve_auth_context};

#[derive(serde::Deserialize)]
pub(crate) struct ListGroupsQuery {
    channel_id: Option<String>,
}

pub async fn create_group_handler(
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: CreateGroupRequest = match parse_json_body(body.as_ref()) {
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
        "actae.api.v1.groups.create",
        body.as_ref(),
        &headers,
    ) {
        return resp;
    }

    match state
        .event_handler
        .create_group(
            &request.group_id,
            &request.channel_id,
            request.metadata.as_ref(),
            &auth,
        )
        .await
    {
        Ok(group) => (
            StatusCode::CREATED,
            Json(clean_json!({
                "status": "created",
                "group_id": group.group_id,
                "channel_id": group.channel_id,
                "created_at": group.created_at.to_rfc3339(),
                "metadata": group.metadata,
            })),
        ),
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({ "status": status, "error": err_to_client_message(&e) })),
            )
        }
    }
}

pub async fn list_groups_handler(
    headers: HeaderMap,
    Query(params): Query<ListGroupsQuery>,
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
        .list_groups(params.channel_id.as_deref(), &auth)
        .await
    {
        Ok(groups) => {
            let list: Vec<sonic_rs::Value> = groups
                .into_iter()
                .map(|g| {
                    clean_json!({
                        "group_id": g.group_id,
                        "channel_id": g.channel_id,
                        "created_at": g.created_at.to_rfc3339(),
                        "metadata": g.metadata,
                    })
                    .0
                })
                .collect();
            (
                StatusCode::OK,
                Json(clean_json!({
                    "status": "ok",
                    "groups": list,
                    "count": list.len(),
                })),
            )
        }
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({ "status": status, "error": err_to_client_message(&e) })),
            )
        }
    }
}

pub async fn delete_group_handler(
    headers: HeaderMap,
    Path(group_id): Path<String>,
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
        "actae.api.v1.groups.delete",
        b"",
        &headers,
    ) {
        return resp;
    }

    match state.event_handler.delete_group(&group_id, &auth).await {
        Ok(true) => (
            StatusCode::OK,
            Json(clean_json!({"status": "deleted", "group_id": group_id})),
        ),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(clean_json!({"error": "group_not_found"})),
        ),
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({ "status": status, "error": err_to_client_message(&e) })),
            )
        }
    }
}

pub async fn join_group_handler(
    headers: HeaderMap,
    Path(group_id): Path<String>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: JoinGroupRequest = match parse_json_body(body.as_ref()) {
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
    if let Err(resp) =
        crate::handlers::federation_op_guard_body(&auth, "actae.api.v1.groups.join", b"", &headers)
    {
        return resp;
    }

    match state
        .event_handler
        .join_group(
            &group_id,
            &request.consumer_id,
            request.lease_seconds.unwrap_or(60),
            &auth,
        )
        .await
    {
        Ok(offset) => (
            StatusCode::OK,
            Json(clean_json!({
                "status": "joined",
                "group_id": offset.group_id,
                "consumer_id": offset.consumer_id,
                "last_cursor": offset.last_cursor,
                "claimed_cursor": offset.claimed_cursor,
            })),
        ),
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({ "status": status, "error": err_to_client_message(&e) })),
            )
        }
    }
}

pub async fn claim_work_handler(
    headers: HeaderMap,
    Path(group_id): Path<String>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: ClaimWorkRequest = match parse_json_body(body.as_ref()) {
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
        "actae.api.v1.groups.claim_work",
        b"",
        &headers,
    ) {
        return resp;
    }

    match state
        .event_handler
        .claim_work(
            &group_id,
            &request.consumer_id,
            request.limit.unwrap_or(100),
            &auth,
        )
        .await
    {
        Ok(work) => {
            let events: Vec<sonic_rs::Value> = work
                .events
                .into_iter()
                .map(|e| {
                    clean_json!({
                        "id": e.id.to_string(),
                        "channel_id": e.channel_id,
                        "type": e.event_type,
                        "payload": e.payload,
                        "actor": e.metadata.actor,
                        "agent_id": e.metadata.agent_id,
                        "metadata": e.metadata.metadata,
                        "cursor": e.cursor,
                        "channel_cursor": e.cursor,
                        "timestamp": e.timestamp.to_rfc3339(),
                        "depends_on": e.depends_on.map(|d| d.to_string()),
                    })
                    .0
                })
                .collect();
            (
                StatusCode::OK,
                Json(clean_json!({
                    "status": "ok",
                    "group_id": work.group_id,
                    "consumer_id": work.consumer_id,
                    "lease_until": work.lease_until.to_rfc3339(),
                    "events": events,
                    "count": events.len(),
                })),
            )
        }
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({ "status": status, "error": err_to_client_message(&e) })),
            )
        }
    }
}

pub async fn ack_handler(
    headers: HeaderMap,
    Path(group_id): Path<String>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: AckRequest = match parse_json_body(body.as_ref()) {
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
    if let Err(resp) =
        crate::handlers::federation_op_guard_body(&auth, "actae.api.v1.groups.ack", b"", &headers)
    {
        return resp;
    }

    match state
        .event_handler
        .ack_work(&group_id, &request.consumer_id, request.cursor, &auth)
        .await
    {
        Ok(()) => (StatusCode::OK, Json(clean_json!({"status": "acked"}))),
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({ "status": status, "error": err_to_client_message(&e) })),
            )
        }
    }
}

pub async fn heartbeat_handler(
    headers: HeaderMap,
    Path(group_id): Path<String>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: HeartbeatRequest = match parse_json_body(body.as_ref()) {
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
        "actae.api.v1.groups.heartbeat",
        b"",
        &headers,
    ) {
        return resp;
    }

    match state
        .event_handler
        .heartbeat(
            &group_id,
            &request.consumer_id,
            request.lease_seconds.unwrap_or(60),
            &auth,
        )
        .await
    {
        Ok(()) => (StatusCode::OK, Json(clean_json!({"status": "ok"}))),
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({ "status": status, "error": err_to_client_message(&e) })),
            )
        }
    }
}

pub async fn offsets_handler(
    headers: HeaderMap,
    Path(group_id): Path<String>,
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

    match state.event_handler.group_offsets(&group_id, &auth).await {
        Ok(offsets) => {
            let list: Vec<sonic_rs::Value> = offsets
                .into_iter()
                .map(|o| {
                    clean_json!({
                        "group_id": o.group_id,
                        "consumer_id": o.consumer_id,
                        "last_cursor": o.last_cursor,
                        "claimed_cursor": o.claimed_cursor,
                        "updated_at": o.updated_at.to_rfc3339(),
                    })
                    .0
                })
                .collect();
            (
                StatusCode::OK,
                Json(clean_json!({
                    "status": "ok",
                    "group_id": group_id,
                    "offsets": list,
                    "count": list.len(),
                })),
            )
        }
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({ "status": status, "error": err_to_client_message(&e) })),
            )
        }
    }
}
