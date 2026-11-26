// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use actae::clean_json;
use actae::database::json_compat::{parse_json_body, CleanJson};
use actae::events::schema::CreateWakeupRequest;
use axum::{
    extract::{Path, Query},
    http::{HeaderMap, StatusCode},
    Json,
};

use crate::handlers::{err_to_client_message, err_to_status};
use crate::{check_rate_limit, resolve_auth_context};

#[derive(serde::Deserialize)]
pub(crate) struct ListWakeupsQuery {
    channel_id: Option<String>,
    status: Option<String>,
}

pub async fn create_wakeup_handler(
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: CreateWakeupRequest = match parse_json_body(body.as_ref()) {
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
        "actae.api.v1.scheduler.wakeups.create",
        body.as_ref(),
        &headers,
    ) {
        return resp;
    }

    match state.event_handler.create_wakeup(&request, &auth).await {
        Ok(wakeup) => (
            StatusCode::CREATED,
            Json(clean_json!({
                "status": "scheduled",
                "id": wakeup.id,
                "channel_id": wakeup.channel_id,
                "run_at": wakeup.run_at.to_rfc3339(),
                "status": wakeup.status,
                "payload": wakeup.payload,
                "created_at": wakeup.created_at.to_rfc3339(),
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

pub async fn list_wakeups_handler(
    headers: HeaderMap,
    Query(params): Query<ListWakeupsQuery>,
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
        .list_wakeups(
            params.channel_id.as_deref(),
            params.status.as_deref(),
            &auth,
        )
        .await
    {
        Ok(wakeups) => {
            let list: Vec<sonic_rs::Value> = wakeups
                .into_iter()
                .map(|w| {
                    clean_json!({
                        "id": w.id,
                        "channel_id": w.channel_id,
                        "run_at": w.run_at.to_rfc3339(),
                        "status": w.status,
                        "payload": w.payload,
                        "created_at": w.created_at.to_rfc3339(),
                        "executed_at": w.executed_at.map(|t| t.to_rfc3339()),
                        "attempts": w.attempts,
                    })
                    .0
                })
                .collect();
            (
                StatusCode::OK,
                Json(clean_json!({
                    "status": "ok",
                    "wakeups": list,
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

pub async fn get_wakeup_handler(
    headers: HeaderMap,
    Path(id): Path<String>,
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
    let id = match uuid::Uuid::parse_str(&id) {
        Ok(u) => u,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error": "Invalid wakeup id"})),
            )
        }
    };

    match state.event_handler.get_wakeup(id, &auth).await {
        Ok(Some(wakeup)) => (
            StatusCode::OK,
            Json(clean_json!({
                "id": wakeup.id,
                "channel_id": wakeup.channel_id,
                "run_at": wakeup.run_at.to_rfc3339(),
                "status": wakeup.status,
                "payload": wakeup.payload,
                "created_at": wakeup.created_at.to_rfc3339(),
                "executed_at": wakeup.executed_at.map(|t| t.to_rfc3339()),
                "attempts": wakeup.attempts,
                "error": wakeup.error,
            })),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(clean_json!({"error": "wakeup_not_found"})),
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

pub async fn cancel_wakeup_handler(
    headers: HeaderMap,
    Path(id): Path<String>,
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
        "actae.api.v1.scheduler.wakeups.cancel",
        b"",
        &headers,
    ) {
        return resp;
    }
    let id = match uuid::Uuid::parse_str(&id) {
        Ok(u) => u,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error": "Invalid wakeup id"})),
            )
        }
    };

    match state.event_handler.cancel_wakeup(id, &auth).await {
        Ok(true) => (StatusCode::OK, Json(clean_json!({"status": "cancelled"}))),
        Ok(false) => (
            StatusCode::CONFLICT,
            Json(clean_json!({"error": "wakeup_not_pending"})),
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
