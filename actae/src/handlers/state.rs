// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use actae::clean_json;
use actae::database::json_compat::{parse_json_body, CleanJson};
use axum::{
    extract::{Path, Query},
    http::{HeaderMap, StatusCode},
    Json,
};
use sonic_rs::json;

use crate::handlers::{err_to_client_message, err_to_status};
use crate::{check_rate_limit, resolve_auth_context};

#[derive(serde::Deserialize)]
struct SaveStateRequest {
    cursor: i64,
    state: sonic_rs::Value,
    channel_id: String,
    #[serde(default)]
    expected_version: Option<i64>,
    #[serde(default)]
    expected_cursor: Option<i64>,
}

#[derive(serde::Deserialize)]
pub(crate) struct ListStatesQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

pub async fn save_state_handler(
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: SaveStateRequest = match parse_json_body(body.as_ref()) {
        Ok(r) => r,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error": "Invalid request body"})),
            )
        }
    };
    if request.channel_id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(clean_json!({"error": "channel_id is required"})),
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
        "actae.api.v1.state.save",
        body.as_ref(),
        &headers,
    ) {
        return resp;
    }

    match state
        .event_handler
        .save_state(
            &request.channel_id,
            request.cursor,
            &request.state,
            request.expected_version,
            request.expected_cursor,
            &auth,
        )
        .await
    {
        Ok(version) => (
            StatusCode::OK,
            Json(clean_json!({"status": "saved", "version": version})),
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

pub async fn list_states_handler(
    headers: HeaderMap,
    Path(channel_id): Path<String>,
    Query(params): Query<ListStatesQuery>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }

    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    if let Err(resp) = crate::handlers::federation_op_guard_body(
        &auth,
        "actae.api.v1.state.versions.list",
        b"",
        &headers,
    ) {
        return resp;
    }

    match state
        .event_handler
        .list_states(&channel_id, params.limit, params.offset, &auth)
        .await
    {
        Ok(versions) => {
            let version_list: Vec<sonic_rs::Value> = versions
                .into_iter()
                .map(|v| {
                    json!({
                        "version": v.version,
                        "cursor": v.cursor,
                        "timestamp": v.timestamp.to_rfc3339(),
                    })
                })
                .collect();
            let count = version_list.len();
            (
                StatusCode::OK,
                Json(clean_json!({
                    "channel_id": channel_id,
                    "versions": version_list,
                    "count": count,
                })),
            )
        }
        Err(e) => {
            let (code, error_status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": error_status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

pub async fn load_state_version_handler(
    headers: HeaderMap,
    Path((channel_id, version)): Path<(String, i64)>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }

    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    if let Err(resp) = crate::handlers::federation_op_guard_body(
        &auth,
        "actae.api.v1.state.version.get",
        b"",
        &headers,
    ) {
        return resp;
    }

    match state
        .event_handler
        .load_state_version(&channel_id, version, &auth)
        .await
    {
        Ok(Some(state_value)) => (StatusCode::OK, Json(CleanJson(state_value))),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(clean_json!({"error": "version not found"})),
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

pub async fn delete_state_version_handler(
    headers: HeaderMap,
    Path((channel_id, version)): Path<(String, i64)>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }

    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    if let Err(resp) = crate::handlers::federation_op_guard_body(
        &auth,
        "actae.api.v1.state.version.delete",
        b"",
        &headers,
    ) {
        return resp;
    }

    match state
        .event_handler
        .delete_state_version(&channel_id, version, &auth)
        .await
    {
        Ok(_count) => (StatusCode::OK, Json(clean_json!({"status": "deleted"}))),
        Err(e) => {
            let (code, error_status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": error_status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

pub async fn load_state_handler(
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
        Err(resp) => return resp,
    };
    if let Err(resp) =
        crate::handlers::federation_op_guard_body(&auth, "actae.api.v1.state.load", b"", &headers)
    {
        return resp;
    }

    match state.event_handler.load_state(&channel_id, &auth).await {
        Ok(Some(state_value)) => (StatusCode::OK, Json(CleanJson(state_value))),
        Ok(None) => (
            StatusCode::OK,
            Json(clean_json!({"cursor": null, "state": null})),
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
