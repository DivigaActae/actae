// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use actae::clean_json;
use actae::database::json_compat::{clean_json_from, clean_sonic_value, parse_json_body};
use actae::events::schema::{AddExperimentMemberRequest, CreateExperimentRequest};
use axum::{
    extract::Path,
    http::{HeaderMap, StatusCode},
    Json,
};
use sonic_rs::json;

use crate::handlers::{err_to_client_message, err_to_status};
use crate::{check_rate_limit, resolve_auth_context};

/// POST /api/v1/experiments — create an experiment group.
pub async fn create_experiment_handler(
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<actae::database::json_compat::CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: CreateExperimentRequest = match parse_json_body(body.as_ref()) {
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
        "actae.api.v1.experiments.create",
        body.as_ref(),
        &headers,
    ) {
        return resp;
    }
    match state.event_handler.create_experiment(&request, &auth).await {
        Ok(group) => (StatusCode::CREATED, Json(clean_json_from(&group))),
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

/// GET /api/v1/experiments — list experiment groups.
pub async fn list_experiments_handler(
    headers: HeaderMap,
) -> (StatusCode, Json<actae::database::json_compat::CleanJson>) {
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
    match state.event_handler.list_experiments(&auth).await {
        Ok(groups) => (StatusCode::OK, Json(clean_json_from(&groups))),
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

/// GET /api/v1/experiments/{group_id} — get one experiment with members.
pub async fn get_experiment_handler(
    headers: HeaderMap,
    Path(group_id): Path<String>,
) -> (StatusCode, Json<actae::database::json_compat::CleanJson>) {
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
    match state.event_handler.get_experiment(&group_id, &auth).await {
        Ok(Some(group)) => (StatusCode::OK, Json(clean_json_from(&group))),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(clean_json!({"status": "not_found", "error": "Experiment not found"})),
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

/// POST /api/v1/experiments/{group_id}/members — add a fork.
pub async fn add_experiment_member_handler(
    headers: HeaderMap,
    Path(group_id): Path<String>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<actae::database::json_compat::CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: AddExperimentMemberRequest = match parse_json_body(body.as_ref()) {
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
        "actae.api.v1.experiments.members.add",
        b"",
        &headers,
    ) {
        return resp;
    }
    match state
        .event_handler
        .add_experiment_member(&group_id, &request, &auth)
        .await
    {
        Ok(group) => (StatusCode::OK, Json(clean_json_from(&group))),
        Err(e) => {
            let (code, status) = err_to_status(&e);
            (
                code,
                Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
            )
        }
    }
}

/// GET /api/v1/experiments/{group_id}/rank — rank members by score.
pub async fn rank_experiment_handler(
    headers: HeaderMap,
    Path(group_id): Path<String>,
) -> (StatusCode, Json<actae::database::json_compat::CleanJson>) {
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
    match state.event_handler.rank_experiment(&group_id, &auth).await {
        Ok((group, members)) => {
            let list: Vec<sonic_rs::Value> = members
                .iter()
                .map(|m| {
                    json!({
                        "channel_id": m.channel_id,
                        "role": m.role,
                        "declared_delta": m.declared_delta.as_ref().map(clean_sonic_value),
                        "outcome": m.outcome,
                        "result_score": m.result_score,
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(clean_json!({
                    "group": clean_json_from(&group),
                    "ranked": list,
                    "count": list.len(),
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
