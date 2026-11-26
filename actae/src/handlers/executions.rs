// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use actae::clean_json;
use actae::database::json_compat::{parse_json_body, CleanJson};
use actae::events::schema::{
    CancelExecutionRequest, ClaimExecutionRequest, CompleteExecutionRequest, ExecutionInfo,
    FailExecutionRequest, HeartbeatExecutionRequest,
};
use axum::{
    extract::{Path, Query},
    http::{HeaderMap, StatusCode},
    Json,
};

use crate::handlers::{err_to_client_message, err_to_status};
use crate::{check_rate_limit, resolve_auth_context};

/// Serialize an `ExecutionInfo` to the canonical JSON shape.
fn execution_json(e: &ExecutionInfo) -> sonic_rs::Value {
    clean_json!({
        "id": e.id,
        "channel_id": e.channel_id,
        "key_name": e.key_name,
        "tool_name": e.tool_name,
        "status": e.status,
        "attempts": e.attempts,
        "lease_until": e.lease_until.map(|t| t.to_rfc3339()),
        "started_cursor": e.started_cursor,
        "completed_cursor": e.completed_cursor,
        "started_event_id": e.started_event_id.map(|u| u.to_string()),
        "completed_event_id": e.completed_event_id.map(|u| u.to_string()),
        "replay_emitted": e.replay_emitted,
        "params": e.params,
        "result": e.result,
        "error": e.error,
        "created_at": e.created_at.to_rfc3339(),
        "updated_at": e.updated_at.to_rfc3339(),
        "completed_at": e.completed_at.map(|t| t.to_rfc3339()),
    })
    .0
}

fn parse_execution_id(id: &str) -> Result<uuid::Uuid, (StatusCode, Json<CleanJson>)> {
    uuid::Uuid::parse_str(id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(clean_json!({"error": "Invalid execution id"})),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use sonic_rs::{json, JsonValueTrait};
    use uuid::Uuid;

    fn ts() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn execution() -> ExecutionInfo {
        ExecutionInfo {
            id: Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap(),
            channel_id: "ch-1".into(),
            key_name: "web.search:query-42".into(),
            tool_name: "web.search".into(),
            status: "running".into(),
            attempts: 2,
            lease_until: Some(ts()),
            started_cursor: Some(3),
            completed_cursor: None,
            started_event_id: Some(
                Uuid::parse_str("22222222-2222-3333-4444-555555555555").unwrap(),
            ),
            completed_event_id: None,
            replay_emitted: false,
            params: Some(sonic_rs::json!({"query": "actae"})),
            result: None,
            error: None,
            created_at: ts(),
            updated_at: ts(),
            completed_at: None,
            expired_at: None,
        }
    }

    #[test]
    fn execution_json_serializes_running_shape() {
        let v = execution_json(&execution());
        assert_eq!(v["id"], json!("11111111-2222-3333-4444-555555555555"));
        assert_eq!(v["channel_id"], json!("ch-1"));
        assert_eq!(v["key_name"], json!("web.search:query-42"));
        assert_eq!(v["tool_name"], json!("web.search"));
        assert_eq!(v["status"], json!("running"));
        assert_eq!(v["attempts"], json!(2));
        assert_eq!(v["lease_until"], json!("2026-01-01T00:00:00+00:00"));
        assert_eq!(v["started_cursor"], json!(3));
        assert_eq!(
            v["started_event_id"],
            json!("22222222-2222-3333-4444-555555555555")
        );
        assert!(v["completed_cursor"].is_null());
        assert!(v["completed_event_id"].is_null());
        assert!(v["completed_at"].is_null());
        assert_eq!(v["params"]["query"], json!("actae"));
        assert!(v["result"].is_null());
        assert!(v["error"].is_null());
        assert_eq!(v["replay_emitted"], json!(false));
    }

    #[test]
    fn execution_json_serializes_completed_shape() {
        let mut e = execution();
        e.status = "completed".into();
        e.completed_cursor = Some(7);
        e.completed_event_id =
            Some(Uuid::parse_str("33333333-2222-3333-4444-555555555555").unwrap());
        e.replay_emitted = true;
        e.result = Some(sonic_rs::json!({"answer": 42}));
        e.completed_at = Some(ts());
        e.lease_until = None;
        let v = execution_json(&e);
        assert_eq!(v["status"], json!("completed"));
        assert_eq!(v["completed_cursor"], json!(7));
        assert_eq!(
            v["completed_event_id"],
            json!("33333333-2222-3333-4444-555555555555")
        );
        assert_eq!(v["result"]["answer"], json!(42));
        assert_eq!(v["replay_emitted"], json!(true));
        assert_eq!(v["completed_at"], json!("2026-01-01T00:00:00+00:00"));
        assert!(v["lease_until"].is_null());
    }

    #[test]
    fn parse_execution_id_accepts_valid_uuid() {
        let id = Uuid::new_v4();
        assert_eq!(parse_execution_id(&id.to_string()).ok().unwrap(), id);
    }

    #[test]
    fn parse_execution_id_rejects_garbage() {
        let err = parse_execution_id("not-a-uuid").unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert_eq!(err.1 .0 .0["error"], "Invalid execution id");
    }
}

/// POST /api/v1/executions/claim
pub async fn claim_execution_handler(
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: ClaimExecutionRequest = match parse_json_body(body.as_ref()) {
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
        "actae.api.v1.executions.claim",
        body.as_ref(),
        &headers,
    ) {
        return resp;
    }

    match state.event_handler.claim_execution(&request, &auth).await {
        Ok(claim) => (
            StatusCode::CREATED,
            Json(clean_json!({
                "status": claim.status,
                "execution": execution_json(&claim.execution),
                "result": claim.result,
                "claim_token": claim.claim_token.map(|t| t.to_string()),
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

/// POST /api/v1/executions/{id}/complete
pub async fn complete_execution_handler(
    headers: HeaderMap,
    Path(id): Path<String>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let execution_id = match parse_execution_id(&id) {
        Ok(u) => u,
        Err(e) => return e,
    };
    let request: CompleteExecutionRequest = match parse_json_body(body.as_ref()) {
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
        "actae.api.v1.executions.complete",
        b"",
        &headers,
    ) {
        return resp;
    }

    match state
        .event_handler
        .complete_execution(execution_id, &request, &auth)
        .await
    {
        Ok(info) => (
            StatusCode::OK,
            Json(clean_json!({
                "status": "completed",
                "execution": execution_json(&info),
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

/// POST /api/v1/executions/{id}/fail
pub async fn fail_execution_handler(
    headers: HeaderMap,
    Path(id): Path<String>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let execution_id = match parse_execution_id(&id) {
        Ok(u) => u,
        Err(e) => return e,
    };
    let request: FailExecutionRequest = match parse_json_body(body.as_ref()) {
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
        "actae.api.v1.executions.fail",
        b"",
        &headers,
    ) {
        return resp;
    }

    match state
        .event_handler
        .fail_execution(execution_id, &request, &auth)
        .await
    {
        Ok(info) => (
            StatusCode::OK,
            Json(clean_json!({
                "status": "failed",
                "execution": execution_json(&info),
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

/// POST /api/v1/executions/{id}/heartbeat
pub async fn heartbeat_execution_handler(
    headers: HeaderMap,
    Path(id): Path<String>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let execution_id = match parse_execution_id(&id) {
        Ok(u) => u,
        Err(e) => return e,
    };
    let request: HeartbeatExecutionRequest = match parse_json_body(body.as_ref()) {
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
        "actae.api.v1.executions.heartbeat",
        b"",
        &headers,
    ) {
        return resp;
    }

    match state
        .event_handler
        .heartbeat_execution(execution_id, &request, &auth)
        .await
    {
        Ok(info) => (
            StatusCode::OK,
            Json(clean_json!({
                "status": "ok",
                "execution": execution_json(&info),
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

/// POST /api/v1/executions/{id}/cancel
pub async fn cancel_execution_handler(
    headers: HeaderMap,
    Path(id): Path<String>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let execution_id = match parse_execution_id(&id) {
        Ok(u) => u,
        Err(e) => return e,
    };
    let request: CancelExecutionRequest = match parse_json_body(body.as_ref()) {
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
        "actae.api.v1.executions.cancel",
        b"",
        &headers,
    ) {
        return resp;
    }

    match state
        .event_handler
        .cancel_execution(execution_id, &request, &auth)
        .await
    {
        Ok(info) => (
            StatusCode::OK,
            Json(clean_json!({
                "status": "cancelled",
                "execution": execution_json(&info),
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

/// GET /api/v1/executions/{id}
pub async fn get_execution_handler(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let execution_id = match parse_execution_id(&id) {
        Ok(u) => u,
        Err(e) => return e,
    };
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(e) => return e,
    };

    match state.event_handler.get_execution(execution_id, &auth).await {
        Ok(Some(info)) => (
            StatusCode::OK,
            Json(clean_json!({
                "status": "ok",
                "execution": execution_json(&info),
            })),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(clean_json!({"status": "execution_not_found", "error": "execution_not_found"})),
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

#[derive(serde::Deserialize)]
pub(crate) struct ListExecutionsQuery {
    channel_id: String,
    limit: Option<i64>,
}

/// GET /api/v1/executions?channel_id=&limit=
pub async fn list_executions_handler(
    headers: HeaderMap,
    Query(params): Query<ListExecutionsQuery>,
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
        .list_executions(&params.channel_id, params.limit.unwrap_or(50), &auth)
        .await
    {
        Ok(executions) => {
            let list: Vec<sonic_rs::Value> = executions.iter().map(execution_json).collect();
            (
                StatusCode::OK,
                Json(clean_json!({
                    "status": "ok",
                    "executions": list,
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

/// DELETE /api/v1/executions/{id}
pub async fn delete_execution_handler(
    headers: HeaderMap,
    Path(id): Path<String>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let execution_id = match parse_execution_id(&id) {
        Ok(u) => u,
        Err(e) => return e,
    };
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(a) => a,
        Err(e) => return e,
    };
    if let Err(resp) = crate::handlers::federation_op_guard_body(
        &auth,
        "actae.api.v1.executions.delete",
        b"",
        &headers,
    ) {
        return resp;
    }

    match state
        .event_handler
        .delete_execution(execution_id, &auth)
        .await
    {
        Ok(true) => (StatusCode::OK, Json(clean_json!({"status": "deleted"}))),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(clean_json!({"status": "execution_not_found", "error": "execution_not_found"})),
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
