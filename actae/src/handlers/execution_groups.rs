// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! HTTP surface for durable, distributed execution groups.
//!
//! The protocol is deliberately channel-based: messages become normal events
//! in the recipient member's channel and can therefore be replayed before the
//! SDK moves to live WebSocket delivery.

use actae::clean_json;
use actae::database::json_compat::{parse_json_body, CleanJson};
use actae::events::schema::{
    AckGroupMessageRequest, AddExecutionGroupMemberRequest, ClaimMemberRequest,
    CreateExecutionGroupRequest, CreateGroupForkRequest, HeartbeatMemberRequest,
    ReleaseMemberRequest, SendGroupMessageRequest,
};
use axum::{
    extract::{Path, Query},
    http::{HeaderMap, StatusCode},
    Json,
};
use uuid::Uuid;

use crate::handlers::{err_to_client_message, err_to_status};
use crate::{check_rate_limit, resolve_auth_context};

#[derive(serde::Deserialize)]
pub(crate) struct MessageQuery {
    pub after: Option<Uuid>,
    pub limit: Option<i64>,
}

fn group_json(group: &actae::events::schema::ExecutionGroup) -> sonic_rs::Value {
    clean_json!({"group_id": group.group_id, "status": group.status, "metadata": group.metadata, "created_at": group.created_at.to_rfc3339(), "updated_at": group.updated_at.to_rfc3339()}).0
}
fn member_json(member: &actae::events::schema::ExecutionGroupMember) -> sonic_rs::Value {
    clean_json!({"group_id": member.group_id, "member_id": member.member_id, "channel_id": member.channel_id, "role": member.role, "metadata": member.metadata, "owner_id": member.owner_id, "generation": member.generation, "lease_until": member.lease_until.map(|v| v.to_rfc3339()), "created_at": member.created_at.to_rfc3339(), "updated_at": member.updated_at.to_rfc3339()}).0
}
fn lease_json(lease: &actae::events::schema::MemberLease) -> sonic_rs::Value {
    clean_json!({"group_id": lease.group_id, "member_id": lease.member_id, "owner_id": lease.owner_id, "generation": lease.generation, "lease_until": lease.lease_until.to_rfc3339()}).0
}
fn message_json(message: &actae::events::schema::GroupMessage) -> sonic_rs::Value {
    clean_json!({"message_id": message.message_id.to_string(), "group_id": message.group_id, "from_member_id": message.from_member_id, "to_member_id": message.to_member_id, "type": message.message_type, "payload": message.payload, "causal_context": message.causal_context, "source_event_id": message.source_event_id.to_string(), "delivery_event_id": message.delivery_event_id.to_string(), "status": message.status, "acknowledged_at": message.acknowledged_at.map(|v| v.to_rfc3339()), "acknowledged_by": message.acknowledged_by, "created_at": message.created_at.to_rfc3339()}).0
}
fn fork_json(fork: &actae::events::schema::GroupForkReceipt) -> sonic_rs::Value {
    clean_json!({"fork_group_id":fork.fork_group_id,"source_group_id":fork.source_group_id,"intervention_member_id":fork.intervention_member_id,"intervention_cursor":fork.intervention_cursor,"member_policies":fork.member_policies,"tool_policies":fork.tool_policies,"status":fork.status,"first_divergence_event_id":fork.first_divergence_event_id.map(|v|v.to_string()),"receipt":fork.receipt,"created_at":fork.created_at.to_rfc3339(),"updated_at":fork.updated_at.to_rfc3339()}).0
}

pub async fn create(headers: HeaderMap, body: axum::body::Bytes) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: CreateExecutionGroupRequest = match parse_json_body(body.as_ref()) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error":"Invalid request body"})),
            )
        }
    };
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(v) => v,
        Err(e) => return e,
    };
    if let Err(resp) = crate::handlers::federation_op_guard_body(
        &auth,
        "actae.api.v1.execution_groups.create",
        body.as_ref(),
        &headers,
    ) {
        return resp;
    }
    match state
        .event_handler
        .create_execution_group(&request.group_id, request.metadata.as_ref(), &auth)
        .await
    {
        Ok(group) => (
            StatusCode::CREATED,
            Json(clean_json!({"status":"created", "group": group_json(&group)})),
        ),
        Err(e) => error(e),
    }
}
pub async fn list(headers: HeaderMap) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(v) => v,
        Err(e) => return e,
    };
    match state.event_handler.list_execution_groups(&auth).await {
        Ok(groups) => {
            let values: Vec<_> = groups.iter().map(group_json).collect();
            (
                StatusCode::OK,
                Json(clean_json!({"status":"ok", "groups": values})),
            )
        }
        Err(e) => error(e),
    }
}
pub async fn add_member(
    headers: HeaderMap,
    Path(group_id): Path<String>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: AddExecutionGroupMemberRequest = match parse_json_body(body.as_ref()) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error":"Invalid request body"})),
            )
        }
    };
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(v) => v,
        Err(e) => return e,
    };
    if let Err(resp) = crate::handlers::federation_op_guard_body(
        &auth,
        "actae.api.v1.execution_groups.members.add",
        b"",
        &headers,
    ) {
        return resp;
    }
    match state
        .event_handler
        .add_execution_group_member(
            &group_id,
            &request.member_id,
            &request.channel_id,
            request.role.as_deref(),
            request.metadata.as_ref(),
            &auth,
        )
        .await
    {
        Ok(member) => (
            StatusCode::CREATED,
            Json(clean_json!({"status":"created", "member":member_json(&member)})),
        ),
        Err(e) => error(e),
    }
}
pub async fn members(
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
        Ok(v) => v,
        Err(e) => return e,
    };
    match state
        .event_handler
        .list_execution_group_members(&group_id, &auth)
        .await
    {
        Ok(members) => {
            let values: Vec<_> = members.iter().map(member_json).collect();
            (
                StatusCode::OK,
                Json(clean_json!({"status":"ok", "members":values})),
            )
        }
        Err(e) => error(e),
    }
}
pub async fn claim_member(
    headers: HeaderMap,
    Path((group_id, member_id)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: ClaimMemberRequest = match parse_json_body(body.as_ref()) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error":"Invalid request body"})),
            )
        }
    };
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(v) => v,
        Err(e) => return e,
    };
    if let Err(resp) = crate::handlers::federation_op_guard_body(
        &auth,
        "actae.api.v1.execution_groups.members.claim",
        b"",
        &headers,
    ) {
        return resp;
    }
    match state
        .event_handler
        .claim_execution_group_member(
            &group_id,
            &member_id,
            &request.owner_id,
            request.lease_seconds.unwrap_or(60),
            &auth,
        )
        .await
    {
        Ok(lease) => (
            StatusCode::OK,
            Json(clean_json!({"status":"claimed", "lease":lease_json(&lease)})),
        ),
        Err(e) => error(e),
    }
}
pub async fn heartbeat_member(
    headers: HeaderMap,
    Path((group_id, member_id)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: HeartbeatMemberRequest = match parse_json_body(body.as_ref()) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error":"Invalid request body"})),
            )
        }
    };
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(v) => v,
        Err(e) => return e,
    };
    if let Err(resp) = crate::handlers::federation_op_guard(
        &auth,
        "actae.api.v1.execution_groups.members.heartbeat",
    ) {
        return resp;
    }
    match state
        .event_handler
        .heartbeat_execution_group_member(
            &group_id,
            &member_id,
            &request.owner_id,
            request.generation,
            request.lease_seconds.unwrap_or(60),
            &auth,
        )
        .await
    {
        Ok(lease) => (
            StatusCode::OK,
            Json(clean_json!({"status":"ok", "lease":lease_json(&lease)})),
        ),
        Err(e) => error(e),
    }
}
pub async fn release_member(
    headers: HeaderMap,
    Path((group_id, member_id)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(response) = check_rate_limit(&state.security_manager, &headers) {
        return response;
    }
    let request: ReleaseMemberRequest = match parse_json_body(body.as_ref()) {
        Ok(value) => value,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error":"Invalid request body"})),
            )
        }
    };
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(value) => value,
        Err(error) => return error,
    };
    if let Err(resp) = crate::handlers::federation_op_guard_body(
        &auth,
        "actae.api.v1.execution_groups.members.release",
        b"",
        &headers,
    ) {
        return resp;
    }
    match state
        .event_handler
        .release_execution_group_member(
            &group_id,
            &member_id,
            &request.owner_id,
            request.generation,
            &auth,
        )
        .await
    {
        Ok(()) => (StatusCode::OK, Json(clean_json!({"status":"released"}))),
        Err(value) => error(value),
    }
}

pub async fn send(
    headers: HeaderMap,
    Path(group_id): Path<String>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: SendGroupMessageRequest = match parse_json_body(body.as_ref()) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error":"Invalid request body"})),
            )
        }
    };
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(v) => v,
        Err(e) => return e,
    };
    if let Err(resp) = crate::handlers::federation_op_guard_body(
        &auth,
        "actae.api.v1.execution_groups.messages.send",
        b"",
        &headers,
    ) {
        return resp;
    }
    match state
        .event_handler
        .send_execution_group_message(
            &group_id,
            &request.from_member_id,
            &request.to_member_id,
            &request.message_type,
            &request.payload,
            request.causal_context.as_ref(),
            request.operation_id,
            &auth,
        )
        .await
    {
        Ok(message) => (
            StatusCode::CREATED,
            Json(clean_json!({"status":"delivered", "message":message_json(&message)})),
        ),
        Err(e) => error(e),
    }
}
pub async fn messages(
    headers: HeaderMap,
    Path((group_id, member_id)): Path<(String, String)>,
    Query(query): Query<MessageQuery>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(v) => v,
        Err(e) => return e,
    };
    match state
        .event_handler
        .list_execution_group_messages(
            &group_id,
            &member_id,
            query.after,
            query.limit.unwrap_or(100),
            &auth,
        )
        .await
    {
        Ok(messages) => {
            let values: Vec<_> = messages.iter().map(message_json).collect();
            (
                StatusCode::OK,
                Json(clean_json!({"status":"ok", "messages":values})),
            )
        }
        Err(e) => error(e),
    }
}
pub async fn acknowledge(
    headers: HeaderMap,
    Path(message_id): Path<Uuid>,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: AckGroupMessageRequest = match parse_json_body(body.as_ref()) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error":"Invalid request body"})),
            )
        }
    };
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(v) => v,
        Err(e) => return e,
    };
    if let Err(resp) = crate::handlers::federation_op_guard_body(
        &auth,
        "actae.api.v1.execution_groups.messages.ack",
        b"",
        &headers,
    ) {
        return resp;
    }
    match state
        .event_handler
        .acknowledge_execution_group_message(
            message_id,
            &request.owner_id,
            request.generation,
            &auth,
        )
        .await
    {
        Ok(message) => (
            StatusCode::OK,
            Json(clean_json!({"status":"acknowledged", "message":message_json(&message)})),
        ),
        Err(e) => error(e),
    }
}
pub async fn create_fork(
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: CreateGroupForkRequest = match parse_json_body(body.as_ref()) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(clean_json!({"error":"Invalid request body"})),
            )
        }
    };
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(v) => v,
        Err(e) => return e,
    };
    if let Err(resp) = crate::handlers::federation_op_guard_body(
        &auth,
        "actae.api.v1.execution_groups.forks.create",
        body.as_ref(),
        &headers,
    ) {
        return resp;
    }
    match state
        .event_handler
        .create_group_fork(
            &request.fork_group_id,
            &request.source_group_id,
            &request.intervention_member_id,
            request.intervention_cursor,
            &request.member_policies,
            &request.tool_policies,
            &auth,
        )
        .await
    {
        Ok(fork) => (
            StatusCode::CREATED,
            Json(clean_json!({"status":"created","fork":fork_json(&fork)})),
        ),
        Err(e) => error(e),
    }
}
pub async fn get_fork(
    headers: HeaderMap,
    Path(fork_group_id): Path<String>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    };
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(v) => v,
        Err(e) => return e,
    };
    match state
        .event_handler
        .get_group_fork(&fork_group_id, &auth)
        .await
    {
        Ok(fork) => (
            StatusCode::OK,
            Json(clean_json!({"status":"ok","fork":fork_json(&fork)})),
        ),
        Err(e) => error(e),
    }
}
pub async fn promote_fork_member(
    headers: HeaderMap,
    Path((fork_group_id, member_id)): Path<(String, String)>,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let auth = match resolve_auth_context(&headers, &state.app_state, &state.security_manager, true)
        .await
    {
        Ok(value) => value,
        Err(error) => return error,
    };
    if let Err(resp) = crate::handlers::federation_op_guard_body(
        &auth,
        "actae.api.v1.execution_groups.forks.promote",
        b"",
        &headers,
    ) {
        return resp;
    }
    match state
        .event_handler
        .promote_group_fork_member(&fork_group_id, &member_id, &auth)
        .await
    {
        Ok(fork) => (
            StatusCode::OK,
            Json(clean_json!({"status":"promoted","fork":fork_json(&fork)})),
        ),
        Err(error_value) => error(error_value),
    }
}
fn error(e: actae::events::handler::EventError) -> (StatusCode, Json<CleanJson>) {
    let (code, status) = err_to_status(&e);
    (
        code,
        Json(clean_json!({"status":status,"error":err_to_client_message(&e)})),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use actae::events::schema::{
        ExecutionGroup, ExecutionGroupMember, GroupForkReceipt, GroupMessage, MemberLease,
    };
    use chrono::{DateTime, Utc};
    use sonic_rs::{json, JsonValueTrait};
    use uuid::Uuid;

    fn ts() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn uid() -> Uuid {
        Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap()
    }

    #[test]
    fn group_json_serializes_expected_fields() {
        let group = ExecutionGroup {
            group_id: "g1".into(),
            status: "active".into(),
            created_at: ts(),
            updated_at: ts(),
            metadata: Some(json!({"team": "support"})),
        };
        let v = group_json(&group);
        assert_eq!(v["group_id"], json!("g1"));
        assert_eq!(v["status"], json!("active"));
        assert_eq!(v["metadata"]["team"], json!("support"));
        assert_eq!(v["created_at"], json!("2026-01-01T00:00:00+00:00"));
        assert_eq!(v["updated_at"], json!("2026-01-01T00:00:00+00:00"));
    }

    #[test]
    fn member_json_serializes_expected_fields() {
        let member = ExecutionGroupMember {
            group_id: "g1".into(),
            member_id: "router".into(),
            channel_id: "ch-router".into(),
            role: Some("router".into()),
            metadata: Some(json!({"lang": "py"})),
            owner_id: Some("owner-a".into()),
            generation: 2,
            lease_until: Some(ts()),
            created_at: ts(),
            updated_at: ts(),
        };
        let v = member_json(&member);
        assert_eq!(v["group_id"], json!("g1"));
        assert_eq!(v["member_id"], json!("router"));
        assert_eq!(v["channel_id"], json!("ch-router"));
        assert_eq!(v["role"], json!("router"));
        assert_eq!(v["metadata"]["lang"], json!("py"));
        assert_eq!(v["owner_id"], json!("owner-a"));
        assert_eq!(v["generation"], json!(2));
        assert_eq!(v["lease_until"], json!("2026-01-01T00:00:00+00:00"));
    }

    #[test]
    fn member_json_unclaimed_owner_is_null() {
        let member = ExecutionGroupMember {
            group_id: "g1".into(),
            member_id: "research".into(),
            channel_id: "ch-research".into(),
            role: None,
            metadata: None,
            owner_id: None,
            generation: 0,
            lease_until: None,
            created_at: ts(),
            updated_at: ts(),
        };
        let v = member_json(&member);
        assert!(v["owner_id"].is_null());
        assert!(v["lease_until"].is_null());
        assert!(v["role"].is_null());
        assert_eq!(v["generation"], json!(0));
    }

    #[test]
    fn lease_json_serializes_expected_fields() {
        let lease = MemberLease {
            group_id: "g1".into(),
            member_id: "router".into(),
            owner_id: "owner-a".into(),
            generation: 3,
            lease_until: ts(),
        };
        let v = lease_json(&lease);
        assert_eq!(v["group_id"], json!("g1"));
        assert_eq!(v["member_id"], json!("router"));
        assert_eq!(v["owner_id"], json!("owner-a"));
        assert_eq!(v["generation"], json!(3));
        assert_eq!(v["lease_until"], json!("2026-01-01T00:00:00+00:00"));
    }

    #[test]
    fn message_json_serializes_expected_fields() {
        let msg = GroupMessage {
            message_id: uid(),
            group_id: "g1".into(),
            from_member_id: "research".into(),
            to_member_id: "router".into(),
            message_type: "research.result".into(),
            payload: json!({"answer": 42}),
            causal_context: Some(json!({"depth": 2})),
            source_event_id: uid(),
            delivery_event_id: uid(),
            status: "delivered".into(),
            acknowledged_at: None,
            acknowledged_by: None,
            created_at: ts(),
        };
        let v = message_json(&msg);
        assert_eq!(v["message_id"], json!(uid().to_string()));
        assert_eq!(v["from_member_id"], json!("research"));
        assert_eq!(v["to_member_id"], json!("router"));
        // The wire key for the message type is `type` (matches the request
        // body key, where the schema renames message_type → type).
        assert_eq!(v["type"], json!("research.result"));
        assert_eq!(v["payload"]["answer"], json!(42));
        assert_eq!(v["causal_context"]["depth"], json!(2));
        assert_eq!(v["source_event_id"], json!(uid().to_string()));
        assert_eq!(v["delivery_event_id"], json!(uid().to_string()));
        assert_eq!(v["status"], json!("delivered"));
        assert!(v["acknowledged_at"].is_null());
        assert!(v["acknowledged_by"].is_null());
    }

    #[test]
    fn message_json_acknowledged_fields_populated() {
        let msg = GroupMessage {
            message_id: uid(),
            group_id: "g1".into(),
            from_member_id: "research".into(),
            to_member_id: "router".into(),
            message_type: "research.result".into(),
            payload: json!({"answer": 42}),
            causal_context: None,
            source_event_id: uid(),
            delivery_event_id: uid(),
            status: "acknowledged".into(),
            acknowledged_at: Some(ts()),
            acknowledged_by: Some("owner-a".into()),
            created_at: ts(),
        };
        let v = message_json(&msg);
        assert_eq!(v["status"], json!("acknowledged"));
        assert_eq!(v["acknowledged_by"], json!("owner-a"));
        assert_eq!(v["acknowledged_at"], json!("2026-01-01T00:00:00+00:00"));
        assert!(v["causal_context"].is_null());
    }

    #[test]
    fn fork_json_serializes_expected_fields() {
        let fork = GroupForkReceipt {
            fork_group_id: "g1-alt".into(),
            source_group_id: "g1".into(),
            intervention_member_id: "router".into(),
            intervention_cursor: 18,
            member_policies: Some(json!({"router": "live", "research": "frozen"})),
            tool_policies: Some(json!({"web.search": "replay", "email.send": "block"})),
            status: "active".into(),
            first_divergence_event_id: Some(uid()),
            receipt: Some(json!({
                "replayed_events": ["a", "b"],
                "promoted_members": [],
                "live_events": [],
            })),
            created_at: ts(),
            updated_at: ts(),
        };
        let v = fork_json(&fork);
        assert_eq!(v["fork_group_id"], json!("g1-alt"));
        assert_eq!(v["source_group_id"], json!("g1"));
        assert_eq!(v["intervention_member_id"], json!("router"));
        assert_eq!(v["intervention_cursor"], json!(18));
        assert_eq!(v["member_policies"]["router"], json!("live"));
        assert_eq!(v["member_policies"]["research"], json!("frozen"));
        assert_eq!(v["tool_policies"]["web.search"], json!("replay"));
        assert_eq!(v["status"], json!("active"));
        assert_eq!(v["first_divergence_event_id"], json!(uid().to_string()));
        assert_eq!(v["receipt"]["replayed_events"], json!(["a", "b"]));
        assert_eq!(v["receipt"]["live_events"], json!([]));
    }

    #[test]
    fn fork_json_first_divergence_null_when_none() {
        let fork = GroupForkReceipt {
            fork_group_id: "g1-alt".into(),
            source_group_id: "g1".into(),
            intervention_member_id: "router".into(),
            intervention_cursor: 0,
            member_policies: Some(json!({"router": "live"})),
            tool_policies: Some(json!({})),
            status: "active".into(),
            first_divergence_event_id: None,
            receipt: None,
            created_at: ts(),
            updated_at: ts(),
        };
        let v = fork_json(&fork);
        assert!(v["first_divergence_event_id"].is_null());
        assert!(v["receipt"].is_null());
    }
}
