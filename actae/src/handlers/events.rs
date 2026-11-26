// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use actae::clean_json;
use actae::database::json_compat::{parse_json_body, CleanJson};
use actae::events::schema::{PublishEventRequest, QueryRequest, ReplayRequest, TransitionRequest};
use axum::{
    extract::{Path, Query},
    http::{HeaderMap, StatusCode},
    Json,
};
use sonic_rs::json;
use uuid::Uuid;

use crate::handlers::{err_to_client_message, err_to_status};
use crate::{check_rate_limit, resolve_auth_context};

/// Canonical event wire shape (kept identical to the pre-federation shape).
fn event_to_json(e: actae::events::schema::AgentEvent) -> sonic_rs::Value {
    json!({
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
}

/// Apply the effective federation source policy to a read result. Returns
/// `Ok(None)` when the request must be denied outright (remote access disabled
/// or channel denied); `Ok(Some(events))` carries the redacted/filtered list.
/// Non-federated principals are never touched.
fn apply_source_policy(
    app: &actae::state::AppState,
    auth: &actae::auth::types::AuthContext,
    events: Vec<actae::events::schema::AgentEvent>,
    channel: Option<&str>,
) -> Result<Option<Vec<actae::events::schema::AgentEvent>>, (axum::http::StatusCode, Json<CleanJson>)>
{
    if !auth.is_federated() {
        return Ok(Some(events));
    }
    let policy = app.control_plane.effective_source_policy();
    if !policy.enabled {
        return Err((
            axum::http::StatusCode::FORBIDDEN,
            Json(clean_json!({
                "status": "forbidden",
                "error": "Remote access is disabled on this instance"
            })),
        ));
    }
    if let Some(ch) = channel {
        if !policy.channel_allowed(ch) {
            return Err((
                axum::http::StatusCode::FORBIDDEN,
                Json(clean_json!({
                    "status": "forbidden",
                    "error": "Channel is not allowed by the source policy"
                })),
            ));
        }
    }
    Ok(Some(
        events
            .into_iter()
            .filter_map(|mut e| policy.apply_to_event(&mut e).then_some(e))
            .collect(),
    ))
}

#[derive(serde::Deserialize)]
pub(crate) struct ReplayQueryParams {
    cursor: Option<i64>,
    /// Deprecated alias for `cursor` (pre-per-channel SDK clients).
    channel_cursor: Option<i64>,
    limit: Option<i64>,
    event_type: Option<String>,
}

#[derive(serde::Deserialize)]
pub(crate) struct CausalGraphQuery {
    max_nodes: Option<i64>,
}

pub async fn causal_graph_handler(
    headers: HeaderMap,
    Path(event_id): Path<Uuid>,
    Query(params): Query<CausalGraphQuery>,
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
        "actae.api.v1.events.causal_graph.get",
        b"",
        &headers,
    ) {
        return resp;
    }
    match state
        .event_handler
        .causal_graph(event_id, params.max_nodes.unwrap_or(1000), &auth)
        .await
    {
        Ok(graph) => {
            let events: Vec<_> = graph.events.into_iter().map(|event| clean_json!({"id":event.id.to_string(),"channel_id":event.channel_id,"type":event.event_type,"payload":event.payload,"actor":event.metadata.actor,"agent_id":event.metadata.agent_id,"user_id":event.metadata.user_id,"metadata":event.metadata.metadata,"cursor":event.cursor,"timestamp":event.timestamp.to_rfc3339(),"depends_on":event.depends_on.map(|value|value.to_string())}).0).collect();
            let dependencies: Vec<_> = graph.dependencies.into_iter().map(|edge| clean_json!({"event_id":edge.event_id.to_string(),"depends_on_event_id":edge.depends_on_event_id.to_string(),"dependency_type":edge.dependency_type}).0).collect();
            (
                StatusCode::OK,
                Json(
                    clean_json!({"status":"ok","root_event_id":graph.root_event_id.to_string(),"events":events,"dependencies":dependencies,"truncated":graph.truncated}),
                ),
            )
        }
        Err(error) => {
            let (code, status) = err_to_status(&error);
            (
                code,
                Json(clean_json!({"status":status,"error":err_to_client_message(&error)})),
            )
        }
    }
}

pub async fn record_event_handler(
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: PublishEventRequest = match parse_json_body(body.as_ref()) {
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
        "actae.api.v1.events.record",
        body.as_ref(),
        &headers,
    ) {
        return resp;
    }

    match state.event_handler.record(request, &auth, None).await {
        Ok(event) => (
            StatusCode::CREATED,
            Json(clean_json!({
                "status": "recorded",
                "event": {
                    "id": event.id.to_string(),
                    "channel_id": event.channel_id,
                    "type": event.event_type,
                    "payload": event.payload,
                    "actor": event.metadata.actor,
                    "cursor": event.cursor,
                    "channel_cursor": event.cursor,
                    "timestamp": event.timestamp.to_rfc3339(),
                    "depends_on": event.depends_on.map(|d| d.to_string()),
                }
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

pub async fn transition_handler(
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let request: TransitionRequest = match parse_json_body(body.as_ref()) {
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
        "actae.api.v1.events.transition",
        body.as_ref(),
        &headers,
    ) {
        return resp;
    }

    match state
        .event_handler
        .record_transition(request, &auth, None)
        .await
    {
        Ok((event, version)) => (
            StatusCode::CREATED,
            Json(clean_json!({
                "status": "recorded",
                "event": {
                    "id": event.id.to_string(),
                    "channel_id": event.channel_id,
                    "type": event.event_type,
                    "payload": event.payload,
                    "actor": event.metadata.actor,
                    "cursor": event.cursor,
                    "channel_cursor": event.cursor,
                    "timestamp": event.timestamp.to_rfc3339(),
                    "depends_on": event.depends_on.map(|d| d.to_string()),
                },
                "state_version": version,
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

pub async fn replay_events_handler(
    headers: HeaderMap,
    Path(channel_id): Path<String>,
    Query(params): Query<ReplayQueryParams>,
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
        "actae.api.v1.events.replay",
        b"",
        &headers,
    ) {
        return resp;
    }

    let request = ReplayRequest {
        channel_id,
        cursor: params.cursor,
        channel_cursor: params.channel_cursor,
        limit: params.limit,
        event_type: params.event_type,
    };

    match state.event_handler.replay(&request, &auth).await {
        Ok(events) => {
            let events = match apply_source_policy(
                &state.app_state,
                &auth,
                events,
                Some(&request.channel_id),
            ) {
                Ok(Some(events)) => events,
                Ok(None) => unreachable!("helper returns Some for non-denied"),
                Err(e) => return e,
            };
            let event_list: Vec<sonic_rs::Value> = events.into_iter().map(event_to_json).collect();

            (
                StatusCode::OK,
                Json(clean_json!({
                    "status": "ok",
                    "events": event_list,
                    "count": event_list.len(),
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

pub async fn query_events_handler(
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, Json<CleanJson>) {
    let state = actae::api::global_state();
    if let Some(resp) = check_rate_limit(&state.security_manager, &headers) {
        return resp;
    }
    let body_str = std::str::from_utf8(&body).unwrap_or("{}");
    let request: QueryRequest = match sonic_rs::from_str(body_str) {
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
        "actae.api.v1.events.query",
        body.as_ref(),
        &headers,
    ) {
        return resp;
    }

    // Keyset pagination (Phase 1, PR-007): when a `page_token` is supplied,
    // or when `pagination: "keyset"` is explicit, the query runs through the
    // keyset path — including a tokenless FIRST page. Without either, the
    // legacy offset-based query path runs unchanged (D-011 — keep direct API
    // behavior backward compatible).
    let keyset_mode =
        request.pagination.as_deref() == Some("keyset") || request.page_token.is_some();
    if !keyset_mode {
        return match state.event_handler.query(&request, &auth).await {
            Ok(events) => {
                let events = match apply_source_policy(&state.app_state, &auth, events, None) {
                    Ok(Some(events)) => events,
                    Ok(None) => unreachable!(),
                    Err(e) => return e,
                };
                let event_list: Vec<sonic_rs::Value> =
                    events.into_iter().map(event_to_json).collect();
                (
                    StatusCode::OK,
                    Json(
                        clean_json!({"status": "ok", "events": event_list, "count": event_list.len()}),
                    ),
                )
            }
            Err(e) => {
                let (code, status) = err_to_status(&e);
                (
                    code,
                    Json(clean_json!({"status": status, "error": err_to_client_message(&e)})),
                )
            }
        };
    }

    // Page-token path: verify against the CURRENT request's normalized filters
    // + caller scope so a token can never be reused with changed filters or by
    // a different principal (actae/src/events/page_token.rs).
    let secret = state.app_state.config.api_key_hash_secret.as_bytes();
    let filter_hash = actae::events::page_token::filter_fingerprint(&request);
    let scope_hash = actae::events::page_token::principal_scope_fingerprint(&auth);

    let after_global_cursor: Option<i64> = match &request.page_token {
        None => None,
        Some(token) => {
            match actae::events::page_token::verify(secret, token, &filter_hash, &scope_hash) {
                Ok(claims) => {
                    // Retention check: if the continuation bound was purged the
                    // page can never be satisfied — typed 410.
                    let min_gc = match state.event_handler.min_global_cursor().await {
                        Ok(m) => m,
                        Err(e) => {
                            let (code, status) = err_to_status(&e);
                            return (
                                code,
                                Json(
                                    clean_json!({"status": status, "error": err_to_client_message(&e)}),
                                ),
                            );
                        }
                    };
                    if let Some(min) = min_gc {
                        // PR-007: a continuation bound EQUAL to the smallest
                        // retained row is also unsatisfiable — the keyset
                        // fetches strictly below the bound, and rows below
                        // `min` were purged.
                        if claims.global_cursor <= min {
                            let e = actae::events::handler::EventError::PageTokenUnsatisfiable {
                                at_global_cursor: claims.global_cursor,
                            };
                            return (
                                StatusCode::GONE,
                                Json(clean_json!({
                                    "status": "page_token_unsatisfiable",
                                    "error": err_to_client_message(&e),
                                })),
                            );
                        }
                    }
                    Some(claims.global_cursor)
                }
                Err(page_err) => {
                    use actae::events::page_token::PageTokenError;
                    return match page_err {
                        PageTokenError::Expired => (
                            StatusCode::GONE,
                            Json(clean_json!({
                                "status": "page_token_expired",
                                "error": "Page token expired. Re-run the query from the first page with the same filters.",
                            })),
                        ),
                        PageTokenError::Invalid(msg) => (
                            StatusCode::BAD_REQUEST,
                            Json(clean_json!({
                                "status": "invalid_page_token",
                                "error": format!("Invalid page token: {}", msg),
                            })),
                        ),
                    };
                }
            }
        }
    };

    match state
        .event_handler
        .query_page(&request, after_global_cursor, &auth)
        .await
    {
        Ok((events, has_more, last_global_cursor)) => {
            let events = match apply_source_policy(&state.app_state, &auth, events, None) {
                Ok(Some(events)) => events,
                Ok(None) => unreachable!(),
                Err(e) => return e,
            };
            let event_list: Vec<sonic_rs::Value> = events.into_iter().map(event_to_json).collect();
            let next_page_token = if has_more {
                last_global_cursor.and_then(|gc| {
                    actae::events::page_token::mint(secret, &filter_hash, &scope_hash, gc).ok()
                })
            } else {
                None
            };
            (
                StatusCode::OK,
                Json(clean_json!({
                    "status": "ok",
                    "events": event_list,
                    "count": event_list.len(),
                    "next_page_token": next_page_token,
                    "has_more": has_more,
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

pub async fn latest_cursor_handler(
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
        "actae.api.v1.events.cursor.get",
        b"",
        &headers,
    ) {
        return resp;
    }

    match state
        .event_handler
        .get_latest_cursor(&channel_id, &auth)
        .await
    {
        Ok(cursor) => (
            StatusCode::OK,
            Json(clean_json!({
                "status": "ok",
                "channel_id": channel_id,
                "latest_cursor": cursor,
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

pub async fn list_channels_handler(headers: HeaderMap) -> (StatusCode, Json<CleanJson>) {
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
        "actae.api.v1.channels.list",
        b"",
        &headers,
    ) {
        return resp;
    }

    match state.event_handler.list_channels_with_stats(&auth).await {
        Ok(stats) => {
            let channels: Vec<String> = stats.iter().map(|s| s.channel_id.clone()).collect();
            // Additive per-channel stats (event counts, latest cursor, last
            // activity, friendly name + fork parent) for the dashboard. The
            // `channels` array keeps its historical string shape for SDK
            // compatibility.
            let mut stats_map = sonic_rs::Object::new();
            for s in stats {
                stats_map.insert(
                    s.channel_id.as_str(),
                    sonic_rs::json!({
                        "event_count": s.event_count,
                        "latest_cursor": s.latest_cursor,
                        "last_event_at": s.last_event_at.map(|t| t.to_rfc3339()),
                        "display_name": s.display_name,
                        "parent_channel_id": s.parent_channel_id,
                    }),
                );
            }
            (
                StatusCode::OK,
                Json(clean_json!({
                    "channels": channels,
                    "stats": stats_map,
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

/// GET /api/v1/events/verify/{channel_id} — verify the tamper-evident hash
/// chain for a channel (audit selling point 3). Read-authorized.
pub async fn verify_chain_handler(
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
        "actae.api.v1.events.verify_chain",
        b"",
        &headers,
    ) {
        return resp;
    }
    match state.event_handler.verify_chain(&channel_id, &auth).await {
        Ok(v) => (
            StatusCode::OK,
            Json(clean_json!({
                "channel_id": channel_id,
                "valid": v.valid,
                "verified_events": v.verified_events,
                "unverified_events": v.unverified_events,
                "broken_at_cursor": v.broken_at_cursor,
                "first_bad_hash": v.first_bad_hash,
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
