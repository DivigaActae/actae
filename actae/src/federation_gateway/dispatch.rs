// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Typed fleet dispatch (Phase 5/6).
//!
//! The gateway maps each typed fleet route to ONE canonical instance
//! operation (RUST-004): it never reimplements fork/state/event logic. The
//! canonical operation registry is derived from the embedded instance
//! manifest, so the fleet surface can never drift from the contract.
//!
//! Every mutation requires `Idempotency-Key`; the gateway derives a
//! deterministic canonical `operation_id` from
//! (org, instance, operation, resource, key) and forwards it — the canonical
//! server replays the original event on retry and fails loudly
//! (`409 idempotency_conflict`) when the same key is reused with different
//! content. Destructive operations additionally require
//! `X-Actae-Confirm-Target` equal to the canonical target resource id.

use anyhow::{anyhow, Result};
use sonic_rs::{JsonContainerTrait, JsonValueMutTrait, JsonValueTrait};
use std::collections::HashMap;

/// The canonical instance manifest, embedded at build time. The gateway's
/// `/ops/{op_id}` dispatch is typed BY this manifest (D-005): the canonical
/// path/method/action come from the manifest, never from the request, so the
/// fleet surface can never forward an arbitrary path.
const MANIFEST: &str = include_str!("../../../spec/api-parity/v1/manifest.json");

/// A canonical instance operation resolved from the embedded manifest.
#[derive(Debug, Clone)]
pub struct ManifestOp {
    pub id: String,
    pub method: String,
    pub path: String,
    pub action: String,
    /// The canonical fleet scope required to invoke this operation
    /// (`manifest.operations[].scope`), e.g. `events:write`. The gateway
    /// enforces it against the `fleet_access` token's granted scopes
    /// (PR-001); `public` operations are not fleet-invokable.
    pub scope: String,
}

/// Look up a canonical operation by its manifest id (e.g.
/// `actae.api.v1.state.save`). Returns `None` for unknown ops — the gateway
/// refuses them (no generic proxy).
pub fn lookup_op(op_id: &str) -> Option<ManifestOp> {
    let manifest: sonic_rs::Value = sonic_rs::from_str(MANIFEST).ok()?;
    let ops = manifest.as_object()?.get(&"operations")?.as_array()?;
    ops.iter()
        .find(|o| o.get("id").and_then(|v| v.as_str()) == Some(op_id))
        .map(|o| ManifestOp {
            id: o
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            method: o
                .get("method")
                .and_then(|v| v.as_str())
                .unwrap_or("POST")
                .to_string(),
            path: o
                .get("path")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            action: o
                .get("action")
                .and_then(|v| v.as_str())
                .unwrap_or("read")
                .to_string(),
            scope: o
                .get("scope")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        })
}

/// Whether a canonical operation is fleet-invokable. Auth/session/dashboard
/// operations (`scope == "public"` or the operation is on the auth surface)
/// are not fleet commands — federation command authority is intentionally not
/// dashboard/session authority (PR-016). Returns `None` for unknown ops.
pub fn fleet_supported_scope(op_id: &str) -> Option<String> {
    let op = lookup_op(op_id)?;
    if op.scope.is_empty() || op.scope == "public" {
        return None;
    }
    Some(op.scope)
}

/// The sorted list of fleet-invokable operation ids (PR-017 capability
/// negotiation). Auth/session/dashboard operations are excluded.
pub fn fleet_supported_operation_ids() -> Vec<String> {
    let manifest: sonic_rs::Value = match sonic_rs::from_str(MANIFEST) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let mut out: Vec<String> = Vec::new();
    if let Some(ops) = manifest
        .as_object()
        .and_then(|o| o.get(&"operations"))
        .and_then(|v| v.as_array())
    {
        for o in ops {
            let id = o.get("id").and_then(|v| v.as_str()).unwrap_or("");
            let scope = o.get("scope").and_then(|v| v.as_str()).unwrap_or("");
            if id.is_empty() || scope.is_empty() || scope == "public" {
                continue;
            }
            out.push(id.to_string());
        }
    }
    out.sort();
    out
}

/// Render a canonical path template (`/api/v1/state/{channel_id}`) from the
/// supplied path params. Returns an error when a placeholder has no value.
pub fn render_manifest_path(op: &ManifestOp, params: &HashMap<String, String>) -> Result<String> {
    let mut out = op.path.clone();
    for (name, value) in params {
        let placeholder = format!("{{{name}}}");
        if out.contains(&placeholder) {
            out = out.replace(
                &placeholder,
                &percent_encoding::utf8_percent_encode(value, percent_encoding::NON_ALPHANUMERIC)
                    .to_string(),
            );
        }
    }
    if out.contains('{') {
        return Err(anyhow!(
            "operation {} requires path params: {}",
            op.id,
            op.path
        ));
    }
    Ok(out)
}

/// Append an opaque manifest query map to a rendered canonical path. Values
/// are encoded with the standard form-url-encoding rules; callers decide
/// whether the operation's method permits query parameters.
pub fn append_query(path: &str, query: &HashMap<String, String>) -> String {
    if query.is_empty() {
        return path.to_string();
    }
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    let mut keys: Vec<_> = query.keys().collect();
    keys.sort();
    for key in keys {
        if let Some(value) = query.get(key) {
            serializer.append_pair(key, value);
        }
    }
    let encoded = serializer.finish();
    format!(
        "{path}{}{}",
        if path.contains('?') { '&' } else { '?' },
        encoded
    )
}

/// Derive the resource pattern for a manifest op from path params + body.
pub fn manifest_resource(
    _op: &ManifestOp,
    params: &HashMap<String, String>,
    body: &sonic_rs::Value,
) -> String {
    if let Some(ch) = params.get("channel_id").or_else(|| params.get("channel")) {
        return ch.clone();
    }
    if let Some(ch) = body
        .get("channel_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
    {
        return ch;
    }
    if let Some(first) = params.values().next() {
        return first.clone();
    }
    "*".to_string()
}

/// One canonical instance operation a typed fleet route may dispatch.
#[derive(Debug, Clone, Copy)]
pub struct CanonicalOp {
    pub id: &'static str,
    pub action: &'static str,
    pub method: &'static str,
    /// Canonical path template with `{channel}` placeholders.
    pub path: &'static str,
}

/// The primary canonical operation id used by the typed fleet routes.
pub const OP_FORK: CanonicalOp = CanonicalOp {
    id: "actae.api.v1.forks.create",
    action: "write",
    method: "POST",
    path: "/api/v1/channels/fork",
};
pub const OP_RECORD: CanonicalOp = CanonicalOp {
    id: "actae.api.v1.events.record",
    action: "write",
    method: "POST",
    path: "/api/v1/events/record",
};
pub const OP_SAVE_STATE: CanonicalOp = CanonicalOp {
    id: "actae.api.v1.state.save",
    action: "write",
    method: "POST",
    path: "/api/v1/state/{channel}",
};
pub const OP_DELETE_CHANNEL: CanonicalOp = CanonicalOp {
    id: "actae.api.v1.channels.delete",
    action: "delete",
    method: "DELETE",
    path: "/api/v1/channels/{channel}",
};
pub const OP_QUERY: CanonicalOp = CanonicalOp {
    id: "actae.api.v1.events.query",
    action: "read",
    method: "POST",
    path: "/api/v1/events/query",
};
pub const OP_WAKEUP: CanonicalOp = CanonicalOp {
    id: "actae.api.v1.scheduler.wakeups.create",
    action: "write",
    method: "POST",
    path: "/api/v1/scheduler/wakeups",
};

/// Deterministic canonical operation id derived from the fleet idempotency
/// key. Binds org/instance/operation/resource/key so the canonical server's
/// operation-id idempotency replays or rejects correctly.
pub fn derive_operation_id(
    org: &str,
    instance: &str,
    op_id: &str,
    resource: &str,
    key: &str,
) -> String {
    let ns = uuid::Uuid::NAMESPACE_URL;
    let s = format!("actae:fleet:v1:{org}:{instance}:{op_id}:{resource}:{key}");
    uuid::Uuid::new_v5(&ns, s.as_bytes()).to_string()
}

/// Derive a resource pattern from the request body / path for the
/// purpose-bound instance token and the confirm-target check.
pub fn resource_for_fork(body: &sonic_rs::Value) -> Option<String> {
    body.get("source_channel_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

pub fn resource_for_record(body: &sonic_rs::Value) -> Option<String> {
    body.get("channel_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Parse the JSON body (bounded) into a value; `sonic_rs` with
/// arbitrary_precision so int64 payloads round-trip.
pub fn parse_body(body: &[u8]) -> Result<sonic_rs::Value> {
    sonic_rs::from_slice(body).map_err(|e| anyhow!("invalid JSON body: {e}"))
}

/// Fill the canonical path template (`{channel}`).
pub fn render_path(op: &CanonicalOp, channel: Option<&str>) -> String {
    match channel {
        Some(ch) => op.path.replace(
            "{channel}",
            &percent_encoding::utf8_percent_encode(ch, percent_encoding::NON_ALPHANUMERIC)
                .to_string(),
        ),
        None => op.path.to_string(),
    }
}

/// Extract the canonical resource id from an `Idempotency-Key`-bound request
/// for the confirm-target check. Returns `None` for non-destructive ops.
pub fn confirm_target_for(op: &CanonicalOp, channel: Option<&str>) -> Option<String> {
    match op.action {
        "delete" => channel.map(|c| c.to_string()),
        _ => None,
    }
}

/// Per-route configuration: which canonical op + how to extract the resource.
#[derive(Debug, Clone, Copy)]
pub struct RouteSpec {
    pub op: CanonicalOp,
    /// Extractor for the resource pattern (path channel for state/delete).
    pub channel_from_path: bool,
    /// Body key for the resource pattern (source_channel_id / channel_id).
    pub resource_body_key: Option<&'static str>,
}

pub const ROUTE_FORK: RouteSpec = RouteSpec {
    op: OP_FORK,
    channel_from_path: false,
    resource_body_key: Some("source_channel_id"),
};
pub const ROUTE_RECORD: RouteSpec = RouteSpec {
    op: OP_RECORD,
    channel_from_path: false,
    resource_body_key: Some("channel_id"),
};
pub const ROUTE_SAVE_STATE: RouteSpec = RouteSpec {
    op: OP_SAVE_STATE,
    channel_from_path: true,
    resource_body_key: None,
};
pub const ROUTE_DELETE: RouteSpec = RouteSpec {
    op: OP_DELETE_CHANNEL,
    channel_from_path: true,
    resource_body_key: None,
};
pub const ROUTE_WAKEUP: RouteSpec = RouteSpec {
    op: OP_WAKEUP,
    channel_from_path: false,
    resource_body_key: Some("channel_id"),
};

/// Derive the resource patterns for a route spec from the path channel and/or
/// body. Fork needs BOTH the source and the child channel (the engine checks
/// source read + child write); other routes need just their channel.
pub fn resources_for(
    spec: &RouteSpec,
    channel: Option<&str>,
    body: &sonic_rs::Value,
) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(key) = spec.resource_body_key {
        if let Some(v) = body.get(key).and_then(|v| v.as_str()) {
            out.push(v.to_string());
        }
    }
    if let Some(ch) = channel {
        if !out.contains(&ch.to_string()) {
            out.push(ch.to_string());
        }
    }
    // Fork additionally needs the child channel in the token's resources.
    if spec.op.id == "actae.api.v1.forks.create" {
        if let Some(v) = body
            .get("new_channel_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
        {
            if !out.contains(&v) {
                out.push(v);
            }
        }
    }
    out
}

/// The primary resource (first derived) for confirm-target + operation id.
pub fn resource_for(
    spec: &RouteSpec,
    channel: Option<&str>,
    body: &sonic_rs::Value,
) -> Option<String> {
    resources_for(spec, channel, body).into_iter().next()
}

/// Deserialize the `Idempotency-Key` + `X-Actae-Confirm-Target` headers.
pub struct FleetHeaders {
    pub idempotency_key: Option<String>,
    pub confirm_target: Option<String>,
}

impl FleetHeaders {
    pub fn from_headers(headers: &axum::http::HeaderMap) -> Self {
        Self {
            idempotency_key: headers
                .get("idempotency-key")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            confirm_target: headers
                .get("x-actae-confirm-target")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
        }
    }
}

/// Validate the confirm-target for a destructive operation.
pub fn check_confirm(spec: &RouteSpec, headers: &FleetHeaders, resource: &str) -> Result<()> {
    if spec.op.action == "delete" {
        let target = headers.confirm_target.as_deref().ok_or_else(|| {
            anyhow!("destructive fleet operation requires X-Actae-Confirm-Target")
        })?;
        if target != resource {
            return Err(anyhow!(
                "X-Actae-Confirm-Target does not match the canonical target resource"
            ));
        }
    }
    Ok(())
}

/// The canonical request to forward to an instance.
#[derive(Debug)]
pub struct CanonicalRequest {
    pub url: String,
    pub method: &'static str,
    pub body: Option<Vec<u8>>,
    pub instance_token: String,
    /// Trace/audit request id propagated to the instance (X-Request-Id).
    pub request_id: String,
    /// Idempotency-Key forwarded to the instance (PR-014 digest verification).
    pub idempotency_key: Option<String>,
    /// X-Actae-Confirm-Target forwarded to the instance (PR-014).
    pub confirm_target: Option<String>,
}

/// Canonical request digest for content-bound command tokens (PR-014).
///
/// The digest binds a command token to the exact serialized body, idempotency
/// key and confirmation target. Method/path are already bound to the token by
/// the `op` gate and resource patterns, so the digest binds the mutable
/// CONTENT — the anti-replay property. The canonical server recomputes the
/// same digest over the received body/headers and rejects a mismatch before
/// handler execution. `body` must be the exact bytes forwarded to the
/// instance (including any injected `operation_id`).
pub fn request_digest(
    body: Option<&[u8]>,
    idempotency_key: Option<&str>,
    confirm_target: Option<&str>,
) -> String {
    crate::auth::federation::compute_request_digest(
        body.unwrap_or_default(),
        idempotency_key,
        confirm_target,
    )
}

/// Build the canonical request for a single-instance mutation. Injects the
/// derived `operation_id` when an `Idempotency-Key` is present.
pub fn build_canonical_request(
    base_url: &str,
    spec: &RouteSpec,
    channel: Option<&str>,
    body: sonic_rs::Value,
    operation_id: Option<&str>,
    instance_token: &str,
    request_id: &str,
) -> Result<CanonicalRequest> {
    let url = format!(
        "{}{}",
        base_url.trim_end_matches('/'),
        render_path(&spec.op, channel)
    );
    let body_bytes = match operation_id {
        Some(op_id) => {
            let mut b = body;
            if let Some(obj) = b.as_object_mut() {
                obj.insert("operation_id", sonic_rs::json!(op_id));
            }
            sonic_rs::to_vec(&b).map_err(|e| anyhow!("failed to serialize body: {e}"))?
        }
        None => sonic_rs::to_vec(&body).map_err(|e| anyhow!("failed to serialize body: {e}"))?,
    };
    Ok(CanonicalRequest {
        url,
        method: spec.op.method,
        body: Some(body_bytes),
        instance_token: instance_token.to_string(),
        request_id: request_id.to_string(),
        idempotency_key: None,
        confirm_target: None,
    })
}

/// Fleet response envelope: wraps (never changes) the canonical data.
pub fn envelope(instance_id: &str, instance_name: &str, data: sonic_rs::Value) -> sonic_rs::Value {
    sonic_rs::json!({
        "instance_id": instance_id,
        "instance_name": instance_name,
        "data": data,
        "observed_at": chrono::Utc::now().to_rfc3339(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_id_is_deterministic_and_scoped() {
        let a = derive_operation_id("org_1", "ins_1", OP_RECORD.id, "ch", "key-1");
        let b = derive_operation_id("org_1", "ins_1", OP_RECORD.id, "ch", "key-1");
        assert_eq!(a, b);
        let c = derive_operation_id("org_1", "ins_1", OP_RECORD.id, "ch", "key-2");
        let d = derive_operation_id("org_2", "ins_1", OP_RECORD.id, "ch", "key-1");
        assert_ne!(a, c);
        assert_ne!(a, d);
    }

    #[test]
    fn render_path_replaces_channel() {
        assert_eq!(render_path(&OP_SAVE_STATE, Some("ch")), "/api/v1/state/ch");
        assert_eq!(render_path(&OP_FORK, None), "/api/v1/channels/fork");
        // Channel ids follow the shared grammar ([A-Za-z0-9._:-], no slashes);
        // encoding is defensive and must not leave an unescaped path break.
        let rendered = render_path(&OP_SAVE_STATE, Some("a.b/c"));
        assert!(rendered.starts_with("/api/v1/state/a"));
        assert!(rendered.contains("%2F"));
        assert!(!rendered.contains("/c"));
    }

    #[test]
    fn append_query_is_encoded_and_stable() {
        let query = HashMap::from([
            ("limit".to_string(), "2".to_string()),
            ("event_type".to_string(), "tool/x".to_string()),
        ]);
        assert_eq!(
            append_query("/api/v1/events/replay/ch", &query),
            "/api/v1/events/replay/ch?event_type=tool%2Fx&limit=2"
        );
    }

    #[test]
    fn confirm_target_required_for_delete() {
        let h = FleetHeaders {
            idempotency_key: Some("k".into()),
            confirm_target: None,
        };
        assert!(check_confirm(&ROUTE_DELETE, &h, "ch").is_err());
        let h2 = FleetHeaders {
            idempotency_key: Some("k".into()),
            confirm_target: Some("other".into()),
        };
        assert!(check_confirm(&ROUTE_DELETE, &h2, "ch").is_err());
        let h3 = FleetHeaders {
            idempotency_key: Some("k".into()),
            confirm_target: Some("ch".into()),
        };
        assert!(check_confirm(&ROUTE_DELETE, &h3, "ch").is_ok());
        // Non-destructive ops never require confirmation.
        assert!(check_confirm(&ROUTE_RECORD, &h, "ch").is_ok());
    }

    #[test]
    fn canonical_request_injects_operation_id() {
        let body = sonic_rs::json!({"channel_id": "ch", "type": "tool.x", "payload": {"n": 1}, "metadata": {"actor": "a"}});
        let req = build_canonical_request(
            "http://ins:8002",
            &ROUTE_RECORD,
            None,
            body,
            Some("op-123"),
            "tok",
            "req-1",
        )
        .unwrap();
        assert_eq!(req.url, "http://ins:8002/api/v1/events/record");
        assert_eq!(req.request_id, "req-1");
        let parsed: sonic_rs::Value = sonic_rs::from_slice(req.body.as_ref().unwrap()).unwrap();
        assert_eq!(
            parsed.get("operation_id").and_then(|v| v.as_str()),
            Some("op-123")
        );
        assert_eq!(
            parsed.get("channel_id").and_then(|v| v.as_str()),
            Some("ch")
        );
    }

    #[test]
    fn resource_extraction() {
        let body = sonic_rs::json!({"source_channel_id": "src"});
        assert_eq!(
            resource_for(&ROUTE_FORK, None, &body).as_deref(),
            Some("src")
        );
        assert_eq!(
            resource_for(&ROUTE_DELETE, Some("ch"), &sonic_rs::json!({})).as_deref(),
            Some("ch")
        );
        assert_eq!(resource_for_fork(&body).as_deref(), Some("src"));
        // Fork resources include both source and child.
        let fork_body = sonic_rs::json!({"source_channel_id": "src", "new_channel_id": "child"});
        let res = resources_for(&ROUTE_FORK, None, &fork_body);
        assert_eq!(res, vec!["src", "child"]);
    }

    #[test]
    fn lookup_op_known_and_unknown() {
        let save = lookup_op("actae.api.v1.state.save").expect("known op");
        assert_eq!(save.method, "POST");
        assert_eq!(save.action, "write");
        assert!(save.path.contains("{channel_id}"));
        let state_load = lookup_op("actae.api.v1.state.load").expect("known op");
        assert_eq!(state_load.method, "GET");
        assert_eq!(state_load.action, "read");
        assert!(lookup_op("actae.api.v1.not.real").is_none());
    }

    #[test]
    fn render_manifest_path_substitutes_and_validates() {
        use std::collections::HashMap;
        let mut params = HashMap::new();
        params.insert("channel_id".into(), "ch".into());
        let save = lookup_op("actae.api.v1.state.save").unwrap();
        assert_eq!(
            render_manifest_path(&save, &params).unwrap(),
            "/api/v1/state/ch"
        );
        // Missing param -> error (no generic proxy / incomplete request).
        let load = lookup_op("actae.api.v1.state.load").unwrap();
        assert!(render_manifest_path(&load, &HashMap::new()).is_err());
        // A no-param op renders as-is.
        let record = lookup_op("actae.api.v1.events.record").unwrap();
        assert_eq!(
            render_manifest_path(&record, &HashMap::new()).unwrap(),
            "/api/v1/events/record"
        );
    }

    #[test]
    fn manifest_resource_derivation() {
        use std::collections::HashMap;
        let mut params = HashMap::new();
        params.insert("channel_id".into(), "ch".into());
        let body = sonic_rs::json!({"state": {}});
        let save = lookup_op("actae.api.v1.state.save").unwrap();
        assert_eq!(manifest_resource(&save, &params, &body), "ch");
        let record = lookup_op("actae.api.v1.events.record").unwrap();
        let body2 = sonic_rs::json!({"channel_id": "rec-ch"});
        assert_eq!(
            manifest_resource(&record, &HashMap::new(), &body2),
            "rec-ch"
        );
        // Fallback to "*" (the source policy's narrowing still governs).
        assert_eq!(
            manifest_resource(&record, &HashMap::new(), &sonic_rs::json!({})),
            "*"
        );
    }
}
