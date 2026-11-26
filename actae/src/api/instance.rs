// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! `GET /api/v1/instance` — instance identity for federation and diagnostics.
//!
//! Returns this instance's immutable installation/instance id, engine version,
//! deployment mode, and safe capability metadata. Used by the fleet gateway to
//! resolve authorized instances and by `actae doctor`/monitoring to identify a
//! deployment. Additive to `actae.api.v1` (D-011); it never exposes secrets,
//! database URLs, or customer payloads.

use crate::clean_json;
use crate::database::json_compat::CleanJson;
use axum::{http::StatusCode, routing::get, Json, Router};
use sonic_rs::JsonContainerTrait;

/// Operation ids from the canonical manifest, embedded at build time so the
/// binary and the published contract can never drift apart (RUST-001). The
/// file lives in the repo `spec/` tree; include_str! resolves relative to this
/// source file.
const MANIFEST: &str = include_str!("../../../spec/api-parity/v1/manifest.json");

pub fn routes() -> Router<()> {
    Router::new().route("/", get(instance_handler))
}

/// GET /api/v1/instance — return immutable instance identity + safe features.
async fn instance_handler() -> (StatusCode, Json<CleanJson>) {
    let state = crate::api::global_state();
    let config = &state.app_state.config;

    let instance_id = config
        .install_id
        .clone()
        .unwrap_or_else(|| "unmanaged".to_string());

    let manifest_ops = sonic_rs::from_str::<sonic_rs::Value>(MANIFEST)
        .ok()
        .and_then(|v| v.as_object().and_then(|o| o.get(&"operations")).cloned())
        .unwrap_or_default();

    (
        StatusCode::OK,
        Json(clean_json!({
            "instance_id": instance_id,
            "installation_id": instance_id,
            "version": env!("CARGO_PKG_VERSION"),
            "mode": if config.managed_storage { "embedded" } else { &config.deployment_mode },
            "api_version": "actae.api.v1",
            "host": config.host,
            "port": config.port,
            "control_plane_url": config.control_plane_url,
            "capabilities": {
                "scopes": [
                    "events:read", "events:write",
                    "state:read", "state:write", "state:delete",
                    "channels:read", "channels:write", "channels:delete",
                    "forks:create", "forks:promote",
                    "groups:manage", "execution_groups:manage",
                    "experiments:manage", "scheduler:manage", "executions:manage",
                    "streams:read", "payloads:read"
                ],
                "keyset_pagination": true,
                "subscribe_many": true,
                "fork_receipts": true,
                "durable_wakeups": true,
                "operation_manifest": manifest_ops
            }
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_rs::JsonValueTrait;

    #[test]
    fn manifest_embedded_at_build_time() {
        let v: sonic_rs::Value = sonic_rs::from_str(MANIFEST).expect("manifest JSON parses");
        let ops = v
            .as_object()
            .and_then(|o| o.get(&"operations"))
            .and_then(|x| x.as_array());
        assert!(
            ops.is_some_and(|o| !o.is_empty()),
            "manifest has operations"
        );
    }

    #[test]
    fn manifest_has_instance_operation_id() {
        let v: sonic_rs::Value = sonic_rs::from_str(MANIFEST).expect("manifest JSON parses");
        let ids: Vec<_> = v
            .as_object()
            .and_then(|o| o.get(&"operations"))
            .and_then(|x| x.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|o| {
                        o.as_object()
                            .and_then(|obj| obj.get(&"id"))
                            .and_then(|i| i.as_str())
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        assert!(ids.contains(&"actae.api.v1.instance.get"));
    }
}
