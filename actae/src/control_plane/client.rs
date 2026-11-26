// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Wire types shared with the managed control plane.
//!
//! The community build does not talk to a control plane. These types remain
//! because the dashboard token verifier and the local key store reference the
//! same shapes. There is no HTTP client here.

use serde::Deserialize;

/// A key as delivered by the control plane. Only used when decoding signed
/// payloads; nothing is fetched in this build.
#[derive(Debug, Clone, Deserialize)]
pub struct RemoteKey {
    pub id: String,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub user_id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub permissions: Option<serde_json::Value>,
    #[serde(default)]
    pub expires_at: Option<String>,
}

/// An Ed25519 federation verification key distributed by the control plane.
#[derive(Debug, Clone, Deserialize)]
pub struct FederationKeyWire {
    pub kid: String,
    /// base64 (std or url-safe-no-pad) encoded Ed25519 public key (32 bytes).
    pub public_key: String,
}

/// Full-access permission set for locally created keys.
pub fn full_access_permissions() -> sonic_rs::Value {
    sonic_rs::json!({"read": ["*"], "write": ["*"], "delete": [], "admin": []})
}

/// Public download host. Used by `actae upgrade`, not for enrollment.
pub const DEFAULT_CONTROL_PLANE_URL: &str = "https://actae.dev";
