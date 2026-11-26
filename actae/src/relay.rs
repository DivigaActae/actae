// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Self-hosted outbound relay (Phase 7).
//!
//! A self-hosted Actae instance may opt in to remote fleet access by dialing
//! OUT to the fleet gateway's relay listener — the gateway NEVER dials a URL
//! supplied by a customer request (D-008). The relay transport is a versioned
//! WebSocket session with a rotated credential, fencing generation, heartbeat,
//! bounded memory, and immediate close on disable/revoke.
//!
//! Disabled by default (`ACTAE_RELAY_ENABLED`). Enabling logs a privacy
//! warning; when disabled the client never dials out.

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub const RELAY_PROTOCOL_VERSION: u32 = 1;

/// Spawn the outbound relay client task (no-op when disabled).
///
/// The instance dials OUT to the relay (the gateway's relay listener), holds
/// an authenticated versioned session with heartbeats, and executes relayed
/// canonical commands against its OWN canonical API (the single engine).
pub fn spawn_relay_client(
    cfg: RelayConfig,
    local_base_url: String,
) -> Option<tokio::task::JoinHandle<()>> {
    if !cfg.enabled {
        return None;
    }
    let url = cfg.url.clone().unwrap_or_default();
    let instance_id = cfg.instance_id.clone().unwrap_or_default();
    // PR-012: the per-instance derived credential, not the raw shared secret.
    let credential = match cfg.credential_for_instance() {
        Some(c) => c,
        None => cfg.credential.clone().unwrap_or_default(),
    };
    Some(tokio::spawn(async move {
        let mut delay = cfg.reconnect_min;
        loop {
            match run_relay_session(&cfg, &url, &credential, &instance_id, &local_base_url).await {
                Ok(()) => {
                    tracing::info!("relay session closed cleanly; not reconnecting");
                    break;
                }
                Err(e) => {
                    tracing::warn!("relay session ended: {e}; reconnecting in {:?}", delay);
                }
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(cfg.reconnect_max);
        }
    }))
}

async fn run_relay_session(
    cfg: &RelayConfig,
    relay_url: &str,
    credential: &str,
    instance_id: &str,
    local_base_url: &str,
) -> Result<()> {
    use futures_util::{SinkExt, StreamExt};

    let ws_url = relay_ws_url(relay_url, instance_id)?;
    let (mut stream, _resp) =
        tokio_websockets::ClientBuilder::from_uri(ws_url.parse::<axum::http::Uri>()?)
            .connect()
            .await
            .context("relay connect failed")?;

    let handshake = RelayFrame::Handshake {
        version: RELAY_PROTOCOL_VERSION,
        instance_id: instance_id.to_string(),
        credential: credential.to_string(),
    };
    stream
        .send(tokio_websockets::Message::text(
            String::from_utf8_lossy(&handshake.as_json_bytes()?).to_string(),
        ))
        .await
        .context("relay handshake send failed")?;

    let http = reqwest::Client::builder()
        .timeout(cfg.heartbeat_timeout)
        .connect_timeout(cfg.connect_timeout)
        .build()
        .context("relay local HTTP client")?;

    let mut heartbeat = tokio::time::interval(cfg.heartbeat_interval);
    let mut last_seen = std::time::Instant::now();
    let mut session_ok = false;

    loop {
        tokio::select! {
            frame = stream.next() => {
                let msg = match frame {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => return Err(anyhow!("relay socket error: {e}")),
                    None => return Err(anyhow!("relay closed the connection")),
                };
                let text = match msg.as_text() {
                    Some(t) => t.to_string(),
                    None => continue,
                };
                last_seen = std::time::Instant::now();
                let frame = match RelayFrame::from_json(&text) {
                    Ok(f) => f,
                    Err(_) => {
                        tracing::warn!("relay: ignoring malformed frame");
                        continue;
                    }
                };
                match frame {
                    RelayFrame::HandshakeOk { session_id, fencing_generation } => {
                        session_ok = true;
                        tracing::info!(
                            session_id = %session_id,
                            fencing_generation,
                            "relay session established (outbound)"
                        );
                    }
                    RelayFrame::HandshakeError { code, message } => {
                        return Err(anyhow!("relay rejected handshake: {code}: {message}"));
                    }
                    RelayFrame::Command { id, request_id, method, path, body, instance_token, idempotency_key, confirm_target } => {
                        let result = execute_relayed_command(&http, local_base_url, &method, &path, body.as_deref(), &instance_token, &request_id, idempotency_key.as_deref(), confirm_target.as_deref()).await;
                        let out = match result {
                            Ok((status, body)) => RelayFrame::CommandResult { id, status, body },
                            Err(e) => RelayFrame::CommandResult {
                                id,
                                status: 502,
                                body: Some(format!("{{\"error\":\"{}\"}}", e.to_string().replace('"', "\\\""))),
                            },
                        };
                        if stream
                            .send(tokio_websockets::Message::text(
                                String::from_utf8_lossy(&out.as_json_bytes()?).to_string(),
                            ))
                            .await
                            .is_err()
                        {
                            return Err(anyhow!("relay result send failed"));
                        }
                    }
                    RelayFrame::Heartbeat => {}
                    RelayFrame::Close { reason } => {
                        tracing::info!("relay closed the session: {reason}");
                        return Ok(());
                    }
                    RelayFrame::Handshake { .. } | RelayFrame::CommandResult { .. } => {
                        tracing::warn!("relay: unexpected frame type; ignoring");
                    }
                }
            }
            _ = heartbeat.tick() => {
                if session_ok {
                    if last_seen.elapsed() > cfg.heartbeat_timeout {
                        return Err(anyhow!("relay heartbeat timed out"));
                    }
                    let hb = RelayFrame::Heartbeat;
                    if stream
                        .send(tokio_websockets::Message::text(
                            String::from_utf8_lossy(&hb.as_json_bytes()?).to_string(),
                        ))
                        .await
                        .is_err()
                    {
                        return Err(anyhow!("relay heartbeat send failed"));
                    }
                }
            }
        }
    }
}

/// Execute a relayed canonical command against the instance's OWN canonical
/// API — the exact same handler/domain path a direct client uses, authorized
/// by the purpose-bound instance token (no second implementation).
async fn execute_relayed_command(
    http: &reqwest::Client,
    local_base_url: &str,
    method: &str,
    path: &str,
    body: Option<&str>,
    instance_token: &str,
    request_id: &str,
    idempotency_key: Option<&str>,
    confirm_target: Option<&str>,
) -> Result<(u16, Option<String>)> {
    // PR-013: the instance re-validates the resolved method/path — only the
    // canonical `/api/` surface is reachable through the relay, never an
    // operator/admin/dashboard path. The purpose-bound command token is then
    // verified by the instance's OWN auth layer (operation gate), so a
    // malicious frame cannot reach a different operation.
    if !path.starts_with("/api/") || path.contains("..") {
        return Err(anyhow!("relayed command path is not a canonical API path"));
    }
    let url = format!("{}{}", local_base_url.trim_end_matches('/'), path);
    let mut builder = match method {
        "GET" => http.get(&url),
        "POST" => http.post(&url),
        "PUT" => http.put(&url),
        "PATCH" => http.patch(&url),
        "DELETE" => http.delete(&url),
        m => return Err(anyhow!("unsupported relayed method {m}")),
    };
    builder = builder
        .header("authorization", format!("Bearer {instance_token}"))
        .header("x-request-id", request_id)
        .header("content-type", "application/json");
    // PR-014/Deep-004: forward the idempotency key + confirmation target so
    // the canonical server can verify the command token's content-bound
    // request digest. Without them relayed mutations fail digest verification.
    if let Some(key) = idempotency_key {
        builder = builder.header("idempotency-key", key);
    }
    if let Some(target) = confirm_target {
        builder = builder.header("x-actae-confirm-target", target);
    }
    if let Some(b) = body {
        builder = builder.body(b.to_string());
    }
    let resp = builder
        .send()
        .await
        .map_err(|e| anyhow!("relayed command to local API failed: {e}"))?;
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    let out = if text.is_empty() { None } else { Some(text) };
    Ok((status, out))
}

fn relay_ws_url(relay_url: &str, instance_id: &str) -> Result<String> {
    let base = relay_url.trim_end_matches('/');
    let ws = if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        return Err(anyhow!("relay URL must be http(s)://"));
    };
    Ok(format!("{ws}/v1/relay/instances/{instance_id}/connect"))
}

// ---------------------------------------------------------------------- //
// Wire frames
// ---------------------------------------------------------------------- //

/// An outbound frame from the instance to the relay (or the reverse).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RelayFrame {
    /// Instance → relay on connect: authenticate + version negotiate.
    #[serde(rename = "relay_handshake")]
    Handshake {
        version: u32,
        instance_id: String,
        credential: String,
    },
    /// Relay → instance: accepted (with fencing generation).
    #[serde(rename = "relay_handshake_ok")]
    HandshakeOk {
        session_id: String,
        fencing_generation: u64,
    },
    /// Relay → instance: rejected.
    #[serde(rename = "relay_handshake_error")]
    HandshakeError { code: String, message: String },
    /// Relay → instance: a typed canonical command to execute locally.
    #[serde(rename = "relay_command")]
    Command {
        id: String,
        request_id: String,
        method: String,
        path: String,
        #[serde(default)]
        body: Option<String>, // JSON body (raw UTF-8); None for GET/DELETE
        instance_token: String,
        /// PR-014/Deep-004: the idempotency key and confirmation target are
        /// forwarded so the canonical server can verify the command token's
        /// content-bound request digest (the direct path forwards the same
        /// headers). Dropping them made relayed mutations fail digest
        /// verification.
        #[serde(default)]
        idempotency_key: Option<String>,
        #[serde(default)]
        confirm_target: Option<String>,
    },
    /// Instance → relay: the command result.
    #[serde(rename = "relay_command_result")]
    CommandResult {
        id: String,
        status: u16,
        #[serde(default)]
        body: Option<String>,
    },
    /// Both directions: keepalive.
    #[serde(rename = "relay_heartbeat")]
    Heartbeat,
    /// Relay → instance: immediate close (disable/revoke/suspend).
    #[serde(rename = "relay_close")]
    Close { reason: String },
}

impl RelayFrame {
    /// Maximum serialized relay frame size (PR-013): 1 MiB — bounded frames so
    /// an oversized/malicious frame cannot exhaust memory on either side.
    pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

    pub fn as_json_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).context("serialize relay frame")
    }
    pub fn from_json(s: &str) -> Result<Self> {
        if s.len() > Self::MAX_FRAME_BYTES {
            return Err(anyhow!(
                "relay frame exceeds {} bytes",
                Self::MAX_FRAME_BYTES
            ));
        }
        serde_json::from_str(s).context("parse relay frame")
    }
}

/// Outbound relay configuration (disabled by default).
#[derive(Debug, Clone)]
pub struct RelayConfig {
    pub enabled: bool,
    pub url: Option<String>,
    pub credential: Option<String>,
    pub instance_id: Option<String>,
    pub connect_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
}

impl RelayConfig {
    pub fn from_env() -> Result<Self> {
        use std::env;
        let enabled = env::var("ACTAE_RELAY_ENABLED")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let url = env::var("ACTAE_RELAY_URL")
            .ok()
            .filter(|s| !s.trim().is_empty());
        let credential = env::var("ACTAE_RELAY_CREDENTIAL")
            .ok()
            .filter(|s| !s.trim().is_empty());
        let instance_id = env::var("ACTAE_INSTALL_ID")
            .ok()
            .filter(|s| !s.trim().is_empty());
        let cfg = Self {
            enabled,
            url,
            credential,
            instance_id,
            connect_timeout: Duration::from_secs(
                env::var("ACTAE_RELAY_CONNECT_TIMEOUT_SECS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(10),
            ),
            heartbeat_interval: Duration::from_secs(
                env::var("ACTAE_RELAY_HEARTBEAT_SECS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(20),
            ),
            heartbeat_timeout: Duration::from_secs(
                env::var("ACTAE_RELAY_HEARTBEAT_TIMEOUT_SECS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(45),
            ),
            reconnect_min: Duration::from_secs(
                env::var("ACTAE_RELAY_RECONNECT_MIN_SECS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1),
            ),
            reconnect_max: Duration::from_secs(
                env::var("ACTAE_RELAY_RECONNECT_MAX_SECS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(60),
            ),
        };
        if cfg.enabled {
            if cfg.url.is_none() {
                return Err(anyhow!(
                    "ACTAE_RELAY_ENABLED requires ACTAE_RELAY_URL (wss:// relay endpoint)"
                ));
            }
            if cfg.credential.is_none() {
                return Err(anyhow!(
                    "ACTAE_RELAY_ENABLED requires ACTAE_RELAY_CREDENTIAL"
                ));
            }
            if cfg.instance_id.is_none() {
                return Err(anyhow!("ACTAE_RELAY_ENABLED requires ACTAE_INSTALL_ID"));
            }
            tracing::warn!(
                "outbound relay is ENABLED — this instance dials the relay at {} with a persistent credential; customer event payloads leave this host only when a remote read/stream grants them (source policy applies).",
                cfg.url.as_deref().unwrap_or("?")
            );
        }
        Ok(cfg)
    }

    /// The per-instance relay credential sent in the handshake (PR-012).
    /// `HMAC-SHA256(ACTAE_RELAY_CREDENTIAL, "actae-relay:" + install_id)`,
    /// byte-identical to the gateway's `GatewayConfig::relay_credential_for`.
    /// A leaked credential for one instance cannot impersonate another.
    pub fn credential_for_instance(&self) -> Option<String> {
        let secret = self.credential.as_ref()?;
        let instance_id = self.instance_id.as_ref()?;
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;
        let mut mac =
            HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
        mac.update(b"actae-relay:");
        mac.update(instance_id.as_bytes());
        Some(format!("{:x}", mac.finalize().into_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip() {
        let f = RelayFrame::Handshake {
            version: 1,
            instance_id: "ins_1".into(),
            credential: "cred".into(),
        };
        let bytes = f.as_json_bytes().unwrap();
        let back = RelayFrame::from_json(std::str::from_utf8(&bytes).unwrap()).unwrap();
        assert!(matches!(back, RelayFrame::Handshake { version: 1, .. }));
    }

    #[test]
    fn command_frame_roundtrip() {
        let f = RelayFrame::Command {
            id: "c1".into(),
            request_id: "r1".into(),
            method: "POST".into(),
            path: "/api/v1/events/record".into(),
            body: Some("{\"x\":1}".into()),
            instance_token: "tok".into(),
            idempotency_key: Some("idem-1".into()),
            confirm_target: Some("ins_1".into()),
        };
        let bytes = f.as_json_bytes().unwrap();
        let back: RelayFrame = RelayFrame::from_json(std::str::from_utf8(&bytes).unwrap()).unwrap();
        match back {
            RelayFrame::Command {
                method,
                path,
                body,
                idempotency_key,
                confirm_target,
                ..
            } => {
                assert_eq!(method, "POST");
                assert_eq!(path, "/api/v1/events/record");
                assert_eq!(body.as_deref(), Some("{\"x\":1}"));
                assert_eq!(idempotency_key.as_deref(), Some("idem-1"));
                assert_eq!(confirm_target.as_deref(), Some("ins_1"));
            }
            other => panic!("wrong frame: {other:?}"),
        }
    }

    #[test]
    fn disabled_relay_requires_nothing() {
        // Unset env (already unset in tests) -> disabled.
        let cfg = RelayConfig::from_env().unwrap();
        assert!(!cfg.enabled);
    }

    #[test]
    fn enabled_relay_requires_fields() {
        std::env::set_var("ACTAE_RELAY_ENABLED", "1");
        std::env::remove_var("ACTAE_RELAY_URL");
        assert!(RelayConfig::from_env().is_err());
        std::env::remove_var("ACTAE_RELAY_ENABLED");
    }
}
