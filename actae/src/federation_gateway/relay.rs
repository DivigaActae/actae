// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Gateway relay listener (Phase 7): accepts outbound sessions from
//! relay-enabled self-hosted instances and routes typed canonical commands
//! through them. The gateway never dials the instance — the instance dials
//! OUT (D-008).
//!
//! Fencing: a second session for the same instance bumps the fencing
//! generation and closes the previous one, so a stale session can never act.

use anyhow::{anyhow, Result};
use axum::{
    extract::{Path, State, WebSocketUpgrade},
    response::IntoResponse,
};
use dashmap::DashMap;
use std::sync::Arc;

use crate::relay::{RelayFrame, RELAY_PROTOCOL_VERSION};

use super::GatewayState;

fn frame_text(frame: &RelayFrame) -> Result<String> {
    Ok(String::from_utf8(frame.as_json_bytes()?)?)
}

/// One live outbound session from a self-hosted instance.
#[derive(Clone)]
pub struct RelaySession {
    /// Sends relay frames to the instance.
    sender: flume::Sender<RelayFrame>,
    /// command id -> result channel (bounded, one-shot).
    pending: Arc<DashMap<String, flume::Sender<RelayFrame>>>,
    pub fencing_generation: u64,
    /// Stable per-session id (PR-013): registry cleanup is compare-and-remove
    /// on this id so a fenced (replaced) session can never delete the healthy
    /// replacement's registry entry.
    pub session_id: String,
}

impl RelaySession {
    pub fn is_alive(&self) -> bool {
        !self.sender.is_disconnected()
    }
}

/// Registry of live outbound relay sessions keyed by instance id.
#[derive(Default)]
pub struct RelayRegistry {
    sessions: DashMap<String, RelaySession>,
}

impl RelayRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fence: register a new session, closing any previous one for the same
    /// instance with the bumped generation. Returns the (new) generation.
    fn fence(&self, instance_id: &str, mut session: RelaySession) -> u64 {
        let new_gen = if let Some(old) = self.sessions.remove(instance_id) {
            let _ = old.1.sender.send(RelayFrame::Close {
                reason: "fenced by a newer session".into(),
            });
            old.1.fencing_generation + 1
        } else {
            1
        };
        session.fencing_generation = new_gen;
        self.sessions.insert(instance_id.to_string(), session);
        new_gen
    }

    /// Look up a live session.
    pub fn get(&self, instance_id: &str) -> Option<RelaySession> {
        self.sessions.get(instance_id).map(|e| e.value().clone())
    }

    /// PR-013: remove a session ONLY if it is still the registered one for the
    /// instance. A fenced (replaced) session exiting must not delete the
    /// healthy replacement's entry.
    pub fn remove_if_current(&self, instance_id: &str, session_id: &str) -> bool {
        let is_current = self
            .sessions
            .get(instance_id)
            .map(|e| e.value().session_id == session_id)
            .unwrap_or(false);
        if is_current {
            return self.sessions.remove(instance_id).is_some();
        }
        false
    }

    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }
}

/// A relayed canonical command result.
#[derive(Debug, Clone)]
pub struct RelayResponse {
    pub status: u16,
    pub body: Option<Vec<u8>>,
}

/// Execute a canonical command through the instance's relay session. Bounded
/// by `request_timeout`.
#[allow(clippy::too_many_arguments)]
pub async fn relay_command(
    session: &RelaySession,
    request_id: &str,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
    instance_token: &str,
    timeout: std::time::Duration,
    idempotency_key: Option<&str>,
    confirm_target: Option<&str>,
) -> Result<RelayResponse> {
    let id = format!("cmd_{}", uuid::Uuid::new_v4().simple());
    let (tx, rx) = flume::bounded(1);
    session.pending.insert(id.clone(), tx.clone());
    let frame = RelayFrame::Command {
        id: id.clone(),
        request_id: request_id.to_string(),
        method: method.to_string(),
        path: path.to_string(),
        body: body.map(|b| String::from_utf8_lossy(b).to_string()),
        instance_token: instance_token.to_string(),
        idempotency_key: idempotency_key.map(|s| s.to_string()),
        confirm_target: confirm_target.map(|s| s.to_string()),
    };
    // PR-013: a failed send must not leak the pending entry — remove it before
    // returning so a later timeout/cancel can never fire into a stale channel.
    if session.sender.send(frame).is_err() {
        session.pending.remove(&id);
        return Err(anyhow!("relay session closed while dispatching"));
    }
    let result = tokio::time::timeout(timeout, rx.recv_async())
        .await
        .map_err(|_| {
            session.pending.remove(&id);
            anyhow!("relay command timed out")
        })?
        .map_err(|_| {
            session.pending.remove(&id);
            anyhow!("relay session closed awaiting result")
        })?;
    session.pending.remove(&id);
    match result {
        RelayFrame::CommandResult { status, body, .. } => Ok(RelayResponse {
            status,
            body: body.map(|b| b.into_bytes()),
        }),
        other => Err(anyhow!("unexpected relay frame: {other:?}")),
    }
}

/// Deep-002: the installation identity bound to a verified relay client
/// certificate. Extracted from the certificate's `actae-relay://<install_id>`
/// URI SAN at the mTLS edge and injected per-request. The handler refuses a
/// certificate that is not bound to the requested `{instance_id}`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelayInstallId(pub String);

/// The gateway's relay connect endpoint: the instance dials OUT to
/// `wss://gateway/v1/relay/instances/{instance_id}/connect`.
pub async fn handle_relay_connect(
    State(state): State<Arc<GatewayState>>,
    Path(instance_id): Path<String>,
    ws: WebSocketUpgrade,
    axum::extract::Extension(rid): axum::extract::Extension<String>,
    mls_install: Option<axum::extract::Extension<RelayInstallId>>,
) -> impl IntoResponse {
    // Deep-002: when the relay mTLS edge verified a client certificate, the
    // certificate's installation identity MUST match the requested
    // instance_id. A valid cert bound to a different installation is rejected
    // (defense-in-depth on top of the edge-level SAN check).
    if let Some(axum::extract::Extension(install)) = mls_install {
        if install.0 != instance_id {
            return super::err_json(
                axum::http::StatusCode::FORBIDDEN,
                "client certificate is not bound to this installation",
                &rid,
            )
            .into_response();
        }
    }
    // PR-012: the credential is derived per instance (`relay_credential_for`),
    // so a leaked credential for one instance cannot impersonate another. If
    // relay is disabled the endpoint is indistinguishable from missing.
    let expected = match state.config.relay_credential_for(&instance_id) {
        Some(c) => c,
        None => {
            return super::err_json(
                axum::http::StatusCode::NOT_FOUND,
                "relay not configured",
                &rid,
            )
            .into_response()
        }
    };
    ws.on_upgrade(move |socket| async move {
        let _ = run_relay_session_loop(&state, &instance_id, socket, &expected).await;
    })
}

async fn run_relay_session_loop(
    state: &GatewayState,
    instance_id: &str,
    socket: axum::extract::ws::WebSocket,
    expected_credential: &str,
) -> Result<()> {
    use futures_util::{SinkExt, StreamExt};

    let (mut sender, mut receiver) = socket.split();
    // Expect a handshake first (version + credential).
    let handshake = match tokio::time::timeout(state.config.connect_timeout, receiver.next()).await
    {
        Ok(Some(Ok(msg))) => msg,
        _ => return Err(anyhow!("relay handshake timeout/closed")),
    };
    let text = handshake
        .into_text()
        .map_err(|_| anyhow!("relay handshake not text"))?;
    let frame = RelayFrame::from_json(&text)?;
    let RelayFrame::Handshake {
        version,
        instance_id: their_id,
        credential,
    } = frame
    else {
        return Err(anyhow!("expected relay_handshake"));
    };
    if their_id != instance_id {
        let _ = sender
            .send(axum::extract::ws::Message::text(frame_text(
                &RelayFrame::HandshakeError {
                    code: "instance_mismatch".into(),
                    message: "instance id mismatch".into(),
                },
            )?))
            .await;
        return Ok(());
    }
    if version != RELAY_PROTOCOL_VERSION {
        let _ = sender
            .send(axum::extract::ws::Message::text(frame_text(
                &RelayFrame::HandshakeError {
                    code: "version_mismatch".into(),
                    message: format!("unsupported relay version {version}"),
                },
            )?))
            .await;
        return Ok(());
    }
    if credential != expected_credential {
        let _ = sender
            .send(axum::extract::ws::Message::text(frame_text(
                &RelayFrame::HandshakeError {
                    code: "unauthorized".into(),
                    message: "invalid relay credential".into(),
                },
            )?))
            .await;
        return Ok(());
    }

    let (relay_tx, relay_rx) = flume::bounded(1024);
    let pending: Arc<DashMap<String, flume::Sender<RelayFrame>>> = Arc::new(DashMap::new());
    let session_id = format!("rel_{}", uuid::Uuid::new_v4().simple());
    let generation = state.relay_registry.fence(
        instance_id,
        RelaySession {
            sender: relay_tx,
            pending: pending.clone(),
            fencing_generation: 0,
            session_id: session_id.clone(),
        },
    );
    super::metrics::GATEWAY_RELAY_SESSIONS.set(state.relay_registry.len() as i64);
    let _ = sender
        .send(axum::extract::ws::Message::text(frame_text(
            &RelayFrame::HandshakeOk {
                session_id: session_id.clone(),
                fencing_generation: generation,
            },
        )?))
        .await;
    tracing::info!(
        instance_id,
        fencing_generation = generation,
        "relay session accepted"
    );

    // Forward outbound relay frames (commands) to the WS socket.
    let relay_reader = relay_rx;
    let mut heartbeat = tokio::time::interval(state.config.relay_heartbeat_interval);
    let mut last_seen = std::time::Instant::now();
    loop {
        tokio::select! {
            frame = relay_reader.recv_async() => {
                match frame {
                    Ok(RelayFrame::Close { reason }) => {
                        let _ = sender.send(axum::extract::ws::Message::text(frame_text(&RelayFrame::Close { reason })?)).await;
                        break;
                    }
                    Ok(f) => {
                        let bytes = f.as_json_bytes()?;
                        let text = String::from_utf8_lossy(&bytes).to_string();
                        if sender
                            .send(axum::extract::ws::Message::text(text))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            incoming = receiver.next() => {
                match incoming {
                    Some(Ok(msg)) => {
                        let text = match msg.into_text() {
                            Ok(t) => t,
                            Err(_) => continue,
                        };
                        last_seen = std::time::Instant::now();
                        let frame = match RelayFrame::from_json(&text) {
                            Ok(f) => f,
                            Err(_) => continue,
                        };
                        match frame {
                            RelayFrame::Heartbeat => {}
                            RelayFrame::CommandResult { id, status, body } => {
                                if let Some(tx) = pending.get(&id) {
                                    let _ = tx.send(RelayFrame::CommandResult { id, status, body });
                                }
                            }
                            RelayFrame::Handshake { .. } | RelayFrame::HandshakeOk { .. }
                            | RelayFrame::HandshakeError { .. } | RelayFrame::Command { .. }
                            | RelayFrame::Close { .. } => {
                                tracing::warn!("relay: unexpected frame from instance; ignoring");
                            }
                        }
                    }
                    Some(Err(_)) | None => break,
                }
            }
            _ = heartbeat.tick() => {
                if last_seen.elapsed() > state.config.relay_heartbeat_timeout {
                    tracing::warn!(instance_id, "relay session heartbeat timed out");
                    break;
                }
                let hb = RelayFrame::Heartbeat;
                let _ = sender.send(axum::extract::ws::Message::text(frame_text(&hb)?)).await;
            }
        }
    }
    // PR-013: compare-and-remove — only delete the registry entry if THIS
    // session is still the registered one (a fenced replacement must not be
    // deleted by the old session's exit).
    state
        .relay_registry
        .remove_if_current(instance_id, &session_id);
    super::metrics::GATEWAY_RELAY_SESSIONS.set(state.relay_registry.len() as i64);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn relay_registry_fences() {
        let reg = RelayRegistry::new();
        let (tx, _rx) = flume::bounded(4);
        let pending = Arc::new(DashMap::new());
        let s1 = RelaySession {
            sender: tx,
            pending: pending.clone(),
            fencing_generation: 0,
            session_id: "s1".into(),
        };
        let g1 = reg.fence("ins_1", s1);
        let (tx2, _rx2) = flume::bounded(4);
        let s2 = RelaySession {
            sender: tx2,
            pending: pending.clone(),
            fencing_generation: 0,
            session_id: "s2".into(),
        };
        let g2 = reg.fence("ins_1", s2);
        assert!(g2 > g1);
        assert_eq!(reg.len(), 1);
        assert!(reg.get("ins_1").is_some());
        assert!(reg.get("ins_other").is_none());
    }

    #[test]
    fn fenced_session_cannot_delete_replacement() {
        // PR-013: when session s1 is fenced by s2, s1's exit must NOT remove
        // the registry entry (compare-and-remove on session id).
        let reg = RelayRegistry::new();
        let pending = Arc::new(DashMap::new());
        let (tx1, _rx1) = flume::bounded(4);
        reg.fence(
            "ins_1",
            RelaySession {
                sender: tx1,
                pending: pending.clone(),
                fencing_generation: 0,
                session_id: "s1".into(),
            },
        );
        let (tx2, _rx2) = flume::bounded(4);
        reg.fence(
            "ins_1",
            RelaySession {
                sender: tx2,
                pending: pending.clone(),
                fencing_generation: 0,
                session_id: "s2".into(),
            },
        );
        // The fenced s1 exits: must NOT remove s2's entry.
        assert!(!reg.remove_if_current("ins_1", "s1"));
        assert_eq!(reg.len(), 1);
        assert!(reg.get("ins_1").is_some());
        // The current s2 exits: removes the entry.
        assert!(reg.remove_if_current("ins_1", "s2"));
        assert_eq!(reg.len(), 0);
    }

    #[tokio::test]
    async fn relay_command_roundtrip() {
        let (tx, rx) = flume::bounded(8);
        let pending = Arc::new(DashMap::new());
        let session = RelaySession {
            sender: tx.clone(),
            pending: pending.clone(),
            fencing_generation: 1,
            session_id: "s".into(),
        };
        // Spawn a fake instance-side reader that replies.
        let pending2 = pending.clone();
        let handle = tokio::spawn(async move {
            while let Ok(frame) = rx.recv_async().await {
                if let RelayFrame::Command { id, .. } = frame {
                    let _ = pending2.get(&id).and_then(|e| {
                        e.send(RelayFrame::CommandResult {
                            id,
                            status: 201,
                            body: Some("{\"ok\":true}".into()),
                        })
                        .ok()
                    });
                }
            }
        });
        let resp = relay_command(
            &session,
            "req-1",
            "POST",
            "/api/v1/events/record",
            Some(b"{}"),
            "tok",
            Duration::from_secs(2),
            Some("idem-1"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(resp.status, 201);
        assert_eq!(resp.body.as_deref(), Some(b"{\"ok\":true}".as_slice()));
        handle.abort();
    }
}
