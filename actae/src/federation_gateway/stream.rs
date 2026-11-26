// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Hosted live stream (Phase 5).
//!
//! The fleet stream is a live subscription to the canonical `/ws` of one
//! authorized instance: the gateway connects to the instance's canonical
//! WebSocket with an `instance_observation` token, subscribes to the ticket's
//! channels, and forwards live frames to browser bridges (fleet envelope)
//! while publishing them to the live bus (in-memory or NATS Core).
//! Deduplication by `(instance_id, event_id)`; heartbeat; graceful drain.
//! No second WS endpoint is created on the instance — the canonical `/ws` is
//! reused (D-001).
//!
//! Deep-003: exactly ONE upstream connector is shared per instance. Browser
//! bridges are fan-out subscribers over a `tokio::sync::broadcast` channel
//! instead of each spawning their own upstream source. The connector owns the
//! fenced lease (acquired on connect, renewed via the SaaS `renew` endpoint
//! inside the read loop, conditionally released on exit), one persistent
//! dedupe ring, and the resume cursor. Renewal failure / fencing is fed into
//! the live I/O wait via `tokio::select!` so forwarding stops before a
//! terminal typed error is emitted. The connector is torn down (cancelled,
//! awaited, lease released, map entry removed) when the last subscriber
//! leaves.

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use sonic_rs::JsonValueTrait;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use super::bus::{LiveBus, LiveFrame};
use super::saas::SaasInternalClient;

/// Converts an instance base URL to its `/ws` URL.
pub fn ws_url_for(base_url: &str) -> Result<String> {
    let base = base_url.trim_end_matches('/');
    let ws = if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        return Err(anyhow!("instance base URL must be http(s)://"));
    };
    Ok(format!("{ws}/ws"))
}

/// Bounded dedupe ring keyed by (instance_id, event_id).
#[derive(Debug, Clone)]
pub struct SeenEvents {
    seen: std::collections::VecDeque<String>,
    set: std::collections::HashSet<String>,
    cap: usize,
}

impl SeenEvents {
    pub fn new(cap: usize) -> Self {
        Self {
            seen: std::collections::VecDeque::new(),
            set: std::collections::HashSet::new(),
            cap: cap.max(1),
        }
    }

    /// Returns `true` if `key` is new (and records it); `false` if it was
    /// already seen (duplicate).
    pub fn insert(&mut self, key: String) -> bool {
        if self.set.contains(&key) {
            return false;
        }
        if self.set.len() >= self.cap {
            if let Some(old) = self.seen.pop_front() {
                self.set.remove(&old);
            }
        }
        self.set.insert(key.clone());
        self.seen.push_back(key);
        true
    }
}

/// Extract the canonical event object + event id from a canonical WS frame.
///
/// The canonical server has two supported broadcast encodings on the wire:
/// the legacy externally-tagged `{"Broadcast": {..}}` representation and
/// the current type-tagged `{"type":"broadcast", ..}` representation used
/// by persisted HTTP records. Both carry the event at `payload.event`.
/// Treating the latter as a generic control frame makes direct browser fan-out
/// appear to work while silently bypassing `LiveBus::publish` (and therefore
/// NATS), so keep both forms explicit here.
///
/// Returns `None` for non-event frames (subscriptions, cursor_sync,
/// connection, pings).
pub fn extract_event(frame: &sonic_rs::Value) -> Option<(String, sonic_rs::Value)> {
    let payload = if let Some(broadcast) = frame
        .get("Broadcast")
        .or_else(|| frame.get("broadcast").filter(|v| v.is_object()))
    {
        broadcast.get("payload")?
    } else if frame.get("type").and_then(|v| v.as_str()) == Some("broadcast") {
        frame.get("payload")?
    } else {
        return None;
    };
    let event = payload.get("event")?;
    let id = event.get("id").and_then(|v| v.as_str())?;
    Some((id.to_string(), event.clone()))
}

/// Build the auth frame for the canonical WS.
pub fn auth_frame(token: &str) -> Bytes {
    let json = sonic_rs::json!({"type": "auth", "token": token, "payload": {}});
    Bytes::from(sonic_rs::to_string(&json).unwrap_or_default())
}

/// Build the `subscribe_many` frame for the canonical WS.
pub fn subscribe_many_frame(channels: &[String], resume_cursor: Option<i64>) -> Bytes {
    let topics: Vec<sonic_rs::Value> = channels
        .iter()
        .map(|c| sonic_rs::json!({"topic": c, "cursor": resume_cursor}))
        .collect();
    let json = sonic_rs::json!({"type": "subscribe_many", "topics": topics});
    Bytes::from(sonic_rs::to_string(&json).unwrap_or_default())
}

/// A typed lag/control frame emitted when a subscriber falls behind (PR-010):
/// carries the safe resume cursor so a reconnecting bridge can catch up
/// without a permanent gap. The subscriber closes rather than silently
/// dropping frames.
pub fn lag_frame(resume_cursor: Option<i64>) -> Bytes {
    let json = sonic_rs::json!({
        "type": "fleet_lag",
        "resume_cursor": resume_cursor,
    });
    Bytes::from(sonic_rs::to_string(&json).unwrap_or_default())
}

/// Config for a single hosted stream session.
#[derive(Debug, Clone)]
pub struct StreamSession {
    pub instance_id: String,
    pub instance_name: String,
    pub base_url: String,
    pub observation_token: String,
    pub channels: Vec<String>,
    /// Cursor to pass to every granted topic on initial subscribe. The
    /// gateway deliberately accepts one conservative cursor; callers that
    /// need per-topic precision mint separate tickets.
    pub resume_cursor: Option<i64>,
    pub bus_subject: String,
    /// Heartbeat interval for the browser bridge.
    pub heartbeat_interval: Duration,
    /// PR-010: gateway workload identity for the fenced connector lease.
    pub gateway_id: String,
    /// Organization ID for re-minting observation tokens on reconnect.
    pub org: String,
    /// SaaS client for re-minting observation tokens on reconnect.
    pub saas: Option<Arc<SaasInternalClient>>,
    /// Lease renewal interval (Deep-003): the connector renews inside the read
    /// loop at this cadence; a renewal failure fences the connector.
    pub renew_every: Duration,
}

/// Outcome of one upstream connection attempt.
struct ConnectionOutcome {
    /// The highest event cursor observed on this connection — used as the
    /// catch-up resume cursor on reconnect (no permanent gap, bounded
    /// duplicates).
    last_cursor: Option<i64>,
    /// `true` when the connection was terminated because the lease renewal
    /// failed (fenced by a takeover). The connector must stop immediately and
    /// emit a terminal typed lag frame.
    fenced: bool,
}

/// A fenced connector lease (PR-010/Deep-003). Acquired before connecting,
/// renewed inside the live read loop via the dedicated SaaS `renew` endpoint
/// (409 ⇒ fenced by a takeover), and conditionally released on connector exit
/// with the last observed fencing generation so a stale owner can never erase
/// a replacement's lease.
pub struct LeaseHandle {
    saas: Arc<SaasInternalClient>,
    instance_id: String,
    gateway_id: String,
    renew_every: Duration,
    ttl_secs: u64,
    /// True while this gateway is the current lease holder.
    held: AtomicBool,
    /// Last observed fencing generation — required for a conditional release.
    generation: AtomicU64,
}

impl LeaseHandle {
    pub fn new(
        saas: Arc<SaasInternalClient>,
        instance_id: String,
        gateway_id: String,
        renew_every: Duration,
    ) -> Self {
        // Lease TTL = renew interval + 50% safety margin, minimum 30s.
        let ttl_secs = ((renew_every.as_secs_f64() * 1.5) as u64).max(30);
        Self {
            saas,
            instance_id,
            gateway_id,
            renew_every,
            ttl_secs,
            held: AtomicBool::new(false),
            generation: AtomicU64::new(0),
        }
    }

    pub fn renew_every(&self) -> Duration {
        self.renew_every
    }

    /// Acquire (or re-acquire) the lease before a connection attempt.
    async fn acquire(&self) -> Result<()> {
        let info = self
            .saas
            .acquire_lease(&self.instance_id, &self.gateway_id, self.ttl_secs)
            .await?;
        self.generation
            .store(info.fencing_generation, Ordering::SeqCst);
        self.held.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Renew the lease. Returns `Err` when a takeover fenced us (the SaaS
    /// `renew` endpoint returns 409) — the caller MUST stop forwarding.
    async fn renew(&self) -> Result<()> {
        let info = self
            .saas
            .renew_lease(&self.instance_id, &self.gateway_id, self.ttl_secs)
            .await?;
        self.generation
            .store(info.fencing_generation, Ordering::SeqCst);
        Ok(())
    }

    /// Conditional release: only when this gateway + generation still holds the
    /// lease. A stale owner's release is a no-op on both sides (the SaaS
    /// returns 409 and we ignore it). Idempotent.
    async fn release(&self) {
        if self.held.load(Ordering::SeqCst) {
            let _ = self
                .saas
                .release_lease(
                    &self.instance_id,
                    &self.gateway_id,
                    self.generation.load(Ordering::SeqCst),
                )
                .await;
            self.held.store(false, Ordering::SeqCst);
        }
    }
}

/// Shared connector handle (Deep-003). Exactly ONE exists per instance. Browser
/// bridges hold a `broadcast::Receiver` via `subscribe()` and MUST call
/// `release()` when they disconnect; the connector is torn down when the last
/// subscriber leaves.
pub struct ConnectorHandle {
    pub instance_id: String,
    /// Shared live fan-out: the connector broadcasts every frame here; each
    /// subscriber clones a receiver.
    frames: broadcast::Sender<LiveFrame>,
    /// Set when the connector has reached a terminal state (fenced, retry
    /// exhausted, cancelled). A new subscriber must spawn a fresh connector.
    done: Arc<AtomicBool>,
    /// Cancels the connector task when the last subscriber leaves.
    cancel: CancellationToken,
    /// Connector task join handle (set after spawn; drained on last release).
    connector_task: Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// Active subscriber count; drives teardown at zero.
    subscribers: Arc<AtomicUsize>,
    /// Latest observed cursor — used for subscriber-side `fleet_lag` frames
    /// when a broadcast consumer falls behind. `-1` = none yet.
    last_cursor: Arc<AtomicI64>,
}

impl ConnectorHandle {
    /// Register a new subscriber; returns a broadcast receiver. Callers MUST
    /// call `release()` when the subscriber disconnects.
    pub fn subscribe(&self) -> broadcast::Receiver<LiveFrame> {
        self.subscribers.fetch_add(1, Ordering::SeqCst);
        self.frames.subscribe()
    }

    /// Called when a subscriber disconnects. When the LAST subscriber leaves,
    /// the connector is cancelled, awaited, its lease released, and the map
    /// entry removed — no renewal task or upstream source leaks.
    pub fn release(&self) {
        let prev = self.subscribers.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
            self.cancel.cancel();
            if let Some(task) = self.connector_task.lock().unwrap().take() {
                // Drain the task in the background so a disconnect never blocks
                // the bridge; the task exits promptly on cancellation and runs
                // its lease-release + map-removal cleanup.
                tokio::spawn(async move {
                    let _ = task.await;
                });
            }
        }
    }

    pub fn is_done(&self) -> bool {
        self.done.load(Ordering::SeqCst)
    }

    fn done_flag(&self) -> Arc<AtomicBool> {
        self.done.clone()
    }

    pub fn last_cursor(&self) -> Arc<AtomicI64> {
        self.last_cursor.clone()
    }

    #[allow(dead_code)]
    fn shutdown(&self) {
        self.cancel.cancel();
    }
}

/// Spawn the shared upstream connector for an instance (Deep-003). Returns the
/// handle; the caller registers it in the per-instance map. The connector task
/// owns the lease, dedupe ring, resume cursor and live fan-out.
pub fn spawn_connector(
    session: StreamSession,
    bus: Arc<dyn LiveBus>,
    connectors: Arc<dashmap::DashMap<String, Arc<ConnectorHandle>>>,
) -> Arc<ConnectorHandle> {
    let (frames_tx, _) = broadcast::channel(4096);
    let done = Arc::new(AtomicBool::new(false));
    let cancel = CancellationToken::new();
    let last_cursor = Arc::new(AtomicI64::new(session.resume_cursor.unwrap_or_default()));
    let subscribers = Arc::new(AtomicUsize::new(0));
    let instance_id = session.instance_id.clone();

    let task = tokio::spawn(run_connector_task(
        session,
        bus,
        frames_tx.clone(),
        done.clone(),
        cancel.clone(),
        last_cursor.clone(),
        connectors.clone(),
    ));

    Arc::new(ConnectorHandle {
        instance_id,
        frames: frames_tx,
        done,
        cancel,
        connector_task: Arc::new(std::sync::Mutex::new(Some(task))),
        subscribers,
        last_cursor,
    })
}

/// Terminal cleanup shared by every connector exit path: mark done, remove this
/// connector from the per-instance map (only if it is still the registered
/// entry — a replacement spawned after a takeover must not be removed), and
/// release the fenced lease conditionally. `lease` is released by the caller
/// (async) right before this runs.
fn mark_terminal(
    done: &Arc<AtomicBool>,
    connectors: &dashmap::DashMap<String, Arc<ConnectorHandle>>,
    key: &str,
) {
    done.store(true, Ordering::SeqCst);
    let my_flag = done.clone();
    connectors.remove_if(key, |_, h| Arc::ptr_eq(&h.done_flag(), &my_flag));
}

async fn run_connector_task(
    session: StreamSession,
    bus: Arc<dyn LiveBus>,
    frames: broadcast::Sender<LiveFrame>,
    done: Arc<AtomicBool>,
    cancel: CancellationToken,
    last_cursor: Arc<AtomicI64>,
    connectors: Arc<dashmap::DashMap<String, Arc<ConnectorHandle>>>,
) {
    let Some(saas) = session.saas.clone() else {
        tracing::error!(
            instance_id = %session.instance_id,
            "stream session is missing a SaaS client; connector cannot start"
        );
        mark_terminal(&done, &connectors, &session.instance_id);
        return;
    };
    let ws_url = match ws_url_for(&session.base_url) {
        Ok(u) => u,
        Err(e) => {
            tracing::error!(
                instance_id = %session.instance_id,
                %e,
                "invalid instance base URL; connector cannot start"
            );
            mark_terminal(&done, &connectors, &session.instance_id);
            return;
        }
    };

    let lease = LeaseHandle::new(
        saas.clone(),
        session.instance_id.clone(),
        session.gateway_id.clone(),
        session.renew_every,
    );

    // Persistent dedupe + resume cursor live across reconnects.
    let mut seen = SeenEvents::new(10_000);
    let mut resume_cursor = session.resume_cursor;
    let mut observation_token = session.observation_token.clone();
    let mut attempt: u32 = 0;
    const MAX_ATTEMPTS: u32 = 20;
    const BASE_BACKOFF: Duration = Duration::from_millis(250);

    loop {
        if cancel.is_cancelled() {
            break;
        }
        // Deep-003: acquire the fenced lease BEFORE connecting. A takeover or
        // renewal failure stops the connector immediately.
        if let Err(e) = lease.acquire().await {
            tracing::error!(
                instance_id = %session.instance_id,
                %e,
                "fleet connector lease acquire failed; stopping"
            );
            let _ = frames.send(LiveFrame {
                instance_id: session.instance_id.clone(),
                event_id: String::new(),
                payload: lag_frame(resume_cursor),
                timestamp: chrono::Utc::now().timestamp(),
            });
            break;
        }
        let outcome = run_connection(
            &session,
            &bus,
            &frames,
            &mut seen,
            resume_cursor,
            &ws_url,
            &mut observation_token,
            &lease,
            &cancel,
            &last_cursor,
        )
        .await;
        match &outcome {
            Ok(o) => {
                if let Some(c) = o.last_cursor {
                    resume_cursor = Some(c);
                    last_cursor.store(c, Ordering::SeqCst);
                }
                if o.fenced {
                    tracing::warn!(
                        instance_id = %session.instance_id,
                        "fleet connector lease fenced; stopping"
                    );
                    let _ = frames.send(LiveFrame {
                        instance_id: session.instance_id.clone(),
                        event_id: String::new(),
                        payload: lag_frame(resume_cursor),
                        timestamp: chrono::Utc::now().timestamp(),
                    });
                    break;
                }
            }
            Err(e) => {
                tracing::warn!(
                    instance_id = %session.instance_id,
                    attempt,
                    %e,
                    "fleet upstream connector attempt failed"
                );
            }
        }
        attempt += 1;
        if attempt >= MAX_ATTEMPTS {
            tracing::error!(
                instance_id = %session.instance_id,
                "fleet upstream connector exhausted its retry budget"
            );
            let _ = frames.send(LiveFrame {
                instance_id: session.instance_id.clone(),
                event_id: String::new(),
                payload: lag_frame(resume_cursor),
                timestamp: chrono::Utc::now().timestamp(),
            });
            break;
        }
        if cancel.is_cancelled() {
            break;
        }
        // Re-mint the observation token before the next reconnect attempt so
        // an expired token doesn't cause repeated auth failures.
        if let Some(saas) = &session.saas {
            match super::GatewayState::observation_token_via(
                saas,
                &session.org,
                &session.instance_id,
                &session.channels,
            )
            .await
            {
                Ok(new_token) => {
                    observation_token = new_token;
                }
                Err(e) => {
                    tracing::warn!(
                        instance_id = %session.instance_id,
                        %e,
                        "observation token refresh failed on reconnect"
                    );
                    let _ = frames.send(LiveFrame {
                        instance_id: session.instance_id.clone(),
                        event_id: String::new(),
                        payload: lag_frame(resume_cursor),
                        timestamp: chrono::Utc::now().timestamp(),
                    });
                    break;
                }
            }
        }
        // Bounded exponential backoff with jitter (±25%), cancellation-aware.
        let exp = BASE_BACKOFF.saturating_mul(2u32.saturating_pow(attempt.min(10)));
        let jitter_ms = exp.as_millis() as u64 / 4;
        let delay = exp + Duration::from_millis(jitter_ms);
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = cancel.cancelled() => break,
        }
    }

    // Conditional lease release (idempotent; a fenced owner is a no-op).
    lease.release().await;
    mark_terminal(&done, &connectors, &session.instance_id);
}

/// One canonical `/ws` connection attempt: authenticate, subscribe (with
/// resume cursor for catch-up), forward event/control frames, and return the
/// last observed cursor. Renewal and cancellation are fed into the live I/O
/// wait (Deep-003): a renewal failure stops forwarding immediately and returns
/// a fenced outcome; cancellation returns a normal shutdown outcome.
#[allow(clippy::too_many_arguments, clippy::ptr_arg)]
async fn run_connection(
    session: &StreamSession,
    bus: &Arc<dyn LiveBus>,
    frames: &broadcast::Sender<LiveFrame>,
    seen: &mut SeenEvents,
    resume_cursor: Option<i64>,
    ws_url: &str,
    observation_token: &mut String,
    lease: &LeaseHandle,
    cancel: &CancellationToken,
    last_cursor_shared: &AtomicI64,
) -> Result<ConnectionOutcome> {
    use futures_util::{SinkExt, StreamExt};

    let (mut stream, _resp) =
        tokio_websockets::ClientBuilder::from_uri(ws_url.parse::<axum::http::Uri>()?)
            .connect()
            .await
            .context("canonical /ws connect failed")?;

    stream
        .send(tokio_websockets::Message::text(
            String::from_utf8_lossy(&auth_frame(observation_token)).to_string(),
        ))
        .await
        .context("auth frame send failed")?;

    let mut authed = false;
    let mut last_cursor: Option<i64> = resume_cursor;

    // Deep-003: renew the lease INSIDE the read loop at `renew_every`, so a
    // renewal failure / fencing is observed promptly rather than only between
    // reconnects. `biased` prioritises cancellation + renewal over new frames.
    let mut renew = tokio::time::interval(lease.renew_every());
    renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                return Ok(ConnectionOutcome { last_cursor, fenced: false });
            }
            _ = renew.tick() => {
                if let Err(e) = lease.renew().await {
                    tracing::warn!(
                        instance_id = %session.instance_id,
                        %e,
                        "fleet connector lease renewal failed; fenced"
                    );
                    let _ = frames.send(LiveFrame {
                        instance_id: session.instance_id.clone(),
                        event_id: String::new(),
                        payload: lag_frame(last_cursor),
                        timestamp: chrono::Utc::now().timestamp(),
                    });
                    return Ok(ConnectionOutcome { last_cursor, fenced: true });
                }
            }
            msg = stream.next() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => return Err(anyhow!("canonical /ws error: {e}")),
                    None => return Ok(ConnectionOutcome { last_cursor, fenced: false }),
                };
                let text = match msg.as_text() {
                    Some(t) => t.to_string(),
                    None => continue,
                };
                let value: sonic_rs::Value = match sonic_rs::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                // Wait for the authenticated event before subscribing.
                if !authed {
                    if value.get("Connection").is_some()
                        && value
                            .get("Connection")
                            .and_then(|c| c.get("event"))
                            .and_then(|e| e.as_str())
                            == Some("authenticated")
                    {
                        authed = true;
                        if let Err(e) = stream
                            .send(tokio_websockets::Message::text(
                                String::from_utf8_lossy(&subscribe_many_frame(
                                    &session.channels,
                                    resume_cursor,
                                ))
                                .to_string(),
                            ))
                            .await
                        {
                            return Err(anyhow!("subscribe_many send failed: {e}"));
                        }
                    }
                    continue;
                }

                if let Some((event_id, event)) = extract_event(&value) {
                    let key = format!("{}:{}", session.instance_id, event_id);
                    if !seen.insert(key) {
                        continue; // duplicate (documented; D-010 at-least-once)
                    }
                    if let Some(cursor) = event.get("cursor").and_then(|v| v.as_i64()) {
                        last_cursor = Some(cursor);
                        last_cursor_shared.store(cursor, Ordering::SeqCst);
                    }
                    let ts = event
                        .get("timestamp")
                        .and_then(|v| v.as_str())
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                        .map(|d| d.timestamp())
                        .unwrap_or_else(|| chrono::Utc::now().timestamp());
                    let frame = LiveFrame {
                        instance_id: session.instance_id.clone(),
                        event_id,
                        payload: Bytes::from(sonic_rs::to_string(&value).unwrap_or_default()),
                        timestamp: ts,
                    };
                    // Fan out to subscribers (bounded broadcast; a lagged
                    // subscriber receives a `fleet_lag` typed frame on its
                    // side). Also publish to the live bus (NATS/in-memory).
                    let _ = frames.send(frame.clone());
                    bus.publish(&session.bus_subject, frame);
                } else {
                    // Non-event frame: forward as a control frame
                    // (subscription confirmations, cursor_sync, pings, errors).
                    let frame = LiveFrame {
                        instance_id: session.instance_id.clone(),
                        event_id: String::new(),
                        payload: Bytes::from(text.into_bytes()),
                        timestamp: chrono::Utc::now().timestamp(),
                    };
                    let _ = frames.send(frame);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ws_url_conversion() {
        assert_eq!(
            ws_url_for("http://127.0.0.1:8002").unwrap(),
            "ws://127.0.0.1:8002/ws"
        );
        assert_eq!(
            ws_url_for("https://ins.example.com").unwrap(),
            "wss://ins.example.com/ws"
        );
        assert!(ws_url_for("ftp://x").is_err());
    }

    #[test]
    fn dedupe_ring() {
        let mut seen = SeenEvents::new(3);
        assert!(seen.insert("a".into()));
        assert!(seen.insert("b".into()));
        assert!(!seen.insert("a".into())); // duplicate
        assert!(seen.insert("c".into()));
        assert!(seen.insert("d".into())); // evicts "a"
        assert!(seen.insert("a".into())); // "a" was evicted -> new again
    }

    #[test]
    fn extract_event_from_broadcast() {
        let frame = sonic_rs::json!({
            "Broadcast": {
                "topic": "ch",
                "payload": {
                    "event": {"id": "evt-1", "channel_id": "ch", "type": "tool.x", "timestamp": "2026-01-01T00:00:00Z"}
                }
            }
        });
        let (id, event) = extract_event(&frame).expect("event extracted");
        assert_eq!(id, "evt-1");
        assert_eq!(event.get("channel_id").and_then(|v| v.as_str()), Some("ch"));
        // Non-event frames yield None.
        let conn = sonic_rs::json!({"Connection": {"event": "connected", "payload": {}}});
        assert!(extract_event(&conn).is_none());
    }

    #[test]
    fn extract_event_from_type_tagged_http_record_broadcast() {
        let frame = sonic_rs::json!({
            "type": "broadcast",
            "topic": "public.nats",
            "payload": {
                "event": {
                    "id": "evt-http-1",
                    "event_type": "tool.nats",
                    "payload": {"over": "nats"},
                    "cursor": 7
                }
            }
        });
        let (id, event) = extract_event(&frame).expect("type-tagged event");
        assert_eq!(id, "evt-http-1");
        assert_eq!(
            event.get("event_type").and_then(|v| v.as_str()),
            Some("tool.nats")
        );
        assert_eq!(event.get("cursor").and_then(|v| v.as_i64()), Some(7));
    }

    #[test]
    fn type_tagged_control_frame_is_not_an_event() {
        let frame = sonic_rs::json!({
            "type": "subscription",
            "topic": "public.nats",
            "event": "subscribed"
        });
        assert!(extract_event(&frame).is_none());
    }

    #[test]
    fn frames_are_wellformed() {
        let auth = String::from_utf8_lossy(&auth_frame("tok")).to_string();
        let v: sonic_rs::Value = sonic_rs::from_str(&auth).unwrap();
        assert_eq!(v.get("type").and_then(|x| x.as_str()), Some("auth"));
        let sub =
            String::from_utf8_lossy(&subscribe_many_frame(&["a".into(), "b".into()], Some(7)))
                .to_string();
        let v2: sonic_rs::Value = sonic_rs::from_str(&sub).unwrap();
        assert_eq!(
            v2.get("type").and_then(|x| x.as_str()),
            Some("subscribe_many")
        );
        assert!(v2.get("topics").is_some());
    }

    #[test]
    fn connector_handle_tracks_subscribers() {
        // Deep-003: subscriber count drives teardown — the handle is "done"
        // only after the last subscriber releases.
        let handle = Arc::new(ConnectorHandle {
            instance_id: "ins_1".into(),
            frames: broadcast::channel(8).0,
            done: Arc::new(AtomicBool::new(false)),
            cancel: CancellationToken::new(),
            connector_task: Arc::new(std::sync::Mutex::new(None)),
            subscribers: Arc::new(AtomicUsize::new(0)),
            last_cursor: Arc::new(AtomicI64::new(0)),
        });
        let r1 = handle.subscribe();
        let r2 = handle.subscribe();
        assert_eq!(handle.subscribers.load(Ordering::SeqCst), 2);
        handle.release();
        assert_eq!(handle.subscribers.load(Ordering::SeqCst), 1);
        assert!(!handle.is_done());
        handle.release();
        assert_eq!(handle.subscribers.load(Ordering::SeqCst), 0);
        // Dropping the receivers is harmless; release() is the contract.
        drop(r1);
        drop(r2);
    }

    #[test]
    fn mark_terminal_only_removes_own_entry() {
        // Deep-003: a stale connector must never remove a REPLACEMENT entry
        // from the map (identity-compare on the done flag).
        let map: dashmap::DashMap<String, Arc<ConnectorHandle>> = dashmap::DashMap::new();
        let old_done = Arc::new(AtomicBool::new(false));
        let new_done = Arc::new(AtomicBool::new(false));
        let mk = |done: Arc<AtomicBool>| {
            Arc::new(ConnectorHandle {
                instance_id: "ins_1".into(),
                frames: broadcast::channel(8).0,
                done,
                cancel: CancellationToken::new(),
                connector_task: Arc::new(std::sync::Mutex::new(None)),
                subscribers: Arc::new(AtomicUsize::new(0)),
                last_cursor: Arc::new(AtomicI64::new(0)),
            })
        };
        map.insert("ins_1".into(), mk(new_done.clone()));
        // The OLD connector exits and tries to remove its own entry — but the
        // map now holds the replacement, so nothing is removed.
        mark_terminal(&old_done, &map, "ins_1");
        assert!(map.contains_key("ins_1"));
        // The replacement's own terminal exit removes the entry.
        mark_terminal(&new_done, &map, "ins_1");
        assert!(!map.contains_key("ins_1"));
    }

    #[tokio::test]
    async fn lease_release_is_idempotent_and_conditional() {
        // Deep-003: releasing twice / releasing after a takeover is a no-op.
        // (Unit-level: no SaaS round-trip; we just verify the guard math.)
        let saas = Arc::new(
            SaasInternalClient::new(
                "http://saas:8004",
                "internal-token",
                Duration::from_secs(1),
                Duration::from_secs(2),
                None,
                None,
                None,
            )
            .unwrap(),
        );
        let lease = LeaseHandle::new(saas, "ins_1".into(), "gw-1".into(), Duration::from_secs(20));
        // Not held yet -> release is a no-op.
        lease.release().await;
        assert!(!lease.held.load(Ordering::SeqCst));
        // Simulate a successful acquire without a network round-trip.
        lease.held.store(true, Ordering::SeqCst);
        lease.generation.store(2, Ordering::SeqCst);
        lease.release().await; // SaaS unreachable in unit test -> release() tolerates Err
        assert!(!lease.held.load(Ordering::SeqCst));
    }
}
