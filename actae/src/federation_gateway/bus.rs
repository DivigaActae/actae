// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Live frame bus (Phase 5).
//!
//! Transient live frames only — no persistence. The default build uses an
//! in-memory broadcast bus; the `gateway-nats` feature swaps in a NATS Core
//! transport (opaque hashed instance subjects, bounded frame size, no
//! JetStream). Deduplication by `(instance_id, event_id)` is the connector's
//! job (D-010).

use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;

/// A bounded frame for the live bus.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LiveFrame {
    pub instance_id: String,
    pub event_id: String,
    pub payload: Bytes,
    pub timestamp: i64,
}

/// Live-bus transport. `subject` is an opaque hashed instance subject; the
/// payload is a bounded transient frame.
pub trait LiveBus: Send + Sync + 'static {
    fn publish(&self, subject: &str, frame: LiveFrame);
    /// Subscribe to a subject; returns a receiver of live frames.
    fn subscribe(&self, subject: &str) -> flume::Receiver<LiveFrame>;
}

/// In-memory broadcast bus (default/test transport). One publisher per
/// subject, many subscribers; full subscriber queues drop the oldest frame
/// (bounded, documented — the fleet stream handles lag via its own queue).
pub struct InMemoryBus {
    subjects: Arc<DashMap<String, Vec<flume::Sender<LiveFrame>>>>,
    max_subscribers_per_subject: usize,
}

impl InMemoryBus {
    pub fn new() -> Self {
        Self {
            subjects: Arc::new(DashMap::new()),
            max_subscribers_per_subject: 64,
        }
    }

    /// Cleanup dead subscriber channels on a best-effort basis.
    fn prune(&self, subject: &str) {
        if let Some(mut senders) = self.subjects.get_mut(subject) {
            senders.retain(|s| !s.is_disconnected());
        }
    }
}

impl Default for InMemoryBus {
    fn default() -> Self {
        Self::new()
    }
}

impl LiveBus for InMemoryBus {
    fn publish(&self, subject: &str, frame: LiveFrame) {
        self.prune(subject);
        if let Some(senders) = self.subjects.get(subject) {
            for sender in senders.iter() {
                // Bounded queues: drop the newest frame rather than blocking
                // the publisher (slow subscribers see a documented lag/drop).
                let _ = sender.try_send(frame.clone());
            }
        }
    }

    fn subscribe(&self, subject: &str) -> flume::Receiver<LiveFrame> {
        let (tx, rx) = flume::bounded(1024);
        {
            let mut senders = self.subjects.entry(subject.to_string()).or_default();
            if senders.len() >= self.max_subscribers_per_subject {
                senders.remove(0);
            }
            senders.push(tx);
        }
        rx
    }
}

/// NATS Core transport (feature `gateway-nats`): transient publish/subscribe
/// on opaque hashed instance subjects. JetStream is never enabled for payload
/// subjects (D-006).
///
/// PR-011 hardening:
/// * TLS/mTLS — CA bundle + optional client cert/key (server trust + client
///   identity; auth rotation is the operator's NATS config);
/// * bounded publish work — one worker task drains a bounded queue instead of
///   spawning an unbounded task per frame;
/// * frame-size limit — oversized frames are rejected before publish and
///   after receive;
/// * non-blocking subscribe — a full subscriber queue drops the newest frame
///   (documented lag) instead of blocking the NATS reader task.
#[cfg(feature = "gateway-nats")]
pub struct NatsBus {
    client: async_nats::Client,
    subject_prefix: String,
    max_frame_bytes: usize,
    /// Bounded publish queue (PR-011): frames are serialized through one
    /// worker task; a full queue is dropped (documented lag) rather than
    /// spawning an unbounded task per frame.
    publish_tx: flume::Sender<(String, Bytes)>,
}

/// NATS connection security config (PR-011).
#[cfg(feature = "gateway-nats")]
pub struct NatsTls {
    pub ca_bundle_path: Option<String>,
    pub client_cert_path: Option<String>,
    pub client_key_path: Option<String>,
    pub max_frame_bytes: usize,
}

#[cfg(feature = "gateway-nats")]
impl NatsBus {
    pub async fn connect(
        servers: &str,
        subject_prefix: &str,
        tls: Option<NatsTls>,
    ) -> anyhow::Result<Self> {
        let mut opts = async_nats::ConnectOptions::new().name(format!(
            "actae-federation-gateway/{}",
            env!("CARGO_PKG_VERSION")
        ));
        let max_frame_bytes = tls
            .as_ref()
            .map(|t| t.max_frame_bytes)
            .unwrap_or(1024 * 1024);
        if let Some(tls) = tls {
            // TLS server trust (CA bundle); when a client cert/key are given,
            // present the gateway's own identity (mTLS workload identity).
            if let Some(ca) = &tls.ca_bundle_path {
                opts = opts.add_root_certificates(ca.as_str().into());
            }
            if let (Some(cert), Some(key)) = (&tls.client_cert_path, &tls.client_key_path) {
                opts = opts.add_client_certificate(cert.as_str().into(), key.as_str().into());
            }
        }
        let client = opts
            .connect(servers)
            .await
            .map_err(|e| anyhow::anyhow!("NATS connect failed: {e}"))?;
        let (publish_tx, publish_rx) = flume::bounded::<(String, Bytes)>(4096);
        let subject_prefix = subject_prefix.trim_end_matches('.').to_string();
        // Bounded publish worker (PR-011): serializes NATS publishes so a
        // frame flood cannot spawn unbounded tasks or saturate the event loop.
        let worker_client = client.clone();
        tokio::spawn(async move {
            while let Ok((subject, payload)) = publish_rx.recv_async().await {
                let _ = worker_client.publish(subject, payload.into()).await;
            }
        });
        Ok(Self {
            client,
            subject_prefix,
            max_frame_bytes,
            publish_tx,
        })
    }
}

#[cfg(feature = "gateway-nats")]
impl LiveBus for NatsBus {
    fn publish(&self, subject: &str, frame: LiveFrame) {
        let bytes = match serde_json::to_vec(&frame) {
            Ok(b) => b,
            Err(_) => return,
        };
        // PR-011: reject oversized frames before publish.
        if bytes.len() > self.max_frame_bytes {
            return;
        }
        let full = format!("{}.{}", self.subject_prefix, subject);
        let _ = self.publish_tx.try_send((full, Bytes::from(bytes)));
    }

    fn subscribe(&self, subject: &str) -> flume::Receiver<LiveFrame> {
        let prefix = self.subject_prefix.clone();
        let subject = subject.to_string();
        let client = self.client.clone();
        let max_frame_bytes = self.max_frame_bytes;
        let (tx, rx) = flume::bounded(1024);
        tokio::spawn(async move {
            use futures_util::StreamExt;
            let full = format!("{}.{}", prefix, subject);
            let sub = match client.subscribe(full.clone()).await {
                Ok(s) => s,
                Err(_) => return,
            };
            let mut inbox = sub;
            while let Some(msg) = inbox.next().await {
                // PR-011: reject oversized frames after receive.
                if msg.payload.len() > max_frame_bytes {
                    continue;
                }
                if let Ok(frame) = serde_json::from_slice::<LiveFrame>(&msg.payload) {
                    // PR-011: non-blocking — a full queue drops the newest
                    // frame (documented lag) instead of stalling the reader.
                    if tx.try_send(frame).is_err() {
                        continue;
                    }
                }
            }
        });
        rx
    }
}

/// Opaque hashed subject for an (org, instance) pair — no readable tenant or
/// channel names on the wire (section 8).
pub fn hashed_subject(org: &str, instance_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"actae:fleet:live:v1:");
    h.update(org.as_bytes());
    h.update(b":");
    h.update(instance_id.as_bytes());
    hex::encode(h.finalize())[..24].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_bus_broadcasts() {
        let bus = InMemoryBus::new();
        let subject = hashed_subject("org_1", "ins_1");
        let rx = bus.subscribe(&subject);
        let frame = LiveFrame {
            instance_id: "ins_1".into(),
            event_id: "evt-1".into(),
            payload: Bytes::from_static(b"{\"type\":\"broadcast\"}"),
            timestamp: 1,
        };
        bus.publish(&subject, frame.clone());
        let got = rx.try_recv().expect("frame delivered");
        assert_eq!(got.event_id, "evt-1");
        assert_eq!(got.instance_id, "ins_1");
    }

    #[test]
    fn hashed_subject_is_opaque_and_stable() {
        let a = hashed_subject("org_1", "ins_1");
        let b = hashed_subject("org_1", "ins_1");
        assert_eq!(a, b);
        assert_ne!(a, hashed_subject("org_2", "ins_1"));
        // 24 hex chars; no tenant/channel names leak.
        assert_eq!(a.len(), 24);
        assert!(!a.contains("org") && !a.contains("ins"));
    }

    #[test]
    fn slow_subscriber_drops_newest_not_block() {
        let bus = InMemoryBus::new();
        let subject = "s";
        let rx = bus.subscribe(subject);
        for i in 0..2000 {
            bus.publish(
                subject,
                LiveFrame {
                    instance_id: "i".into(),
                    event_id: format!("e{i}"),
                    payload: Bytes::new(),
                    timestamp: i,
                },
            );
        }
        // The channel is bounded (1024) and publish uses try_send — the
        // publisher never blocks and the subscriber can drain.
        let mut seen = 0;
        while rx.try_recv().is_ok() {
            seen += 1;
        }
        assert!(seen > 0);
        assert!(seen <= 1024);
    }
}
