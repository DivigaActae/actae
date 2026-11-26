// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Delivery tracker with at-least-once semantics.
//!
//! Tracks pending broadcast deliveries per connection. For each broadcast
//! event, a PendingDelivery entry is created for every subscriber. Entries
//! are removed on Ack, retried on transient failure, and moved to the
//! dead-letter queue after exhausting retries.
//!
//! Design follows patterns from stream-based consumer groups (pending-entry
//! lists) and Kafka consumer offset tracking.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::RwLock;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::metrics::basic_metrics;

// ── Configuration ──────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DeliveryConfig {
    pub max_retries: u32,
    pub base_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub retry_interval_ms: u64,
    pub dead_letter_enabled: bool,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            base_backoff_ms: 100,
            max_backoff_ms: 30_000,
            retry_interval_ms: 500,
            dead_letter_enabled: true,
        }
    }
}

// ── Pending delivery ──────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    Pending,
    Retrying,
    Acked,
    Nacked,
    Dead,
}

#[derive(Debug, Clone)]
pub struct PendingDelivery {
    pub delivery_id: Uuid,
    pub event_id: Uuid,
    pub channel_id: String,
    pub connection_id: Uuid,
    pub payload_json: String,
    pub attempts: u32,
    pub status: DeliveryStatus,
    pub created_at: Instant,
    pub last_attempt: Option<Instant>,
    pub next_attempt: Instant,
    pub last_error: Option<String>,
}

// ── Trait for DLQ integration ─────────────────────────────────────

pub trait DeadLetterSink: Send + Sync {
    fn enqueue(&self, delivery: &PendingDelivery);
}

// ── Delivery tracker ──────────────────────────────────────────────

pub struct DeliveryTracker {
    pending: Arc<DashMap<Uuid, PendingDelivery>>,
    connection_pending: Arc<DashMap<Uuid, Vec<Uuid>>>,
    config: DeliveryConfig,
    dlq: Arc<RwLock<Option<Arc<dyn DeadLetterSink>>>>,
}

impl DeliveryTracker {
    pub fn new(config: DeliveryConfig) -> Self {
        basic_metrics::BROADCAST_PENDING_COUNT.set(0);
        basic_metrics::BROADCAST_DLQ_SIZE.set(0);

        Self {
            pending: Arc::new(DashMap::new()),
            connection_pending: Arc::new(DashMap::new()),
            config,
            dlq: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn set_dlq(&self, sink: Arc<dyn DeadLetterSink>) {
        *self.dlq.write().await = Some(sink);
    }

    /// Enqueue a delivery attempt for a connection.
    /// Returns true if the delivery was successfully enqueued (flume send OK).
    pub fn enqueue(
        &self,
        delivery_id: Uuid,
        event_id: Uuid,
        channel_id: &str,
        connection_id: Uuid,
        payload_json: String,
        send_ok: bool,
    ) -> bool {
        let now = Instant::now();

        if send_ok {
            basic_metrics::BROADCAST_DELIVERED_TOTAL
                .with_label_values(&["first_try"])
                .inc();
            // Delivery sent on first attempt — still track it for Ack
            let entry = PendingDelivery {
                delivery_id,
                event_id,
                channel_id: channel_id.to_string(),
                connection_id,
                payload_json,
                attempts: 1,
                status: DeliveryStatus::Pending,
                created_at: now,
                last_attempt: Some(now),
                next_attempt: now + Duration::from_millis(self.config.retry_interval_ms),
                last_error: None,
            };
            self.pending.insert(delivery_id, entry);
            self.track_connection(connection_id, delivery_id);
            basic_metrics::BROADCAST_PENDING_COUNT.inc();
            true
        } else {
            basic_metrics::BROADCAST_DELIVERED_TOTAL
                .with_label_values(&["failed_first"])
                .inc();
            // Flume send failed — schedule retry
            let entry = PendingDelivery {
                delivery_id,
                event_id,
                channel_id: channel_id.to_string(),
                connection_id,
                payload_json,
                attempts: 0,
                status: DeliveryStatus::Pending,
                created_at: now,
                last_attempt: None,
                next_attempt: now + Duration::from_millis(self.config.base_backoff_ms),
                last_error: Some("flume channel full".to_string()),
            };
            self.pending.insert(delivery_id, entry);
            self.track_connection(connection_id, delivery_id);
            basic_metrics::BROADCAST_PENDING_COUNT.inc();
            false
        }
    }

    /// Called when a subscriber acknowledges a delivery.
    pub fn ack(&self, delivery_id: Uuid) {
        if let Some(mut entry) = self.pending.get_mut(&delivery_id) {
            if entry.status == DeliveryStatus::Acked {
                return;
            }
            entry.status = DeliveryStatus::Acked;
            basic_metrics::BROADCAST_ACKED_TOTAL.inc();
            basic_metrics::BROADCAST_PENDING_COUNT.dec();
            debug!(
                delivery_id = %delivery_id,
                event_id = %entry.event_id,
                connection_id = %entry.connection_id,
                "Delivery acked"
            );
        }
    }

    /// Called when a subscriber Nacks a delivery.
    pub fn nack(&self, delivery_id: Uuid, reason: String) {
        if let Some(mut entry) = self.pending.get_mut(&delivery_id) {
            if entry.status == DeliveryStatus::Acked {
                return;
            }
            entry.status = DeliveryStatus::Nacked;
            entry.last_error = Some(reason.clone());
            entry.next_attempt = Instant::now(); // retry immediately
            basic_metrics::BROADCAST_NACKED_TOTAL.inc();
            warn!(
                delivery_id = %delivery_id,
                event_id = %entry.event_id,
                connection_id = %entry.connection_id,
                reason = %reason,
                "Delivery nacked"
            );
        }
    }

    /// Retry worker — periodically scans pending deliveries and retries
    /// those due for another attempt.
    pub async fn run_retry_worker(
        self: Arc<Self>,
        send_fn: Arc<dyn Fn(Uuid, &str) -> Result<(), String> + Send + Sync>,
    ) {
        loop {
            tokio::time::sleep(Duration::from_millis(self.config.retry_interval_ms)).await;

            let now = Instant::now();
            let max_retries = self.config.max_retries;
            let dlq_enabled = self.config.dead_letter_enabled;

            let mut to_retry: Vec<(Uuid, Uuid, String)> = Vec::new();
            let mut to_dead: Vec<Uuid> = Vec::new();

            // Scan pending deliveries
            for entry in self.pending.iter() {
                let delivery = entry.value();
                if delivery.status == DeliveryStatus::Acked
                    || delivery.status == DeliveryStatus::Dead
                {
                    continue;
                }
                if delivery.next_attempt > now {
                    continue;
                }
                if delivery.attempts >= max_retries {
                    to_dead.push(delivery.delivery_id);
                } else {
                    to_retry.push((
                        delivery.delivery_id,
                        delivery.connection_id,
                        delivery.payload_json.clone(),
                    ));
                }
            }

            // Process retries
            for (did, cid, payload) in &to_retry {
                match send_fn(*cid, payload) {
                    Ok(()) => {
                        if let Some(mut entry) = self.pending.get_mut(did) {
                            entry.attempts += 1;
                            entry.status = DeliveryStatus::Pending;
                            entry.last_attempt = Some(now);
                            entry.last_error = None;
                            entry.next_attempt = now + self.backoff(entry.attempts);
                            basic_metrics::BROADCAST_DELIVERED_TOTAL
                                .with_label_values(&["retry"])
                                .inc();
                        }
                        basic_metrics::BROADCAST_RETRY_TOTAL.inc();
                    }
                    Err(e) => {
                        if let Some(mut entry) = self.pending.get_mut(did) {
                            entry.attempts += 1;
                            entry.last_error = Some(e);
                            entry.last_attempt = Some(now);
                            entry.next_attempt = now + self.backoff(entry.attempts);
                        }
                        basic_metrics::BROADCAST_RETRY_TOTAL.inc();
                    }
                }
            }

            // Move to dead letter queue
            if dlq_enabled {
                let dlq_guard = self.dlq.read().await;
                if let Some(ref dlq) = *dlq_guard {
                    for did in &to_dead {
                        if let Some(mut entry) = self.pending.get_mut(did) {
                            entry.status = DeliveryStatus::Dead;
                            dlq.enqueue(&entry);
                            basic_metrics::BROADCAST_DEAD_LETTER_TOTAL.inc();
                            basic_metrics::BROADCAST_PENDING_COUNT.dec();
                        }
                    }
                }
            } else {
                for did in &to_dead {
                    if let Some(mut entry) = self.pending.get_mut(did) {
                        entry.status = DeliveryStatus::Dead;
                    }
                    basic_metrics::BROADCAST_DEAD_LETTER_TOTAL.inc();
                    basic_metrics::BROADCAST_PENDING_COUNT.dec();
                }
            }

            // GC old acked/dead entries (keep last 30s)
            let cutoff = Instant::now() - Duration::from_secs(30);
            self.pending.retain(|_, v| {
                v.created_at > cutoff
                    || (v.status != DeliveryStatus::Acked && v.status != DeliveryStatus::Dead)
            });
        }
    }

    fn backoff(&self, attempt: u32) -> Duration {
        let ms = self.config.base_backoff_ms * 2u64.pow(attempt.min(10));
        Duration::from_millis(ms.min(self.config.max_backoff_ms))
    }

    fn track_connection(&self, connection_id: Uuid, delivery_id: Uuid) {
        self.connection_pending
            .entry(connection_id)
            .or_default()
            .push(delivery_id);
    }

    /// Clean up all pending deliveries for a disconnected connection.
    /// Returns the list of delivery_ids that were pending.
    pub fn connection_disconnected(&self, connection_id: Uuid) -> Vec<Uuid> {
        if let Some((_, ids)) = self.connection_pending.remove(&connection_id) {
            for did in &ids {
                if let Some(mut entry) = self.pending.get_mut(did) {
                    if entry.status != DeliveryStatus::Acked {
                        entry.status = DeliveryStatus::Nacked;
                        entry.last_error = Some("connection disconnected".to_string());
                    }
                }
            }
            return ids;
        }
        Vec::new()
    }

    pub fn pending_count(&self) -> usize {
        self.pending
            .iter()
            .filter(|e| {
                e.value().status != DeliveryStatus::Acked
                    && e.value().status != DeliveryStatus::Dead
            })
            .count()
    }

    /// Get a delivery entry by ID (for testing/debugging).
    #[doc(hidden)]
    pub fn get_delivery(&self, delivery_id: &Uuid) -> Option<PendingDelivery> {
        self.pending.get(delivery_id).map(|r| r.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn tracker() -> DeliveryTracker {
        DeliveryTracker::new(DeliveryConfig {
            max_retries: 3,
            base_backoff_ms: 10,
            max_backoff_ms: 1000,
            retry_interval_ms: 50,
            dead_letter_enabled: false,
        })
    }

    fn make_id(id: u8) -> Uuid {
        Uuid::from_bytes([id; 16])
    }

    #[test]
    fn enqueue_success_increments_first_try() {
        let t = tracker();
        let did = make_id(1);
        let cid = make_id(10);
        assert!(t.enqueue(did, make_id(0), "ch", cid, r#"{"k":"v"}"#.into(), true));
        assert_eq!(t.pending_count(), 1);
    }

    #[test]
    fn enqueue_failure_increments_failed_first() {
        let t = tracker();
        let did = make_id(2);
        let cid = make_id(20);
        assert!(!t.enqueue(did, make_id(0), "ch", cid, r#"{"k":"v"}"#.into(), false));
        assert_eq!(t.pending_count(), 1);
    }

    #[test]
    fn ack_marks_delivery_acked() {
        let t = tracker();
        let did = make_id(3);
        let cid = make_id(30);
        t.enqueue(did, make_id(0), "ch", cid, "{}".into(), true);
        assert_eq!(t.pending_count(), 1);

        t.ack(did);
        assert_eq!(t.pending_count(), 0);
    }

    #[test]
    fn double_ack_is_idempotent() {
        let t = tracker();
        let did = make_id(4);
        let cid = make_id(40);
        t.enqueue(did, make_id(0), "ch", cid, "{}".into(), true);
        t.ack(did);
        t.ack(did); // second ack should not panic
        assert_eq!(t.pending_count(), 0);
    }

    #[test]
    fn nack_schedules_immediate_retry() {
        let t = tracker();
        let did = make_id(5);
        let cid = make_id(50);
        t.enqueue(did, make_id(0), "ch", cid, "{}".into(), true);
        t.nack(did, "timeout".into());

        // After nack, should still be pending (not acked)
        assert_eq!(t.pending_count(), 1);
    }

    #[test]
    fn ack_after_nack_clears_delivery() {
        let t = tracker();
        let did = make_id(6);
        let cid = make_id(60);
        t.enqueue(did, make_id(0), "ch", cid, "{}".into(), true);
        t.nack(did, "retry".into());
        t.ack(did);
        assert_eq!(t.pending_count(), 0);
    }

    #[test]
    fn ack_unknown_delivery_does_not_panic() {
        let t = tracker();
        t.ack(make_id(99));
        assert_eq!(t.pending_count(), 0);
    }

    #[test]
    fn nack_unknown_delivery_does_not_panic() {
        let t = tracker();
        t.nack(make_id(99), "x".into());
        assert_eq!(t.pending_count(), 0);
    }

    #[test]
    fn connection_disconnected_marks_all_pending() {
        let t = tracker();
        let cid = make_id(100);
        for i in 0..5 {
            t.enqueue(make_id(i), make_id(0), "ch", cid, "{}".into(), true);
        }
        assert_eq!(t.pending_count(), 5);

        let ids = t.connection_disconnected(cid);
        assert_eq!(ids.len(), 5);
        // After disconnect, deliveries are marked nacked but still tracked
        assert!(t.pending_count() >= 4); // some may still be pending (not GC'd yet)
    }

    #[test]
    fn pending_count_filters_acked() {
        let t = tracker();
        let cid = make_id(200);
        t.enqueue(make_id(10), make_id(0), "ch", cid, "{}".into(), true);
        t.enqueue(make_id(11), make_id(0), "ch", cid, "{}".into(), true);
        t.ack(make_id(10));
        assert_eq!(t.pending_count(), 1); // only 11 is pending
    }

    #[test]
    fn backoff_exponential() {
        let t = tracker();
        let b0 = t.backoff(0); // 10 * 2^0 = 10ms (clamped to min)
        let b1 = t.backoff(1); // 10 * 2^1 = 20ms
        let b5 = t.backoff(5); // 10 * 2^5 = 320ms
        let b10 = t.backoff(10); // 10 * 2^10 = 10240ms > max → 1000ms
        assert!(b0 <= Duration::from_millis(10));
        assert_eq!(b1, Duration::from_millis(20));
        assert_eq!(b5, Duration::from_millis(320));
        assert_eq!(b10, Duration::from_millis(1000)); // capped
    }

    #[test]
    fn multiple_subscribers_independent_tracking() {
        let t = tracker();
        let cid_a = make_id(1);
        let cid_b = make_id(2);
        t.enqueue(make_id(100), make_id(0), "ch", cid_a, "{}".into(), true);
        t.enqueue(make_id(101), make_id(0), "ch", cid_b, "{}".into(), true);
        assert_eq!(t.pending_count(), 2);

        // Ack subscriber A only
        t.ack(make_id(100));
        assert_eq!(t.pending_count(), 1); // B still pending
    }

    #[test]
    fn pending_delivery_has_correct_fields() {
        let t = tracker();
        let did = make_id(200);
        let eid = make_id(201);
        let cid = make_id(202);
        t.enqueue(did, eid, "my-ch", cid, r#"{"key":"val"}"#.into(), true);

        let entry = t.get_delivery(&did).unwrap();
        assert_eq!(entry.delivery_id, did);
        assert_eq!(entry.event_id, eid);
        assert_eq!(entry.channel_id, "my-ch");
        assert_eq!(entry.connection_id, cid);
        assert_eq!(entry.attempts, 1);
        assert_eq!(entry.status, DeliveryStatus::Pending);
        assert_eq!(entry.payload_json, r#"{"key":"val"}"#);
    }

    // ── exponential backoff math ───────────────────────────────────

    #[test]
    fn backoff_doubles_per_attempt() {
        let tracker = tracker(); // base 10ms
        assert_eq!(tracker.backoff(0), Duration::from_millis(10));
        assert_eq!(tracker.backoff(1), Duration::from_millis(20));
        assert_eq!(tracker.backoff(2), Duration::from_millis(40));
        assert_eq!(tracker.backoff(3), Duration::from_millis(80));
    }

    #[test]
    fn backoff_caps_at_max_backoff_ms() {
        let tracker = tracker(); // max 1000ms
                                 // 10ms * 2^7 = 1280ms already exceeds the 1s cap.
        assert_eq!(tracker.backoff(7), Duration::from_millis(1000));
        assert_eq!(tracker.backoff(10), Duration::from_millis(1000));
        assert_eq!(tracker.backoff(20), Duration::from_millis(1000));
    }

    #[test]
    fn backoff_respects_attempt_cap_before_overflow() {
        // attempt is clamped to 10 before the shift, so a huge attempt count
        // cannot overflow u64 during 2^attempt.
        let tracker = tracker();
        assert_eq!(tracker.backoff(100), Duration::from_millis(1000));
        assert_eq!(tracker.backoff(u32::MAX), Duration::from_millis(1000));
    }

    #[test]
    fn delivery_status_variants_distinct() {
        assert_ne!(DeliveryStatus::Pending, DeliveryStatus::Acked);
        assert_ne!(DeliveryStatus::Retrying, DeliveryStatus::Nacked);
        assert_eq!(DeliveryStatus::Dead, DeliveryStatus::Dead);
    }
}
