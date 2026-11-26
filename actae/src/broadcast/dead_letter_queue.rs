// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Dead letter queue for undeliverable broadcast events.
//!
//! Persists events that could not be delivered to a subscriber after
//! exhausting all retry attempts. Events can be inspected and replayed
//! via dedicated API endpoints.

use std::sync::Arc;

use chrono::Utc;
use deadpool_postgres::Pool;
use tracing::{debug, info, instrument};
use uuid::Uuid;

use crate::database::json_compat::JsonValue;

use super::delivery_tracker::PendingDelivery;

#[derive(Debug, thiserror::Error)]
pub enum DlqError {
    #[error("Pool error: {0}")]
    Pool(#[from] deadpool_postgres::PoolError),
    #[error("Database error: {0}")]
    Execute(#[from] tokio_postgres::Error),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeadLetterEntry {
    pub id: Uuid,
    pub event_id: Uuid,
    pub channel_id: String,
    pub payload: sonic_rs::Value,
    pub connection_id: Option<Uuid>,
    pub subscriber_id: Option<String>,
    pub failure_reason: String,
    pub retry_count: i32,
    pub created_at: String,
    pub requeued_at: Option<String>,
    pub requeue_count: i32,
}

pub struct DeadLetterQueue {
    pool: Arc<Pool>,
}

impl DeadLetterQueue {
    pub fn new(pool: Arc<Pool>) -> Self {
        Self { pool }
    }

    pub async fn init(&self) -> Result<(), DlqError> {
        let client = self.pool.get().await?;
        client
            .batch_execute(
                "
                CREATE TABLE IF NOT EXISTS broadcast_dead_letters (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    event_id UUID NOT NULL,
                    channel_id TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    connection_id UUID,
                    subscriber_id TEXT,
                    failure_reason TEXT NOT NULL,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    requeued_at TIMESTAMPTZ,
                    requeue_count INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_dlq_channel
                    ON broadcast_dead_letters (channel_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_dlq_connection
                    ON broadcast_dead_letters (connection_id);
                CREATE INDEX IF NOT EXISTS idx_dlq_created
                    ON broadcast_dead_letters (created_at);
                ",
            )
            .await?;
        info!("Dead letter queue initialized");
        Ok(())
    }

    #[instrument(skip(self, delivery))]
    pub async fn enqueue(&self, delivery: &PendingDelivery) -> Result<(), DlqError> {
        let client = self.pool.get().await?;
        let payload = JsonValue::new(
            sonic_rs::from_str::<sonic_rs::Value>(&delivery.payload_json)
                .unwrap_or_else(|_| sonic_rs::json!({})),
        );

        client
            .execute(
                "INSERT INTO broadcast_dead_letters
                 (event_id, channel_id, payload, connection_id,
                  failure_reason, retry_count)
                 VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &delivery.event_id,
                    &delivery.channel_id,
                    &payload,
                    &delivery.connection_id,
                    &delivery
                        .last_error
                        .as_deref()
                        .unwrap_or("max retries exhausted"),
                    &(delivery.attempts as i32),
                ],
            )
            .await?;

        debug!(
            event_id = %delivery.event_id,
            channel = %delivery.channel_id,
            connection = %delivery.connection_id,
            "Event moved to dead letter queue"
        );

        Ok(())
    }

    pub async fn query_by_channel(
        &self,
        channel_id: &str,
        limit: i64,
    ) -> Result<Vec<DeadLetterEntry>, DlqError> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT id, event_id, channel_id, payload, connection_id,
                        subscriber_id, failure_reason, retry_count,
                        created_at::text, requeued_at::text, requeue_count
                 FROM broadcast_dead_letters
                 WHERE channel_id = $1
                 ORDER BY created_at DESC
                 LIMIT $2",
                &[&channel_id, &limit],
            )
            .await?;

        Ok(rows.iter().map(row_to_entry).collect())
    }

    pub async fn count_by_channel(&self, channel_id: &str) -> Result<i64, DlqError> {
        let client = self.pool.get().await?;
        let row = client
            .query_one(
                "SELECT COUNT(*) FROM broadcast_dead_letters WHERE channel_id = $1",
                &[&channel_id],
            )
            .await?;
        Ok(row.get::<_, i64>(0))
    }

    pub async fn total_count(&self) -> Result<i64, DlqError> {
        let client = self.pool.get().await?;
        let row = client
            .query_one("SELECT COUNT(*) FROM broadcast_dead_letters", &[])
            .await?;
        Ok(row.get::<_, i64>(0))
    }

    pub async fn purge_older_than(&self, before: chrono::DateTime<Utc>) -> Result<u64, DlqError> {
        let client = self.pool.get().await?;
        let count = client
            .execute(
                "DELETE FROM broadcast_dead_letters WHERE created_at < $1",
                &[&before],
            )
            .await?;
        Ok(count)
    }
}

fn row_to_entry(row: &tokio_postgres::Row) -> DeadLetterEntry {
    let payload_json: JsonValue = row.get("payload");
    let payload = payload_json.0;
    DeadLetterEntry {
        id: row.get("id"),
        event_id: row.get("event_id"),
        channel_id: row.get("channel_id"),
        payload,
        connection_id: row.get("connection_id"),
        subscriber_id: row.get("subscriber_id"),
        failure_reason: row.get("failure_reason"),
        retry_count: row.get("retry_count"),
        created_at: row.get("created_at"),
        requeued_at: row.get("requeued_at"),
        requeue_count: row.get("requeue_count"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn pg_error() -> tokio_postgres::Error {
        tokio_postgres::Error::__private_api_timeout()
    }

    fn sample_entry() -> DeadLetterEntry {
        DeadLetterEntry {
            id: Uuid::nil(),
            event_id: Uuid::parse_str("67e55044-10b1-426f-9247-bb680e5fe0c8").unwrap(),
            channel_id: "test-channel".into(),
            payload: sonic_rs::json!({"key": "value", "num": 42}),
            connection_id: Some(Uuid::parse_str("a1b2c3d4-e5f6-7890-abcd-ef1234567890").unwrap()),
            subscriber_id: Some("subscriber-1".into()),
            failure_reason: "connection refused".into(),
            retry_count: 3,
            created_at: "2026-07-01T12:00:00Z".into(),
            requeued_at: None,
            requeue_count: 0,
        }
    }

    fn sample_entry_requeued() -> DeadLetterEntry {
        DeadLetterEntry {
            id: Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap(),
            event_id: Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap(),
            channel_id: "requeued-channel".into(),
            payload: sonic_rs::json!({"action": "retry"}),
            connection_id: None,
            subscriber_id: None,
            failure_reason: "timeout after 30s".into(),
            retry_count: 5,
            created_at: "2026-07-01T10:00:00Z".into(),
            requeued_at: Some("2026-07-01T11:00:00Z".into()),
            requeue_count: 1,
        }
    }

    // ── DeadLetterEntry serde ───────────────────────────────────────

    #[test]
    fn test_dead_letter_entry_serde_roundtrip() {
        let entry = sample_entry();
        let json = sonic_rs::to_string(&entry).unwrap();
        let parsed: DeadLetterEntry = sonic_rs::from_str(&json).unwrap();

        assert_eq!(parsed.id, entry.id);
        assert_eq!(parsed.event_id, entry.event_id);
        assert_eq!(parsed.channel_id, entry.channel_id);
        assert_eq!(parsed.connection_id, entry.connection_id);
        assert_eq!(parsed.subscriber_id, entry.subscriber_id);
        assert_eq!(parsed.failure_reason, entry.failure_reason);
        assert_eq!(parsed.retry_count, entry.retry_count);
        assert_eq!(parsed.created_at, entry.created_at);
        assert_eq!(parsed.requeued_at, entry.requeued_at);
        assert_eq!(parsed.requeue_count, entry.requeue_count);
    }

    #[test]
    fn test_dead_letter_entry_serde_with_requeued() {
        let entry = sample_entry_requeued();
        let json = sonic_rs::to_string(&entry).unwrap();
        let parsed: DeadLetterEntry = sonic_rs::from_str(&json).unwrap();

        assert_eq!(parsed.requeue_count, 1);
        assert_eq!(parsed.requeued_at, Some("2026-07-01T11:00:00Z".into()));
        assert_eq!(parsed.connection_id, None);
    }

    #[test]
    fn test_dead_letter_entry_serde_payload_preserved() {
        let entry = DeadLetterEntry {
            id: Uuid::nil(),
            event_id: Uuid::nil(),
            channel_id: "ch".into(),
            payload: sonic_rs::json!({"nested": {"deep": true}, "arr": [1,2,3]}),
            connection_id: None,
            subscriber_id: None,
            failure_reason: "reason".into(),
            retry_count: 0,
            created_at: "2026-01-01T00:00:00Z".into(),
            requeued_at: None,
            requeue_count: 0,
        };
        let json = sonic_rs::to_string(&entry).unwrap();
        let parsed: DeadLetterEntry = sonic_rs::from_str(&json).unwrap();

        let payload_str = sonic_rs::to_string(&parsed.payload).unwrap();
        assert!(payload_str.contains("\"nested\""));
        assert!(payload_str.contains("\"arr\""));
    }

    #[test]
    fn test_dead_letter_entry_serde_no_optionals() {
        let json = r#"{
            "id": "00000000-0000-0000-0000-000000000001",
            "event_id": "67e55044-10b1-426f-9247-bb680e5fe0c8",
            "channel_id": "minimal",
            "payload": {},
            "connection_id": null,
            "subscriber_id": null,
            "failure_reason": "test",
            "retry_count": 0,
            "created_at": "2026-01-01T00:00:00Z",
            "requeued_at": null,
            "requeue_count": 0
        }"#;
        let entry: DeadLetterEntry = sonic_rs::from_str(json).unwrap();
        assert_eq!(entry.channel_id, "minimal");
        assert_eq!(entry.connection_id, None);
        assert_eq!(entry.subscriber_id, None);
        assert_eq!(entry.requeued_at, None);
        assert_eq!(entry.retry_count, 0);
        assert_eq!(entry.requeue_count, 0);
    }

    #[test]
    fn test_dead_letter_entry_serde_with_all_optionals() {
        let json = r#"{
            "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "event_id": "67e55044-10b1-426f-9247-bb680e5fe0c8",
            "channel_id": "full",
            "payload": {"action": "test"},
            "connection_id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
            "subscriber_id": "sub-42",
            "failure_reason": "timeout",
            "retry_count": 7,
            "created_at": "2026-01-01T00:00:00Z",
            "requeued_at": "2026-01-02T00:00:00Z",
            "requeue_count": 3
        }"#;
        let entry: DeadLetterEntry = sonic_rs::from_str(json).unwrap();
        assert_eq!(entry.channel_id, "full");
        assert_eq!(entry.subscriber_id, Some("sub-42".into()));
        assert_eq!(entry.retry_count, 7);
        assert_eq!(entry.requeue_count, 3);
        assert_eq!(entry.requeued_at, Some("2026-01-02T00:00:00Z".into()));
        assert!(entry.connection_id.is_some());
    }

    #[test]
    fn test_dead_letter_entry_serialized_keys_present() {
        let entry = sample_entry();
        let json = sonic_rs::to_string(&entry).unwrap();
        assert!(json.contains("\"id\""));
        assert!(json.contains("\"event_id\""));
        assert!(json.contains("\"channel_id\""));
        assert!(json.contains("\"payload\""));
        assert!(json.contains("\"connection_id\""));
        assert!(json.contains("\"subscriber_id\""));
        assert!(json.contains("\"failure_reason\""));
        assert!(json.contains("\"retry_count\""));
        assert!(json.contains("\"created_at\""));
        assert!(json.contains("\"requeued_at\""));
        assert!(json.contains("\"requeue_count\""));
    }

    #[test]
    fn test_dead_letter_entry_high_retry_count() {
        let entry = DeadLetterEntry {
            id: Uuid::nil(),
            event_id: Uuid::nil(),
            channel_id: "ch".into(),
            payload: sonic_rs::json!({"test": true}),
            connection_id: None,
            subscriber_id: None,
            failure_reason: "max retries".into(),
            retry_count: i32::MAX,
            created_at: "2026-01-01T00:00:00Z".into(),
            requeued_at: None,
            requeue_count: 0,
        };
        let json = sonic_rs::to_string(&entry).unwrap();
        let parsed: DeadLetterEntry = sonic_rs::from_str(&json).unwrap();
        assert_eq!(parsed.retry_count, i32::MAX);
    }

    // ── DeadLetterEntry construction / traits ───────────────────────

    #[test]
    fn test_dead_letter_entry_construction() {
        let entry = DeadLetterEntry {
            id: Uuid::new_v4(),
            event_id: Uuid::new_v4(),
            channel_id: "test-channel".into(),
            payload: sonic_rs::json!({"key": "value"}),
            connection_id: Some(Uuid::new_v4()),
            subscriber_id: Some("sub-1".into()),
            failure_reason: "timeout".into(),
            retry_count: 3,
            created_at: "2024-01-01T00:00:00Z".into(),
            requeued_at: None,
            requeue_count: 0,
        };
        assert_eq!(entry.channel_id, "test-channel");
        assert_eq!(entry.retry_count, 3);
        assert_eq!(entry.requeued_at, None);
        assert!(entry.connection_id.is_some());
    }

    #[test]
    fn test_dead_letter_entry_requeued() {
        let entry = DeadLetterEntry {
            id: Uuid::new_v4(),
            event_id: Uuid::new_v4(),
            channel_id: "ch1".into(),
            payload: sonic_rs::json!({"a": 1}),
            connection_id: None,
            subscriber_id: None,
            failure_reason: "network".into(),
            retry_count: 5,
            created_at: "2024-01-01T00:00:00Z".into(),
            requeued_at: Some("2024-01-02T00:00:00Z".into()),
            requeue_count: 2,
        };
        assert_eq!(entry.requeue_count, 2);
        assert!(entry.requeued_at.is_some());
    }

    #[test]
    fn test_dead_letter_entry_default_like() {
        let entry = DeadLetterEntry {
            id: Uuid::nil(),
            event_id: Uuid::nil(),
            channel_id: String::new(),
            payload: sonic_rs::Value::default(),
            connection_id: None,
            subscriber_id: None,
            failure_reason: String::new(),
            retry_count: 0,
            created_at: String::new(),
            requeued_at: None,
            requeue_count: 0,
        };
        assert_eq!(entry.retry_count, 0);
        assert_eq!(entry.requeue_count, 0);
        assert!(entry.connection_id.is_none());
        assert!(entry.requeued_at.is_none());
    }

    #[test]
    fn test_dead_letter_entry_clone() {
        let entry = sample_entry();
        let cloned = entry.clone();
        assert_eq!(cloned.id, entry.id);
        assert_eq!(cloned.event_id, entry.event_id);
        assert_eq!(cloned.channel_id, entry.channel_id);
        assert_eq!(cloned.failure_reason, entry.failure_reason);
        assert_eq!(cloned.retry_count, entry.retry_count);
    }

    #[test]
    fn test_dead_letter_entry_debug() {
        let entry = sample_entry();
        let dbg = format!("{entry:?}");
        assert!(dbg.contains("DeadLetterEntry"));
        assert!(dbg.contains("test-channel"));
        assert!(dbg.contains("connection refused"));
    }

    // ── DlqError ────────────────────────────────────────────────────

    #[test]
    fn test_dlq_error_display_pool_closed() {
        let err = DlqError::Pool(deadpool_postgres::PoolError::Closed);
        assert!(err.to_string().contains("Pool error"));
    }

    #[test]
    fn test_dlq_error_display_pool_timeout() {
        let err = DlqError::Pool(deadpool_postgres::PoolError::Timeout(
            deadpool_postgres::TimeoutType::Wait,
        ));
        assert!(err.to_string().contains("Pool error"));
    }

    #[test]
    fn test_dlq_error_display_pool_backend() {
        let err = DlqError::Pool(deadpool_postgres::PoolError::Backend(pg_error()));
        let msg = err.to_string();
        assert!(msg.contains("Pool error"));
    }

    #[test]
    fn test_dlq_error_display_execute() {
        let err = DlqError::Execute(pg_error());
        let msg = err.to_string();
        assert!(msg.contains("Database error"));
    }

    #[test]
    fn test_dlq_error_debug_pool() {
        let err = DlqError::Pool(deadpool_postgres::PoolError::Closed);
        let dbg = format!("{err:?}");
        assert!(!dbg.is_empty());
    }

    #[test]
    fn test_dlq_error_debug_execute() {
        let err = DlqError::Execute(pg_error());
        let dbg = format!("{err:?}");
        assert!(!dbg.is_empty());
    }

    #[test]
    fn test_dlq_error_from_pool_error() {
        let pool_err = deadpool_postgres::PoolError::Closed;
        let dlq_err: DlqError = pool_err.into();
        assert!(matches!(dlq_err, DlqError::Pool(_)));
    }

    #[test]
    fn test_dlq_error_from_pg_error() {
        let _err: DlqError = pg_error().into();
    }

    #[test]
    fn test_dlq_error_implements_std_error() {
        use std::error::Error;
        let err: Box<dyn Error> = Box::new(DlqError::Pool(deadpool_postgres::PoolError::Closed));
        let _ = err.source();
    }

    // ── DeadLetterQueue ─────────────────────────────────────────────

    #[test]
    fn test_dlq_new_constructable() {
        if let Ok(pool) = deadpool_postgres::Config::new().create_pool(
            Some(deadpool_postgres::Runtime::Tokio1),
            tokio_postgres::NoTls,
        ) {
            let _dlq = DeadLetterQueue::new(Arc::new(pool));
        }
    }
}
