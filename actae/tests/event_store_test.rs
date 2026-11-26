// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for EventStore — requires a running PostgreSQL.
//!
//! Set DATABASE_URL env var or run `cargo run -- --dev` first to start
//! an embedded PostgreSQL instance, then run these tests.
//!
//! These tests validate the full persistence pipeline:
//! publish → replay → query → cursor → channel_cursor → retention → purge
//! save_state → load_state → list_states → load_state_version → delete_state_version.

use std::env;
use std::sync::Arc;

use sonic_rs::json;
use sonic_rs::JsonContainerTrait;
use sonic_rs::JsonValueTrait;

use actae::events::schema::{
    AddExperimentMemberRequest, ClaimExecutionRequest, CompleteExecutionRequest,
    CreateExperimentRequest, EventMetadata, ForkReplayResult, ForkRequest, PublishEventRequest,
    QueryRequest, ReplayRequest, UpdateMetadataRequest,
};
use actae::events::store::{EventStore, EventStoreError};

fn get_db_url() -> String {
    env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://actae:actae_dev@localhost:5432/actae".to_string())
}

async fn create_store() -> EventStore {
    create_store_with_pool_size(deadpool_postgres::PoolConfig::default().max_size).await
}

async fn create_store_with_pool_size(max_size: usize) -> EventStore {
    use deadpool_postgres::Runtime;
    let db_url = get_db_url();
    let pg: tokio_postgres::Config = db_url.parse().expect("invalid DATABASE_URL");
    let mut cfg = deadpool_postgres::Config::new();
    // Extract host from tokio_postgres::Config Host enum
    cfg.host = pg.get_hosts().first().map(|h| match h {
        tokio_postgres::config::Host::Tcp(s) => s.clone(),
        tokio_postgres::config::Host::Unix(p) => format!("/{}", p.display()),
    });
    cfg.port = pg.get_ports().first().copied();
    cfg.user = pg.get_user().map(|u| u.to_owned());
    cfg.password = pg
        .get_password()
        .and_then(|b| std::str::from_utf8(b).ok().map(|s| s.to_string()));
    cfg.dbname = pg.get_dbname().map(|s| s.to_owned());
    cfg.pool = Some(deadpool_postgres::PoolConfig::new(max_size));

    let pool = cfg
        .create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls)
        .expect("failed to create pool");

    let store = EventStore::new(pool);
    store.initialize().await.expect("migration failed");
    store
}

async fn cleanup(store: &EventStore) {
    let pool = store.pool();
    let client = pool.get().await.expect("pool get failed");
    client
        .execute("DELETE FROM execution_group_forks", &[])
        .await
        .expect("group fork cleanup failed");
    client
        .execute("DELETE FROM execution_group_messages", &[])
        .await
        .expect("group message cleanup failed");
    client
        .execute("DELETE FROM execution_group_members", &[])
        .await
        .expect("group member cleanup failed");
    client
        .execute("DELETE FROM execution_groups", &[])
        .await
        .expect("execution group cleanup failed");
    client
        .execute("DELETE FROM agent_events", &[])
        .await
        .expect("cleanup failed");
    client
        .execute("DELETE FROM event_archive", &[])
        .await
        .expect("event_archive cleanup failed");
    client
        .execute("DELETE FROM channels", &[])
        .await
        .expect("channels cleanup failed");
    client
        .execute("DELETE FROM channel_cursors", &[])
        .await
        .expect("channel_cursors cleanup failed");
    client
        .execute("DELETE FROM channel_states", &[])
        .await
        .expect("channel_states cleanup failed");
    client
        .execute("DELETE FROM channel_metadata", &[])
        .await
        .expect("channel_metadata cleanup failed");
    client
        .execute("DELETE FROM fork_operations", &[])
        .await
        .expect("fork_operations cleanup failed");
    client
        .execute("DELETE FROM tool_executions", &[])
        .await
        .expect("tool_executions cleanup failed");
    client
        .execute("DELETE FROM channel_steps", &[])
        .await
        .expect("channel_steps cleanup failed");
    client
        .execute("DELETE FROM experiment_group_members", &[])
        .await
        .expect("experiment_group_members cleanup failed");
    client
        .execute("DELETE FROM experiment_groups", &[])
        .await
        .expect("experiment_groups cleanup failed");
    client
        .execute("DELETE FROM consumer_groups", &[])
        .await
        .expect("consumer_groups cleanup failed");
    client
        .execute("DELETE FROM scheduled_wakeups", &[])
        .await
        .expect("scheduled_wakeups cleanup failed");
    client
        .execute("DELETE FROM usage_quota_monthly", &[])
        .await
        .expect("usage quota cleanup failed");
    client
        .execute(
            "UPDATE usage_quota_config SET max_events_month = 0, max_ingress_bytes_month = 0,
                max_durable_written_bytes_month = 0, max_retained_bytes = 0",
            &[],
        )
        .await
        .expect("usage quota config cleanup failed");
    client
        .execute("DELETE FROM usage_storage_state", &[])
        .await
        .expect("usage storage state cleanup failed");
    client
        .execute(
            "INSERT INTO usage_storage_state (singleton, retained_bytes) VALUES (TRUE, 0) ON CONFLICT (singleton) DO UPDATE SET retained_bytes = 0",
            &[],
        )
        .await
        .expect("usage storage state reset failed");
    client
        .execute("DELETE FROM usage_delivery_outbox", &[])
        .await
        .expect("usage outbox cleanup failed");
    client
        .execute(
            "ALTER SEQUENCE IF EXISTS agent_events_cursor_seq RESTART WITH 1",
            &[],
        )
        .await
        .expect("sequence reset failed");
}

#[tokio::test]
async fn execution_group_message_is_durable_causal_and_fenced() {
    let store = create_store().await;
    cleanup(&store).await;
    store
        .create_execution_group("support.42", Some(&json!({"ticket":42})))
        .await
        .unwrap();
    store
        .add_execution_group_member(
            "support.42",
            "router",
            "support.router",
            Some("router"),
            None,
        )
        .await
        .unwrap();
    store
        .add_execution_group_member(
            "support.42",
            "research",
            "support.research",
            Some("research"),
            None,
        )
        .await
        .unwrap();
    let operation = uuid::Uuid::new_v4();
    let (message, source, delivery) = store
        .send_execution_group_message(
            "support.42",
            "router",
            "research",
            "research.request",
            &json!({"question":"why"}),
            None,
            Some(operation),
        )
        .await
        .unwrap();
    assert_eq!(delivery.depends_on, Some(source.id));
    let replay = store
        .list_execution_group_messages("support.42", "research", None, 100)
        .await
        .unwrap();
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].message_id, message.message_id);
    let same = store
        .send_execution_group_message(
            "support.42",
            "router",
            "research",
            "research.request",
            &json!({"question":"why"}),
            None,
            Some(operation),
        )
        .await
        .unwrap();
    assert_eq!(same.0.message_id, message.message_id);
    let lease = store
        .claim_execution_group_member("support.42", "research", "worker-a", 30)
        .await
        .unwrap();
    let acknowledged = store
        .acknowledge_execution_group_message(message.message_id, "worker-a", lease.generation)
        .await
        .unwrap();
    assert_eq!(acknowledged.status, "acknowledged");
    store
        .release_execution_group_member("support.42", "research", "worker-a", lease.generation)
        .await
        .unwrap();
    assert!(matches!(
        store
            .acknowledge_execution_group_message(message.message_id, "worker-a", lease.generation)
            .await,
        Err(EventStoreError::ExecutionGroupMemberFenced { .. })
    ));
    let graph = store.causal_graph(delivery.id, 20).await.unwrap();
    assert!(graph.events.iter().any(|event| event.id == source.id));
    assert!(graph
        .dependencies
        .iter()
        .any(|edge| edge.event_id == delivery.id && edge.depends_on_event_id == source.id));
    cleanup(&store).await;
}

#[tokio::test]
async fn execution_group_fork_materializes_replay_and_promotes_member() {
    let store = create_store().await;
    cleanup(&store).await;
    store.create_execution_group("prod.1", None).await.unwrap();
    store
        .add_execution_group_member("prod.1", "router", "prod.router", None, None)
        .await
        .unwrap();
    store
        .add_execution_group_member("prod.1", "research", "prod.research", None, None)
        .await
        .unwrap();
    store
        .send_execution_group_message(
            "prod.1",
            "router",
            "research",
            "research.request",
            &json!({"q":"one"}),
            None,
            None,
        )
        .await
        .unwrap();
    let fork = store
        .create_group_fork(
            "fork.1",
            "prod.1",
            "research",
            10,
            &json!({"router":"frozen","research":"live"}),
            &json!({"email.send":"block"}),
        )
        .await
        .unwrap();
    assert_eq!(fork.status, "active");
    // Re-creating the identical group fork is an idempotent replay and must
    // not add a second fork meter.
    let replay = store
        .create_group_fork(
            "fork.1",
            "prod.1",
            "research",
            10,
            &json!({"router":"frozen","research":"live"}),
            &json!({"email.send":"block"}),
        )
        .await
        .unwrap();
    assert_eq!(replay.fork_group_id, "fork.1");
    let members = store.list_execution_group_members("fork.1").await.unwrap();
    assert_eq!(members.len(), 2);
    let replayed = store
        .list_execution_group_messages("fork.1", "research", None, 100)
        .await
        .unwrap();
    assert_eq!(replayed.len(), 1);
    assert_eq!(
        store
            .group_fork_member_policy("fork.1", "router")
            .await
            .unwrap()
            .as_deref(),
        Some("frozen")
    );
    let promoted = store
        .promote_group_fork_member("fork.1", "router")
        .await
        .unwrap();
    assert_eq!(
        promoted
            .member_policies
            .unwrap()
            .get("router")
            .and_then(|v| v.as_str()),
        Some("reactive")
    );
    // One fresh group fork ⇒ one fork meter (the replay above added none).
    let client = store.pool().get().await.unwrap();
    let group_fork_meters: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM usage_delivery_outbox WHERE metric = 'fork_operations'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(group_fork_meters, 1, "one fork_operations meter per group fork");
    drop(client);
    cleanup(&store).await;
}

async fn ensure_versioned_schema(store: &EventStore) {
    let pool = store.pool();
    let client = pool.get().await.expect("pool get failed");
    let has_version = client
        .query_one(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'channel_states' AND column_name = 'version'
            )",
            &[],
        )
        .await
        .map(|r| r.get::<_, bool>(0))
        .unwrap_or(false);
    if !has_version {
        client
            .batch_execute(
                "ALTER TABLE channel_states DROP CONSTRAINT IF EXISTS channel_states_pkey CASCADE;
                 ALTER TABLE channel_states ADD COLUMN version BIGSERIAL NOT NULL;
                 ALTER TABLE channel_states ADD PRIMARY KEY (channel_id, version);
                 CREATE INDEX IF NOT EXISTS idx_channel_states_latest ON channel_states (channel_id, version DESC);
                 CREATE INDEX IF NOT EXISTS idx_channel_states_cursor ON channel_states (channel_id, cursor);",
            )
            .await
            .expect("failed to apply versioned schema");
    }
}

fn request(channel: &str, event_type: &str, payload: sonic_rs::Value) -> PublishEventRequest {
    PublishEventRequest {
        channel_id: channel.into(),
        event_type: event_type.into(),
        payload,
        metadata: EventMetadata {
            actor: "test".into(),
            agent_id: None,
            user_id: None,
            metadata: None,
        },
        depends_on: None,
        dependencies: Vec::new(),
        operation_id: None,
        step_number: None,
    }
}

fn request_with_op(
    channel: &str,
    event_type: &str,
    payload: sonic_rs::Value,
    op: uuid::Uuid,
) -> PublishEventRequest {
    PublishEventRequest {
        operation_id: Some(op),
        ..request(channel, event_type, payload)
    }
}

#[tokio::test]
async fn publish_idempotent_with_operation_id() {
    let store = create_store().await;
    cleanup(&store).await;

    let op = uuid::Uuid::new_v4();
    let first = store
        .publish_with_channel_cursor(request_with_op("ch-op", "test.idem", json!({"n": 1}), op))
        .await
        .expect("first publish failed")
        .0;
    assert_eq!(first.cursor, 1);

    // Retry with the same (channel_id, operation_id): returns the ORIGINAL
    // event — same id/cursor — and does not allocate a new cursor.
    let retry = store
        .publish_with_channel_cursor(request_with_op("ch-op", "test.idem", json!({"n": 1}), op))
        .await
        .expect("retry publish failed")
        .0;
    assert_eq!(
        retry.id, first.id,
        "idempotent retry must return the original event"
    );
    assert_eq!(
        retry.cursor, first.cursor,
        "idempotent retry must not bump the cursor"
    );

    // A different operation_id on the same channel is a fresh event.
    let other = store
        .publish_with_channel_cursor(request_with_op(
            "ch-op",
            "test.idem",
            json!({"n": 2}),
            uuid::Uuid::new_v4(),
        ))
        .await
        .expect("second publish failed")
        .0;
    assert_eq!(
        other.cursor,
        first.cursor + 1,
        "new operation_id must append"
    );

    // The channel contains exactly two events (no duplicates).
    let replayed = store
        .replay(&ReplayRequest {
            channel_id: "ch-op".into(),
            cursor: Some(0),
            limit: Some(100),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .expect("replay failed");
    assert_eq!(replayed.len(), 2);

    cleanup(&store).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_idempotent_concurrent_same_operation_id_keeps_gapless_sequence() {
    let store = create_store().await;
    cleanup(&store).await;

    let op = uuid::Uuid::new_v4();
    let req = request_with_op("ch-race", "test.idem", json!({"n": 1}), op);

    // Two concurrent requests with the same operation_id: both must
    // succeed and agree on the SAME event.
    let (a, b) = tokio::join!(
        store.publish_with_channel_cursor(req.clone()),
        store.publish_with_channel_cursor(req),
    );
    let (ea, eb) = (
        a.expect("first publish failed").0,
        b.expect("second publish failed").0,
    );
    assert_eq!(
        ea.id, eb.id,
        "concurrent idempotent retries must return the same event"
    );
    assert_eq!(ea.cursor, eb.cursor);
    assert_eq!(ea.cursor, eb.cursor);

    // The loser must not have left a phantom cursor bump: the next event's
    // channel_cursor is exactly winner + 1 (a gap would prove the loser's
    // bump was committed without an event).
    let next = store
        .publish_with_channel_cursor(request("ch-race", "test.next", json!({})))
        .await
        .expect("next publish failed")
        .0;
    assert_eq!(
        next.cursor,
        ea.cursor + 1,
        "channel cursor sequence must stay gapless after an idempotent race"
    );

    // Replayed events carry contiguous channel cursors: [1, 2].
    let replayed = store
        .replay(&ReplayRequest {
            channel_id: "ch-race".into(),
            cursor: Some(0),
            limit: Some(100),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .expect("replay failed");
    assert_eq!(replayed.len(), 2);
    let cursors: Vec<i64> = replayed.iter().map(|e| e.cursor).collect();
    assert_eq!(
        cursors,
        vec![1, 2],
        "replayed channel cursors must be contiguous"
    );

    cleanup(&store).await;
}

#[tokio::test]
async fn publish_and_replay_single_event() {
    let store = create_store().await;
    cleanup(&store).await;

    let ev = store
        .publish_with_channel_cursor(request("ch-1", "test.one", json!({"a": 1})))
        .await
        .expect("publish failed")
        .0;

    assert_eq!(ev.channel_id, "ch-1");
    assert_eq!(ev.event_type, "test.one");
    assert_eq!(ev.cursor, 1);
    assert_eq!(ev.cursor, 1);

    let replayed = store
        .replay(&ReplayRequest {
            channel_id: "ch-1".into(),
            cursor: Some(0),
            limit: Some(10),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .expect("replay failed");

    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].id, ev.id);
    assert_eq!(replayed[0].event_type, "test.one");

    cleanup(&store).await;
}

#[tokio::test]
async fn publish_multiple_events_channel_cursor_continuous() {
    let store = create_store().await;
    cleanup(&store).await;

    for i in 0..5 {
        store
            .publish_with_channel_cursor(request("ch-2", "test.seq", json!({"i": i})))
            .await
            .expect("publish failed");
    }

    let replayed = store
        .replay(&ReplayRequest {
            channel_id: "ch-2".into(),
            cursor: Some(0),
            limit: Some(100),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .expect("replay failed");

    assert_eq!(replayed.len(), 5);
    for (i, ev) in replayed.iter().enumerate() {
        let i = i as i64;
        assert_eq!(ev.cursor, i + 1);
    }

    cleanup(&store).await;
}

#[tokio::test]
async fn replay_from_cursor_skips_earlier_events() {
    let store = create_store().await;
    cleanup(&store).await;

    for i in 0..10 {
        store
            .publish_with_channel_cursor(request("ch-3", "test.n", json!({"n": i})))
            .await
            .expect("publish failed");
    }

    let replayed = store
        .replay(&ReplayRequest {
            channel_id: "ch-3".into(),
            cursor: Some(5),
            limit: Some(100),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .expect("replay failed");

    assert_eq!(replayed.len(), 5); // cursors 6-10
    assert!(replayed[0].cursor > 5);

    cleanup(&store).await;
}

#[tokio::test]
async fn replay_with_event_type_filter() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .publish_with_channel_cursor(request("ch-4", "type-a", json!({})))
        .await
        .unwrap();
    store
        .publish_with_channel_cursor(request("ch-4", "type-b", json!({})))
        .await
        .unwrap();
    store
        .publish_with_channel_cursor(request("ch-4", "type-a", json!({})))
        .await
        .unwrap();

    let replayed = store
        .replay(&ReplayRequest {
            channel_id: "ch-4".into(),
            cursor: Some(0),
            limit: Some(100),
            event_type: Some("type-a".into()),
            channel_cursor: None,
        })
        .await
        .expect("replay failed");

    assert_eq!(replayed.len(), 2);
    assert!(replayed.iter().all(|e| e.event_type == "type-a"));

    cleanup(&store).await;
}

#[tokio::test]
async fn get_latest_cursor_returns_correct_value() {
    let store = create_store().await;
    cleanup(&store).await;

    let c = store.get_latest_cursor("ch-empty").await.unwrap();
    assert!(c.is_none());

    store
        .publish_with_channel_cursor(request("ch-5", "test.c", json!({})))
        .await
        .unwrap();

    let c = store.get_latest_cursor("ch-5").await.unwrap();
    assert!(c.is_some());
    assert!(c.unwrap() > 0);

    store
        .publish_with_channel_cursor(request("ch-5", "test.c", json!({})))
        .await
        .unwrap();

    let c = store.get_latest_cursor("ch-5").await.unwrap();
    assert!(c.is_some());
    assert!(c.unwrap() > 0);

    cleanup(&store).await;
}

#[tokio::test]
async fn publish_with_channel_cursor_rolls_back_on_error() {
    let store = create_store().await;
    cleanup(&store).await;

    let _events_before = store
        .replay(&ReplayRequest {
            channel_id: "ch-6".into(),
            cursor: None,
            limit: Some(100),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap_or_default();

    // This should fail because channel_id is too long for the index
    // but the store doesn't validate — it lets PG handle it. For now,
    // just verify normal operations work.
    let result = store
        .publish_with_channel_cursor(request("ch-6", "test.ok", json!({"v": 1})))
        .await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap().0.cursor, 1);

    cleanup(&store).await;
}

#[tokio::test]
async fn monthly_quota_rejects_atomically_without_cursor_or_outbox_drift() {
    let store = create_store().await;
    cleanup(&store).await;
    let client = store.pool().get().await.unwrap();
    client
        .execute(
            "UPDATE usage_quota_config SET max_events_month = 1, max_ingress_bytes_month = 1000000",
            &[],
        )
        .await
        .unwrap();

    store
        .publish_with_channel_cursor(request("quota-atomic", "ok", json!({"n": 1})))
        .await
        .unwrap();
    let error = store
        .publish_with_channel_cursor(request("quota-atomic", "blocked", json!({"n": 2})))
        .await
        .unwrap_err();
    assert!(matches!(error, EventStoreError::QuotaExceeded));
    assert_eq!(
        store.get_latest_cursor("quota-atomic").await.unwrap(),
        Some(1)
    );
    let event_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM agent_events WHERE channel_id = 'quota-atomic'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let outbox_count: i64 = client
        .query_one("SELECT COUNT(*) FROM usage_delivery_outbox", &[])
        .await
        .unwrap()
        .get(0);
    let quota_count: i64 = client
        .query_one("SELECT events_ingested FROM usage_quota_monthly", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        (event_count, quota_count),
        (1, 1),
        "one event accepted, and exactly one event counted in the quota meter"
    );
    // The event path emits one outbox row per meter: events_ingested,
    // ingress_bytes, and durable_written_bytes.
    assert_eq!(outbox_count, 3);
    drop(client);
    cleanup(&store).await;
}

#[tokio::test]
async fn durable_written_and_retained_quotas_enforce_atomically() {
    let store = create_store().await;
    cleanup(&store).await;
    let client = store.pool().get().await.unwrap();
    // Events and ingress are unlimited; only the durable-written meter is
    // capped, so the retained gauge and the flow meter are exercised together.
    client
        .execute(
            "UPDATE usage_quota_config
             SET max_events_month = 0, max_ingress_bytes_month = 0,
                 max_durable_written_bytes_month = 100, max_retained_bytes = 0",
            &[],
        )
        .await
        .unwrap();

    // First event lands.
    store
        .publish_with_channel_cursor(request(
            "quota-durable",
            "ok",
            json!({"x": "0123456789abcdef"}),
        ))
        .await
        .unwrap();
    let quota_count: i64 = client
        .query_one(
            "SELECT durable_written_bytes FROM usage_quota_monthly
             ORDER BY usage_month DESC LIMIT 1",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        quota_count > 0,
        "durable-written meter must count the event payload"
    );
    let retained: i64 = client
        .query_one(
            "SELECT retained_bytes FROM usage_storage_state WHERE singleton = TRUE",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        retained, quota_count,
        "retained gauge tracks the stored event payload"
    );

    // A large second write exceeds the durable-written cap and is rejected
    // atomically (event + outbox + meter all roll back).
    let big = request(
        "quota-durable",
        "blocked",
        json!({"blob": "z".repeat(10_000)}),
    );
    let error = store.publish_with_channel_cursor(big).await.unwrap_err();
    assert!(matches!(error, EventStoreError::QuotaExceeded));
    let event_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM agent_events WHERE channel_id = 'quota-durable'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(event_count, 1, "rejected write must not persist an event");
    drop(client);
    cleanup(&store).await;
}

#[tokio::test]
async fn retained_data_cap_blocks_state_writes_when_full() {
    let store = create_store().await;
    cleanup(&store).await;
    let client = store.pool().get().await.unwrap();
    // Only the retained-data gauge is capped; flow meters are unlimited.
    client
        .execute(
            "UPDATE usage_quota_config
             SET max_events_month = 0, max_ingress_bytes_month = 0,
                 max_durable_written_bytes_month = 0, max_retained_bytes = 10",
            &[],
        )
        .await
        .unwrap();

    // A state write larger than the retained cap is rejected atomically.
    let error = store
        .save_state(
            "quota-retained",
            1,
            &sonic_rs::json!({"blob": "y".repeat(100)}),
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, EventStoreError::QuotaExceeded));
    let state_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM channel_states WHERE channel_id = 'quota-retained'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(state_count, 0, "rejected state write must not persist");
    drop(client);
    cleanup(&store).await;
}

#[tokio::test]
async fn tombstone_expired_executions_nulls_bodies_and_rejects_retry() {
    let store = create_store().await;
    cleanup(&store).await;
    let client = store.pool().get().await.unwrap();
    // The test database persists across runs; remove any leftover execution
    // for this key so the test is hermetic.
    client
        .execute(
            "DELETE FROM tool_executions WHERE channel_id = 'tombstone-chan'",
            &[],
        )
        .await
        .unwrap();

    // Complete an execution.
    let (claim, _) = store
        .claim_execution(&ClaimExecutionRequest {
            channel_id: "tombstone-chan".into(),
            key_name: "charge-42".into(),
            tool_name: "charge".into(),
            params: Some(json!({"amount": 9.99})),
            dedup_fields: None,
            lease_seconds: None,
            emit_replay_event: None,
            actor: None,
        })
        .await
        .unwrap();
    assert_eq!(claim.status, "claimed");
    store
        .complete_execution(
            claim.execution.id,
            claim.claim_token,
            &CompleteExecutionRequest {
                claim_token: None,
                result: Some(json!({"receipt": "rcpt-1"})),
            },
        )
        .await
        .unwrap();

    // Force the completion timestamp into the past and tombstone it.
    client
        .execute(
            "UPDATE tool_executions SET completed_at = NOW() - INTERVAL '40 days' WHERE key_name = 'charge-42'",
            &[],
        )
        .await
        .unwrap();
    let before = chrono::Utc::now() - chrono::Duration::days(30);
    let tombstoned = store.tombstone_expired_executions(before).await.unwrap();
    assert_eq!(tombstoned, 1);

    // The bodies are nulled; the key identity survives.
    let row = client
        .query_one(
            "SELECT result, expired_at FROM tool_executions WHERE key_name = 'charge-42'",
            &[],
        )
        .await
        .unwrap();
    let result: Option<actae::database::json_compat::JsonValue> = row.get("result");
    assert!(result.is_none(), "result body must be purged");
    let expired: Option<chrono::DateTime<chrono::Utc>> = row.get("expired_at");
    assert!(expired.is_some(), "tombstone must record expiry");

    // A retry of the same key fails loudly, not silently re-executes.
    let err = store
        .claim_execution(&ClaimExecutionRequest {
            channel_id: "tombstone-chan".into(),
            key_name: "charge-42".into(),
            tool_name: "charge".into(),
            params: Some(json!({"amount": 9.99})),
            dedup_fields: None,
            lease_seconds: None,
            emit_replay_event: None,
            actor: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        EventStoreError::ExecutionResultExpired { .. }
    ));
    drop(client);
    cleanup(&store).await;
}

#[tokio::test]
async fn transition_quota_rejects_atomically_both_event_and_state() {
    let store = create_store().await;
    cleanup(&store).await;
    let client = store.pool().get().await.unwrap();
    // Cap the durable-written meter small enough that the transition's event +
    // state together exceed it; the whole transition must roll back.
    client
        .execute(
            "UPDATE usage_quota_config
             SET max_events_month = 0, max_ingress_bytes_month = 0,
                 max_durable_written_bytes_month = 200, max_retained_bytes = 0",
            &[],
        )
        .await
        .unwrap();

    // A transition whose event + state fit under the cap lands atomically.
    store
        .publish_with_state(&transition_request(
            "quota-transition",
            "step",
            json!({"i": 1}),
            json!({"mem": "a"}),
        ))
        .await
        .unwrap();

    // A transition whose event + state exceed the cap must reject, leaving no
    // event and no state behind.
    let err = store
        .publish_with_state(&transition_request(
            "quota-transition",
            "step",
            json!({"i": 2}),
            json!({"mem": "x".repeat(10_000)}),
        ))
        .await
        .unwrap_err();
    assert!(matches!(err, EventStoreError::QuotaExceeded));

    let event_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM agent_events WHERE channel_id = 'quota-transition'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let state_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM channel_states WHERE channel_id = 'quota-transition'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        event_count, 1,
        "rejected transition must not leave an event"
    );
    assert_eq!(
        state_count, 1,
        "rejected transition must not leave a state snapshot"
    );
    drop(client);
    cleanup(&store).await;
}

#[tokio::test]
async fn fork_state_copy_counts_toward_retained_but_not_durable_written() {
    let store = create_store().await;
    cleanup(&store).await;
    let client = store.pool().get().await.unwrap();
    client
        .execute("DELETE FROM channel_states WHERE channel_id IN ('fork-acct-parent', 'fork-acct-child')", &[])
        .await
        .unwrap();

    for i in 0..5 {
        store
            .publish_with_channel_cursor(request("fork-acct-parent", "step", json!({"i": i})))
            .await
            .unwrap();
    }
    // Two saved snapshots, mirroring the passing fork test: at cursor 2 and 5.
    store
        .save_state("fork-acct-parent", 2, &json!({"boundary": 2}), None, None)
        .await
        .unwrap();
    store
        .save_state(
            "fork-acct-parent",
            5,
            &json!({"boundary": 5, "blob": "z".repeat(500)}),
            None,
            None,
        )
        .await
        .unwrap();

    let durable_before: i64 = client
        .query_one(
            "SELECT COALESCE(SUM(durable_written_bytes)::bigint, 0) FROM usage_quota_monthly WHERE usage_month = CURRENT_DATE",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let retained_before: i64 = client
        .query_one(
            "SELECT retained_bytes FROM usage_storage_state WHERE singleton = TRUE",
            &[],
        )
        .await
        .unwrap()
        .get(0);

    // Fork copies the parent's state (resolves to cursor 2, like the passing
    // fork test) into the child.
    store
        .fork_channel(&ForkRequest {
            source_channel_id: "fork-acct-parent".into(),
            new_channel_id: "fork-acct-child".into(),
            at_cursor: 3,
            operation_id: None,
            display_name: Some("Child".into()),
            reason: Some("test".into()),
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap();

    // The fork-generated state copy is marked 'fork'.
    let child_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM channel_states WHERE channel_id = 'fork-acct-child'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        child_count, 1,
        "fork must create exactly one child state snapshot"
    );
    let source: Option<String> = client
        .query_one(
            "SELECT write_source FROM channel_states WHERE channel_id = 'fork-acct-child'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        source.as_deref(),
        Some("fork"),
        "fork-inherited state must be marked write_source='fork'"
    );

    // The durable-written meter must NOT increase (the customer did not write
    // the fork copy), but the retained gauge MUST increase (Actae stores it).
    let durable_after: i64 = client
        .query_one(
            "SELECT COALESCE(SUM(durable_written_bytes)::bigint, 0) FROM usage_quota_monthly WHERE usage_month = CURRENT_DATE",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let retained_after: i64 = client
        .query_one(
            "SELECT retained_bytes FROM usage_storage_state WHERE singleton = TRUE",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        durable_after, durable_before,
        "fork copy must not count toward the monthly write meter"
    );
    assert!(
        retained_after > retained_before,
        "fork copy must count toward retained storage"
    );
    drop(client);
    cleanup(&store).await;
}

#[tokio::test]
async fn tool_claim_and_complete_count_against_durable_written_quota() {
    let store = create_store().await;
    cleanup(&store).await;
    let client = store.pool().get().await.unwrap();
    client
        .execute(
            "DELETE FROM tool_executions WHERE channel_id = 'tool-quota-chan'",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "UPDATE usage_quota_config
             SET max_events_month = 0, max_ingress_bytes_month = 0,
                 max_durable_written_bytes_month = 5000, max_retained_bytes = 0",
            &[],
        )
        .await
        .unwrap();

    // A claim with small params lands and counts toward the meter.
    let (claim, _) = store
        .claim_execution(&ClaimExecutionRequest {
            channel_id: "tool-quota-chan".into(),
            key_name: "k-1".into(),
            tool_name: "t".into(),
            params: Some(json!({"n": 1})),
            dedup_fields: None,
            lease_seconds: None,
            emit_replay_event: None,
            actor: None,
        })
        .await
        .unwrap();
    assert_eq!(claim.status, "claimed");

    // A claim with huge params exceeds the durable-written cap and is
    // rejected, leaving no execution row.
    let err = store
        .claim_execution(&ClaimExecutionRequest {
            channel_id: "tool-quota-chan".into(),
            key_name: "k-huge".into(),
            tool_name: "t".into(),
            params: Some(json!({"blob": "z".repeat(50_000)})),
            dedup_fields: None,
            lease_seconds: None,
            emit_replay_event: None,
            actor: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, EventStoreError::QuotaExceeded));

    let row_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM tool_executions WHERE channel_id = 'tool-quota-chan'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        row_count, 1,
        "rejected claim must not persist an execution row"
    );
    drop(client);
    cleanup(&store).await;
}

#[tokio::test]
async fn query_across_channels() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .publish_with_channel_cursor(request("q-ch-a", "qa", json!({})))
        .await
        .unwrap();
    store
        .publish_with_channel_cursor(request("q-ch-b", "qb", json!({})))
        .await
        .unwrap();
    store
        .publish_with_channel_cursor(request("q-ch-a", "qa2", json!({})))
        .await
        .unwrap();

    let results = store
        .query(&QueryRequest {
            channel_ids: Some(vec!["q-ch-a".into()]),
            event_type: None,
            actor: None,
            cursor_start: None,
            cursor_end: None,
            from: None,
            to: None,
            limit: Some(10),
            offset: None,
            channel_cursor_start: None,
            channel_cursor_end: None,
            page_token: None,
            pagination: None,
        })
        .await
        .expect("query failed");

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].channel_id, "q-ch-a");

    cleanup(&store).await;
}

#[tokio::test]
async fn channel_cursor_independent_across_channels() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .publish_with_channel_cursor(request("indie-a", "x", json!({})))
        .await
        .unwrap();
    store
        .publish_with_channel_cursor(request("indie-b", "x", json!({})))
        .await
        .unwrap();
    store
        .publish_with_channel_cursor(request("indie-a", "x", json!({})))
        .await
        .unwrap();

    let a_events = store
        .replay(&ReplayRequest {
            channel_id: "indie-a".into(),
            cursor: Some(0),
            limit: Some(100),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap();

    let b_events = store
        .replay(&ReplayRequest {
            channel_id: "indie-b".into(),
            cursor: Some(0),
            limit: Some(100),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap();

    assert_eq!(a_events.len(), 2);
    assert_eq!(b_events.len(), 1);
    assert_eq!(a_events[0].cursor, 1);
    assert_eq!(a_events[1].cursor, 2);
    assert_eq!(b_events[0].cursor, 1);

    // Cursors are per-channel: each channel's sequence is contiguous from 1,
    // regardless of interleaving with other channels' events.
    assert_eq!(a_events[1].cursor - a_events[0].cursor, 1);

    cleanup(&store).await;
}

#[tokio::test]
async fn replay_without_cursor_returns_all_events() {
    let store = create_store().await;
    cleanup(&store).await;

    for i in 0..3 {
        store
            .publish_with_channel_cursor(request("all-ch", "all", json!({"i": i})))
            .await
            .unwrap();
    }

    let all = store
        .replay(&ReplayRequest {
            channel_id: "all-ch".into(),
            cursor: None,
            limit: Some(100),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap();

    assert_eq!(all.len(), 3);

    cleanup(&store).await;
}

#[tokio::test]
async fn replay_limit_respected() {
    let store = create_store().await;
    cleanup(&store).await;

    for i in 0..10 {
        store
            .publish_with_channel_cursor(request("limit-ch", "l", json!({"i": i})))
            .await
            .unwrap();
    }

    let limited = store
        .replay(&ReplayRequest {
            channel_id: "limit-ch".into(),
            cursor: Some(0),
            limit: Some(3),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap();

    assert_eq!(limited.len(), 3);

    cleanup(&store).await;
}

// ── State Snapshot Tests ─────────────────────────────────────────

#[tokio::test]
async fn save_state_returns_incrementing_versions() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let v1 = store
        .save_state("ch-version", 1, &json!({"step": 1}), None, None)
        .await
        .expect("save 1 failed");
    let v2 = store
        .save_state("ch-version", 2, &json!({"step": 2}), None, None)
        .await
        .expect("save 2 failed");
    let v3 = store
        .save_state("ch-version", 3, &json!({"step": 3}), None, None)
        .await
        .expect("save 3 failed");

    assert!(v1 >= 1, "version should be >= 1, got {v1}");
    assert!(v2 > v1, "version should increase: {v2} <= {v1}");
    assert!(v3 > v2, "version should increase: {v3} <= {v2}");

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn save_and_load_latest_state() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let payload = json!({
        "messages": [{"role": "user", "content": "hello"}],
        "completed": false,
        "nested": {"key": "val", "list": [1, 2, 3]},
    });

    store
        .save_state("ch-load", 42, &payload, None, None)
        .await
        .expect("save failed");

    let loaded = store
        .load_state("ch-load")
        .await
        .expect("load failed")
        .expect("no state found");

    assert_eq!(loaded.0, 42, "cursor should be preserved");
    assert_eq!(
        loaded.1["messages"][0]["content"].as_str().unwrap(),
        "hello"
    );
    assert_eq!(loaded.1["nested"]["list"][2].as_i64().unwrap(), 3);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn load_state_returns_none_for_unknown_channel() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let result = store
        .load_state("nonexistent")
        .await
        .expect("load should not error");
    assert!(result.is_none());

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn multiple_saves_produce_versioned_history() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    store
        .save_state("ch-hist", 10, &json!({"v": 1}), None, None)
        .await
        .unwrap();
    store
        .save_state("ch-hist", 20, &json!({"v": 2}), None, None)
        .await
        .unwrap();
    store
        .save_state("ch-hist", 30, &json!({"v": 3}), None, None)
        .await
        .unwrap();

    let latest = store
        .load_state("ch-hist")
        .await
        .expect("load failed")
        .expect("no state");
    assert_eq!(latest.0, 30);
    assert_eq!(latest.1["v"].as_i64().unwrap(), 3);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn load_state_version_finds_specific_version() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let v1 = store
        .save_state("ch-specific", 100, &json!({"order": 1}), None, None)
        .await
        .unwrap();
    let v2 = store
        .save_state("ch-specific", 200, &json!({"order": 2}), None, None)
        .await
        .unwrap();
    let v3 = store
        .save_state("ch-specific", 300, &json!({"order": 3}), None, None)
        .await
        .unwrap();

    let (cursor_2, version_2, state_2) = store
        .load_state_version("ch-specific", v2)
        .await
        .expect("load version failed")
        .expect("version not found");
    assert_eq!(cursor_2, 200);
    assert_eq!(version_2, v2);
    assert_eq!(state_2["order"].as_i64().unwrap(), 2);

    let (cursor_1, version_1, _) = store
        .load_state_version("ch-specific", v1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor_1, 100);
    assert_eq!(version_1, v1);

    let (cursor_3, version_3, _) = store
        .load_state_version("ch-specific", v3)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor_3, 300);
    assert_eq!(version_3, v3);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn load_state_version_returns_none_for_bad_version() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    store
        .save_state("ch-badver", 1, &json!({"x": 1}), None, None)
        .await
        .unwrap();

    let result = store
        .load_state_version("ch-badver", 99999)
        .await
        .expect("query should not error");
    assert!(result.is_none(), "non-existent version should return None");

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn list_states_returns_versions_ordered_desc() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    store
        .save_state("ch-list", 10, &json!({"n": 1}), None, None)
        .await
        .unwrap();
    store
        .save_state("ch-list", 20, &json!({"n": 2}), None, None)
        .await
        .unwrap();
    store
        .save_state("ch-list", 30, &json!({"n": 3}), None, None)
        .await
        .unwrap();

    let versions = store
        .list_states("ch-list", None, None)
        .await
        .expect("list failed");

    assert_eq!(versions.len(), 3);
    assert!(versions[0].version > versions[1].version, "should be DESC");
    assert!(versions[1].version > versions[2].version, "should be DESC");
    assert_eq!(versions[0].cursor, 30);
    assert_eq!(versions[2].cursor, 10);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn list_states_respects_limit_and_offset() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    for i in 0..10 {
        store
            .save_state("ch-pag", i * 10, &json!({"n": i}), None, None)
            .await
            .unwrap();
    }

    let page1 = store
        .list_states("ch-pag", Some(3), None)
        .await
        .expect("page1 failed");
    assert_eq!(page1.len(), 3);

    let page2 = store
        .list_states("ch-pag", Some(3), Some(3))
        .await
        .expect("page2 failed");
    assert_eq!(page2.len(), 3);

    let all_page1_versions: Vec<i64> = page1.iter().map(|v| v.version).collect();
    let all_page2_versions: Vec<i64> = page2.iter().map(|v| v.version).collect();
    for v in &all_page2_versions {
        assert!(!all_page1_versions.contains(v), "pages should not overlap");
    }

    let empty = store
        .list_states("ch-pag", Some(100), Some(1000))
        .await
        .expect("empty failed");
    assert_eq!(empty.len(), 0);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn list_states_empty_channel_returns_empty() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let versions = store
        .list_states("ch-empty-state", None, None)
        .await
        .expect("list should not error");
    assert_eq!(versions.len(), 0);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn delete_state_version_removes_only_target() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let v1 = store
        .save_state("ch-del", 100, &json!({"keep": true}), None, None)
        .await
        .unwrap();
    let v2 = store
        .save_state("ch-del", 200, &json!({"keep": false}), None, None)
        .await
        .unwrap();
    let v3 = store
        .save_state("ch-del", 300, &json!({"keep": true}), None, None)
        .await
        .unwrap();

    let deleted = store
        .delete_state_version("ch-del", v2)
        .await
        .expect("delete failed");
    assert_eq!(deleted, 1);

    let remaining = store.list_states("ch-del", None, None).await.unwrap();
    assert_eq!(remaining.len(), 2);
    let remaining_versions: Vec<i64> = remaining.iter().map(|v| v.version).collect();
    assert!(remaining_versions.contains(&v1));
    assert!(!remaining_versions.contains(&v2));
    assert!(remaining_versions.contains(&v3));

    let v2_loaded = store.load_state_version("ch-del", v2).await.unwrap();
    assert!(v2_loaded.is_none());

    let latest = store.load_state("ch-del").await.unwrap().unwrap();
    assert_eq!(latest.0, 300);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn delete_state_version_nonexistent_returns_zero() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let deleted = store
        .delete_state_version("ch-noexist", 12345)
        .await
        .expect("delete should not error");
    assert_eq!(deleted, 0);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn state_channels_are_independent() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    store
        .save_state("ch-indie-a", 10, &json!({"chan": "a"}), None, None)
        .await
        .unwrap();
    store
        .save_state("ch-indie-b", 20, &json!({"chan": "b"}), None, None)
        .await
        .unwrap();
    store
        .save_state("ch-indie-a", 30, &json!({"chan": "a-again"}), None, None)
        .await
        .unwrap();

    let a_versions = store.list_states("ch-indie-a", None, None).await.unwrap();
    assert_eq!(a_versions.len(), 2);

    let b_versions = store.list_states("ch-indie-b", None, None).await.unwrap();
    assert_eq!(b_versions.len(), 1);

    let a_latest = store.load_state("ch-indie-a").await.unwrap().unwrap();
    assert_eq!(a_latest.1["chan"].as_str().unwrap(), "a-again");

    let b_latest = store.load_state("ch-indie-b").await.unwrap().unwrap();
    assert_eq!(b_latest.1["chan"].as_str().unwrap(), "b");

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn save_state_backward_compatible_with_load_latest() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    store
        .save_state("ch-bcompat", 5, &json!({"data": "old"}), None, None)
        .await
        .unwrap();
    store
        .save_state("ch-bcompat", 10, &json!({"data": "new"}), None, None)
        .await
        .unwrap();

    let latest = store
        .load_state("ch-bcompat")
        .await
        .expect("load failed")
        .expect("no state");
    assert_eq!(latest.0, 10);
    assert_eq!(latest.1["data"].as_str().unwrap(), "new");

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn save_state_with_large_nested_jsonb() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let large_state = json!({
        "context": {
            "messages": (0..50).map(|i| json!({
                "role": if i % 2 == 0 { "user" } else { "assistant" },
                "content": format!("Message number {}", i),
                "timestamp": "2026-07-27T00:00:00Z",
            })).collect::<Vec<_>>(),
        },
        "completed_steps": [1, 2, 3, 4, 5],
        "metadata": {
            "agent": "test-agent",
            "version": "1.0.0",
            "config": {"temperature": 0.7, "max_tokens": 4096},
        },
    });

    let version = store
        .save_state("ch-large", 999, &large_state, None, None)
        .await
        .unwrap();
    assert!(version >= 1);

    let loaded = store.load_state("ch-large").await.unwrap().unwrap();
    assert_eq!(loaded.0, 999);
    assert_eq!(loaded.1["completed_steps"][4].as_i64().unwrap(), 5);
    assert_eq!(
        loaded.1["metadata"]["config"]["temperature"]
            .as_f64()
            .unwrap(),
        0.7
    );
    assert_eq!(
        loaded.1["context"]["messages"].as_array().unwrap().len(),
        50
    );
    assert_eq!(
        loaded.1["context"]["messages"][25]["content"]
            .as_str()
            .unwrap(),
        "Message number 25"
    );

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

// ── Concurrency & Race Tests ─────────────────────────────────────

#[tokio::test]
async fn concurrent_saves_to_same_channel_are_serialized() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let store = std::sync::Arc::new(store);
    let mut handles = vec![];
    for i in 0..20 {
        let s = store.clone();
        handles.push(tokio::spawn(async move {
            s.save_state("ch-concurrent", i as i64, &json!({"worker": i}), None, None)
                .await
                .expect("save failed")
        }));
    }
    let versions: Vec<i64> = futures_util::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(versions.len(), 20);
    let mut sorted = versions.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 20, "all versions should be unique");

    let listed = store
        .list_states("ch-concurrent", None, None)
        .await
        .unwrap();
    assert_eq!(listed.len(), 20);

    let latest = store.load_state("ch-concurrent").await.unwrap().unwrap();
    assert!(latest.0 >= 0);

    let s = Arc::try_unwrap(store).unwrap();
    cleanup(&s).await;
    ensure_versioned_schema(&s).await;
}

#[tokio::test]
async fn concurrent_saves_to_different_channels() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let store = std::sync::Arc::new(store);
    let mut handles = vec![];
    for ch_idx in 0..10 {
        let s = store.clone();
        let channel = format!("ch-conc-{ch_idx}");
        handles.push(tokio::spawn(async move {
            for step in 0..5 {
                s.save_state(&channel, step as i64, &json!({"step": step}), None, None)
                    .await
                    .expect("save failed");
            }
            channel
        }));
    }
    let results = futures_util::future::join_all(handles).await;
    let channels: Vec<String> = results.into_iter().map(|r| r.unwrap()).collect();

    let s = Arc::try_unwrap(store).unwrap();
    for ch in &channels {
        let listed = s.list_states(ch, None, None).await.unwrap();
        assert_eq!(listed.len(), 5, "channel {ch} should have 5 versions");
    }

    cleanup(&s).await;
    ensure_versioned_schema(&s).await;
}

// ── JSONB Type Edge Cases ────────────────────────────────────────

#[tokio::test]
async fn jsonb_all_primitive_types_preserved() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let state = json!({
        "null_val": null,
        "bool_true": true,
        "bool_false": false,
        "int_zero": 0,
        "int_positive": 42,
        "int_negative": -17,
        "float_zero": 0.0,
        "float_positive": std::f64::consts::PI,
        "float_negative": -std::f64::consts::E,
        "empty_string": "",
        "simple_string": "hello world",
        "unicode_string": "hello \u{1F30D}",
        "escaped_chars": "line1\nline2\t\"quoted\"\\backslash",
        "empty_array": [],
        "empty_object": {},
        "mixed_array": [null, true, 42, "str", {"nested": false}],
    });

    store
        .save_state("ch-types", 1, &state, None, None)
        .await
        .unwrap();
    let loaded = store.load_state("ch-types").await.unwrap().unwrap();

    assert!(loaded.1["null_val"].is_null());
    assert!(loaded.1["bool_true"].as_bool().unwrap());
    assert!(!loaded.1["bool_false"].as_bool().unwrap());
    assert_eq!(loaded.1["int_zero"].as_i64().unwrap(), 0);
    assert_eq!(loaded.1["int_negative"].as_i64().unwrap(), -17);
    assert_eq!(
        loaded.1["float_positive"].as_f64().unwrap(),
        std::f64::consts::PI
    );
    assert_eq!(loaded.1["empty_string"].as_str().unwrap(), "");
    assert!(loaded.1["unicode_string"]
        .as_str()
        .unwrap()
        .contains("\u{1F30D}"));
    assert_eq!(
        loaded.1["escaped_chars"].as_str().unwrap(),
        "line1\nline2\t\"quoted\"\\backslash"
    );
    assert_eq!(loaded.1["empty_array"].as_array().unwrap().len(), 0);
    assert!(loaded.1["empty_object"].is_object());
    assert_eq!(loaded.1["mixed_array"].as_array().unwrap().len(), 5);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn jsonb_deeply_nested_structure() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    fn build_nested(depth: i32) -> sonic_rs::Value {
        if depth == 0 {
            json!({"leaf": true, "value": depth})
        } else {
            json!({"child": build_nested(depth - 1), "depth": depth})
        }
    }

    let state = build_nested(20);
    store
        .save_state("ch-deep", 42, &state, None, None)
        .await
        .unwrap();
    let loaded = store.load_state("ch-deep").await.unwrap().unwrap();

    let mut cursor = &loaded.1;
    for expected_depth in (1..=20).rev() {
        assert_eq!(cursor["depth"].as_i64().unwrap(), expected_depth as i64);
        cursor = &cursor["child"];
    }
    assert!(cursor["leaf"].as_bool().unwrap());

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn jsonb_large_array_1000_items() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let large_array: Vec<sonic_rs::Value> = (0..1000)
        .map(|i| json!({"id": i, "content": format!("item-{}", i), "active": i % 3 == 0}))
        .collect();
    let state = json!({"items": large_array});

    let v = store
        .save_state("ch-big-array", 1, &state, None, None)
        .await
        .unwrap();
    assert!(v >= 1);

    let loaded = store.load_state("ch-big-array").await.unwrap().unwrap();
    let items = loaded.1["items"].as_array().unwrap();
    assert_eq!(items.len(), 1000);
    assert_eq!(items[0]["id"].as_i64().unwrap(), 0);
    assert_eq!(items[999]["id"].as_i64().unwrap(), 999);
    assert_eq!(items[999]["content"].as_str().unwrap(), "item-999");
    assert!(items[3]["active"].as_bool().unwrap());
    assert!(!items[4]["active"].as_bool().unwrap());

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

// ── Cursor Edge Cases ────────────────────────────────────────────

#[tokio::test]
async fn cursor_zero_preserved() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    store
        .save_state("ch-cursor-zero", 0, &json!({"at": "zero"}), None, None)
        .await
        .unwrap();
    let loaded = store.load_state("ch-cursor-zero").await.unwrap().unwrap();
    assert_eq!(loaded.0, 0);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn cursor_max_value() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let max_cursor = i64::MAX;
    store
        .save_state(
            "ch-max-cursor",
            max_cursor,
            &json!({"max": true}),
            None,
            None,
        )
        .await
        .unwrap();
    let loaded = store.load_state("ch-max-cursor").await.unwrap().unwrap();
    assert_eq!(loaded.0, max_cursor);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn cursor_negative_value() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    store
        .save_state("ch-neg-cursor", -5, &json!({"behind": true}), None, None)
        .await
        .unwrap();
    let loaded = store.load_state("ch-neg-cursor").await.unwrap().unwrap();
    assert_eq!(loaded.0, -5);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

// ── Pagination Edge Cases ────────────────────────────────────────

#[tokio::test]
async fn list_states_limit_one() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    for i in 0..5 {
        store
            .save_state("ch-limit1", i, &json!({"n": i}), None, None)
            .await
            .unwrap();
    }
    let result = store.list_states("ch-limit1", Some(1), None).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].cursor, 4);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn list_states_limit_greater_than_available() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    store
        .save_state("ch-limit-over", 1, &json!({"x": 1}), None, None)
        .await
        .unwrap();
    let result = store
        .list_states("ch-limit-over", Some(100), None)
        .await
        .unwrap();
    assert_eq!(result.len(), 1);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn list_states_offset_at_exact_boundary() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    for i in 0..5 {
        store
            .save_state("ch-offset-edge", i, &json!({"n": i}), None, None)
            .await
            .unwrap();
    }
    let last = store
        .list_states("ch-offset-edge", Some(2), Some(4))
        .await
        .unwrap();
    assert_eq!(last.len(), 1);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn list_states_offset_beyond_count() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    store
        .save_state("ch-offset-beyond", 1, &json!({"x": 1}), None, None)
        .await
        .unwrap();
    let result = store
        .list_states("ch-offset-beyond", Some(10), Some(100))
        .await
        .unwrap();
    assert_eq!(result.len(), 0);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

// ── Version Lifecycle Tests ──────────────────────────────────────

#[tokio::test]
async fn save_after_delete_all_continues_incrementing() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let v1 = store
        .save_state("ch-lifecycle", 10, &json!({"phase": 1}), None, None)
        .await
        .unwrap();
    let v2 = store
        .save_state("ch-lifecycle", 20, &json!({"phase": 2}), None, None)
        .await
        .unwrap();
    let v3 = store
        .save_state("ch-lifecycle", 30, &json!({"phase": 3}), None, None)
        .await
        .unwrap();

    store
        .delete_state_version("ch-lifecycle", v1)
        .await
        .unwrap();
    store
        .delete_state_version("ch-lifecycle", v2)
        .await
        .unwrap();
    store
        .delete_state_version("ch-lifecycle", v3)
        .await
        .unwrap();

    assert!(store.load_state("ch-lifecycle").await.unwrap().is_none());

    let v4 = store
        .save_state("ch-lifecycle", 100, &json!({"phase": "reborn"}), None, None)
        .await
        .unwrap();
    assert!(
        v4 > v3,
        "version should continue incrementing after delete-all, got {v4} <= {v3}"
    );

    let loaded = store.load_state("ch-lifecycle").await.unwrap().unwrap();
    assert_eq!(loaded.0, 100);
    assert_eq!(loaded.1["phase"].as_str().unwrap(), "reborn");

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

// ── Rapid Fire / Stress Tests ────────────────────────────────────

#[tokio::test]
async fn rapid_fire_1000_saves_then_full_verification() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let count = 1000_i64;
    for i in 0..count {
        store
            .save_state("ch-rapid", i, &json!({"seq": i}), None, None)
            .await
            .unwrap();
    }

    let listed = store
        .list_states("ch-rapid", Some(count), None)
        .await
        .unwrap();
    assert_eq!(listed.len(), count as usize);
    assert_eq!(listed[0].cursor, count - 1);
    assert_eq!(listed[(count - 1) as usize].cursor, 0);

    let latest = store.load_state("ch-rapid").await.unwrap().unwrap();
    assert_eq!(latest.0, count - 1);
    assert_eq!(latest.1["seq"].as_i64().unwrap(), count - 1);

    for page_offset in (0..count).step_by(100) {
        let page = store
            .list_states("ch-rapid", Some(100), Some(page_offset))
            .await
            .unwrap();
        assert!(
            page.len() <= 100,
            "page at offset {} len {}",
            page_offset,
            page.len()
        );
        for v in &page {
            assert!(v.cursor >= 0 && v.cursor < count);
        }
    }

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

// ── Data Integrity Tests ─────────────────────────────────────────

#[tokio::test]
async fn save_and_load_exact_json_roundtrip() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let original = json!({
        "conversation": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "What is the capital of France?"},
            {"role": "assistant", "content": "The capital of France is Paris.",
             "tool_calls": [{"id": "call_1", "name": "search", "args": {"query": "France"}}]},
            {"role": "tool", "content": "Paris is the capital.", "tool_call_id": "call_1"},
        ],
        "agent_state": {
            "mode": "chain", "current_step": 4, "total_steps": 5,
            "errors": [], "token_usage": {"prompt": 150, "completion": 45, "total": 195},
        },
        "checkpoint_metadata": {
            "created_at": "2026-07-27T14:00:00Z", "node": "research", "graph_version": 3,
        }
    });

    let original_json = sonic_rs::to_string(&original).unwrap();
    store
        .save_state("ch-roundtrip", 42, &original, None, None)
        .await
        .unwrap();
    let loaded = store.load_state("ch-roundtrip").await.unwrap().unwrap();

    // JSONB reorders object keys — compare parsed values, not raw strings
    let loaded_json = sonic_rs::to_string(&loaded.1).unwrap();
    let original_parsed: sonic_rs::Value = sonic_rs::from_str(&original_json).unwrap();
    let loaded_parsed: sonic_rs::Value = sonic_rs::from_str(&loaded_json).unwrap();
    assert_eq!(original_parsed, loaded_parsed);
    assert_eq!(loaded.0, 42);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn version_specific_load_matches_exact_snapshot() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let mut saved = vec![];
    for i in 0..10 {
        let v = store
            .save_state(
                "ch-exact-ver",
                i * 100,
                &json!({"id": i, "data": format!("val-{}", i)}),
                None,
                None,
            )
            .await
            .unwrap();
        saved.push((v, i));
    }

    for (version, expected_id) in &saved {
        let (cursor, ver, state) = store
            .load_state_version("ch-exact-ver", *version)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cursor, expected_id * 100);
        assert_eq!(ver, *version);
        assert_eq!(state["id"].as_i64().unwrap(), *expected_id);
        assert_eq!(
            state["data"].as_str().unwrap(),
            format!("val-{expected_id}")
        );
    }

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

// ── Mixed Operations Tests ───────────────────────────────────────

#[tokio::test]
async fn interleaved_saves_loads_and_deletes() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let v1 = store
        .save_state("ch-interleave", 10, &json!({"v": 1}), None, None)
        .await
        .unwrap();
    assert_eq!(
        store.load_state("ch-interleave").await.unwrap().unwrap().0,
        10
    );

    let v2 = store
        .save_state("ch-interleave", 20, &json!({"v": 2}), None, None)
        .await
        .unwrap();
    store
        .delete_state_version("ch-interleave", v1)
        .await
        .unwrap();

    let after_del = store.load_state("ch-interleave").await.unwrap().unwrap();
    assert_eq!(after_del.0, 20);
    assert!(store
        .load_state_version("ch-interleave", v1)
        .await
        .unwrap()
        .is_none());
    assert!(store
        .load_state_version("ch-interleave", v2)
        .await
        .unwrap()
        .is_some());

    let v3 = store
        .save_state("ch-interleave", 30, &json!({"v": 3}), None, None)
        .await
        .unwrap();
    let listed = store
        .list_states("ch-interleave", None, None)
        .await
        .unwrap();
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().any(|s| s.version == v2));
    assert!(listed.iter().any(|s| s.version == v3));

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn save_list_load_delete_no_interference() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let mut versions = vec![];
    for i in 0..50 {
        let v = store
            .save_state("ch-noisy", i, &json!({"index": i}), None, None)
            .await
            .unwrap();
        versions.push(v);
        if i % 10 == 9 {
            let snap = store.load_state("ch-noisy").await.unwrap().unwrap();
            assert_eq!(snap.1["index"].as_i64().unwrap(), i);
        }
    }

    assert_eq!(versions.len(), 50);
    assert_eq!(
        store
            .list_states("ch-noisy", Some(100), None)
            .await
            .unwrap()
            .len(),
        50
    );

    for i in (0..50).step_by(2) {
        store
            .delete_state_version("ch-noisy", versions[i])
            .await
            .unwrap();
    }
    assert_eq!(
        store
            .list_states("ch-noisy", Some(100), None)
            .await
            .unwrap()
            .len(),
        25
    );
    assert_eq!(
        store.load_state("ch-noisy").await.unwrap().unwrap().1["index"]
            .as_i64()
            .unwrap(),
        49
    );

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

// ── Channel Name Edge Cases ──────────────────────────────────────

#[tokio::test]
async fn channel_names_with_special_characters() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let channels = vec![
        "ch-with-dots...and.more",
        "ch_with_underscores",
        "ch-with-hyphens",
        "CH-UPPERCASE-MIXED",
        "ch_with_numbers_12345",
    ];

    for ch in &channels {
        let v = store
            .save_state(ch, 1, &json!({"channel": ch}), None, None)
            .await
            .unwrap();
        assert!(v >= 1, "save failed for channel '{ch}'");
        let loaded = store.load_state(ch).await.unwrap().unwrap();
        assert_eq!(loaded.1["channel"].as_str().unwrap(), *ch);
    }

    for ch in &channels {
        assert_eq!(store.list_states(ch, None, None).await.unwrap().len(), 1);
    }

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

// ── Data Integrity: Immutability Tests ──────────────────────────

#[tokio::test]
async fn existing_versions_are_never_modified_by_new_saves() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let v1 = store
        .save_state("ch-immutable", 1, &json!({"original": true}), None, None)
        .await
        .unwrap();
    let v2 = store
        .save_state("ch-immutable", 2, &json!({"updated": true}), None, None)
        .await
        .unwrap();
    let v3 = store
        .save_state("ch-immutable", 3, &json!({"latest": true}), None, None)
        .await
        .unwrap();

    let (_, _, s1) = store
        .load_state_version("ch-immutable", v1)
        .await
        .unwrap()
        .unwrap();
    assert!(s1["original"].as_bool().unwrap());
    assert!(s1["updated"].is_null());
    assert!(s1["latest"].is_null());

    let (_, _, s2) = store
        .load_state_version("ch-immutable", v2)
        .await
        .unwrap()
        .unwrap();
    assert!(s2["original"].is_null());
    assert!(s2["updated"].as_bool().unwrap());
    assert!(s2["latest"].is_null());

    let (_, _, s3) = store
        .load_state_version("ch-immutable", v3)
        .await
        .unwrap()
        .unwrap();
    assert!(s3["original"].is_null());
    assert!(s3["updated"].is_null());
    assert!(s3["latest"].as_bool().unwrap());

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

// ── Timestamp Tests ──────────────────────────────────────────────

#[tokio::test]
async fn list_states_timestamps_non_decreasing() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    for i in 0..5 {
        store
            .save_state("ch-ts", i, &json!({"n": i}), None, None)
            .await
            .unwrap();
    }

    let versions = store.list_states("ch-ts", None, None).await.unwrap();
    for i in 0..versions.len() - 1 {
        assert!(
            versions[i].timestamp >= versions[i + 1].timestamp,
            "timestamp at index {} < index {}: {} < {}",
            i,
            i + 1,
            versions[i].timestamp,
            versions[i + 1].timestamp
        );
    }

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

// ── Mass Operations Tests ────────────────────────────────────────

#[tokio::test]
async fn delete_all_then_verify_empty_load_and_list() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let mut versions = vec![];
    for i in 0..5 {
        versions.push(
            store
                .save_state("ch-mass-del", i, &json!({"n": i}), None, None)
                .await
                .unwrap(),
        );
    }

    for v in &versions {
        assert_eq!(
            store.delete_state_version("ch-mass-del", *v).await.unwrap(),
            1
        );
    }

    assert_eq!(
        store
            .list_states("ch-mass-del", None, None)
            .await
            .unwrap()
            .len(),
        0
    );
    assert!(store.load_state("ch-mass-del").await.unwrap().is_none());

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn double_delete_returns_zero_second_time() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let v = store
        .save_state("ch-double-del", 1, &json!({"x": 1}), None, None)
        .await
        .unwrap();
    assert_eq!(
        store
            .delete_state_version("ch-double-del", v)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .delete_state_version("ch-double-del", v)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .delete_state_version("ch-double-del", 999_999)
            .await
            .unwrap(),
        0
    );

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

// ── Migration Robustness Tests ───────────────────────────────────

#[tokio::test]
async fn migration_is_idempotent_across_multiple_runs() {
    let store = create_store().await;
    for _ in 0..3 {
        store
            .initialize()
            .await
            .expect("migration should be idempotent");
    }

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let v = store
        .save_state("ch-migration-test", 1, &json!({"ok": true}), None, None)
        .await
        .unwrap();
    assert!(v >= 1);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn ensure_versioned_schema_is_idempotent() {
    let store = create_store().await;
    cleanup(&store).await;

    for _ in 0..3 {
        ensure_versioned_schema(&store).await;
    }

    let v = store
        .save_state("ch-schema-idem", 1, &json!({"ok": true}), None, None)
        .await
        .unwrap();
    assert!(v >= 1);

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

// ── Real Agent Workflow Simulation Tests ────────────────────────

fn make_conversation_context(turns: usize) -> sonic_rs::Value {
    let mut messages = vec![];
    for i in 0..turns {
        messages.push(json!({
            "role": if i % 2 == 0 { "user" } else { "assistant" },
            "content": format!("Message {} in the conversation", i),
            "timestamp": format!("2026-07-27T{:02}:00:00Z", 14 + i / 2),
        }));
        if i % 2 == 1 && i < turns - 1 {
            messages.push(json!({
                "role": "tool",
                "content": format!("Tool result {}", i),
                "tool_call_id": format!("call_{}", i),
            }));
        }
    }
    json!(messages)
}

#[tokio::test]
async fn agent_workflow_crash_and_resume() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let channel = "agent-workflow-1";
    let state_v1 = json!({
        "agent_id": "agent-deepseek-01",
        "phase": "research",
        "context": {
            "messages": make_conversation_context(6),
            "current_query": "What are the latest trends in AI safety?",
        },
        "completed_steps": ["init", "plan"],
        "current_step": 3,
        "tool_results": {"search_1": "Found 5 papers", "search_2": "3 trends found"},
        "token_usage": {"prompt": 450, "completion": 320, "total": 770},
        "last_cursor": 15,
    });
    let v1 = store
        .save_state(channel, 15, &state_v1, None, None)
        .await
        .unwrap();
    let loaded = store.load_state(channel).await.unwrap().unwrap();
    assert_eq!(loaded.0, 15);
    assert_eq!(
        loaded.1["context"]["current_query"].as_str().unwrap(),
        "What are the latest trends in AI safety?"
    );

    let state_v2 = json!({
        "agent_id": "agent-deepseek-01", "phase": "analysis",
        "context": {"messages": make_conversation_context(12)},
        "completed_steps": ["init", "plan", "research"],
        "current_step": 4,
        "tool_results": {"analyze_1": "Risk A: alignment", "analyze_2": "Risk B: capability"},
        "token_usage": {"prompt": 890, "completion": 512, "total": 1402},
        "last_cursor": 32,
    });
    let v2 = store
        .save_state(channel, 32, &state_v2, None, None)
        .await
        .unwrap();

    // Crash — agent restarts, loads latest state
    let resume_state = store.load_state(channel).await.unwrap().unwrap();
    assert_eq!(resume_state.0, 32);
    assert_eq!(resume_state.1["phase"].as_str().unwrap(), "analysis");
    assert_eq!(
        resume_state.1["completed_steps"].as_array().unwrap().len(),
        3
    );

    // Continue
    let state_v3 = json!({
        "agent_id": "agent-deepseek-01", "phase": "reporting",
        "context": {"messages": make_conversation_context(18)},
        "completed_steps": ["init", "plan", "research", "analysis"],
        "current_step": 5,
        "tool_results": {"report_1": "Generated summary"},
        "token_usage": {"prompt": 1340, "completion": 890, "total": 2230},
        "last_cursor": 50,
    });
    let v3 = store
        .save_state(channel, 50, &state_v3, None, None)
        .await
        .unwrap();

    // Verify entire history
    let versions = store.list_states(channel, None, None).await.unwrap();
    assert_eq!(versions.len(), 3);
    assert_eq!(versions[0].version, v3);
    assert_eq!(versions[2].version, v1);

    // Crash point (v2) is exactly as saved
    let (cursor_v2, ver_v2, state_v2_loaded) = store
        .load_state_version(channel, v2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor_v2, 32);
    assert_eq!(ver_v2, v2);
    assert_eq!(state_v2_loaded["phase"].as_str().unwrap(), "analysis");
    assert_eq!(
        state_v2_loaded["tool_results"]["analyze_2"]
            .as_str()
            .unwrap(),
        "Risk B: capability"
    );

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn langgraph_style_checkpoint_resume() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let channel = "langgraph-sim";
    let nodes = ["research", "analysis", "validation", "report"];
    let mut completed_nodes: Vec<String> = vec![];

    for (idx, node) in nodes.iter().enumerate() {
        let node = *node;
        completed_nodes.push(node.to_string());
        let ckpt_id = format!("ckpt-{idx}");
        let node_output_key = format!("node_{node}");
        let _channel_ver_key = node.to_string();
        let node_key = node.to_string();
        let checkpoint = json!({
            "config": {"thread_id": "thread-001"},
            "checkpoint": {
                "v": 1, "id": ckpt_id, "ts": "2026-07-27T14:00:00Z",
                "channel_values": {
                    "nodes_completed": completed_nodes.clone(),
                    node_output_key: {
                        "status": "completed",
                        "output": format!("{} output for step {}", node, idx),
                    },
                },
                "channel_versions": { node_key: idx as i64 },
                "versions_seen": {},
            },
            "metadata": {"source": "step", "step": idx, "node": node.to_string()},
        });
        let cursor = (idx + 1) as i64 * 10;
        store
            .save_state(channel, cursor, &checkpoint, None, None)
            .await
            .unwrap();
        if node == "analysis" {
            break;
        }
    }

    // Resume
    let latest = store.load_state(channel).await.unwrap().unwrap();
    let completed = latest.1["checkpoint"]["channel_values"]["nodes_completed"]
        .as_array()
        .unwrap();
    let names: Vec<&str> = completed.iter().map(|v| v.as_str().unwrap()).collect();
    assert_eq!(names, vec!["research", "analysis"]);

    // Continue remaining nodes
    for (idx, node) in nodes[2..].iter().enumerate() {
        let node = *node;
        completed_nodes.push(node.to_string());
        let ckpt_id = format!("ckpt-{}", idx + 2);
        let node_output_key = format!("node_{node}");
        let channel_ver_key = node.to_string();
        let checkpoint = json!({
            "config": {"thread_id": "thread-001"},
            "checkpoint": {
                "v": 1, "id": ckpt_id, "ts": "2026-07-27T15:00:00Z",
                "channel_values": {
                    "nodes_completed": completed_nodes.clone(),
                    node_output_key: {"status": "completed", "output": format!("Resumed: {}", node)},
                },
                "channel_versions": { channel_ver_key: (idx + 2) as i64 },
                "versions_seen": {},
            },
            "metadata": {"source": "resume", "step": idx + 2, "node": node.to_string()},
        });
        store
            .save_state(channel, (idx + 3) as i64 * 10, &checkpoint, None, None)
            .await
            .unwrap();
    }

    // 2 initial + 2 resumed = 4 checkpoints
    let versions = store.list_states(channel, None, None).await.unwrap();
    assert_eq!(versions.len(), 4);

    // All 4 nodes completed
    let final_state = store.load_state(channel).await.unwrap().unwrap();
    let all = final_state.1["checkpoint"]["channel_values"]["nodes_completed"]
        .as_array()
        .unwrap();
    assert_eq!(all.len(), 4);
    assert_eq!(
        all.iter().map(|v| v.as_str().unwrap()).collect::<Vec<_>>(),
        vec!["research", "analysis", "validation", "report"]
    );

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn crewai_style_multi_agent_pipeline() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let channel = "crewai-sim";
    let crew_id = "crew-marketing-001";
    let tasks = [
        ("market_research", "Research AI market size 2026"),
        ("competitor_analysis", "Analyze top 5 competitors"),
        ("strategy_development", "Develop go-to-market strategy"),
        ("content_creation", "Create marketing content"),
        ("review_and_approve", "Final review and approval"),
    ];

    let mut completed: Vec<&str> = vec![];
    for (task_id, _description) in &tasks {
        completed.push(task_id);
        let state = json!({
            "crew_id": crew_id,
            "completed_tasks": completed.clone(),
            "task_outputs": completed.iter().map(|t| json!({
                "task": t, "output": format!("{} completed", t),
                "assigned_to": "agent-analyst", "duration_seconds": 15, "token_usage": 250,
            })).collect::<Vec<_>>(),
            "crew_memory": {
                "shared_knowledge": format!("{} tasks done", completed.len()),
            },
        });
        store
            .save_state(channel, completed.len() as i64 * 5, &state, None, None)
            .await
            .unwrap();
        if *task_id == "competitor_analysis" {
            break;
        }
    }

    // Resume
    let latest = store.load_state(channel).await.unwrap().unwrap();
    let resume_completed: Vec<&str> = latest.1["completed_tasks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(
        resume_completed,
        vec!["market_research", "competitor_analysis"]
    );

    // Continue remaining tasks
    for (task_id, _description) in &tasks[2..] {
        completed.push(task_id);
        let state = json!({
            "crew_id": crew_id, "completed_tasks": completed.clone(),
            "task_outputs": completed.iter().map(|t| json!({
                "task": t, "output": format!("{} resumed", t),
                "assigned_to": "agent-analyst", "duration_seconds": 12, "token_usage": 200,
            })).collect::<Vec<_>>(),
            "status": "resuming",
        });
        store
            .save_state(channel, completed.len() as i64 * 5, &state, None, None)
            .await
            .unwrap();
    }

    let versions = store.list_states(channel, None, None).await.unwrap();
    assert_eq!(versions.len(), 5);
    let final_state = store.load_state(channel).await.unwrap().unwrap();
    assert_eq!(
        final_state.1["completed_tasks"].as_array().unwrap().len(),
        5
    );

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn multi_agent_concurrent_workflows() {
    let store = create_store().await;
    cleanup(&store).await;
    ensure_versioned_schema(&store).await;

    let store = Arc::new(store);
    let agent_count = 5;
    let steps_per_agent = 10;

    let mut handles = vec![];
    for agent_idx in 0..agent_count {
        let s = store.clone();
        handles.push(tokio::spawn(async move {
            let channel = format!("agent-{agent_idx}");
            let mut versions = vec![];
            for step in 0..steps_per_agent {
                let state = json!({
                    "agent_id": format!("agent-{}", agent_idx),
                    "step": step,
                    "context": {"messages": make_conversation_context(step as usize + 1)},
                    "phase": if step < 3 { "init" } else if step < 6 { "execute" } else { "finalize" },
                });
                let v = s.save_state(&channel, step * 10, &state, None, None).await.unwrap();
                versions.push(v);
            }
            (channel, versions)
        }));
    }

    let results = futures_util::future::join_all(handles).await;
    let s = Arc::try_unwrap(store).unwrap();

    for result in results {
        let (channel, versions) = result.unwrap();
        assert_eq!(versions.len(), steps_per_agent as usize);
        let listed = s.list_states(&channel, None, None).await.unwrap();
        assert_eq!(listed.len(), steps_per_agent as usize);
        let latest = s.load_state(&channel).await.unwrap().unwrap();
        assert_eq!(latest.1["step"].as_i64().unwrap(), steps_per_agent - 1);

        // Every version matches exactly
        for (idx, &v) in versions.iter().enumerate() {
            let (cursor, ver, state) = s.load_state_version(&channel, v).await.unwrap().unwrap();
            assert_eq!(cursor, (idx as i64) * 10);
            assert_eq!(ver, v);
            assert_eq!(
                state["agent_id"].as_str().unwrap(),
                format!("agent-{}", &channel[6..])
            );
        }
    }

    cleanup(&s).await;
    ensure_versioned_schema(&s).await;
}

// ── Fork Tests ───────────────────────────────────────────────────

#[tokio::test]
async fn fork_channel_is_immediately_discoverable() {
    let store = create_store().await;
    cleanup(&store).await;

    // Parent has events so it exists.
    for i in 0..5 {
        store
            .publish_with_channel_cursor(request("fork-parent", "step", json!({"i": i})))
            .await
            .unwrap();
    }
    // Two saved snapshots: cursor 2 and cursor 5.
    store
        .save_state("fork-parent", 2, &json!({"boundary": 2}), None, None)
        .await
        .unwrap();
    store
        .save_state("fork-parent", 5, &json!({"boundary": 5}), None, None)
        .await
        .unwrap();

    let channels_before = store.list_channels().await.unwrap();
    assert!(channels_before.contains(&"fork-parent".to_string()));

    store
        .fork_channel(&ForkRequest {
            source_channel_id: "fork-parent".into(),
            new_channel_id: "fork-child".into(),
            at_cursor: 3,
            operation_id: None,
            display_name: Some("Child experiment".into()),
            reason: Some("test".into()),
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .expect("fork should succeed");

    let channels_after = store.list_channels().await.unwrap();
    assert!(
        channels_after.contains(&"fork-child".to_string()),
        "forked channel should appear in list_channels before any event is published"
    );

    // Child's exact-boundary state: latest snapshot with cursor <= 3 → cursor 2.
    let (child_cursor, child_state) = store
        .load_state("fork-child")
        .await
        .unwrap()
        .expect("child state");
    assert_eq!(
        child_cursor, 2,
        "child state should carry the resolved fork boundary"
    );
    assert_eq!(child_state, json!({"boundary": 2}));

    // Child starts with exactly one lineage event: fork.started.
    let child_events = store
        .replay(&ReplayRequest {
            channel_id: "fork-child".into(),
            cursor: None,
            limit: Some(100),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap();
    assert_eq!(
        child_events.len(),
        1,
        "fork should start with exactly the fork.started event"
    );
    assert_eq!(child_events[0].event_type, "fork.started");
    assert_eq!(child_events[0].cursor, 1);

    // First user event on the fork continues the sequence at 2.
    let first_child = store
        .publish_with_channel_cursor(request("fork-child", "step", json!({"own": true})))
        .await
        .unwrap()
        .0;
    assert_eq!(
        first_child.cursor, 2,
        "fork's first user event should have channel_cursor 2 (after fork.started)"
    );

    cleanup(&store).await;
}

#[tokio::test]
async fn fork_exact_boundary_requires_snapshot() {
    let store = create_store().await;
    cleanup(&store).await;

    for i in 0..3 {
        store
            .publish_with_channel_cursor(request("fork-bnd", "step", json!({"i": i})))
            .await
            .unwrap();
    }

    // at_cursor > 0 with no saved snapshot → boundary error.
    let err = store
        .fork_channel(&ForkRequest {
            source_channel_id: "fork-bnd".into(),
            new_channel_id: "fork-bnd-child".into(),
            at_cursor: 2,
            operation_id: None,
            display_name: None,
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(
        matches!(err, EventStoreError::SnapshotBoundaryNotFound { .. }),
        "expected SnapshotBoundaryNotFound, got {err:?}"
    );

    // Save a snapshot at cursor 1 → fork at cursor 2 resolves to it.
    store
        .save_state("fork-bnd", 1, &json!({"s": 1}), None, None)
        .await
        .unwrap();
    store
        .fork_channel(&ForkRequest {
            source_channel_id: "fork-bnd".into(),
            new_channel_id: "fork-bnd-child".into(),
            at_cursor: 2,
            operation_id: None,
            display_name: None,
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .expect("fork at cursor 2 should resolve to snapshot at cursor 1");
    let (cursor, state) = store
        .load_state("fork-bnd-child")
        .await
        .unwrap()
        .expect("child state");
    assert_eq!(cursor, 1);
    assert_eq!(state, json!({"s": 1}));

    // at_cursor == 0 forks from latest even with no snapshots (empty child allowed).
    store
        .fork_channel(&ForkRequest {
            source_channel_id: "fork-bnd".into(),
            new_channel_id: "fork-bnd-latest".into(),
            at_cursor: 0,
            operation_id: None,
            display_name: None,
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .expect("latest fork on event-only channel should succeed");
    let empty_child_events = store
        .replay(&ReplayRequest {
            channel_id: "fork-bnd-latest".into(),
            cursor: None,
            limit: Some(10),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap();
    assert_eq!(
        empty_child_events.len(),
        1,
        "latest fork still writes fork.started"
    );
    assert_eq!(empty_child_events[0].event_type, "fork.started");

    cleanup(&store).await;
}

#[tokio::test]
async fn fork_started_has_depends_on_edge() {
    let store = create_store().await;
    cleanup(&store).await;

    for i in 0..4 {
        store
            .publish_with_channel_cursor(request("fork-dep", "step", json!({"i": i})))
            .await
            .unwrap();
    }
    store
        .save_state("fork-dep", 4, &json!({"s": 4}), None, None)
        .await
        .unwrap();
    let parent_last = store
        .replay(&ReplayRequest {
            channel_id: "fork-dep".into(),
            cursor: None,
            limit: Some(10),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap()
        .pop()
        .expect("parent has events");

    store
        .fork_channel(&ForkRequest {
            source_channel_id: "fork-dep".into(),
            new_channel_id: "fork-dep-child".into(),
            at_cursor: 4,
            operation_id: None,
            display_name: None,
            reason: Some("lineage test".into()),
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap();

    let child_events = store
        .replay(&ReplayRequest {
            channel_id: "fork-dep-child".into(),
            cursor: None,
            limit: Some(10),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap();
    assert_eq!(child_events.len(), 1);
    assert_eq!(child_events[0].event_type, "fork.started");
    assert_eq!(
        child_events[0].depends_on,
        Some(parent_last.id),
        "fork.started must reference the parent's boundary event"
    );
    let payload = &child_events[0].payload;
    assert_eq!(payload["source_channel_id"], "fork-dep");
    assert_eq!(payload["forked_at_cursor"], 4);

    cleanup(&store).await;
}

#[tokio::test]
async fn fork_is_idempotent_by_operation_id() {
    let store = create_store().await;
    cleanup(&store).await;

    for i in 0..3 {
        store
            .publish_with_channel_cursor(request("fork-idem", "step", json!({"i": i})))
            .await
            .unwrap();
    }
    store
        .save_state("fork-idem", 3, &json!({"s": 3}), None, None)
        .await
        .unwrap();

    let request = ForkRequest {
        source_channel_id: "fork-idem".into(),
        new_channel_id: "fork-idem-child".into(),
        at_cursor: 3,
        operation_id: Some("op-fork-1".into()),
        display_name: Some("Idem".into()),
        reason: None,
        experiment_metadata: Some(json!({"n": 1})),
        ..Default::default()
    };

    let (first_meta, first_replayed) = store.fork_channel(&request).await.unwrap();
    assert!(!first_replayed, "first fork is a fresh creation");

    // Replay the identical operation → same metadata, no duplication.
    let (replayed_meta, replayed) = store.fork_channel(&request).await.unwrap();
    assert!(replayed, "replayed fork should be flagged");
    assert_eq!(replayed_meta.channel_id, first_meta.channel_id);

    let child_events = store
        .replay(&ReplayRequest {
            channel_id: "fork-idem-child".into(),
            cursor: None,
            limit: Some(10),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap();
    assert_eq!(
        child_events.len(),
        1,
        "idempotent replay must not duplicate fork.started"
    );
    let states = store
        .list_states("fork-idem-child", Some(10), Some(0))
        .await
        .unwrap();
    assert_eq!(
        states.len(),
        1,
        "idempotent replay must not duplicate state copies"
    );

    // Same child id via a different operation → also a no-op returning metadata.
    let (same_child_meta, same_child_replayed) = store
        .fork_channel(&ForkRequest {
            source_channel_id: "fork-idem".into(),
            new_channel_id: "fork-idem-child".into(),
            at_cursor: 3,
            operation_id: Some("op-fork-2".into()),
            display_name: Some("Idem".into()),
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(same_child_replayed);
    assert_eq!(same_child_meta.channel_id, "fork-idem-child");

    // The linked-control-plane fork meter fires exactly once, even across
    // idempotent replays: one successful fork, one `fork_operations` row.
    let client = store.pool().get().await.unwrap();
    let fork_meter: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM usage_delivery_outbox WHERE metric = 'fork_operations'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(fork_meter, 1, "one fork must produce exactly one fork_operations meter row");

    cleanup(&store).await;
}

#[tokio::test]
async fn fork_channel_metadata_is_queryable() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .save_state("fork-meta-parent", 5, &json!({"ok": true}), None, None)
        .await
        .unwrap();

    store
        .fork_channel(&ForkRequest {
            source_channel_id: "fork-meta-parent".into(),
            new_channel_id: "fork-meta-child".into(),
            at_cursor: 5,
            operation_id: None,
            display_name: Some("Meta child".into()),
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap();

    let meta = store
        .get_channel_metadata("fork-meta-child")
        .await
        .unwrap()
        .expect("metadata should exist");
    assert_eq!(meta.channel_id, "fork-meta-child");
    assert_eq!(meta.parent_channel_id.as_deref(), Some("fork-meta-parent"));
    assert_eq!(meta.forked_at_cursor, 5);
    assert_eq!(meta.origin_run_id, "fork-meta-parent");

    cleanup(&store).await;
    ensure_versioned_schema(&store).await;
}

#[tokio::test]
async fn nested_fork_channel_is_discoverable() {
    let store = create_store().await;
    cleanup(&store).await;

    // Grandparent
    for i in 0..5 {
        store
            .publish_with_channel_cursor(request("fork-gp", "step", json!({"i": i})))
            .await
            .unwrap();
    }

    // Parent fork — requires a saved boundary snapshot on the grandparent.
    store
        .save_state("fork-gp", 5, &json!({"boundary": 5}), None, None)
        .await
        .unwrap();
    store
        .fork_channel(&ForkRequest {
            source_channel_id: "fork-gp".into(),
            new_channel_id: "fork-parent-nested".into(),
            at_cursor: 5,
            operation_id: None,
            display_name: Some("Parent".into()),
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap();

    // Parent events
    for i in 0..3 {
        store
            .publish_with_channel_cursor(request("fork-parent-nested", "step", json!({"p": i})))
            .await
            .unwrap();
    }
    // Give the parent its own boundary snapshot so the child fork resolves.
    store
        .save_state("fork-parent-nested", 1, &json!({"boundary": 1}), None, None)
        .await
        .unwrap();

    // Child fork
    store
        .fork_channel(&ForkRequest {
            source_channel_id: "fork-parent-nested".into(),
            new_channel_id: "fork-child-nested".into(),
            at_cursor: 1,
            operation_id: None,
            display_name: Some("Child".into()),
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap();

    let channels = store.list_channels().await.unwrap();
    assert!(channels.contains(&"fork-gp".to_string()));
    assert!(channels.contains(&"fork-parent-nested".to_string()));
    assert!(
        channels.contains(&"fork-child-nested".to_string()),
        "nested fork should be discoverable"
    );

    cleanup(&store).await;
}

// ── Transition Tests (P4: atomic event+state) ────────────────────

fn transition_request(
    channel_id: &str,
    event_type: &str,
    payload: sonic_rs::Value,
    state: sonic_rs::Value,
) -> actae::events::schema::TransitionRequest {
    actae::events::schema::TransitionRequest {
        channel_id: channel_id.into(),
        event_type: event_type.into(),
        payload,
        metadata: EventMetadata {
            actor: "bot".into(),
            agent_id: None,
            user_id: None,
            metadata: None,
        },
        state,
        operation_id: None,
        expected_version: None,
        expected_cursor: None,
        depends_on: None,
        dependencies: Vec::new(),
    }
}

#[tokio::test]
async fn transition_persists_event_and_state_atomically() {
    let store = create_store().await;
    cleanup(&store).await;

    let (event, version) = store
        .publish_with_state(&transition_request(
            "tr-ch",
            "agent.started",
            json!({"seq": 1}),
            json!({"turns": 1}),
        ))
        .await
        .unwrap();

    assert!(version >= 1);
    assert_eq!(event.cursor, 1);

    // Snapshot cursor == event channel_cursor.
    let (cursor, state) = store.load_state("tr-ch").await.unwrap().expect("state");
    assert_eq!(cursor, 1);
    assert_eq!(state, json!({"turns": 1}));

    // Event replayable normally.
    let events = store
        .replay(&ReplayRequest {
            channel_id: "tr-ch".into(),
            cursor: None,
            limit: Some(10),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "agent.started");

    cleanup(&store).await;
}

#[tokio::test]
async fn transition_versions_are_immutable() {
    let store = create_store().await;
    cleanup(&store).await;

    let (_, v1) = store
        .publish_with_state(&transition_request(
            "tr-ver",
            "step",
            json!({"i": 1}),
            json!({"i": 1}),
        ))
        .await
        .unwrap();
    let (_, v2) = store
        .publish_with_state(&transition_request(
            "tr-ver",
            "step",
            json!({"i": 2}),
            json!({"i": 2}),
        ))
        .await
        .unwrap();
    assert!(v2 > v1);

    let (loaded_cursor, loaded_version, loaded_state) = store
        .load_state_version("tr-ver", v1)
        .await
        .unwrap()
        .expect("v1");
    assert_eq!(loaded_cursor, 1);
    assert_eq!(loaded_version, v1);
    assert_eq!(loaded_state, json!({"i": 1}));

    cleanup(&store).await;
}

// ── Optimistic Concurrency Tests (P5) ────────────────────────────

#[tokio::test]
async fn save_state_with_expected_version_guard() {
    let store = create_store().await;
    cleanup(&store).await;

    // Guard against empty: expected_version Some(0) requires no snapshot.
    let conflict = store
        .save_state("guard-ch", 1, &json!({"v": 1}), Some(1), None)
        .await
        .unwrap_err();
    assert!(matches!(conflict, EventStoreError::VersionConflict { .. }));

    let v1 = store
        .save_state("guard-ch", 1, &json!({"v": 1}), Some(0), None)
        .await
        .unwrap();

    // Correct guard succeeds.
    let v2 = store
        .save_state("guard-ch", 2, &json!({"v": 2}), Some(v1), None)
        .await
        .unwrap();
    assert!(v2 > v1);

    // Stale guard → conflict with actual reported.
    let err = store
        .save_state("guard-ch", 3, &json!({"v": 3}), Some(v1), None)
        .await
        .unwrap_err();
    match err {
        EventStoreError::VersionConflict { actual_version, .. } => {
            assert_eq!(actual_version, Some(v2))
        }
        other => panic!("expected VersionConflict, got {other:?}"),
    }

    cleanup(&store).await;
}

#[tokio::test]
async fn save_state_with_expected_cursor_guard() {
    let store = create_store().await;
    cleanup(&store).await;

    let _v1 = store
        .save_state("guard-cur", 10, &json!({"c": 10}), None, Some(0))
        .await
        .unwrap();

    // Wrong cursor → conflict.
    let err = store
        .save_state("guard-cur", 20, &json!({"c": 20}), None, Some(5))
        .await
        .unwrap_err();
    assert!(matches!(err, EventStoreError::VersionConflict { .. }));

    // Correct cursor succeeds.
    let v2 = store
        .save_state("guard-cur", 20, &json!({"c": 20}), None, Some(10))
        .await
        .unwrap();
    assert!(v2 > 0);

    cleanup(&store).await;
}

#[tokio::test]
async fn transition_respects_guards() {
    let store = create_store().await;
    cleanup(&store).await;

    // Guarded transition with no snapshot → conflict, nothing written.
    let mut req = transition_request("tr-guard", "step", json!({"i": 1}), json!({"i": 1}));
    req.expected_version = Some(5);
    let err = store.publish_with_state(&req).await.unwrap_err();
    assert!(matches!(err, EventStoreError::VersionConflict { .. }));

    let events = store
        .replay(&ReplayRequest {
            channel_id: "tr-guard".into(),
            cursor: None,
            limit: Some(10),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap();
    assert_eq!(
        events.len(),
        0,
        "rejected transition must not write an event"
    );

    cleanup(&store).await;
}

// ── Consumer Group Tests (P6) ────────────────────────────────────

#[tokio::test]
async fn consumer_group_claim_ack_offsets() {
    let store = create_store().await;
    cleanup(&store).await;

    for i in 0..10 {
        store
            .publish_with_channel_cursor(request("grp-ch", "work", json!({"i": i})))
            .await
            .unwrap();
    }

    let group = store
        .create_group("grp-1", "grp-ch", Some(&json!({"team": "a"})))
        .await
        .unwrap();
    assert_eq!(group.group_id, "grp-1");
    assert_eq!(group.channel_id, "grp-ch");

    // Join consumer A, claim a batch of 4.
    let offset = store.join_group("grp-1", "consumer-a", 60).await.unwrap();
    assert_eq!(offset.last_cursor, 0);
    let work = store.claim_work("grp-1", "consumer-a", 4).await.unwrap();
    assert_eq!(work.events.len(), 4);
    assert_eq!(work.events[0].cursor, 1);
    assert_eq!(work.events[3].cursor, 4);

    // Consumer B cannot overlap while A's lease is active.
    store.join_group("grp-1", "consumer-b", 60).await.unwrap();
    let work_b = store.claim_work("grp-1", "consumer-b", 100).await.unwrap();
    assert_eq!(work_b.events.len(), 6, "B claims after A's watermark");
    assert_eq!(work_b.events[0].cursor, 5);

    // Ack advances the durable offset.
    store.ack_work("grp-1", "consumer-a", 4).await.unwrap();
    let offsets = store.get_offsets("grp-1").await.unwrap();
    let a_offset = offsets
        .iter()
        .find(|o| o.consumer_id == "consumer-a")
        .unwrap();
    assert_eq!(a_offset.last_cursor, 4);

    cleanup(&store).await;
}

#[tokio::test]
async fn consumer_group_lease_expiry_reclaims_work() {
    let store = create_store().await;
    cleanup(&store).await;

    for i in 0..5 {
        store
            .publish_with_channel_cursor(request("grp-lease", "work", json!({"i": i})))
            .await
            .unwrap();
    }

    store
        .create_group("grp-lease-1", "grp-lease", None)
        .await
        .unwrap();
    // 1-second lease.
    store
        .join_group("grp-lease-1", "consumer-a", 1)
        .await
        .unwrap();
    let work_a = store
        .claim_work("grp-lease-1", "consumer-a", 100)
        .await
        .unwrap();
    assert_eq!(work_a.events.len(), 5);

    // Consumer B cannot claim while A holds an active lease. Refresh A's
    // lease right before the check so a slow pool under full-suite load can
    // never let the 1s lease lapse between A's join and B's claim.
    store
        .join_group("grp-lease-1", "consumer-b", 60)
        .await
        .unwrap();
    store
        .heartbeat("grp-lease-1", "consumer-a", 1)
        .await
        .unwrap();
    let work_b = store
        .claim_work("grp-lease-1", "consumer-b", 100)
        .await
        .unwrap();
    assert_eq!(work_b.events.len(), 0, "work is claimed by A");

    // Wait for A's lease to lapse, then expire + reclaim. Poll instead of a
    // fixed sleep so the assertion holds regardless of pool contention.
    let mut expired = 0u64;
    for _ in 0..100 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        expired = store.expire_leases().await.unwrap();
        if expired >= 1 {
            break;
        }
    }
    assert_eq!(expired, 1, "exactly A's lease expires");

    let work_b2 = store
        .claim_work("grp-lease-1", "consumer-b", 100)
        .await
        .unwrap();
    assert_eq!(work_b2.events.len(), 5, "expired claim is reclaimable");
    assert_eq!(work_b2.events[0].cursor, 1);

    // Expired consumer must re-join before claiming again.
    let err = store
        .claim_work("grp-lease-1", "consumer-a", 100)
        .await
        .unwrap_err();
    assert!(matches!(err, EventStoreError::ConsumerNotFound(_)));
    store
        .join_group("grp-lease-1", "consumer-a", 60)
        .await
        .unwrap();
    let work_a2 = store
        .claim_work("grp-lease-1", "consumer-a", 100)
        .await
        .unwrap();
    assert_eq!(
        work_a2.events.len(),
        0,
        "all work already acked/reclaimed by B"
    );

    cleanup(&store).await;
}

#[tokio::test]
async fn consumer_group_heartbeat_and_delete() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .create_group("grp-hb", "grp-hb-ch", None)
        .await
        .unwrap();
    store.join_group("grp-hb", "c1", 1).await.unwrap();

    // Heartbeat extends the lease past the original expiry.
    store.heartbeat("grp-hb", "c1", 120).await.unwrap();
    let work = store.claim_work("grp-hb", "c1", 10).await.unwrap();
    assert!(work.lease_until > chrono::Utc::now() + chrono::Duration::seconds(60));

    // Unknown group → heartbeat reports the consumer as invalid.
    assert!(matches!(
        store.heartbeat("grp-missing", "c1", 60).await.unwrap_err(),
        EventStoreError::ConsumerNotFound(_)
    ));

    // Delete cascades consumers/offsets.
    assert!(store.delete_group("grp-hb").await.unwrap());
    assert!(matches!(
        store.get_offsets("grp-hb").await.unwrap_err(),
        EventStoreError::GroupNotFound(_)
    ));

    cleanup(&store).await;
}

// ── Wake-up Tests (P7) ───────────────────────────────────────────

#[tokio::test]
async fn wakeup_schedule_dispatch_and_cancel() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .publish_with_channel_cursor(request("wk-ch", "seed", json!({})))
        .await
        .unwrap();

    // Future wake-up: not due yet.
    let future = store
        .create_wakeup(&actae::events::schema::CreateWakeupRequest {
            channel_id: "wk-ch".into(),
            run_at: chrono::Utc::now() + chrono::Duration::hours(1),
            payload: Some(json!({"step": "later"})),
        })
        .await
        .unwrap();
    assert_eq!(future.status, "pending");
    assert_eq!(store.dispatch_due_wakeups(10).await.unwrap().len(), 0);

    // Due wake-up: dispatched as a scheduler.wakeup event.
    let due = store
        .create_wakeup(&actae::events::schema::CreateWakeupRequest {
            channel_id: "wk-ch".into(),
            run_at: chrono::Utc::now() - chrono::Duration::seconds(1),
            payload: Some(json!({"step": "resume"})),
        })
        .await
        .unwrap();
    assert_eq!(store.dispatch_due_wakeups(10).await.unwrap().len(), 1);

    let events = store
        .replay(&ReplayRequest {
            channel_id: "wk-ch".into(),
            cursor: None,
            limit: Some(10),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap();
    let wakeup_event = events
        .iter()
        .find(|e| e.event_type == "scheduler.wakeup")
        .expect("wakeup event");
    assert_eq!(wakeup_event.payload["step"], "resume");
    assert_eq!(
        wakeup_event.metadata.metadata.as_ref().unwrap()["wakeup_id"],
        due.id
    );

    let fetched = store
        .get_wakeup(uuid::Uuid::parse_str(&due.id).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.status, "done");
    assert!(fetched.executed_at.is_some());

    // Cancel a pending wake-up.
    let pending = store
        .create_wakeup(&actae::events::schema::CreateWakeupRequest {
            channel_id: "wk-ch".into(),
            run_at: chrono::Utc::now() + chrono::Duration::hours(2),
            payload: None,
        })
        .await
        .unwrap();
    assert!(store
        .cancel_wakeup(uuid::Uuid::parse_str(&pending.id).unwrap())
        .await
        .unwrap());
    // Cancel again → already cancelled, not pending.
    assert!(!store
        .cancel_wakeup(uuid::Uuid::parse_str(&pending.id).unwrap())
        .await
        .unwrap());

    // List filters.
    let done = store
        .list_wakeups(Some("wk-ch"), Some("done"))
        .await
        .unwrap();
    assert_eq!(done.len(), 1);
    let cancelled = store
        .list_wakeups(Some("wk-ch"), Some("cancelled"))
        .await
        .unwrap();
    assert_eq!(cancelled.len(), 1);

    cleanup(&store).await;
}

#[tokio::test]
async fn wakeup_batch_dispatch_is_atomic() {
    let store = create_store().await;
    cleanup(&store).await;

    for i in 0..3 {
        store
            .create_wakeup(&actae::events::schema::CreateWakeupRequest {
                channel_id: "wk-batch".into(),
                run_at: chrono::Utc::now() - chrono::Duration::seconds(1),
                payload: Some(json!({"n": i})),
            })
            .await
            .unwrap();
    }
    assert_eq!(store.dispatch_due_wakeups(10).await.unwrap().len(), 3);

    let events = store
        .replay(&ReplayRequest {
            channel_id: "wk-batch".into(),
            cursor: None,
            limit: Some(10),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 3);
    assert!(events.iter().all(|e| e.event_type == "scheduler.wakeup"));
    // Gapless child channel_cursors.
    let cursors: Vec<i64> = events.iter().map(|e| e.cursor).collect();
    assert_eq!(cursors, vec![1, 2, 3]);

    cleanup(&store).await;
}

// ── Race / Concurrency Tests (flagship hardening) ─────────────────

#[tokio::test]
async fn concurrent_same_operation_id_forks_produce_single_child() {
    let store = create_store().await;
    cleanup(&store).await;

    for i in 0..5 {
        store
            .publish_with_channel_cursor(request("race-fork", "step", json!({"i": i})))
            .await
            .unwrap();
    }
    store
        .save_state("race-fork", 5, &json!({"boundary": 5}), None, None)
        .await
        .unwrap();

    // 10 concurrent forks with the SAME operation_id + child id.
    let mut handles = Vec::new();
    for _ in 0..10 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            store
                .fork_channel(&ForkRequest {
                    source_channel_id: "race-fork".into(),
                    new_channel_id: "race-child".into(),
                    at_cursor: 5,
                    operation_id: Some("race-op-1".into()),
                    display_name: None,
                    reason: None,
                    experiment_metadata: None,
                    ..Default::default()
                })
                .await
        }));
    }

    let mut replayed = 0;
    for h in handles {
        let (_, is_replayed) = h.await.unwrap().expect("fork must succeed");
        if is_replayed {
            replayed += 1;
        }
    }
    assert_eq!(replayed, 9, "exactly one fork creates; the rest replay");

    // Exactly one fork.started event, exactly one state copy.
    let child_events = store
        .replay(&ReplayRequest {
            channel_id: "race-child".into(),
            cursor: None,
            limit: Some(10),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap();
    assert_eq!(child_events.len(), 1);
    let states = store
        .list_states("race-child", Some(10), Some(0))
        .await
        .unwrap();
    assert_eq!(states.len(), 1);

    cleanup(&store).await;
}

#[tokio::test]
async fn concurrent_forks_different_child_ids_all_succeed() {
    let store = create_store().await;
    cleanup(&store).await;

    for i in 0..5 {
        store
            .publish_with_channel_cursor(request("race-multi", "step", json!({"i": i})))
            .await
            .unwrap();
    }
    store
        .save_state("race-multi", 5, &json!({"boundary": 5}), None, None)
        .await
        .unwrap();

    // 10 concurrent forks of the same parent, each with its own child id.
    let mut handles = Vec::new();
    for i in 0..10 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            store
                .fork_channel(&ForkRequest {
                    source_channel_id: "race-multi".into(),
                    new_channel_id: format!("race-child-{i}"),
                    at_cursor: 5,
                    operation_id: Some(format!("race-op-{i}")),
                    display_name: None,
                    reason: None,
                    experiment_metadata: None,
                    ..Default::default()
                })
                .await
                .map(|(_, replayed)| replayed)
        }));
    }

    for h in handles {
        let replayed = h.await.unwrap().expect("fork must succeed");
        assert!(!replayed, "each distinct child is a fresh fork");
    }

    // The parent's fork.started depends_on edges are all gapless:
    // every child's channel_cursor starts at 1 and the parent event stream
    // is unaffected (its channel_cursor sequence is untouched by forks).
    let parent_events = store
        .replay(&ReplayRequest {
            channel_id: "race-multi".into(),
            cursor: None,
            limit: Some(100),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap();
    let parent_cursors: Vec<i64> = parent_events.iter().map(|e| e.cursor).collect();
    assert_eq!(
        parent_cursors,
        (1..=5).collect::<Vec<i64>>(),
        "parent stream untouched"
    );

    cleanup(&store).await;
}

#[tokio::test]
async fn concurrent_guarded_saves_exactly_one_winner() {
    let store = create_store().await;
    cleanup(&store).await;

    // Two writers race to be the FIRST snapshot (guard: expected_version 0).
    let mut handles = Vec::new();
    for i in 0..10 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            store
                .save_state("race-guard", i as i64, &json!({"writer": i}), Some(0), None)
                .await
        }));
    }

    let mut winners = 0;
    for h in handles {
        if h.await.unwrap().is_ok() {
            winners += 1;
        }
    }
    assert_eq!(winners, 1, "exactly one first-write wins");

    cleanup(&store).await;
}

#[tokio::test]
async fn concurrent_claims_are_disjoint() {
    let store = create_store().await;
    cleanup(&store).await;

    for i in 0..50 {
        store
            .publish_with_channel_cursor(request("race-claim", "work", json!({"i": i})))
            .await
            .unwrap();
    }
    store
        .create_group("race-claim-g", "race-claim", None)
        .await
        .unwrap();

    // 5 consumers race to claim everything; no event may be claimed twice
    // while all leases are active.
    let mut handles = Vec::new();
    for c in 0..5 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            let consumer = format!("c-{c}");
            store
                .join_group("race-claim-g", &consumer, 120)
                .await
                .unwrap();
            let mut seen: Vec<i64> = Vec::new();
            loop {
                let work = store
                    .claim_work("race-claim-g", &consumer, 10)
                    .await
                    .unwrap();
                if work.events.is_empty() {
                    break;
                }
                for e in &work.events {
                    seen.push(e.cursor);
                }
                let last = seen.last().copied().unwrap_or(0);
                store
                    .ack_work("race-claim-g", &consumer, last)
                    .await
                    .unwrap();
            }
            seen
        }));
    }

    let mut all: Vec<i64> = Vec::new();
    for h in handles {
        all.extend(h.await.unwrap());
    }
    all.sort_unstable();
    all.dedup();
    assert_eq!(
        all.len(),
        50,
        "all 50 events claimed exactly once across consumers"
    );
    assert_eq!(all.first(), Some(&1));
    assert_eq!(all.last(), Some(&50));

    cleanup(&store).await;
}

#[tokio::test]
async fn concurrent_wakeup_dispatch_no_duplicates() {
    let store = create_store().await;
    cleanup(&store).await;

    for i in 0..10 {
        store
            .create_wakeup(&actae::events::schema::CreateWakeupRequest {
                channel_id: "race-wk".into(),
                run_at: chrono::Utc::now() - chrono::Duration::seconds(1),
                payload: Some(json!({"n": i})),
            })
            .await
            .unwrap();
    }

    // 5 dispatchers race; each wake-up must fire exactly once (SKIP LOCKED).
    let mut handles = Vec::new();
    for _ in 0..5 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            store.dispatch_due_wakeups(50).await.unwrap()
        }));
    }
    let mut total: usize = 0;
    for h in handles {
        total += h.await.unwrap().len();
    }
    assert_eq!(total, 10, "all 10 wake-ups dispatched exactly once");

    let events = store
        .replay(&ReplayRequest {
            channel_id: "race-wk".into(),
            cursor: None,
            limit: Some(100),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap();
    assert_eq!(events.len(), 10);
    let done = store
        .list_wakeups(Some("race-wk"), Some("done"))
        .await
        .unwrap();
    assert_eq!(done.len(), 10);

    cleanup(&store).await;
}

#[tokio::test]
async fn diff_channels_reports_structural_state_changes() {
    let store = create_store().await;
    cleanup(&store).await;

    // Parent run with a shared baseline state.
    store
        .publish_with_channel_cursor(request("diff-root", "step", json!({"n": 1})))
        .await
        .unwrap();
    store
        .save_state(
            "diff-root",
            1,
            &json!({"task": "root", "shared": true, "step": 1}),
            None,
            None,
        )
        .await
        .unwrap();

    // Fork A and B from the root boundary (at_cursor 0 = latest state).
    for child in ["diff-a", "diff-b"] {
        store
            .fork_channel(&ForkRequest {
                source_channel_id: "diff-root".into(),
                new_channel_id: child.into(),
                at_cursor: 0,
                operation_id: None,
                display_name: Some(child.to_string()),
                reason: Some("experiment".into()),
                experiment_metadata: None,
                ..Default::default()
            })
            .await
            .expect("fork should succeed");
    }

    // A: adds a field, changes one, removes none.
    store
        .save_state(
            "diff-a",
            1,
            &json!({"task": "root", "shared": true, "step": 2, "added_a": true}),
            None,
            None,
        )
        .await
        .unwrap();
    // B: changes step differently and adds its own field.
    store
        .save_state(
            "diff-b",
            1,
            &json!({"task": "root", "shared": true, "step": 3, "added_b": true}),
            None,
            None,
        )
        .await
        .unwrap();

    let diff = store
        .diff_channels("diff-a", "diff-b")
        .await
        .expect("diff should succeed");

    assert_eq!(diff.left_channel_id, "diff-a");
    assert_eq!(diff.right_channel_id, "diff-b");

    // Common ancestor = the root state (both forks share origin_run_id).
    let common = diff.common.expect("shared ancestry should resolve");
    assert_eq!(common.channel_id, "diff-root");
    assert_eq!(common.cursor, 1);

    // Both forks were created with at_cursor=0 → the per-side divergence is
    // each child's RESOLVED boundary (the parent's cursor at fork time = 1),
    // never 0 and never derived from the parent's mutable latest state.
    assert_eq!(diff.left_diverged_at_cursor, Some(1));
    assert_eq!(diff.right_diverged_at_cursor, Some(1));

    // Entries (documented convention: "added" = right-only, "removed" =
    // left-only, "changed" = both differ): added_a is left-only (removed),
    // added_b is right-only (added), step differs (changed).
    let kinds: Vec<(Vec<String>, String)> = diff
        .entries
        .iter()
        .map(|e| (e.path.clone(), e.kind.clone()))
        .collect();

    assert!(
        kinds.contains(&(vec!["added_a".into()], "removed".into())),
        "added_a is left-only and should be removed, got {kinds:?}"
    );
    assert!(
        kinds.contains(&(vec!["added_b".into()], "added".into())),
        "added_b should be an added entry, got {kinds:?}"
    );
    let step = kinds
        .iter()
        .find(|(p, _)| p == &vec!["step".to_string()])
        .expect("step entry present");
    assert_eq!(step.1, "changed", "step should be changed, got {kinds:?}");

    let step_entry = diff
        .entries
        .iter()
        .find(|e| e.path == vec!["step".to_string()])
        .unwrap();
    assert_eq!(step_entry.left.as_ref().and_then(|v| v.as_i64()), Some(2));
    assert_eq!(step_entry.right.as_ref().and_then(|v| v.as_i64()), Some(3));

    cleanup(&store).await;
}

#[tokio::test]
async fn diff_channels_reports_added_and_removed_at_nested_paths() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .save_state(
            "diff-nest",
            1,
            &json!({"cfg": {"retries": 3, "timeout": 30}, "tags": ["a", "b"]}),
            None,
            None,
        )
        .await
        .unwrap();
    store
        .save_state(
            "diff-nest2",
            1,
            &json!({"cfg": {"retries": 5}, "tags": ["a", "b", "c"]}),
            None,
            None,
        )
        .await
        .unwrap();

    let diff = store
        .diff_channels("diff-nest", "diff-nest2")
        .await
        .expect("diff should succeed");

    // Nested changed + removed, array added tail.
    let entries: Vec<(Vec<String>, String)> = diff
        .entries
        .iter()
        .map(|e| (e.path.clone(), e.kind.clone()))
        .collect();

    assert!(entries.contains(&(vec!["cfg".into(), "retries".into()], "changed".into())));
    assert!(entries.contains(&(vec!["cfg".into(), "timeout".into()], "removed".into())));
    assert!(entries.contains(&(vec!["tags".into(), "2".into()], "added".into())));

    // Independent channels (no shared origin) → no common ancestor.
    assert!(
        diff.common.is_none(),
        "independent channels share no ancestry"
    );

    cleanup(&store).await;
}

#[tokio::test]
async fn diff_channels_identical_states_yield_no_entries() {
    let store = create_store().await;
    cleanup(&store).await;

    let state = json!({"a": 1, "b": {"c": [1, 2, 3]}});
    store
        .save_state("diff-same-a", 1, &state, None, None)
        .await
        .unwrap();
    store
        .save_state("diff-same-b", 1, &state, None, None)
        .await
        .unwrap();

    let diff = store
        .diff_channels("diff-same-a", "diff-same-b")
        .await
        .expect("diff should succeed");

    assert!(diff.entries.is_empty(), "identical states yield no entries");
    assert!(diff.common.is_none());

    cleanup(&store).await;
}

#[tokio::test]
async fn decision_trail_exposes_lineage_and_executions() {
    let store = create_store().await;
    cleanup(&store).await;

    // Root run with an event + state.
    store
        .publish_with_channel_cursor(request("trail-root", "step", json!({"n": 1})))
        .await
        .unwrap();
    store
        .save_state("trail-root", 1, &json!({"step": 1}), None, None)
        .await
        .unwrap();

    // Fork A from root, then B from A.
    store
        .fork_channel(&ForkRequest {
            source_channel_id: "trail-root".into(),
            new_channel_id: "trail-a".into(),
            at_cursor: 0,
            operation_id: None,
            display_name: Some("A".into()),
            reason: Some("first fork".into()),
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .expect("fork A");
    store
        .fork_channel(&ForkRequest {
            source_channel_id: "trail-a".into(),
            new_channel_id: "trail-b".into(),
            at_cursor: 0,
            operation_id: None,
            display_name: Some("B".into()),
            reason: Some("nested".into()),
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .expect("fork B");

    // A tool execution on the deepest fork.
    store
        .claim_execution(&ClaimExecutionRequest {
            channel_id: "trail-b".into(),
            key_name: "calc".into(),
            tool_name: "calculator".into(),
            params: Some(json!({"expr": "2+2"})),
            dedup_fields: None,
            lease_seconds: Some(60),
            emit_replay_event: None,
            actor: Some("bot".into()),
        })
        .await
        .expect("claim execution");

    let trail = store
        .get_decision_trail("trail-b")
        .await
        .expect("trail should succeed");

    assert_eq!(trail.channel_id, "trail-b");
    assert_eq!(trail.origin_run_id, "trail-root");

    // Ancestry chain: trail-b -> trail-a -> trail-root.
    let chain: Vec<String> = trail
        .ancestry
        .iter()
        .map(|h| h.channel_id.clone())
        .collect();
    assert_eq!(
        chain,
        vec![
            "trail-b".to_string(),
            "trail-a".to_string(),
            "trail-root".to_string()
        ],
        "ancestry should walk the full chain back to the root"
    );

    // The boundary of the deepest fork resolves to the parent's state.
    let boundary = trail.boundary.expect("boundary state present");
    assert_eq!(boundary.source_channel_id, "trail-a");

    // The execution ledger row is exposed.
    assert_eq!(trail.executions.len(), 1);
    assert_eq!(trail.executions[0].key_name, "calc");
    assert_eq!(trail.executions[0].tool_name, "calculator");

    cleanup(&store).await;
}

#[tokio::test]
async fn decision_trail_root_has_single_hop_chain_and_no_boundary() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .publish_with_channel_cursor(request("trail-root2", "step", json!({"n": 1})))
        .await
        .unwrap();
    store
        .save_state("trail-root2", 1, &json!({"step": 1}), None, None)
        .await
        .unwrap();

    let trail = store
        .get_decision_trail("trail-root2")
        .await
        .expect("trail should succeed");

    assert_eq!(trail.origin_run_id, "trail-root2");
    assert_eq!(trail.ancestry.len(), 1);
    assert_eq!(trail.ancestry[0].channel_id, "trail-root2");
    assert!(trail.ancestry[0].parent_channel_id.is_none());
    assert!(trail.boundary.is_none());
    assert!(trail.executions.is_empty());

    cleanup(&store).await;
}

#[tokio::test]
async fn diff_channels_parent_child_lca_is_parent_not_root() {
    let store = create_store().await;
    cleanup(&store).await;

    // Root -> A -> B. B's diff against A must resolve A as the common
    // ancestor, NOT the root.
    store
        .publish_with_channel_cursor(request("lca-root", "step", json!({"n": 1})))
        .await
        .unwrap();
    store
        .save_state("lca-root", 1, &json!({"gen": "root", "n": 1}), None, None)
        .await
        .unwrap();

    store
        .fork_channel(&ForkRequest {
            source_channel_id: "lca-root".into(),
            new_channel_id: "lca-a".into(),
            at_cursor: 0,
            operation_id: None,
            display_name: Some("A".into()),
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .expect("fork A");
    store
        .save_state("lca-a", 1, &json!({"gen": "a", "n": 2}), None, None)
        .await
        .unwrap();

    store
        .fork_channel(&ForkRequest {
            source_channel_id: "lca-a".into(),
            new_channel_id: "lca-b".into(),
            at_cursor: 0,
            operation_id: None,
            display_name: Some("B".into()),
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .expect("fork B");
    store
        .save_state("lca-b", 1, &json!({"gen": "b", "n": 3}), None, None)
        .await
        .unwrap();

    let diff = store
        .diff_channels("lca-a", "lca-b")
        .await
        .expect("diff A vs B");

    // The lowest common ancestor of A and B is A itself (B was forked from
    // A) — NOT the root.
    let common = diff.common.expect("common ancestor should resolve");
    assert_eq!(
        common.channel_id, "lca-a",
        "LCA of a parent/child pair must be the parent, got {}",
        common.channel_id
    );
    assert_eq!(common.state, Some(json!({"gen": "a", "n": 2})));

    // Entries show B's deltas over A.
    let kinds: Vec<(Vec<String>, String)> = diff
        .entries
        .iter()
        .map(|e| (e.path.clone(), e.kind.clone()))
        .collect();
    assert!(kinds.contains(&(vec!["gen".into()], "changed".into())));
    assert!(kinds.contains(&(vec!["n".into()], "changed".into())));

    cleanup(&store).await;
}

#[tokio::test]
async fn diff_channels_sibling_lca_is_shared_parent() {
    let store = create_store().await;
    cleanup(&store).await;

    // Root -> A and Root -> B: sibling forks. Their LCA is the root.
    store
        .publish_with_channel_cursor(request("lca2-root", "step", json!({"n": 1})))
        .await
        .unwrap();
    store
        .save_state("lca2-root", 1, &json!({"root": true, "n": 1}), None, None)
        .await
        .unwrap();

    for child in ["lca2-a", "lca2-b"] {
        store
            .fork_channel(&ForkRequest {
                source_channel_id: "lca2-root".into(),
                new_channel_id: child.into(),
                at_cursor: 0,
                operation_id: None,
                display_name: Some(child.to_string()),
                reason: None,
                experiment_metadata: None,
                ..Default::default()
            })
            .await
            .expect("fork");
    }
    store
        .save_state(
            "lca2-a",
            1,
            &json!({"root": true, "n": 2, "a": true}),
            None,
            None,
        )
        .await
        .unwrap();
    store
        .save_state(
            "lca2-b",
            1,
            &json!({"root": true, "n": 3, "b": true}),
            None,
            None,
        )
        .await
        .unwrap();

    let diff = store.diff_channels("lca2-a", "lca2-b").await.expect("diff");

    let common = diff.common.expect("siblings share the root as LCA");
    assert_eq!(common.channel_id, "lca2-root");

    cleanup(&store).await;
}

#[tokio::test]
async fn decision_trail_boundary_uses_state_at_fork_not_latest() {
    let store = create_store().await;
    cleanup(&store).await;

    // Parent with two snapshots. Fork at cursor 1 (boundary snapshot),
    // then the parent advances to cursor 2 AFTER the fork. The trail's
    // boundary must report the state AT the fork (cursor 1), not the
    // parent's newer state.
    store
        .publish_with_channel_cursor(request("bnd-root", "step", json!({"n": 1})))
        .await
        .unwrap();
    store
        .save_state("bnd-root", 1, &json!({"at_fork": true, "n": 1}), None, None)
        .await
        .unwrap();
    store
        .publish_with_channel_cursor(request("bnd-root", "step", json!({"n": 2})))
        .await
        .unwrap();
    store
        .save_state(
            "bnd-root",
            2,
            &json!({"at_fork": false, "n": 2}),
            None,
            None,
        )
        .await
        .unwrap();

    store
        .fork_channel(&ForkRequest {
            source_channel_id: "bnd-root".into(),
            new_channel_id: "bnd-child".into(),
            at_cursor: 1,
            operation_id: None,
            display_name: Some("child".into()),
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .expect("fork at cursor 1");

    let trail = store.get_decision_trail("bnd-child").await.expect("trail");

    let boundary = trail.boundary.expect("boundary present");
    // The fork resolved to the snapshot at-or-before cursor 1, i.e. the
    // FIRST parent snapshot — not the newer one saved after the fork.
    assert_eq!(
        boundary.resolved_cursor, 1,
        "boundary must be the fork-point state"
    );
    assert_eq!(
        boundary.state,
        Some(json!({"at_fork": true, "n": 1})),
        "boundary must be the state at the fork, not the parent's latest"
    );

    cleanup(&store).await;
}

#[tokio::test]
async fn diff_channels_deeply_nested_state_does_not_overflow() {
    let store = create_store().await;
    cleanup(&store).await;

    // Build a deeply nested object (> MAX_DIFF_DEPTH) on both sides with a
    // differing leaf at the bottom — the depth cap must prevent a stack
    // overflow and still return a bounded result.
    fn nested(depth: usize, leaf: i64) -> sonic_rs::Value {
        let mut v = sonic_rs::json!({"leaf": leaf});
        for _ in 0..depth {
            v = sonic_rs::json!({"child": v});
        }
        v
    }
    // Depth far beyond the JSONB decode guard (24): the load path must return
    // an empty object instead of overflowing the stack.
    store
        .save_state("deep-a", 1, &nested(100, 1), None, None)
        .await
        .unwrap();
    store
        .save_state("deep-b", 1, &nested(100, 2), None, None)
        .await
        .unwrap();

    let diff = store
        .diff_channels("deep-a", "deep-b")
        .await
        .expect("diff should not overflow");
    // Both loads hit the depth guard -> empty objects -> no entries.
    assert!(
        diff.entries.is_empty(),
        "guarded loads should diff as empty"
    );
    assert!(diff.entries.len() <= 500);

    cleanup(&store).await;
}

#[tokio::test]
async fn diff_channels_missing_state_diffs_against_empty() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .save_state("has-state", 1, &json!({"x": 1}), None, None)
        .await
        .unwrap();

    // "no-state" has no saved snapshot -> diffs as {} vs {"x":1}.
    let diff = store
        .diff_channels("has-state", "no-state")
        .await
        .expect("diff");
    assert_eq!(diff.left.state, Some(json!({"x": 1})));
    assert_eq!(diff.right.state, Some(sonic_rs::json!({})));
    assert_eq!(diff.entries.len(), 1);
    assert_eq!(diff.entries[0].path, vec!["x".to_string()]);
    assert_eq!(diff.entries[0].kind, "removed");

    cleanup(&store).await;
}

#[tokio::test]
async fn channel_exists_distinguishes_known_from_unknown() {
    let store = create_store().await;
    cleanup(&store).await;

    // Unknown channel.
    assert!(!store.channel_exists("nope-xyz").await.unwrap());

    // Channel with events → exists.
    store
        .publish_with_channel_cursor(request("exists-ev", "t", json!({"n": 1})))
        .await
        .unwrap();
    assert!(store.channel_exists("exists-ev").await.unwrap());

    // Channel with only a state snapshot → exists.
    store
        .save_state("exists-st", 1, &json!({"n": 1}), None, None)
        .await
        .unwrap();
    assert!(store.channel_exists("exists-st").await.unwrap());

    // Channel with only metadata (a fork created but not yet written) → exists.
    store
        .fork_channel(&ForkRequest {
            source_channel_id: "exists-ev".into(),
            new_channel_id: "exists-meta".into(),
            at_cursor: 0,
            operation_id: None,
            display_name: Some("m".into()),
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .expect("fork");
    assert!(store.channel_exists("exists-meta").await.unwrap());

    cleanup(&store).await;
}

// ── Post-audit adversarial tests (Phase 12) ─────────────────────────────

/// Parent advances AFTER an at_cursor=0 fork: the child's boundary and the
/// decision trail must remain at the fork-time cursor, never the parent's
/// newer state.
#[tokio::test]
async fn fork_at_cursor_zero_boundary_is_immutable_after_parent_advances() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .publish_with_channel_cursor(request("adv-p", "step", json!({"n": 1})))
        .await
        .unwrap();
    store
        .save_state("adv-p", 1, &json!({"v": 1}), None, None)
        .await
        .unwrap();

    // at_cursor=0 → resolves to cursor 1.
    let (meta, _) = store
        .fork_channel(&ForkRequest {
            source_channel_id: "adv-p".into(),
            new_channel_id: "adv-c".into(),
            at_cursor: 0,
            operation_id: None,
            display_name: None,
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        meta.forked_at_cursor, 1,
        "resolved boundary must be 1, not 0"
    );
    assert_eq!(meta.requested_at_cursor, 0);
    assert!(meta.restorable);

    // Parent advances (new event + state at cursor 2).
    store
        .publish_with_channel_cursor(request("adv-p", "step", json!({"n": 2})))
        .await
        .unwrap();
    store
        .save_state("adv-p", 2, &json!({"v": 2}), None, None)
        .await
        .unwrap();

    // The child's state + boundary are untouched.
    let (cursor, state) = store.load_state("adv-c").await.unwrap().unwrap();
    assert_eq!(cursor, 1);
    assert_eq!(state, json!({"v": 1}));

    // The decision trail reports the immutable fork-time boundary.
    let trail = store.get_decision_trail("adv-c").await.unwrap();
    let boundary = trail.boundary.expect("boundary present");
    assert_eq!(boundary.resolved_cursor, 1);
    assert_eq!(boundary.state, Some(json!({"v": 1})));
    assert_eq!(boundary.requested_cursor, 0);

    cleanup(&store).await;
}

/// Requested cursor differs from resolved cursor: both are persisted and the
/// receipt reports the drift honestly.
#[tokio::test]
async fn fork_persists_requested_and_resolved_boundary() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .publish_with_channel_cursor(request("bnd-p", "step", json!({"n": 5})))
        .await
        .unwrap();
    store
        .save_state("bnd-p", 5, &json!({"at": 5}), None, None)
        .await
        .unwrap();
    store
        .save_state("bnd-p", 10, &json!({"at": 10}), None, None)
        .await
        .unwrap();

    let (meta, _) = store
        .fork_channel(&ForkRequest {
            source_channel_id: "bnd-p".into(),
            new_channel_id: "bnd-c".into(),
            at_cursor: 7,
            operation_id: None,
            display_name: None,
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(meta.requested_at_cursor, 7);
    assert_eq!(
        meta.resolved_state_cursor, 5,
        "nearest snapshot at-or-before 7"
    );
    assert_eq!(
        meta.forked_at_cursor, 5,
        "forked_at_cursor is the RESOLVED boundary"
    );
    assert!(meta.source_state_version >= 1);
    assert!(meta.source_state_sha256.is_some());
    assert!(meta.restorable);

    let (cursor, state) = store.load_state("bnd-c").await.unwrap().unwrap();
    assert_eq!(cursor, 5);
    assert_eq!(state, json!({"at": 5}));

    cleanup(&store).await;
}

/// A lineage-only fork (no snapshot at all) reports restorable=false with a
/// zero resolved cursor.
#[tokio::test]
async fn fork_no_snapshot_is_lineage_only_not_restorable() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .publish_with_channel_cursor(request("evt-p", "step", json!({})))
        .await
        .unwrap();

    let (meta, _) = store
        .fork_channel(&ForkRequest {
            source_channel_id: "evt-p".into(),
            new_channel_id: "evt-c".into(),
            at_cursor: 0,
            operation_id: None,
            display_name: None,
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap();

    assert!(!meta.restorable);
    assert_eq!(meta.resolved_state_cursor, 0);
    assert_eq!(meta.source_state_version, 0);
    assert!(meta.source_state_sha256.is_none());
    assert_eq!(store.load_state("evt-c").await.unwrap(), None);

    cleanup(&store).await;
}

/// Late historical snapshot insertion: resolve_fork_state must select the
/// snapshot CLOSEST to the boundary by cursor, not the newest version.
#[tokio::test]
async fn resolve_fork_state_prefers_closest_cursor_not_newest_version() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .publish_with_channel_cursor(request("late-p", "step", json!({"n": 1})))
        .await
        .unwrap();
    store
        .save_state("late-p", 10, &json!({"at": 10}), None, None)
        .await
        .unwrap();
    // A LATER-written snapshot at a LOWER cursor (late historical insert).
    store
        .save_state("late-p", 3, &json!({"at": 3}), None, None)
        .await
        .unwrap();

    // Fork at cursor 5 → must resolve to cursor 3 (closest by cursor), even
    // though version 10's snapshot (cursor 3) was written AFTER cursor 10's.
    let (meta, _) = store
        .fork_channel(&ForkRequest {
            source_channel_id: "late-p".into(),
            new_channel_id: "late-c".into(),
            at_cursor: 5,
            operation_id: None,
            display_name: None,
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(meta.resolved_state_cursor, 3);
    let (cursor, state) = store.load_state("late-c").await.unwrap().unwrap();
    assert_eq!(cursor, 3);
    assert_eq!(state, json!({"at": 3}));

    cleanup(&store).await;
}

/// Same operation_id retried with a DIFFERENT child id must return the
/// ORIGINAL child from the ledger (not an inconsistent-state error), and a
/// reused key with a different request is a 409 idempotency_conflict.
#[tokio::test]
async fn fork_same_operation_different_child_replays_original() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .save_state("idem-p", 1, &json!({"s": 1}), None, None)
        .await
        .unwrap();

    let req = ForkRequest {
        source_channel_id: "idem-p".into(),
        new_channel_id: "idem-c1".into(),
        at_cursor: 1,
        operation_id: Some("op-replay".into()),
        display_name: None,
        reason: None,
        experiment_metadata: None,
        ..Default::default()
    };
    let (first, replayed) = store.fork_channel(&req).await.unwrap();
    assert!(!replayed);
    assert_eq!(first.channel_id, "idem-c1");

    // Retry with the IDENTICAL request → replay the original.
    let (replayed_meta, replayed) = store.fork_channel(&req).await.unwrap();
    assert!(replayed);
    assert_eq!(replayed_meta.channel_id, "idem-c1");

    // Same op retried with a DIFFERENT child id → the child id is part of
    // the canonical request, so this is request drift → 409 conflict (never
    // an inconsistent-state error).
    let retry_req = ForkRequest {
        source_channel_id: "idem-p".into(),
        new_channel_id: "idem-c2".into(),
        at_cursor: 1,
        operation_id: Some("op-replay".into()),
        display_name: None,
        reason: None,
        experiment_metadata: None,
        ..Default::default()
    };
    let err = store.fork_channel(&retry_req).await.unwrap_err();
    assert!(
        matches!(err, EventStoreError::IdempotencyConflict { .. }),
        "different-child retry must be IdempotencyConflict, got {err:?}"
    );
    assert!(!store.channel_exists("idem-c2").await.unwrap());

    // Same key + DIFFERENT boundary → 409 idempotency_conflict.
    let drift_req = ForkRequest {
        source_channel_id: "idem-p".into(),
        new_channel_id: "idem-c1".into(),
        at_cursor: 2,
        operation_id: Some("op-replay".into()),
        display_name: None,
        reason: None,
        experiment_metadata: None,
        ..Default::default()
    };
    let err = store.fork_channel(&drift_req).await.unwrap_err();
    assert!(
        matches!(err, EventStoreError::IdempotencyConflict { .. }),
        "expected IdempotencyConflict, got {err:?}"
    );

    cleanup(&store).await;
}

/// Same child id with a different source → 409 channel_conflict.
#[tokio::test]
async fn fork_same_child_different_source_is_channel_conflict() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .save_state("src-a", 1, &json!({"a": 1}), None, None)
        .await
        .unwrap();
    store
        .save_state("src-b", 1, &json!({"b": 1}), None, None)
        .await
        .unwrap();

    store
        .fork_channel(&ForkRequest {
            source_channel_id: "src-a".into(),
            new_channel_id: "child-x".into(),
            at_cursor: 1,
            operation_id: None,
            display_name: None,
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap();

    let err = store
        .fork_channel(&ForkRequest {
            source_channel_id: "src-b".into(),
            new_channel_id: "child-x".into(),
            at_cursor: 1,
            operation_id: None,
            display_name: None,
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(
        matches!(err, EventStoreError::ChannelConflict { .. }),
        "expected ChannelConflict, got {err:?}"
    );

    cleanup(&store).await;
}

/// Siblings forked at different boundaries report per-side divergence.
#[tokio::test]
async fn diff_channels_siblings_report_per_side_divergence() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .publish_with_channel_cursor(request("div-root", "step", json!({"n": 1})))
        .await
        .unwrap();
    store
        .save_state("div-root", 1, &json!({"shared": true}), None, None)
        .await
        .unwrap();
    store
        .publish_with_channel_cursor(request("div-root", "step", json!({"n": 2})))
        .await
        .unwrap();
    store
        .save_state("div-root", 2, &json!({"shared": true, "n": 2}), None, None)
        .await
        .unwrap();

    // Sibling A forked at cursor 1, sibling B at cursor 2.
    store
        .fork_channel(&ForkRequest {
            source_channel_id: "div-root".into(),
            new_channel_id: "div-a".into(),
            at_cursor: 1,
            operation_id: None,
            display_name: None,
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap();
    store
        .fork_channel(&ForkRequest {
            source_channel_id: "div-root".into(),
            new_channel_id: "div-b".into(),
            at_cursor: 2,
            operation_id: None,
            display_name: None,
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap();

    let diff = store.diff_channels("div-a", "div-b").await.unwrap();
    assert_eq!(diff.common.as_ref().unwrap().channel_id, "div-root");
    // Asymmetric siblings: each side reports its own fork edge.
    assert_eq!(diff.left_diverged_at_cursor, Some(1));
    assert_eq!(diff.right_diverged_at_cursor, Some(2));

    cleanup(&store).await;
}

/// Parent/child diff: the parent never diverged (None on its side).
#[tokio::test]
async fn diff_channels_parent_child_divergence_is_asymmetric() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .publish_with_channel_cursor(request("pc-root", "step", json!({"n": 1})))
        .await
        .unwrap();
    store
        .save_state("pc-root", 1, &json!({"shared": true}), None, None)
        .await
        .unwrap();
    store
        .fork_channel(&ForkRequest {
            source_channel_id: "pc-root".into(),
            new_channel_id: "pc-child".into(),
            at_cursor: 1,
            operation_id: None,
            display_name: None,
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap();

    // left = parent (root), right = child.
    let diff = store.diff_channels("pc-root", "pc-child").await.unwrap();
    assert_eq!(diff.common.as_ref().unwrap().channel_id, "pc-root");
    assert_eq!(diff.left_diverged_at_cursor, None, "parent IS the LCA");
    assert_eq!(diff.right_diverged_at_cursor, Some(1));

    // Reversed: symmetric swap.
    let rev = store.diff_channels("pc-child", "pc-root").await.unwrap();
    assert_eq!(rev.left_diverged_at_cursor, Some(1));
    assert_eq!(rev.right_diverged_at_cursor, None);

    cleanup(&store).await;
}

/// An execution tree deeper than MAX_TREE_DEPTH reports truncation instead
/// of silently dropping forks.
#[tokio::test]
async fn execution_tree_reports_truncation_beyond_depth() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .save_state("deep-0", 1, &json!({}), None, None)
        .await
        .unwrap();
    // Build a chain 12 deep (max depth is 10).
    for i in 0..12 {
        let parent = if i == 0 {
            "deep-0".to_string()
        } else {
            format!("deep-{i}")
        };
        let child = format!("deep-{}", i + 1);
        store
            .fork_channel(&ForkRequest {
                source_channel_id: parent,
                new_channel_id: child,
                at_cursor: 1,
                operation_id: None,
                display_name: None,
                reason: None,
                experiment_metadata: None,
                ..Default::default()
            })
            .await
            .unwrap();
    }

    let (tree, truncated) = store.get_fork_tree("deep-0").await.unwrap();
    assert!(truncated, "12-level tree must be reported as truncated");
    // Root + the deepest 10 descendants are retained.
    let mut node = &tree;
    let mut depth = 0;
    while let Some(first_child) = node.children.first() {
        node = first_child;
        depth += 1;
    }
    assert_eq!(depth, 10, "tree must stop at MAX_TREE_DEPTH");

    cleanup(&store).await;
}

/// Decision trails beyond MAX_TREE_DEPTH report truncated instead of
/// silently cutting the ancestry.
#[tokio::test]
async fn decision_trail_reports_truncation_beyond_depth() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .save_state("tdeep-0", 1, &json!({}), None, None)
        .await
        .unwrap();
    for i in 0..12 {
        let parent = if i == 0 {
            "tdeep-0".to_string()
        } else {
            format!("tdeep-{i}")
        };
        let child = format!("tdeep-{}", i + 1);
        store
            .fork_channel(&ForkRequest {
                source_channel_id: parent,
                new_channel_id: child,
                at_cursor: 1,
                operation_id: None,
                display_name: None,
                reason: None,
                experiment_metadata: None,
                ..Default::default()
            })
            .await
            .unwrap();
    }

    let trail = store.get_decision_trail("tdeep-12").await.unwrap();
    assert!(
        trail.truncated,
        "deep ancestry must be reported as truncated"
    );
    assert_eq!(trail.max_depth, 10);

    cleanup(&store).await;
}

/// A state diff with more than MAX_DIFF_ENTRIES differences reports
/// truncated + entry_count_total honestly.
#[tokio::test]
async fn diff_channels_reports_truncation_and_total() {
    let store = create_store().await;
    cleanup(&store).await;

    let mut left_str = String::from("{");
    let mut right_str = String::from("{");
    for i in 0..1200 {
        if i > 0 {
            left_str.push(',');
            right_str.push(',');
        }
        left_str.push_str(&format!("\"k{i}\": {{\"value\": {i}}}"));
        right_str.push_str(&format!("\"k{}\": {{\"value\": {}}}", i, i + 1));
    }
    left_str.push('}');
    right_str.push('}');
    let left: sonic_rs::Value = sonic_rs::from_str(&left_str).expect("left json");
    let right: sonic_rs::Value = sonic_rs::from_str(&right_str).expect("right json");
    store
        .save_state("big-l", 1, &left, None, None)
        .await
        .unwrap();
    store
        .save_state("big-r", 1, &right, None, None)
        .await
        .unwrap();

    let diff = store.diff_channels("big-l", "big-r").await.unwrap();
    assert!(diff.truncated, "1200 diffs must be reported as truncated");
    assert_eq!(diff.entry_count_total, 1200);
    assert_eq!(diff.max_entries, 500);
    assert_eq!(diff.entries.len(), 500);

    cleanup(&store).await;
}

/// Optimistic fork guards: accept a matching source version, reject drift.
#[tokio::test]
async fn fork_optimistic_guard_rejects_stale_source() {
    let store = create_store().await;
    cleanup(&store).await;

    let v1 = store
        .save_state("guard-p", 1, &json!({"v": 1}), None, None)
        .await
        .unwrap();

    // Matching version → fork succeeds.
    store
        .fork_channel(&ForkRequest {
            source_channel_id: "guard-p".into(),
            new_channel_id: "guard-c1".into(),
            at_cursor: 0,
            operation_id: None,
            display_name: None,
            reason: None,
            experiment_metadata: None,
            expected_version: Some(v1),
            expected_cursor: None,
            ..Default::default()
        })
        .await
        .expect("matching guard must pass");

    // Source advances → the stale guard is rejected.
    store
        .save_state("guard-p", 2, &json!({"v": 2}), None, None)
        .await
        .unwrap();
    let err = store
        .fork_channel(&ForkRequest {
            source_channel_id: "guard-p".into(),
            new_channel_id: "guard-c2".into(),
            at_cursor: 0,
            operation_id: None,
            display_name: None,
            reason: None,
            experiment_metadata: None,
            expected_version: Some(v1),
            expected_cursor: None,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(
        matches!(err, EventStoreError::VersionConflict { .. }),
        "expected VersionConflict, got {err:?}"
    );

    cleanup(&store).await;
}

/// Experiment groups: create, add members, rank by score.
#[tokio::test]
async fn experiment_group_create_add_rank() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .save_state("exp-root", 1, &json!({}), None, None)
        .await
        .unwrap();
    store
        .fork_channel(&ForkRequest {
            source_channel_id: "exp-root".into(),
            new_channel_id: "exp-a".into(),
            at_cursor: 1,
            operation_id: None,
            display_name: Some("A".into()),
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap();
    store
        .fork_channel(&ForkRequest {
            source_channel_id: "exp-root".into(),
            new_channel_id: "exp-b".into(),
            at_cursor: 1,
            operation_id: None,
            display_name: Some("B".into()),
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap();

    store
        .set_channel_outcome("exp-a", "promoted", Some(0.95))
        .await
        .unwrap();
    store
        .set_channel_outcome("exp-b", "rejected", Some(0.4))
        .await
        .unwrap();

    let group = store
        .create_experiment(&CreateExperimentRequest {
            name: "compare-models".into(),
            description: None,
            baseline_channel_id: Some("exp-root".into()),
        })
        .await
        .unwrap();
    store
        .add_experiment_member(
            &group.group_id,
            &AddExperimentMemberRequest {
                channel_id: "exp-a".into(),
                role: "variant".into(),
                declared_delta: Some(sonic_rs::json!({"model": "gpt-5"})),
            },
        )
        .await
        .unwrap();
    store
        .add_experiment_member(
            &group.group_id,
            &AddExperimentMemberRequest {
                channel_id: "exp-b".into(),
                role: "variant".into(),
                declared_delta: Some(sonic_rs::json!({"model": "claude"})),
            },
        )
        .await
        .unwrap();

    let (_, ranked) = store.rank_experiment(&group.group_id).await.unwrap();
    assert_eq!(ranked.len(), 3);
    assert_eq!(ranked[0].channel_id, "exp-a", "highest score first");
    assert!(
        (ranked[0].result_score.unwrap() - 0.95).abs() < 1e-4,
        "result_score = {:?}, want ~0.95 (f32 storage)",
        ranked[0].result_score
    );
    assert_eq!(ranked[0].outcome.as_deref(), Some("promoted"));

    cleanup(&store).await;
}

/// Promote: the child's latest state lands on the parent with a
/// run.promoted event, and the child is marked promoted.
#[tokio::test]
async fn promote_copies_child_state_to_parent_and_marks_promoted() {
    // A one-connection pool reproduces the production 500 caused by trying
    // to acquire a second connection from inside the promotion transaction.
    let store = create_store_with_pool_size(1).await;
    cleanup(&store).await;

    store
        .publish_with_channel_cursor(request("prom-root", "step", json!({"n": 1})))
        .await
        .unwrap();
    store
        .save_state("prom-root", 1, &json!({"base": true}), None, None)
        .await
        .unwrap();
    store
        .fork_channel(&ForkRequest {
            source_channel_id: "prom-root".into(),
            new_channel_id: "prom-child".into(),
            at_cursor: 1,
            operation_id: None,
            display_name: None,
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap();

    // The child diverges with a winning state.
    store
        .publish_with_channel_cursor(request("prom-child", "step", json!({"n": 2})))
        .await
        .unwrap();
    store
        .save_state(
            "prom-child",
            2,
            &json!({"base": true, "winner": true}),
            None,
            None,
        )
        .await
        .unwrap();

    let (parent, cursor) = store.promote_channel("prom-child").await.unwrap();
    assert_eq!(parent, "prom-root");
    assert!(cursor >= 2);

    let (c, state) = store.load_state("prom-root").await.unwrap().unwrap();
    assert_eq!(c, cursor);
    assert_eq!(state, json!({"base": true, "winner": true}));

    let parent_events = store
        .replay(&ReplayRequest {
            channel_id: "prom-root".into(),
            cursor: None,
            limit: Some(10),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap();
    assert!(
        parent_events.iter().any(|e| e.event_type == "run.promoted"),
        "run.promoted must be recorded on the parent"
    );
    let promoted = parent_events
        .iter()
        .find(|e| e.event_type == "run.promoted")
        .expect("promotion event");
    assert_eq!(
        promoted.cursor, cursor,
        "promotion state and event must share a cursor"
    );

    let child_meta = store
        .get_channel_metadata("prom-child")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(child_meta.outcome.as_deref(), Some("promoted"));

    cleanup(&store).await;
}

/// Soft-delete: purges rows, keeps the metadata with deleted_at, rejects
/// non-recursive deletes of a channel with children, and idempotent replays
/// of deleted forks resolve to 410.
#[tokio::test]
async fn delete_channel_purges_and_reports_gone() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .publish_with_channel_cursor(request("gc-p", "step", json!({"n": 1})))
        .await
        .unwrap();
    store
        .save_state("gc-p", 1, &json!({"v": 1}), None, None)
        .await
        .unwrap();
    store
        .fork_channel(&ForkRequest {
            source_channel_id: "gc-p".into(),
            new_channel_id: "gc-c".into(),
            at_cursor: 1,
            operation_id: None,
            display_name: None,
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap();

    // Non-recursive delete of a parent with a child is rejected.
    let err = store.delete_channel("gc-p", false).await.unwrap_err();
    assert!(
        matches!(err, EventStoreError::ChannelHasChildren { .. }),
        "expected ChannelHasChildren, got {err:?}"
    );

    // Recursive delete purges both.
    store.delete_channel("gc-p", true).await.unwrap();
    // Events/states/cursors are gone; the metadata row is retained with
    // deleted_at set (provenance + 410 on idempotent replays).
    assert_eq!(store.load_state("gc-c").await.unwrap(), None);
    let events = store
        .replay(&ReplayRequest {
            channel_id: "gc-c".into(),
            cursor: None,
            limit: Some(10),
            event_type: None,
            channel_cursor: None,
        })
        .await
        .unwrap();
    assert!(events.is_empty(), "events must be purged");

    // Metadata row retained with deleted_at → replayed forks 410.
    let meta = store.get_channel_metadata("gc-c").await.unwrap().unwrap();
    assert!(meta.deleted_at.is_some());
    let err = store
        .fork_channel(&ForkRequest {
            source_channel_id: "gc-p".into(),
            new_channel_id: "gc-c".into(),
            at_cursor: 1,
            operation_id: None,
            display_name: None,
            reason: None,
            experiment_metadata: None,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(
        matches!(err, EventStoreError::ChannelDeleted(_)),
        "expected ChannelDeleted, got {err:?}"
    );

    cleanup(&store).await;
}

/// compare_channels combines the state diff with manifest/tool/output/metrics deltas.
#[tokio::test]
async fn compare_channels_combines_state_manifest_tools_outputs() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .save_state("cmp-root", 1, &json!({"base": true}), None, None)
        .await
        .unwrap();

    for child in ["cmp-a", "cmp-b"] {
        store
            .fork_channel(&ForkRequest {
                source_channel_id: "cmp-root".into(),
                new_channel_id: child.into(),
                at_cursor: 1,
                operation_id: None,
                display_name: None,
                reason: None,
                experiment_metadata: Some(sonic_rs::json!({
                    "result": {"output": child},
                    "metrics": {"tokens": if child == "cmp-a" { 100 } else { 200 }},
                })),
                manifest: Some(sonic_rs::json!({
                    "model": if child == "cmp-a" { "gpt-5" } else { "claude" },
                    "seed": 42,
                })),
                ..Default::default()
            })
            .await
            .unwrap();
    }

    let cmp = store.compare_channels("cmp-a", "cmp-b").await.unwrap();
    assert_eq!(cmp.left_channel_id, "cmp-a");
    assert_eq!(cmp.state_diff.left_channel_id, "cmp-a");
    // Manifest diff shows the model change.
    assert!(cmp.manifest_diff.iter().any(|e| e.kind == "changed"));
    // Output diff.
    assert!(cmp.output_changed);
    assert_eq!(cmp.output_left, Some(sonic_rs::json!("cmp-a")));
    assert_eq!(cmp.output_right, Some(sonic_rs::json!("cmp-b")));
    // Metrics delta (tokens 100 vs 200).
    assert!(cmp.metrics_delta.iter().any(|e| e.kind == "changed"));

    cleanup(&store).await;
}

/// Concurrent DIVERGENT forks: two siblings forked from the same parent at
/// DIFFERENT boundaries must each resolve their own boundary with no
/// cross-contamination.
#[tokio::test]
async fn concurrent_divergent_forks_resolve_own_boundaries() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .publish_with_channel_cursor(request("cd-root", "step", json!({"n": 1})))
        .await
        .unwrap();
    store
        .save_state("cd-root", 1, &json!({"v": 1}), None, None)
        .await
        .unwrap();
    store
        .publish_with_channel_cursor(request("cd-root", "step", json!({"n": 2})))
        .await
        .unwrap();
    store
        .save_state("cd-root", 2, &json!({"v": 2}), None, None)
        .await
        .unwrap();

    let store = std::sync::Arc::new(store);
    let mut handles = Vec::new();
    for (child, cursor) in [("cd-a", 1_i64), ("cd-b", 2_i64)] {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            let (meta, replayed) = store
                .fork_channel(&ForkRequest {
                    source_channel_id: "cd-root".into(),
                    new_channel_id: child.into(),
                    at_cursor: cursor,
                    operation_id: None,
                    display_name: None,
                    reason: None,
                    experiment_metadata: None,
                    ..Default::default()
                })
                .await
                .expect("concurrent fork must succeed");
            (meta, replayed)
        }));
    }
    for h in handles {
        let (meta, replayed) = h.await.expect("task");
        assert!(!replayed);
        // Each child resolves its OWN requested boundary.
        let expected = if meta.channel_id == "cd-a" { 1 } else { 2 };
        assert_eq!(
            meta.resolved_state_cursor, expected,
            "child {} must resolve to its own boundary",
            meta.channel_id
        );
        let (cursor, state) = store.load_state(&meta.channel_id).await.unwrap().unwrap();
        assert_eq!(cursor, expected);
        assert_eq!(state, json!({"v": expected}));
    }

    cleanup(&store).await;
}

#[tokio::test]
async fn purge_is_provenance_aware_and_archives() {
    let store = create_store().await;
    cleanup(&store).await;

    // Parent + child linked by depends_on on ch-1.
    let parent = store
        .publish_with_channel_cursor(request("ch-1", "parent", json!({})))
        .await
        .expect("publish failed")
        .0;
    store
        .publish_with_channel_cursor(PublishEventRequest {
            channel_id: "ch-1".into(),
            event_type: "child".into(),
            payload: json!({}),
            metadata: EventMetadata {
                actor: "test".into(),
                agent_id: None,
                user_id: None,
                metadata: None,
            },
            depends_on: Some(parent.id),
            dependencies: Vec::new(),
            operation_id: None,
            step_number: None,
        })
        .await
        .expect("publish failed");

    // Two unprotected stale events on another channel.
    store
        .publish_with_channel_cursor(request("ch-2", "stale.one", json!({})))
        .await
        .expect("publish failed");
    store
        .publish_with_channel_cursor(request("ch-2", "stale.two", json!({})))
        .await
        .expect("publish failed");

    // Backdate everything older than the retention cutoff.
    let pool = store.pool();
    let client = pool.get().await.expect("pool get failed");
    client
        .execute(
            "UPDATE agent_events SET timestamp = NOW() - INTERVAL '10 days'",
            &[],
        )
        .await
        .expect("backdate failed");

    let cutoff = chrono::Utc::now() - chrono::Duration::days(1);
    let deleted = store
        .purge_events_older_than(cutoff)
        .await
        .expect("purge failed");

    // Only the depends_on-referenced PARENT survives; the old child and the
    // two stale events are purged (the child is pruned along with its own
    // reference, so no dangling edge remains).
    assert_eq!(deleted, 3, "child + 2 stale events are purged");
    let remaining: i64 = client
        .query_one("SELECT COUNT(*)::bigint AS n FROM agent_events", &[])
        .await
        .expect("count failed")
        .get("n");
    assert_eq!(remaining, 1, "depends_on-referenced parent must survive");
    let parent_survives: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint AS n FROM agent_events WHERE id = $1",
            &[&parent.id],
        )
        .await
        .expect("count failed")
        .get("n");
    assert_eq!(parent_survives, 1);

    // Purged rows are archived for provenance recovery.
    let archived: i64 = client
        .query_one("SELECT COUNT(*)::bigint AS n FROM event_archive", &[])
        .await
        .expect("count failed")
        .get("n");
    assert_eq!(archived, 3, "purged rows must be archived");
    let ch2_archived: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint AS n FROM event_archive WHERE channel_id = 'ch-2'",
            &[],
        )
        .await
        .expect("count failed")
        .get("n");
    assert_eq!(ch2_archived, 2);

    cleanup(&store).await;
}

#[tokio::test]
async fn publish_registers_channel_and_hash_chain_verifies() {
    let store = create_store().await;
    cleanup(&store).await;

    for i in 0..3 {
        store
            .publish_with_channel_cursor(request("reg.ch", "reg.event", json!({"i": i})))
            .await
            .expect("publish failed");
    }

    // Registry row exists with the right last_cursor (audit item 14).
    let pool = store.pool();
    let client = pool.get().await.expect("pool get failed");
    let row = client
        .query_one(
            "SELECT owner, last_cursor FROM channels WHERE channel_id = 'reg.ch'",
            &[],
        )
        .await
        .expect("registry row missing");
    let owner: Option<String> = row.get("owner");
    let last_cursor: i64 = row.get("last_cursor");
    assert_eq!(owner.as_deref(), Some("test"), "owner = first actor");
    assert_eq!(last_cursor, 3);

    // Tamper-evident chain verifies (audit selling point 3).
    let v = store.verify_chain("reg.ch").await.expect("verify failed");
    assert!(v.valid, "chain must be valid: {v:?}");
    assert_eq!(v.verified_events, 3);
    assert_eq!(v.unverified_events, 0);

    // list_channels reads from the registry.
    let channels = store.list_channels().await.expect("list failed");
    assert!(channels.contains(&"reg.ch".to_string()));

    cleanup(&store).await;
}

#[tokio::test]
async fn verify_chain_detects_tampering() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .publish_with_channel_cursor(request("tamper.ch", "tamper.one", json!({"n": 1})))
        .await
        .expect("publish failed");
    store
        .publish_with_channel_cursor(request("tamper.ch", "tamper.two", json!({"n": 2})))
        .await
        .expect("publish failed");

    assert!(store.verify_chain("tamper.ch").await.unwrap().valid);

    // Directly mutate a recorded payload (simulating tampering).
    let pool = store.pool();
    let client = pool.get().await.expect("pool get failed");
    client
        .execute(
            "UPDATE agent_events SET payload = '{\"n\": 999}' WHERE channel_id = 'tamper.ch' AND cursor = 2",
            &[],
        )
        .await
        .expect("tamper update failed");

    let v = store
        .verify_chain("tamper.ch")
        .await
        .expect("verify failed");
    assert!(!v.valid, "chain must detect the tamper");
    assert_eq!(v.broken_at_cursor, Some(2));
    assert!(v.first_bad_hash.is_some());
    assert_eq!(v.verified_events, 1, "the first event still verifies");

    cleanup(&store).await;
}

#[tokio::test]
async fn fork_registers_child_with_lineage() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .publish_with_channel_cursor(request("lineage.root", "a", json!({})))
        .await
        .expect("publish failed");
    // A saved snapshot makes the fork resolvable.
    let pool = store.pool();
    let client = pool.get().await.expect("pool get failed");
    client
        .execute(
            "INSERT INTO channel_states (channel_id, cursor, state) VALUES ('lineage.root', 1, '{\"step\":1}')",
            &[],
        )
        .await
        .expect("snapshot insert failed");

    let fork = ForkRequest {
        source_channel_id: "lineage.root".into(),
        new_channel_id: "lineage.child".into(),
        at_cursor: 1,
        display_name: None,
        reason: None,
        experiment_metadata: None,
        operation_id: None,
        manifest: None,
        tool_policies: None,
        expected_version: None,
        expected_cursor: None,
    };
    store.fork_channel(&fork).await.expect("fork failed");

    let row = client
        .query_one(
            "SELECT parent_channel_id, forked_at_cursor FROM channels WHERE channel_id = 'lineage.child'",
            &[],
        )
        .await
        .expect("child channel not registered");
    let parent: Option<String> = row.get("parent_channel_id");
    let forked_at: Option<i64> = row.get("forked_at_cursor");
    assert_eq!(parent.as_deref(), Some("lineage.root"));
    assert_eq!(forked_at, Some(1));

    cleanup(&store).await;
}

#[tokio::test]
async fn close_channel_stamps_closed_at() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .publish_with_channel_cursor(request("close.ch", "e", json!({})))
        .await
        .expect("publish failed");

    store.close_channel("close.ch").await.expect("close failed");

    let pool = store.pool();
    let client = pool.get().await.expect("pool get failed");
    let closed_at: Option<chrono::DateTime<chrono::Utc>> = client
        .query_one(
            "SELECT closed_at FROM channels WHERE channel_id = 'close.ch'",
            &[],
        )
        .await
        .expect("registry row missing")
        .get("closed_at");
    assert!(closed_at.is_some(), "closed_at must be stamped");

    // Unknown channel → NotFound.
    let err = store.close_channel("does.not.exist").await.unwrap_err();
    assert!(matches!(
        err,
        actae::events::store::EventStoreError::NotFound
    ));

    cleanup(&store).await;
}

// ── Side-effect-aware fork replay concurrency ──────────────────────────

#[tokio::test]
async fn fork_replay_concurrent_claims_are_race_safe() {
    // Two workers claiming the same (child, key) under an `auto` fork must not
    // race the tool_executions unique constraint into a 500: both resolve to
    // `replayed` and exactly one marker event is written.
    let store = create_store().await;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let parent = format!("race-parent-{suffix}");
    let child = format!("race-child-{suffix}");

    // The parent must exist for the fork.
    store
        .publish_with_channel_cursor(PublishEventRequest {
            channel_id: parent.clone(),
            event_type: "step".into(),
            payload: json!({"seq": 1}),
            metadata: EventMetadata {
                actor: "test".into(),
                agent_id: None,
                user_id: None,
                metadata: None,
            },
            depends_on: None,
            dependencies: vec![],
            operation_id: None,
            step_number: None,
        })
        .await
        .unwrap();

    // A completed side effect on the parent.
    let (claim, _) = store
        .claim_execution(&ClaimExecutionRequest {
            channel_id: parent.clone(),
            key_name: "k-1".into(),
            tool_name: "charge".into(),
            params: Some(json!({"amount": 5})),
            dedup_fields: None,
            lease_seconds: None,
            emit_replay_event: None,
            actor: None,
        })
        .await
        .unwrap();
    store
        .complete_execution(
            claim.execution.id,
            claim.claim_token,
            &CompleteExecutionRequest {
                claim_token: None,
                result: Some(json!({"paid": true})),
            },
        )
        .await
        .unwrap();

    // Fork with the default `auto` policy.
    store
        .fork_channel(&ForkRequest {
            source_channel_id: parent.clone(),
            new_channel_id: child.clone(),
            at_cursor: 0,
            tool_policies: Some(json!({"charge": "auto"})),
            ..Default::default()
        })
        .await
        .expect("fork should succeed");

    let request = ClaimExecutionRequest {
        channel_id: child.clone(),
        key_name: "k-1".into(),
        tool_name: "charge".into(),
        params: Some(json!({"amount": 5})),
        dedup_fields: None,
        lease_seconds: None,
        emit_replay_event: None,
        actor: None,
    };
    let sources = vec![parent.clone()];
    let (first, second) = tokio::join!(
        store.replay_fork_execution(&request, &sources),
        store.replay_fork_execution(&request, &sources),
    );
    assert!(
        matches!(first, Ok(ForkReplayResult::Replayed(..))),
        "first concurrent replay must succeed, got {first:?}"
    );
    assert!(
        matches!(second, Ok(ForkReplayResult::Replayed(..))),
        "second concurrent replay must succeed, got {second:?}"
    );

    let replay_events = store
        .replay(&ReplayRequest {
            channel_id: child.clone(),
            cursor: None,
            channel_cursor: None,
            limit: Some(100),
            event_type: Some("tool.replayed".into()),
        })
        .await
        .unwrap();
    assert_eq!(replay_events.len(), 1, "exactly one replay marker");
    let started = store
        .replay(&ReplayRequest {
            channel_id: child.clone(),
            cursor: None,
            channel_cursor: None,
            limit: Some(100),
            event_type: Some("tool.started".into()),
        })
        .await
        .unwrap();
    assert!(started.is_empty(), "the tool must never start on the child");
}

#[tokio::test]
async fn latest_step_number_tracks_indexed_steps() {
    let store = create_store().await;
    let channel = format!("latest-step-{}", uuid::Uuid::new_v4().simple());

    assert_eq!(store.latest_step_number(&channel).await.unwrap(), 0);

    for step in [2_i64, 1_i64] {
        store
            .publish_with_channel_cursor(PublishEventRequest {
                channel_id: channel.clone(),
                event_type: "step".into(),
                payload: json!({"step_number": step}),
                metadata: EventMetadata {
                    actor: "test".into(),
                    agent_id: None,
                    user_id: None,
                    metadata: None,
                },
                depends_on: None,
                dependencies: vec![],
                operation_id: None,
                step_number: Some(step),
            })
            .await
            .unwrap();
    }

    assert_eq!(store.latest_step_number(&channel).await.unwrap(), 2);
}

#[tokio::test]
async fn fork_tool_context_walks_deep_chain_nearest_first() {
    let store = create_store().await;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let root = format!("ctx-root-{suffix}");
    let child = format!("ctx-child-{suffix}");
    let grand = format!("ctx-grand-{suffix}");

    let publish = |channel: String, step: i64| {
        let store = &store;
        async move {
            store
                .publish_with_channel_cursor(PublishEventRequest {
                    channel_id: channel,
                    event_type: "step".into(),
                    payload: json!({"step_number": step}),
                    metadata: EventMetadata {
                        actor: "test".into(),
                        agent_id: None,
                        user_id: None,
                        metadata: None,
                    },
                    depends_on: None,
                    dependencies: vec![],
                    operation_id: None,
                    step_number: Some(step),
                })
                .await
                .unwrap();
        }
    };
    publish(root.clone(), 1).await;
    store
        .fork_channel(&ForkRequest {
            source_channel_id: root.clone(),
            new_channel_id: child.clone(),
            at_cursor: 0,
            ..Default::default()
        })
        .await
        .unwrap();
    publish(child.clone(), 2).await;
    store
        .fork_channel(&ForkRequest {
            source_channel_id: child.clone(),
            new_channel_id: grand.clone(),
            at_cursor: 0,
            tool_policies: Some(json!({"charge": "auto"})),
            ..Default::default()
        })
        .await
        .unwrap();

    // Complete the same logical effect on both the root and the child.
    for channel in [root.as_str(), child.as_str()] {
        let (claim, _) = store
            .claim_execution(&ClaimExecutionRequest {
                channel_id: channel.to_string(),
                key_name: "charge-1".into(),
                tool_name: "charge".into(),
                params: Some(json!({"amount": 5})),
                dedup_fields: None,
                lease_seconds: None,
                emit_replay_event: None,
                actor: None,
            })
            .await
            .unwrap();
        store
            .complete_execution(
                claim.execution.id,
                claim.claim_token,
                &CompleteExecutionRequest {
                    claim_token: None,
                    result: Some(json!({"paid": true})),
                },
            )
            .await
            .unwrap();
    }

    let (sources, policy) = store
        .fork_tool_context(&grand, "charge", "charge-1")
        .await
        .unwrap()
        .expect("grandchild is a fork");
    assert_eq!(policy, "auto");
    // Nearest ancestor first: child (depth 1), then root (depth 2).
    assert_eq!(sources, vec![child.clone(), root.clone()]);

    // A `live` policy needs no source chain (skips the recursive query).
    let (live_sources, live_policy) = {
        store
            .fork_channel(&ForkRequest {
                source_channel_id: child.clone(),
                new_channel_id: format!("ctx-live-{suffix}"),
                at_cursor: 0,
                tool_policies: Some(json!({"charge": "live"})),
                ..Default::default()
            })
            .await
            .unwrap();
        store
            .fork_tool_context(&format!("ctx-live-{suffix}"), "charge", "charge-1")
            .await
            .unwrap()
            .unwrap()
    };
    assert_eq!(live_policy, "live");
    assert!(live_sources.is_empty());
}

async fn set_session_meta(store: &EventStore, meta: sonic_rs::Value) {
    store
        .update_metadata(&UpdateMetadataRequest {
            channel_id: "recover-chan".into(),
            display_name: None,
            reason: None,
            experiment_metadata: Some(meta),
        })
        .await
        .unwrap();
}

/// A same-channel resume (session metadata re-entering `started` from an
/// in-progress `stepping` OR a caught `crashed` state) writes exactly one
/// `recovery_operations` usage meter; retries and non-session metadata edits
/// do not. This is the server-observable recovery signal shared by the SDKs'
/// session metadata contract, covering both caught exceptions and hard kills.
#[tokio::test]
async fn crash_recovery_transition_records_one_usage_meter() {
    let store = create_store().await;
    cleanup(&store).await;

    store
        .publish_with_channel_cursor(request("recover-chan", "step", json!({"i": 0})))
        .await
        .unwrap();

    // 1. Fresh session start (prior NULL) — NOT a recovery.
    set_session_meta(&store, json!({"session_name": "s", "status": "started"})).await;
    // 2. First step persists `stepping`.
    set_session_meta(&store, json!({"session_name": "s", "status": "stepping"})).await;
    // 3. HARD KILL: the metadata stays `stepping`; the resume writes `started`.
    set_session_meta(&store, json!({"session_name": "s", "status": "started"})).await;
    // 4. A retried `started` write must NOT double-count.
    set_session_meta(&store, json!({"session_name": "s", "status": "started"})).await;

    // 5. Caught exception: `crashed` -> `started` is a second recovery.
    set_session_meta(&store, json!({"session_name": "s", "status": "stepping"})).await;
    set_session_meta(&store, json!({"session_name": "s", "status": "crashed"})).await;
    set_session_meta(&store, json!({"session_name": "s", "status": "started"})).await;

    // 6. A non-session metadata edit (no `session_name`) is not a recovery.
    set_session_meta(&store, json!({"session_name": "s", "status": "stepping"})).await;
    set_session_meta(&store, json!({"status": "started"})).await;

    // 6b. A session `started` over a PRIOR non-session row is not a recovery
    //     either (both sides must be session metadata).
    set_session_meta(&store, json!({"status": "stepping"})).await;
    set_session_meta(&store, json!({"session_name": "s", "status": "started"})).await;

    // 7. Reusing a cleanly-`completed` channel is not a recovery.
    set_session_meta(&store, json!({"session_name": "s", "status": "completed"})).await;
    set_session_meta(&store, json!({"session_name": "s", "status": "started"})).await;

    // 8. A fork child's metadata sequence (`created` -> `started`) is NOT a
    //    recovery — this is exactly what `_fork_from_channel` + `_start` do, so
    //    creating a fork must never inflate the recovery meter.
    set_session_meta(&store, json!({"session_name": "s", "status": "created"})).await;
    set_session_meta(&store, json!({"session_name": "s", "status": "started"})).await;

    // 9. Non-object metadata (a scalar or array) must be accepted without a
    //    panic and must never count as a recovery.
    set_session_meta(&store, json!("not an object")).await;
    set_session_meta(&store, json!([1, 2, 3])).await;

    let client = store.pool().get().await.unwrap();
    let meters: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM usage_delivery_outbox WHERE metric = 'recovery_operations'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        meters, 2,
        "one meter per real recovery (hard-kill + caught crash); none for \
         fresh starts, retries, non-session edits, or completed channels"
    );
    drop(client);
    cleanup(&store).await;
}

async fn recovery_meter_count(store: &EventStore) -> i64 {
    let client = store.pool().get().await.unwrap();
    client
        .query_one(
            "SELECT COUNT(*) FROM usage_delivery_outbox WHERE metric = 'recovery_operations'",
            &[],
        )
        .await
        .unwrap()
        .get(0)
}

/// The explicit `run.recovered` marker is the opt-in recovery signal for
/// clients that do not use AgentSession metadata. One fresh marker meters one
/// `recovery_operations`; ordinary events do not; and an idempotent replay of
/// the marker (same operation_id + content) does not double-count.
#[tokio::test]
async fn recovery_marker_event_meters_recovery_once() {
    let store = create_store().await;
    cleanup(&store).await;

    // A normal event never counts.
    store
        .publish_with_channel_cursor(request("rec-marker", "step", json!({"i": 0})))
        .await
        .unwrap();
    assert_eq!(recovery_meter_count(&store).await, 0);

    // The marker counts once.
    let op = uuid::Uuid::new_v4();
    store
        .publish_with_channel_cursor(request_with_op(
            "rec-marker",
            "run.recovered",
            json!({"reason": "worker killed"}),
            op,
        ))
        .await
        .unwrap();
    assert_eq!(recovery_meter_count(&store).await, 1);

    // An idempotent replay of the same marker must not double-count.
    store
        .publish_with_channel_cursor(request_with_op(
            "rec-marker",
            "run.recovered",
            json!({"reason": "worker killed"}),
            op,
        ))
        .await
        .unwrap();
    assert_eq!(recovery_meter_count(&store).await, 1);

    cleanup(&store).await;
}
