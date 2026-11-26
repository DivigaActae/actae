// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use postgres_types::{IsNull, ToSql, Type};
use tokio_postgres::Row;
use tracing::{info, instrument, warn};
use uuid::Uuid;

use super::schema::{
    AddExperimentMemberRequest, AgentEvent, CancelExecutionRequest, CausalGraph, ChannelMetadata,
    ClaimExecutionRequest, ClaimedWork, CompareResponse, CompleteExecutionRequest,
    CreateExperimentRequest, CreateWakeupRequest, DecisionTrail, EventDependency, EventMetadata,
    ExecutionClaimResult, ExecutionGroup, ExecutionGroupMember, ExecutionInfo, ExperimentGroup,
    ExperimentMember, FailExecutionRequest, ForkBoundary, ForkInfo, ForkReplayResult, ForkRequest,
    GroupForkReceipt, GroupInfo, GroupMessage, GroupOffsetInfo, HeartbeatExecutionRequest,
    LineageHop, MemberLease, PublishEventRequest, QueryRequest, RankedExperimentMember,
    ReplayRequest, StateDiffEntry, StateDiffResponse, StatePoint, TransitionRequest,
    UpdateMetadataRequest, WakeupInfo,
};
use crate::database::json_compat::JsonValue;
use sha2::{Digest, Sha256};
use sonic_rs::{JsonContainerTrait, JsonValueTrait};

/// Tamper-evident ledger hash (audit selling point 3).
///
/// `hash = sha256(channel_id | "|" | cursor | "|" | event_type | "|" |
/// canonical_payload | "|" | prev_hash)` where `canonical_payload` is the
/// sonic-rs serialization of the event payload (stable for the same input)
/// and `prev_hash` is the previous event's hash on the channel (`""` for the
/// first event). Any mutation of the recorded fields breaks the chain.
fn event_hash(
    channel_id: &str,
    cursor: i64,
    event_type: &str,
    canonical_payload: &str,
    prev_hash: Option<&str>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(channel_id.as_bytes());
    hasher.update(b"|");
    hasher.update(cursor.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(event_type.as_bytes());
    hasher.update(b"|");
    hasher.update(canonical_payload.as_bytes());
    hasher.update(b"|");
    hasher.update(prev_hash.unwrap_or("").as_bytes());
    hex::encode(hasher.finalize())
}

/// Concrete query parameter type that is both ToSql and Send.
/// Used instead of Box<dyn ToSql + Sync> which is !Send.
#[derive(Debug)]
enum Arg {
    Text(String),
    Int(i64),
    Timestamp(DateTime<Utc>),
}

/// Result of a tamper-evident ledger verification (audit selling point 3).
#[derive(Debug, Clone)]
pub struct ChainVerification {
    /// True when every event with a recorded hash is internally consistent.
    pub valid: bool,
    /// Number of events that have a recorded hash and passed verification.
    pub verified_events: u64,
    /// Number of pre-ledger events (NULL hash) that cannot be verified.
    pub unverified_events: u64,
    /// Cursor of the first event that failed verification (None when valid).
    pub broken_at_cursor: Option<i64>,
    /// The stored hash of the first broken event.
    pub first_bad_hash: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct UsageOutboxItem {
    pub idempotency_key: String,
    pub usage_date: String,
    pub metric: String,
    pub quantity: i64,
    pub source_generation: i64,
}

/// Result of the lowest-common-ancestor analysis between two channels.
struct LcaResult {
    lca_channel: String,
    /// The fork boundary (edge) where `left` departed the shared chain,
    /// or `None` when `left` IS the LCA (it never diverged).
    left_diverged_at_cursor: Option<i64>,
    /// The fork boundary (edge) where `right` departed the shared chain,
    /// or `None` when `right` IS the LCA.
    right_diverged_at_cursor: Option<i64>,
}

/// Per-channel summary for the dashboard overview.
#[derive(Debug, Clone)]
pub struct ChannelStat {
    pub channel_id: String,
    pub event_count: i64,
    pub latest_cursor: Option<i64>,
    pub last_event_at: Option<DateTime<Utc>>,
    /// Friendly name from `channel_metadata` (None when the channel has no
    /// metadata row). Lets the dashboard render names without a per-channel
    /// metadata lookup.
    pub display_name: Option<String>,
    /// Immediate parent for forked channels (None for root runs).
    pub parent_channel_id: Option<String>,
}

/// DB-backed counters for the dashboard overview (metrics endpoint).
#[derive(Debug, Clone, Default)]
pub struct EventStats {
    pub total_events: i64,
    pub channel_count: i64,
    pub state_snapshots: i64,
}

impl ToSql for Arg {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            Arg::Text(s) => s.to_sql(ty, out),
            Arg::Int(i) => i.to_sql(ty, out),
            Arg::Timestamp(ts) => ts.to_sql(ty, out),
        }
    }

    fn accepts(ty: &Type) -> bool {
        <String as ToSql>::accepts(ty) || <i64 as ToSql>::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            Arg::Text(s) => s.to_sql_checked(ty, out),
            Arg::Int(i) => i.to_sql_checked(ty, out),
            Arg::Timestamp(ts) => ts.to_sql_checked(ty, out),
        }
    }
}

/// Postgres-backed append-only event store.
/// This is the persistence layer for agent execution history.
/// Events are written to a single `agent_events` table with
/// composite indexes for efficient replay and query.
#[derive(Debug, Clone)]
pub struct EventStore {
    pool: Pool,
}

impl EventStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    /// Get the connection pool (for integration tests).
    #[doc(hidden)]
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Initialize the event store schema via the migration system.
    /// All SQL is idempotent and tracked in the _migrations table.
    pub async fn initialize(&self) -> Result<(), EventStoreError> {
        super::migrations::run_migrations(&self.pool).await?;
        let max_events = parse_non_negative_env("EVENT_MAX_EVENTS_PER_MONTH")?;
        let max_bytes = parse_non_negative_env("EVENT_MAX_INGRESS_BYTES_PER_MONTH")?;
        let max_durable_written =
            parse_non_negative_env("EVENT_MAX_DURABLE_WRITTEN_BYTES_PER_MONTH")?;
        let max_retained = parse_non_negative_env("EVENT_MAX_RETAINED_BYTES")?;
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        client
            .execute(
                "UPDATE usage_quota_config
                 SET max_events_month = $1,
                     max_ingress_bytes_month = $2,
                     max_durable_written_bytes_month = $3,
                     max_retained_bytes = $4,
                     updated_at = NOW()
                 WHERE singleton = TRUE",
                &[&max_events, &max_bytes, &max_durable_written, &max_retained],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(())
    }

    /// Publish an event with a per-channel sequential cursor.
    /// Uses a channel_cursors table with row-level locking to guarantee
    /// gapless, sequential per-channel cursors for audit trail completeness.
    ///
    /// Tradeoff: writes to the same channel serialize behind a row lock.
    /// Different channels never contend. For 95th-percentile workloads
    /// (agent events at 1-100/sec per channel), this is negligible.
    /// For hot channels (>500 writes/sec), prefer multiple sub-channels.
    ///
    /// Uses a TRANSACTION to ensure the counter increment and INSERT are atomic:
    ///   BEGIN
    ///   UPDATE channel_cursors SET last_cursor = last_cursor + 1
    ///     (or INSERT if first write to this channel)
    ///   INSERT agent_events (..., cursor)  -- the per-channel gapless cursor
    ///   COMMIT
    pub async fn publish_with_channel_cursor(
        &self,
        request: PublishEventRequest,
    ) -> Result<(AgentEvent, i64), EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;

        let mut transaction = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;

        if let Some(operation_id) = request.operation_id {
            if let Some(replayed) =
                find_transition_replay(&transaction, &request.channel_id, operation_id).await?
            {
                // A reused operation_id must carry the SAME content; a
                // different request is a caller bug (409 instead of a
                // silent, possibly corrupting replay).
                if event_content_matches(
                    &request.event_type,
                    &request.payload,
                    &request.metadata,
                    &replayed.0,
                ) && dependency_edges_match(
                    &transaction,
                    replayed.0.id,
                    request.depends_on,
                    &request.dependencies,
                )
                .await?
                {
                    return Ok(replayed);
                }
                return Err(EventStoreError::IdempotencyConflict {
                    operation_id: operation_id.to_string(),
                });
            }
        }

        let (id, cursor) = insert_event_in_tx(
            &mut transaction,
            &request.channel_id,
            &request.event_type,
            &request.payload,
            &request.metadata,
            request.depends_on,
            request.operation_id,
            Utc::now(),
        )
        .await?;
        insert_dependency_edges(&transaction, id, request.depends_on, &request.dependencies)
            .await?;

        // Server-owned step index (Phase 8): when the client supplies a step
        // number, record a durable step → (cursor, event) mapping in the SAME
        // transaction so step→cursor resolution and fork_at_step lineage are
        // exact and server-side. Idempotent replays hit ON CONFLICT DO NOTHING.
        if let Some(step_number) = request.step_number {
            transaction
                .execute(
                    "INSERT INTO channel_steps (channel_id, step_number, cursor, event_id)
                     VALUES ($1, $2, $3, $4)
                     ON CONFLICT (channel_id, step_number) DO NOTHING",
                    &[&request.channel_id, &step_number, &cursor, &id],
                )
                .await
                .map_err(EventStoreError::Execute)?;
        }

        // The channel's state version at the new event's cursor — 0 when
        // no snapshot exists yet. Mirrors the replay path below so the
        // fresh write and the idempotent replay agree.
        let version = latest_state_version(&transaction, &request.channel_id).await?;

        transaction
            .commit()
            .await
            .map_err(EventStoreError::Execute)?;

        Ok((
            AgentEvent {
                id,
                channel_id: request.channel_id,
                event_type: request.event_type,
                payload: request.payload,
                metadata: request.metadata,
                cursor,
                depends_on: request.depends_on,
                timestamp: Utc::now(),
            },
            version,
        ))
    }

    /// Atomically persist an event AND its resulting state snapshot in a
    /// single transaction (the "transition" operation).
    ///
    /// Optional optimistic-concurrency guards (`expected_version` /
    /// `expected_cursor`) reject the write unless the channel's latest saved
    /// state matches. `Some(0)` as expected_version means "no snapshot yet".
    ///
    /// Returns (event, new_state_version).
    #[instrument(skip(self, request), fields(channel = %request.channel_id, event_type = %request.event_type))]
    pub async fn publish_with_state(
        &self,
        request: &TransitionRequest,
    ) -> Result<(AgentEvent, i64), EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let now = Utc::now();
        let state_json = JsonValue::new(request.state.clone());

        let mut transaction = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;

        // Idempotent replay: a transition whose operation_id already has an
        // event on this channel is a retry of a previously committed
        // transition — IF the request content matches. The original attempt
        // persisted BOTH the event and its state snapshot in one
        // transaction, so a matching replay returns the original result
        // WITHOUT inserting anything again (a second snapshot row at the
        // same cursor would duplicate the state and bloat version history),
        // and skips the guards (the caller's expected_* are stale by
        // definition). A NON-matching request with a reused operation_id is
        // a caller bug and fails loudly with 409 idempotency_conflict.
        if let Some(op) = request.operation_id {
            if let Some((event, version)) =
                find_transition_replay(&transaction, &request.channel_id, op).await?
            {
                if !event_content_matches(
                    &request.event_type,
                    &request.payload,
                    &request.metadata,
                    &event,
                ) || !dependency_edges_match(
                    &transaction,
                    event.id,
                    request.depends_on,
                    &request.dependencies,
                )
                .await?
                {
                    return Err(EventStoreError::IdempotencyConflict {
                        operation_id: op.to_string(),
                    });
                }
                let version = if version > 0 {
                    version
                } else {
                    // A transition always writes a snapshot in the same
                    // transaction, so a replay with no snapshot at the
                    // event's cursor indicates data inconsistency.
                    return Err(EventStoreError::InconsistentState(format!(
                        "transition replay: state snapshot for cursor {} not found (channel {})",
                        event.cursor, request.channel_id
                    )));
                };
                // The snapshot is part of the transition's content: a retry
                // carrying different state for the same operation_id is a
                // caller bug, not a replay (409 instead of silently serving
                // the ORIGINAL snapshot).
                let snapshot_state =
                    resolve_fork_state(&transaction, &request.channel_id, event.cursor)
                        .await?
                        .map(|(_, _, state)| state);
                if !snapshot_state
                    .is_some_and(|stored| json_values_equal(&request.state, &stored.0))
                {
                    return Err(EventStoreError::IdempotencyConflict {
                        operation_id: op.to_string(),
                    });
                }
                transaction
                    .commit()
                    .await
                    .map_err(EventStoreError::Execute)?;
                return Ok((event, version));
            }
        }

        check_state_guards(
            &transaction,
            &request.channel_id,
            request.expected_version,
            request.expected_cursor,
        )
        .await?;

        let (id, cursor) = insert_event_in_tx(
            &mut transaction,
            &request.channel_id,
            &request.event_type,
            &request.payload,
            &request.metadata,
            request.depends_on,
            request.operation_id,
            now,
        )
        .await?;
        insert_dependency_edges(&transaction, id, request.depends_on, &request.dependencies)
            .await?;

        let state_row = transaction
            .query_one(
                "INSERT INTO channel_states (channel_id, cursor, state, timestamp)
                 VALUES ($1, $2, $3, $4)
                 RETURNING version",
                &[&request.channel_id, &cursor, &state_json, &now],
            )
            .await
            .map_err(map_quota_exec_error)?;
        let version: i64 = state_row.get("version");

        let state_bytes = sonic_rs::to_string(&request.state)
            .unwrap_or_default()
            .len()
            .min(i64::MAX as usize) as i64;
        let usage_date = now.date_naive();
        transaction
            .execute(
                "INSERT INTO usage_delivery_outbox
                 (idempotency_key, usage_date, metric, quantity, source_generation)
                 VALUES ($1, $2::date, 'state_bytes', $3, 1),
                        ($4, $2::date, 'durable_written_bytes', $3, 1)
                 ON CONFLICT (idempotency_key) DO NOTHING",
                &[
                    &format!("state:{id}"),
                    &usage_date,
                    &state_bytes,
                    &format!("state-durable:{id}"),
                ],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        transaction
            .commit()
            .await
            .map_err(EventStoreError::Execute)?;

        Ok((
            AgentEvent {
                id,
                channel_id: request.channel_id.clone(),
                event_type: request.event_type.clone(),
                payload: request.payload.clone(),
                metadata: request.metadata.clone(),
                cursor,
                depends_on: request.depends_on,
                timestamp: now,
            },
            version,
        ))
    }

    /// Replay events from a channel, starting from a per-channel cursor
    /// position: replays events where `cursor >` the given value (or from
    /// the beginning when no cursor is given).
    #[instrument(skip(self, request), fields(channel = %request.channel_id, cursor = ?request.cursor, limit = request.limit))]
    pub async fn replay(
        &self,
        request: &ReplayRequest,
    ) -> Result<Vec<AgentEvent>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let limit = request.limit.unwrap_or(100).min(1000);

        // `channel_cursor` is a deprecated alias for `cursor` (pre-per-channel
        // clients sent it); prefer the canonical field.
        let cursor_value = request.channel_cursor.or(request.cursor);
        let cursor_clause: &str = if cursor_value.is_some() {
            "cursor >"
        } else {
            ""
        };
        let cursor_bound = cursor_value.unwrap_or(0);

        let rows = if let Some(ref event_type) = request.event_type {
            if cursor_clause.is_empty() {
                client
                    .query(
                        "
                        SELECT id, channel_id, event_type, payload, actor,
                               agent_id, user_id, metadata, cursor, timestamp, depends_on
                        FROM agent_events
                        WHERE channel_id = $1
                          AND event_type = $2
                        ORDER BY cursor ASC
                        LIMIT $3
                        ",
                        &[&request.channel_id, event_type, &limit],
                    )
                    .await
                    .map_err(EventStoreError::Execute)?
            } else {
                client
                    .query(
                        "
                        SELECT id, channel_id, event_type, payload, actor,
                               agent_id, user_id, metadata, cursor, timestamp, depends_on
                        FROM agent_events
                        WHERE channel_id = $1
                          AND cursor > $2
                          AND event_type = $3
                        ORDER BY cursor ASC
                        LIMIT $4
                        ",
                        &[&request.channel_id, &cursor_bound, event_type, &limit],
                    )
                    .await
                    .map_err(EventStoreError::Execute)?
            }
        } else if cursor_clause.is_empty() {
            client
                .query(
                    "
                    SELECT id, channel_id, event_type, payload, actor,
                           agent_id, user_id, metadata, cursor, timestamp, depends_on
                    FROM agent_events
                    WHERE channel_id = $1
                    ORDER BY cursor ASC
                    LIMIT $2
                    ",
                    &[&request.channel_id, &limit],
                )
                .await
                .map_err(EventStoreError::Execute)?
        } else {
            client
                .query(
                    "
                    SELECT id, channel_id, event_type, payload, actor,
                           agent_id, user_id, metadata, cursor, timestamp, depends_on
                    FROM agent_events
                    WHERE channel_id = $1
                      AND cursor > $2
                    ORDER BY cursor ASC
                    LIMIT $3
                    ",
                    &[&request.channel_id, &cursor_bound, &limit],
                )
                .await
                .map_err(EventStoreError::Execute)?
        };

        let events = rows.iter().map(row_to_event).collect();
        Ok(events)
    }

    /// Query events across channels with filters.
    #[instrument(skip(self, request), fields(channel_count = request.channel_ids.as_ref().map(|c| c.len())))]
    pub async fn query(&self, request: &QueryRequest) -> Result<Vec<AgentEvent>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let limit = request.limit.unwrap_or(100).min(1000);
        let offset = request.offset.unwrap_or(0);

        let mut conditions: Vec<String> = Vec::new();
        let mut params: Vec<Arg> = Vec::new();
        let mut param_idx = 1u32;

        if let Some(ref channel_ids) = request.channel_ids {
            if channel_ids.len() == 1 {
                conditions.push(format!("channel_id = ${param_idx}"));
                params.push(Arg::Text(channel_ids[0].clone()));
                param_idx += 1;
            } else if channel_ids.len() > 1 {
                let placeholders: Vec<String> = channel_ids
                    .iter()
                    .enumerate()
                    .map(|(i, _)| format!("${}", param_idx + i as u32))
                    .collect();
                conditions.push(format!("channel_id IN ({})", placeholders.join(",")));
                for id in channel_ids {
                    params.push(Arg::Text(id.clone()));
                }
                param_idx += channel_ids.len() as u32;
            }
        }

        if let Some(ref event_type) = request.event_type {
            conditions.push(format!("event_type = ${param_idx}"));
            params.push(Arg::Text(event_type.clone()));
            param_idx += 1;
        }

        if let Some(ref actor) = request.actor {
            conditions.push(format!("actor = ${param_idx}"));
            params.push(Arg::Text(actor.clone()));
            param_idx += 1;
        }

        if let Some(cursor_start) = request.cursor_start {
            conditions.push(format!("cursor >= ${param_idx}"));
            params.push(Arg::Int(cursor_start));
            param_idx += 1;
        }

        if let Some(cursor_end) = request.cursor_end {
            conditions.push(format!("cursor <= ${param_idx}"));
            params.push(Arg::Int(cursor_end));
            param_idx += 1;
        }

        // Deprecated aliases for cursor_start/cursor_end (older SDK clients).
        if let Some(channel_cursor_start) = request.channel_cursor_start {
            conditions.push(format!("cursor >= ${param_idx}"));
            params.push(Arg::Int(channel_cursor_start));
            param_idx += 1;
        }

        if let Some(channel_cursor_end) = request.channel_cursor_end {
            conditions.push(format!("cursor <= ${param_idx}"));
            params.push(Arg::Int(channel_cursor_end));
            param_idx += 1;
        }

        if let Some(from_str) = &request.from {
            let from_ts = DateTime::parse_from_rfc3339(from_str)
                .map_err(|_| EventStoreError::InvalidTimeRange(from_str.clone()))?;
            conditions.push(format!("timestamp >= ${param_idx}"));
            params.push(Arg::Timestamp(from_ts.with_timezone(&Utc)));
            param_idx += 1;
        }

        if let Some(to_str) = &request.to {
            let to_ts = DateTime::parse_from_rfc3339(to_str)
                .map_err(|_| EventStoreError::InvalidTimeRange(to_str.clone()))?;
            conditions.push(format!("timestamp <= ${param_idx}"));
            params.push(Arg::Timestamp(to_ts.with_timezone(&Utc)));
            param_idx += 1;
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let sql = format!(
            "
            SELECT id, channel_id, event_type, payload, actor,
                   agent_id, user_id, metadata, cursor, timestamp, depends_on
            FROM agent_events
            {}
            ORDER BY global_cursor DESC
            LIMIT ${} OFFSET ${}
            ",
            where_clause,
            param_idx,
            param_idx + 1
        );
        params.push(Arg::Int(limit));

        params.push(Arg::Int(offset));

        let param_refs: Vec<&(dyn ToSql + Sync)> =
            params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();

        let rows = client
            .query(&sql, &param_refs[..])
            .await
            .map_err(EventStoreError::Execute)?;

        let events: Vec<AgentEvent> = rows.iter().map(row_to_event).collect();
        Ok(events)
    }

    /// Keyset-paginated variant of [`EventStore::query`].
    ///
    /// Orders by the internal `global_cursor` (stable cross-channel order, no
    /// omission/duplication under concurrent inserts). When `after_global_cursor`
    /// is set, continues with `global_cursor < bound`. Fetches `limit + 1` rows
    /// to report `has_more` without a second query. Returns `(events, has_more,
    /// last_global_cursor)` where `last_global_cursor` is the continuation bound
    /// for the next page (None when the page is empty).
    #[instrument(skip(self, request), fields(channel_count = request.channel_ids.as_ref().map(|c| c.len())))]
    pub async fn query_page(
        &self,
        request: &QueryRequest,
        after_global_cursor: Option<i64>,
    ) -> Result<(Vec<AgentEvent>, bool, Option<i64>), EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let limit = request.limit.unwrap_or(100).clamp(1, 1000);
        let fetch = limit + 1;

        let mut conditions: Vec<String> = Vec::new();
        let mut params: Vec<Arg> = Vec::new();
        let mut param_idx = 1u32;

        if let Some(ref channel_ids) = request.channel_ids {
            if channel_ids.len() == 1 {
                conditions.push(format!("channel_id = ${param_idx}"));
                params.push(Arg::Text(channel_ids[0].clone()));
                param_idx += 1;
            } else if channel_ids.len() > 1 {
                let placeholders: Vec<String> = channel_ids
                    .iter()
                    .enumerate()
                    .map(|(i, _)| format!("${}", param_idx + i as u32))
                    .collect();
                conditions.push(format!("channel_id IN ({})", placeholders.join(",")));
                for id in channel_ids {
                    params.push(Arg::Text(id.clone()));
                }
                param_idx += channel_ids.len() as u32;
            }
        }

        if let Some(ref event_type) = request.event_type {
            conditions.push(format!("event_type = ${param_idx}"));
            params.push(Arg::Text(event_type.clone()));
            param_idx += 1;
        }

        if let Some(ref actor) = request.actor {
            conditions.push(format!("actor = ${param_idx}"));
            params.push(Arg::Text(actor.clone()));
            param_idx += 1;
        }

        if let Some(cursor_start) = request.cursor_start.or(request.channel_cursor_start) {
            conditions.push(format!("cursor >= ${param_idx}"));
            params.push(Arg::Int(cursor_start));
            param_idx += 1;
        }

        if let Some(cursor_end) = request.cursor_end.or(request.channel_cursor_end) {
            conditions.push(format!("cursor <= ${param_idx}"));
            params.push(Arg::Int(cursor_end));
            param_idx += 1;
        }

        if let Some(from_str) = &request.from {
            let from_ts = DateTime::parse_from_rfc3339(from_str)
                .map_err(|_| EventStoreError::InvalidTimeRange(from_str.clone()))?;
            conditions.push(format!("timestamp >= ${param_idx}"));
            params.push(Arg::Timestamp(from_ts.with_timezone(&Utc)));
            param_idx += 1;
        }

        if let Some(to_str) = &request.to {
            let to_ts = DateTime::parse_from_rfc3339(to_str)
                .map_err(|_| EventStoreError::InvalidTimeRange(to_str.clone()))?;
            conditions.push(format!("timestamp <= ${param_idx}"));
            params.push(Arg::Timestamp(to_ts.with_timezone(&Utc)));
            param_idx += 1;
        }

        // Keyset bound: strict `<` on the internal global_cursor so a page
        // never re-reads or skips rows under concurrent inserts.
        if let Some(bound) = after_global_cursor {
            conditions.push(format!("global_cursor < ${param_idx}"));
            params.push(Arg::Int(bound));
            param_idx += 1;
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let sql = format!(
            "
            SELECT id, channel_id, event_type, payload, actor,
                   agent_id, user_id, metadata, cursor, timestamp, depends_on, global_cursor
            FROM agent_events
            {where_clause}
            ORDER BY global_cursor DESC
            LIMIT ${param_idx}
            "
        );
        params.push(Arg::Int(fetch));

        let param_refs: Vec<&(dyn ToSql + Sync)> =
            params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();

        let rows = client
            .query(&sql, &param_refs[..])
            .await
            .map_err(EventStoreError::Execute)?;

        let has_more = rows.len() > limit as usize;
        let page_rows = &rows[..rows.len().min(limit as usize)];
        let mut events: Vec<AgentEvent> = Vec::with_capacity(page_rows.len());
        let mut last_global_cursor: Option<i64> = None;
        for row in page_rows {
            events.push(row_to_event(row));
            last_global_cursor = row
                .try_get::<_, Option<i64>>("global_cursor")
                .ok()
                .flatten();
        }
        Ok((events, has_more, last_global_cursor))
    }

    /// Smallest retained `global_cursor` in the event store, used to answer a
    /// typed `410 page_token_unsatisfiable` when retention has purged the page
    /// a token continues from. `None` when the store is empty.
    pub async fn min_global_cursor(&self) -> Result<Option<i64>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let row = client
            .query_opt("SELECT MIN(global_cursor) AS min_gc FROM agent_events", &[])
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(row.and_then(|r| r.get::<_, Option<i64>>("min_gc")))
    }

    /// Get the current highest cursor for a channel.
    /// Useful for determining where to resume a subscription.
    #[instrument(skip(self), fields(channel = %channel_id))]
    pub async fn get_latest_cursor(
        &self,
        channel_id: &str,
    ) -> Result<Option<i64>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;

        let row = client
            .query_opt(
                "
                SELECT MAX(cursor) as max_cursor
                FROM agent_events
                WHERE channel_id = $1
                ",
                &[&channel_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        Ok(row.and_then(|r| r.get::<_, Option<i64>>("max_cursor")))
    }

    /// Get the latest per-channel cursor from the channel_cursors counter table.
    /// This is the gapless sequential cursor maintained by publish_with_channel_cursor().
    #[instrument(skip(self), fields(channel = %channel_id))]
    pub async fn get_latest_channel_cursor(
        &self,
        channel_id: &str,
    ) -> Result<Option<i64>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;

        let row = client
            .query_opt(
                "
                SELECT last_cursor
                FROM channel_cursors
                WHERE channel_id = $1
                ",
                &[&channel_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        Ok(row.map(|r| r.get::<_, i64>("last_cursor")))
    }

    /// Get the total event count for a channel.
    pub async fn get_event_count(&self, channel_id: &str) -> Result<i64, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;

        let row = client
            .query_one(
                "
                SELECT COUNT(*) as count
                FROM agent_events
                WHERE channel_id = $1
                ",
                &[&channel_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        Ok(row.get("count"))
    }

    /// List channels ordered by most recent activity (latest event timestamp
    /// first, then channel_id) so dashboards can auto-select the latest one.
    pub async fn list_channels(&self) -> Result<Vec<String>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;

        // Read from the first-class channels registry (audit item 14),
        // backfilled by migration v22 so every existing channel is present.
        let rows = client
            .query(
                "SELECT channel_id
                 FROM channels c
                 LEFT JOIN LATERAL (
                     SELECT MAX(timestamp) AS last_ts
                     FROM agent_events
                     WHERE channel_id = c.channel_id
                 ) e ON true
                 ORDER BY e.last_ts DESC NULLS LAST, c.channel_id ASC",
                &[],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        Ok(rows.into_iter().map(|r| r.get("channel_id")).collect())
    }

    /// List channels ordered by most recent activity, enriched with event
    /// counts, latest cursor and last event timestamp. The channel ordering
    /// matches [`Self::list_channels`] — this is additive, not a replacement.
    pub async fn list_channels_with_stats(&self) -> Result<Vec<ChannelStat>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;

        let rows = client
            .query(
                "SELECT c.channel_id,
                        COUNT(e.cursor)::bigint AS event_count,
                        MAX(e.cursor) AS latest_cursor,
                        MAX(e.timestamp) AS last_event_at,
                        cm.display_name,
                        cm.parent_channel_id
                 FROM channels c
                 LEFT JOIN agent_events e ON e.channel_id = c.channel_id
                 LEFT JOIN channel_metadata cm ON cm.channel_id = c.channel_id
                 GROUP BY c.channel_id, cm.display_name, cm.parent_channel_id
                 ORDER BY last_event_at DESC NULLS LAST, c.channel_id ASC",
                &[],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        Ok(rows
            .into_iter()
            .map(|r| ChannelStat {
                channel_id: r.get("channel_id"),
                event_count: r.get("event_count"),
                latest_cursor: r.get("latest_cursor"),
                last_event_at: r.get("last_event_at"),
                display_name: r.get("display_name"),
                parent_channel_id: r.get("parent_channel_id"),
            })
            .collect())
    }

    /// Verify the tamper-evident hash chain for a channel (audit selling
    /// point 3): every event with a recorded `hash` must equal
    /// `sha256(channel|cursor|type|canonical_payload|prev_hash)` and its
    /// `prev_hash` must match the previous event's hash. Pre-ledger events
    /// (NULL hash) are counted as unverified and do not fail the chain.
    pub async fn verify_chain(
        &self,
        channel_id: &str,
    ) -> Result<ChainVerification, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;

        let rows = client
            .query(
                "SELECT cursor, event_type, payload, hash, prev_hash
                 FROM agent_events
                 WHERE channel_id = $1
                 ORDER BY cursor ASC",
                &[&channel_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        let mut verified: u64 = 0;
        let mut unverified: u64 = 0;
        let mut prev_recorded_hash: Option<String> = None;

        for row in rows {
            let cursor: i64 = row.get("cursor");
            let event_type: String = row.get("event_type");
            let payload: JsonValue = row.get("payload");
            let hash: Option<String> = row.get("hash");
            let prev_hash: Option<String> = row.get("prev_hash");

            let Some(hash) = hash else {
                // Pre-ledger event — cannot be verified retroactively.
                unverified += 1;
                prev_recorded_hash = None;
                continue;
            };

            // Link check: prev_hash must equal the previous event's hash.
            if prev_hash != prev_recorded_hash {
                return Ok(ChainVerification {
                    valid: false,
                    verified_events: verified,
                    unverified_events: unverified,
                    broken_at_cursor: Some(cursor),
                    first_bad_hash: Some(hash),
                });
            }

            // Content check: recompute the hash from the stored fields.
            let canonical_payload = sonic_rs::to_string(&payload.0).unwrap_or_default();
            let expected = event_hash(
                channel_id,
                cursor,
                &event_type,
                &canonical_payload,
                prev_hash.as_deref(),
            );
            if expected != hash {
                return Ok(ChainVerification {
                    valid: false,
                    verified_events: verified,
                    unverified_events: unverified,
                    broken_at_cursor: Some(cursor),
                    first_bad_hash: Some(hash),
                });
            }

            verified += 1;
            prev_recorded_hash = Some(hash);
        }

        Ok(ChainVerification {
            valid: true,
            verified_events: verified,
            unverified_events: unverified,
            broken_at_cursor: None,
            first_bad_hash: None,
        })
    }

    /// Stamp `closed_at` on a channel's registry row (audit item 14).
    /// Returns an error when the channel is not registered.
    pub async fn close_channel(&self, channel_id: &str) -> Result<(), EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let rows = client
            .execute(
                "UPDATE channels SET closed_at = NOW(), updated_at = NOW()
                 WHERE channel_id = $1 AND closed_at IS NULL",
                &[&channel_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        if rows == 0 {
            // Either unknown or already closed; an unknown channel is the
            // interesting case.
            let exists: bool = client
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM channels WHERE channel_id = $1)",
                    &[&channel_id],
                )
                .await
                .map_err(EventStoreError::Execute)?
                .get(0);
            if !exists {
                return Err(EventStoreError::NotFound);
            }
        }
        Ok(())
    }

    /// Global counters across the whole database: total persisted events,
    /// channel count and state snapshot count.
    pub async fn event_stats(&self) -> Result<EventStats, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;

        let row = client
            .query_one(
                "SELECT (SELECT COUNT(*)::bigint FROM agent_events)    AS total_events,
                        (SELECT COUNT(*)::bigint FROM channel_cursors) AS channel_count,
                        (SELECT COUNT(*)::bigint FROM channel_states)  AS state_snapshots",
                &[],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        Ok(EventStats {
            total_events: row.get("total_events"),
            channel_count: row.get("channel_count"),
            state_snapshots: row.get("state_snapshots"),
        })
    }

    /// Purge events older than the given timestamp for a channel.
    /// Returns the number of events deleted.
    pub async fn purge_before(
        &self,
        channel_id: &str,
        before: DateTime<Utc>,
    ) -> Result<u64, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;

        let result = client
            .execute(
                "
                DELETE FROM agent_events
                WHERE channel_id = $1
                  AND timestamp < $2
                ",
                &[&channel_id, &before],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        Ok(result)
    }

    /// Purge events older than the given timestamp across all channels.
    /// Returns the total number of events deleted.
    ///
    /// Provenance-aware (audit item 13): events referenced by another
    /// event's `depends_on`, referenced by `fork_operations.resolved_event_id`,
    /// or serving as a fork boundary (the event at `fork_operations.at_cursor`
    /// on its channel) are NEVER purged — otherwise `depends_on` edges, fork
    /// receipts, and decision trails would silently lose their evidence.
    /// Purged rows are copied into `event_archive` inside the same
    /// transaction, so provenance can be audited/recovered after the prune.
    pub async fn purge_events_older_than(
        &self,
        before: DateTime<Utc>,
    ) -> Result<u64, EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let tx = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;

        let archived = tx
            .execute(
                "
                INSERT INTO event_archive
                    (id, global_cursor, channel_id, event_type, payload, actor,
                     agent_id, user_id, metadata, depends_on, operation_id,
                     cursor, timestamp)
                SELECT e.id, e.global_cursor, e.channel_id, e.event_type,
                       e.payload, e.actor, e.agent_id, e.user_id, e.metadata,
                       e.depends_on, e.operation_id, e.cursor, e.timestamp
                FROM agent_events e
                WHERE e.timestamp < $1
                  AND e.id NOT IN (
                      -- Events that must survive because they are provenance
                      -- anchors (audit item 13):
                      -- 1. referenced as a parent by a child's depends_on
                      SELECT depends_on FROM agent_events WHERE depends_on IS NOT NULL
                      UNION
                      SELECT depends_on_event_id FROM event_dependencies
                      UNION
                      -- 2. referenced by a fork operation's resolved boundary
                      SELECT resolved_event_id FROM fork_operations WHERE resolved_event_id IS NOT NULL
                      UNION
                      -- 3. the event that a fork boundary points at (by cursor)
                      SELECT e2.id FROM fork_operations fo
                          JOIN agent_events e2 ON e2.channel_id = fo.source_channel_id AND e2.cursor = fo.at_cursor
                  )
                ",
                &[&before],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        let deleted = tx
            .execute(
                "
                DELETE FROM agent_events e
                WHERE e.timestamp < $1
                  AND e.id NOT IN (
                      SELECT depends_on FROM agent_events WHERE depends_on IS NOT NULL
                      UNION
                      SELECT depends_on_event_id FROM event_dependencies
                      UNION
                      SELECT resolved_event_id FROM fork_operations WHERE resolved_event_id IS NOT NULL
                      UNION
                      SELECT e2.id FROM fork_operations fo
                          JOIN agent_events e2 ON e2.channel_id = fo.source_channel_id AND e2.cursor = fo.at_cursor
                  )
                ",
                &[&before],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        tx.commit().await.map_err(EventStoreError::Execute)?;

        if archived > 0 {
            info!(
                "event retention: archived {} and purged {} events older than {}",
                archived, deleted, before
            );
        }
        Ok(deleted)
    }

    /// Permanently erase archived payloads older than the contracted archive
    /// window. This is deliberately separate from hot-table retention.
    pub async fn purge_archive_older_than(
        &self,
        before: DateTime<Utc>,
    ) -> Result<u64, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let deleted = client
            .execute("DELETE FROM event_archive WHERE timestamp < $1", &[&before])
            .await
            .map_err(EventStoreError::Execute)?;
        if deleted > 0 {
            info!(
                "event retention: permanently erased {} archived events older than {}",
                deleted, before
            );
        }
        Ok(deleted)
    }

    /// Age-based purge of versioned state snapshots, subject to provenance
    /// requirements. The latest state per channel is always preserved (it is
    /// the restorable boundary), as are snapshots referenced by a live fork
    /// boundary or required for lineage (fork receipts point at resolved
    /// boundaries by cursor). Older snapshots are removed so retained storage
    /// tracks the logical payload bytes that are actually inspectable.
    ///
    /// The retained gauge is decremented in the same transaction via the
    /// DELETE trigger on `channel_states`.
    pub async fn purge_states_older_than(
        &self,
        before: DateTime<Utc>,
    ) -> Result<u64, EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let tx = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;

        let deleted = tx
            .execute(
                "
                DELETE FROM channel_states s
                WHERE s.timestamp < $1
                  AND NOT EXISTS (
                      -- The latest snapshot per channel is the restorable
                      -- boundary; keep it regardless of age.
                      SELECT 1 FROM channel_states s2
                      WHERE s2.channel_id = s.channel_id
                        AND s2.version > s.version
                  )
                  AND NOT EXISTS (
                      -- Snapshots referenced by a live fork boundary (the
                      -- child's inherited state) must survive.
                      SELECT 1 FROM fork_operations fo
                      WHERE fo.child_channel_id = s.channel_id
                        AND fo.at_cursor = s.cursor
                  )
                ",
                &[&before],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        if deleted > 0 {
            info!(
                "state retention: purged {} state snapshots older than {} (provenance anchors preserved)",
                deleted, before
            );
        }
        tx.commit().await.map_err(EventStoreError::Execute)?;
        Ok(deleted)
    }

    /// Tombstone completed/failed tool executions older than the retention
    /// window: the large payload bodies (params/result/error) are nulled and
    /// the row keeps only its identity, key, tool, hash, status, and
    /// timestamps. A later claim on the same key then fails loudly with
    /// `result_expired` instead of silently executing again — the idempotency
    /// window is explicit, not accidental. The retained gauge is decremented
    /// by the freed bytes in the same transaction via the UPDATE trigger on
    /// `tool_executions`.
    pub async fn tombstone_expired_executions(
        &self,
        before: DateTime<Utc>,
    ) -> Result<u64, EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let tx = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;

        let tombstoned = tx
            .execute(
                "UPDATE tool_executions
                 SET params = '{}'::jsonb,
                     result = NULL,
                     error = NULL,
                     expired_at = COALESCE(expired_at, NOW()),
                     updated_at = NOW()
                 WHERE status IN ('completed', 'failed')
                   AND completed_at IS NOT NULL
                   AND completed_at < $1
                   AND expired_at IS NULL",
                &[&before],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        if tombstoned > 0 {
            info!(
                "tool-execution retention: tombstoned {} completed/failed executions completed before {} (result replay window expired)",
                tombstoned, before
            );
        }
        tx.commit().await.map_err(EventStoreError::Execute)?;
        Ok(tombstoned)
    }

    /// Emit the current retained-logical-payload gauge into the usage outbox.
    /// Unlike the daily-summed flow meters, retained bytes is a point-in-time
    /// value; the control plane records it as the current value (overwrite,
    /// not accumulate). A per-day idempotency key means the latest snapshot of
    /// the day wins and a single value is delivered per day.
    pub async fn snapshot_retained_bytes(&self) -> Result<u64, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let row = client
            .query_opt(
                "SELECT retained_bytes FROM usage_storage_state WHERE singleton = TRUE",
                &[],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        let Some(row) = row else {
            return Ok(0);
        };
        let retained: i64 = row.get("retained_bytes");
        let usage_date = Utc::now().date_naive();
        client
            .execute(
                "INSERT INTO usage_delivery_outbox
                 (idempotency_key, usage_date, metric, quantity, source_generation)
                 VALUES ($1, $2::date, 'retained_bytes', $3, 1)
                 ON CONFLICT (idempotency_key) DO UPDATE
                 SET quantity = EXCLUDED.quantity, updated_at = NOW()",
                &[
                    &format!("retained-snapshot:{usage_date}"),
                    &usage_date,
                    &retained,
                ],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(retained as u64)
    }

    /// Read, but do not remove, the oldest usage delivery batch. Removal is
    /// a separate acknowledgement after the control plane returns success.
    pub async fn usage_outbox_batch(
        &self,
        limit: i64,
    ) -> Result<Vec<UsageOutboxItem>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let rows = client
            .query(
                "SELECT idempotency_key, usage_date::text AS usage_date, metric,
                        quantity, source_generation
                 FROM usage_delivery_outbox ORDER BY created_at, idempotency_key
                 LIMIT $1",
                &[&limit.clamp(1, 1000)],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(rows
            .into_iter()
            .map(|row| UsageOutboxItem {
                idempotency_key: row.get("idempotency_key"),
                usage_date: row.get("usage_date"),
                metric: row.get("metric"),
                quantity: row.get("quantity"),
                source_generation: row.get("source_generation"),
            })
            .collect())
    }

    pub async fn acknowledge_usage_outbox(&self, keys: &[String]) -> Result<u64, EventStoreError> {
        if keys.is_empty() {
            return Ok(0);
        }
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        client
            .execute(
                "DELETE FROM usage_delivery_outbox WHERE idempotency_key = ANY($1)",
                &[&keys],
            )
            .await
            .map_err(EventStoreError::Execute)
    }

    /// Purge ALL events.
    /// Use with extreme caution.
    pub async fn purge_all(&self) -> Result<u64, EventStoreError> {
        warn!("PURGE ALL — deleting ALL agent_events rows");
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;

        let result = client
            .execute("DELETE FROM agent_events", &[])
            .await
            .map_err(EventStoreError::Execute)?;

        Ok(result)
    }
}

fn parse_non_negative_env(name: &str) -> Result<i64, EventStoreError> {
    let value = std::env::var(name)
        .unwrap_or_else(|_| "0".to_string())
        .parse::<i64>()
        .map_err(|_| {
            EventStoreError::InvalidRequest(format!("{name} must be a non-negative integer"))
        })?;
    if value < 0 {
        return Err(EventStoreError::InvalidRequest(format!(
            "{name} must be a non-negative integer"
        )));
    }
    Ok(value)
}

/// Map a tokio-postgres error, translating the storage-quota exception raised
/// by the DB triggers into `QuotaExceeded`. Used on write paths whose quota is
/// enforced by a trigger (state snapshots and tool executions) rather than the
/// event trigger.
fn map_quota_exec_error(error: tokio_postgres::Error) -> EventStoreError {
    if error
        .as_db_error()
        .map(|db| db.message().starts_with("ACTAE_QUOTA_EXCEEDED:"))
        .unwrap_or(false)
    {
        EventStoreError::QuotaExceeded
    } else {
        EventStoreError::Execute(error)
    }
}

#[cfg(test)]
mod quota_tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn quota_env_defaults_unlimited_and_rejects_negative_values() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("ACTAE_TEST_QUOTA");
        assert_eq!(parse_non_negative_env("ACTAE_TEST_QUOTA").unwrap(), 0);
        std::env::set_var("ACTAE_TEST_QUOTA", "42");
        assert_eq!(parse_non_negative_env("ACTAE_TEST_QUOTA").unwrap(), 42);
        std::env::set_var("ACTAE_TEST_QUOTA", "-1");
        assert!(parse_non_negative_env("ACTAE_TEST_QUOTA").is_err());
        std::env::remove_var("ACTAE_TEST_QUOTA");
    }
}

async fn find_transition_replay(
    tx: &tokio_postgres::Transaction<'_>,
    channel_id: &str,
    operation_id: Uuid,
) -> Result<Option<(AgentEvent, i64)>, EventStoreError> {
    let row = tx
        .query_opt(
            "SELECT id, channel_id, event_type, payload, actor, agent_id, user_id,
                    metadata, cursor, depends_on, timestamp
             FROM agent_events
             WHERE channel_id = $1 AND operation_id = $2",
            &[&channel_id, &operation_id],
        )
        .await
        .map_err(EventStoreError::Execute)?;
    let Some(row) = row else { return Ok(None) };
    let event = row_to_event(&row);
    // State version as of the replayed event's cursor — 0 when the event
    // was a plain publish with no snapshot (idempotent retries must not
    // fail just because no state exists).
    let version = tx
        .query_opt(
            "SELECT COALESCE(MAX(version), 0) AS version FROM channel_states
             WHERE channel_id = $1 AND cursor = $2",
            &[&channel_id, &event.cursor],
        )
        .await
        .map_err(EventStoreError::Execute)?
        // COALESCE guarantees a row; the fallback is defensive only.
        .map(|row| row.get::<_, i64>("version"))
        .unwrap_or(0);
    Ok(Some((event, version)))
}

/// Canonical, key-order-insensitive JSON value equality. jsonb storage does
/// not preserve key order, so direct sonic Value equality would treat two
/// semantically identical documents as different across a round trip.
fn json_values_equal(a: &sonic_rs::Value, b: &sonic_rs::Value) -> bool {
    crate::database::json_compat::sonic_to_serde(a)
        == crate::database::json_compat::sonic_to_serde(b)
}

/// Whether a replay request carries the same event content as the stored
/// event (type + payload + metadata). A retry that re-serializes its
/// payload with a different key order still replays; a genuinely different
/// request fails loudly (409 idempotency_conflict).
fn event_content_matches(
    request_event_type: &str,
    request_payload: &sonic_rs::Value,
    request_metadata: &EventMetadata,
    stored: &AgentEvent,
) -> bool {
    if request_event_type != stored.event_type {
        return false;
    }
    if !json_values_equal(request_payload, &stored.payload) {
        return false;
    }
    match (
        serde_json::to_value(request_metadata),
        serde_json::to_value(&stored.metadata),
    ) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// The channel's latest state version — 0 when no snapshot exists yet.
/// Runs inside the caller's transaction so it sees uncommitted state.
async fn latest_state_version(
    tx: &tokio_postgres::Transaction<'_>,
    channel_id: &str,
) -> Result<i64, EventStoreError> {
    Ok(tx
        .query_opt(
            "SELECT COALESCE(MAX(version), 0) AS version FROM channel_states WHERE channel_id = $1",
            &[&channel_id],
        )
        .await
        .map_err(EventStoreError::Execute)?
        .map(|row| row.get::<_, i64>("version"))
        .unwrap_or(0))
}

/// Verify optimistic-concurrency guards against the channel's latest state.
///
/// Serialization: a channel-scoped transaction advisory lock is taken first,
/// then the latest snapshot row is locked with `FOR UPDATE`. The advisory
/// lock is the load-bearing part — `FOR UPDATE` on a `SELECT` that returns
/// no row (the very common "first snapshot, expected_version = 0" case)
/// locks nothing, so concurrent first-writes would otherwise all pass the
/// guard. `pg_advisory_xact_lock` is transaction-scoped and auto-released on
/// commit/rollback.
/// Returns Err(VersionConflict) when any guard mismatches.
async fn check_state_guards(
    tx: &tokio_postgres::Transaction<'_>,
    channel_id: &str,
    expected_version: Option<i64>,
    expected_cursor: Option<i64>,
) -> Result<(), EventStoreError> {
    if expected_version.is_none() && expected_cursor.is_none() {
        return Ok(());
    }

    tx.execute("SELECT pg_advisory_xact_lock(hashtext($1))", &[&channel_id])
        .await
        .map_err(EventStoreError::Execute)?;

    let row = tx
        .query_opt(
            "SELECT version, cursor FROM channel_states
             WHERE channel_id = $1
             ORDER BY version DESC LIMIT 1
             FOR UPDATE",
            &[&channel_id],
        )
        .await
        .map_err(EventStoreError::Execute)?;

    let (actual_version, actual_cursor) = match row {
        Some(r) => {
            let v: i64 = r.get("version");
            let c: i64 = r.get("cursor");
            (Some(v), Some(c))
        }
        None => (None, None),
    };

    let version_ok = match expected_version {
        Some(0) => actual_version.is_none(),
        Some(exp) => actual_version == Some(exp),
        None => true,
    };
    let cursor_ok = match expected_cursor {
        Some(0) => actual_cursor.is_none() || actual_cursor == Some(0),
        Some(exp) => actual_cursor == Some(exp),
        None => true,
    };

    if !version_ok || !cursor_ok {
        return Err(EventStoreError::VersionConflict {
            channel_id: channel_id.to_string(),
            expected_version,
            actual_version,
            expected_cursor,
            actual_cursor,
        });
    }
    Ok(())
}

/// Shared per-channel event insert used inside transactions.
/// Allocates the next gapless per-channel cursor (creating the
/// channel_cursors row on first write) and inserts the event row.
/// Returns (event_id, cursor).
///
/// Idempotency: when `operation_id` is Some, an existing row with the same
/// (channel_id, operation_id) is returned without allocating a new cursor
/// or inserting — retries after lost responses replay the original event.
///
/// Concurrency: the partial unique index makes concurrent duplicate
/// inserts race-safe. The loser rolls back its cursor bump inside a
/// savepoint before returning the winner's row, so the per-channel gapless
/// sequence never gains a phantom hole (each last_cursor value is assigned
/// to exactly one event).
#[allow(clippy::too_many_arguments)]
async fn insert_event_in_tx(
    tx: &mut tokio_postgres::Transaction<'_>,
    channel_id: &str,
    event_type: &str,
    payload: &sonic_rs::Value,
    metadata: &EventMetadata,
    depends_on: Option<Uuid>,
    operation_id: Option<Uuid>,
    timestamp: DateTime<Utc>,
) -> Result<(Uuid, i64), EventStoreError> {
    if let Some(op) = operation_id {
        if let Some(row) = find_by_operation_id(tx, channel_id, op).await? {
            return Ok(row);
        }

        // Bump + insert inside a savepoint so a lost race can undo the
        // bump (rollback to savepoint) and hand back the winner's event.
        let sp = tx
            .savepoint("idempotent_record")
            .await
            .map_err(EventStoreError::Execute)?;
        match insert_event_row(
            &sp,
            channel_id,
            event_type,
            payload,
            metadata,
            depends_on,
            Some(op),
            timestamp,
        )
        .await?
        {
            Some(inserted) => {
                sp.commit().await.map_err(EventStoreError::Execute)?;
                Ok(inserted)
            }
            None => {
                // A concurrent request with the same operation_id won.
                // Roll back the savepoint (undoing the cursor bump) so the
                // channel's gapless sequence is preserved.
                sp.rollback().await.map_err(EventStoreError::Execute)?;
                find_by_operation_id(tx, channel_id, op)
                    .await?
                    .ok_or(EventStoreError::NotFound)
            }
        }
    } else {
        match insert_event_row(
            tx, channel_id, event_type, payload, metadata, depends_on, None, timestamp,
        )
        .await?
        {
            Some(v) => Ok(v),
            // No operation_id → no conflict target → the insert always lands.
            None => unreachable!("plain insert cannot conflict"),
        }
    }
}

async fn insert_dependency_edges(
    tx: &tokio_postgres::Transaction<'_>,
    event_id: Uuid,
    legacy_parent: Option<Uuid>,
    dependencies: &[super::schema::EventDependencyRequest],
) -> Result<(), EventStoreError> {
    let edges = normalized_dependency_edges(legacy_parent, dependencies);
    for (parent, dependency_type) in edges {
        if parent == event_id {
            return Err(EventStoreError::InvalidRequest(
                "an event cannot depend on itself".into(),
            ));
        }
        if dependency_type.is_empty()
            || dependency_type.len() > 64
            || !dependency_type
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        {
            return Err(EventStoreError::InvalidRequest(
                "dependency_type must be 1..=64 characters using [A-Za-z0-9._-]".into(),
            ));
        }
        let inserted = tx.execute(
            "INSERT INTO event_dependencies (event_id, depends_on_event_id, dependency_type)
             SELECT $1, $2, $3 WHERE EXISTS (SELECT 1 FROM agent_events WHERE id=$2)
             ON CONFLICT (event_id, depends_on_event_id) DO UPDATE SET dependency_type=EXCLUDED.dependency_type",
            &[&event_id, &parent, &dependency_type],
        ).await.map_err(EventStoreError::Execute)?;
        if inserted == 0 {
            return Err(EventStoreError::InvalidRequest(format!(
                "dependency event not found: {parent}"
            )));
        }
    }
    Ok(())
}

fn normalized_dependency_edges(
    legacy_parent: Option<Uuid>,
    dependencies: &[super::schema::EventDependencyRequest],
) -> Vec<(Uuid, String)> {
    let mut edges: Vec<(Uuid, String)> = dependencies
        .iter()
        .map(|dependency| (dependency.event_id, dependency.dependency_type.clone()))
        .collect();
    if let Some(parent) = legacy_parent {
        if !edges.iter().any(|(event, _)| *event == parent) {
            edges.push((parent, "causal".to_string()));
        }
    }
    edges.sort_by_key(|(event, _)| *event);
    edges.dedup_by_key(|(event, _)| *event);
    edges
}

async fn dependency_edges_match(
    tx: &tokio_postgres::Transaction<'_>,
    event_id: Uuid,
    legacy_parent: Option<Uuid>,
    dependencies: &[super::schema::EventDependencyRequest],
) -> Result<bool, EventStoreError> {
    let expected = normalized_dependency_edges(legacy_parent, dependencies);
    let rows = tx.query("SELECT depends_on_event_id, dependency_type FROM event_dependencies WHERE event_id=$1 ORDER BY depends_on_event_id", &[&event_id]).await.map_err(EventStoreError::Execute)?;
    let actual: Vec<(Uuid, String)> = rows
        .into_iter()
        .map(|row| (row.get("depends_on_event_id"), row.get("dependency_type")))
        .collect();
    Ok(actual == expected)
}

/// Look up an event row by its (channel_id, operation_id) idempotency key.
async fn find_by_operation_id(
    tx: &tokio_postgres::Transaction<'_>,
    channel_id: &str,
    op: Uuid,
) -> Result<Option<(Uuid, i64)>, EventStoreError> {
    let row = tx
        .query_opt(
            "SELECT id, cursor FROM agent_events
             WHERE channel_id = $1 AND operation_id = $2",
            &[&channel_id, &op],
        )
        .await
        .map_err(EventStoreError::Execute)?;
    Ok(row.map(|r| (r.get("id"), r.get("cursor"))))
}

/// Bump the channel cursor and insert the event row. Returns
/// `Some((id, cursor))` on success and `None` when the insert lost an
/// idempotency race (ON CONFLICT DO NOTHING) — in which case the caller is
/// responsible for undoing the bump.
#[allow(clippy::too_many_arguments)]
async fn insert_event_row(
    tx: &tokio_postgres::Transaction<'_>,
    channel_id: &str,
    event_type: &str,
    payload: &sonic_rs::Value,
    metadata: &EventMetadata,
    depends_on: Option<Uuid>,
    operation_id: Option<Uuid>,
    timestamp: DateTime<Utc>,
) -> Result<Option<(Uuid, i64)>, EventStoreError> {
    let id = Uuid::new_v4();
    let payload_json = JsonValue::new(payload.clone());
    let metadata_json = metadata.metadata.clone().map(JsonValue::new);

    let cursor_row = tx
        .query_one(
            "
            INSERT INTO channel_cursors (channel_id, last_cursor)
            VALUES ($1, 1)
            ON CONFLICT (channel_id) DO UPDATE
            SET last_cursor = channel_cursors.last_cursor + 1
            RETURNING last_cursor
            ",
            &[&channel_id],
        )
        .await
        .map_err(EventStoreError::Execute)?;
    let cursor: i64 = cursor_row.get("last_cursor");

    // Tamper-evident chain: link this event to the previous event's hash on
    // the channel (audit selling point 3). Pre-ledger rows have NULL hashes,
    // so the chain starts fresh at the first post-migration event.
    let prev_hash: Option<String> = if cursor > 1 {
        tx.query_opt(
            "SELECT hash FROM agent_events WHERE channel_id = $1 AND cursor = $2",
            &[&channel_id, &(cursor - 1)],
        )
        .await
        .map_err(EventStoreError::Execute)?
        .and_then(|r| r.try_get::<_, Option<String>>("hash").ok().flatten())
    } else {
        None
    };
    let canonical_payload = sonic_rs::to_string(payload).unwrap_or_default();
    let hash = event_hash(
        channel_id,
        cursor,
        event_type,
        &canonical_payload,
        prev_hash.as_deref(),
    );

    let maybe_row = tx
        .query_opt(
            "
            INSERT INTO agent_events (
                id, channel_id, event_type, payload, actor,
                agent_id, user_id, metadata, cursor,
                timestamp, depends_on, operation_id, hash, prev_hash
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (channel_id, operation_id) WHERE operation_id IS NOT NULL DO NOTHING
            RETURNING id
            ",
            &[
                &id,
                &channel_id,
                &event_type,
                &payload_json,
                &metadata.actor,
                &metadata.agent_id.as_deref(),
                &metadata.user_id.as_deref(),
                &metadata_json,
                &cursor,
                &timestamp,
                &depends_on,
                &operation_id,
                &hash,
                &prev_hash,
            ],
        )
        .await
        .map_err(|error| {
            if error
                .as_db_error()
                .map(|db| db.message().starts_with("ACTAE_QUOTA_EXCEEDED:"))
                .unwrap_or(false)
            {
                EventStoreError::QuotaExceeded
            } else {
                EventStoreError::Execute(error)
            }
        })?;

    if maybe_row.is_some() {
        let metadata_bytes = metadata_json
            .as_ref()
            .map(|value| value.0.to_string().len())
            .unwrap_or(0);
        let ingress_bytes = canonical_payload
            .len()
            .saturating_add(metadata_bytes)
            .min(i64::MAX as usize) as i64;
        let usage_date = timestamp.date_naive();
        tx.execute(
            "INSERT INTO usage_delivery_outbox
             (idempotency_key, usage_date, metric, quantity, source_generation)
             VALUES ($1, $2::date, 'events_ingested', 1, 1),
                    ($3, $2::date, 'ingress_bytes', $4, 1),
                    ($5, $2::date, 'durable_written_bytes', $4, 1)
             ON CONFLICT (idempotency_key) DO NOTHING",
            &[
                &format!("event:{id}"),
                &usage_date,
                &format!("event-bytes:{id}"),
                &ingress_bytes,
                &format!("event-durable:{id}"),
            ],
        )
        .await
        .map_err(EventStoreError::Execute)?;

        // `run.recovered` is the explicit, opt-in recovery declaration for
        // clients that do not use AgentSession's session-metadata contract
        // (raw record() calls, framework adapters, curl). Metered in the same
        // transaction as the event; `maybe_row` is `Some` only on a fresh
        // insert, so an idempotent replay never double-counts.
        if event_type == crate::events::schema::RECOVERY_MARKER_EVENT {
            tx.execute(
                "INSERT INTO usage_delivery_outbox
                 (idempotency_key, usage_date, metric, quantity, source_generation)
                 VALUES ($1, $2::date, 'recovery_operations', 1, 1)
                 ON CONFLICT (idempotency_key) DO NOTHING",
                &[&format!("recovery-event:{id}"), &usage_date],
            )
            .await
            .map_err(EventStoreError::Execute)?;
            crate::metrics::basic_metrics::RECOVERIES_TOTAL.inc();
        }

        // First-class channel registry (audit item 14): ensure the channel
        // row exists and advance its last_cursor in the SAME transaction.
        // Owner is set only on first creation (never overwritten).
        tx.execute(
            "INSERT INTO channels (channel_id, owner, last_cursor, updated_at)
             VALUES ($1, $2, $3, NOW())
             ON CONFLICT (channel_id) DO UPDATE
             SET last_cursor = GREATEST(channels.last_cursor, EXCLUDED.last_cursor),
                 updated_at = NOW()",
            &[&channel_id, &metadata.actor, &cursor],
        )
        .await
        .map_err(EventStoreError::Execute)?;
    }

    Ok(maybe_row.map(|_| (id, cursor)))
}

/// Resolve a snapshot boundary for a fork.
/// - `at_cursor == 0`: latest saved state (if any).
/// - `at_cursor > 0`: snapshot whose `cursor <= at_cursor`, choosing the
///   **closest cursor** (not the newest version — late historical snapshot
///   inserts must not win over the snapshot nearest the fork point).
///
/// Returns `(version, cursor, state)` or `None` when no snapshot exists.
async fn resolve_fork_state(
    tx: &tokio_postgres::Transaction<'_>,
    channel_id: &str,
    at_cursor: i64,
) -> Result<Option<(i64, i64, JsonValue)>, EventStoreError> {
    let row = if at_cursor > 0 {
        tx.query_opt(
            "SELECT version, cursor, state FROM channel_states
             WHERE channel_id = $1 AND cursor <= $2
             ORDER BY cursor DESC, version DESC LIMIT 1",
            &[&channel_id, &at_cursor],
        )
        .await
        .map_err(EventStoreError::Execute)?
    } else {
        tx.query_opt(
            "SELECT version, cursor, state FROM channel_states
             WHERE channel_id = $1
             ORDER BY version DESC LIMIT 1",
            &[&channel_id],
        )
        .await
        .map_err(EventStoreError::Execute)?
    };
    Ok(row.map(|r| {
        let version: i64 = r.get("version");
        let cursor: i64 = r.get("cursor");
        let state: JsonValue = r.get("state");
        (version, cursor, state)
    }))
}

/// Canonical SHA-256 fingerprint of a state snapshot (the immutable
/// `source_state_sha256` recorded at fork time).
fn state_sha256(state: &sonic_rs::Value) -> String {
    use sha2::{Digest, Sha256};
    let canonical = sonic_rs::to_string(state).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let digest = hasher.finalize();
    format!("{digest:x}")
}

/// Whether a fork manifest carries the fields required to reproduce the
/// experiment context: model, environment fingerprint, dependency versions
/// and random seed. Absent/manifest-less forks cannot be graded
/// `context_exact`.
fn manifest_complete(manifest: Option<&sonic_rs::Value>) -> bool {
    match manifest.and_then(|m| m.as_object()) {
        Some(obj) => ["model", "environment", "dependencies", "seed"]
            .iter()
            .all(|k| obj.contains_key(k)),
        None => false,
    }
}

/// Canonical hash of the fork-defining fields, used for request deduplication.
/// Includes `new_channel_id` so the same operation_id with a different child
/// id is detected as a conflicting request, and the manifest so a replayed
/// fork with drift is never silently treated as a success.
fn fork_request_hash(request: &ForkRequest) -> String {
    use sha2::{Digest, Sha256};
    let mut canonical = String::new();
    canonical.push_str(&request.source_channel_id);
    canonical.push('\n');
    canonical.push_str(&request.new_channel_id);
    canonical.push('\n');
    canonical.push_str(&request.at_cursor.to_string());
    canonical.push('\n');
    canonical.push_str(request.display_name.as_deref().unwrap_or(""));
    canonical.push('\n');
    canonical.push_str(request.reason.as_deref().unwrap_or(""));
    canonical.push('\n');
    if let Some(exp) = &request.experiment_metadata {
        canonical.push_str(&sonic_rs::to_string(exp).unwrap_or_default());
    }
    canonical.push('\n');
    if let Some(manifest) = &request.manifest {
        canonical.push_str(&sonic_rs::to_string(manifest).unwrap_or_default());
    }
    // Appended only when present so the hash of a policy-free fork is
    // byte-identical to the pre-v29 algorithm (retries of an in-flight fork
    // still replay rather than reporting a spurious idempotency conflict).
    if let Some(policies) = &request.tool_policies {
        canonical.push('\n');
        canonical.push_str(&sonic_rs::to_string(policies).unwrap_or_default());
    }
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let digest = hasher.finalize();
    format!("{digest:x}")
}

/// Compare two JSON values structurally, emitting a flat list of diff
/// entries keyed by JSON-path.
///
/// - Objects: compare key-by-key; keys present on one side only are
///   `added`/`removed`; keys on both sides recurse.
/// - Arrays: compare element-by-element up to the shorter length; the tail
///   of the longer array is emitted as `added`/`removed` per index.
/// - Scalars (incl. null) and type-mismatched containers: `changed` when
///   the two values are not equal.
///
/// `out` accumulates up to `limit` returned entries; `total` counts every
/// differing entry regardless of the returned cap so callers can report
/// truncation honestly. Traversal stops once `total` hits the hard work cap
/// (`MAX_DIFF_ENTRIES_TOTAL`) or the nesting depth limit.
fn walk_diff(
    left: &sonic_rs::Value,
    right: &sonic_rs::Value,
    path: &mut Vec<String>,
    out: &mut Vec<StateDiffEntry>,
    total: &mut usize,
    limit: usize,
    depth: usize,
) {
    if *total >= EventStore::MAX_DIFF_ENTRIES_TOTAL || depth > EventStore::MAX_DIFF_DEPTH {
        return;
    }

    use sonic_rs::JsonContainerTrait as _;

    // Emit one diff entry, respecting the returned-entries cap while always
    // counting the total so callers can report truncation honestly.
    macro_rules! push {
        ($entry:expr) => {{
            *total += 1;
            if out.len() < limit {
                out.push($entry);
            }
        }};
    }

    match (left.as_object(), right.as_object()) {
        (Some(l), Some(r)) => {
            // Union of keys, walking shared keys recursively.
            let mut keys: Vec<&str> = l.iter().map(|(k, _)| k).collect();
            for (k, _) in r.iter() {
                if !keys.contains(&k) {
                    keys.push(k);
                }
            }
            keys.sort_unstable();
            for key in keys {
                if *total >= EventStore::MAX_DIFF_ENTRIES_TOTAL {
                    break;
                }
                match (l.get(&key), r.get(&key)) {
                    (Some(lv), Some(rv)) => {
                        path.push(key.to_string());
                        walk_diff(lv, rv, path, out, total, limit, depth + 1);
                        path.pop();
                    }
                    (Some(_), None) => {
                        let mut p = path.clone();
                        p.push(key.to_string());
                        push!(StateDiffEntry {
                            path: p,
                            kind: "removed".to_string(),
                            left: l.get(&key).cloned(),
                            right: None,
                            left_cursor: None,
                            right_cursor: None,
                        });
                    }
                    (None, Some(_)) => {
                        let mut p = path.clone();
                        p.push(key.to_string());
                        push!(StateDiffEntry {
                            path: p,
                            kind: "added".to_string(),
                            left: None,
                            right: r.get(&key).cloned(),
                            left_cursor: None,
                            right_cursor: None,
                        });
                    }
                    (None, None) => {}
                }
            }
        }
        _ => match (left.as_array(), right.as_array()) {
            (Some(l), Some(r)) => {
                let min = l.len().min(r.len());
                for i in 0..min {
                    if *total >= EventStore::MAX_DIFF_ENTRIES_TOTAL {
                        break;
                    }
                    path.push(i.to_string());
                    walk_diff(&l[i], &r[i], path, out, total, limit, depth + 1);
                    path.pop();
                }
                for i in min..l.len() {
                    if *total >= EventStore::MAX_DIFF_ENTRIES_TOTAL {
                        break;
                    }
                    let mut p = path.clone();
                    p.push(i.to_string());
                    push!(StateDiffEntry {
                        path: p,
                        kind: "removed".to_string(),
                        left: Some(l[i].clone()),
                        right: None,
                        left_cursor: None,
                        right_cursor: None,
                    });
                }
                for i in min..r.len() {
                    if *total >= EventStore::MAX_DIFF_ENTRIES_TOTAL {
                        break;
                    }
                    let mut p = path.clone();
                    p.push(i.to_string());
                    push!(StateDiffEntry {
                        path: p,
                        kind: "added".to_string(),
                        left: None,
                        right: Some(r[i].clone()),
                        left_cursor: None,
                        right_cursor: None,
                    });
                }
            }
            _ => {
                // Scalars, nulls, or mixed-type containers.
                if !values_equal(left, right) {
                    push!(StateDiffEntry {
                        path: path.clone(),
                        kind: "changed".to_string(),
                        left: Some(left.clone()),
                        right: Some(right.clone()),
                        left_cursor: None,
                        right_cursor: None,
                    });
                }
            }
        },
    }
}

/// Best-effort JSON value equality for diff purposes.
///
/// Treats the sonic-rs arbitrary-precision numbers by their raw string form
/// so `1` and `1.0` compare equal, and falls back to string comparison of
/// the serialized forms as a last resort.
fn values_equal(left: &sonic_rs::Value, right: &sonic_rs::Value) -> bool {
    use sonic_rs::JsonValueTrait as _;
    if let (true, true) = (left.is_i64(), right.is_i64()) {
        return left.as_i64() == right.as_i64();
    }
    if let (true, true) = (left.is_u64(), right.is_u64()) {
        return left.as_u64() == right.as_u64();
    }
    if let (true, true) = (left.is_f64(), right.is_f64()) {
        return left.as_f64() == right.as_f64();
    }
    if let (Some(a), Some(b)) = (left.as_str(), right.as_str()) {
        return a == b;
    }
    if let (Some(a), Some(b)) = (left.as_bool(), right.as_bool()) {
        return a == b;
    }
    if left.is_null() && right.is_null() {
        return true;
    }
    sonic_rs::to_string(left).ok() == sonic_rs::to_string(right).ok()
}

fn row_to_event(row: &Row) -> AgentEvent {
    let id: Uuid = row.get("id");
    let channel_id: String = row.get("channel_id");
    let event_type: String = row.get("event_type");
    let payload: crate::database::json_compat::JsonValue = row.get("payload");
    let actor: String = row.get("actor");
    let agent_id: Option<String> = row.get("agent_id");
    let user_id: Option<String> = row.get("user_id");
    let metadata: Option<crate::database::json_compat::JsonValue> = row.get("metadata");
    let cursor: i64 = row.get("cursor");
    let depends_on: Option<Uuid> = row.try_get("depends_on").ok().flatten();
    let timestamp: DateTime<Utc> = row.get("timestamp");

    AgentEvent {
        id,
        channel_id,
        event_type,
        payload: payload.0,
        metadata: EventMetadata {
            actor,
            agent_id,
            user_id,
            metadata: metadata.map(|v| v.0),
        },
        cursor,
        depends_on,
        timestamp,
    }
}

async fn event_by_id(
    tx: &tokio_postgres::Transaction<'_>,
    id: Uuid,
) -> Result<AgentEvent, EventStoreError> {
    let row = tx.query_opt(
        "SELECT id, channel_id, event_type, payload, actor, agent_id, user_id, metadata, cursor, timestamp, depends_on FROM agent_events WHERE id = $1",
        &[&id],
    ).await.map_err(EventStoreError::Execute)?;
    row.as_ref()
        .map(row_to_event)
        .ok_or(EventStoreError::NotFound)
}

fn row_to_execution_group(row: &Row) -> ExecutionGroup {
    let metadata: JsonValue = row.get("metadata");
    ExecutionGroup {
        group_id: row.get("group_id"),
        status: row.get("status"),
        metadata: Some(metadata.0),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn row_to_execution_group_member(row: &Row) -> ExecutionGroupMember {
    let metadata: JsonValue = row.get("metadata");
    ExecutionGroupMember {
        group_id: row.get("group_id"),
        member_id: row.get("member_id"),
        channel_id: row.get("channel_id"),
        role: row.get("role"),
        metadata: Some(metadata.0),
        owner_id: row.get("owner_id"),
        generation: row.get("generation"),
        lease_until: row.get("lease_until"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn row_to_group_message(row: &Row) -> GroupMessage {
    let payload: JsonValue = row.get("payload");
    let causal_context: Option<JsonValue> = row.get("causal_context");
    GroupMessage {
        message_id: row.get("message_id"),
        group_id: row.get("group_id"),
        from_member_id: row.get("from_member_id"),
        to_member_id: row.get("to_member_id"),
        message_type: row.get("message_type"),
        payload: payload.0,
        causal_context: causal_context.map(|v| v.0),
        source_event_id: row.get("source_event_id"),
        delivery_event_id: row.get("delivery_event_id"),
        status: row.get("status"),
        acknowledged_at: row.get("acknowledged_at"),
        acknowledged_by: row.get("acknowledged_by"),
        created_at: row.get("created_at"),
    }
}

fn row_to_group_fork_receipt(row: &Row) -> GroupForkReceipt {
    let members: JsonValue = row.get("member_policies");
    let tools: JsonValue = row.get("tool_policies");
    let receipt: JsonValue = row.get("receipt");
    GroupForkReceipt {
        fork_group_id: row.get("fork_group_id"),
        source_group_id: row.get("source_group_id"),
        intervention_member_id: row.get("intervention_member_id"),
        intervention_cursor: row.get("intervention_cursor"),
        member_policies: Some(members.0),
        tool_policies: Some(tools.0),
        status: row.get("status"),
        first_divergence_event_id: row.get("first_divergence_event_id"),
        receipt: Some(receipt.0),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

/// Column list that `row_to_metadata` expects. All queries returning a
/// `ChannelMetadata` must SELECT exactly these columns.
const METADATA_COLUMNS: &str = "channel_id, parent_channel_id, origin_run_id, forked_at_cursor, \
     forked_at, display_name, reason, experiment_metadata, requested_at_cursor, \
     resolved_state_cursor, resolved_event_id, source_state_version, source_state_sha256, \
     restorable, manifest, reproducibility, tool_policies, outcome, result_score, deleted_at";

fn row_to_metadata(row: &Row) -> ChannelMetadata {
    let channel_id: String = row.get("channel_id");
    let parent_channel_id: Option<String> = row.get("parent_channel_id");
    let origin_run_id: String = row.get("origin_run_id");
    let forked_at_cursor: i64 = row.get("forked_at_cursor");
    let forked_at: DateTime<Utc> = row.get("forked_at");
    let display_name: Option<String> = row.get("display_name");
    let reason: Option<String> = row.get("reason");
    let experiment_metadata: Option<crate::database::json_compat::JsonValue> =
        row.get("experiment_metadata");
    let requested_at_cursor: i64 = row.try_get("requested_at_cursor").unwrap_or(0);
    let resolved_state_cursor: i64 = row.try_get("resolved_state_cursor").unwrap_or(0);
    let resolved_event_id: Option<Uuid> = row.try_get("resolved_event_id").ok().flatten();
    let source_state_version: i64 = row.try_get("source_state_version").unwrap_or(0);
    let source_state_sha256: Option<String> = row.try_get("source_state_sha256").ok().flatten();
    let restorable: bool = row.try_get("restorable").unwrap_or(false);
    let manifest: Option<crate::database::json_compat::JsonValue> =
        row.try_get("manifest").ok().flatten();
    let reproducibility: Option<String> = row.try_get("reproducibility").ok().flatten();
    let tool_policies: Option<crate::database::json_compat::JsonValue> =
        row.try_get("tool_policies").ok().flatten();
    let outcome: Option<String> = row.try_get("outcome").ok().flatten();
    let result_score: Option<f64> = row
        .try_get::<_, Option<f32>>("result_score")
        .ok()
        .flatten()
        .map(|s| s as f64);
    let deleted_at: Option<DateTime<Utc>> = row.try_get("deleted_at").ok().flatten();

    ChannelMetadata {
        channel_id,
        parent_channel_id,
        origin_run_id,
        forked_at_cursor,
        forked_at,
        display_name,
        reason,
        experiment_metadata: experiment_metadata.map(|v| v.0),
        requested_at_cursor,
        resolved_state_cursor,
        resolved_event_id,
        source_state_version,
        source_state_sha256,
        restorable,
        manifest: manifest.map(|v| v.0),
        reproducibility,
        tool_policies: tool_policies.map(|v| v.0),
        outcome,
        result_score,
        deleted_at,
    }
}

fn row_to_experiment_group(row: &Row) -> ExperimentGroup {
    ExperimentGroup {
        group_id: row.get::<_, Uuid>("group_id").to_string(),
        name: row.get("name"),
        description: row.try_get("description").ok().flatten(),
        baseline_channel_id: row.try_get("baseline_channel_id").ok().flatten(),
        status: row.get("status"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        members: Vec::new(),
    }
}

fn row_to_group(row: &Row) -> GroupInfo {
    GroupInfo {
        group_id: row.get("group_id"),
        channel_id: row.get("channel_id"),
        created_at: row.get("created_at"),
        metadata: row
            .try_get::<_, Option<crate::database::json_compat::JsonValue>>("metadata")
            .ok()
            .flatten()
            .map(|v| v.0),
    }
}

fn row_to_offset(row: &Row) -> GroupOffsetInfo {
    GroupOffsetInfo {
        group_id: row.get("group_id"),
        consumer_id: row.get("consumer_id"),
        last_cursor: row.get("last_cursor"),
        claimed_cursor: row.get("claimed_cursor"),
        updated_at: row.get("updated_at"),
    }
}

fn row_to_wakeup(row: &Row) -> WakeupInfo {
    WakeupInfo {
        id: row.get::<_, Uuid>("id").to_string(),
        channel_id: row.get("channel_id"),
        run_at: row.get("run_at"),
        status: row.get("status"),
        payload: row
            .try_get::<_, Option<crate::database::json_compat::JsonValue>>("payload")
            .ok()
            .flatten()
            .map(|v| v.0),
        created_at: row.get("created_at"),
        executed_at: row.get("executed_at"),
        attempts: row.get("attempts"),
        error: row.get("error"),
    }
}

#[derive(Debug)]
pub enum EventStoreError {
    Pool(deadpool_postgres::PoolError),
    Execute(tokio_postgres::Error),
    NotFound,
    /// No saved snapshot exists at-or-before the requested fork cursor.
    SnapshotBoundaryNotFound {
        channel_id: String,
        at_cursor: i64,
    },
    /// Optimistic-concurrency guard rejected the write.
    VersionConflict {
        channel_id: String,
        expected_version: Option<i64>,
        actual_version: Option<i64>,
        expected_cursor: Option<i64>,
        actual_cursor: Option<i64>,
    },
    GroupNotFound(String),
    ConsumerNotFound(String),
    ExecutionGroupNotFound(String),
    ExecutionGroupMemberNotFound {
        group_id: String,
        member_id: String,
    },
    ExecutionGroupMemberClaimed {
        group_id: String,
        member_id: String,
    },
    ExecutionGroupMemberFenced {
        group_id: String,
        member_id: String,
    },
    CounterfactualBlocked {
        fork_group_id: String,
        boundary: String,
    },
    WakeupNotFound(String),
    /// The requested idempotency key already reached a terminal state under a
    /// different request (request hash mismatch).
    ExecutionKeyMismatch {
        channel_id: String,
        key_name: String,
    },
    ExecutionNotFound(String),
    /// The operation is not permitted for the supplied claim token (a stale
    /// attempt trying to close an execution owned by another attempt).
    ExecutionNotOwned(String),
    /// Internal: a concurrent claim inserted the same idempotency key while
    /// this claim was mid-flight; the caller should retry the claim.
    ExecutionInsertRace,
    /// Malformed RFC 3339 timestamp in a query time-range filter.
    InvalidTimeRange(String),
    /// A request was semantically invalid (missing field, invalid value).
    InvalidRequest(String),
    /// Internal data inconsistency (e.g. an idempotency record exists
    /// without its corresponding resource).
    InconsistentState(String),
    /// The same fork operation_id was reused with a DIFFERENT canonical
    /// request (request_hash mismatch) — replay the original request or use a
    /// fresh key.
    IdempotencyConflict {
        operation_id: String,
    },
    /// The requested child channel already exists but was created by a
    /// different fork definition (different source or boundary).
    ChannelConflict {
        channel_id: String,
    },
    /// The channel was soft-deleted (retention/GC); replays of deleted forks
    /// surface as 410 Gone instead of resurrecting a half-purged channel.
    ChannelDeleted(String),
    /// A non-recursive delete was attempted on a channel that has children.
    ChannelHasChildren {
        channel_id: String,
        count: usize,
    },
    /// The configured monthly event/ingress allowance was exhausted.
    QuotaExceeded,
    /// A completed execution's result expired under retention (tombstoned).
    /// The key identity survived but the result body was purged; a retry must
    /// not silently execute again.
    ExecutionResultExpired {
        channel_id: String,
        key_name: String,
        execution_id: Uuid,
    },
}

impl std::fmt::Display for EventStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventStoreError::Pool(e) => write!(f, "Connection pool error: {e}"),
            EventStoreError::Execute(e) => write!(f, "Database error: {e}"),
            EventStoreError::NotFound => write!(f, "Event not found"),
            EventStoreError::SnapshotBoundaryNotFound { channel_id, at_cursor } => {
                write!(
                    f,
                    "No saved state boundary at or before cursor {at_cursor} for channel {channel_id}"
                )
            }
            EventStoreError::VersionConflict {
                channel_id,
                expected_version,
                actual_version,
                expected_cursor,
                actual_cursor,
            } => write!(
                f,
                "State version conflict for channel {channel_id}: expected version {expected_version:?} (actual {actual_version:?}), expected cursor {expected_cursor:?} (actual {actual_cursor:?})"
            ),
            EventStoreError::GroupNotFound(g) => write!(f, "Consumer group not found: {g}"),
            EventStoreError::ConsumerNotFound(c) => {
                write!(f, "Consumer not found or lease expired: {c}")
            }
            EventStoreError::ExecutionGroupNotFound(g) => write!(f, "Execution group not found: {g}"),
            EventStoreError::ExecutionGroupMemberNotFound { group_id, member_id } => write!(f, "Execution-group member not found: {group_id}/{member_id}"),
            EventStoreError::ExecutionGroupMemberClaimed { group_id, member_id } => write!(f, "Execution-group member is held by a live owner: {group_id}/{member_id}"),
            EventStoreError::ExecutionGroupMemberFenced { group_id, member_id } => write!(f, "Execution-group member lease is stale or fenced: {group_id}/{member_id}"),
            EventStoreError::CounterfactualBlocked { fork_group_id, boundary } => write!(f, "Counterfactual '{fork_group_id}' blocked at {boundary}"),
            EventStoreError::WakeupNotFound(w) => write!(f, "Wake-up not found: {w}"),
            EventStoreError::ExecutionKeyMismatch { channel_id, key_name } => write!(
                f,
                "Idempotency key conflict: key '{key_name}' on channel '{channel_id}' was already used with a different request. Reuse a fresh key or return the original request parameters."
            ),
            EventStoreError::ExecutionNotFound(id) => write!(f, "Execution not found: {id}"),
            EventStoreError::ExecutionNotOwned(id) => write!(
                f,
                "Execution {id} is owned by another attempt; the provided claim token is no longer current."
            ),
            EventStoreError::ExecutionInsertRace => {
                write!(f, "Execution insert race; retry the claim")
            }
            EventStoreError::InvalidTimeRange(s) => {
                write!(f, "Invalid timestamp in query filter: {s}")
            }
            EventStoreError::InvalidRequest(m) => write!(f, "Invalid request: {m}"),
            EventStoreError::InconsistentState(m) => write!(f, "Inconsistent state: {m}"),
            EventStoreError::IdempotencyConflict { operation_id } => write!(
                f,
                "Idempotency conflict: fork operation '{operation_id}' was already used with a different request. Replay the original request or use a fresh operation_id."
            ),
            EventStoreError::ChannelConflict { channel_id } => write!(
                f,
                "Channel conflict: '{channel_id}' already exists as a fork created from a different source or boundary. Use a fresh child channel id."
            ),
            EventStoreError::ChannelDeleted(id) => write!(
                f,
                "Channel '{id}' was deleted and is no longer available."
            ),
            EventStoreError::ChannelHasChildren { channel_id, count } => write!(
                f,
                "Channel '{channel_id}' has {count} child fork(es); pass recursive=true to delete the whole subtree."
            ),
            EventStoreError::QuotaExceeded => write!(f, "Monthly event or ingress quota exceeded"),
            EventStoreError::ExecutionResultExpired {
                channel_id,
                key_name,
                execution_id,
            } => write!(
                f,
                "Execution result expired: key '{key_name}' on channel '{channel_id}' (execution {execution_id}) completed more than the retention window ago and its stored result was purged. The prior outcome is unknowable; resolve it downstream before retrying with a fresh key."
            ),
        }
    }
}

impl std::error::Error for EventStoreError {}

// ── State Snapshot Operations ──────────────────────────────────

impl EventStore {
    /// Save a state snapshot aligned to a cursor for a channel.
    /// Each call creates a new immutable version. Returns the version number.
    ///
    /// Optional optimistic-concurrency guards (`expected_version` /
    /// `expected_cursor`) reject the write unless the channel's latest saved
    /// state matches. `Some(0)` as expected_version means "no snapshot yet".
    #[instrument(skip(self, state))]
    pub async fn save_state(
        &self,
        channel_id: &str,
        cursor: i64,
        state: &sonic_rs::Value,
        expected_version: Option<i64>,
        expected_cursor: Option<i64>,
    ) -> Result<i64, EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let state_json = JsonValue::new(state.clone());

        let transaction = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;

        check_state_guards(&transaction, channel_id, expected_version, expected_cursor).await?;

        let row = transaction
            .query_one(
                "INSERT INTO channel_states (channel_id, cursor, state, timestamp)
                 VALUES ($1, $2, $3, NOW())
                 RETURNING version",
                &[&channel_id, &cursor, &state_json],
            )
            .await
            .map_err(map_quota_exec_error)?;
        let version: i64 = row.get("version");

        // Account this state write against the durable-written meter so the
        // control plane sees it (the DB trigger on channel_states enforces
        // the actual quota atomically; this outbox row feeds usage reporting).
        let state_bytes = sonic_rs::to_string(state)
            .unwrap_or_default()
            .len()
            .min(i64::MAX as usize) as i64;
        let usage_date = Utc::now().date_naive();
        transaction
            .execute(
                "INSERT INTO usage_delivery_outbox
                 (idempotency_key, usage_date, metric, quantity, source_generation)
                 VALUES ($1, $2::date, 'state_bytes', $3, 1),
                        ($4, $2::date, 'durable_written_bytes', $3, 1)
                 ON CONFLICT (idempotency_key) DO NOTHING",
                &[
                    &format!("state:{channel_id}:{cursor}:{version}"),
                    &usage_date,
                    &state_bytes,
                    &format!("state-durable:{channel_id}:{cursor}:{version}"),
                ],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        // Registry: a state snapshot can exist without events yet — ensure
        // the channel row exists (audit item 14).
        transaction
            .execute(
                "INSERT INTO channels (channel_id, last_cursor, updated_at)
                 VALUES ($1, GREATEST($2::bigint, 0), NOW())
                 ON CONFLICT (channel_id) DO UPDATE
                 SET last_cursor = GREATEST(channels.last_cursor, EXCLUDED.last_cursor),
                     updated_at = NOW()",
                &[&channel_id, &cursor],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        transaction
            .commit()
            .await
            .map_err(EventStoreError::Execute)?;

        Ok(version)
    }

    /// Load the latest state snapshot for a channel.
    #[instrument(skip(self))]
    pub async fn load_state(
        &self,
        channel_id: &str,
    ) -> Result<Option<(i64, sonic_rs::Value)>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let row = client
            .query_opt(
                "SELECT cursor, state FROM channel_states
                 WHERE channel_id = $1
                 ORDER BY version DESC
                 LIMIT 1",
                &[&channel_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        match row {
            Some(r) => {
                let cursor: i64 = r.get("cursor");
                let state_json: JsonValue = r.get("state");
                Ok(Some((cursor, state_json.into())))
            }
            None => Ok(None),
        }
    }

    /// Load the latest state snapshot at-or-before `at_cursor` for a channel
    /// (or the latest snapshot overall when `at_cursor <= 0`). This is the
    /// pool-level counterpart of `resolve_fork_state`, used to report what a
    /// fork actually inherited at its boundary.
    #[instrument(skip(self))]
    pub async fn load_state_at_or_before(
        &self,
        channel_id: &str,
        at_cursor: i64,
    ) -> Result<Option<(i64, sonic_rs::Value)>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let row = if at_cursor > 0 {
            client
                .query_opt(
                    "SELECT cursor, state FROM channel_states
                     WHERE channel_id = $1 AND cursor <= $2
                     ORDER BY cursor DESC, version DESC LIMIT 1",
                    &[&channel_id, &at_cursor],
                )
                .await
                .map_err(EventStoreError::Execute)?
        } else {
            client
                .query_opt(
                    "SELECT cursor, state FROM channel_states
                     WHERE channel_id = $1
                     ORDER BY version DESC LIMIT 1",
                    &[&channel_id],
                )
                .await
                .map_err(EventStoreError::Execute)?
        };
        Ok(row.map(|r| {
            let cursor: i64 = r.get("cursor");
            let state_json: JsonValue = r.get("state");
            (cursor, state_json.into())
        }))
    }

    /// Fork a channel: create new channel with state copied from the parent
    /// at an exact boundary, plus a `fork.started` lineage event on the
    /// child. Fully transactional and idempotent.
    ///
    /// Semantics:
    /// - `at_cursor == 0`: fork from the latest saved state (no snapshot →
    ///   empty child, allowed).
    /// - `at_cursor > 0`: fork from the latest snapshot version whose
    ///   `cursor <= at_cursor`; missing boundary → SnapshotBoundaryNotFound.
    /// - The child state row carries the resolved parent cursor, so the
    ///   child's `load_state` reports the true fork boundary.
    /// - The child's first event is `fork.started` (cursor 1,
    ///   `depends_on` = parent's boundary event id).
    /// - `operation_id` (or an existing `new_channel_id`) makes the fork
    ///   idempotent: replays return the original result without duplication.
    #[instrument(skip(self, request))]
    pub async fn fork_channel(
        &self,
        request: &ForkRequest,
    ) -> Result<(ChannelMetadata, bool), EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;

        let now = Utc::now();
        let exp_meta_json = request
            .experiment_metadata
            .as_ref()
            .map(|v| JsonValue::new(v.clone()));

        let mut transaction = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;

        let request_hash = fork_request_hash(request);
        let manifest_json = request.manifest.as_ref().map(|v| JsonValue::new(v.clone()));
        let tool_policies_json = request
            .tool_policies
            .as_ref()
            .map(|v| JsonValue::new(v.clone()));

        // ── Idempotency, ledger-first ──────────────────────────────────────
        // The `fork_operations` ledger is the authoritative map
        // operation_id -> child_channel_id. Retries with the same operation_id
        // replay the ORIGINAL child (even when the retried request names a
        // different child id). Request drift (same key, different canonical
        // request) is a 409 idempotency_conflict.
        if let Some(op_id) = &request.operation_id {
            // `ON CONFLICT DO NOTHING` (no target) covers BOTH uniqueness
            // constraints: a pre-used operation_id AND a child_channel_id
            // already owned by another operation.
            let inserted = transaction
                .query_opt(
                    "INSERT INTO fork_operations
                        (operation_id, child_channel_id, request_hash, source_channel_id, at_cursor,
                         requested_at_cursor, resolved_state_cursor, resolved_event_id,
                         source_state_version, source_state_sha256, restorable, manifest,
                         tool_policies)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                     ON CONFLICT DO NOTHING
                     RETURNING operation_id",
                    &[
                        &op_id,
                        &request.new_channel_id,
                        &request_hash,
                        &request.source_channel_id,
                        &request.at_cursor,
                        &request.at_cursor, // requested_at_cursor (provisional; final row upserted later)
                        &0_i64,             // resolved_state_cursor (provisional)
                        &None::<Uuid>,      // resolved_event_id (provisional)
                        &0_i64,             // source_state_version (provisional)
                        &None::<Option<String>>, // source_state_sha256 (provisional)
                        &false,             // restorable (provisional)
                        &manifest_json,
                        &tool_policies_json,
                    ],
                )
                .await
                .map_err(EventStoreError::Execute)?;
            if inserted.is_none() {
                // Distinguish "this op_id was already used" from "this child
                // id is owned by a different operation".
                let ledger = transaction
                    .query_opt(
                        "SELECT child_channel_id, request_hash FROM fork_operations
                         WHERE operation_id = $1",
                        &[&op_id],
                    )
                    .await
                    .map_err(EventStoreError::Execute)?;
                if let Some(r) = ledger {
                    let stored_child: String = r.get("child_channel_id");
                    let stored_hash: String = r.get("request_hash");
                    if stored_hash != request_hash {
                        transaction
                            .rollback()
                            .await
                            .map_err(EventStoreError::Execute)?;
                        return Err(EventStoreError::IdempotencyConflict {
                            operation_id: op_id.clone(),
                        });
                    }
                    let child = transaction
                        .query_opt(
                            &format!(
                                "SELECT {METADATA_COLUMNS} FROM channel_metadata WHERE channel_id = $1"
                            ),
                            &[&stored_child],
                        )
                        .await
                        .map_err(EventStoreError::Execute)?;
                    transaction
                        .commit()
                        .await
                        .map_err(EventStoreError::Execute)?;
                    return match child {
                        Some(row) => {
                            let meta = row_to_metadata(&row);
                            if meta.deleted_at.is_some() {
                                return Err(EventStoreError::ChannelDeleted(meta.channel_id));
                            }
                            Ok((meta, true))
                        }
                        None => Err(EventStoreError::InconsistentState(format!(
                            "Fork operation {op_id} exists without child channel {stored_child}"
                        ))),
                    };
                }
                // No ledger row for this op_id → the conflict was the child
                // channel's UNIQUE constraint: another operation already owns
                // this child id. Fall through to the child-id check below.
            }
        }

        // ── Child-id idempotency ──────────────────────────────────────────
        // An existing child channel replays only when the definition matches
        // (same source + same requested boundary); any drift is a 409
        // channel_conflict. Also catches the edge where the operation_id path
        // just inserted a ledger row for a child that already exists.
        let existing_child = transaction
            .query_opt(
                &format!("SELECT {METADATA_COLUMNS} FROM channel_metadata WHERE channel_id = $1"),
                &[&request.new_channel_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        if let Some(row) = existing_child {
            let meta = row_to_metadata(&row);
            if meta.deleted_at.is_some() {
                transaction
                    .rollback()
                    .await
                    .map_err(EventStoreError::Execute)?;
                return Err(EventStoreError::ChannelDeleted(meta.channel_id));
            }
            let same_definition = meta.parent_channel_id.as_deref()
                == Some(request.source_channel_id.as_str())
                && meta.requested_at_cursor == request.at_cursor;
            if !same_definition {
                transaction
                    .rollback()
                    .await
                    .map_err(EventStoreError::Execute)?;
                return Err(EventStoreError::ChannelConflict {
                    channel_id: request.new_channel_id.clone(),
                });
            }
            transaction
                .commit()
                .await
                .map_err(EventStoreError::Execute)?;
            return Ok((meta, true));
        }

        // ── Optimistic guard (item 18): reject unless the source state is
        //    exactly what the caller observed ──
        if request.expected_version.is_some() || request.expected_cursor.is_some() {
            check_state_guards(
                &transaction,
                &request.source_channel_id,
                request.expected_version,
                request.expected_cursor,
            )
            .await?;
        }

        // ── Source existence check — inside the transaction to prevent
        //    TOCTOU races between the check and the INSERT ──
        let source_exists = transaction
            .query_opt(
                "SELECT 1 FROM channel_cursors WHERE channel_id = $1
             FOR UPDATE",
                &[&request.source_channel_id],
            )
            .await
            .map_err(EventStoreError::Execute)?
            .is_some()
            || transaction
                .query_opt(
                    "SELECT 1 FROM channel_states WHERE channel_id = $1 LIMIT 1
             FOR UPDATE",
                    &[&request.source_channel_id],
                )
                .await
                .map_err(EventStoreError::Execute)?
                .is_some()
            || transaction
                .query_opt(
                    "SELECT 1 FROM agent_events WHERE channel_id = $1 LIMIT 1
             FOR UPDATE",
                    &[&request.source_channel_id],
                )
                .await
                .map_err(EventStoreError::Execute)?
                .is_some();

        if !source_exists {
            transaction
                .rollback()
                .await
                .map_err(EventStoreError::Execute)?;
            return Err(EventStoreError::NotFound);
        }

        // ── Exact-boundary state resolution (item 2/6) ──
        let resolved =
            resolve_fork_state(&transaction, &request.source_channel_id, request.at_cursor).await?;
        if request.at_cursor > 0 && resolved.is_none() {
            transaction
                .rollback()
                .await
                .map_err(EventStoreError::Execute)?;
            return Err(EventStoreError::SnapshotBoundaryNotFound {
                channel_id: request.source_channel_id.clone(),
                at_cursor: request.at_cursor,
            });
        }

        let (resolved_version, resolved_cursor, copied_state, state_sha) = match &resolved {
            Some((version, cursor, state)) => {
                let value: sonic_rs::Value = state.clone().into();
                let sha = state_sha256(&value);
                (*version, *cursor, Some(value), Some(sha))
            }
            None => (0, 0, None, None),
        };

        // ── Parent's boundary event id for the child's depends_on edge (P2) ──
        // Snapshot cursors live in the same per-channel cursor space as the
        // events they align with, so the boundary event resolves directly.
        // Prefer the event at the exact resolved snapshot cursor; fall back
        // to the latest event at-or-before at_cursor for state-only channels
        // where no event matches.
        let parent_event_id: Option<Uuid> = if request.at_cursor > 0 {
            let mut row = None;
            if resolved_cursor > 0 {
                row = transaction
                    .query_opt(
                        "SELECT id FROM agent_events
                         WHERE channel_id = $1 AND cursor = $2",
                        &[&request.source_channel_id, &resolved_cursor],
                    )
                    .await
                    .map_err(EventStoreError::Execute)?;
            }
            if row.is_none() {
                row = transaction
                    .query_opt(
                        "SELECT id FROM agent_events
                         WHERE channel_id = $1 AND cursor <= $2
                         ORDER BY cursor DESC LIMIT 1",
                        &[&request.source_channel_id, &request.at_cursor],
                    )
                    .await
                    .map_err(EventStoreError::Execute)?;
            }
            row.map(|r| r.get("id"))
        } else {
            let row = transaction
                .query_opt(
                    "SELECT id FROM agent_events
                     WHERE channel_id = $1
                     ORDER BY cursor DESC LIMIT 1",
                    &[&request.source_channel_id],
                )
                .await
                .map_err(EventStoreError::Execute)?;
            row.map(|r| r.get("id"))
        };

        // ── Copy the resolved state into the child (item 2) ──
        if let Some(state) = &copied_state {
            transaction
                .execute(
                    "INSERT INTO channel_states (channel_id, cursor, state, timestamp, write_source)
                 VALUES ($1, $2, $3, $4, 'fork')",
                    &[
                        &request.new_channel_id,
                        &resolved_cursor,
                        &JsonValue::new(state.clone()),
                        &now,
                    ],
                )
                .await
                .map_err(EventStoreError::Execute)?;
        }

        let parent_meta = transaction
            .query_opt(
                "SELECT origin_run_id FROM channel_metadata WHERE channel_id = $1",
                &[&request.source_channel_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        let origin_run_id = match parent_meta {
            Some(row) => {
                let origin: String = row.get("origin_run_id");
                origin
            }
            None => request.source_channel_id.clone(),
        };

        // ── Reproducibility grade (item 11) ──
        //   context_exact        : restorable && exact boundary && complete manifest
        //   execution_replayable : restorable && exact boundary && tool ledger present
        //   state_exact          : restorable && exact boundary (0 = latest, self-consistent)
        //   best_effort          : everything else (incl. approximate boundary, lineage-only)
        let reproducibility = if resolved.is_none() {
            "best_effort".to_string()
        } else {
            let exact = request.at_cursor == 0 || request.at_cursor == resolved_cursor;
            if !exact {
                "best_effort".to_string()
            } else if manifest_complete(request.manifest.as_ref()) {
                "context_exact".to_string()
            } else {
                let has_ledger = transaction
                    .query_opt(
                        "SELECT EXISTS(SELECT 1 FROM tool_executions WHERE channel_id = $1) AS e",
                        &[&request.source_channel_id],
                    )
                    .await
                    .map_err(EventStoreError::Execute)?
                    .map(|r| r.get::<_, bool>("e"))
                    .unwrap_or(false);
                if has_ledger {
                    "execution_replayable".to_string()
                } else {
                    "state_exact".to_string()
                }
            }
        };

        // ── Lineage event on the child (P2) — also creates the child's
        //    channel_cursors row so the fork is immediately discoverable ──
        let fork_payload = sonic_rs::json!({
            "source_channel_id": request.source_channel_id,
            "forked_at_cursor": resolved_cursor,
            "requested_at_cursor": request.at_cursor,
            "resolved_cursor": resolved_cursor,
            "resolved_event_id": parent_event_id,
            "source_state_version": resolved_version,
            "source_state_sha256": state_sha,
            "restorable": resolved.is_some(),
            "reproducibility": reproducibility,
            "reason": request.reason,
            "display_name": request.display_name,
        });
        insert_event_in_tx(
            &mut transaction,
            &request.new_channel_id,
            "fork.started",
            &fork_payload,
            &EventMetadata {
                actor: "system".to_string(),
                agent_id: None,
                user_id: None,
                metadata: None,
            },
            parent_event_id,
            None,
            now,
        )
        .await?;

        // Registry lineage (audit item 14): the fork.started insert above
        // already registered the child channel; stamp its parent + boundary.
        transaction
            .execute(
                "UPDATE channels
                 SET parent_channel_id = $2, forked_at_cursor = $3, updated_at = NOW()
                 WHERE channel_id = $1",
                &[
                    &request.new_channel_id,
                    &request.source_channel_id,
                    &resolved_cursor,
                ],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        transaction
            .execute(
                "INSERT INTO channel_metadata (
                channel_id, parent_channel_id, origin_run_id, forked_at_cursor,
                forked_at, display_name, reason, experiment_metadata,
                requested_at_cursor, resolved_state_cursor, resolved_event_id,
                source_state_version, source_state_sha256, restorable, manifest,
                reproducibility, tool_policies
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            ON CONFLICT (channel_id) DO NOTHING",
                &[
                    &request.new_channel_id,
                    &request.source_channel_id,
                    &origin_run_id,
                    &resolved_cursor, // forked_at_cursor = RESOLVED boundary (item 2)
                    &now,
                    &request.display_name.as_deref(),
                    &request.reason.as_deref(),
                    &exp_meta_json,
                    &request.at_cursor,  // requested_at_cursor
                    &resolved_cursor,    // resolved_state_cursor
                    &parent_event_id,    // resolved_event_id
                    &resolved_version,   // source_state_version
                    &state_sha,          // source_state_sha256
                    &resolved.is_some(), // restorable
                    &manifest_json,
                    &reproducibility,
                    &tool_policies_json,
                ],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        // ── Finalize the ledger row with the actual resolved provenance ──
        if let Some(op_id) = &request.operation_id {
            transaction
                .execute(
                    "UPDATE fork_operations SET
                        requested_at_cursor = $2,
                        resolved_state_cursor = $3,
                        resolved_event_id = $4,
                        source_state_version = $5,
                        source_state_sha256 = $6,
                        restorable = $7,
                        manifest = COALESCE($8, manifest),
                        tool_policies = COALESCE($9, tool_policies),
                        applied_at = NOW()
                     WHERE operation_id = $1",
                    &[
                        &op_id,
                        &request.at_cursor,
                        &resolved_cursor,
                        &parent_event_id,
                        &resolved_version,
                        &state_sha,
                        &resolved.is_some(),
                        &manifest_json,
                        &tool_policies_json,
                    ],
                )
                .await
                .map_err(EventStoreError::Execute)?;
        }

        // ── Meter the differentiated primitive: one successful fork ──
        // Written in the SAME transaction as the fork so the linked control
        // plane's count can never diverge from the durable fork. Delivered
        // only on a linked sync; a standalone instance keeps it locally and
        // sends nothing. Replays return earlier (the idempotency paths above
        // commit/return before reaching here), so this fires once per real
        // fork. `fork_operations` also covers `resume(fork_at_step=)`, which
        // is a fork under the hood.
        transaction
            .execute(
                "INSERT INTO usage_delivery_outbox
                 (idempotency_key, usage_date, metric, quantity, source_generation)
                 VALUES ($1, $2::date, 'fork_operations', 1, 1)
                 ON CONFLICT (idempotency_key) DO NOTHING",
                &[
                    &format!("fork:{}", request.new_channel_id),
                    &now.date_naive(),
                ],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        transaction
            .commit()
            .await
            .map_err(EventStoreError::Execute)?;

        Ok((
            ChannelMetadata {
                channel_id: request.new_channel_id.clone(),
                parent_channel_id: Some(request.source_channel_id.clone()),
                origin_run_id,
                forked_at_cursor: resolved_cursor,
                forked_at: now,
                display_name: request.display_name.clone(),
                reason: request.reason.clone(),
                experiment_metadata: request.experiment_metadata.clone(),
                requested_at_cursor: request.at_cursor,
                resolved_state_cursor: resolved_cursor,
                resolved_event_id: parent_event_id,
                source_state_version: resolved_version,
                source_state_sha256: state_sha,
                restorable: resolved.is_some(),
                manifest: request.manifest.clone(),
                reproducibility: Some(reproducibility),
                tool_policies: request.tool_policies.clone(),
                outcome: None,
                result_score: None,
                deleted_at: None,
            },
            false,
        ))
    }

    /// Get metadata for a single channel.
    #[instrument(skip(self))]
    pub async fn get_channel_metadata(
        &self,
        channel_id: &str,
    ) -> Result<Option<ChannelMetadata>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let row = client
            .query_opt(
                &format!(
                    "SELECT {METADATA_COLUMNS}
             FROM channel_metadata
             WHERE channel_id = $1"
                ),
                &[&channel_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        match row {
            Some(r) => Ok(Some(row_to_metadata(&r))),
            None => Ok(None),
        }
    }

    /// Resolve a step number to the channel that OWNS it and its cursor, using
    /// the server-owned `channel_steps` index.
    ///
    /// Walks the fork lineage: a fork inherits its first
    /// `experiment_metadata.forked_at_step` steps from its parent, so step N
    /// of a fork resolves against the ancestor that recorded it. Returns
    /// `(owner_channel, cursor, event_id)` when the step exists on the owner.
    #[instrument(skip(self))]
    pub async fn resolve_step(
        &self,
        channel_id: &str,
        step_number: i64,
    ) -> Result<Option<(String, i64, Option<Uuid>)>, EventStoreError> {
        if step_number < 1 {
            return Ok(None);
        }
        // Walk up the lineage to the channel that recorded this step.
        let mut owner = channel_id.to_string();
        for _ in 0..Self::MAX_TREE_DEPTH {
            let meta = self.get_channel_metadata(&owner).await?;
            let Some(meta) = meta else { break };
            let forked_at_step = meta
                .experiment_metadata
                .as_ref()
                .and_then(|v| v.get("forked_at_step"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            if forked_at_step == 0 || step_number > forked_at_step {
                break;
            }
            let parent = meta.parent_channel_id.unwrap_or_default();
            if parent.is_empty() || parent == meta.channel_id {
                break;
            }
            owner = parent;
        }

        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let row = client
            .query_opt(
                "SELECT cursor, event_id FROM channel_steps
                 WHERE channel_id = $1 AND step_number = $2",
                &[&owner, &step_number],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(row.map(|r| {
            let cursor: i64 = r.get("cursor");
            let event_id: Option<Uuid> = r.get("event_id");
            (owner, cursor, event_id)
        }))
    }

    /// The highest step number recorded on this channel (0 when it has no
    /// indexed steps). O(1) via the `channel_steps` primary key. This is the
    /// authoritative "where is this run?" answer for crash recovery, so a
    /// resume does not have to page the event log or trust lagging metadata.
    #[instrument(skip(self))]
    pub async fn latest_step_number(&self, channel_id: &str) -> Result<i64, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let row = client
            .query_one(
                "SELECT COALESCE(MAX(step_number), 0) AS last_step
                 FROM channel_steps WHERE channel_id = $1",
                &[&channel_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(row.get("last_step"))
    }

    /// Return whether a channel has ever existed: it has at least one event,
    /// state snapshot, metadata row, or channel_cursors row. Used to
    /// distinguish a genuinely-unknown channel (404) from a real channel that
    /// merely has no fork metadata (synthesized default).
    #[instrument(skip(self))]
    pub async fn channel_exists(&self, channel_id: &str) -> Result<bool, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let row = client
            .query_opt(
                "SELECT EXISTS (
                    SELECT 1 FROM agent_events WHERE channel_id = $1
                    UNION ALL
                    SELECT 1 FROM channel_states WHERE channel_id = $1
                    UNION ALL
                    SELECT 1 FROM channel_metadata WHERE channel_id = $1
                    UNION ALL
                    SELECT 1 FROM channel_cursors WHERE channel_id = $1
                 ) AS exists_flag",
                &[&channel_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(row
            .map(|r| r.get::<_, bool>("exists_flag"))
            .unwrap_or(false))
    }

    /// Load a specific version of a state snapshot.
    #[instrument(skip(self))]
    pub async fn load_state_version(
        &self,
        channel_id: &str,
        version: i64,
    ) -> Result<Option<(i64, i64, sonic_rs::Value)>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let row = client
            .query_opt(
                "SELECT cursor, version, state FROM channel_states
                 WHERE channel_id = $1 AND version = $2",
                &[&channel_id, &version],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        match row {
            Some(r) => {
                let cursor: i64 = r.get("cursor");
                let v: i64 = r.get("version");
                let state_json: JsonValue = r.get("state");
                Ok(Some((cursor, v, state_json.into())))
            }
            None => Ok(None),
        }
    }

    /// List direct children (forks) of a channel.
    #[instrument(skip(self))]
    pub async fn list_forks(
        &self,
        parent_channel_id: &str,
    ) -> Result<Vec<ChannelMetadata>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let rows = client
            .query(
                &format!(
                    "SELECT {METADATA_COLUMNS}
             FROM channel_metadata
             WHERE parent_channel_id = $1 AND deleted_at IS NULL
             ORDER BY forked_at ASC"
                ),
                &[&parent_channel_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        Ok(rows.iter().map(row_to_metadata).collect())
    }

    /// Maximum depth for execution tree traversal to prevent runaway recursion.
    const MAX_TREE_DEPTH: u32 = 10;

    /// Get the full execution tree rooted at a channel.
    /// Returns the root plus all descendants up to MAX_TREE_DEPTH; when the
    /// tree is deeper the response reports `truncated` instead of silently
    /// dropping forks.
    #[instrument(skip(self))]
    pub async fn get_fork_tree(
        &self,
        root_channel_id: &str,
    ) -> Result<(ForkInfo, bool), EventStoreError> {
        let metadata = self
            .get_channel_metadata(root_channel_id)
            .await?
            .unwrap_or_else(|| ChannelMetadata {
                channel_id: root_channel_id.to_string(),
                parent_channel_id: None,
                origin_run_id: root_channel_id.to_string(),
                forked_at_cursor: 0,
                forked_at: Utc::now(),
                display_name: None,
                reason: None,
                experiment_metadata: None,
                ..Default::default()
            });

        let event_count = self.get_event_count(root_channel_id).await?;
        let latest_cursor = self.get_latest_cursor(root_channel_id).await?;

        // Batch-fetch all descendants via a single recursive CTE query with
        // cycle detection and a depth cap. Returns the tree plus whether the
        // depth cap was hit (truncation is reported, never silent).
        let (children, truncated) = self.fetch_descendants_batched(root_channel_id, 0).await?;

        Ok((
            ForkInfo {
                metadata,
                children,
                event_count,
                latest_cursor,
            },
            truncated,
        ))
    }

    /// Fetch children at a given depth level, then recurse for grandchildren.
    /// Limits total depth to MAX_TREE_DEPTH and reports truncation via the
    /// returned boolean (a deep tree is never silently trimmed).
    #[instrument(skip(self))]
    async fn fetch_descendants_batched(
        &self,
        parent_id: &str,
        depth: u32,
    ) -> Result<(Vec<ForkInfo>, bool), EventStoreError> {
        if depth >= Self::MAX_TREE_DEPTH {
            tracing::warn!(
                "Execution tree depth limit ({}) reached at channel {}",
                Self::MAX_TREE_DEPTH,
                parent_id
            );
            return Ok((Vec::new(), true));
        }

        // Single query: fetch all immediate children with their metrics in one roundtrip.
        let children = self.list_forks(parent_id).await?;
        if children.is_empty() {
            return Ok((Vec::new(), false));
        }

        // Batch-fetch event counts and cursors for all children in parallel.
        let batch_size = children.len();
        let mut child_infos = Vec::with_capacity(batch_size);
        for child in &children {
            let ev_count = self.get_event_count(&child.channel_id).await?;
            let latest = self.get_latest_cursor(&child.channel_id).await?;
            child_infos.push(ForkInfo {
                metadata: child.clone(),
                children: Vec::new(),
                event_count: ev_count,
                latest_cursor: latest,
            });
        }

        // Recurse for each child's descendants — use Box::pin to break the
        // recursive async future size explosion.
        let mut truncated = false;
        for child_info in &mut child_infos {
            let child_id = child_info.metadata.channel_id.clone();
            let (subtree, sub_truncated) =
                Box::pin(self.fetch_descendants_batched(&child_id, depth + 1)).await?;
            truncated |= sub_truncated;
            child_info.children = subtree;
        }

        Ok((child_infos, truncated))
    }

    /// Maximum number of diff entries returned per call (bounds response size
    /// for very divergent state trees). Truncation is reported honestly via
    /// `truncated` / `entry_count_total`.
    const MAX_DIFF_ENTRIES: usize = 500;

    /// Hard cap on the number of differing entries COUNTED while walking a
    /// diff. Bounds CPU on pathologically divergent states while still
    /// reporting an honest `entry_count_total`/`truncated`.
    const MAX_DIFF_ENTRIES_TOTAL: usize = 100_000;

    /// Maximum JSON nesting depth walked by the diff. Postgres caps JSONB
    /// nesting at ~100 levels anyway, but this keeps `walk_diff` from
    /// recursing unboundedly on adversarial input.
    const MAX_DIFF_DEPTH: usize = 64;

    /// Structural diff of two channels' latest saved states.
    ///
    /// Walks both JSONB states recursively and emits one `StateDiffEntry`
    /// per differing leaf path (added / removed / changed). When both
    /// channels are related by fork lineage, `common` carries the **lowest
    /// common ancestor's** latest state and `left_diverged_at_cursor` /
    /// `right_diverged_at_cursor` the per-side fork boundaries (the fork
    /// edge immediately after the LCA on each side; `None` when a side IS
    /// the LCA). Siblings forked at different cursors are asymmetric and
    /// both values are reported. Unrelated runs get `common = None`.
    #[instrument(skip(self))]
    pub async fn diff_channels(
        &self,
        left: &str,
        right: &str,
    ) -> Result<StateDiffResponse, EventStoreError> {
        let left_state = self.load_state(left).await?;
        let right_state = self.load_state(right).await?;

        let (left_cursor, left_value) = left_state.unwrap_or((0, sonic_rs::json!({})));
        let (right_cursor, right_value) = right_state.unwrap_or((0, sonic_rs::json!({})));

        let mut entries = Vec::new();
        let mut total: usize = 0;
        walk_diff(
            &left_value,
            &right_value,
            &mut Vec::new(),
            &mut entries,
            &mut total,
            Self::MAX_DIFF_ENTRIES,
            0,
        );

        // Determine shared ancestry + per-side divergence via the LOWEST
        // common ancestor (not just the shared origin_run_id — for a
        // parent/child diff the parent is the true ancestor, for siblings
        // it's their shared fork parent).
        let (common, left_diverged_at_cursor, right_diverged_at_cursor) =
            match self.common_ancestor(left, right).await? {
                Some(lca) => {
                    let common = self.load_state(&lca.lca_channel).await?;
                    (
                        common.map(|(cursor, state)| StatePoint {
                            channel_id: lca.lca_channel,
                            cursor,
                            state: Some(state),
                        }),
                        lca.left_diverged_at_cursor,
                        lca.right_diverged_at_cursor,
                    )
                }
                None => (None, None, None),
            };

        Ok(StateDiffResponse {
            left_channel_id: left.to_string(),
            right_channel_id: right.to_string(),
            left: StatePoint {
                channel_id: left.to_string(),
                cursor: left_cursor,
                state: Some(left_value),
            },
            right: StatePoint {
                channel_id: right.to_string(),
                cursor: right_cursor,
                state: Some(right_value),
            },
            common,
            left_diverged_at_cursor,
            right_diverged_at_cursor,
            entries,
            truncated: total > Self::MAX_DIFF_ENTRIES,
            entry_count_total: total,
            max_entries: Self::MAX_DIFF_ENTRIES,
        })
    }

    /// Find the lowest common ancestor of two channels in the fork lineage.
    ///
    /// Lineage is modeled as EDGES: each non-root channel carries its own
    /// `forked_at_cursor` (the resolved boundary where it split from its
    /// parent). The divergence point is the first non-common edge — the
    /// first fork after the LCA on each side — NOT the `forked_at_cursor`
    /// attached to the shared-prefix nodes (which for a root is 0 and for
    /// the LCA itself describes its own split, not the divergence being
    /// measured). Returns `None` for unrelated channels.
    async fn common_ancestor(
        &self,
        left: &str,
        right: &str,
    ) -> Result<Option<LcaResult>, EventStoreError> {
        if left == right {
            return Ok(Some(LcaResult {
                lca_channel: left.to_string(),
                left_diverged_at_cursor: None,
                right_diverged_at_cursor: None,
            }));
        }
        // Build leaf->root chains, then reverse to root->leaf for a common
        // prefix scan. Channels unrelated to any fork have a single-entry
        // chain.
        let mut left_chain = self.ancestry_chain(left).await?;
        let mut right_chain = self.ancestry_chain(right).await?;
        left_chain.reverse();
        right_chain.reverse();

        // Common prefix scan.
        let mut common_len = 0usize;
        let mut lca: Option<String> = None;
        let max_common = left_chain.len().min(right_chain.len());
        for i in 0..max_common {
            if left_chain[i].channel_id != right_chain[i].channel_id {
                break;
            }
            lca = Some(left_chain[i].channel_id.clone());
            common_len = i + 1;
        }

        let Some(lca_channel) = lca else {
            return Ok(None);
        };

        // Per-side divergence: the fork edge immediately after the LCA.
        let left_diverged_at_cursor = left_chain.get(common_len).map(|hop| hop.forked_at_cursor);
        let right_diverged_at_cursor = right_chain.get(common_len).map(|hop| hop.forked_at_cursor);

        Ok(Some(LcaResult {
            lca_channel,
            left_diverged_at_cursor,
            right_diverged_at_cursor,
        }))
    }

    /// Build the lineage chain leaf→root for a channel (bounded, self first).
    async fn ancestry_chain(&self, channel_id: &str) -> Result<Vec<LineageHop>, EventStoreError> {
        let mut chain: Vec<LineageHop> = Vec::new();
        let mut cursor = channel_id.to_string();
        for _ in 0..Self::MAX_TREE_DEPTH {
            let meta = self.get_channel_metadata(&cursor).await?;
            let parent = meta.as_ref().and_then(|m| m.parent_channel_id.clone());
            let is_root = parent.is_none() || parent.as_deref() == Some(cursor.as_str());
            chain.push(LineageHop {
                channel_id: cursor.clone(),
                parent_channel_id: parent.clone(),
                forked_at_cursor: meta.as_ref().map(|m| m.forked_at_cursor).unwrap_or(0),
                display_name: meta.as_ref().and_then(|m| m.display_name.clone()),
                reason: meta.as_ref().and_then(|m| m.reason.clone()),
            });
            match parent {
                Some(p) if !is_root && p != cursor => cursor = p,
                _ => break,
            }
        }
        Ok(chain)
    }

    /// Build the full decision trail for a channel: metadata, ancestry chain
    /// back to the `origin_run_id` root, the immutable fork boundary it
    /// inherited, and the tool-execution ledger rows that happened on this
    /// channel.
    ///
    /// The boundary is derived from IMMUTABLE fork-time data: the child's
    /// first `channel_states` row (the copied snapshot) plus the stored
    /// provenance columns — never from the parent's mutable latest state.
    /// This keeps the audit surface correct even when the parent advanced
    /// after the fork (audit item 3).
    #[instrument(skip(self))]
    pub async fn get_decision_trail(
        &self,
        channel_id: &str,
    ) -> Result<DecisionTrail, EventStoreError> {
        let metadata = self.get_channel_metadata(channel_id).await?;
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;

        // Walk parent_channel_id up to the root, bounded like the tree.
        // Always include the channel itself as the first hop, then each
        // ancestor that has channel_metadata (roots created via
        // publish_with_channel_cursor have no metadata row but still appear).
        let mut ancestry: Vec<LineageHop> = Vec::new();
        let mut cursor = channel_id.to_string();
        let mut reached_root = false;
        for _ in 0..Self::MAX_TREE_DEPTH {
            let meta = self.get_channel_metadata(&cursor).await?;
            let parent = meta.as_ref().and_then(|m| m.parent_channel_id.clone());
            let is_root = parent.is_none() || parent.as_deref() == Some(cursor.as_str());
            // Include the hop even when metadata is missing (root runs
            // created directly without an explicit metadata row).
            ancestry.push(LineageHop {
                channel_id: cursor.clone(),
                parent_channel_id: parent.clone(),
                forked_at_cursor: meta.as_ref().map(|m| m.forked_at_cursor).unwrap_or(0),
                display_name: meta.as_ref().and_then(|m| m.display_name.clone()),
                reason: meta.as_ref().and_then(|m| m.reason.clone()),
            });
            match parent {
                Some(p) if !is_root && p != cursor => cursor = p,
                _ => {
                    reached_root = true;
                    break;
                }
            }
        }
        let truncated = !reached_root;

        // Immutable boundary: the state the child actually inherited at fork
        // time. The child's FIRST snapshot row IS the copied state; its
        // cursor equals resolved_state_cursor. Roots and lineage-only forks
        // (no snapshot) have no boundary.
        let boundary = if let Some(meta) = &metadata {
            if let Some(parent) = &meta.parent_channel_id {
                if parent != &meta.channel_id {
                    let first_snapshot = client
                        .query_opt(
                            "SELECT cursor, state FROM channel_states
                             WHERE channel_id = $1
                             ORDER BY version ASC LIMIT 1",
                            &[&channel_id],
                        )
                        .await
                        .map_err(EventStoreError::Execute)?;
                    if meta.restorable || first_snapshot.is_some() {
                        let state = first_snapshot.map(|r| {
                            let s: crate::database::json_compat::JsonValue = r.get("state");
                            s.0
                        });
                        Some(ForkBoundary {
                            source_channel_id: parent.clone(),
                            requested_cursor: meta.requested_at_cursor,
                            resolved_cursor: meta.resolved_state_cursor,
                            resolved_event_id: meta.resolved_event_id,
                            source_state_version: meta.source_state_version,
                            source_state_sha256: meta.source_state_sha256.clone(),
                            restorable: meta.restorable,
                            state,
                        })
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        let executions = self
            .list_executions(channel_id, MAX_EXECUTION_LIST_LIMIT)
            .await?;

        Ok(DecisionTrail {
            channel_id: channel_id.to_string(),
            origin_run_id: metadata
                .as_ref()
                .map(|m| m.origin_run_id.clone())
                .unwrap_or_else(|| channel_id.to_string()),
            ancestry,
            boundary,
            executions,
            truncated,
            max_depth: Self::MAX_TREE_DEPTH,
        })
    }

    /// Update or insert channel metadata (display_name, reason, experiment_metadata).
    #[instrument(skip(self, request))]
    pub async fn update_metadata(
        &self,
        request: &UpdateMetadataRequest,
    ) -> Result<ChannelMetadata, EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let exp_meta_json = request
            .experiment_metadata
            .as_ref()
            .map(|v| JsonValue::new(v.clone()));
        // The incoming session status (if this metadata carries the session
        // contract). Guarded to JSON objects so a caller cannot send a scalar
        // or array and reach the key lookups. Only session metadata counts —
        // the SDKs always set `session_name` — so an operator/dashboard
        // metadata edit is never mistaken for a session transition.
        let incoming_meta = request
            .experiment_metadata
            .as_ref()
            .filter(|meta| meta.is_object());
        let incoming_status = incoming_meta
            .and_then(|meta| meta.get("status"))
            .and_then(|s| s.as_str())
            .map(|s| s.to_string());
        let incoming_is_session = incoming_meta
            .map(|meta| meta.get("session_name").is_some())
            .unwrap_or(false);

        let transaction = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;

        // Prior status under the row lock, so a concurrent update cannot race
        // the crash-recovery transition. Also confirm the prior row was
        // session metadata (an operator/dashboard edit must never be mistaken
        // for a session transition).
        let prev_row = transaction
            .query_opt(
                "SELECT experiment_metadata->>'status' AS status,
                        experiment_metadata->>'session_name' AS session_name
                 FROM channel_metadata WHERE channel_id = $1 FOR UPDATE",
                &[&request.channel_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        let prev_status: Option<String> = prev_row
            .as_ref()
            .and_then(|r| r.get::<_, Option<String>>("status"));
        let prev_is_session: bool = prev_row
            .as_ref()
            .and_then(|r| r.get::<_, Option<String>>("session_name"))
            .is_some();

        let row = transaction
            .query_one(
                "INSERT INTO channel_metadata (channel_id, origin_run_id, forked_at_cursor, display_name, reason, experiment_metadata)
                 VALUES ($1, $1, 0, $2, $3, COALESCE($4, '{}'::jsonb))
                 ON CONFLICT (channel_id) DO UPDATE
                 SET display_name = COALESCE($5, channel_metadata.display_name),
                     reason = COALESCE($6, channel_metadata.reason),
                     experiment_metadata = COALESCE($7, channel_metadata.experiment_metadata)
                 RETURNING channel_id, parent_channel_id, origin_run_id, forked_at_cursor,
                           forked_at, display_name, reason, experiment_metadata,
                           requested_at_cursor, resolved_state_cursor, resolved_event_id,
                           source_state_version, source_state_sha256, restorable, manifest,
                           reproducibility, outcome, result_score, deleted_at",
                &[
                    &request.channel_id,
                    &request.display_name.as_deref(),
                    &request.reason.as_deref(),
                    &exp_meta_json,
                    &request.display_name.as_deref(),
                    &request.reason.as_deref(),
                    &exp_meta_json,
                ],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        // Crash recovery completed: a session re-enters `started` on the SAME
        // channel from an in-progress or crashed state. This is the
        // server-observable recovery signal shared by every SDK that writes
        // the session metadata contract — no SDK-side telemetry involved.
        //
        // The prior state is one of:
        //   * `crashed`  — a caught exception ran the SDK's crash handler;
        //   * `stepping` — the process was hard-killed mid-run (SIGKILL /
        //      os._exit) after at least one step (all SDKs persist this).
        // A brand-new session on a fresh channel (prior `NULL`), a session
        // killed before any step (`started`), or one reusing a cleanly
        // `completed` channel is NOT counted — and deliberately so: a retried
        // `started` write is indistinguishable from those, so counting
        // `started -> started` would double-count under retry.
        let interrupted = matches!(prev_status.as_deref(), Some("crashed") | Some("stepping"));
        if incoming_is_session
            && prev_is_session
            && interrupted
            && incoming_status.as_deref() == Some("started")
        {
            let now = Utc::now();
            transaction
                .execute(
                    "INSERT INTO usage_delivery_outbox
                     (idempotency_key, usage_date, metric, quantity, source_generation)
                     VALUES ($1, $2::date, 'recovery_operations', 1, 1)
                     ON CONFLICT (idempotency_key) DO NOTHING",
                    &[
                        &format!("recovery:{}:{}", request.channel_id, now.timestamp_millis()),
                        &now.date_naive(),
                    ],
                )
                .await
                .map_err(EventStoreError::Execute)?;
            crate::metrics::basic_metrics::RECOVERIES_TOTAL.inc();
        }

        transaction
            .commit()
            .await
            .map_err(EventStoreError::Execute)?;

        Ok(row_to_metadata(&row))
    }

    /// List state versions for a channel (cursor-aligned metadata, no state blobs).
    #[instrument(skip(self))]
    pub async fn list_states(
        &self,
        channel_id: &str,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<super::schema::StateVersion>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let limit = limit.unwrap_or(100).min(1000);
        let offset = offset.unwrap_or(0);
        let rows = client
            .query(
                "SELECT version, cursor, timestamp FROM channel_states
                 WHERE channel_id = $1
                 ORDER BY version DESC
                 LIMIT $2 OFFSET $3",
                &[&channel_id, &limit, &offset],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        let versions: Vec<super::schema::StateVersion> = rows
            .iter()
            .map(|r| super::schema::StateVersion {
                version: r.get("version"),
                cursor: r.get("cursor"),
                timestamp: r.get("timestamp"),
            })
            .collect();
        Ok(versions)
    }

    /// Delete a specific state version.
    /// Returns the number of rows deleted (0 or 1).
    #[instrument(skip(self))]
    pub async fn delete_state_version(
        &self,
        channel_id: &str,
        version: i64,
    ) -> Result<u64, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let count = client
            .execute(
                "DELETE FROM channel_states WHERE channel_id = $1 AND version = $2",
                &[&channel_id, &version],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(count)
    }

    // ── Distributed execution groups ────────────────────────────────

    pub async fn create_execution_group(
        &self,
        group_id: &str,
        metadata: Option<&sonic_rs::Value>,
    ) -> Result<ExecutionGroup, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let metadata = metadata.map(|v| JsonValue::new(v.clone()));
        let row = client.query_one(
            "INSERT INTO execution_groups (group_id, metadata) VALUES ($1, COALESCE($2, '{}'::jsonb))
             ON CONFLICT (group_id) DO UPDATE SET updated_at = execution_groups.updated_at
             RETURNING group_id, status, metadata, created_at, updated_at",
            &[&group_id, &metadata],
        ).await.map_err(EventStoreError::Execute)?;
        Ok(row_to_execution_group(&row))
    }

    pub async fn get_execution_group(
        &self,
        group_id: &str,
    ) -> Result<ExecutionGroup, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let row = client.query_opt(
            "SELECT group_id, status, metadata, created_at, updated_at FROM execution_groups WHERE group_id = $1",
            &[&group_id],
        ).await.map_err(EventStoreError::Execute)?;
        row.as_ref()
            .map(row_to_execution_group)
            .ok_or_else(|| EventStoreError::ExecutionGroupNotFound(group_id.to_string()))
    }

    pub async fn list_execution_groups(&self) -> Result<Vec<ExecutionGroup>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let rows = client.query(
            "SELECT group_id, status, metadata, created_at, updated_at FROM execution_groups ORDER BY created_at DESC", &[]
        ).await.map_err(EventStoreError::Execute)?;
        Ok(rows.iter().map(row_to_execution_group).collect())
    }

    pub async fn add_execution_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        channel_id: &str,
        role: Option<&str>,
        metadata: Option<&sonic_rs::Value>,
    ) -> Result<ExecutionGroupMember, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let metadata = metadata.map(|v| JsonValue::new(v.clone()));
        let row = client.query_one(
            "INSERT INTO execution_group_members (group_id, member_id, channel_id, role, metadata)
             VALUES ($1, $2, $3, $4, COALESCE($5, '{}'::jsonb))
             ON CONFLICT (group_id, member_id) DO UPDATE
               SET channel_id = EXCLUDED.channel_id, role = EXCLUDED.role,
                   metadata = EXCLUDED.metadata, updated_at = NOW()
             RETURNING group_id, member_id, channel_id, role, metadata, owner_id, generation, lease_until, created_at, updated_at",
            &[&group_id, &member_id, &channel_id, &role, &metadata],
        ).await.map_err(EventStoreError::Execute)?;
        Ok(row_to_execution_group_member(&row))
    }

    pub async fn list_execution_group_members(
        &self,
        group_id: &str,
    ) -> Result<Vec<ExecutionGroupMember>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let rows = client.query(
            "SELECT group_id, member_id, channel_id, role, metadata, owner_id, generation, lease_until, created_at, updated_at
             FROM execution_group_members WHERE group_id = $1 ORDER BY member_id", &[&group_id]
        ).await.map_err(EventStoreError::Execute)?;
        if rows.is_empty()
            && client
                .query_opt(
                    "SELECT 1 FROM execution_groups WHERE group_id = $1",
                    &[&group_id],
                )
                .await
                .map_err(EventStoreError::Execute)?
                .is_none()
        {
            return Err(EventStoreError::ExecutionGroupNotFound(
                group_id.to_string(),
            ));
        }
        Ok(rows.iter().map(row_to_execution_group_member).collect())
    }

    pub async fn claim_execution_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        owner_id: &str,
        lease_seconds: i64,
    ) -> Result<MemberLease, EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let tx = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;
        let seconds = lease_seconds.clamp(1, 86_400);
        let row = tx.query_opt(
            "SELECT owner_id, generation, lease_until FROM execution_group_members WHERE group_id = $1 AND member_id = $2 FOR UPDATE",
            &[&group_id, &member_id],
        ).await.map_err(EventStoreError::Execute)?;
        let row = row.ok_or_else(|| EventStoreError::ExecutionGroupMemberNotFound {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
        })?;
        let current_owner: Option<String> = row.get("owner_id");
        let generation: i64 = row.get("generation");
        let lease_until: Option<DateTime<Utc>> = row.get("lease_until");
        if current_owner.as_deref() != Some(owner_id)
            && lease_until.is_some_and(|until| until > Utc::now())
        {
            return Err(EventStoreError::ExecutionGroupMemberClaimed {
                group_id: group_id.into(),
                member_id: member_id.into(),
            });
        }
        let next_generation = if current_owner.as_deref() == Some(owner_id) {
            generation
        } else {
            generation + 1
        };
        let lease = tx
            .query_one(
                "UPDATE execution_group_members SET owner_id = $3, generation = $4,
                 lease_until = NOW() + ($5::bigint * INTERVAL '1 second'), updated_at = NOW()
             WHERE group_id = $1 AND member_id = $2
             RETURNING lease_until",
                &[&group_id, &member_id, &owner_id, &next_generation, &seconds],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        let lease_until: DateTime<Utc> = lease.get("lease_until");
        tx.commit().await.map_err(EventStoreError::Execute)?;
        Ok(MemberLease {
            group_id: group_id.into(),
            member_id: member_id.into(),
            owner_id: owner_id.into(),
            generation: next_generation,
            lease_until,
        })
    }

    pub async fn heartbeat_execution_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        owner_id: &str,
        generation: i64,
        lease_seconds: i64,
    ) -> Result<MemberLease, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let seconds = lease_seconds.clamp(1, 86_400);
        let row = client.query_opt(
            "UPDATE execution_group_members SET lease_until = NOW() + ($5::bigint * INTERVAL '1 second'), updated_at = NOW()
             WHERE group_id = $1 AND member_id = $2 AND owner_id = $3 AND generation = $4 AND lease_until > NOW()
             RETURNING lease_until",
            &[&group_id, &member_id, &owner_id, &generation, &seconds],
        ).await.map_err(EventStoreError::Execute)?;
        let row = row.ok_or_else(|| EventStoreError::ExecutionGroupMemberFenced {
            group_id: group_id.into(),
            member_id: member_id.into(),
        })?;
        Ok(MemberLease {
            group_id: group_id.into(),
            member_id: member_id.into(),
            owner_id: owner_id.into(),
            generation,
            lease_until: row.get("lease_until"),
        })
    }

    pub async fn release_execution_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        owner_id: &str,
        generation: i64,
    ) -> Result<(), EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let changed=client.execute("UPDATE execution_group_members SET owner_id=NULL,lease_until=NULL,updated_at=NOW() WHERE group_id=$1 AND member_id=$2 AND owner_id=$3 AND generation=$4 AND lease_until>NOW()", &[&group_id,&member_id,&owner_id,&generation]).await.map_err(EventStoreError::Execute)?;
        if changed == 0 {
            return Err(EventStoreError::ExecutionGroupMemberFenced {
                group_id: group_id.into(),
                member_id: member_id.into(),
            });
        }
        Ok(())
    }

    /// Atomically records the sender's intent, recipient delivery, and the
    /// durable message envelope.  The delivery event causally depends on the
    /// intent event, so ordinary replay/WS delivery remains the sole transport.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_execution_group_message(
        &self,
        group_id: &str,
        from_member_id: &str,
        to_member_id: &str,
        message_type: &str,
        payload: &sonic_rs::Value,
        causal_context: Option<&sonic_rs::Value>,
        operation_id: Option<Uuid>,
    ) -> Result<(GroupMessage, AgentEvent, AgentEvent), EventStoreError> {
        if from_member_id == to_member_id {
            return Err(EventStoreError::InvalidRequest(
                "a group message must target a different member".into(),
            ));
        }
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let mut tx = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;
        let group = tx
            .query_opt(
                "SELECT status FROM execution_groups WHERE group_id = $1 FOR UPDATE",
                &[&group_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        match group {
            None => return Err(EventStoreError::ExecutionGroupNotFound(group_id.into())),
            Some(row) if row.get::<_, String>("status") != "active" => {
                return Err(EventStoreError::InvalidRequest(
                    "execution group is closed".into(),
                ))
            }
            _ => {}
        }
        if let Some(op) = operation_id {
            if let Some(row) = tx.query_opt(
                "SELECT message_id, group_id, from_member_id, to_member_id, message_type, payload, causal_context,
                        source_event_id, delivery_event_id, status, acknowledged_at, acknowledged_by, created_at
                 FROM execution_group_messages WHERE group_id = $1 AND operation_id = $2", &[&group_id, &op]
            ).await.map_err(EventStoreError::Execute)? {
                let message = row_to_group_message(&row);
                // jsonb does not preserve object key order, so the stored
                // envelope's payload/causal_context must be compared
                // canonically (key-order-insensitive) — otherwise a retry
                // that re-serializes its payload with a different key order
                // would spuriously fail idempotency.
                let causal_matches = match (&message.causal_context, causal_context) {
                    (Some(left), Some(right)) => json_values_equal(left, right),
                    (None, None) => true,
                    _ => false,
                };
                if message.from_member_id != from_member_id || message.to_member_id != to_member_id || message.message_type != message_type || !json_values_equal(&message.payload, payload) || !causal_matches {
                    return Err(EventStoreError::IdempotencyConflict { operation_id: op.to_string() });
                }
                let source = event_by_id(&tx, message.source_event_id).await?;
                let delivery = event_by_id(&tx, message.delivery_event_id).await?;
                tx.commit().await.map_err(EventStoreError::Execute)?;
                return Ok((message, source, delivery));
            }
        }
        let members = tx
            .query(
                "SELECT member_id, channel_id FROM execution_group_members
             WHERE group_id = $1 AND member_id = ANY($2) FOR UPDATE",
                &[&group_id, &vec![from_member_id, to_member_id]],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        if members.len() != 2 {
            return Err(EventStoreError::ExecutionGroupMemberNotFound {
                group_id: group_id.into(),
                member_id: "sender or recipient".into(),
            });
        }
        let source_channel: String = members
            .iter()
            .find(|r| r.get::<_, String>("member_id") == from_member_id)
            .expect("two distinct members")
            .get("channel_id");
        let delivery_channel: String = members
            .iter()
            .find(|r| r.get::<_, String>("member_id") == to_member_id)
            .expect("two distinct members")
            .get("channel_id");
        let message_id = Uuid::new_v4();
        let now = Utc::now();
        let envelope = sonic_rs::json!({
            "message_id": message_id.to_string(), "group_id": group_id,
            "from": {"member": from_member_id, "channel": source_channel},
            "to": {"member": to_member_id, "channel": delivery_channel},
            "type": message_type, "payload": payload, "causal_context": causal_context,
        });
        let source_metadata = EventMetadata {
            actor: format!("group:{from_member_id}"),
            agent_id: Some(from_member_id.into()),
            user_id: None,
            metadata: Some(
                sonic_rs::json!({"group_id": group_id, "message_id": message_id.to_string(), "direction": "outbound"}),
            ),
        };
        let (source_id, source_cursor) = insert_event_in_tx(
            &mut tx,
            &source_channel,
            "message.sent",
            &envelope,
            &source_metadata,
            None,
            None,
            now,
        )
        .await?;
        let delivery_metadata = EventMetadata {
            actor: format!("group:{to_member_id}"),
            agent_id: Some(to_member_id.into()),
            user_id: None,
            metadata: Some(
                sonic_rs::json!({"group_id": group_id, "message_id": message_id.to_string(), "direction": "inbound"}),
            ),
        };
        let (delivery_id, delivery_cursor) = insert_event_in_tx(
            &mut tx,
            &delivery_channel,
            "message.received",
            &envelope,
            &delivery_metadata,
            Some(source_id),
            None,
            now,
        )
        .await?;
        tx.execute("INSERT INTO event_dependencies (event_id, depends_on_event_id, dependency_type) VALUES ($1, $2, 'message') ON CONFLICT DO NOTHING", &[&delivery_id, &source_id]).await.map_err(EventStoreError::Execute)?;
        let payload_json = JsonValue::new(payload.clone());
        let context_json = causal_context.map(|v| JsonValue::new(v.clone()));
        let row = tx.query_one(
            "INSERT INTO execution_group_messages (message_id, group_id, from_member_id, to_member_id, message_type, payload, causal_context, operation_id, source_event_id, delivery_event_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
             RETURNING message_id, group_id, from_member_id, to_member_id, message_type, payload, causal_context, source_event_id, delivery_event_id, status, acknowledged_at, acknowledged_by, created_at",
            &[&message_id, &group_id, &from_member_id, &to_member_id, &message_type, &payload_json, &context_json, &operation_id, &source_id, &delivery_id],
        ).await.map_err(EventStoreError::Execute)?;
        tx.commit().await.map_err(EventStoreError::Execute)?;
        let message = row_to_group_message(&row);
        let source = AgentEvent {
            id: source_id,
            channel_id: source_channel,
            event_type: "message.sent".into(),
            payload: envelope.clone(),
            metadata: source_metadata,
            cursor: source_cursor,
            depends_on: None,
            timestamp: now,
        };
        let delivery = AgentEvent {
            id: delivery_id,
            channel_id: delivery_channel,
            event_type: "message.received".into(),
            payload: envelope,
            metadata: delivery_metadata,
            cursor: delivery_cursor,
            depends_on: Some(source_id),
            timestamp: now,
        };
        Ok((message, source, delivery))
    }

    pub async fn list_execution_group_messages(
        &self,
        group_id: &str,
        member_id: &str,
        after: Option<Uuid>,
        limit: i64,
    ) -> Result<Vec<GroupMessage>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let limit = limit.clamp(1, 1000);
        let rows = client.query(
            "SELECT message_id, group_id, from_member_id, to_member_id, message_type, payload, causal_context,
                    source_event_id, delivery_event_id, status, acknowledged_at, acknowledged_by, created_at
             FROM execution_group_messages WHERE group_id = $1 AND to_member_id = $2
               AND ($3::uuid IS NULL OR created_at > (SELECT created_at FROM execution_group_messages WHERE message_id = $3))
             ORDER BY created_at, message_id LIMIT $4", &[&group_id, &member_id, &after, &limit]
        ).await.map_err(EventStoreError::Execute)?;
        Ok(rows.iter().map(row_to_group_message).collect())
    }

    pub async fn acknowledge_execution_group_message(
        &self,
        message_id: Uuid,
        owner_id: &str,
        generation: i64,
    ) -> Result<GroupMessage, EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let tx = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;
        let row = tx.query_opt(
            "SELECT m.message_id, m.group_id, m.from_member_id, m.to_member_id, m.message_type, m.payload, m.causal_context,
                    m.source_event_id, m.delivery_event_id, m.status, m.acknowledged_at, m.acknowledged_by, m.created_at
             FROM execution_group_messages m JOIN execution_group_members x ON x.group_id=m.group_id AND x.member_id=m.to_member_id
             WHERE m.message_id=$1 AND x.owner_id=$2 AND x.generation=$3 AND x.lease_until > NOW() FOR UPDATE",
            &[&message_id, &owner_id, &generation],
        ).await.map_err(EventStoreError::Execute)?;
        let existing = row.ok_or_else(|| EventStoreError::ExecutionGroupMemberFenced {
            group_id: "unknown".into(),
            member_id: "recipient".into(),
        })?;
        let message = row_to_group_message(&existing);
        let row = tx.query_one(
            "UPDATE execution_group_messages SET status='acknowledged', acknowledged_at=COALESCE(acknowledged_at,NOW()), acknowledged_by=COALESCE(acknowledged_by,$2)
             WHERE message_id=$1 RETURNING message_id, group_id, from_member_id, to_member_id, message_type, payload, causal_context, source_event_id, delivery_event_id, status, acknowledged_at, acknowledged_by, created_at",
            &[&message_id, &owner_id],
        ).await.map_err(EventStoreError::Execute)?;
        tx.commit().await.map_err(EventStoreError::Execute)?;
        let updated = row_to_group_message(&row);
        debug_assert_eq!(message.message_id, updated.message_id);
        Ok(updated)
    }

    /// Return bounded transitive causal ancestry directly from the persisted
    /// dependency DAG, including cross-channel message edges.
    pub async fn causal_graph(
        &self,
        root_event_id: Uuid,
        max_nodes: i64,
    ) -> Result<CausalGraph, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let cap = max_nodes.clamp(1, 5_000);
        let rows = client.query(
            "WITH RECURSIVE walk(event_id, depth, path) AS (
                SELECT $1::uuid, 0, ARRAY[$1::uuid]
                UNION ALL
                SELECT d.depends_on_event_id, walk.depth + 1, walk.path || d.depends_on_event_id
                FROM event_dependencies d JOIN walk ON d.event_id = walk.event_id
                WHERE walk.depth < 128 AND NOT d.depends_on_event_id = ANY(walk.path)
             ), nodes AS (SELECT DISTINCT event_id FROM walk LIMIT $2)
             SELECT e.id, e.channel_id, e.event_type, e.payload, e.actor, e.agent_id, e.user_id, e.metadata, e.cursor, e.timestamp, e.depends_on
             FROM agent_events e JOIN nodes n ON n.event_id=e.id ORDER BY e.timestamp, e.id",
            &[&root_event_id, &cap],
        ).await.map_err(EventStoreError::Execute)?;
        if rows.is_empty() {
            return Err(EventStoreError::NotFound);
        }
        let events: Vec<AgentEvent> = rows.iter().map(row_to_event).collect();
        let ids: Vec<Uuid> = events.iter().map(|event| event.id).collect();
        let edge_rows = client.query(
            "SELECT event_id, depends_on_event_id, dependency_type FROM event_dependencies WHERE event_id = ANY($1)",
            &[&ids],
        ).await.map_err(EventStoreError::Execute)?;
        let dependencies = edge_rows
            .into_iter()
            .map(|row| EventDependency {
                event_id: row.get("event_id"),
                depends_on_event_id: row.get("depends_on_event_id"),
                dependency_type: row.get("dependency_type"),
            })
            .collect();
        Ok(CausalGraph {
            root_event_id,
            truncated: events.len() as i64 >= cap,
            events,
            dependencies,
        })
    }

    pub async fn create_group_fork(
        &self,
        fork_group_id: &str,
        source_group_id: &str,
        intervention_member_id: &str,
        intervention_cursor: i64,
        member_policies: &sonic_rs::Value,
        tool_policies: &sonic_rs::Value,
    ) -> Result<GroupForkReceipt, EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let mut tx = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;
        tx.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            &[&fork_group_id],
        )
        .await
        .map_err(EventStoreError::Execute)?;
        if let Some(existing) = tx.query_opt("SELECT fork_group_id, source_group_id, intervention_member_id, intervention_cursor, member_policies, tool_policies, status, first_divergence_event_id, receipt, created_at, updated_at FROM execution_group_forks WHERE fork_group_id=$1", &[&fork_group_id]).await.map_err(EventStoreError::Execute)? {
            let receipt = row_to_group_fork_receipt(&existing);
            // jsonb normalizes object key order, so policy equality must be
            // canonical (key-order-insensitive) — re-creating a fork with the
            // same policies in a different key order is an idempotent replay,
            // not a conflict.
            if receipt.source_group_id == source_group_id && receipt.intervention_member_id == intervention_member_id && receipt.intervention_cursor == intervention_cursor && receipt.member_policies.as_ref().is_some_and(|v| json_values_equal(v, member_policies)) && receipt.tool_policies.as_ref().is_some_and(|v| json_values_equal(v, tool_policies)) { tx.commit().await.map_err(EventStoreError::Execute)?; return Ok(receipt); }
            return Err(EventStoreError::ChannelConflict { channel_id: fork_group_id.into() });
        }
        let members = JsonValue::new(member_policies.clone());
        let tools = JsonValue::new(tool_policies.clone());
        let source_members=tx.query("SELECT member_id,channel_id,role,metadata FROM execution_group_members WHERE group_id=$1 ORDER BY member_id FOR SHARE", &[&source_group_id]).await.map_err(EventStoreError::Execute)?;
        if source_members.is_empty() {
            return Err(EventStoreError::ExecutionGroupNotFound(
                source_group_id.into(),
            ));
        }
        tx.execute("INSERT INTO execution_groups (group_id,status,metadata) VALUES ($1,'active',jsonb_build_object('forked_from',$2::text))", &[&fork_group_id,&source_group_id]).await.map_err(EventStoreError::Execute)?;
        for source in &source_members {
            let member_id: String = source.get("member_id");
            let channel_id = format!("{fork_group_id}:{member_id}");
            if channel_id.len() > 256 {
                return Err(EventStoreError::InvalidRequest(
                    "derived fork channel exceeds 256 bytes".into(),
                ));
            }
            let role: Option<String> = source.get("role");
            let metadata: JsonValue = source.get("metadata");
            tx.execute("INSERT INTO execution_group_members (group_id,member_id,channel_id,role,metadata) VALUES ($1,$2,$3,$4,$5)",&[&fork_group_id,&member_id,&channel_id,&role,&metadata]).await.map_err(EventStoreError::Execute)?;
        }
        let initial_receipt = JsonValue::new(
            sonic_rs::json!({"replayed_events":[],"invalidated_events":[],"promoted_members":[],"blocked_boundaries":[],"tool_results_replayed":[],"live_tool_executions":[],"live_events":[]}),
        );
        tx.execute("INSERT INTO execution_group_forks (fork_group_id, source_group_id, intervention_member_id, intervention_cursor, member_policies, tool_policies, receipt) VALUES ($1,$2,$3,$4,$5,$6,$7)", &[&fork_group_id,&source_group_id,&intervention_member_id,&intervention_cursor,&members,&tools,&initial_receipt]).await.map_err(EventStoreError::Execute)?;

        let recorded=tx.query("SELECT m.message_id,m.from_member_id,m.message_type,m.payload,m.causal_context,m.source_event_id,m.delivery_event_id FROM execution_group_messages m JOIN agent_events e ON e.id=m.delivery_event_id WHERE m.group_id=$1 AND m.to_member_id=$2 AND e.cursor <= $3 ORDER BY e.cursor", &[&source_group_id,&intervention_member_id,&intervention_cursor]).await.map_err(EventStoreError::Execute)?;
        let live_channel = format!("{fork_group_id}:{intervention_member_id}");
        let mut replayed_events: Vec<String> = Vec::new();
        for source in recorded {
            let original_message: Uuid = source.get("message_id");
            let from_member: String = source.get("from_member_id");
            let message_type: String = source.get("message_type");
            let payload: JsonValue = source.get("payload");
            let causal: Option<JsonValue> = source.get("causal_context");
            let original_source: Uuid = source.get("source_event_id");
            let original_delivery: Uuid = source.get("delivery_event_id");
            let sender_channel = format!("{fork_group_id}:{from_member}");
            let message_id = Uuid::new_v4();
            let now = Utc::now();
            let envelope = sonic_rs::json!({"message_id":message_id.to_string(),"source_message_id":original_message.to_string(),"group_id":fork_group_id,"from":{"member":from_member,"channel":sender_channel},"to":{"member":intervention_member_id,"channel":live_channel},"type":message_type,"payload":payload.0,"causal_context":causal.as_ref().map(|value|value.0.clone()),"replayed":true});
            let meta = EventMetadata {
                actor: "group-fork-replay".into(),
                agent_id: Some(from_member.clone()),
                user_id: None,
                metadata: Some(
                    sonic_rs::json!({"fork_group_id":fork_group_id,"source_message_id":original_message.to_string()}),
                ),
            };
            let (sender_event, _) = insert_event_in_tx(
                &mut tx,
                &sender_channel,
                "message.replayed",
                &envelope,
                &meta,
                Some(original_source),
                None,
                now,
            )
            .await?;
            insert_dependency_edges(&tx, sender_event, Some(original_source), &[]).await?;
            let delivery_meta = EventMetadata {
                actor: "group-fork-replay".into(),
                agent_id: Some(intervention_member_id.into()),
                user_id: None,
                metadata: meta.metadata.clone(),
            };
            let (delivery_event, _) = insert_event_in_tx(
                &mut tx,
                &live_channel,
                "message.received",
                &envelope,
                &delivery_meta,
                Some(sender_event),
                None,
                now,
            )
            .await?;
            insert_dependency_edges(
                &tx,
                delivery_event,
                Some(sender_event),
                &[super::schema::EventDependencyRequest {
                    event_id: original_delivery,
                    dependency_type: "replay_source".into(),
                }],
            )
            .await?;
            tx.execute("INSERT INTO execution_group_messages (message_id,group_id,from_member_id,to_member_id,message_type,payload,causal_context,source_event_id,delivery_event_id,status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'delivered')", &[&message_id,&fork_group_id,&from_member,&intervention_member_id,&message_type,&payload,&causal,&sender_event,&delivery_event]).await.map_err(EventStoreError::Execute)?;
            replayed_events.push(sender_event.to_string());
            replayed_events.push(delivery_event.to_string());
        }
        let source_intervention_channel = source_members
            .iter()
            .find_map(|member| {
                let id: String = member.get("member_id");
                (id == intervention_member_id).then(|| member.get::<_, String>("channel_id"))
            })
            .ok_or_else(|| EventStoreError::ExecutionGroupMemberNotFound {
                group_id: source_group_id.into(),
                member_id: intervention_member_id.into(),
            })?;
        if let Some((_version, _source_cursor, state)) =
            resolve_fork_state(&tx, &source_intervention_channel, intervention_cursor).await?
        {
            let target_cursor: i64=tx.query_one("SELECT COALESCE(MAX(cursor),0) AS cursor FROM agent_events WHERE channel_id=$1", &[&live_channel]).await.map_err(EventStoreError::Execute)?.get("cursor");
            tx.execute("INSERT INTO channel_states (channel_id,cursor,state,timestamp,write_source) VALUES ($1,$2,$3,NOW(),'fork')", &[&live_channel,&target_cursor,&state]).await.map_err(EventStoreError::Execute)?;
        }
        let replayed = JsonValue::new(sonic_rs::json!(replayed_events));
        let row=tx.query_one("UPDATE execution_group_forks SET receipt=jsonb_set(receipt,'{replayed_events}',$2::jsonb,true),updated_at=NOW() WHERE fork_group_id=$1 RETURNING fork_group_id, source_group_id, intervention_member_id, intervention_cursor, member_policies, tool_policies, status, first_divergence_event_id, receipt, created_at, updated_at", &[&fork_group_id,&replayed]).await.map_err(EventStoreError::Execute)?;
        let receipt = row_to_group_fork_receipt(&row);
        // Meter the group fork as a fork operation (the replay path above
        // returns early, so this fires once per fresh group fork). Idempotent
        // by fork_group_id; delivered only from a linked instance.
        let now = Utc::now();
        tx.execute(
            "INSERT INTO usage_delivery_outbox
             (idempotency_key, usage_date, metric, quantity, source_generation)
             VALUES ($1, $2::date, 'fork_operations', 1, 1)
             ON CONFLICT (idempotency_key) DO NOTHING",
            &[&format!("groupfork:{fork_group_id}"), &now.date_naive()],
        )
        .await
        .map_err(EventStoreError::Execute)?;
        tx.commit().await.map_err(EventStoreError::Execute)?;
        crate::metrics::basic_metrics::FORKS_TOTAL.inc();
        Ok(receipt)
    }
    pub async fn get_group_fork(
        &self,
        fork_group_id: &str,
    ) -> Result<GroupForkReceipt, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let row=client.query_opt("SELECT fork_group_id, source_group_id, intervention_member_id, intervention_cursor, member_policies, tool_policies, status, first_divergence_event_id, receipt, created_at, updated_at FROM execution_group_forks WHERE fork_group_id=$1", &[&fork_group_id]).await.map_err(EventStoreError::Execute)?;
        row.as_ref()
            .map(row_to_group_fork_receipt)
            .ok_or(EventStoreError::NotFound)
    }

    /// Return the effective member policy when `group_id` is a counterfactual.
    /// Ordinary execution groups deliberately return `None`.
    pub async fn group_fork_member_policy(
        &self,
        group_id: &str,
        member_id: &str,
    ) -> Result<Option<String>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let row = client
            .query_opt(
                "SELECT member_policies ->> $2 AS policy FROM execution_group_forks WHERE fork_group_id=$1",
                &[&group_id, &member_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(row.and_then(|row| row.get::<_, Option<String>>("policy")))
    }

    /// Persist a strict frozen boundary. The receipt is append-only at the
    /// logical level: every observation is appended to its classification
    /// array and source events remain immutable.
    pub async fn block_group_fork(
        &self,
        fork_group_id: &str,
        boundary: &str,
        detail: &sonic_rs::Value,
    ) -> Result<(), EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let detail = JsonValue::new(sonic_rs::json!({
            "boundary": boundary,
            "detail": detail,
            "at": Utc::now().to_rfc3339(),
        }));
        let changed = client.execute(
            "UPDATE execution_group_forks SET status='blocked', receipt=jsonb_set(COALESCE(receipt,'{}'::jsonb), '{blocked_boundaries}', COALESCE(receipt->'blocked_boundaries','[]'::jsonb) || jsonb_build_array($2::jsonb), true), updated_at=NOW() WHERE fork_group_id=$1",
            &[&fork_group_id, &detail],
        ).await.map_err(EventStoreError::Execute)?;
        if changed == 0 {
            return Err(EventStoreError::NotFound);
        }
        Ok(())
    }

    /// Promote a frozen member after an explicit operator decision. This is
    /// the only transition out of frozen; live members cannot be demoted and
    /// source policy is never rewritten.
    pub async fn promote_group_fork_member(
        &self,
        fork_group_id: &str,
        member_id: &str,
    ) -> Result<GroupForkReceipt, EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let tx = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;
        let policy = tx.query_opt(
            "SELECT member_policies ->> $2 AS policy FROM execution_group_forks WHERE fork_group_id=$1 FOR UPDATE",
            &[&fork_group_id, &member_id],
        ).await.map_err(EventStoreError::Execute)?
            .ok_or(EventStoreError::NotFound)?
            .get::<_, Option<String>>("policy");
        match policy.as_deref() {
            Some("frozen") => {}
            Some("reactive") => {
                let row=tx.query_one("SELECT fork_group_id, source_group_id, intervention_member_id, intervention_cursor, member_policies, tool_policies, status, first_divergence_event_id, receipt, created_at, updated_at FROM execution_group_forks WHERE fork_group_id=$1", &[&fork_group_id]).await.map_err(EventStoreError::Execute)?;
                tx.commit().await.map_err(EventStoreError::Execute)?;
                return Ok(row_to_group_fork_receipt(&row));
            }
            Some(other) => {
                return Err(EventStoreError::InvalidRequest(format!(
                    "member '{member_id}' has policy '{other}' and cannot be promoted"
                )))
            }
            None => {
                return Err(EventStoreError::ExecutionGroupMemberNotFound {
                    group_id: fork_group_id.into(),
                    member_id: member_id.into(),
                })
            }
        }
        let promoted = JsonValue::new(
            sonic_rs::json!({"member_id":member_id,"from":"frozen","to":"reactive","at":Utc::now().to_rfc3339()}),
        );
        let path = vec![member_id.to_string()];
        let row=tx.query_one(
            "UPDATE execution_group_forks SET member_policies=jsonb_set(member_policies,$2,'\"reactive\"'::jsonb,false), status='active', receipt=jsonb_set(COALESCE(receipt,'{}'::jsonb),'{promoted_members}',COALESCE(receipt->'promoted_members','[]'::jsonb)||jsonb_build_array($3::jsonb),true), updated_at=NOW() WHERE fork_group_id=$1 RETURNING fork_group_id, source_group_id, intervention_member_id, intervention_cursor, member_policies, tool_policies, status, first_divergence_event_id, receipt, created_at, updated_at",
            &[&fork_group_id,&path,&promoted],
        ).await.map_err(EventStoreError::Execute)?;
        tx.commit().await.map_err(EventStoreError::Execute)?;
        Ok(row_to_group_fork_receipt(&row))
    }

    pub async fn record_group_fork_divergence(
        &self,
        fork_group_id: &str,
        event_id: Uuid,
        member_id: &str,
    ) -> Result<(), EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let tx = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;
        let context = tx.query_opt("SELECT f.source_group_id,f.intervention_cursor,sm.channel_id FROM execution_group_forks f JOIN execution_group_members sm ON sm.group_id=f.source_group_id AND sm.member_id=$2 WHERE f.fork_group_id=$1 FOR SHARE", &[&fork_group_id,&member_id]).await.map_err(EventStoreError::Execute)?;
        let Some(context) = context else {
            return Err(EventStoreError::NotFound);
        };
        let _source_group: String = context.get("source_group_id");
        let boundary: i64 = context.get("intervention_cursor");
        let source_channel: String = context.get("channel_id");
        let invalidated:JsonValue=tx.query_one("WITH RECURSIVE seeds AS (SELECT e.id FROM agent_events e WHERE e.channel_id=$1 AND e.cursor>$2), cone(event_id) AS (SELECT id FROM seeds UNION SELECT d.event_id FROM event_dependencies d JOIN cone c ON c.event_id=d.depends_on_event_id) SELECT COALESCE(jsonb_agg(to_jsonb(event_id::text) ORDER BY event_id),'[]'::jsonb) AS ids FROM cone", &[&source_channel,&boundary]).await.map_err(EventStoreError::Execute)?.get("ids");
        let entry = JsonValue::new(
            sonic_rs::json!({"event_id":event_id.to_string(),"member_id":member_id,"classification":"live","at":Utc::now().to_rfc3339()}),
        );
        tx.execute(
            "UPDATE execution_group_forks SET first_divergence_event_id=COALESCE(first_divergence_event_id,$2), receipt=jsonb_set(jsonb_set(COALESCE(receipt,'{}'::jsonb),'{live_events}',COALESCE(receipt->'live_events','[]'::jsonb)||jsonb_build_array($3::jsonb),true),'{invalidated_events}',COALESCE(receipt->'invalidated_events','[]'::jsonb)||$4::jsonb,true), updated_at=NOW() WHERE fork_group_id=$1",
            &[&fork_group_id,&event_id,&entry,&invalidated],
        ).await.map_err(EventStoreError::Execute)?;
        tx.commit().await.map_err(EventStoreError::Execute)?;
        Ok(())
    }

    pub async fn record_group_fork_live_tool(
        &self,
        fork_group_id: &str,
        execution: &ExecutionInfo,
    ) -> Result<(), EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let entry = JsonValue::new(
            sonic_rs::json!({"execution_id":execution.id.to_string(),"tool_name":execution.tool_name,"key_name":execution.key_name,"status":execution.status,"at":Utc::now().to_rfc3339()}),
        );
        client.execute("UPDATE execution_group_forks SET receipt=jsonb_set(COALESCE(receipt,'{}'::jsonb),'{live_tool_executions}',COALESCE(receipt->'live_tool_executions','[]'::jsonb)||jsonb_build_array($2::jsonb),true),updated_at=NOW() WHERE fork_group_id=$1", &[&fork_group_id,&entry]).await.map_err(EventStoreError::Execute)?;
        Ok(())
    }

    // ── Durable Consumer Groups (P6) ────────────────────────────────

    /// Create a durable consumer group bound to a channel.
    pub async fn create_group(
        &self,
        group_id: &str,
        channel_id: &str,
        metadata: Option<&sonic_rs::Value>,
    ) -> Result<GroupInfo, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let metadata_json = metadata.map(|v| JsonValue::new(v.clone()));
        let row = client
            .query_one(
                "INSERT INTO consumer_groups (group_id, channel_id, metadata)
                 VALUES ($1, $2, COALESCE($3, '{}'::jsonb))
                 ON CONFLICT (group_id) DO UPDATE SET channel_id = EXCLUDED.channel_id
                 RETURNING group_id, channel_id, created_at, metadata",
                &[&group_id, &channel_id, &metadata_json],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(row_to_group(&row))
    }

    /// List consumer groups, optionally filtered by channel.
    pub async fn list_groups(
        &self,
        channel_id: Option<&str>,
    ) -> Result<Vec<GroupInfo>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let rows = match channel_id {
            Some(ch) => {
                client
                    .query(
                        "SELECT group_id, channel_id, created_at, metadata
                         FROM consumer_groups WHERE channel_id = $1 ORDER BY created_at",
                        &[&ch],
                    )
                    .await
            }
            None => {
                client
                    .query(
                        "SELECT group_id, channel_id, created_at, metadata
                         FROM consumer_groups ORDER BY created_at",
                        &[],
                    )
                    .await
            }
        }
        .map_err(EventStoreError::Execute)?;
        Ok(rows.iter().map(row_to_group).collect())
    }

    /// Delete a group and cascade its consumers/offsets.
    pub async fn delete_group(&self, group_id: &str) -> Result<bool, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let count = client
            .execute(
                "DELETE FROM consumer_groups WHERE group_id = $1",
                &[&group_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(count > 0)
    }

    /// Fetch a single group (used for auth channel resolution).
    pub async fn get_group(&self, group_id: &str) -> Result<GroupInfo, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let row = client
            .query_opt(
                "SELECT group_id, channel_id, created_at, metadata
                 FROM consumer_groups WHERE group_id = $1",
                &[&group_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        row.as_ref()
            .map(row_to_group)
            .ok_or_else(|| EventStoreError::GroupNotFound(group_id.to_string()))
    }

    /// Register (or renew) a consumer's lease in a group.
    /// Creates the consumer's durable offset row on first join.
    pub async fn join_group(
        &self,
        group_id: &str,
        consumer_id: &str,
        lease_seconds: i64,
    ) -> Result<GroupOffsetInfo, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let lease_seconds = lease_seconds.clamp(1, 86_400);
        let row = client
            .query_one(
                "INSERT INTO group_consumers (group_id, consumer_id, lease_until, status)
                 VALUES ($1, $2, NOW() + ($3::bigint * INTERVAL '1 second'), 'active')
                 ON CONFLICT (group_id, consumer_id) DO UPDATE
                 SET lease_until = NOW() + ($3::bigint * INTERVAL '1 second'),
                     status = 'active',
                     last_heartbeat = NOW()
                 RETURNING group_id, consumer_id",
                &[&group_id, &consumer_id, &lease_seconds],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        let _: String = row.get("group_id");

        let offset_row = client
            .query_one(
                "INSERT INTO group_offsets (group_id, consumer_id)
                 VALUES ($1, $2)
                 ON CONFLICT (group_id, consumer_id) DO UPDATE
                 SET claimed_cursor = group_offsets.last_cursor,
                     updated_at = NOW()
                 RETURNING group_id, consumer_id, last_cursor, claimed_cursor, updated_at",
                &[&group_id, &consumer_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(row_to_offset(&offset_row))
    }

    /// Claim a batch of events for a consumer.
    ///
    /// Semantics (at-least-once, lease-based):
    /// - Claim start = max(committed offset, valid claimed watermark across
    ///   all consumers with active leases). Expired leases are excluded, so
    ///   their claims become reclaimable.
    /// - The batch is reserved via `FOR UPDATE SKIP LOCKED`, guaranteeing a
    ///   given event is claimed by exactly one active consumer at a time.
    /// - The offset advances only on `ack`; a crashed consumer's unacked work
    ///   is redelivered after its lease expires.
    pub async fn claim_work(
        &self,
        group_id: &str,
        consumer_id: &str,
        limit: i64,
    ) -> Result<ClaimedWork, EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let limit = limit.clamp(1, 1000);

        let transaction = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;

        // Serialize all claims in this group: locking the group row makes the
        // watermark read + event SELECT below a single consistent snapshot.
        // Without this, a concurrent claim can commit between our watermark
        // read and our event SELECT (READ COMMITTED), letting two consumers
        // claim the same events.
        let group = transaction
            .query_opt(
                "SELECT channel_id FROM consumer_groups WHERE group_id = $1 FOR UPDATE",
                &[&group_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        let channel_id: String = match group {
            Some(r) => r.get("channel_id"),
            None => {
                return Err(EventStoreError::GroupNotFound(group_id.to_string()));
            }
        };

        let lease = transaction
            .query_opt(
                "SELECT 1 FROM group_consumers
                 WHERE group_id = $1 AND consumer_id = $2
                   AND status = 'active' AND lease_until > NOW()",
                &[&group_id, &consumer_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        if lease.is_none() {
            return Err(EventStoreError::ConsumerNotFound(consumer_id.to_string()));
        }

        // Durable claim watermark across consumers with active leases.
        let watermark_row = transaction
            .query_one(
                "SELECT COALESCE(MAX(o.claimed_cursor), 0)::bigint AS watermark
                 FROM group_offsets o
                 JOIN group_consumers c
                   ON c.group_id = o.group_id AND c.consumer_id = o.consumer_id
                 WHERE o.group_id = $1 AND c.status = 'active' AND c.lease_until > NOW()",
                &[&group_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        let watermark: i64 = watermark_row.get("watermark");

        let own = transaction
            .query_one(
                "SELECT last_cursor, claimed_cursor FROM group_offsets
                 WHERE group_id = $1 AND consumer_id = $2 FOR UPDATE",
                &[&group_id, &consumer_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        let committed: i64 = own.get("last_cursor");
        let own_claimed: i64 = own.get("claimed_cursor");

        let start = committed.max(watermark).max(own_claimed);

        let rows = transaction
            .query(
                "SELECT id, channel_id, event_type, payload, actor,
                        agent_id, user_id, metadata, cursor, timestamp, depends_on
                 FROM agent_events
                 WHERE channel_id = $1 AND cursor > $2
                 ORDER BY cursor ASC
                 LIMIT $3
                 FOR UPDATE SKIP LOCKED",
                &[&channel_id, &start, &limit],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        let mut events: Vec<AgentEvent> = Vec::with_capacity(rows.len());
        let mut last_claimed = committed;
        for row in rows.iter() {
            let ev = row_to_event(row);
            last_claimed = last_claimed.max(ev.cursor);
            events.push(ev);
        }

        if last_claimed > committed {
            transaction
                .execute(
                    "UPDATE group_offsets SET claimed_cursor = $3, updated_at = NOW()
                     WHERE group_id = $1 AND consumer_id = $2",
                    &[&group_id, &consumer_id, &last_claimed],
                )
                .await
                .map_err(EventStoreError::Execute)?;
        }

        let lease_until = transaction
            .query_one(
                "SELECT lease_until FROM group_consumers
                 WHERE group_id = $1 AND consumer_id = $2",
                &[&group_id, &consumer_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        let lease_until: DateTime<Utc> = lease_until.get("lease_until");

        transaction
            .commit()
            .await
            .map_err(EventStoreError::Execute)?;

        Ok(ClaimedWork {
            group_id: group_id.to_string(),
            consumer_id: consumer_id.to_string(),
            events,
            lease_until,
        })
    }

    /// Acknowledge processed work: durably advances the consumer's committed
    /// offset. Only a consumer with an active lease may ack.
    pub async fn ack_work(
        &self,
        group_id: &str,
        consumer_id: &str,
        cursor: i64,
    ) -> Result<(), EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let lease_ok = client
            .query_opt(
                "SELECT 1 FROM group_consumers
                 WHERE group_id = $1 AND consumer_id = $2
                   AND status = 'active' AND lease_until > NOW()",
                &[&group_id, &consumer_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        if lease_ok.is_none() {
            return Err(EventStoreError::ConsumerNotFound(consumer_id.to_string()));
        }
        client
            .execute(
                "UPDATE group_offsets
                 SET last_cursor = GREATEST(last_cursor, $3),
                     updated_at = NOW()
                 WHERE group_id = $1 AND consumer_id = $2",
                &[&group_id, &consumer_id, &cursor],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(())
    }

    /// Extend a consumer's lease.
    pub async fn heartbeat(
        &self,
        group_id: &str,
        consumer_id: &str,
        lease_seconds: i64,
    ) -> Result<(), EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let lease_seconds = lease_seconds.clamp(1, 86_400);
        let count = client
            .execute(
                "UPDATE group_consumers
                 SET lease_until = NOW() + ($3::bigint * INTERVAL '1 second'),
                     last_heartbeat = NOW()
                 WHERE group_id = $1 AND consumer_id = $2 AND status = 'active'",
                &[&group_id, &consumer_id, &lease_seconds],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        if count == 0 {
            return Err(EventStoreError::ConsumerNotFound(consumer_id.to_string()));
        }
        Ok(())
    }

    /// Per-consumer durable offsets for a group.
    pub async fn get_offsets(
        &self,
        group_id: &str,
    ) -> Result<Vec<GroupOffsetInfo>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let exists = client
            .query_opt(
                "SELECT 1 FROM consumer_groups WHERE group_id = $1",
                &[&group_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        if exists.is_none() {
            return Err(EventStoreError::GroupNotFound(group_id.to_string()));
        }
        let rows = client
            .query(
                "SELECT group_id, consumer_id, last_cursor, claimed_cursor, updated_at
                 FROM group_offsets WHERE group_id = $1 ORDER BY consumer_id",
                &[&group_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(rows.iter().map(row_to_offset).collect())
    }

    /// Expire leases that have lapsed. Returns the number of consumers expired.
    /// Expired consumers' claims become reclaimable by other consumers.
    /// Scheduled by the `consumer_lease_reaper` task.
    pub async fn expire_leases(&self) -> Result<u64, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let count = client
            .execute(
                "UPDATE group_consumers SET status = 'expired'
                 WHERE status = 'active' AND lease_until < NOW()",
                &[],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(count)
    }

    // ── Persisted Wake-ups (P7) ────────────────────────────────────

    /// Schedule a wake-up: fires a `scheduler.wakeup` event on the channel
    /// at or after `run_at`.
    pub async fn create_wakeup(
        &self,
        request: &CreateWakeupRequest,
    ) -> Result<WakeupInfo, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let payload_json = request.payload.as_ref().map(|v| JsonValue::new(v.clone()));
        let row = client
            .query_one(
                "INSERT INTO scheduled_wakeups (channel_id, run_at, payload)
                 VALUES ($1, $2, COALESCE($3, '{}'::jsonb))
                 RETURNING id, channel_id, run_at, status, payload, created_at,
                           executed_at, attempts::bigint AS attempts, error",
                &[&request.channel_id, &request.run_at, &payload_json],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(row_to_wakeup(&row))
    }

    /// List wake-ups, optionally filtered by channel and status.
    pub async fn list_wakeups(
        &self,
        channel_id: Option<&str>,
        status: Option<&str>,
    ) -> Result<Vec<WakeupInfo>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let rows = client
            .query(
                "SELECT id, channel_id, run_at, status, payload, created_at,
                        executed_at, attempts::bigint AS attempts, error
                 FROM scheduled_wakeups
                 WHERE ($1::text IS NULL OR channel_id = $1)
                   AND ($2::text IS NULL OR status = $2)
                 ORDER BY run_at ASC",
                &[&channel_id, &status],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(rows.iter().map(row_to_wakeup).collect())
    }

    /// Fetch a single wake-up.
    pub async fn get_wakeup(&self, id: Uuid) -> Result<Option<WakeupInfo>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let row = client
            .query_opt(
                "SELECT id, channel_id, run_at, status, payload, created_at,
                        executed_at, attempts::bigint AS attempts, error
                 FROM scheduled_wakeups WHERE id = $1",
                &[&id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(row.map(|r| row_to_wakeup(&r)))
    }

    /// Cancel a pending wake-up. Returns false when already fired/cancelled.
    pub async fn cancel_wakeup(&self, id: Uuid) -> Result<bool, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let count = client
            .execute(
                "UPDATE scheduled_wakeups SET status = 'cancelled'
                 WHERE id = $1 AND status = 'pending'",
                &[&id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(count > 0)
    }

    /// Dispatch due wake-ups: claims due rows with `FOR UPDATE SKIP LOCKED`
    /// (single-writer safe across instances), fires a `scheduler.wakeup`
    /// event per wake-up, and marks them done — all in one transaction.
    ///
    /// Failed batches are marked `failed` with an exponential backoff on
    /// `next_attempt_at` and retried by the next pass.
    /// Returns the dispatched wake-up events (callers broadcast them live).
    #[instrument(skip(self))]
    pub async fn dispatch_due_wakeups(
        &self,
        batch_limit: i64,
    ) -> Result<Vec<AgentEvent>, EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let batch_limit = batch_limit.clamp(1, 500);

        let mut transaction = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;

        let rows = transaction
            .query(
                "SELECT id, channel_id, payload
                 FROM scheduled_wakeups
                 WHERE (status = 'pending' OR (status = 'failed' AND next_attempt_at <= NOW()))
                   AND run_at <= NOW()
                 ORDER BY run_at ASC
                 LIMIT $1
                 FOR UPDATE SKIP LOCKED",
                &[&batch_limit],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        if rows.is_empty() {
            transaction
                .commit()
                .await
                .map_err(EventStoreError::Execute)?;
            return Ok(Vec::new());
        }

        let mut ids: Vec<Uuid> = Vec::with_capacity(rows.len());
        let mut dispatched: Vec<AgentEvent> = Vec::with_capacity(rows.len());
        let now = Utc::now();

        for row in rows.iter() {
            let id: Uuid = row.get("id");
            let channel_id: String = row.get("channel_id");
            let payload: Option<crate::database::json_compat::JsonValue> = row.get("payload");
            let payload_value = payload.map(|v| v.0).unwrap_or_else(|| sonic_rs::json!({}));

            let (event_id, cursor) = insert_event_in_tx(
                &mut transaction,
                &channel_id,
                "scheduler.wakeup",
                &payload_value,
                &EventMetadata {
                    actor: "system".to_string(),
                    agent_id: None,
                    user_id: None,
                    metadata: Some(sonic_rs::json!({"wakeup_id": id.to_string()})),
                },
                None,
                None,
                now,
            )
            .await?;

            dispatched.push(AgentEvent {
                id: event_id,
                channel_id: channel_id.clone(),
                event_type: "scheduler.wakeup".to_string(),
                payload: payload_value,
                metadata: EventMetadata {
                    actor: "system".to_string(),
                    agent_id: None,
                    user_id: None,
                    metadata: Some(sonic_rs::json!({"wakeup_id": id.to_string()})),
                },
                cursor,
                depends_on: None,
                timestamp: now,
            });

            ids.push(id);
        }

        transaction
            .execute(
                "UPDATE scheduled_wakeups
                 SET status = 'done', executed_at = NOW()
                 WHERE id = ANY($1)",
                &[&ids],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        transaction
            .commit()
            .await
            .map_err(EventStoreError::Execute)?;

        Ok(dispatched)
    }

    // ── Idempotent Tool Executions ────────────────────────────────────

    /// Resolve a fork tool policy from a derived member channel.
    pub async fn group_fork_tool_context(
        &self,
        channel_id: &str,
        tool_name: &str,
    ) -> Result<Option<(String, String, String, String, String)>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let row = client.query_opt(
            "SELECT f.fork_group_id, m.member_id, sm.channel_id AS source_channel_id, f.member_policies ->> m.member_id AS member_policy, COALESCE(f.tool_policies ->> $2, f.tool_policies ->> '*', 'live') AS policy FROM execution_group_forks f JOIN execution_group_members m ON m.group_id=f.fork_group_id AND m.channel_id=$1 JOIN execution_group_members sm ON sm.group_id=f.source_group_id AND sm.member_id=m.member_id",
            &[&channel_id, &tool_name],
        ).await.map_err(EventStoreError::Execute)?;
        Ok(row.map(|row| {
            (
                row.get("fork_group_id"),
                row.get("member_id"),
                row.get("source_channel_id"),
                row.get("member_policy"),
                row.get("policy"),
            )
        }))
    }

    /// Resolve the side-effect policy and replay-source chain for an ordinary
    /// (non-group) forked channel.
    ///
    /// Returns `None` for a channel with no `parent_channel_id` (not a fork),
    /// so non-forked channels keep today's exact `claim_execution` behavior.
    /// For a fork:
    /// - `policy` is the child's `tool_policies` entry for `tool_name` (or its
    ///   `"*"` fallback), defaulting to `"auto"`.
    /// - `sources` is the ancestors that recorded a completed execution for
    ///   `key_name`, nearest first — computed by ONE bounded recursive query
    ///   (no per-ancestor round trips). Empty for `live`/`block`, which need no
    ///   source; a fork-of-fork can still replay a result any ancestor recorded.
    pub async fn fork_tool_context(
        &self,
        channel_id: &str,
        tool_name: &str,
        key_name: &str,
    ) -> Result<Option<(Vec<String>, String)>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let row = client
            .query_opt(
                "SELECT parent_channel_id, \
                        COALESCE(tool_policies ->> $2, tool_policies ->> '*') AS policy \
                 FROM channel_metadata WHERE channel_id = $1",
                &[&channel_id, &tool_name],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        let Some(row) = row else { return Ok(None) };
        if row
            .try_get::<_, Option<String>>("parent_channel_id")
            .ok()
            .flatten()
            .is_none()
        {
            // Not a fork: keep the pre-existing claim behavior.
            return Ok(None);
        }
        let policy: String = row
            .try_get::<_, Option<String>>("policy")
            .ok()
            .flatten()
            .unwrap_or_else(|| "auto".to_string());

        // Only `auto`/`replay` consult the source ledger; `live`/`block` do not.
        let mut sources: Vec<String> = Vec::new();
        if policy == "auto" || policy == "replay" {
            let rows = client
                .query(
                    "WITH RECURSIVE chain(channel_id, depth) AS ( \
                         SELECT parent_channel_id, 1 \
                         FROM channel_metadata \
                         WHERE channel_id = $1 AND parent_channel_id IS NOT NULL \
                         UNION ALL \
                         SELECT m.parent_channel_id, c.depth + 1 \
                         FROM chain c \
                         JOIN channel_metadata m ON m.channel_id = c.channel_id \
                         WHERE m.parent_channel_id IS NOT NULL AND c.depth < 64 \
                     ) \
                     SELECT c.channel_id \
                     FROM chain c \
                     JOIN tool_executions t \
                       ON t.channel_id = c.channel_id \
                      AND t.key_name = $2 \
                      AND t.status = 'completed' \
                     ORDER BY c.depth",
                    &[&channel_id, &key_name],
                )
                .await
                .map_err(EventStoreError::Execute)?;
            sources = rows
                .iter()
                .map(|r| r.get::<_, String>("channel_id"))
                .collect();
        }
        Ok(Some((sources, policy)))
    }

    /// Materialize a tool call for an ordinary fork by replaying the first
    /// exact match in the ancestor chain's immutable ledger. The tool is never
    /// invoked. `Miss`/`Mismatch` are handed back to the caller, which decides
    /// block vs live per the fork's policy.
    pub async fn replay_fork_execution(
        &self,
        request: &ClaimExecutionRequest,
        source_channels: &[String],
    ) -> Result<ForkReplayResult, EventStoreError> {
        // A concurrent replay/claim for the same (child, key) can win the
        // INSERT race; retry so the winner's row is observed under its lock
        // and the caller gets a graceful replayed/miss/mismatch instead of a
        // 500 (mirrors `claim_execution`'s insert-race retry).
        const MAX_REPLAY_RETRIES: u32 = 4;
        let mut retries: u32 = 0;
        loop {
            match self
                .replay_fork_execution_once(request, source_channels)
                .await
            {
                Err(EventStoreError::ExecutionInsertRace) if retries < MAX_REPLAY_RETRIES => {
                    retries += 1;
                    continue;
                }
                other => return other,
            }
        }
    }

    async fn replay_fork_execution_once(
        &self,
        request: &ClaimExecutionRequest,
        source_channels: &[String],
    ) -> Result<ForkReplayResult, EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let mut tx = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;
        let request_hash = canonical_execution_request(
            &request.tool_name,
            &request.key_name,
            request.dedup_fields.as_deref(),
            request.params.as_ref(),
        );

        // Idempotent re-claim on the child: if this fork already materialized
        // an execution for (child, key), serve it and never re-run.
        if let Some(existing) = tx
            .query_opt(
                &format!("SELECT {EXECUTION_COLUMNS} FROM tool_executions WHERE channel_id=$1 AND key_name=$2 FOR UPDATE"),
                &[&request.channel_id, &request.key_name],
            )
            .await
            .map_err(EventStoreError::Execute)?
        {
            let stored_hash: String = existing.get("request_hash");
            if stored_hash != request_hash {
                tx.commit().await.map_err(EventStoreError::Execute)?;
                return Ok(ForkReplayResult::Mismatch);
            }
            let info = row_to_execution(&existing);
            if info.status != "completed" {
                tx.commit().await.map_err(EventStoreError::Execute)?;
                return Ok(ForkReplayResult::Miss);
            }
            let result = info.result.clone();
            let event_id = info.completed_event_id.ok_or_else(|| {
                EventStoreError::InconsistentState("fork replay lacks marker event".into())
            })?;
            let event = event_by_id(&tx, event_id).await?;
            tx.commit().await.map_err(EventStoreError::Execute)?;
            return Ok(ForkReplayResult::Replayed(
                ExecutionClaimResult {
                    status: "replayed".into(),
                    execution: info,
                    result,
                    claim_token: None,
                },
                event,
            ));
        }

        // Try each ancestor, nearest first, for an exact request match.
        let mut saw_key = false;
        for source_channel_id in source_channels {
            let source = tx
                .query_opt(
                    &format!("SELECT {EXECUTION_COLUMNS}, request_hash FROM tool_executions WHERE channel_id=$1 AND key_name=$2 AND status='completed' FOR SHARE"),
                    &[source_channel_id, &request.key_name],
                )
                .await
                .map_err(EventStoreError::Execute)?;
            let Some(source) = source else { continue };
            saw_key = true;
            let source_hash: String = source.get("request_hash");
            if source_hash != request_hash {
                continue;
            }
            let source_info = row_to_execution(&source);
            let source_event = source_info.completed_event_id.ok_or_else(|| {
                EventStoreError::InconsistentState(
                    "source tool result has no completion event".into(),
                )
            })?;
            let execution_id = Uuid::new_v4();
            let now = Utc::now();
            let payload = execution_lifecycle_payload(
                &request.tool_name,
                &request.key_name,
                &execution_id,
                1,
                request.params.as_ref(),
            );
            let metadata = EventMetadata {
                actor: request
                    .actor
                    .clone()
                    .unwrap_or_else(|| "fork-replay".into()),
                agent_id: None,
                user_id: None,
                metadata: Some(sonic_rs::json!({
                    "source_execution_id": source_info.id.to_string(),
                    "fork_source_channel_id": source_channel_id,
                    "classification": "replayed",
                })),
            };
            let (event_id, cursor) = insert_event_in_tx(
                &mut tx,
                &request.channel_id,
                "tool.replayed",
                &payload,
                &metadata,
                Some(source_event),
                None,
                now,
            )
            .await?;
            insert_dependency_edges(&tx, event_id, Some(source_event), &[]).await?;
            let params = request
                .params
                .as_ref()
                .map(|value| JsonValue::new(value.clone()));
            let result = source_info.result.clone();
            let result_json = result.as_ref().map(|value| JsonValue::new(value.clone()));
            let actor = request
                .actor
                .clone()
                .unwrap_or_else(|| "fork-replay".into());
            let inserted = tx
                .query_opt(
                    &format!("INSERT INTO tool_executions (execution_id,channel_id,key_name,tool_name,request_hash,status,attempts,actor,started_event_id,completed_event_id,started_cursor,completed_cursor,replay_emitted,params,result,created_at,updated_at,completed_at) VALUES ($1,$2,$3,$4,$5,'completed',1,$6,$7,$7,$8,$8,true,$9,$10,$11,$11,$11) ON CONFLICT (channel_id, key_name) DO NOTHING RETURNING {EXECUTION_COLUMNS}"),
                    &[
                        &execution_id,
                        &request.channel_id,
                        &request.key_name,
                        &request.tool_name,
                        &request_hash,
                        &actor,
                        &event_id,
                        &cursor,
                        &params,
                        &result_json,
                        &now,
                    ],
                )
                .await
                .map_err(EventStoreError::Execute)?;
            let Some(row) = inserted else {
                // Lost the race to a concurrent replay/claim for this key:
                // roll back the staged tool.replayed event and retry so the
                // winner's row is observed.
                tx.rollback().await.map_err(EventStoreError::Execute)?;
                return Err(EventStoreError::ExecutionInsertRace);
            };
            let info = row_to_execution(&row);
            let event = AgentEvent {
                id: event_id,
                channel_id: request.channel_id.clone(),
                event_type: "tool.replayed".into(),
                payload,
                metadata,
                cursor,
                depends_on: Some(source_event),
                timestamp: now,
            };
            tx.commit().await.map_err(EventStoreError::Execute)?;
            return Ok(ForkReplayResult::Replayed(
                ExecutionClaimResult {
                    status: "replayed".into(),
                    execution: info,
                    result,
                    claim_token: None,
                },
                event,
            ));
        }

        tx.commit().await.map_err(EventStoreError::Execute)?;
        Ok(if saw_key {
            ForkReplayResult::Mismatch
        } else {
            ForkReplayResult::Miss
        })
    }

    /// Materialize a REPLAY-policy tool call from the immutable source
    /// ledger. A miss is a blocked boundary, never permission to execute.
    pub async fn replay_group_fork_execution(
        &self,
        request: &ClaimExecutionRequest,
        fork_group_id: &str,
        source_channel_id: &str,
    ) -> Result<(ExecutionClaimResult, AgentEvent), EventStoreError> {
        // Same insert-race retry as the ordinary fork path: a concurrent
        // replay for the same (member channel, key) must not surface a 500.
        const MAX_REPLAY_RETRIES: u32 = 4;
        let mut retries: u32 = 0;
        loop {
            match self
                .replay_group_fork_execution_once(request, fork_group_id, source_channel_id)
                .await
            {
                Err(EventStoreError::ExecutionInsertRace) if retries < MAX_REPLAY_RETRIES => {
                    retries += 1;
                    continue;
                }
                other => return other,
            }
        }
    }

    async fn replay_group_fork_execution_once(
        &self,
        request: &ClaimExecutionRequest,
        fork_group_id: &str,
        source_channel_id: &str,
    ) -> Result<(ExecutionClaimResult, AgentEvent), EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let mut tx = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;
        let request_hash = canonical_execution_request(
            &request.tool_name,
            &request.key_name,
            request.dedup_fields.as_deref(),
            request.params.as_ref(),
        );
        if let Some(existing) = tx.query_opt(
            &format!("SELECT {EXECUTION_COLUMNS} FROM tool_executions WHERE channel_id=$1 AND key_name=$2 FOR UPDATE"),
            &[&request.channel_id,&request.key_name],
        ).await.map_err(EventStoreError::Execute)? {
            let stored_hash: String = existing.get("request_hash");
            if stored_hash != request_hash { return Err(EventStoreError::ExecutionKeyMismatch { channel_id: request.channel_id.clone(), key_name: request.key_name.clone() }); }
            let info=row_to_execution(&existing);
            if info.status != "completed" { return Err(EventStoreError::InconsistentState("counterfactual replay target is not completed".into())); }
            let result=info.result.clone();
            let event_id=info.completed_event_id.ok_or_else(|| EventStoreError::InconsistentState("counterfactual replay lacks marker event".into()))?;
            let event=event_by_id(&tx,event_id).await?;
            tx.commit().await.map_err(EventStoreError::Execute)?;
            return Ok((ExecutionClaimResult { status:"replayed".into(), execution:info, result, claim_token:None },event));
        }
        let source = tx.query_opt(
            &format!("SELECT {EXECUTION_COLUMNS}, request_hash FROM tool_executions WHERE channel_id=$1 AND key_name=$2 AND status='completed' FOR SHARE"),
            &[&source_channel_id,&request.key_name],
        ).await.map_err(EventStoreError::Execute)?
            .ok_or_else(|| EventStoreError::CounterfactualBlocked { fork_group_id:fork_group_id.into(), boundary:format!("tool:{}:replay_miss",request.tool_name) })?;
        let source_hash: String = source.get("request_hash");
        if source_hash != request_hash {
            return Err(EventStoreError::CounterfactualBlocked {
                fork_group_id: fork_group_id.into(),
                boundary: format!("tool:{}:request_mismatch", request.tool_name),
            });
        }
        let source_info = row_to_execution(&source);
        let source_event = source_info.completed_event_id.ok_or_else(|| {
            EventStoreError::InconsistentState("source tool result has no completion event".into())
        })?;
        let execution_id = Uuid::new_v4();
        let now = Utc::now();
        let payload = execution_lifecycle_payload(
            &request.tool_name,
            &request.key_name,
            &execution_id,
            1,
            request.params.as_ref(),
        );
        let metadata = EventMetadata {
            actor: request
                .actor
                .clone()
                .unwrap_or_else(|| "group-fork-replay".into()),
            agent_id: None,
            user_id: None,
            metadata: Some(
                sonic_rs::json!({"fork_group_id":fork_group_id,"source_execution_id":source_info.id.to_string(),"classification":"replayed"}),
            ),
        };
        let (event_id, cursor) = insert_event_in_tx(
            &mut tx,
            &request.channel_id,
            "tool.replayed",
            &payload,
            &metadata,
            Some(source_event),
            None,
            now,
        )
        .await?;
        insert_dependency_edges(&tx, event_id, Some(source_event), &[]).await?;
        let params = request
            .params
            .as_ref()
            .map(|value| JsonValue::new(value.clone()));
        let result = source_info.result.clone();
        let result_json = result.as_ref().map(|value| JsonValue::new(value.clone()));
        let actor = request
            .actor
            .clone()
            .unwrap_or_else(|| "group-fork-replay".into());
        let row=tx.query_opt(
            &format!("INSERT INTO tool_executions (execution_id,channel_id,key_name,tool_name,request_hash,status,attempts,actor,started_event_id,completed_event_id,started_cursor,completed_cursor,replay_emitted,params,result,created_at,updated_at,completed_at) VALUES ($1,$2,$3,$4,$5,'completed',1,$6,$7,$7,$8,$8,true,$9,$10,$11,$11,$11) ON CONFLICT (channel_id, key_name) DO NOTHING RETURNING {EXECUTION_COLUMNS}"),
            &[&execution_id,&request.channel_id,&request.key_name,&request.tool_name,&request_hash,&actor,&event_id,&cursor,&params,&result_json,&now],
        ).await.map_err(EventStoreError::Execute)?;
        let Some(row) = row else {
            // Lost the insert race; roll back the staged replay marker and retry.
            tx.rollback().await.map_err(EventStoreError::Execute)?;
            return Err(EventStoreError::ExecutionInsertRace);
        };
        let receipt_entry = JsonValue::new(
            sonic_rs::json!({"tool_name":request.tool_name,"key_name":request.key_name,"source_execution_id":source_info.id.to_string(),"execution_id":execution_id.to_string(),"event_id":event_id.to_string(),"at":now.to_rfc3339()}),
        );
        tx.execute("UPDATE execution_group_forks SET receipt=jsonb_set(COALESCE(receipt,'{}'::jsonb),'{tool_results_replayed}',COALESCE(receipt->'tool_results_replayed','[]'::jsonb)||jsonb_build_array($2::jsonb),true),updated_at=NOW() WHERE fork_group_id=$1", &[&fork_group_id,&receipt_entry]).await.map_err(EventStoreError::Execute)?;
        let info = row_to_execution(&row);
        let event = AgentEvent {
            id: event_id,
            channel_id: request.channel_id.clone(),
            event_type: "tool.replayed".into(),
            payload,
            metadata,
            cursor,
            depends_on: Some(source_event),
            timestamp: now,
        };
        tx.commit().await.map_err(EventStoreError::Execute)?;
        Ok((
            ExecutionClaimResult {
                status: "replayed".into(),
                execution: info,
                result,
                claim_token: None,
            },
            event,
        ))
    }

    /// Claim an idempotent tool execution.
    ///
    /// Outcomes (see `ExecutionClaimResult::status`):
    /// - `claimed`: this caller now owns a running execution; the persisted
    ///   `tool.started` event (returned in the second tuple position) is
    ///   broadcast by the caller.
    /// - `replayed`: a completed execution with a matching request hash was
    ///   found; its result is returned and no event is emitted unless the
    ///   caller requested `emit_replay_event`, in which case a one-time
    ///   `tool.replayed` marker is written.
    /// - `reclaimed`: a previous attempt abandoned a stale run; the caller
    ///   takes ownership (`attempts` incremented). The prior outcome is
    ///   ambiguous.
    /// - `in_progress`: another owner holds a valid lease; no state changes.
    ///
    /// The idempotency unit `(channel_id, key_name)` serializes on a row lock.
    /// Claim an idempotent tool execution, retrying transparently when a
    /// concurrent claim wins the INSERT race for a brand-new idempotency key.
    #[instrument(skip(self, request), fields(channel = %request.channel_id, key = %request.key_name, tool = %request.tool_name))]
    pub async fn claim_execution(
        &self,
        request: &ClaimExecutionRequest,
    ) -> Result<(ExecutionClaimResult, Option<AgentEvent>), EventStoreError> {
        const MAX_CLAIM_RETRIES: u32 = 4;
        let mut retries: u32 = 0;
        loop {
            match self.claim_execution_once(request).await {
                Err(EventStoreError::ExecutionInsertRace) if retries < MAX_CLAIM_RETRIES => {
                    retries += 1;
                    continue;
                }
                result => return result,
            }
        }
    }

    /// Single attempt at claiming an idempotent tool execution (see the
    /// public wrapper for retry semantics).
    async fn claim_execution_once(
        &self,
        request: &ClaimExecutionRequest,
    ) -> Result<(ExecutionClaimResult, Option<AgentEvent>), EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let lease = request
            .lease_seconds
            .unwrap_or(DEFAULT_EXECUTION_LEASE_SECONDS)
            .clamp(1, MAX_EXECUTION_LEASE_SECONDS);
        let now = Utc::now();
        let request_hash = canonical_execution_request(
            &request.tool_name,
            &request.key_name,
            request.dedup_fields.as_deref(),
            request.params.as_ref(),
        );
        let actor = request
            .actor
            .clone()
            .unwrap_or_else(|| "system".to_string());

        let mut transaction = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;

        let existing = transaction
            .query_opt(
                &format!(
                    "SELECT {EXECUTION_COLUMNS}
                     FROM tool_executions
                     WHERE channel_id = $1 AND key_name = $2
                     FOR UPDATE"
                ),
                &[&request.channel_id, &request.key_name],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        let (claim, event) = if let Some(row) = existing {
            let execution_id: Uuid = row.get("execution_id");
            let stored_status: String = row.get("status");
            let stored_hash: String = row.get("request_hash");
            let stored_attempts: i64 = row.get("attempts");
            let lease_until: Option<DateTime<Utc>> = row.get("lease_until");
            let stored_result: Option<JsonValue> = row.get("result");
            let stored_result = stored_result.map(|v| v.0);
            let depends_on: Option<Uuid> = row.try_get("started_event_id").ok().flatten();

            match stored_status.as_str() {
                // A completed execution whose result has expired under
                // retention is a tombstone: the key identity survives but the
                // result body was purged. The retry fails loudly instead of
                // silently executing again — the caller must treat the prior
                // outcome as unknowable and resolve it downstream.
                "completed"
                    if row
                        .try_get::<_, Option<DateTime<Utc>>>("expired_at")
                        .ok()
                        .flatten()
                        .is_some() =>
                {
                    if stored_hash != request_hash {
                        transaction
                            .rollback()
                            .await
                            .map_err(EventStoreError::Execute)?;
                        return Err(EventStoreError::ExecutionKeyMismatch {
                            channel_id: request.channel_id.clone(),
                            key_name: request.key_name.clone(),
                        });
                    }
                    crate::metrics::basic_metrics::EXECUTIONS_TOTAL
                        .with_label_values(&["result_expired"])
                        .inc();
                    transaction
                        .rollback()
                        .await
                        .map_err(EventStoreError::Execute)?;
                    return Err(EventStoreError::ExecutionResultExpired {
                        channel_id: request.channel_id.clone(),
                        key_name: request.key_name.clone(),
                        execution_id,
                    });
                }

                // A completed execution is the idempotency hit: replay the
                // persisted result, optionally recording a one-time marker.
                "completed" => {
                    if stored_hash != request_hash {
                        transaction
                            .rollback()
                            .await
                            .map_err(EventStoreError::Execute)?;
                        return Err(EventStoreError::ExecutionKeyMismatch {
                            channel_id: request.channel_id.clone(),
                            key_name: request.key_name.clone(),
                        });
                    }
                    crate::metrics::basic_metrics::EXECUTIONS_TOTAL
                        .with_label_values(&["replayed"])
                        .inc();
                    crate::metrics::basic_metrics::EXECUTION_REPLAYS_TOTAL.inc();

                    let replay_event = if request.emit_replay_event.unwrap_or(false) {
                        let already: bool = row.get("replay_emitted");
                        if !already {
                            let payload = execution_lifecycle_payload(
                                &request.tool_name,
                                &request.key_name,
                                &execution_id,
                                stored_attempts,
                                request.params.as_ref(),
                            );
                            let (event_id, cursor) = insert_event_in_tx(
                                &mut transaction,
                                &request.channel_id,
                                "tool.replayed",
                                &payload,
                                &EventMetadata {
                                    actor: actor.clone(),
                                    agent_id: None,
                                    user_id: None,
                                    metadata: Some(sonic_rs::json!({
                                        "execution_id": execution_id.to_string()
                                    })),
                                },
                                depends_on,
                                None,
                                now,
                            )
                            .await?;
                            transaction
                                .execute(
                                    "UPDATE tool_executions
                                     SET replay_emitted = TRUE, updated_at = NOW()
                                     WHERE execution_id = $1",
                                    &[&execution_id],
                                )
                                .await
                                .map_err(EventStoreError::Execute)?;
                            Some(lifecycle_agent_event(
                                event_id,
                                execution_id,
                                &request.channel_id,
                                "tool.replayed",
                                &payload,
                                &actor,
                                depends_on,
                                cursor,
                                now,
                            ))
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    (
                        ExecutionClaimResult {
                            status: "replayed".to_string(),
                            execution: row_to_execution(&row),
                            result: stored_result,
                            claim_token: None,
                        },
                        replay_event,
                    )
                }

                // Another owner still holds a valid lease — hand back the
                // progress state and mutate nothing.
                "running" if lease_until.map(|until| until > now).unwrap_or(false) => (
                    ExecutionClaimResult {
                        status: "in_progress".to_string(),
                        execution: row_to_execution(&row),
                        result: None,
                        claim_token: None,
                    },
                    None,
                ),

                // Abandoned/expired run: take over ownership under a fresh
                // claim token. A prior `failed` status is unambiguous and is
                // reported as `claimed`; everything else is `reclaimed`.
                "running" | "failed" | "interrupted" | "cancelled" | "expired" => {
                    if stored_hash != request_hash {
                        transaction
                            .rollback()
                            .await
                            .map_err(EventStoreError::Execute)?;
                        return Err(EventStoreError::ExecutionKeyMismatch {
                            channel_id: request.channel_id.clone(),
                            key_name: request.key_name.clone(),
                        });
                    }

                    let new_attempts = stored_attempts + 1;
                    let fresh_token = Uuid::new_v4();
                    let lease_until_new = now + chrono::Duration::seconds(lease);
                    let payload = execution_lifecycle_payload(
                        &request.tool_name,
                        &request.key_name,
                        &execution_id,
                        new_attempts,
                        request.params.as_ref(),
                    );

                    let (event_id, cursor) = insert_event_in_tx(
                        &mut transaction,
                        &request.channel_id,
                        "tool.started",
                        &payload,
                        &EventMetadata {
                            actor: actor.clone(),
                            agent_id: None,
                            user_id: None,
                            metadata: Some(sonic_rs::json!({
                                "execution_id": execution_id.to_string()
                            })),
                        },
                        depends_on,
                        None,
                        now,
                    )
                    .await?;

                    transaction
                        .execute(
                            "UPDATE tool_executions
                             SET status = 'running',
                                 attempts = $2,
                                 claim_token = $3,
                                 lease_until = $4,
                                 started_event_id = $5,
                                 started_cursor = $6,
                                 completed_event_id = NULL,
                                 completed_cursor = NULL,
                                 result = NULL,
                                 error = NULL,
                                 completed_at = NULL,
                                 updated_at = NOW()
                             WHERE execution_id = $1",
                            &[
                                &execution_id,
                                &(new_attempts as i32),
                                &fresh_token,
                                &lease_until_new,
                                &event_id,
                                &cursor,
                            ],
                        )
                        .await
                        .map_err(EventStoreError::Execute)?;

                    let claimed = stored_status == "failed";
                    crate::metrics::basic_metrics::EXECUTIONS_TOTAL
                        .with_label_values(if claimed {
                            &["claimed"]
                        } else {
                            &["reclaimed"]
                        })
                        .inc();

                    let fresh = fetch_execution_in_tx(&transaction, &execution_id).await?;
                    (
                        ExecutionClaimResult {
                            status: if claimed {
                                "claimed".to_string()
                            } else {
                                "reclaimed".to_string()
                            },
                            execution: fresh,
                            result: None,
                            claim_token: Some(fresh_token),
                        },
                        Some(lifecycle_agent_event(
                            event_id,
                            execution_id,
                            &request.channel_id,
                            "tool.started",
                            &payload,
                            &actor,
                            depends_on,
                            cursor,
                            now,
                        )),
                    )
                }

                other => {
                    tracing::warn!(execution = %execution_id, status = other, "unexpected execution status on claim");
                    transaction
                        .rollback()
                        .await
                        .map_err(EventStoreError::Execute)?;
                    return Err(EventStoreError::ExecutionKeyMismatch {
                        channel_id: request.channel_id.clone(),
                        key_name: request.key_name.clone(),
                    });
                }
            }
        } else {
            // Brand-new execution: create, start it, and persist `tool.started`.
            let execution_id = Uuid::new_v4();
            let fresh_token = Uuid::new_v4();
            let lease_until_new = now + chrono::Duration::seconds(lease);
            let payload = execution_lifecycle_payload(
                &request.tool_name,
                &request.key_name,
                &execution_id,
                1,
                request.params.as_ref(),
            );

            let (event_id, cursor) = insert_event_in_tx(
                &mut transaction,
                &request.channel_id,
                "tool.started",
                &payload,
                &EventMetadata {
                    actor: actor.clone(),
                    agent_id: None,
                    user_id: None,
                    metadata: Some(sonic_rs::json!({
                        "execution_id": execution_id.to_string()
                    })),
                },
                None,
                None,
                now,
            )
            .await?;

            let params_json = request.params.as_ref().map(|p| JsonValue::new(p.clone()));

            let inserted = transaction
                .execute(
                    "INSERT INTO tool_executions (
                        execution_id, channel_id, key_name, tool_name, request_hash, status,
                        attempts, claim_token, lease_until, actor, started_event_id, started_cursor,
                        params
                     ) VALUES ($1, $2, $3, $4, $5, 'running', 1, $6, $7, $8, $9, $10, $11)
                     ON CONFLICT (channel_id, key_name) DO NOTHING",
                    &[
                        &execution_id,
                        &request.channel_id,
                        &request.key_name,
                        &request.tool_name,
                        &request_hash,
                        &fresh_token,
                        &lease_until_new,
                        &actor,
                        &event_id,
                        &cursor,
                        &params_json,
                    ],
                )
                .await
                .map_err(map_quota_exec_error)?;

            if inserted == 0 {
                // A concurrent claim for the same idempotency key won the
                // INSERT race (the SELECT above saw no row, but another
                // transaction created one first). Roll back this attempt —
                // including the staged `tool.started` event — and retry so
                // the row is observed under its lock and reported correctly
                // (in_progress / replayed / reclaimed).
                transaction
                    .rollback()
                    .await
                    .map_err(EventStoreError::Execute)?;
                return Err(EventStoreError::ExecutionInsertRace);
            }

            crate::metrics::basic_metrics::EXECUTIONS_TOTAL
                .with_label_values(&["claimed"])
                .inc();

            // Report the claim's durable params bytes (the DB trigger enforces
            // the quota atomically; this outbox row feeds control-plane usage
            // reporting for the durable-written meter).
            let params_bytes = params_json
                .as_ref()
                .map(|p| p.0.to_string().len())
                .unwrap_or(0)
                .min(i64::MAX as usize) as i64;
            let usage_date = now.date_naive();
            transaction
                .execute(
                    "INSERT INTO usage_delivery_outbox
                     (idempotency_key, usage_date, metric, quantity, source_generation)
                     VALUES ($1, $2::date, 'tool_params_bytes', $3, 1),
                            ($4, $2::date, 'durable_written_bytes', $3, 1)
                     ON CONFLICT (idempotency_key) DO NOTHING",
                    &[
                        &format!("tool-params:{execution_id}"),
                        &usage_date,
                        &params_bytes,
                        &format!("tool-params-durable:{execution_id}"),
                    ],
                )
                .await
                .map_err(EventStoreError::Execute)?;

            let fresh = fetch_execution_in_tx(&transaction, &execution_id).await?;
            (
                ExecutionClaimResult {
                    status: "claimed".to_string(),
                    execution: fresh,
                    result: None,
                    claim_token: Some(fresh_token),
                },
                Some(lifecycle_agent_event(
                    event_id,
                    execution_id,
                    &request.channel_id,
                    "tool.started",
                    &payload,
                    &actor,
                    None,
                    cursor,
                    now,
                )),
            )
        };

        transaction
            .commit()
            .await
            .map_err(EventStoreError::Execute)?;

        Ok((claim, event))
    }

    /// Complete a claimed execution with its persisted result. Ownership is
    /// enforced via the `claim_token` returned by claim. A duplicate complete
    /// (e.g. retried after an ambiguous transport failure) returns the stored
    /// result without emitting another event.
    #[instrument(skip(self, request), fields(execution = %execution_id))]
    pub async fn complete_execution(
        &self,
        execution_id: Uuid,
        claim_token: Option<Uuid>,
        request: &CompleteExecutionRequest,
    ) -> Result<(ExecutionInfo, Option<AgentEvent>), EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let mut tx = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;

        let row = tx
            .query_opt(
                &format!(
                    "SELECT {EXECUTION_COLUMNS} FROM tool_executions WHERE execution_id = $1 FOR UPDATE"
                ),
                &[&execution_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        let row = match row {
            Some(r) => r,
            None => {
                tx.rollback().await.map_err(EventStoreError::Execute)?;
                return Err(EventStoreError::ExecutionNotFound(execution_id.to_string()));
            }
        };

        let status: String = row.get("status");
        if status == "completed" {
            // Ambiguous-connection retry of a complete that already committed.
            let info = row_to_execution(&row);
            tx.rollback().await.map_err(EventStoreError::Execute)?;
            return Ok((info, None));
        }
        if status != "running" {
            tx.rollback().await.map_err(EventStoreError::Execute)?;
            return Err(EventStoreError::ExecutionNotOwned(execution_id.to_string()));
        }
        let current_token: Option<Uuid> = row.get("claim_token");
        if current_token != claim_token {
            tx.rollback().await.map_err(EventStoreError::Execute)?;
            return Err(EventStoreError::ExecutionNotOwned(execution_id.to_string()));
        }

        let started_event_id: Option<Uuid> = row.get("started_event_id");
        let tool_name: String = row.get("tool_name");
        let key_name: String = row.get("key_name");
        let actor: String = row.get("actor");
        let attempts: i64 = row.get("attempts");
        let channel_id: String = row.get("channel_id");
        let params: Option<JsonValue> = row.get("params");
        let params = params.map(|v| v.0);
        let now = Utc::now();

        let payload = execution_lifecycle_payload(
            &tool_name,
            &key_name,
            &execution_id,
            attempts,
            params.as_ref(),
        );
        let (event_id, cursor) = insert_event_in_tx(
            &mut tx,
            &channel_id,
            "tool.completed",
            &payload,
            &EventMetadata {
                actor: actor.clone(),
                agent_id: None,
                user_id: None,
                metadata: Some(sonic_rs::json!({"execution_id": execution_id.to_string()})),
            },
            started_event_id,
            None,
            now,
        )
        .await?;

        let result_json = request.result.as_ref().map(|v| JsonValue::new(v.clone()));

        tx.execute(
            "UPDATE tool_executions
             SET status = 'completed',
                 result = $2,
                 error = NULL,
                 claim_token = NULL,
                 lease_until = NULL,
                 completed_event_id = $3,
                 completed_cursor = $4,
                 completed_at = NOW(),
                 updated_at = NOW()
             WHERE execution_id = $1",
            &[&execution_id, &result_json, &event_id, &cursor],
        )
        .await
        .map_err(map_quota_exec_error)?;

        crate::metrics::basic_metrics::EXECUTIONS_TOTAL
            .with_label_values(&["completed"])
            .inc();
        if let Ok(created) = row.try_get::<_, DateTime<Utc>>("created_at") {
            let secs = (now - created).num_milliseconds() as f64 / 1000.0;
            crate::metrics::basic_metrics::EXECUTION_DURATION_SECONDS.observe(secs.max(0.0));
        }

        // Report the completed result's durable bytes (the DB trigger enforces
        // the quota atomically; this outbox row feeds control-plane reporting
        // for the durable-written meter).
        let result_bytes = result_json
            .as_ref()
            .map(|r| r.0.to_string().len())
            .unwrap_or(0)
            .min(i64::MAX as usize) as i64;
        if result_bytes > 0 {
            let usage_date = now.date_naive();
            tx.execute(
                "INSERT INTO usage_delivery_outbox
                 (idempotency_key, usage_date, metric, quantity, source_generation)
                 VALUES ($1, $2::date, 'tool_result_bytes', $3, 1),
                        ($4, $2::date, 'durable_written_bytes', $3, 1)
                 ON CONFLICT (idempotency_key) DO NOTHING",
                &[
                    &format!("tool-result:{execution_id}"),
                    &usage_date,
                    &result_bytes,
                    &format!("tool-result-durable:{execution_id}"),
                ],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        }

        let info = fetch_execution_in_tx(&tx, &execution_id).await?;
        tx.commit().await.map_err(EventStoreError::Execute)?;

        let evt = lifecycle_agent_event(
            event_id,
            execution_id,
            &channel_id,
            "tool.completed",
            &payload,
            &actor,
            started_event_id,
            cursor,
            now,
        );
        Ok((info, Some(evt)))
    }

    /// Mark a claimed execution failed with a structured error, emitting a
    /// `tool.failed` lifecycle event. A duplicate fail (retried after an
    /// ambiguous transport failure) returns the stored error without emitting
    /// another event.
    #[instrument(skip(self, request), fields(execution = %execution_id))]
    pub async fn fail_execution(
        &self,
        execution_id: Uuid,
        claim_token: Option<Uuid>,
        request: &FailExecutionRequest,
    ) -> Result<(ExecutionInfo, Option<AgentEvent>), EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let mut tx = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;

        let row = tx
            .query_opt(
                &format!(
                    "SELECT {EXECUTION_COLUMNS} FROM tool_executions WHERE execution_id = $1 FOR UPDATE"
                ),
                &[&execution_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        let row = match row {
            Some(r) => r,
            None => {
                tx.rollback().await.map_err(EventStoreError::Execute)?;
                return Err(EventStoreError::ExecutionNotFound(execution_id.to_string()));
            }
        };

        let status: String = row.get("status");
        if status == "failed" {
            let info = row_to_execution(&row);
            tx.rollback().await.map_err(EventStoreError::Execute)?;
            return Ok((info, None));
        }
        if status != "running" {
            tx.rollback().await.map_err(EventStoreError::Execute)?;
            return Err(EventStoreError::ExecutionNotOwned(execution_id.to_string()));
        }
        let current_token: Option<Uuid> = row.get("claim_token");
        if current_token != claim_token {
            tx.rollback().await.map_err(EventStoreError::Execute)?;
            return Err(EventStoreError::ExecutionNotOwned(execution_id.to_string()));
        }

        let started_event_id: Option<Uuid> = row.get("started_event_id");
        let tool_name: String = row.get("tool_name");
        let key_name: String = row.get("key_name");
        let actor: String = row.get("actor");
        let attempts: i64 = row.get("attempts");
        let channel_id: String = row.get("channel_id");
        let params: Option<JsonValue> = row.get("params");
        let params = params.map(|v| v.0);
        let now = Utc::now();

        let error_payload = sonic_rs::json!({
            "error_type": request.error_type,
            "message": request.message.clone(),
            "stack": request.stack,
        });
        let error_json = JsonValue::new(error_payload.clone());
        // Self-describing `tool.failed` event: lifecycle fields plus the
        // structured error folded into the payload.
        let failed_payload = sonic_rs::json!({
            "tool": tool_name,
            "key": key_name,
            "execution_id": execution_id.to_string(),
            "attempt": attempts,
            "params": params,
            "error": error_payload,
        });

        let (event_id, cursor) = insert_event_in_tx(
            &mut tx,
            &channel_id,
            "tool.failed",
            &failed_payload,
            &EventMetadata {
                actor: actor.clone(),
                agent_id: None,
                user_id: None,
                metadata: Some(sonic_rs::json!({"execution_id": execution_id.to_string()})),
            },
            started_event_id,
            None,
            now,
        )
        .await?;

        tx.execute(
            "UPDATE tool_executions
             SET status = 'failed',
                 error = $2,
                 result = NULL,
                 claim_token = NULL,
                 lease_until = NULL,
                 completed_event_id = $3,
                 completed_cursor = $4,
                 completed_at = NOW(),
                 updated_at = NOW()
             WHERE execution_id = $1",
            &[&execution_id, &error_json, &event_id, &cursor],
        )
        .await
        .map_err(map_quota_exec_error)?;

        crate::metrics::basic_metrics::EXECUTIONS_TOTAL
            .with_label_values(&["failed"])
            .inc();

        // Report the structured error's durable bytes (the DB trigger enforces
        // the quota atomically; this outbox row feeds control-plane reporting
        // for the durable-written meter).
        let error_bytes = error_json.0.to_string().len().min(i64::MAX as usize) as i64;
        if error_bytes > 0 {
            let usage_date = now.date_naive();
            tx.execute(
                "INSERT INTO usage_delivery_outbox
                 (idempotency_key, usage_date, metric, quantity, source_generation)
                 VALUES ($1, $2::date, 'tool_error_bytes', $3, 1),
                        ($4, $2::date, 'durable_written_bytes', $3, 1)
                 ON CONFLICT (idempotency_key) DO NOTHING",
                &[
                    &format!("tool-error:{execution_id}"),
                    &usage_date,
                    &error_bytes,
                    &format!("tool-error-durable:{execution_id}"),
                ],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        }

        let info = fetch_execution_in_tx(&tx, &execution_id).await?;
        tx.commit().await.map_err(EventStoreError::Execute)?;

        let evt = lifecycle_agent_event(
            event_id,
            execution_id,
            &channel_id,
            "tool.failed",
            &failed_payload,
            &actor,
            started_event_id,
            cursor,
            now,
        );
        Ok((info, Some(evt)))
    }

    /// Extend a running execution's lease while its tool is still working.
    /// Requires the current `claim_token`; a token under a different attempt
    /// is rejected with `ExecutionNotOwned`.
    #[instrument(skip(self, request), fields(execution = %execution_id))]
    pub async fn heartbeat_execution(
        &self,
        execution_id: Uuid,
        request: &HeartbeatExecutionRequest,
    ) -> Result<ExecutionInfo, EventStoreError> {
        let claim_token: Option<Uuid> = request
            .claim_token
            .as_deref()
            .map(|t| Uuid::parse_str(t).unwrap_or(Uuid::nil()))
            .filter(|u| !u.is_nil());
        let lease = request
            .lease_seconds
            .unwrap_or(DEFAULT_EXECUTION_LEASE_SECONDS)
            .clamp(1, MAX_EXECUTION_LEASE_SECONDS);

        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        // Token mismatch yields zero updated rows; probe the row to give a
        // precise error before any state was committed.
        let updated = client
            .execute(
                "UPDATE tool_executions
                 SET lease_until = NOW() + ($1::bigint * INTERVAL '1 second'),
                     updated_at = NOW()
                 WHERE execution_id = $2 AND status = 'running' AND claim_token = $3",
                &[&lease, &execution_id, &claim_token],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        if updated == 0 {
            let row = client
                .query_opt(
                    "SELECT status FROM tool_executions WHERE execution_id = $1",
                    &[&execution_id],
                )
                .await
                .map_err(EventStoreError::Execute)?;
            return match row {
                None => Err(EventStoreError::ExecutionNotFound(execution_id.to_string())),
                Some(_) => Err(EventStoreError::ExecutionNotOwned(execution_id.to_string())),
            };
        }

        let row = client
            .query_opt(
                &format!("SELECT {EXECUTION_COLUMNS} FROM tool_executions WHERE execution_id = $1"),
                &[&execution_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        match row {
            Some(r) => Ok(row_to_execution(&r)),
            None => Err(EventStoreError::ExecutionNotFound(execution_id.to_string())),
        }
    }

    /// Cancel a running execution without storing a result. The caller is
    /// responsible for notifying the worker out-of-band; a later claim may
    /// reclaim the record.
    #[instrument(skip(self, request), fields(execution = %execution_id))]
    pub async fn cancel_execution(
        &self,
        execution_id: Uuid,
        request: &CancelExecutionRequest,
    ) -> Result<ExecutionInfo, EventStoreError> {
        let claim_token: Option<Uuid> = request
            .claim_token
            .as_deref()
            .map(|v| Uuid::parse_str(v).unwrap_or(Uuid::nil()))
            .filter(|u| !u.is_nil());

        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let updated = client
            .execute(
                "UPDATE tool_executions
                 SET status = 'cancelled',
                     claim_token = NULL,
                     lease_until = NULL,
                     completed_at = NOW(),
                     updated_at = NOW()
                 WHERE execution_id = $1 AND status = 'running' AND claim_token = $2",
                &[&execution_id, &claim_token],
            )
            .await
            .map_err(EventStoreError::Execute)?;

        if updated == 0 {
            let row = client
                .query_opt(
                    "SELECT status FROM tool_executions WHERE execution_id = $1",
                    &[&execution_id],
                )
                .await
                .map_err(EventStoreError::Execute)?;
            return match row {
                None => Err(EventStoreError::ExecutionNotFound(execution_id.to_string())),
                Some(_) => Err(EventStoreError::ExecutionNotOwned(execution_id.to_string())),
            };
        }

        self.get_execution(execution_id)
            .await?
            .ok_or_else(|| EventStoreError::ExecutionNotFound(execution_id.to_string()))
    }

    /// Fetch a single execution by id.
    pub async fn get_execution(
        &self,
        execution_id: Uuid,
    ) -> Result<Option<ExecutionInfo>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let row = client
            .query_opt(
                &format!("SELECT {EXECUTION_COLUMNS} FROM tool_executions WHERE execution_id = $1"),
                &[&execution_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(row.map(|r| row_to_execution(&r)))
    }

    /// List executions for a channel, newest first (bounded).
    pub async fn list_executions(
        &self,
        channel_id: &str,
        limit: i64,
    ) -> Result<Vec<ExecutionInfo>, EventStoreError> {
        let limit = limit.clamp(1, MAX_EXECUTION_LIST_LIMIT);
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let rows = client
            .query(
                &format!(
                    "SELECT {EXECUTION_COLUMNS} FROM tool_executions
                     WHERE channel_id = $1
                     ORDER BY created_at DESC, execution_id DESC
                     LIMIT $2"
                ),
                &[&channel_id, &limit],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(rows.iter().map(row_to_execution).collect())
    }

    /// Mark executions with expired leases as `expired` so they can be
    /// reclaimed. Returns the number of rows transitioned.
    pub async fn expire_executions(&self) -> Result<u64, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let updated = client
            .execute(
                "UPDATE tool_executions
                 SET status = 'expired',
                     claim_token = NULL,
                     lease_until = NULL,
                     updated_at = NOW()
                 WHERE status = 'running' AND lease_until < NOW()",
                &[],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(updated)
    }

    /// Delete an execution record and clear its lease. Returns true when a row
    /// was removed.
    pub async fn delete_execution(&self, execution_id: Uuid) -> Result<bool, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let removed = client
            .execute(
                "DELETE FROM tool_executions WHERE execution_id = $1",
                &[&execution_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(removed > 0)
    }

    // ── Experiment platform (Phase 9) ─────────────────────────────────

    /// Create an experiment group (baseline + variants).
    pub async fn create_experiment(
        &self,
        request: &CreateExperimentRequest,
    ) -> Result<ExperimentGroup, EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        if request.name.trim().is_empty() {
            return Err(EventStoreError::InvalidRequest(
                "experiment name is required".to_string(),
            ));
        }
        let now = Utc::now();
        let tx = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;
        let row = tx
            .query_one(
                "INSERT INTO experiment_groups (name, description, baseline_channel_id, status, created_at, updated_at)
                 VALUES ($1, $2, $3, 'running', $4, $4)
                 RETURNING group_id, name, description, baseline_channel_id, status, created_at, updated_at",
                &[
                    &request.name,
                    &request.description,
                    &request.baseline_channel_id,
                    &now,
                ],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        let group_id: Uuid = row.get("group_id");
        if let Some(baseline) = &request.baseline_channel_id {
            tx.execute(
                "INSERT INTO experiment_group_members (group_id, channel_id, role, created_at)
                 VALUES ($1, $2, 'baseline', $3)
                 ON CONFLICT (group_id, channel_id) DO NOTHING",
                &[&group_id, baseline, &now],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        }
        tx.commit().await.map_err(EventStoreError::Execute)?;
        self.get_experiment(&group_id.to_string())
            .await?
            .ok_or_else(|| EventStoreError::InconsistentState("group disappeared".into()))
    }

    /// List all experiment groups (with members).
    pub async fn list_experiments(&self) -> Result<Vec<ExperimentGroup>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let rows = client
            .query(
                "SELECT group_id, name, description, baseline_channel_id, status, created_at, updated_at
                 FROM experiment_groups ORDER BY created_at DESC",
                &[],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        let mut out = Vec::new();
        for r in rows {
            let gid: Uuid = r.get("group_id");
            let mut group = row_to_experiment_group(&r);
            group.members = self.experiment_members(&client, gid).await?;
            out.push(group);
        }
        Ok(out)
    }

    /// Fetch a single experiment group with members.
    pub async fn get_experiment(
        &self,
        group_id: &str,
    ) -> Result<Option<ExperimentGroup>, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let gid: Uuid = group_id.parse().map_err(|_| EventStoreError::NotFound)?;
        let row = client
            .query_opt(
                "SELECT group_id, name, description, baseline_channel_id, status, created_at, updated_at
                 FROM experiment_groups WHERE group_id = $1",
                &[&gid],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        let Some(row) = row else { return Ok(None) };
        let mut group = row_to_experiment_group(&row);
        group.members = self.experiment_members(&client, gid).await?;
        Ok(Some(group))
    }

    /// Add a fork to an experiment group.
    pub async fn add_experiment_member(
        &self,
        group_id: &str,
        request: &AddExperimentMemberRequest,
    ) -> Result<ExperimentGroup, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let gid: Uuid = group_id.parse().map_err(|_| EventStoreError::NotFound)?;
        if request.channel_id.trim().is_empty() {
            return Err(EventStoreError::InvalidRequest(
                "channel_id is required".to_string(),
            ));
        }
        let delta_json = request
            .declared_delta
            .as_ref()
            .map(|v| JsonValue::new(v.clone()));
        let inserted = client
            .execute(
                "INSERT INTO experiment_group_members (group_id, channel_id, role, declared_delta, created_at)
                 VALUES ($1, $2, $3, $4, NOW())
                 ON CONFLICT (group_id, channel_id) DO UPDATE
                 SET role = EXCLUDED.role, declared_delta = EXCLUDED.declared_delta",
                &[&gid, &request.channel_id, &request.role, &delta_json],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        if inserted == 0 {
            return Err(EventStoreError::GroupNotFound(group_id.to_string()));
        }
        self.get_experiment(&gid.to_string())
            .await?
            .ok_or_else(|| EventStoreError::GroupNotFound(group_id.to_string()))
    }

    /// Rank an experiment's members by result_score (desc, nulls last).
    pub async fn rank_experiment(
        &self,
        group_id: &str,
    ) -> Result<(ExperimentGroup, Vec<RankedExperimentMember>), EventStoreError> {
        let group = self
            .get_experiment(group_id)
            .await?
            .ok_or_else(|| EventStoreError::GroupNotFound(group_id.to_string()))?;
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let gid: Uuid = group_id
            .parse()
            .map_err(|_| EventStoreError::GroupNotFound(group_id.to_string()))?;
        let rows = client
            .query(
                "SELECT m.channel_id, m.role, m.declared_delta, cm.outcome, cm.result_score
                 FROM experiment_group_members m
                 LEFT JOIN channel_metadata cm ON cm.channel_id = m.channel_id
                 WHERE m.group_id = $1
                 ORDER BY cm.result_score DESC NULLS LAST, m.created_at ASC, m.channel_id ASC",
                &[&gid],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        let members = rows
            .iter()
            .map(|r| RankedExperimentMember {
                channel_id: r.get("channel_id"),
                role: r.get("role"),
                declared_delta: r
                    .try_get::<_, Option<JsonValue>>("declared_delta")
                    .ok()
                    .flatten()
                    .map(|v| v.0),
                outcome: r.try_get("outcome").ok().flatten(),
                result_score: r
                    .try_get::<_, Option<f32>>("result_score")
                    .ok()
                    .flatten()
                    .map(|s| s as f64),
            })
            .collect();
        Ok((group, members))
    }

    /// Record a fork outcome + optional score on channel metadata.
    pub async fn set_channel_outcome(
        &self,
        channel_id: &str,
        outcome: &str,
        score: Option<f64>,
    ) -> Result<ChannelMetadata, EventStoreError> {
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let score_f32 = score.map(|s| s as f32);
        let row = client
            .query_opt(
                "UPDATE channel_metadata
                 SET outcome = $2, result_score = $3
                 WHERE channel_id = $1
                 RETURNING channel_id, parent_channel_id, origin_run_id, forked_at_cursor,
                           forked_at, display_name, reason, experiment_metadata,
                           requested_at_cursor, resolved_state_cursor, resolved_event_id,
                           source_state_version, source_state_sha256, restorable, manifest,
                           reproducibility, outcome, result_score, deleted_at",
                &[&channel_id, &outcome, &score_f32],
            )
            .await
            .map_err(EventStoreError::Execute)?
            .ok_or(EventStoreError::NotFound)?;
        Ok(row_to_metadata(&row))
    }

    /// Promote a winning fork: copy its latest state into the parent
    /// channel (append-only merge), record a `run.promoted` event on the
    /// parent, and mark the child `outcome = "promoted"`. Returns the
    /// parent's new cursor.
    pub async fn promote_channel(&self, child_id: &str) -> Result<(String, i64), EventStoreError> {
        let meta = self
            .get_channel_metadata(child_id)
            .await?
            .ok_or_else(|| EventStoreError::NotFound)?;
        let parent = meta.parent_channel_id.clone().ok_or_else(|| {
            EventStoreError::InvalidRequest("cannot promote a root channel (no parent)".to_string())
        })?;
        // Resolve metadata before checking out the connection used by the
        // transaction. Holding one pool slot while this helper acquires a
        // second slot deadlocks deployments configured with a one-slot pool.
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        let now = Utc::now();
        let mut tx = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;

        // Read the winning snapshot through this transaction. Acquiring a
        // second pooled connection here deadlocks when the pool has one slot
        // and made promotion fail with a 500 on small deployments.
        let child_state = tx
            .query_opt(
                "SELECT state FROM channel_states
                 WHERE channel_id = $1 ORDER BY version DESC, cursor DESC LIMIT 1",
                &[&child_id],
            )
            .await
            .map_err(EventStoreError::Execute)?
            .map(|row| row.get::<_, JsonValue>("state"));

        // Parent boundary event id (the child's last event) for depends_on.
        let child_last = tx
            .query_opt(
                "SELECT id FROM agent_events WHERE channel_id = $1 ORDER BY cursor DESC LIMIT 1",
                &[&child_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        let depends_on: Option<Uuid> = child_last.map(|r| r.get("id"));

        // The promotion event allocates the one authoritative parent cursor.
        // The copied state is aligned to that same cursor.
        let (_, parent_cursor) = insert_event_in_tx(
            &mut tx,
            &parent,
            "run.promoted",
            &sonic_rs::json!({
                "child_channel_id": child_id,
                "source_channel_id": parent,
                "resolved_cursor": meta.resolved_state_cursor,
                "source_state_version": meta.source_state_version,
                "source_state_sha256": meta.source_state_sha256,
                "reason": meta.reason,
            }),
            &EventMetadata {
                actor: "system".to_string(),
                agent_id: None,
                user_id: None,
                metadata: None,
            },
            depends_on,
            None,
            now,
        )
        .await?;

        if let Some(state) = child_state {
            tx.execute(
                "INSERT INTO channel_states (channel_id, cursor, state, timestamp, write_source)
                 VALUES ($1, $2, $3, $4, 'fork')",
                &[&parent, &parent_cursor, &state, &now],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        }

        tx.execute(
            "UPDATE channel_metadata SET outcome = 'promoted' WHERE channel_id = $1",
            &[&child_id],
        )
        .await
        .map_err(EventStoreError::Execute)?;

        tx.commit().await.map_err(EventStoreError::Execute)?;
        Ok((parent, parent_cursor))
    }

    /// Soft-delete a channel (and, when `recursive`, its descendants) and
    /// purge the underlying events/states/cursors/executions/step rows. The
    /// channel_metadata row is kept with `deleted_at` set so idempotent
    /// replays resolve to 410 Gone instead of resurrecting a half-purged
    /// channel. Non-recursive deletes of a channel with children are
    /// rejected.
    pub async fn delete_channel(
        &self,
        channel_id: &str,
        recursive: bool,
    ) -> Result<(), EventStoreError> {
        let mut client = self.pool.get().await.map_err(EventStoreError::Pool)?;

        let mut targets = vec![channel_id.to_string()];
        if recursive {
            let mut queue = vec![channel_id.to_string()];
            while let Some(cur) = queue.pop() {
                for child in self.list_forks(&cur).await? {
                    if !targets.contains(&child.channel_id) {
                        targets.push(child.channel_id.clone());
                        queue.push(child.channel_id.clone());
                    }
                }
            }
        } else {
            let children = self.list_forks(channel_id).await?;
            if !children.is_empty() {
                return Err(EventStoreError::ChannelHasChildren {
                    channel_id: channel_id.to_string(),
                    count: children.len(),
                });
            }
        }

        let tx = client
            .transaction()
            .await
            .map_err(EventStoreError::Execute)?;
        for target in &targets {
            for sql in [
                "DELETE FROM agent_events WHERE channel_id = $1",
                "DELETE FROM channel_states WHERE channel_id = $1",
                "DELETE FROM channel_cursors WHERE channel_id = $1",
                "DELETE FROM tool_executions WHERE channel_id = $1",
                "DELETE FROM channel_steps WHERE channel_id = $1",
                "DELETE FROM experiment_group_members WHERE channel_id = $1",
            ] {
                tx.execute(sql, &[target])
                    .await
                    .map_err(EventStoreError::Execute)?;
            }
            tx.execute(
                "UPDATE channel_metadata SET deleted_at = NOW() WHERE channel_id = $1",
                &[target],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        }
        tx.commit().await.map_err(EventStoreError::Execute)?;
        Ok(())
    }

    /// "What changed?" comparison between two channels: state diff + manifest
    /// diff + tool-call diff + output diff + metrics delta (Phase 10).
    pub async fn compare_channels(
        &self,
        left: &str,
        right: &str,
    ) -> Result<CompareResponse, EventStoreError> {
        let left_meta = self.get_channel_metadata(left).await?;
        let right_meta = self.get_channel_metadata(right).await?;

        let state_diff = self.diff_channels(left, right).await?;

        // Manifest diff (empty manifests → empty diff).
        let left_manifest = left_meta
            .as_ref()
            .and_then(|m| m.manifest.clone())
            .unwrap_or_else(|| sonic_rs::json!({}));
        let right_manifest = right_meta
            .as_ref()
            .and_then(|m| m.manifest.clone())
            .unwrap_or_else(|| sonic_rs::json!({}));
        let mut manifest_entries = Vec::new();
        let mut manifest_total: usize = 0;
        walk_diff(
            &left_manifest,
            &right_manifest,
            &mut Vec::new(),
            &mut manifest_entries,
            &mut manifest_total,
            Self::MAX_DIFF_ENTRIES,
            0,
        );

        // Tool-call diff keyed by key_name.
        let tool_entries = self.tool_diff_entries(left, right).await?;
        let tool_truncated = tool_entries.len() >= Self::MAX_DIFF_ENTRIES;

        // Output comparison.
        let output_left = self.extract_output(left_meta.as_ref()).await?;
        let output_right = self.extract_output(right_meta.as_ref()).await?;
        let output_changed = !values_equal(
            output_left.as_ref().unwrap_or(&sonic_rs::json!(null)),
            output_right.as_ref().unwrap_or(&sonic_rs::json!(null)),
        );

        // Metrics delta (latency/token/cost) when present.
        let left_metrics = left_meta
            .as_ref()
            .and_then(|m| m.experiment_metadata.as_ref())
            .and_then(|v| v.get("metrics"))
            .cloned()
            .unwrap_or_else(|| sonic_rs::json!({}));
        let right_metrics = right_meta
            .as_ref()
            .and_then(|m| m.experiment_metadata.as_ref())
            .and_then(|v| v.get("metrics"))
            .cloned()
            .unwrap_or_else(|| sonic_rs::json!({}));
        let mut metrics_entries = Vec::new();
        let mut metrics_total: usize = 0;
        walk_diff(
            &left_metrics,
            &right_metrics,
            &mut Vec::new(),
            &mut metrics_entries,
            &mut metrics_total,
            Self::MAX_DIFF_ENTRIES,
            0,
        );

        Ok(CompareResponse {
            left_channel_id: left.to_string(),
            right_channel_id: right.to_string(),
            state_diff,
            manifest_diff: manifest_entries,
            manifest_diff_truncated: manifest_total > Self::MAX_DIFF_ENTRIES,
            tool_diff: tool_entries,
            tool_diff_truncated: tool_truncated,
            output_left,
            output_right,
            output_changed,
            metrics_delta: metrics_entries,
        })
    }

    /// Diff two channels' tool-execution ledgers by `key_name`, comparing the
    /// persisted results structurally. `added` = right-only, `removed` =
    /// left-only, `changed` = both present with different results.
    async fn tool_diff_entries(
        &self,
        left: &str,
        right: &str,
    ) -> Result<Vec<StateDiffEntry>, EventStoreError> {
        use std::collections::BTreeMap;
        let left_execs = self.list_executions(left, MAX_EXECUTION_LIST_LIMIT).await?;
        let right_execs = self
            .list_executions(right, MAX_EXECUTION_LIST_LIMIT)
            .await?;
        let mut lmap: BTreeMap<String, sonic_rs::Value> = BTreeMap::new();
        let mut rmap: BTreeMap<String, sonic_rs::Value> = BTreeMap::new();
        for e in &left_execs {
            lmap.insert(
                e.key_name.clone(),
                e.result.clone().unwrap_or_else(|| sonic_rs::json!(null)),
            );
        }
        for e in &right_execs {
            rmap.insert(
                e.key_name.clone(),
                e.result.clone().unwrap_or_else(|| sonic_rs::json!(null)),
            );
        }
        let mut entries = Vec::new();
        let mut keys: Vec<&str> = lmap.keys().map(|k| k.as_str()).collect();
        for k in rmap.keys() {
            if !keys.contains(&k.as_str()) {
                keys.push(k.as_str());
            }
        }
        keys.sort_unstable();
        for key in keys {
            if entries.len() >= Self::MAX_DIFF_ENTRIES {
                break;
            }
            match (lmap.get(key), rmap.get(key)) {
                (Some(l), Some(r)) => {
                    if !values_equal(l, r) {
                        entries.push(StateDiffEntry {
                            path: vec![key.to_string()],
                            kind: "changed".to_string(),
                            left: Some(l.clone()),
                            right: Some(r.clone()),
                            left_cursor: None,
                            right_cursor: None,
                        });
                    }
                }
                (Some(l), None) => {
                    entries.push(StateDiffEntry {
                        path: vec![key.to_string()],
                        kind: "removed".to_string(),
                        left: Some(l.clone()),
                        right: None,
                        left_cursor: None,
                        right_cursor: None,
                    });
                }
                (None, Some(r)) => {
                    entries.push(StateDiffEntry {
                        path: vec![key.to_string()],
                        kind: "added".to_string(),
                        left: None,
                        right: Some(r.clone()),
                        left_cursor: None,
                        right_cursor: None,
                    });
                }
                (None, None) => {}
            }
        }
        Ok(entries)
    }

    /// Extract a channel's final "output" for comparison: the
    /// `experiment_metadata.result.output` when present, else the last
    /// user-visible event's `payload.output`.
    async fn extract_output(
        &self,
        meta: Option<&ChannelMetadata>,
    ) -> Result<Option<sonic_rs::Value>, EventStoreError> {
        if let Some(m) = meta {
            if let Some(result) = m.experiment_metadata.as_ref().and_then(|v| v.get("result")) {
                if let Some(output) = result.get("output") {
                    return Ok(Some(output.clone()));
                }
            }
        }
        // Last event with a payload.output.
        let client = self.pool.get().await.map_err(EventStoreError::Pool)?;
        if let Some(meta_owner) = meta {
            let row = client
                .query_opt(
                    "SELECT payload FROM agent_events
                     WHERE channel_id = $1 AND payload ? 'output'
                     ORDER BY cursor DESC LIMIT 1",
                    &[&meta_owner.channel_id],
                )
                .await
                .map_err(EventStoreError::Execute)?;
            if let Some(r) = row {
                let payload: JsonValue = r.get("payload");
                if let Some(out) = payload.0.get("output") {
                    return Ok(Some(out.clone()));
                }
            }
        }
        Ok(None)
    }

    /// Fetch experiment members for a group.
    async fn experiment_members(
        &self,
        client: &deadpool_postgres::Client,
        group_id: Uuid,
    ) -> Result<Vec<ExperimentMember>, EventStoreError> {
        let rows = client
            .query(
                "SELECT m.channel_id, m.role, m.declared_delta, m.created_at,
                        cm.outcome, cm.result_score
                 FROM experiment_group_members m
                 LEFT JOIN channel_metadata cm ON cm.channel_id = m.channel_id
                 WHERE m.group_id = $1
                 ORDER BY m.created_at ASC",
                &[&group_id],
            )
            .await
            .map_err(EventStoreError::Execute)?;
        Ok(rows
            .iter()
            .map(|r| ExperimentMember {
                channel_id: r.get("channel_id"),
                role: r.get("role"),
                declared_delta: r
                    .try_get::<_, Option<JsonValue>>("declared_delta")
                    .ok()
                    .flatten()
                    .map(|v| v.0),
                created_at: r.get("created_at"),
                outcome: r.try_get("outcome").ok().flatten(),
                result_score: r
                    .try_get::<_, Option<f32>>("result_score")
                    .ok()
                    .flatten()
                    .map(|s| s as f64),
            })
            .collect())
    }
}

// ── Idempotent Tool Execution helpers ─────────────────────────

/// Default lease duration (seconds) granted when a claim does not specify one.
const DEFAULT_EXECUTION_LEASE_SECONDS: i64 = 60;

/// Upper bound for a claim/heartbeat lease (seconds).
const MAX_EXECUTION_LEASE_SECONDS: i64 = 86_400;

/// Upper bound for `list_executions` page size.
const MAX_EXECUTION_LIST_LIMIT: i64 = 100;

/// Column list for `tool_executions` reads so `row_to_execution` can map
/// every `ExecutionInfo` field (plus internally-used `claim_token` and
/// `request_hash`). `attempts` is widened to bigint so it reads as `i64`.
const EXECUTION_COLUMNS: &str = "execution_id, channel_id, key_name, tool_name, status, \
    attempts::bigint AS attempts, claim_token, lease_until, actor, started_event_id, \
    completed_event_id, started_cursor, completed_cursor, replay_emitted, params, result, error, \
    created_at, updated_at, completed_at, expired_at, request_hash";

/// Canonical, deterministic serialization of a JSON value: object keys are
/// sorted lexicographically, arrays preserve element order, scalars are
/// emitted compactly. Unlike `sonic_rs::to_string`, the output is stable
/// regardless of the runtime hash-map ordering of parsed objects, so the
/// same logical document always fingerprints identically.
fn canonical_json(value: &sonic_rs::Value, out: &mut String) {
    if let Some(obj) = value.as_object() {
        out.push('{');
        let mut entries: Vec<(&str, &sonic_rs::Value)> = obj.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        for (i, (key, val)) in entries.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push('"');
            out.push_str(&key.replace('\\', "\\\\").replace('"', "\\\""));
            out.push('"');
            out.push(':');
            canonical_json(val, out);
        }
        out.push('}');
    } else if let Some(arr) = value.as_array() {
        out.push('[');
        for (i, val) in arr.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            canonical_json(val, out);
        }
        out.push(']');
    } else {
        out.push_str(&sonic_rs::to_string(value).unwrap_or_default());
    }
}

/// Stable request fingerprint for `(channel_id, key_name)`. Two claims are the
/// same idempotency unit only when they name the same tool and either hash the
/// full `params`, or — with `dedup_fields` — the listed top-level `params`
/// members serialize to identical compact JSON. Object keys are sorted during
/// canonicalization (see `canonical_json`), and `dedup_fields` are treated as
/// an unordered set, so identity never depends on key or field ordering; use
/// `dedup_fields` to exclude volatile fields (timestamps, nonces, attempt ids).
fn canonical_execution_request(
    tool_name: &str,
    key_name: &str,
    dedup_fields: Option<&[String]>,
    params: Option<&sonic_rs::Value>,
) -> String {
    use sha2::{Digest, Sha256};

    let mut canonical = String::new();
    canonical.push_str(tool_name);
    canonical.push('\n');
    canonical.push_str(key_name);
    canonical.push('\n');
    match (dedup_fields, params) {
        (Some(fields), Some(value)) if !fields.is_empty() => {
            let mut parts: Vec<String> = Vec::new();
            for field in fields {
                if let Some(found) = value.get(field) {
                    let mut serialized = String::new();
                    canonical_json(found, &mut serialized);
                    parts.push(format!("{field}={serialized}"));
                }
            }
            parts.sort();
            canonical.push_str(&parts.join("&"));
        }
        (_, Some(value)) => canonical_json(value, &mut canonical),
        (Some(fields), _) => {
            let mut fields: Vec<&String> = fields.iter().collect();
            fields.sort();
            canonical.push_str(
                &fields
                    .iter()
                    .map(|f| f.as_str())
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }
        _ => {}
    }

    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let digest = hasher.finalize();
    format!("{digest:x}")
}

/// Uniform `AgentEvent` for tool lifecycle markers (`tool.started`,
/// `tool.completed`, `tool.failed`, `tool.replayed`). The execution id is
/// recorded in `metadata` so dashboards can correlate the timeline.
#[allow(clippy::too_many_arguments)]
fn lifecycle_agent_event(
    event_id: Uuid,
    execution_id: Uuid,
    channel_id: &str,
    event_type: &str,
    payload: &sonic_rs::Value,
    actor: &str,
    depends_on: Option<Uuid>,
    cursor: i64,
    now: DateTime<Utc>,
) -> AgentEvent {
    AgentEvent {
        id: event_id,
        channel_id: channel_id.to_string(),
        event_type: event_type.to_string(),
        payload: payload.clone(),
        metadata: EventMetadata {
            actor: actor.to_string(),
            agent_id: None,
            user_id: None,
            metadata: Some(sonic_rs::json!({"execution_id": execution_id.to_string()})),
        },
        cursor,
        depends_on,
        timestamp: now,
    }
}

/// Body for tool lifecycle events. `params` reuses the claim's input so the
/// timeline shows what the tool was asked to do on each attempt.
fn execution_lifecycle_payload(
    tool_name: &str,
    key_name: &str,
    execution_id: &Uuid,
    attempt: i64,
    params: Option<&sonic_rs::Value>,
) -> sonic_rs::Value {
    sonic_rs::json!({
        "tool": tool_name,
        "key": key_name,
        "execution_id": execution_id.to_string(),
        "attempt": attempt,
        "params": params,
    })
}

/// Map a fetched `tool_executions` row to the public `ExecutionInfo` shape.
fn row_to_execution(row: &tokio_postgres::Row) -> ExecutionInfo {
    ExecutionInfo {
        id: row.get("execution_id"),
        channel_id: row.get("channel_id"),
        key_name: row.get("key_name"),
        tool_name: row.get("tool_name"),
        status: row.get("status"),
        attempts: row.get("attempts"),
        lease_until: row.get("lease_until"),
        started_cursor: row.get("started_cursor"),
        completed_cursor: row.get("completed_cursor"),
        started_event_id: row.get("started_event_id"),
        completed_event_id: row.get("completed_event_id"),
        replay_emitted: row.get("replay_emitted"),
        params: row.get::<_, Option<JsonValue>>("params").map(|v| v.0),
        result: row.get::<_, Option<JsonValue>>("result").map(|v| v.0),
        error: row.get::<_, Option<JsonValue>>("error").map(|v| v.0),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        completed_at: row.get("completed_at"),
        expired_at: row.get("expired_at"),
    }
}

/// Fetch a full `tool_executions` row inside the caller's transaction.
async fn fetch_execution_in_tx(
    tx: &tokio_postgres::Transaction<'_>,
    execution_id: &Uuid,
) -> Result<ExecutionInfo, EventStoreError> {
    let row = tx
        .query_opt(
            &format!("SELECT {EXECUTION_COLUMNS} FROM tool_executions WHERE execution_id = $1"),
            &[&execution_id],
        )
        .await
        .map_err(EventStoreError::Execute)?;
    match row {
        Some(r) => Ok(row_to_execution(&r)),
        None => Err(EventStoreError::ExecutionNotFound(execution_id.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_rs::{JsonContainerTrait, JsonValueTrait};

    // ── CRITICAL #3: Execution tree depth limit ──────────────────────
    #[test]
    fn test_max_tree_depth_is_reasonable() {
        // The depth limit prevents runaway recursion on deeply nested trees.
        // It must be at least 5 (enough for realistic usage) and at most 50
        // (to stay within stack limits for recursive CTE).
        assert!(
            EventStore::MAX_TREE_DEPTH >= 5,
            "MAX_TREE_DEPTH should allow at least 5 levels"
        );
        assert!(
            EventStore::MAX_TREE_DEPTH <= 50,
            "MAX_TREE_DEPTH should be at most 50 levels"
        );
    }

    // ── Idempotent tool execution helpers ──────────────────────────

    fn sample_params() -> sonic_rs::Value {
        sonic_rs::json!({"customer": "abc", "amount": 10, "ts": "2026-01-01T00:00:00Z"})
    }

    #[test]
    fn test_canonical_execution_request_is_deterministic() {
        let a =
            canonical_execution_request("charge_customer", "pay-123", None, Some(&sample_params()));
        let b =
            canonical_execution_request("charge_customer", "pay-123", None, Some(&sample_params()));
        assert_eq!(a, b);
        assert_eq!(a.len(), 64); // hex sha-256
    }

    #[test]
    fn test_canonical_execution_request_differs_on_tool_and_key() {
        let base = canonical_execution_request("t", "k", None, Some(&sample_params()));
        let other_tool = canonical_execution_request("t2", "k", None, Some(&sample_params()));
        let other_key = canonical_execution_request("t", "k2", None, Some(&sample_params()));
        assert_ne!(base, other_tool);
        assert_ne!(base, other_key);
    }

    #[test]
    fn test_canonical_execution_request_differs_on_params() {
        let a = canonical_execution_request("t", "k", None, Some(&sonic_rs::json!({"a": 1})));
        let b = canonical_execution_request("t", "k", None, Some(&sonic_rs::json!({"a": 2})));
        assert_ne!(a, b);
    }

    #[test]
    fn test_canonical_execution_request_dedup_fields_ignore_volatile() {
        let with_ts = canonical_execution_request(
            "t",
            "k",
            Some(&["customer".to_string(), "amount".to_string()]),
            Some(&sonic_rs::json!({"customer": "abc", "amount": 10, "ts": "a"})),
        );
        let different_ts = canonical_execution_request(
            "t",
            "k",
            Some(&["customer".to_string(), "amount".to_string()]),
            Some(&sonic_rs::json!({"customer": "abc", "amount": 10, "ts": "b"})),
        );
        assert_eq!(with_ts, different_ts);

        // Without dedup_fields the full document is hashed and they differ.
        let full_a =
            canonical_execution_request("t", "k", None, Some(&sonic_rs::json!({"ts": "a"})));
        let full_b =
            canonical_execution_request("t", "k", None, Some(&sonic_rs::json!({"ts": "b"})));
        assert_ne!(full_a, full_b);
    }

    #[test]
    fn test_canonical_execution_request_dedup_field_order_insensitive() {
        let a = canonical_execution_request(
            "t",
            "k",
            Some(&["a".to_string(), "b".to_string()]),
            Some(&sonic_rs::json!({"a": 1, "b": 2, "x": 9})),
        );
        let b = canonical_execution_request(
            "t",
            "k",
            Some(&["b".to_string(), "a".to_string()]),
            Some(&sonic_rs::json!({"a": 1, "b": 2, "x": 99})),
        );
        assert_eq!(a, b);
    }

    #[test]
    fn test_canonical_execution_request_none_params() {
        let a = canonical_execution_request("t", "k", None, None);
        let b = canonical_execution_request("t", "k", None, None);
        assert_eq!(a, b);
        assert_ne!(a, canonical_execution_request("t2", "k", None, None));
    }

    #[test]
    fn test_execution_lifecycle_payload_shape() {
        let params = sonic_rs::json!({"a": 1});
        let payload = execution_lifecycle_payload(
            "charge_customer",
            "pay-123",
            &Uuid::new_v4(),
            3,
            Some(&params),
        );
        let obj = payload.as_object().expect("payload must be an object");
        assert_eq!(obj["tool"].as_str(), Some("charge_customer"));
        assert_eq!(obj["key"].as_str(), Some("pay-123"));
        assert_eq!(obj["attempt"].as_i64(), Some(3));
        assert_eq!(obj["params"]["a"].as_i64(), Some(1));
        assert!(obj["execution_id"].is_str());
    }

    #[test]
    fn test_execution_lifecycle_payload_null_params() {
        let payload = execution_lifecycle_payload("t", "k", &Uuid::new_v4(), 1, None);
        let obj = payload.as_object().expect("payload must be an object");
        assert!(obj["params"].is_null());
    }

    #[test]
    fn test_lease_constants() {
        assert_eq!(DEFAULT_EXECUTION_LEASE_SECONDS, 60);
        assert!(MAX_EXECUTION_LEASE_SECONDS >= DEFAULT_EXECUTION_LEASE_SECONDS);
        assert!(MAX_EXECUTION_LIST_LIMIT > 0);
    }

    // ── Canonical JSON equality (json_values_equal) ────────────────

    #[test]
    fn json_values_equal_is_key_order_insensitive() {
        let a = sonic_rs::json!({"b": 2, "a": 1, "c": {"y": 9, "x": 8}});
        let b = sonic_rs::json!({"c": {"x": 8, "y": 9}, "a": 1, "b": 2});
        assert!(json_values_equal(&a, &b));
    }

    #[test]
    fn json_values_equal_nested_objects() {
        let a = sonic_rs::json!({"outer": {"inner": {"k": "v"}}});
        let same = sonic_rs::json!({"outer": {"inner": {"k": "v"}}});
        assert!(json_values_equal(&a, &same));
        let changed = sonic_rs::json!({"outer": {"inner": {"k": "v2"}}});
        assert!(!json_values_equal(&a, &changed));
    }

    #[test]
    fn json_values_equal_arrays_are_order_sensitive() {
        let a = sonic_rs::json!({"arr": [1, 2, 3]});
        let b = sonic_rs::json!({"arr": [3, 2, 1]});
        assert!(!json_values_equal(&a, &b));
        assert!(json_values_equal(&a, &a));
    }

    #[test]
    fn json_values_equal_scalars() {
        assert!(json_values_equal(&sonic_rs::json!(1), &sonic_rs::json!(1)));
        assert!(!json_values_equal(&sonic_rs::json!(1), &sonic_rs::json!(2)));
        assert!(json_values_equal(
            &sonic_rs::json!("x"),
            &sonic_rs::json!("x")
        ));
        assert!(!json_values_equal(
            &sonic_rs::json!("x"),
            &sonic_rs::json!("y")
        ));
        assert!(json_values_equal(
            &sonic_rs::json!(true),
            &sonic_rs::json!(true)
        ));
        assert!(!json_values_equal(
            &sonic_rs::json!(true),
            &sonic_rs::json!(false)
        ));
        assert!(json_values_equal(
            &sonic_rs::json!(null),
            &sonic_rs::json!(null)
        ));
        assert!(!json_values_equal(
            &sonic_rs::json!(null),
            &sonic_rs::json!(1)
        ));
    }

    #[test]
    fn json_values_equal_number_handling() {
        let big = sonic_rs::json!(9223372036854775807i64);
        assert!(json_values_equal(&big, &big));
        assert!(json_values_equal(
            &sonic_rs::json!(0.5),
            &sonic_rs::json!(0.5)
        ));
        assert!(!json_values_equal(
            &sonic_rs::json!(0.5),
            &sonic_rs::json!(0.6)
        ));
        // Numeric cross-type (int vs float) is NOT equal.
        assert!(!json_values_equal(
            &sonic_rs::json!(1),
            &sonic_rs::json!(1.0)
        ));
    }

    // ── values_equal primitives ────────────────────────────────────

    #[test]
    fn values_equal_handles_primitives() {
        assert!(values_equal(&sonic_rs::json!(7i64), &sonic_rs::json!(7i64)));
        assert!(!values_equal(
            &sonic_rs::json!(7i64),
            &sonic_rs::json!(8i64)
        ));
        assert!(values_equal(&sonic_rs::json!(7u64), &sonic_rs::json!(7u64)));
        assert!(values_equal(&sonic_rs::json!(7.5), &sonic_rs::json!(7.5)));
        assert!(!values_equal(&sonic_rs::json!(7.5), &sonic_rs::json!(8.5)));
        assert!(values_equal(&sonic_rs::json!("s"), &sonic_rs::json!("s")));
        assert!(!values_equal(&sonic_rs::json!("s"), &sonic_rs::json!("t")));
        assert!(values_equal(&sonic_rs::json!(true), &sonic_rs::json!(true)));
        assert!(values_equal(&sonic_rs::json!(null), &sonic_rs::json!(null)));
    }

    #[test]
    fn values_equal_object_fallback_is_string_based() {
        // Objects fall back to a raw serialization compare: identical key
        // order compares equal; reordered keys compare different. This is
        // deliberately narrower than json_values_equal (which is used where a
        // JSONB round-trip has already normalized key order). Values are built
        // from strings because sonic_rs::json! does not guarantee key order.
        let a: sonic_rs::Value = sonic_rs::from_str(r#"{"x": 1, "y": 2}"#).unwrap();
        let b: sonic_rs::Value = sonic_rs::from_str(r#"{"x": 1, "y": 2}"#).unwrap();
        assert!(values_equal(&a, &b));
        let reordered: sonic_rs::Value = sonic_rs::from_str(r#"{"y": 2, "x": 1}"#).unwrap();
        assert!(!values_equal(&a, &reordered));
    }

    // ── fork_request_hash determinism ──────────────────────────────

    fn sample_fork() -> ForkRequest {
        ForkRequest {
            source_channel_id: "parent".into(),
            new_channel_id: "child".into(),
            at_cursor: 5,
            operation_id: None,
            display_name: Some("variant".into()),
            reason: Some("counterfactual".into()),
            experiment_metadata: Some(sonic_rs::json!({"forked_at_step": 5})),
            manifest: Some(sonic_rs::json!({"model": "gpt"})),
            tool_policies: None,
            expected_version: None,
            expected_cursor: None,
        }
    }

    #[test]
    fn fork_request_hash_is_deterministic_sha256_hex() {
        let a = fork_request_hash(&sample_fork());
        let b = fork_request_hash(&sample_fork());
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn fork_request_hash_drifts_on_boundary_and_identity() {
        let base = fork_request_hash(&sample_fork());
        let mut cursor = sample_fork();
        cursor.at_cursor = 6;
        assert_ne!(base, fork_request_hash(&cursor));
        let mut child = sample_fork();
        child.new_channel_id = "child2".into();
        assert_ne!(base, fork_request_hash(&child));
        let mut source = sample_fork();
        source.source_channel_id = "parent2".into();
        assert_ne!(base, fork_request_hash(&source));
    }

    #[test]
    fn fork_request_hash_sensitive_to_metadata_and_manifest() {
        let base = fork_request_hash(&sample_fork());
        let mut no_meta = sample_fork();
        no_meta.experiment_metadata = None;
        assert_ne!(base, fork_request_hash(&no_meta));
        let mut no_manifest = sample_fork();
        no_manifest.manifest = None;
        assert_ne!(base, fork_request_hash(&no_manifest));
        let mut no_display = sample_fork();
        no_display.display_name = None;
        assert_ne!(base, fork_request_hash(&no_display));
        let mut no_reason = sample_fork();
        no_reason.reason = None;
        assert_ne!(base, fork_request_hash(&no_reason));
    }

    #[test]
    fn fork_request_hash_ignores_absent_tool_policies_but_tracks_present_ones() {
        // A policy-free fork must hash identically to the pre-v29 algorithm so
        // an in-flight fork retry still replays rather than reporting a
        // spurious idempotency conflict.
        let legacy = sample_fork();
        assert!(legacy.tool_policies.is_none());
        let mut explicit_none = sample_fork();
        explicit_none.tool_policies = None;
        assert_eq!(
            fork_request_hash(&legacy),
            fork_request_hash(&explicit_none)
        );

        // A present policy participates in the request identity.
        let mut with_policy = sample_fork();
        with_policy.tool_policies = Some(sonic_rs::json!({"charge": "replay"}));
        assert_ne!(fork_request_hash(&legacy), fork_request_hash(&with_policy));

        // Changing the policy changes the identity.
        let mut other_policy = sample_fork();
        other_policy.tool_policies = Some(sonic_rs::json!({"charge": "block"}));
        assert_ne!(
            fork_request_hash(&with_policy),
            fork_request_hash(&other_policy)
        );
    }
}
