// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

// Lightweight migration system for the event store.
//
// Migrations are embedded in the binary as (version, name, sql) tuples.
// The `_migrations` table tracks which versions have been applied.
// Most SQL is idempotent (CREATE IF NOT EXISTS, ADD COLUMN IF NOT EXISTS);
// the few non-idempotent statements (e.g. v16's RENAME COLUMN) are safe
// because each migration's SQL and its ledger insert commit in ONE
// transaction — a crash rolls both back and the migration re-runs cleanly.

use deadpool_postgres::Pool;
use tracing::info;

/// A single migration step.
struct Migration {
    version: i32,
    name: &'static str,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "create_agent_events",
        sql: r#"
CREATE TABLE IF NOT EXISTS agent_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cursor BIGSERIAL NOT NULL,
    txid XID8 NOT NULL DEFAULT pg_current_xact_id(),
    channel_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    actor TEXT NOT NULL,
    agent_id TEXT,
    user_id TEXT,
    metadata JSONB,
    channel_cursor BIGINT,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_events_channel_cursor
    ON agent_events (channel_id, cursor);

CREATE INDEX IF NOT EXISTS idx_agent_events_replay
    ON agent_events (channel_id, cursor ASC, txid);
        "#,
    },
    Migration {
        version: 2,
        name: "create_channel_cursors",
        sql: r#"
CREATE TABLE IF NOT EXISTS channel_cursors (
    channel_id TEXT NOT NULL,
    last_cursor BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (channel_id)
);

ALTER TABLE agent_events ADD COLUMN IF NOT EXISTS channel_cursor BIGINT;
ALTER TABLE agent_events ADD COLUMN IF NOT EXISTS txid XID8 NOT NULL DEFAULT pg_current_xact_id();

CREATE INDEX IF NOT EXISTS idx_agent_events_channel_cursor_seq
    ON agent_events (channel_id, channel_cursor);
        "#,
    },
    Migration {
        version: 3,
        name: "create_dead_letter_queue",
        sql: r#"
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
CREATE INDEX IF NOT EXISTS idx_dlq_channel ON broadcast_dead_letters (channel_id, created_at);
CREATE INDEX IF NOT EXISTS idx_dlq_connection ON broadcast_dead_letters (connection_id);
CREATE INDEX IF NOT EXISTS idx_dlq_created ON broadcast_dead_letters (created_at);
        "#,
    },
    Migration {
        version: 4,
        name: "create_channel_states",
        sql: r#"
CREATE TABLE IF NOT EXISTS channel_states (
    channel_id TEXT NOT NULL,
    cursor BIGINT NOT NULL,
    state JSONB NOT NULL DEFAULT '{}',
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (channel_id)
);

CREATE INDEX IF NOT EXISTS idx_channel_states_channel
    ON channel_states (channel_id, cursor DESC);
        "#,
    },
    Migration {
        version: 5,
        name: "create_channel_metadata",
        sql: r#"
CREATE TABLE IF NOT EXISTS channel_metadata (
    channel_id TEXT PRIMARY KEY,
    parent_channel_id TEXT,
    origin_run_id TEXT NOT NULL,
    forked_at_cursor BIGINT NOT NULL DEFAULT 0,
    forked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    display_name TEXT,
    reason TEXT,
    experiment_metadata JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_channel_metadata_parent
    ON channel_metadata (parent_channel_id);

CREATE INDEX IF NOT EXISTS idx_channel_metadata_origin
    ON channel_metadata (origin_run_id);
        "#,
    },
    Migration {
        version: 6,
        name: "state_snapshots_removed",
        sql: r#"
-- The label-based snapshot API was removed; versioned state lives in
-- channel_states (migration v4/v7). This slot is retained as a no-op so the
-- migration ledger stays consecutive. Migration v14 drops any state_snapshots
-- table left behind by older deployments.
SELECT 1;
        "#,
    },
    Migration {
        version: 7,
        name: "versioned_channel_states",
        sql: r#"
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'channel_states' AND column_name = 'version'
    ) THEN
        ALTER TABLE channel_states DROP CONSTRAINT IF EXISTS channel_states_pkey CASCADE;
        ALTER TABLE channel_states ADD COLUMN version BIGSERIAL NOT NULL;
        ALTER TABLE channel_states ADD PRIMARY KEY (channel_id, version);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_channel_states_latest
    ON channel_states (channel_id, version DESC);
CREATE INDEX IF NOT EXISTS idx_channel_states_cursor
    ON channel_states (channel_id, cursor);
        "#,
    },
    Migration {
        version: 8,
        name: "add_depends_on_column",
        sql: r#"
ALTER TABLE agent_events ADD COLUMN IF NOT EXISTS depends_on UUID;
CREATE INDEX IF NOT EXISTS idx_agent_events_depends_on
    ON agent_events (depends_on);
        "#,
    },
    Migration {
        version: 9,
        name: "drop_tenant_from_agent_events",
        sql: r#"
DROP INDEX IF EXISTS idx_agent_events_tenant_channel;
ALTER TABLE agent_events DROP COLUMN IF EXISTS tenant_id;
        "#,
    },
    Migration {
        version: 10,
        name: "create_fork_operations",
        sql: r#"
CREATE TABLE IF NOT EXISTS fork_operations (
    operation_id      TEXT PRIMARY KEY,
    child_channel_id  TEXT NOT NULL UNIQUE,
    request_hash      TEXT NOT NULL,
    source_channel_id TEXT NOT NULL,
    at_cursor         BIGINT NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_at        TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fork_operations_child
    ON fork_operations (child_channel_id);
        "#,
    },
    Migration {
        version: 11,
        name: "create_consumer_groups",
        sql: r#"
CREATE TABLE IF NOT EXISTS consumer_groups (
    group_id   TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata   JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_consumer_groups_channel
    ON consumer_groups (channel_id);

CREATE TABLE IF NOT EXISTS group_consumers (
    group_id       TEXT NOT NULL REFERENCES consumer_groups(group_id) ON DELETE CASCADE,
    consumer_id    TEXT NOT NULL,
    lease_until    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status         TEXT NOT NULL DEFAULT 'active',
    PRIMARY KEY (group_id, consumer_id)
);

CREATE TABLE IF NOT EXISTS group_offsets (
    group_id       TEXT NOT NULL REFERENCES consumer_groups(group_id) ON DELETE CASCADE,
    consumer_id    TEXT NOT NULL,
    last_cursor    BIGINT NOT NULL DEFAULT 0,
    claimed_cursor BIGINT NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, consumer_id)
);
        "#,
    },
    Migration {
        version: 12,
        name: "create_scheduled_wakeups",
        sql: r#"
CREATE TABLE IF NOT EXISTS scheduled_wakeups (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id      TEXT NOT NULL,
    run_at          TIMESTAMPTZ NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    payload         JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    executed_at     TIMESTAMPTZ,
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    error           TEXT
);

CREATE INDEX IF NOT EXISTS idx_scheduled_wakeups_due
    ON scheduled_wakeups (status, next_attempt_at, run_at);
CREATE INDEX IF NOT EXISTS idx_scheduled_wakeups_channel
    ON scheduled_wakeups (channel_id);
        "#,
    },
    Migration {
        version: 13,
        name: "create_tool_executions",
        sql: r#"
CREATE TABLE IF NOT EXISTS tool_executions (
    execution_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id         TEXT NOT NULL,
    key_name           TEXT NOT NULL,
    tool_name          TEXT NOT NULL,
    request_hash       TEXT NOT NULL,
    status             TEXT NOT NULL DEFAULT 'running',
    attempts           INTEGER NOT NULL DEFAULT 1,
    claim_token        UUID,
    lease_until        TIMESTAMPTZ,
    actor              TEXT NOT NULL DEFAULT 'system',
    started_event_id   UUID,
    completed_event_id UUID,
    started_cursor     BIGINT,
    completed_cursor   BIGINT,
    replay_emitted     BOOLEAN NOT NULL DEFAULT FALSE,
    params             JSONB DEFAULT '{}',
    result             JSONB,
    error              JSONB,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at       TIMESTAMPTZ,
    CONSTRAINT tool_executions_channel_key UNIQUE (channel_id, key_name)
);

CREATE INDEX IF NOT EXISTS idx_tool_executions_status
    ON tool_executions (status);
CREATE INDEX IF NOT EXISTS idx_tool_executions_channel
    ON tool_executions (channel_id, created_at);
        "#,
    },
    Migration {
        version: 14,
        name: "drop_state_snapshots",
        sql: r#"
DROP TABLE IF EXISTS state_snapshots;
        "#,
    },
    Migration {
        version: 15,
        name: "events_operation_id",
        sql: r#"
-- Client-supplied idempotency key for record: (channel_id, operation_id)
-- must be unique so retried records replay the original event instead of
-- duplicating. Partial index keeps the NULL (unset) case unconstrained.
ALTER TABLE public.agent_events ADD COLUMN IF NOT EXISTS operation_id UUID;
CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_events_operation_id
    ON public.agent_events (channel_id, operation_id)
    WHERE operation_id IS NOT NULL;
        "#,
    },
    Migration {
        version: 16,
        name: "per_channel_cursors",
        sql: r#"
-- Public cursors become per-channel. The old `cursor` (a server-wide
-- BIGSERIAL) is renamed `global_cursor` and becomes an internal ordering
-- detail (cross-channel query order); the old `channel_cursor` becomes the
-- public `cursor` — a gapless, per-channel sequence starting at 1.
--
-- Every row is renumbered so pre-v2 NULLs (and any historical gaps) get
-- consistent per-channel positions; gapless values already match their
-- global order, so ordinary channels are unchanged. Tables that store
-- event positions (channel_states, channel_metadata, fork_operations,
-- tool_executions, group_offsets) are re-mapped global -> per-channel
-- through agent_events; rows without a matching event keep their old
-- value (0 / default) rather than failing the migration.
--
-- The whole body is guarded on `global_cursor` not existing yet, so a
-- re-run (e.g. a crash between the SQL batch and the ledger insert in an
-- older binary, or a manual ledger repair) is a clean no-op instead of
-- failing on the non-idempotent RENAME COLUMN statements.

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'agent_events'
      AND column_name = 'global_cursor'
  ) THEN
    UPDATE agent_events SET channel_cursor = seq.pc
    FROM (
        SELECT id,
               ROW_NUMBER() OVER (PARTITION BY channel_id ORDER BY cursor) AS pc
        FROM agent_events
    ) seq
    WHERE agent_events.id = seq.id;

    ALTER TABLE agent_events RENAME COLUMN cursor TO global_cursor;
    ALTER TABLE agent_events RENAME COLUMN channel_cursor TO cursor;
    ALTER TABLE agent_events ALTER COLUMN cursor SET NOT NULL;

    UPDATE channel_states cs SET cursor = ae.cursor
    FROM agent_events ae
    WHERE ae.channel_id = cs.channel_id AND ae.global_cursor = cs.cursor;

    UPDATE channel_metadata cm SET forked_at_cursor = ae.cursor
    FROM agent_events ae
    WHERE ae.channel_id = cm.parent_channel_id AND ae.global_cursor = cm.forked_at_cursor;

    UPDATE fork_operations fo SET at_cursor = ae.cursor
    FROM agent_events ae
    WHERE ae.channel_id = fo.source_channel_id AND ae.global_cursor = fo.at_cursor;

    UPDATE tool_executions te SET started_cursor = ae.cursor
    FROM agent_events ae
    WHERE ae.channel_id = te.channel_id AND ae.global_cursor = te.started_cursor;

    UPDATE tool_executions te SET completed_cursor = ae.cursor
    FROM agent_events ae
    WHERE ae.channel_id = te.channel_id AND ae.global_cursor = te.completed_cursor;

    UPDATE group_offsets go SET last_cursor = ae.cursor
    FROM agent_events ae, consumer_groups cg
    WHERE cg.group_id = go.group_id AND ae.channel_id = cg.channel_id
      AND ae.global_cursor = go.last_cursor;

    UPDATE group_offsets go SET claimed_cursor = ae.cursor
    FROM agent_events ae, consumer_groups cg
    WHERE cg.group_id = go.group_id AND ae.channel_id = cg.channel_id
      AND ae.global_cursor = go.claimed_cursor;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_agent_events_channel_cursor_seq
    ON agent_events (channel_id, cursor);

-- DB-level backstop on the per-channel gapless sequence: the counter in
-- channel_cursors is the primary guarantee, but a UNIQUE constraint makes a
-- duplicate (channel_id, cursor) impossible even under a future bug, a
-- manual fix, or a data import. Backfilled above via ROW_NUMBER, so every
-- row already has a unique value.
CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_events_channel_cursor_unique
    ON agent_events (channel_id, cursor);

UPDATE channel_states cs SET cursor = ae.cursor
FROM agent_events ae
WHERE ae.channel_id = cs.channel_id AND ae.global_cursor = cs.cursor;

UPDATE channel_metadata cm SET forked_at_cursor = ae.cursor
FROM agent_events ae
WHERE ae.channel_id = cm.parent_channel_id AND ae.global_cursor = cm.forked_at_cursor;

UPDATE fork_operations fo SET at_cursor = ae.cursor
FROM agent_events ae
WHERE ae.channel_id = fo.source_channel_id AND ae.global_cursor = fo.at_cursor;

UPDATE tool_executions te SET started_cursor = ae.cursor
FROM agent_events ae
WHERE ae.channel_id = te.channel_id AND ae.global_cursor = te.started_cursor;

UPDATE tool_executions te SET completed_cursor = ae.cursor
FROM agent_events ae
WHERE ae.channel_id = te.channel_id AND ae.global_cursor = te.completed_cursor;

UPDATE group_offsets go SET last_cursor = ae.cursor
FROM agent_events ae, consumer_groups cg
WHERE cg.group_id = go.group_id AND ae.channel_id = cg.channel_id
  AND ae.global_cursor = go.last_cursor;

UPDATE group_offsets go SET claimed_cursor = ae.cursor
FROM agent_events ae, consumer_groups cg
WHERE cg.group_id = go.group_id AND ae.channel_id = cg.channel_id
  AND ae.global_cursor = go.claimed_cursor;
        "#,
    },
    Migration {
        version: 17,
        name: "fork_boundary_provenance",
        sql: r#"
-- Fork boundary provenance: a fork now records BOTH the requested boundary
-- and the actual resolved boundary (the snapshot version/cursor the child
-- inherited), plus an immutable state fingerprint and an optional manifest.
--
-- `forked_at_cursor` now means the RESOLVED boundary cursor (was: the
-- requested cursor). Pre-v17 rows are backfilled with
-- resolved = forked_at_cursor (i.e. treated as if the request was exact).
--
-- Column additions are idempotent (ADD COLUMN IF NOT EXISTS). The backfill
-- DO-block is guarded the same way, so a crash between the batch and the
-- ledger insert re-runs cleanly.

ALTER TABLE channel_metadata ADD COLUMN IF NOT EXISTS requested_at_cursor BIGINT NOT NULL DEFAULT 0;
ALTER TABLE channel_metadata ADD COLUMN IF NOT EXISTS resolved_state_cursor BIGINT NOT NULL DEFAULT 0;
ALTER TABLE channel_metadata ADD COLUMN IF NOT EXISTS resolved_event_id UUID;
ALTER TABLE channel_metadata ADD COLUMN IF NOT EXISTS source_state_version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE channel_metadata ADD COLUMN IF NOT EXISTS source_state_sha256 TEXT;
ALTER TABLE channel_metadata ADD COLUMN IF NOT EXISTS restorable BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE channel_metadata ADD COLUMN IF NOT EXISTS manifest JSONB DEFAULT '{}';
ALTER TABLE channel_metadata ADD COLUMN IF NOT EXISTS reproducibility TEXT;
ALTER TABLE channel_metadata ADD COLUMN IF NOT EXISTS outcome TEXT;
ALTER TABLE channel_metadata ADD COLUMN IF NOT EXISTS result_score REAL;
ALTER TABLE channel_metadata ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

ALTER TABLE fork_operations ADD COLUMN IF NOT EXISTS requested_at_cursor BIGINT NOT NULL DEFAULT 0;
ALTER TABLE fork_operations ADD COLUMN IF NOT EXISTS resolved_state_cursor BIGINT NOT NULL DEFAULT 0;
ALTER TABLE fork_operations ADD COLUMN IF NOT EXISTS resolved_event_id UUID;
ALTER TABLE fork_operations ADD COLUMN IF NOT EXISTS source_state_version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE fork_operations ADD COLUMN IF NOT EXISTS source_state_sha256 TEXT;
ALTER TABLE fork_operations ADD COLUMN IF NOT EXISTS restorable BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE fork_operations ADD COLUMN IF NOT EXISTS manifest JSONB DEFAULT '{}';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'channel_metadata'
          AND column_name = 'resolved_state_cursor'
          AND column_default IS NULL
    ) THEN
        UPDATE channel_metadata
        SET resolved_state_cursor = forked_at_cursor,
            requested_at_cursor = forked_at_cursor,
            restorable = (forked_at_cursor > 0)
        WHERE resolved_state_cursor = 0;
    END IF;
END $$;
        "#,
    },
    Migration {
        version: 18,
        name: "channel_steps",
        sql: r#"
-- Server-owned, durable step -> cursor index. AgentSession.step() records a
-- (channel_id, step_number, cursor, event_id) row atomically with the event,
-- so step->cursor resolution and fork_at_step lineage resolution are exact
-- and server-side instead of being inferred from mutable metadata + replay.
CREATE TABLE IF NOT EXISTS channel_steps (
    channel_id TEXT NOT NULL,
    step_number BIGINT NOT NULL,
    cursor BIGINT NOT NULL,
    event_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (channel_id, step_number)
);

CREATE INDEX IF NOT EXISTS idx_channel_steps_cursor
    ON channel_steps (channel_id, cursor);
        "#,
    },
    Migration {
        version: 19,
        name: "experiment_groups",
        sql: r#"
-- First-class experiment groups: a baseline run plus N variant forks with
-- declared parameter deltas. Members are ranked by their stored result_score.
CREATE TABLE IF NOT EXISTS experiment_groups (
    group_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    description TEXT,
    baseline_channel_id TEXT,
    status TEXT NOT NULL DEFAULT 'running',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_experiment_groups_created
    ON experiment_groups (created_at DESC);

CREATE TABLE IF NOT EXISTS experiment_group_members (
    group_id UUID NOT NULL REFERENCES experiment_groups(group_id) ON DELETE CASCADE,
    channel_id TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'variant',
    declared_delta JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, channel_id)
);

CREATE INDEX IF NOT EXISTS idx_experiment_members_channel
    ON experiment_group_members (channel_id);
        "#,
    },
    Migration {
        version: 20,
        name: "event_archive",
        sql: r#"
-- Provenance archive (audit item 13): a durable copy of agent_events rows
-- that retention purges, so evidence (depends_on chains, fork boundary
-- events, decision trails) survives the live-table prune and can be
-- audited/recovered. Written inside the same transaction as the DELETE.
-- Columns mirror agent_events AFTER migration v16 (per-channel cursors):
-- `global_cursor` is the internal ordering id, `cursor` the public
-- per-channel sequence.
CREATE TABLE IF NOT EXISTS event_archive (
    id UUID NOT NULL,
    global_cursor BIGINT NOT NULL,
    channel_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    actor TEXT NOT NULL,
    agent_id TEXT,
    user_id TEXT,
    metadata JSONB,
    depends_on UUID,
    operation_id UUID,
    cursor BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_event_archive_channel_ts
    ON event_archive (channel_id, timestamp);

CREATE INDEX IF NOT EXISTS idx_event_archive_channel_cursor
    ON event_archive (channel_id, cursor);
        "#,
    },
    Migration {
        version: 21,
        name: "event_hash_chain",
        sql: r#"
-- Tamper-evident execution ledger (audit selling point 3): every event
-- carries `hash = sha256(channel_id | cursor | event_type | canonical
-- payload | prev_hash)` and the previous event's hash. Any mutation of a
-- recorded event (payload, cursor, type, or the chain link itself) breaks
-- the chain and is detected by GET /api/v1/events/verify/{channel_id}.
--
-- Existing rows get NULL hashes (pre-ledger era) — they cannot be verified
-- retroactively without risking an accidental rewrite. The chain begins at
-- the first event recorded after this migration; its prev_hash is NULL.
ALTER TABLE agent_events ADD COLUMN IF NOT EXISTS hash TEXT;
ALTER TABLE agent_events ADD COLUMN IF NOT EXISTS prev_hash TEXT;

CREATE INDEX IF NOT EXISTS idx_agent_events_chain
    ON agent_events (channel_id, cursor) WHERE hash IS NOT NULL;
        "#,
    },
    Migration {
        version: 22,
        name: "channels_registry",
        sql: r#"
-- First-class channel registry (audit item 14): explicit existence, lifecycle
-- timestamps, owner, lineage (parent + fork cursor), retention policy and
-- integrity status — the anchor for durable-execution lifecycle management
-- and analytics. Populated on first write, on fork, and on save_state.
CREATE TABLE IF NOT EXISTS channels (
    channel_id TEXT PRIMARY KEY,
    display_name TEXT,
    owner TEXT,
    parent_channel_id TEXT,
    forked_at_cursor BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at TIMESTAMPTZ,
    retention_policy JSONB,
    integrity_status TEXT NOT NULL DEFAULT 'ok',
    last_cursor BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_channels_parent ON channels (parent_channel_id);
CREATE INDEX IF NOT EXISTS idx_channels_created ON channels (created_at DESC);

-- Backfill existing channels so list/overview queries can read the registry
-- instead of the union of several tables.
INSERT INTO channels (channel_id, created_at, last_cursor)
SELECT c.channel_id, COALESCE(e.first_ts, NOW()), c.last_cursor
FROM channel_cursors c
LEFT JOIN (
    SELECT channel_id, MIN(timestamp) AS first_ts
    FROM agent_events GROUP BY channel_id
) e ON e.channel_id = c.channel_id
ON CONFLICT (channel_id) DO NOTHING;
        "#,
    },
    Migration {
        version: 23,
        name: "usage_delivery_outbox",
        sql: r#"
-- Billing usage is written in the SAME transaction as accepted event/state
-- data. Delivery is asynchronous and idempotent; process metrics are never
-- an invoice source.
CREATE TABLE IF NOT EXISTS usage_delivery_outbox (
    idempotency_key TEXT PRIMARY KEY,
    usage_date DATE NOT NULL,
    metric TEXT NOT NULL,
    quantity BIGINT NOT NULL CHECK (quantity >= 0),
    source_generation BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_usage_delivery_outbox_created
    ON usage_delivery_outbox (created_at);
        "#,
    },
    Migration {
        version: 24,
        name: "monthly_ingress_quotas",
        sql: r#"
CREATE TABLE IF NOT EXISTS usage_quota_config (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    max_events_month BIGINT NOT NULL DEFAULT 0 CHECK (max_events_month >= 0),
    max_ingress_bytes_month BIGINT NOT NULL DEFAULT 0 CHECK (max_ingress_bytes_month >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO usage_quota_config (singleton) VALUES (TRUE) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS usage_quota_monthly (
    usage_month DATE PRIMARY KEY,
    events_ingested BIGINT NOT NULL DEFAULT 0 CHECK (events_ingested >= 0),
    ingress_bytes BIGINT NOT NULL DEFAULT 0 CHECK (ingress_bytes >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO usage_quota_monthly (usage_month, events_ingested, ingress_bytes)
SELECT date_trunc('month', timestamp AT TIME ZONE 'UTC')::date,
       COUNT(*),
       SUM(octet_length(payload::text) + COALESCE(octet_length(metadata::text), 0))
FROM agent_events
GROUP BY 1
ON CONFLICT (usage_month) DO NOTHING;

CREATE OR REPLACE FUNCTION enforce_monthly_event_quota() RETURNS TRIGGER AS $$
DECLARE
    limits usage_quota_config%ROWTYPE;
    month_key DATE;
    event_bytes BIGINT;
    accepted DATE;
BEGIN
    SELECT * INTO limits FROM usage_quota_config WHERE singleton = TRUE;
    month_key := date_trunc('month', NEW.timestamp AT TIME ZONE 'UTC')::date;
    event_bytes := octet_length(NEW.payload::text) + COALESCE(octet_length(NEW.metadata::text), 0);
    INSERT INTO usage_quota_monthly (usage_month, events_ingested, ingress_bytes)
    VALUES (month_key, 1, event_bytes)
    ON CONFLICT (usage_month) DO UPDATE SET
        events_ingested = usage_quota_monthly.events_ingested + 1,
        ingress_bytes = usage_quota_monthly.ingress_bytes + EXCLUDED.ingress_bytes,
        updated_at = NOW()
    WHERE (limits.max_events_month = 0 OR usage_quota_monthly.events_ingested + 1 <= limits.max_events_month)
      AND (limits.max_ingress_bytes_month = 0 OR usage_quota_monthly.ingress_bytes + EXCLUDED.ingress_bytes <= limits.max_ingress_bytes_month)
    RETURNING usage_month INTO accepted;
    IF accepted IS NULL THEN
        RAISE EXCEPTION 'ACTAE_QUOTA_EXCEEDED: monthly event or ingress-byte limit reached'
            USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_enforce_monthly_event_quota ON agent_events;
CREATE TRIGGER trg_enforce_monthly_event_quota
AFTER INSERT ON agent_events
FOR EACH ROW EXECUTE FUNCTION enforce_monthly_event_quota();
        "#,
    },
    Migration {
        version: 25,
        name: "durable_storage_accounting",
        sql: r#"
-- PR3A storage accounting. Two new meters back the commercial model:
--   * durable_written_bytes  — MONTHLY flow meter: every customer-originated
--     durable payload write (event payload+metadata, state snapshots, tool
--     params, tool results). Enforced against max_durable_written_bytes_month.
--   * retained_bytes         — CURRENT gauge: logical customer payload bytes
--     stored right now across events, state versions, and tool records
--     (including fork-generated state copies). Enforced against
--     max_retained_bytes. Deliberately NOT PostgreSQL physical disk usage
--     (indexes, TOAST, bloat, WAL, dead tuples are excluded).

-- 1. Extend the quota config singleton with the two new caps (0 = unlimited).
ALTER TABLE usage_quota_config
    ADD COLUMN IF NOT EXISTS max_durable_written_bytes_month BIGINT NOT NULL DEFAULT 0
        CHECK (max_durable_written_bytes_month >= 0);
ALTER TABLE usage_quota_config
    ADD COLUMN IF NOT EXISTS max_retained_bytes BIGINT NOT NULL DEFAULT 0
        CHECK (max_retained_bytes >= 0);

-- 2. Extend the monthly accumulator with the durable-written meter.
ALTER TABLE usage_quota_monthly
    ADD COLUMN IF NOT EXISTS durable_written_bytes BIGINT NOT NULL DEFAULT 0
        CHECK (durable_written_bytes >= 0);

-- 3. Singleton tracking the current retained logical payload bytes.
CREATE TABLE IF NOT EXISTS usage_storage_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    retained_bytes BIGINT NOT NULL DEFAULT 0 CHECK (retained_bytes >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO usage_storage_state (singleton) VALUES (TRUE) ON CONFLICT DO NOTHING;

-- 4. channel_states writes are marked with their origin so fork-generated
--    copies count toward retained storage but not toward the monthly
--    durable-written meter (the customer did not write those bytes).
ALTER TABLE channel_states
    ADD COLUMN IF NOT EXISTS write_source TEXT NOT NULL DEFAULT 'customer'
        CHECK (write_source IN ('customer', 'fork'));

-- 5. tool_executions gains the tombstone lifecycle markers used by retention.
ALTER TABLE tool_executions
    ADD COLUMN IF NOT EXISTS expired_at TIMESTAMPTZ;

-- 6. Backfill the current retained total from the logical payloads already
--    stored. Runs once at migration time (pre-launch data is empty or tiny).
UPDATE usage_storage_state
SET retained_bytes = (
        COALESCE(
            (SELECT SUM(octet_length(payload::text) + COALESCE(octet_length(metadata::text), 0))
             FROM agent_events), 0)
      + COALESCE(
            (SELECT SUM(octet_length(state::text))
             FROM channel_states), 0)
      + COALESCE(
            (SELECT SUM(octet_length(params::text)
                    + COALESCE(octet_length(result::text), 0)
                    + COALESCE(octet_length(error::text), 0))
             FROM tool_executions), 0)
    ),
    updated_at = NOW()
WHERE singleton = TRUE;

-- Backfill the current month's durable-written meter from events already
-- accepted this month (events are the only flow that predates this migration).
UPDATE usage_quota_monthly m
SET durable_written_bytes = COALESCE(
        (SELECT SUM(octet_length(payload::text) + COALESCE(octet_length(metadata::text), 0))
         FROM agent_events
         WHERE date_trunc('month', timestamp AT TIME ZONE 'UTC')::date = m.usage_month), 0),
    updated_at = NOW();

-- 7. Shared accounting helper: adds to the monthly durable-written meter and
--    to the retained gauge in the SAME transaction as the write, and raises
--    the standard ACTAE_QUOTA_EXCEEDED exception when any cap would be
--    exceeded. The caller's transaction aborts, rolling back the write too.
CREATE OR REPLACE FUNCTION account_durable_write(
    p_usage_date DATE,
    p_durable_bytes BIGINT,
    p_retained_delta BIGINT
) RETURNS VOID AS $$
DECLARE
    limits usage_quota_config%ROWTYPE;
    new_month BIGINT;
    new_retained BIGINT;
BEGIN
    SELECT * INTO limits FROM usage_quota_config WHERE singleton = TRUE;

    IF p_durable_bytes > 0 THEN
        INSERT INTO usage_quota_monthly (usage_month, durable_written_bytes)
        VALUES (p_usage_date, p_durable_bytes)
        ON CONFLICT (usage_month) DO UPDATE SET
            durable_written_bytes = usage_quota_monthly.durable_written_bytes + EXCLUDED.durable_written_bytes,
            updated_at = NOW()
        WHERE limits.max_durable_written_bytes_month = 0
           OR usage_quota_monthly.durable_written_bytes + EXCLUDED.durable_written_bytes <= limits.max_durable_written_bytes_month
        RETURNING durable_written_bytes INTO new_month;
        IF new_month IS NULL THEN
            RAISE EXCEPTION 'ACTAE_QUOTA_EXCEEDED: monthly durable-written-byte limit reached'
                USING ERRCODE = 'P0001';
        END IF;
    END IF;

    IF p_retained_delta <> 0 THEN
        UPDATE usage_storage_state
        SET retained_bytes = retained_bytes + p_retained_delta,
            updated_at = NOW()
        WHERE singleton = TRUE
          AND (limits.max_retained_bytes = 0
               OR retained_bytes + p_retained_delta <= limits.max_retained_bytes)
        RETURNING retained_bytes INTO new_retained;
        IF new_retained IS NULL THEN
            RAISE EXCEPTION 'ACTAE_QUOTA_EXCEEDED: retained-data limit reached'
                USING ERRCODE = 'P0001';
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- 8. Events already enforce events_ingested + ingress_bytes (v24). Extend the
--    event trigger to also feed the durable-written meter and the retained
--    gauge so every accepted event is counted exactly once against every cap.
CREATE OR REPLACE FUNCTION enforce_monthly_event_quota() RETURNS TRIGGER AS $$
DECLARE
    limits usage_quota_config%ROWTYPE;
    month_key DATE;
    event_bytes BIGINT;
    accepted DATE;
BEGIN
    SELECT * INTO limits FROM usage_quota_config WHERE singleton = TRUE;
    month_key := date_trunc('month', NEW.timestamp AT TIME ZONE 'UTC')::date;
    event_bytes := octet_length(NEW.payload::text) + COALESCE(octet_length(NEW.metadata::text), 0);
    INSERT INTO usage_quota_monthly (usage_month, events_ingested, ingress_bytes)
    VALUES (month_key, 1, event_bytes)
    ON CONFLICT (usage_month) DO UPDATE SET
        events_ingested = usage_quota_monthly.events_ingested + 1,
        ingress_bytes = usage_quota_monthly.ingress_bytes + EXCLUDED.ingress_bytes,
        updated_at = NOW()
    WHERE (limits.max_events_month = 0 OR usage_quota_monthly.events_ingested + 1 <= limits.max_events_month)
      AND (limits.max_ingress_bytes_month = 0 OR usage_quota_monthly.ingress_bytes + EXCLUDED.ingress_bytes <= limits.max_ingress_bytes_month)
    RETURNING usage_month INTO accepted;
    IF accepted IS NULL THEN
        RAISE EXCEPTION 'ACTAE_QUOTA_EXCEEDED: monthly event or ingress-byte limit reached'
            USING ERRCODE = 'P0001';
    END IF;
    PERFORM account_durable_write(month_key, event_bytes, event_bytes);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_enforce_monthly_event_quota ON agent_events;
CREATE TRIGGER trg_enforce_monthly_event_quota
AFTER INSERT ON agent_events
FOR EACH ROW EXECUTE FUNCTION enforce_monthly_event_quota();

-- 9. State snapshots: customer-originated writes feed durable-written AND
--    retained; fork-generated copies feed retained only.
CREATE OR REPLACE FUNCTION enforce_state_storage_quota() RETURNS TRIGGER AS $$
DECLARE
    month_key DATE;
    state_bytes BIGINT;
BEGIN
    month_key := date_trunc('month', NEW.timestamp AT TIME ZONE 'UTC')::date;
    state_bytes := octet_length(NEW.state::text);
    IF NEW.write_source = 'fork' THEN
        PERFORM account_durable_write(month_key, 0, state_bytes);
    ELSE
        PERFORM account_durable_write(month_key, state_bytes, state_bytes);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_enforce_state_storage_quota ON channel_states;
CREATE TRIGGER trg_enforce_state_storage_quota
AFTER INSERT ON channel_states
FOR EACH ROW EXECUTE FUNCTION enforce_state_storage_quota();

-- 10. Tool executions: params count on claim; result/error count on
--     complete/fail; a reclaim that clears bodies releases the retained
--     bytes (the monthly durable-written meter never decreases).
CREATE OR REPLACE FUNCTION enforce_tool_storage_quota() RETURNS TRIGGER AS $$
DECLARE
    month_key DATE;
    old_bytes BIGINT;
    new_bytes BIGINT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        month_key := date_trunc('month', NEW.created_at AT TIME ZONE 'UTC')::date;
        new_bytes := octet_length(COALESCE(NEW.params::text, '{}'))
                   + COALESCE(octet_length(NEW.result::text), 0)
                   + COALESCE(octet_length(NEW.error::text), 0);
        PERFORM account_durable_write(month_key, new_bytes, new_bytes);
    ELSIF TG_OP = 'UPDATE' THEN
        month_key := date_trunc('month', NEW.created_at AT TIME ZONE 'UTC')::date;
        old_bytes := octet_length(COALESCE(OLD.params::text, '{}'))
                   + COALESCE(octet_length(OLD.result::text), 0)
                   + COALESCE(octet_length(OLD.error::text), 0);
        new_bytes := octet_length(COALESCE(NEW.params::text, '{}'))
                   + COALESCE(octet_length(NEW.result::text), 0)
                   + COALESCE(octet_length(NEW.error::text), 0);
        IF new_bytes > old_bytes THEN
            PERFORM account_durable_write(month_key, new_bytes - old_bytes, new_bytes - old_bytes);
        ELSIF new_bytes < old_bytes THEN
            UPDATE usage_storage_state
            SET retained_bytes = GREATEST(retained_bytes - (old_bytes - new_bytes), 0),
                updated_at = NOW()
            WHERE singleton = TRUE;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_enforce_tool_storage_quota_insert ON tool_executions;
CREATE TRIGGER trg_enforce_tool_storage_quota_insert
AFTER INSERT ON tool_executions
FOR EACH ROW EXECUTE FUNCTION enforce_tool_storage_quota();
DROP TRIGGER IF EXISTS trg_enforce_tool_storage_quota_update ON tool_executions;
CREATE TRIGGER trg_enforce_tool_storage_quota_update
AFTER UPDATE ON tool_executions
FOR EACH ROW EXECUTE FUNCTION enforce_tool_storage_quota();

-- 11. Release the retained gauge when rows are removed by retention or channel
--     deletion so the gauge reflects the logical payload bytes stored NOW.
CREATE OR REPLACE FUNCTION release_retained_bytes() RETURNS TRIGGER AS $$
DECLARE
    released BIGINT;
BEGIN
    IF TG_TABLE_NAME = 'agent_events' THEN
        released := octet_length(OLD.payload::text) + COALESCE(octet_length(OLD.metadata::text), 0);
    ELSIF TG_TABLE_NAME = 'channel_states' THEN
        released := octet_length(OLD.state::text);
    ELSIF TG_TABLE_NAME = 'tool_executions' THEN
        released := octet_length(COALESCE(OLD.params::text, '{}'))
                  + COALESCE(octet_length(OLD.result::text), 0)
                  + COALESCE(octet_length(OLD.error::text), 0);
    ELSE
        RETURN OLD;
    END IF;
    UPDATE usage_storage_state
    SET retained_bytes = GREATEST(retained_bytes - released, 0),
        updated_at = NOW()
    WHERE singleton = TRUE;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_release_retained_events ON agent_events;
CREATE TRIGGER trg_release_retained_events
AFTER DELETE ON agent_events
FOR EACH ROW EXECUTE FUNCTION release_retained_bytes();
DROP TRIGGER IF EXISTS trg_release_retained_states ON channel_states;
CREATE TRIGGER trg_release_retained_states
AFTER DELETE ON channel_states
FOR EACH ROW EXECUTE FUNCTION release_retained_bytes();
DROP TRIGGER IF EXISTS trg_release_retained_tools ON tool_executions;
CREATE TRIGGER trg_release_retained_tools
AFTER DELETE ON tool_executions
FOR EACH ROW EXECUTE FUNCTION release_retained_bytes();
        "#,
    },
    Migration {
        version: 26,
        name: "execution_groups_and_causal_edges",
        sql: r#"
-- Execution groups are intentionally separate from consumer_groups.  A group
-- is a durable coordination namespace; members retain independent channels.
CREATE TABLE IF NOT EXISTS execution_groups (
    group_id TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'closed')),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS execution_group_members (
    group_id TEXT NOT NULL REFERENCES execution_groups(group_id) ON DELETE CASCADE,
    member_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    role TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    owner_id TEXT,
    generation BIGINT NOT NULL DEFAULT 0,
    lease_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, member_id),
    UNIQUE (group_id, channel_id)
);
CREATE INDEX IF NOT EXISTS idx_execution_group_members_channel
    ON execution_group_members (channel_id);

-- A message has two immutable event facts: sender intent and recipient
-- delivery.  The latter depends_on the former, preserving causal ordering
-- across channels while existing clients remain compatible.
CREATE TABLE IF NOT EXISTS execution_group_messages (
    message_id UUID PRIMARY KEY,
    group_id TEXT NOT NULL REFERENCES execution_groups(group_id) ON DELETE CASCADE,
    from_member_id TEXT NOT NULL,
    to_member_id TEXT NOT NULL,
    message_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    causal_context JSONB,
    operation_id UUID,
    source_event_id UUID NOT NULL UNIQUE,
    delivery_event_id UUID NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'delivered' CHECK (status IN ('delivered', 'acknowledged')),
    acknowledged_at TIMESTAMPTZ,
    acknowledged_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (group_id, from_member_id)
        REFERENCES execution_group_members(group_id, member_id) ON DELETE RESTRICT,
    FOREIGN KEY (group_id, to_member_id)
        REFERENCES execution_group_members(group_id, member_id) ON DELETE RESTRICT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_execution_group_messages_operation
    ON execution_group_messages (group_id, operation_id) WHERE operation_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_execution_group_messages_recipient
    ON execution_group_messages (group_id, to_member_id, created_at, message_id);

-- Additive multi-parent causal DAG. The legacy agent_events.depends_on edge
-- remains the primary/compatibility parent; it is backfilled below.
CREATE TABLE IF NOT EXISTS event_dependencies (
    event_id UUID NOT NULL REFERENCES agent_events(id) ON DELETE CASCADE,
    depends_on_event_id UUID NOT NULL,
    dependency_type TEXT NOT NULL DEFAULT 'causal',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, depends_on_event_id),
    CHECK (event_id <> depends_on_event_id)
);
CREATE INDEX IF NOT EXISTS idx_event_dependencies_parent
    ON event_dependencies (depends_on_event_id);
INSERT INTO event_dependencies (event_id, depends_on_event_id, dependency_type)
SELECT id, depends_on, 'causal'
FROM agent_events
WHERE depends_on IS NOT NULL
ON CONFLICT (event_id, depends_on_event_id) DO NOTHING;
        "#,
    },
    Migration {
        version: 27,
        name: "group_fork_receipts",
        sql: r#"
-- A fork is an immutable counterfactual definition.  It never mutates the
-- source group or its events; evolving observations are stored in receipt.
CREATE TABLE IF NOT EXISTS execution_group_forks (
    fork_group_id TEXT PRIMARY KEY,
    source_group_id TEXT NOT NULL REFERENCES execution_groups(group_id) ON DELETE RESTRICT,
    intervention_member_id TEXT NOT NULL,
    intervention_cursor BIGINT NOT NULL CHECK (intervention_cursor >= 0),
    member_policies JSONB NOT NULL,
    tool_policies JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'blocked', 'completed')),
    first_divergence_event_id UUID,
    receipt JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (source_group_id, intervention_member_id)
      REFERENCES execution_group_members(group_id, member_id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS idx_execution_group_forks_source
    ON execution_group_forks (source_group_id, created_at DESC);
        "#,
    },
    Migration {
        version: 28,
        name: "keyset_pagination_global_cursor_indexes",
        sql: r#"
-- Phase 3 instance change: indexes required by keyset pagination
-- (POST /api/v1/events/query with an opaque page_token). The query orders by
-- the internal global_cursor DESC and continues with `global_cursor < bound`:
--   * a bare global_cursor index serves cross-channel queries (no filter),
--   * a (channel_id, global_cursor) index serves per-channel + IN(...) queries
--     so PostgreSQL can walk the filter+order in index order instead of
--     sorting the filtered rows. Both are additive and safe on existing data.
CREATE INDEX IF NOT EXISTS idx_agent_events_global_cursor
    ON agent_events (global_cursor DESC);
CREATE INDEX IF NOT EXISTS idx_agent_events_channel_global_cursor
    ON agent_events (channel_id, global_cursor DESC);
        "#,
    },
    Migration {
        version: 29,
        name: "fork_tool_policies",
        sql: r#"
-- Side-effect-aware ordinary forks. A fork may declare how tool executions
-- are handled on the child channel, mirroring the group-fork policy model:
--   {"tool_name": "replay" | "block" | "live" | "auto", "*": <default>}
-- The policy is stored on the child's channel_metadata so the claim path can
-- resolve it from the executing channel alone, and mirrored into the
-- immutable fork_operations receipt so provenance survives metadata edits.
-- Absent/NULL means the server default ("auto") for forked channels and
-- today's exact behavior for non-forked channels.
ALTER TABLE channel_metadata ADD COLUMN IF NOT EXISTS tool_policies JSONB;
ALTER TABLE fork_operations ADD COLUMN IF NOT EXISTS tool_policies JSONB;
        "#,
    },
    Migration {
        version: 30,
        name: "rename_branch_events_to_fork",
        sql: r#"
-- Vocabulary unification: "fork" is the canonical term; the child's first
-- event is `fork.started` and the parent-side marker is `fork.created`
-- (previously `branch.started` / `run.forked`).
--
-- The tamper-evident hash chain (v21) commits to `event_type`
-- (hash = sha256(channel_id | cursor | event_type | canonical_payload |
-- prev_hash)), and `canonical_payload` is the sonic-rs serialization — not
-- reproducible in SQL. Rewriting the event_type of a row that already carries
-- a hash would therefore silently break the chain and make `verify_chain`
-- report a false tamper. So this migration is deliberately integrity-safe:
-- only pre-hash-chain rows (hash IS NULL, already reported as
-- `unverified_events`) are rewritten; hashed rows keep their bytes and the
-- Rust `from_str` accepts both the new and legacy spellings.
UPDATE agent_events SET event_type = 'fork.started'
    WHERE event_type = 'branch.started' AND hash IS NULL;
UPDATE agent_events SET event_type = 'fork.created'
    WHERE event_type = 'run.forked' AND hash IS NULL;
        "#,
    },
    Migration {
        version: 31,
        name: "usage_accounting_serialize_writes",
        sql: r#"
-- The storage-accounting triggers updated a shared monthly row
-- (`usage_quota_monthly`) and a singleton gauge (`usage_storage_state`). Under
-- concurrent writes (several agents, or a fork storm) two transactions could
-- acquire those shared-row locks in incompatible orders and deadlock
-- (`deadlock detected`, SQLSTATE 40P01), which surfaced to clients as HTTP 500.
--
-- Take one transaction-scoped advisory lock FIRST in every accounting path so
-- the entire accounting section runs with a single, consistent lock order and
-- concurrent writers queue instead of deadlocking. The singleton gauge update
-- already serialized these writes, so this does not reduce real throughput.

CREATE OR REPLACE FUNCTION account_durable_write(
    p_usage_date DATE,
    p_durable_bytes BIGINT,
    p_retained_delta BIGINT
) RETURNS VOID AS $$
DECLARE
    limits usage_quota_config%ROWTYPE;
    new_month BIGINT;
    new_retained BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('actae_usage_accounting', 0));
    SELECT * INTO limits FROM usage_quota_config WHERE singleton = TRUE;

    IF p_durable_bytes > 0 THEN
        INSERT INTO usage_quota_monthly (usage_month, durable_written_bytes)
        VALUES (p_usage_date, p_durable_bytes)
        ON CONFLICT (usage_month) DO UPDATE SET
            durable_written_bytes = usage_quota_monthly.durable_written_bytes + EXCLUDED.durable_written_bytes,
            updated_at = NOW()
        WHERE limits.max_durable_written_bytes_month = 0
           OR usage_quota_monthly.durable_written_bytes + EXCLUDED.durable_written_bytes <= limits.max_durable_written_bytes_month
        RETURNING durable_written_bytes INTO new_month;
        IF new_month IS NULL THEN
            RAISE EXCEPTION 'ACTAE_QUOTA_EXCEEDED: monthly durable-written-byte limit reached'
                USING ERRCODE = 'P0001';
        END IF;
    END IF;

    IF p_retained_delta <> 0 THEN
        UPDATE usage_storage_state
        SET retained_bytes = retained_bytes + p_retained_delta,
            updated_at = NOW()
        WHERE singleton = TRUE
          AND (limits.max_retained_bytes = 0
               OR retained_bytes + p_retained_delta <= limits.max_retained_bytes)
        RETURNING retained_bytes INTO new_retained;
        IF new_retained IS NULL THEN
            RAISE EXCEPTION 'ACTAE_QUOTA_EXCEEDED: retained-data limit reached'
                USING ERRCODE = 'P0001';
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION enforce_monthly_event_quota() RETURNS TRIGGER AS $$
DECLARE
    limits usage_quota_config%ROWTYPE;
    month_key DATE;
    event_bytes BIGINT;
    accepted DATE;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('actae_usage_accounting', 0));
    SELECT * INTO limits FROM usage_quota_config WHERE singleton = TRUE;
    month_key := date_trunc('month', NEW.timestamp AT TIME ZONE 'UTC')::date;
    event_bytes := octet_length(NEW.payload::text) + COALESCE(octet_length(NEW.metadata::text), 0);
    INSERT INTO usage_quota_monthly (usage_month, events_ingested, ingress_bytes)
    VALUES (month_key, 1, event_bytes)
    ON CONFLICT (usage_month) DO UPDATE SET
        events_ingested = usage_quota_monthly.events_ingested + 1,
        ingress_bytes = usage_quota_monthly.ingress_bytes + EXCLUDED.ingress_bytes,
        updated_at = NOW()
    WHERE (limits.max_events_month = 0 OR usage_quota_monthly.events_ingested + 1 <= limits.max_events_month)
      AND (limits.max_ingress_bytes_month = 0 OR usage_quota_monthly.ingress_bytes + EXCLUDED.ingress_bytes <= limits.max_ingress_bytes_month)
    RETURNING usage_month INTO accepted;
    IF accepted IS NULL THEN
        RAISE EXCEPTION 'ACTAE_QUOTA_EXCEEDED: monthly event or ingress-byte limit reached'
            USING ERRCODE = 'P0001';
    END IF;
    PERFORM account_durable_write(month_key, event_bytes, event_bytes);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION enforce_tool_storage_quota() RETURNS TRIGGER AS $$
DECLARE
    month_key DATE;
    old_bytes BIGINT;
    new_bytes BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('actae_usage_accounting', 0));
    IF TG_OP = 'INSERT' THEN
        month_key := date_trunc('month', NEW.created_at AT TIME ZONE 'UTC')::date;
        new_bytes := octet_length(COALESCE(NEW.params::text, '{}'))
                   + COALESCE(octet_length(NEW.result::text), 0)
                   + COALESCE(octet_length(NEW.error::text), 0);
        PERFORM account_durable_write(month_key, new_bytes, new_bytes);
    ELSIF TG_OP = 'UPDATE' THEN
        month_key := date_trunc('month', NEW.created_at AT TIME ZONE 'UTC')::date;
        old_bytes := octet_length(COALESCE(OLD.params::text, '{}'))
                   + COALESCE(octet_length(OLD.result::text), 0)
                   + COALESCE(octet_length(OLD.error::text), 0);
        new_bytes := octet_length(COALESCE(NEW.params::text, '{}'))
                   + COALESCE(octet_length(NEW.result::text), 0)
                   + COALESCE(octet_length(NEW.error::text), 0);
        IF new_bytes > old_bytes THEN
            PERFORM account_durable_write(month_key, new_bytes - old_bytes, new_bytes - old_bytes);
        ELSIF new_bytes < old_bytes THEN
            UPDATE usage_storage_state
            SET retained_bytes = GREATEST(retained_bytes - (old_bytes - new_bytes), 0),
                updated_at = NOW()
            WHERE singleton = TRUE;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION release_retained_bytes() RETURNS TRIGGER AS $$
DECLARE
    released BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('actae_usage_accounting', 0));
    IF TG_TABLE_NAME = 'agent_events' THEN
        released := octet_length(OLD.payload::text) + COALESCE(octet_length(OLD.metadata::text), 0);
    ELSIF TG_TABLE_NAME = 'channel_states' THEN
        released := octet_length(OLD.state::text);
    ELSIF TG_TABLE_NAME = 'tool_executions' THEN
        released := octet_length(COALESCE(OLD.params::text, '{}'))
                  + COALESCE(octet_length(OLD.result::text), 0)
                  + COALESCE(octet_length(OLD.error::text), 0);
    ELSE
        RETURN OLD;
    END IF;
    UPDATE usage_storage_state
    SET retained_bytes = GREATEST(retained_bytes - released, 0),
        updated_at = NOW()
    WHERE singleton = TRUE;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;
        "#,
    },
];

pub async fn run_migrations(pool: &Pool) -> Result<(), crate::events::store::EventStoreError> {
    let mut client = pool
        .get()
        .await
        .map_err(crate::events::store::EventStoreError::Pool)?;

    client
        .execute(
            "CREATE TABLE IF NOT EXISTS _migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            &[],
        )
        .await
        .map_err(crate::events::store::EventStoreError::Execute)?;

    let row = client
        .query_one(
            "SELECT COALESCE(MAX(version), 0) AS max_version FROM _migrations",
            &[],
        )
        .await
        .map_err(crate::events::store::EventStoreError::Execute)?;

    let current_version: i32 = row.get("max_version");

    let mut applied = 0;
    for migration in MIGRATIONS {
        if migration.version > current_version {
            // Apply the SQL and record it in the ledger inside ONE
            // transaction: if the process dies between the batch and the
            // INSERT, both roll back together and the migration re-runs
            // cleanly on next boot (a partial-apply crash would otherwise
            // leave non-idempotent DDL such as RENAME COLUMN in a broken
            // half-applied state).
            let tx = client
                .transaction()
                .await
                .map_err(crate::events::store::EventStoreError::Execute)?;

            tx.batch_execute(migration.sql)
                .await
                .map_err(crate::events::store::EventStoreError::Execute)?;

            tx.execute(
                "INSERT INTO _migrations (version, name) VALUES ($1, $2)",
                &[&migration.version, &migration.name],
            )
            .await
            .map_err(crate::events::store::EventStoreError::Execute)?;

            tx.commit()
                .await
                .map_err(crate::events::store::EventStoreError::Execute)?;

            applied += 1;
        }
    }

    if applied > 0 {
        info!("Applied {} migrations", applied);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_construction() {
        let m = Migration {
            version: 1,
            name: "test_migration",
            sql: "CREATE TABLE IF NOT EXISTS test (id INT);",
        };
        assert_eq!(m.version, 1);
        assert_eq!(m.name, "test_migration");
        assert_eq!(m.sql, "CREATE TABLE IF NOT EXISTS test (id INT);");
    }

    #[test]
    fn migrations_count() {
        assert!(
            MIGRATIONS.len() >= 9,
            "Expected at least 9 migrations, got {}",
            MIGRATIONS.len()
        );
    }

    #[test]
    fn storage_accounting_serializes_with_advisory_lock() {
        // Regression: the accounting triggers deadlocked (SQLSTATE 40P01) under
        // concurrent writes until every one of them acquired the same advisory
        // lock first (migration 31).
        let m = MIGRATIONS
            .iter()
            .find(|m| m.version == 31)
            .expect("migration 31 (usage_accounting_serialize_writes) is missing");
        assert_eq!(m.name, "usage_accounting_serialize_writes");
        let bodies: Vec<&str> = m.sql.split("CREATE OR REPLACE FUNCTION").skip(1).collect();
        assert_eq!(bodies.len(), 4, "expected 4 accounting trigger functions");
        for body in &bodies {
            assert!(
                body.contains(
                    "pg_advisory_xact_lock(hashtextextended('actae_usage_accounting', 0))"
                ),
                "every accounting trigger must take the advisory lock first"
            );
        }
    }

    #[test]
    fn migrations_unique_versions() {
        let mut versions: Vec<i32> = MIGRATIONS.iter().map(|m| m.version).collect();
        let original_len = versions.len();
        versions.sort_unstable();
        versions.dedup();
        assert_eq!(
            versions.len(),
            original_len,
            "Duplicate migration versions found"
        );
    }

    #[test]
    fn migrations_ordered_versions() {
        for (i, migration) in MIGRATIONS.iter().enumerate() {
            let expected = (i + 1) as i32;
            assert_eq!(
                migration.version, expected,
                "Migration at index {} has version {}, expected {}",
                i, migration.version, expected
            );
        }
    }

    #[test]
    fn migrations_have_non_empty_sql() {
        for migration in MIGRATIONS {
            assert!(
                !migration.sql.trim().is_empty(),
                "Migration {} ({}) has empty SQL",
                migration.version,
                migration.name
            );
        }
    }

    #[test]
    fn migrations_have_non_empty_names() {
        for migration in MIGRATIONS {
            assert!(
                !migration.name.is_empty(),
                "Migration {} has empty name",
                migration.version
            );
        }
    }
}
