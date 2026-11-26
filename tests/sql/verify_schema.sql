-- Schema verification for the agent_events table.
-- Run via: psql -d actae -f tests/sql/verify_schema.sql

\timing on

\echo '=== Column Verification ==='
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_name = 'agent_events'
ORDER BY ordinal_position;

\echo '=== Index Verification ==='
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'agent_events'
ORDER BY indexname;

\echo '=== Constraint Verification ==='
SELECT conname, contype, pg_get_constraintdef(oid) as constraint_def
FROM pg_constraint
WHERE conrelid = 'agent_events'::regclass;

\echo '=== Expected Schema Check ==='
DO $$
DECLARE
    has_txid boolean;
    has_replay_index boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'agent_events' AND column_name = 'txid'
    ) INTO has_txid;

    SELECT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE tablename = 'agent_events' AND indexname = 'idx_agent_events_replay'
    ) INTO has_replay_index;

    IF NOT has_txid THEN
        RAISE WARNING 'MISSING: txid XID8 column';
    ELSE
        RAISE NOTICE 'OK: txid column present';
    END IF;

    IF NOT has_replay_index THEN
        RAISE WARNING 'MISSING: idx_agent_events_replay index (channel_id, cursor ASC, txid)';
    ELSE
        RAISE NOTICE 'OK: idx_agent_events_replay index present';
    END IF;
END $$;

\echo '=== txid Values Sample ==='
SELECT cursor, txid::text, channel_id, event_type, LEFT(actor, 20) as actor
FROM agent_events
WHERE txid IS NOT NULL
ORDER BY cursor DESC
LIMIT 10;

\echo '=== MVCC Snapshot State ==='
SELECT
    pg_current_xact_id()::text AS current_tx,
    pg_snapshot_xmin(pg_current_snapshot())::text AS low_watermark,
    pg_snapshot_xmax(pg_current_snapshot())::text AS high_watermark;

\echo '=== Table Size ==='
SELECT pg_size_pretty(pg_total_relation_size('agent_events')) as total_size;

\echo '=== Row Count ==='
SELECT count(*) as total_events FROM agent_events;

\echo '=== Events by Tenant ==='
SELECT COALESCE(tenant_id, '(none)') as tenant, count(*) as count
FROM agent_events
GROUP BY tenant_id
ORDER BY tenant_id;

\echo '=== Events by Type (Top 20) ==='
SELECT event_type, count(*) as count
FROM agent_events
GROUP BY event_type
ORDER BY count(*) DESC
LIMIT 20;

\echo '=== Latest Cursor per Channel (Top 20) ==='
SELECT channel_id, MAX(cursor) as latest_cursor, count(*) as event_count
FROM agent_events
GROUP BY channel_id
ORDER BY MAX(cursor) DESC
LIMIT 20;
