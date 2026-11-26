-- Runtime inspection queries for debugging test failures.
-- Run individual queries as needed.

-- 1. Full event timeline
-- SELECT cursor, txid::text, channel_id, event_type, actor, tenant_id
-- FROM agent_events
-- ORDER BY cursor ASC
-- LIMIT 50;

-- 2. Find gaps in cursor sequence (should be none)
WITH numbered AS (
    SELECT cursor, LAG(cursor) OVER (ORDER BY cursor) as prev_cursor
    FROM agent_events
)
SELECT cursor, prev_cursor, cursor - prev_cursor - 1 as gap_size
FROM numbered
WHERE prev_cursor IS NOT NULL AND cursor != prev_cursor + 1
ORDER BY cursor;

-- 3. Check for duplicate cursors (should be none)
SELECT cursor, count(*) as cnt
FROM agent_events
GROUP BY cursor
HAVING count(*) > 1;

-- 4. Search events by channel
-- SELECT cursor, txid::text, event_type, actor, timestamp
-- FROM agent_events
-- WHERE channel_id = '<channel-name>'
-- ORDER BY cursor ASC;

-- 5. Check concurrency pattern for a specific channel
-- SELECT cursor, txid::text, event_type, actor
-- FROM agent_events
-- WHERE channel_id LIKE 'concurrency%'
-- ORDER BY cursor ASC;

-- 6. MVCC fence health check
SELECT
    'MVCC Fence Health' as check_name,
    CASE
        WHEN bool_and(txid IS NOT NULL) THEN 'PASS'
        ELSE 'FAIL'
    END as status,
    count(*) as total_events,
    count(*) FILTER (WHERE txid IS NULL) as null_txid_events
FROM agent_events;

-- 7. Event distribution over time
SELECT
    date_trunc('minute', timestamp) as minute_bucket,
    count(*) as events
FROM agent_events
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY minute_bucket
ORDER BY minute_bucket;
