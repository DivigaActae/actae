-- Reset test state: clean the agent_events table for a fresh test run.
-- WARNING: This deletes ALL event data. Use with caution.

BEGIN;

-- Delete all events
DELETE FROM agent_events;

-- Reset the cursor sequence to start from 1
ALTER SEQUENCE agent_events_cursor_seq RESTART WITH 1;

-- If the sequence doesn't exist (BIGSERIAL creates it implicitly),
-- the ALTER will fail silently, which is fine
DO $$
BEGIN
    -- Reset the implicit sequence created by BIGSERIAL
    -- The sequence name is auto-generated: tablename_columnname_seq
    BEGIN
        EXECUTE 'ALTER SEQUENCE IF EXISTS agent_events_cursor_seq RESTART WITH 1';
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    -- Also try the public schema prefix
    BEGIN
        EXECUTE 'ALTER SEQUENCE IF EXISTS public.agent_events_cursor_seq RESTART WITH 1';
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;
END $$;

COMMIT;

\echo 'Event store cleared. Cursor sequence reset.'
\echo 'Remaining rows:'
SELECT count(*) FROM agent_events;
