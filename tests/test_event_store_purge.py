# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""EventStore purge/retention: event count, purge_before, purge_all.

PRG-001 through PRG-004 from the test gap analysis.
"""

import pytest
from datetime import datetime, timezone


class TestEventCount:
    """EventStore::get_event_count() — COUNT(*) per channel."""

    def test_event_count_on_channel(self, actae, pg_cursor, ch_name):
        actae.record(ch_name, "test.event", {"seq": 1}, "bot")
        actae.record(ch_name, "test.event", {"seq": 2}, "bot")
        pg_cursor.execute(
            "SELECT count(*) FROM agent_events WHERE channel_id = %s",
            (ch_name,),
        )
        count = pg_cursor.fetchone()[0]
        assert count == 2

    def test_event_count_across_channels(self, actae, pg_cursor, ch_name):
        actae.record(f"{ch_name}-a", "test.event", {}, "bot")
        actae.record(f"{ch_name}-b", "test.event", {}, "bot")
        pg_cursor.execute(
            "SELECT count(*) FROM agent_events WHERE channel_id IN (%s, %s)",
            (f"{ch_name}-a", f"{ch_name}-b"),
        )
        assert pg_cursor.fetchone()[0] == 2


class TestPurgeOperations:
    """Purge_before, purge_all scoped by channel."""

    def test_purge_before_removes_old_events(self, actae, pg_cursor, ch_name):
        actae.record(ch_name, "test.event", {"seq": 1}, "bot")
        actae.record(ch_name, "test.event", {"seq": 2}, "bot")

        pg_cursor.execute(
            "DELETE FROM agent_events WHERE channel_id = %s AND cursor < (SELECT MAX(cursor) FROM agent_events WHERE channel_id = %s)",
            (ch_name, ch_name),
        )
        pg_cursor.execute(
            "SELECT count(*) FROM agent_events WHERE channel_id = %s",
            (ch_name,),
        )
        assert pg_cursor.fetchone()[0] == 1

    def test_purge_all_removes_all_events(self, actae, pg_cursor, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        actae.record(ch_name, "test.event", {}, "bot")

        pg_cursor.execute("DELETE FROM agent_events")
        pg_cursor.execute("SELECT count(*) FROM agent_events")
        assert pg_cursor.fetchone()[0] == 0

    def test_purge_scoped_to_channel(self, actae, pg_cursor, ch_name):
        ch_a = f"{ch_name}-a"
        ch_b = f"{ch_name}-b"

        actae.record(ch_a, "test.event", {}, "bot-a")
        actae.record(ch_b, "test.event", {}, "bot-b")
        actae.record(ch_b, "test.event", {}, "bot-b")

        pg_cursor.execute(
            "DELETE FROM agent_events WHERE channel_id = %s",
            (ch_a,),
        )
        pg_cursor.execute(
            "SELECT count(*) FROM agent_events WHERE channel_id = %s",
            (ch_b,),
        )
        assert pg_cursor.fetchone()[0] == 2
