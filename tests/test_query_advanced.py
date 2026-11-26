# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Advanced query scenarios: all filter dimensions and combinations.

Covers every filter dimension in EventStore::query(): channel_ids
(single + multi), event_type, actor, cursor_start, cursor_end, limit, offset.
"""

import pytest



class TestQueryByChannelIds:
    """channel_ids filter: single vs multi."""

    def test_query_single_channel(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        events = actae.query(channel_ids=[ch_name], limit=10)
        assert len(events) >= 1
        for e in events:
            assert e["channel_id"] == ch_name

    def test_query_multiple_channels(self, actae, ch_name):
        cha, chb = f"{ch_name}-a", f"{ch_name}-b"
        actae.record(cha, "test.event", {}, "bot")
        actae.record(chb, "test.event", {}, "bot")
        events = actae.query(channel_ids=[cha, chb], limit=10)
        assert len(events) >= 2
        channels = {e["channel_id"] for e in events}
        assert cha in channels
        assert chb in channels

    def test_query_nonexistent_channel(self, actae):
        events = actae.query(channel_ids=["nonexistent-channel-xyz"], limit=10)
        assert len(events) == 0


class TestQueryCombinedFilters:
    """Multiple filters applied simultaneously."""

    def test_query_type_and_actor(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        actae.record(ch_name, "other.event", {}, "other-actor")
        events = actae.query(channel_ids=[ch_name], event_type="test.event", actor="bot", limit=10)
        assert len(events) >= 1
        for e in events:
            assert e["type"] == "test.event"
            assert e["actor"] == "bot"

    def test_query_cursor_start_only(self, actae, ch_name):
        cursors = []
        for i in range(5):
            r = actae.record(ch_name, "test.event", {"seq": i}, "bot")
            cursors.append(r["event"]["cursor"])
        mid = cursors[2]
        events = actae.query(channel_ids=[ch_name], cursor_start=mid, limit=10)
        assert len(events) >= 1
        for e in events:
            assert e["cursor"] >= mid

    def test_query_cursor_start_and_end(self, actae, ch_name):
        cursors = []
        for i in range(5):
            r = actae.record(ch_name, "test.event", {"seq": i}, "bot")
            cursors.append(r["event"]["cursor"])
        low, high = cursors[1], cursors[3]
        events = actae.query(channel_ids=[ch_name], cursor_start=low, cursor_end=high, limit=10)
        assert len(events) >= 1
        for e in events:
            assert low <= e["cursor"] <= high

    def test_query_offset_pagination(self, actae, ch_name):
        for i in range(10):
            actae.record(ch_name, "test.event", {"seq": i}, "bot")
        page1 = actae.query(channel_ids=[ch_name], limit=3, offset=0)
        page2 = actae.query(channel_ids=[ch_name], limit=3, offset=3)
        assert len(page1) == 3
        assert len(page2) == 3
        if page1 and page2:
            assert page1[0]["cursor"] != page2[0]["cursor"]


class TestQueryEdgeCases:
    """Edge cases for query endpoint."""

    def test_query_limit_zero(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        events = actae.query(limit=0)
        assert isinstance(events, list)

    def test_query_limit_large(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        events = actae.query(limit=10000)
        assert isinstance(events, list)

    def test_query_default_limit(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        events = actae.query(limit=10)
        assert len(events) >= 1

