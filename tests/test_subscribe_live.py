# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Subscribe + live event delivery + cursor resume.

Covers: LV, CR from the test strategy.

NOTE: WebSocket subscribe requires RBAC permissions. We test subscribe
functionality via the HTTP API (replay + cursor endpoints) which work with
API key auth. The cursor resume flow is tested via the cursor endpoint.
"""

import pytest



class TestReplayCatchUp:
    def test_replay_full_channel(self, actae, ch_name):
        for i in range(5):
            actae.record(ch_name, "test.event", {"seq": i}, "seeder")
        events = actae.replay(ch_name, cursor=0, limit=100)
        assert len(events) == 5
        assert all(e["type"] == "test.event" for e in events)

    def test_replay_from_midpoint(self, actae, ch_name):
        for i in range(10):
            actae.record(ch_name, "test.event", {"seq": i}, "seeder")
        latest = actae.cursor(ch_name)
        events = actae.replay(ch_name, cursor=latest - 5, limit=100)
        assert len(events) == 5


class TestCursorResume:
    def test_cursor_then_replay(self, actae, ch_name):
        for i in range(5):
            actae.record(ch_name, "test.event", {"seq": i}, "seeder")
        c = actae.cursor(ch_name)
        assert c is not None
        events = actae.replay(ch_name, cursor=c - 2, limit=100)
        assert len(events) == 2
        assert events[0]["cursor"] > c - 2


class TestLiveQuery:
    def test_query_after_record(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "producer")
        actae.record(ch_name, "test.event", {}, "producer")
        events = actae.query(channel_ids=[ch_name], limit=10)
        assert len(events) == 2
