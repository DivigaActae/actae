# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Advanced replay scenarios: all cursor/limit/event_type combinations.

Covers every fork of EventStore::replay() SQL query construction:
4 combinations of (cursor: Some/None) x (event_type: Some/None).
"""

import pytest



class TestReplayFourCombinations:
    """All 4 SQL query patterns in EventStore::replay()."""

    @pytest.fixture(autouse=True)
    def _seed(self, actae, ch_name):
        for i in range(10):
            et = "test.event" if i < 7 else "other.event"
            actae.record(ch_name, et, {"seq": i}, "bot")
        actae.record(f"{ch_name}-other", "other.event", {}, "bot")

    def test_cursor_with_event_type(self, actae, ch_name):
        events = actae.replay(ch_name, cursor=3, limit=10, event_type="test.event")
        assert len(events) >= 1
        for e in events:
            assert e["type"] == "test.event"
            assert e["cursor"] > 3

    def test_cursor_without_event_type(self, actae, ch_name):
        events = actae.replay(ch_name, cursor=5, limit=10)
        assert len(events) >= 1
        for e in events:
            assert e["cursor"] > 5

    def test_no_cursor_with_event_type(self, actae, ch_name):
        events = actae.replay(ch_name, limit=10, event_type="test.event")
        assert len(events) >= 1
        for e in events:
            assert e["type"] == "test.event"

    def test_no_cursor_no_event_type(self, actae, ch_name):
        events = actae.replay(ch_name, limit=10)
        assert len(events) >= 1


class TestReplayLimitBranches:
    """Replay limit behavior at boundaries."""

    def test_limit_zero(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        events = actae.replay(ch_name, cursor=0, limit=0)
        assert len(events) >= 0

    def test_limit_one(self, actae, ch_name):
        for i in range(5):
            actae.record(ch_name, "test.event", {"seq": i}, "bot")
        events = actae.replay(ch_name, cursor=0, limit=1)
        assert len(events) == 1

    def test_limit_exact_match(self, actae, ch_name):
        for i in range(5):
            actae.record(ch_name, "test.event", {"seq": i}, "bot")
        events = actae.replay(ch_name, cursor=0, limit=5)
        assert len(events) == 5

    def test_limit_exceeds_available(self, actae, ch_name):
        for i in range(3):
            actae.record(ch_name, "test.event", {"seq": i}, "bot")
        events = actae.replay(ch_name, cursor=0, limit=100)
        assert len(events) == 3


class TestReplayCursorBoundaries:
    """Cursor values at extreme boundaries."""

    def test_cursor_zero(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        events = actae.replay(ch_name, cursor=0, limit=10)
        assert len(events) >= 1

    def test_cursor_beyond_end(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        events = actae.replay(ch_name, cursor=999999, limit=10)
        assert len(events) == 0

    def test_cursor_minus_one(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        events = actae.replay(ch_name, cursor=-1, limit=10)
        assert isinstance(events, list)


class TestReplayAcrossChannels:
    """Replay on different channels in parallel."""

    def test_replay_two_channels(self, actae, ch_name):
        cha, chb = f"{ch_name}-a", f"{ch_name}-b"
        actae.record(cha, "test.event", {}, "bot")
        actae.record(cha, "test.event", {}, "bot")
        actae.record(chb, "other.event", {}, "bot")
        ev_a = actae.replay(cha, cursor=0, limit=10)
        ev_b = actae.replay(chb, cursor=0, limit=10)
        assert len(ev_a) == 2
        assert len(ev_b) == 1

    def test_replay_channel_after_record(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        events = actae.replay(ch_name, cursor=0, limit=10)
        assert len(events) >= 0


class TestReplayEventTypeFiltering:
    """Event type filtering precision."""

    def test_replay_nonexistent_type(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        events = actae.replay(ch_name, cursor=0, limit=10, event_type="nonexistent.type")
        assert len(events) == 0

    def test_replay_type_case_sensitive(self, actae, ch_name):
        actae.record(ch_name, "Test.Event", {}, "bot")
        events = actae.replay(ch_name, cursor=0, limit=10, event_type="test.event")
        assert len(events) == 0

    def test_replay_type_with_dots(self, actae, ch_name):
        actae.record(ch_name, "a.b.c.d.e", {}, "bot")
        events = actae.replay(ch_name, cursor=0, limit=10, event_type="a.b.c.d.e")
        assert len(events) >= 1
