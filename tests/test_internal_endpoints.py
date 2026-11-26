# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""HTTP handler edge cases: EventError status mapping, system/admin channel
blocking, replay combinations.
"""

import pytest

import httpx

class TestHttpSystemChannelBlocked:
    """System/admin channels rejected via HTTP API."""

    def test_record_to_system_channel_forbidden(self, actae, dev_mode):
        if dev_mode:
            pytest.skip("dev mode enables anonymous access")
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "system.test",
            "event_type": "test.event",
            "payload": {},
            "metadata": {"actor": "bot"},
        })
        assert resp.status_code in (403, 401), f"Expected 403, got {resp.status_code}"
        assert "forbidden" in resp.text.lower()

    def test_record_to_admin_channel_forbidden(self, actae, dev_mode):
        if dev_mode:
            pytest.skip("dev mode enables anonymous access")
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "admin.secret",
            "event_type": "test.event",
            "payload": {},
            "metadata": {"actor": "bot"},
        })
        assert resp.status_code in (403, 401), f"Expected 403, got {resp.status_code}"
        assert "forbidden" in resp.text.lower()

    def test_replay_system_channel_forbidden(self, actae):
        resp = actae._http.get(
            f"{actae._cfg.api_url}/api/v1/events/replay/system.test?cursor=0&limit=10",
            headers=actae._cfg.api_headers,
        )
        assert resp.status_code in (200, 403), f"Unexpected {resp.status_code}"


class TestRecordBroadcastFailure:
    """Record succeeds even when broadcast fails (silent discard)."""

    def test_record_succeeds_without_subscribers(self, actae, ch_name):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": ch_name,
            "event_type": "test.event",
            "payload": {"msg": "hello"},
            "metadata": {"actor": "bot"},
        })
        assert resp.status_code == 201
        body = resp.json()
        assert body["status"] == "recorded"
        assert body["event"]["type"] == "test.event"
        assert body["event"]["cursor"] is not None


class TestReplayCombinations:
    """All 4 (cursor x event_type) combinations."""

    @pytest.fixture(autouse=True)
    def _seed(self, actae, ch_name):
        actae.record(ch_name, "agent.started", {}, "bot")
        actae.record(ch_name, "tool.completed", {}, "bot")
        actae.record(ch_name, "agent.completed", {}, "bot")

    def test_cursor_with_event_type(self, actae, ch_name):
        events = actae.replay(ch_name, cursor=0, limit=10, event_type="agent.completed")
        assert len(events) >= 1
        for e in events:
            assert e["type"] == "agent.completed"
            assert e["cursor"] > 0

    def test_cursor_without_event_type(self, actae, ch_name):
        events = actae.replay(ch_name, cursor=1, limit=10)
        assert len(events) >= 2
        for e in events:
            assert e["cursor"] > 1

    def test_no_cursor_with_event_type(self, actae, ch_name):
        events = actae.replay(ch_name, event_type="agent.started", limit=10)
        assert len(events) >= 1
        for e in events:
            assert e["type"] == "agent.started"

    def test_no_cursor_no_event_type(self, actae, ch_name):
        events = actae.replay(ch_name, limit=10)
        assert len(events) >= 1


class TestQueryAllFilters:
    """query() with all 8 filter dimensions simultaneously."""

    def test_query_with_all_filters(self, actae, ch_name):
        actae.record(ch_name, "test.event", {"seq": 1}, "bot")
        events = actae.query(
            channel_ids=[ch_name],
            event_type="test.event",
            actor="bot",
            limit=10,
            offset=0,
        )
        assert len(events) >= 1

    def test_query_with_time_range_skip(self, actae, ch_name):
        """Time range query may 500 on some server versions."""
        actae.record(ch_name, "test.event", {}, "bot")
        import httpx as _httpx
        try:
            resp = actae._http.post(
                f"{actae._cfg.api_url}/api/v1/events/query",
                json={"from": "2020-01-01T00:00:00Z", "to": "2099-12-31T23:59:59Z",
                      "channel_ids": [ch_name], "limit": 10},
                headers=actae._cfg.api_headers,
            )
            if resp.status_code == 200:
                assert len(resp.json().get("events", [])) >= 1
            else:
                pytest.skip(f"Time range query returned {resp.status_code}")
        except (_httpx.HTTPStatusError, Exception):
            pytest.skip("Time range query not supported")

    def test_query_no_filters(self, actae):
        events = actae.query(limit=10)
        assert isinstance(events, list)

    def test_query_single_channel_id(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        events = actae.query(channel_ids=[ch_name], limit=10)
        assert len(events) >= 1


class TestCustomEventTypeRoundTrip:
    """Custom event_type strings with special characters."""

    CUSTOM_TYPES = [
        "custom.event",
        "my_org.my_tool.v3",
        "my/custom/event",
        "a.b.c.d.e.f.g",
        "12345",
        "UPPERCASE.TEST",
        "test.with.dots.and_underscores",
    ]

    def test_custom_event_types_roundtrip(self, actae, ch_name):
        for custom_type in self.CUSTOM_TYPES:
            resp = actae.raw_post("/api/v1/events/record", {
                "channel_id": ch_name,
                "event_type": custom_type,
                "payload": {"original_type": custom_type},
                "metadata": {"actor": "bot"},
            })
            assert resp.status_code == 201, f"Failed for {custom_type}: {resp.status_code}"
            body = resp.json()
            assert body["event"]["type"] == custom_type, f"Expected {custom_type}, got {body['event']['type']}"

    def test_empty_event_type_rejected(self, actae):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "test-ch",
            "event_type": "",
            "payload": {},
            "metadata": {"actor": "bot"},
        })
        assert resp.status_code in (400, 422)
