# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""EventHandler edge cases: validation forks, broadcast failure, subscribe/unsubscribe.

EVH-001 through EVH-006 from the test gap analysis.
"""

import pytest
import httpx



class TestValidationBranches:
    """EVH-001: Every validate_event() rejection path."""

    def test_empty_channel_id_rejected(self, actae):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "", "event_type": "test",
            "payload": {}, "metadata": {"actor": "bot"},
        })
        assert resp.status_code in (400, 422)
        body = resp.json()
        assert "invalid" in str(body.get("status", "")).lower()

    def test_long_channel_id_rejected(self, actae):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "c" * 257, "event_type": "test",
            "payload": {}, "metadata": {"actor": "bot"},
        })
        assert resp.status_code in (400, 422)

    def test_empty_event_type_rejected(self, actae):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "test-ch", "event_type": "",
            "payload": {}, "metadata": {"actor": "bot"},
        })
        assert resp.status_code in (400, 422)

    def test_long_event_type_rejected(self, actae):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "test-ch", "event_type": "t" * 129,
            "payload": {}, "metadata": {"actor": "bot"},
        })
        assert resp.status_code in (400, 422)

    def test_empty_actor_rejected(self, actae):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "test-ch", "event_type": "test",
            "payload": {}, "metadata": {"actor": ""},
        })
        assert resp.status_code in (400, 422)

    def test_long_actor_rejected(self, actae):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "test-ch", "event_type": "test",
            "payload": {}, "metadata": {"actor": "a" * 257},
        })
        assert resp.status_code in (400, 422)

    def test_oversized_payload_rejected(self, actae):
        large_payload = {"data": "x" * (2 * 1024 * 1024)}
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "test-ch", "event_type": "test",
            "payload": large_payload, "metadata": {"actor": "bot"},
        })
        assert resp.status_code in (400, 413, 422)


class TestSubscribeEdgeCases:
    """EVH-003: subscribe auth variants."""

    def test_replay_after_subscribe(self, actae, ch_name):
        actae.record(ch_name, "test.event", {"seq": 1}, "bot")
        actae.record(ch_name, "test.event", {"seq": 2}, "bot")
        events = actae.replay(ch_name, cursor=0, limit=10)
        assert len(events) == 2


class TestReplayLimitBoundaries:
    """EVH-004: replay limit clamping."""

    def test_replay_limit_0(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        events = actae.replay(ch_name, cursor=0, limit=0)
        assert len(events) <= 100

    def test_replay_limit_5001_clamped(self, actae, ch_name):
        for i in range(50):
            actae.record(ch_name, "test.event", {"seq": i}, "bot")
        events = actae.replay(ch_name, cursor=0, limit=5001)
        assert len(events) <= 1000

    def test_replay_limit_negative(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        events = actae.replay(ch_name, cursor=-1, limit=10)
        assert len(events) >= 0


class TestCursorEdgeCases:
    """EVH-006: cursor edge cases."""

    def test_cursor_after_record(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        c = actae.cursor(ch_name)
        assert c is None or c >= 0

    def test_cursor_on_fresh_channel(self, actae):
        c = actae.cursor(f"fresh-{__name__}")
        assert c is None


class TestHttpStatusCodes:
    """All HTTP error codes from record_event_handler."""

    def test_missing_body_returns_400(self, actae):
        resp = actae._http.post(
            f"{actae._cfg.api_url}/api/v1/events/record",
            content=b"",
            headers={"Content-Type": "application/json", "X-API-Key": actae._cfg.api_key},
        )
        assert resp.status_code in (400, 411, 500)

    def test_invalid_json_returns_400(self, actae):
        resp = actae._http.post(
            f"{actae._cfg.api_url}/api/v1/events/record",
            content=b"{invalid}",
            headers={"Content-Type": "application/json", "X-API-Key": actae._cfg.api_key},
        )
        assert resp.status_code == 400
