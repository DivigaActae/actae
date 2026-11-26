# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Input validation and error handling.

Covers: VA-01 through VA-07 from the test strategy.
"""

import json
import pytest


class TestFieldValidation:
    """VA-01 through VA-06: Required field and size validation."""

    def test_empty_channel_id_rejected(self, actae):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "",
            "event_type": "agent.started",
            "payload": {},
            "metadata": {"actor": "test"},
        })
        assert resp.status_code in (400, 422)
        body = resp.json()
        assert "invalid" in body.get("status", "").lower()

    def test_empty_event_type_rejected(self, actae):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "test-ch",
            "event_type": "",
            "payload": {},
            "metadata": {"actor": "test"},
        })
        assert resp.status_code in (400, 422)
        body = resp.json()
        assert "invalid" in body.get("status", "").lower()

    def test_empty_actor_rejected(self, actae):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "test-ch",
            "event_type": "agent.started",
            "payload": {},
            "metadata": {"actor": ""},
        })
        assert resp.status_code in (400, 422)
        body = resp.json()
        assert "invalid" in body.get("status", "").lower()

    def test_oversized_channel_id_rejected(self, actae):
        long_ch = "c" * 257
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": long_ch,
            "event_type": "agent.started",
            "payload": {},
            "metadata": {"actor": "test"},
        })
        assert resp.status_code in (400, 422)
        body = resp.json()
        assert "invalid" in body.get("status", "").lower()

    def test_oversized_payload_rejected(self, actae):
        large = {"data": "x" * (2 * 1024 * 1024)}
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "test-ch",
            "event_type": "agent.started",
            "payload": large,
            "metadata": {"actor": "test"},
        })
        assert resp.status_code in (400, 413, 422)

    def test_malformed_json_rejected(self, actae):
        resp = actae._http.post(
            f"{actae._cfg.api_url}/api/v1/events/record",
            content=b"not-json-at-all",
            headers={"Content-Type": "application/json", "X-API-Key": actae._cfg.api_key},
        )
        assert resp.status_code in (400, 422)

    def test_oversized_event_type_rejected(self, actae):
        long_type = "t" * 129
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "test-ch",
            "event_type": long_type,
            "payload": {},
            "metadata": {"actor": "test"},
        })
        assert resp.status_code in (400, 422)

    def test_oversized_actor_rejected(self, actae):
        long_actor = "a" * 257
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "test-ch",
            "event_type": "agent.started",
            "payload": {},
            "metadata": {"actor": long_actor},
        })
        assert resp.status_code in (400, 422)


class TestHTTPEdgeCases:
    """HTTP-level edge cases."""

    def test_missing_metadata_rejected(self, actae):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "test-ch",
            "event_type": "agent.started",
            "payload": {},
        })
        # Should fail — metadata is required for actor field
        assert resp.status_code in (400, 422)

    def test_null_payload_accepted(self, actae, ch_name):
        body = {
            "channel_id": ch_name,
            "event_type": "test.event",
            "payload": None,
            "metadata": {"actor": "test"},
        }
        resp = actae.raw_post("/api/v1/events/record", body)
        assert resp.status_code == 201

    def test_empty_object_payload_accepted(self, actae, ch_name):
        result = actae.record(ch_name, "test.event", {}, "test")
        assert result.get("status") == "recorded"

    def test_numeric_metadata_fields(self, actae, ch_name):
        result = actae.record(ch_name, "test.event", {"n": 42, "f": 3.14}, "test")
        assert result.get("status") == "recorded"


class TestReplayEdgeCases:
    """Edge cases specific to replay."""

    def test_replay_with_missing_channel(self, actae):
        events = actae.replay("channel-does-not-exist-xyz", cursor=0, limit=10)
        assert len(events) == 0

    def test_replay_with_zero_limit(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        events = actae.replay(ch_name, cursor=0, limit=0)
        assert len(events) <= 100

    def test_cursor_on_empty_channel(self, actae):
        c = actae.cursor(f"empty-{__name__}-cursor")
        assert c is None

    def test_query_with_empty_result(self, actae):
        events = actae.query(
            event_type="nonexistent.type.xyz",
            limit=10,
        )
        assert len(events) == 0
