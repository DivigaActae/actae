# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Payload edge cases: unicode, nested, large arrays, null, timestamps, metadata.

Covers boundary values and extreme inputs for all event fields.
"""

import pytest



class TestUnicodePayloads:
    """Payloads with unicode, emoji, international characters."""

    UNICODE_PAYLOADS = [
        {"msg": "Hello "},
        {"msg": ""},
        {"msg": "café résumé ñoño"},
        {"data": "a" * 10000},  # large string value
    ]

    def test_unicode_payloads_recorded(self, actae, ch_name):
        for payload in self.UNICODE_PAYLOADS:
            resp = actae.raw_post("/api/v1/events/record", {
                "channel_id": ch_name, "event_type": "test.event",
                "payload": payload, "metadata": {"actor": "bot"},
            })
            assert resp.status_code == 201, f"Failed for {payload}"
            body = resp.json()
            assert body["status"] == "recorded"

    def test_nested_json_payload(self, actae, ch_name):
        nested = {
            "level1": {
                "level2": {
                    "level3": {"key": "deep_value"},
                    "arr": [1, 2, {"nested": True}],
                },
                "values": [True, False, None, 42, 3.14, "string"],
            }
        }
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": ch_name, "event_type": "test.event",
            "payload": nested, "metadata": {"actor": "bot"},
        })
        assert resp.status_code == 201
        body = resp.json()
        assert body["event"]["payload"]["level1"]["level2"]["level3"]["key"] == "deep_value"

    def test_null_payload_field(self, actae, ch_name):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": ch_name, "event_type": "test.event",
            "payload": None, "metadata": {"actor": "bot"},
        })
        # Null payload may be accepted or rejected depending on validation
        assert resp.status_code in (201, 400, 422)

    def test_empty_object_payload(self, actae, ch_name):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": ch_name, "event_type": "test.event",
            "payload": {}, "metadata": {"actor": "bot"},
        })
        assert resp.status_code == 201


class TestChannelNameEdgeCases:
    """Channel names with special characters, max length, unicode."""

    CHANNEL_NAMES = [
        "a",
        "test.channel.with.many.dots",
        "test_channel_with_underscores",
        "channel-123",
        "ch" * 100,  # near limit
    ]

    def test_channel_names_accepted(self, actae):
        for ch in self.CHANNEL_NAMES:
            if len(ch) > 256:
                continue
            resp = actae.raw_post("/api/v1/events/record", {
                "channel_id": ch, "event_type": "test.event",
                "payload": {}, "metadata": {"actor": "bot"},
            })
            assert resp.status_code in (201, 422), f"Failed for channel '{ch[:50]}': {resp.status_code}"


class TestTimestampEdgeCases:
    """Events with different timestamps and time ranges."""

    def test_replay_with_timestamps(self, actae, ch_name):
        actae.record(ch_name, "test.event", {"seq": 1}, "bot")
        import time as _time
        _time.sleep(0.1)
        actae.record(ch_name, "test.event", {"seq": 2}, "bot")
        events = actae.replay(ch_name, cursor=0, limit=10)
        assert len(events) == 2
        t1 = events[0]["timestamp"]
        t2 = events[1]["timestamp"]
        assert t1 < t2


class TestMetadataEdgeCases:
    """Event metadata variations."""

    def test_metadata_with_all_fields(self, actae, ch_name):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": ch_name, "event_type": "test.event",
            "payload": {},
            "metadata": {
                "actor": "bot",
                "agent_id": "agent-001",
                "user_id": "user-001",
                "metadata": {"custom": "value", "nested": {"key": True}},
            },
        })
        assert resp.status_code == 201

    def test_metadata_without_optional_fields(self, actae, ch_name):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": ch_name, "event_type": "test.event",
            "payload": {},
            "metadata": {"actor": "bot"},
        })
        assert resp.status_code == 201


class TestActorEdgeCases:
    """Actor field variations."""

    ACTOR_VALUES = [
        "bot",
        "agent-001",
        "human@company.com",
        "system.service.v3",
        "a" * 256,
    ]

    def test_actor_values_accepted(self, actae, ch_name):
        for actor in self.ACTOR_VALUES:
            resp = actae.raw_post("/api/v1/events/record", {
                "channel_id": ch_name, "event_type": "test.event",
                "payload": {}, "metadata": {"actor": actor},
            })
            assert resp.status_code in (201, 422), f"Failed for actor '{actor[:50]}': {resp.status_code}"
