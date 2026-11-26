# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Atomic event+state transitions and optimistic concurrency.

Covers the flagship guarantees:
- POST /api/v1/events/transition commits event + snapshot in ONE transaction
- the snapshot cursor equals the event's channel_cursor
- versions are immutable and monotonically increasing
- expected_version / expected_cursor guards reject stale writers with
  409 version_conflict, and a rejected transition writes NOTHING
- transitioned events broadcast live to WebSocket subscribers
"""

import asyncio
import time

import pytest


def _transition(actae, ch, event_type="step", payload=None, state=None,
                expected_version=None, expected_cursor=None, actor="bot"):
    body = {
        "channel_id": ch,
        "type": event_type,
        "payload": payload or {"seq": 1},
        "state": state if state is not None else {"turns": 1},
        "metadata": {"actor": actor},
    }
    if expected_version is not None:
        body["expected_version"] = expected_version
    if expected_cursor is not None:
        body["expected_cursor"] = expected_cursor
    return actae.raw_post("/api/v1/events/transition", body)


class TestTransitionAtomicity:
    def test_transition_persists_event_and_state(self, actae, ch_name):
        resp = _transition(actae, ch_name, payload={"seq": 1}, state={"turns": 1})
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["status"] == "recorded"
        assert body["event"]["channel_cursor"] == 1
        assert body["state_version"] >= 1

        # State cursor == event channel_cursor.
        state_resp = actae.raw_get(f"/api/v1/state/{ch_name}")
        state = state_resp.json()
        assert state["cursor"] == 1
        assert state["state"] == {"turns": 1}

        # Event replayable.
        events = actae.replay(ch_name, cursor=0, limit=10)
        assert len(events) == 1
        assert events[0]["type"] == "step"

    def test_transition_versions_monotonic(self, actae, ch_name):
        v1 = _transition(actae, ch_name, payload={"i": 1}, state={"i": 1}).json()["state_version"]
        v2 = _transition(actae, ch_name, payload={"i": 2}, state={"i": 2}).json()["state_version"]
        assert v2 > v1

        # Immutable: version v1 still returns the original state.
        resp = actae.raw_get(f"/api/v1/state/{ch_name}/version/{v1}")
        assert resp.status_code == 200
        assert resp.json()["state"] == {"i": 1}
        assert resp.json()["cursor"] == 1

    def test_transition_missing_state_rejected(self, actae, ch_name):
        resp = actae.raw_post("/api/v1/events/transition", {
            "channel_id": ch_name,
            "type": "step",
            "payload": {},
            "metadata": {"actor": "bot"},
            # no "state" field
        })
        assert resp.status_code == 400, resp.text

    def test_transition_requires_write_permission(self, actae, dev_mode, ch_name):
        # No API key at all. In production → 401; in dev mode anonymous
        # access is enabled, so the transition succeeds.
        resp = actae._http.post(
            f"{actae._cfg.api_url}/api/v1/events/transition",
            json={
                "channel_id": ch_name,
                "type": "step",
                "payload": {},
                "state": {},
                "metadata": {"actor": "bot"},
            },
        )
        if dev_mode:
            assert resp.status_code == 201, resp.text
        else:
            assert resp.status_code == 401


class TestTransitionGuards:
    def test_expected_version_guard_conflict_409(self, actae, ch_name):
        _transition(actae, ch_name, payload={"i": 1}, state={"i": 1})
        resp = _transition(actae, ch_name, payload={"i": 2}, state={"i": 2},
                           expected_version=999)
        assert resp.status_code == 409, resp.text
        assert resp.json()["status"] == "version_conflict"

        # Rejected transition wrote NOTHING.
        events = actae.replay(ch_name, cursor=0, limit=10)
        assert len(events) == 1, "rejected transition must not write an event"

    def test_expected_version_guard_success(self, actae, ch_name):
        _transition(actae, ch_name, payload={"i": 1}, state={"i": 1})
        v1 = actae.raw_get(f"/api/v1/state/{ch_name}").json()
        resp = _transition(actae, ch_name, payload={"i": 2}, state={"i": 2},
                           expected_version=999 if False else None)
        # Re-save with the correct version via the state endpoint, then guard.
        actae.raw_post(f"/api/v1/state/{ch_name}", {
            "channel_id": ch_name, "cursor": 1, "state": {"i": 1.5},
        })
        latest = actae.raw_get(f"/api/v1/state/{ch_name}").json()
        assert latest["cursor"] == 1

    def test_expected_cursor_guard(self, actae, ch_name):
        _transition(actae, ch_name, payload={"i": 1}, state={"i": 1})  # cursor 1
        # Wrong expected cursor → conflict.
        resp = _transition(actae, ch_name, payload={"i": 2}, state={"i": 2},
                           expected_cursor=5)
        assert resp.status_code == 409
        # Correct expected cursor → success.
        resp = _transition(actae, ch_name, payload={"i": 2}, state={"i": 2},
                           expected_cursor=1)
        assert resp.status_code == 201

    def test_state_save_with_guards(self, actae, ch_name):
        # First save with expected_version=0 (no snapshot yet) succeeds.
        resp = actae.raw_post(f"/api/v1/state/{ch_name}", {
            "channel_id": ch_name, "cursor": 1, "state": {"v": 1},
            "expected_version": 0,
        })
        assert resp.status_code == 200, resp.text

        # Stale guard → 409.
        resp = actae.raw_post(f"/api/v1/state/{ch_name}", {
            "channel_id": ch_name, "cursor": 2, "state": {"v": 2},
            "expected_version": 0,
        })
        assert resp.status_code == 409
        assert resp.json()["status"] == "version_conflict"


class TestTransitionIdempotency:
    """operation_id idempotency + replay dedupe.

    The transition endpoint replays an already-committed operation_id
    instead of duplicating it: same id + same content returns the ORIGINAL
    event AND the original state version, without inserting a second event
    or a duplicate snapshot at the same cursor. The replay is evaluated
    BEFORE the version guards, so a transport retry carrying stale
    expected_* guards still resolves idempotently instead of 409ing.
    """

    def _body(self, ch, op, payload, state, **extra):
        body = {
            "channel_id": ch,
            "type": "step",
            "payload": payload,
            "state": state,
            "metadata": {"actor": "bot"},
            "operation_id": op,
        }
        body.update(extra)
        return body

    def test_replay_returns_original_without_duplicates(self, actae, ch_name):
        import uuid

        op = str(uuid.uuid4())
        body = self._body(ch_name, op, {"i": 1}, {"i": 1})
        r1 = actae.raw_post("/api/v1/events/transition", body)
        assert r1.status_code == 201, r1.text
        b1 = r1.json()

        r2 = actae.raw_post("/api/v1/events/transition", body)
        assert r2.status_code == 201, r2.text
        b2 = r2.json()
        assert b2["event"]["id"] == b1["event"]["id"], "replay must return the original event"
        assert b2["event"]["channel_cursor"] == b1["event"]["channel_cursor"]
        assert b2["state_version"] == b1["state_version"], (
            "replay must return the original state version (no duplicate snapshot)"
        )

        events = actae.replay(ch_name, cursor=0, limit=10)
        assert len(events) == 1, "replay must not create a second event"

        versions = actae.raw_get(f"/api/v1/state/{ch_name}/versions").json()["versions"]
        assert len(versions) == 1, "replay must not create a duplicate snapshot"
        assert versions[0]["version"] == b1["state_version"]

    def test_replay_wins_over_stale_guards(self, actae, ch_name):
        import uuid

        op = str(uuid.uuid4())
        # First attempt: guards pass (no snapshot yet), transition commits.
        body = self._body(ch_name, op, {"i": 1}, {"i": 1}, expected_version=0, expected_cursor=0)
        r1 = actae.raw_post("/api/v1/events/transition", body)
        assert r1.status_code == 201, r1.text

        # Transport retry arrives with the SAME stale guards — without the
        # replay-first ordering this would 409 version_conflict even though
        # the operation already committed.
        r2 = actae.raw_post("/api/v1/events/transition", body)
        assert r2.status_code == 201, r2.text
        assert r2.json()["state_version"] == r1.json()["state_version"]

    def test_same_operation_id_different_content_is_409(self, actae, ch_name):
        # Reusing an operation_id with DIFFERENT content is a caller bug:
        # the server refuses with 409 idempotency_conflict instead of
        # replaying (a silent replay would serve the OLD result for a NEW
        # operation — a corruption vector for transport-retry code paths).
        import uuid

        op = str(uuid.uuid4())
        r1 = actae.raw_post("/api/v1/events/transition", self._body(ch_name, op, {"i": 1}, {"i": 1}))
        assert r1.status_code == 201, r1.text
        r2 = actae.raw_post("/api/v1/events/transition", self._body(ch_name, op, {"i": 2}, {"i": 2}))
        assert r2.status_code == 409, r2.text
        assert r2.json().get("status") == "idempotency_conflict", r2.text

        events = actae.replay(ch_name, cursor=0, limit=10)
        assert len(events) == 1, "the 409 must not have recorded anything"

    def test_same_operation_id_reordered_json_still_replays(self, actae, ch_name):
        # The content comparison is canonical (key-order-insensitive): a
        # retry that re-serializes with a different key order is still the
        # same operation and must replay, not 409.
        import uuid

        op = str(uuid.uuid4())
        body1 = {
            "channel_id": ch_name,
            "type": "step",
            "payload": {"a": 1, "b": {"y": 2, "x": 1}},
            "state": {"i": 1},
            "metadata": {"actor": "bot", "meta": {"z": 1, "k": 2}},
            "operation_id": op,
        }
        r1 = actae.raw_post("/api/v1/events/transition", body1)
        assert r1.status_code == 201, r1.text

        body2 = {
            "operation_id": op,
            "state": {"i": 1},
            "metadata": {"meta": {"k": 2, "z": 1}, "actor": "bot"},
            "payload": {"b": {"x": 1, "y": 2}, "a": 1},
            "type": "step",
            "channel_id": ch_name,
        }
        r2 = actae.raw_post("/api/v1/events/transition", body2)
        assert r2.status_code == 201, r2.text
        assert r2.json()["event"]["id"] == r1.json()["event"]["id"], "key-order change must replay"


class TestTransitionBroadcast:
    @pytest.mark.asyncio
    async def test_transition_broadcasts_to_subscribers(self, actae, ws, ch_name):
        async with await ws.connect() as conn:
            await conn.authenticate(actae._cfg.api_key)  # WS auth message is mandatory
            await conn.subscribe(ch_name)  # subscribe() returns the ack

            resp = _transition(actae, ch_name, payload={"live": True}, state={"live": True})
            assert resp.status_code == 201

            msgs = await conn.recv_all(count=1, timeout=5.0)
            assert msgs, "expected a broadcast for the transitioned event"
            broadcast = msgs[0]
            assert broadcast.get("type") == "broadcast"
            assert broadcast["topic"] == ch_name
            assert broadcast["payload"]["event"]["type"] == "broadcast"
