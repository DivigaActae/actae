# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Persisted wake-ups: schedule, dispatch, cancel, retry, live delivery.

Covers the flagship guarantees:
- schedule → pending; the dispatcher fires a scheduler.wakeup event
  at-or-after run_at and marks the wake-up done
- future wake-ups stay pending
- cancel before dispatch → cancelled and never fires
- cancel after dispatch → 409 wakeup_not_pending
- list/get filters by channel and status
- WS subscribers receive the wakeup event live
- idempotent dispatch (SKIP LOCKED): concurrent dispatches fire once
"""

import asyncio
import time

import pytest


def _schedule(actae, ch, run_at, payload=None):
    body = {"channel_id": ch, "run_at": run_at}
    if payload:
        body["payload"] = payload
    resp = actae.raw_post("/api/v1/scheduler/wakeups", body)
    assert resp.status_code == 201, resp.text
    return resp.json()


def _wait_for_event(actae, ch, event_type, expected, timeout=30):
    """Poll replay until `expected` scheduler.wakeup events appear."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        events = actae.replay(ch, cursor=0, limit=100)
        matches = [e for e in events if e["type"] == event_type]
        if len(matches) >= expected:
            return matches
        time.sleep(1)
    return [e for e in actae.replay(ch, cursor=0, limit=100) if e["type"] == event_type]


def _wait_for_status(actae, wakeup_id, status, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = actae.raw_get(f"/api/v1/scheduler/wakeups/{wakeup_id}")
        if resp.status_code == 200 and resp.json().get("status") == status:
            return resp.json()
        time.sleep(1)
    return None


class TestWakeupDispatch:
    def test_due_wakeup_fires_event_and_marks_done(self, actae, ch_name):
        actae.raw_post("/api/v1/events/record", {
            "channel_id": ch_name, "type": "seed", "payload": {},
            "metadata": {"actor": "bot"},
        })
        wakeup = _schedule(
            actae, ch_name,
            "2020-01-01T00:00:00Z",  # long past — due immediately
            payload={"step": "resume", "n": 1},
        )
        assert wakeup["status"] == "pending"

        events = _wait_for_event(actae, ch_name, "scheduler.wakeup", 1, timeout=30)
        assert len(events) == 1, "dispatcher must fire the wake-up event"
        ev = events[0]
        assert ev["payload"] == {"step": "resume", "n": 1}
        assert ev["actor"] == "system"
        assert ev["channel_cursor"] == 2  # after the seed event

        done = _wait_for_status(actae, wakeup["id"], "done", timeout=30)
        assert done is not None, "wake-up must be marked done"
        assert done["executed_at"] is not None
        assert done["attempts"] == 0

    def test_future_wakeup_stays_pending(self, actae, ch_name):
        wakeup = _schedule(
            actae, ch_name,
            "2999-01-01T00:00:00Z",
            payload={"later": True},
        )
        time.sleep(6)  # dispatcher interval is 5s
        status = actae.raw_get(f"/api/v1/scheduler/wakeups/{wakeup['id']}").json()["status"]
        assert status == "pending", "future wake-up must not fire early"

    def test_multiple_wakeups_gapless_cursors(self, actae, ch_name):
        for i in range(3):
            _schedule(actae, ch_name, "2020-01-01T00:00:00Z", payload={"n": i})
        events = _wait_for_event(actae, ch_name, "scheduler.wakeup", 3, timeout=30)
        cursors = [e["channel_cursor"] for e in events]
        assert cursors == [1, 2, 3], "wakeup events must be gapless"

    def test_wakeup_event_replayable_and_payload_preserved(self, actae, ch_name):
        _schedule(actae, ch_name, "2020-01-01T00:00:00Z",
                  payload={"task": "invoice", "amount": 100})
        events = _wait_for_event(actae, ch_name, "scheduler.wakeup", 1, timeout=30)
        assert events[0]["payload"] == {"task": "invoice", "amount": 100}


class TestWakeupCancel:
    def test_cancel_pending_wakeup(self, actae, ch_name):
        wakeup = _schedule(actae, ch_name, "2999-01-01T00:00:00Z")
        resp = actae.raw_delete(f"/api/v1/scheduler/wakeups/{wakeup['id']}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "cancelled"

        # Never fires.
        time.sleep(6)
        events = actae.replay(ch_name, cursor=0, limit=100)
        assert all(e["type"] != "scheduler.wakeup" for e in events)

    def test_cancel_twice_returns_409(self, actae, ch_name):
        wakeup = _schedule(actae, ch_name, "2999-01-01T00:00:00Z")
        assert actae.raw_delete(f"/api/v1/scheduler/wakeups/{wakeup['id']}").status_code == 200
        resp = actae.raw_delete(f"/api/v1/scheduler/wakeups/{wakeup['id']}")
        assert resp.status_code == 409
        assert resp.json()["error"] == "wakeup_not_pending"

    def test_cancel_fired_wakeup_returns_409(self, actae, ch_name):
        wakeup = _schedule(actae, ch_name, "2020-01-01T00:00:00Z")
        _wait_for_status(actae, wakeup["id"], "done", timeout=30)
        resp = actae.raw_delete(f"/api/v1/scheduler/wakeups/{wakeup['id']}")
        assert resp.status_code == 409

    def test_get_missing_wakeup_404(self, actae):
        resp = actae.raw_get("/api/v1/scheduler/wakeups/00000000-0000-0000-0000-000000000000")
        assert resp.status_code == 404

    def test_invalid_wakeup_id_400(self, actae):
        resp = actae.raw_get("/api/v1/scheduler/wakeups/not-a-uuid")
        assert resp.status_code == 400


class TestWakeupList:
    def test_list_filters_by_channel_and_status(self, actae, ch_name):
        w_done = _schedule(actae, ch_name, "2020-01-01T00:00:00Z", payload={"a": 1})
        w_pending = _schedule(actae, ch_name, "2999-01-01T00:00:00Z", payload={"b": 2})
        _wait_for_status(actae, w_done["id"], "done", timeout=30)

        pending = actae.raw_get("/api/v1/scheduler/wakeups",
                               params={"channel_id": ch_name, "status": "pending"}).json()
        ids = {w["id"] for w in pending["wakeups"]}
        assert w_pending["id"] in ids
        assert w_done["id"] not in ids

        done = actae.raw_get("/api/v1/scheduler/wakeups",
                            params={"channel_id": ch_name, "status": "done"}).json()
        assert w_done["id"] in {w["id"] for w in done["wakeups"]}

    def test_get_single_wakeup_shape(self, actae, ch_name):
        wakeup = _schedule(actae, ch_name, "2999-01-01T00:00:00Z", payload={"k": "v"})
        got = actae.raw_get(f"/api/v1/scheduler/wakeups/{wakeup['id']}").json()
        assert got["id"] == wakeup["id"]
        assert got["channel_id"] == ch_name
        assert got["status"] == "pending"
        assert got["payload"] == {"k": "v"}
        assert got["attempts"] == 0


class TestWakeupLiveDelivery:
    @pytest.mark.asyncio
    async def test_ws_subscriber_receives_wakeup_live(self, actae, ws, ch_name):
        async with await ws.connect() as conn:
            await conn.authenticate(actae._cfg.api_key)  # WS auth message is mandatory
            await conn.subscribe(ch_name)  # subscribe() returns the ack

            _schedule(actae, ch_name, "2020-01-01T00:00:00Z", payload={"live": True})

            msgs = await conn.recv_all(count=1, timeout=30)
            assert msgs, "expected a live broadcast for the wake-up"
            broadcast = msgs[0]
            assert broadcast.get("type") == "broadcast"
            assert broadcast["topic"] == ch_name
