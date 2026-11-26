# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Durable consumer groups: leases, durable offsets, work claiming.

Covers the flagship guarantees:
- join creates a durable offset row; claim reserves events with
  FOR UPDATE SKIP LOCKED (disjoint batches across consumers)
- offsets advance only on ack; leases make unacked work reclaimable
- lease expiry → claims released, expired consumer must re-join
- heartbeat extends the lease
- group deletion cascades consumers/offsets
- parallel claim stress: 50 events, 5 consumers, no double delivery
"""

import asyncio
import threading
import time

import pytest


def _record(actae, ch, n):
    for i in range(n):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": ch,
            "type": "work",
            "payload": {"i": i},
            "metadata": {"actor": "bot"},
        })
        assert resp.status_code == 201, resp.text


def _create_group(actae, group_id, ch):
    # Clean slate: a previous failed run may have left the group (create is
    # an upsert). Deletion of a missing group is a 404 — ignore it.
    actae.raw_delete(f"/api/v1/groups/{group_id}")
    resp = actae.raw_post("/api/v1/groups", {"group_id": group_id, "channel_id": ch})
    assert resp.status_code == 201, resp.text
    return resp.json()


def _join(actae, group_id, consumer_id, lease_seconds=60):
    resp = actae.raw_post(f"/api/v1/groups/{group_id}/join",
                         {"consumer_id": consumer_id, "lease_seconds": lease_seconds})
    assert resp.status_code == 200, resp.text
    return resp.json()


def _claim(actae, group_id, consumer_id, limit=100):
    return actae.raw_post(f"/api/v1/groups/{group_id}/work",
                         {"consumer_id": consumer_id, "limit": limit})


def _ack(actae, group_id, consumer_id, cursor):
    resp = actae.raw_post(f"/api/v1/groups/{group_id}/ack",
                         {"consumer_id": consumer_id, "cursor": cursor})
    assert resp.status_code == 200, resp.text


def _heartbeat(actae, group_id, consumer_id, lease_seconds=60):
    return actae.raw_post(f"/api/v1/groups/{group_id}/heartbeat",
                         {"consumer_id": consumer_id, "lease_seconds": lease_seconds})


class TestGroupLifecycle:
    def test_create_list_delete(self, actae, ch_name):
        _record(actae, ch_name, 2)
        _create_group(actae, "grp-lc", ch_name)

        groups = actae.raw_get("/api/v1/groups", params={"channel_id": ch_name}).json()["groups"]
        assert any(g["group_id"] == "grp-lc" for g in groups)

        resp = actae.raw_delete(f"/api/v1/groups/grp-lc")
        assert resp.status_code == 200

        # Deleting again → 404.
        resp = actae.raw_delete(f"/api/v1/groups/grp-lc")
        assert resp.status_code == 404

    def test_join_claim_ack_offsets(self, actae, ch_name):
        _record(actae, ch_name, 10)
        _create_group(actae, "grp-fc", ch_name)

        offset = _join(actae, "grp-fc", "c1")
        assert offset["last_cursor"] == 0
        assert offset["claimed_cursor"] == 0

        work = _claim(actae, "grp-fc", "c1", limit=4).json()
        assert work["count"] == 4
        cursors = [e["channel_cursor"] for e in work["events"]]
        assert cursors == [1, 2, 3, 4]

        _ack(actae, "grp-fc", "c1", 4)
        offsets = actae.raw_get("/api/v1/groups/grp-fc/offsets").json()["offsets"]
        assert offsets[0]["last_cursor"] == 4

        # New consumer resumes from the durable committed offset.
        _join(actae, "grp-fc", "c2")
        work2 = _claim(actae, "grp-fc", "c2", limit=100).json()
        assert work2["count"] == 6
        assert work2["events"][0]["channel_cursor"] == 5

    def test_claim_beyond_end_returns_empty(self, actae, ch_name):
        _record(actae, ch_name, 2)
        _create_group(actae, "grp-end", ch_name)
        _join(actae, "grp-end", "c1")
        _ack(actae, "grp-end", "c1", 2)
        work = _claim(actae, "grp-end", "c1", limit=100).json()
        assert work["count"] == 0

    def test_claim_without_join_returns_409(self, actae, ch_name):
        _record(actae, ch_name, 2)
        _create_group(actae, "grp-nolease", ch_name)
        resp = _claim(actae, "grp-nolease", "ghost", limit=10)
        assert resp.status_code == 409
        assert resp.json()["status"] == "consumer_not_found"

    def test_unknown_group_returns_404(self, actae):
        resp = _claim(actae, "grp-missing", "c1", limit=10)
        assert resp.status_code == 404
        assert resp.json()["status"] == "group_not_found"

    def test_ack_without_lease_returns_409(self, actae, ch_name):
        _record(actae, ch_name, 2)
        _create_group(actae, "grp-ack", ch_name)
        resp = actae.raw_post("/api/v1/groups/grp-ack/ack",
                             {"consumer_id": "ghost", "cursor": 1})
        assert resp.status_code == 409


class TestLeases:
    def test_lease_expiry_reclaims_work(self, actae, ch_name):
        _record(actae, ch_name, 5)
        _create_group(actae, "grp-lease", ch_name)

        _join(actae, "grp-lease", "a", lease_seconds=1)
        work_a = _claim(actae, "grp-lease", "a", limit=100).json()
        assert work_a["count"] == 5

        # Consumer B cannot claim while A's lease is active.
        _join(actae, "grp-lease", "b", lease_seconds=60)
        work_b = _claim(actae, "grp-lease", "b", limit=100).json()
        assert work_b["count"] == 0

        # Wait for A's lease to lapse; the server reaper expires it
        # within 30s. Poll until the work becomes reclaimable.
        deadline = time.monotonic() + 45
        work_b2 = {"count": 0}
        while time.monotonic() < deadline:
            work_b2 = _claim(actae, "grp-lease", "b", limit=100).json()
            if work_b2["count"] == 5:
                break
            time.sleep(2)
        assert work_b2["count"] == 5, "expired claims must be reclaimable"

        # Expired consumer A must re-join before claiming again.
        resp = _claim(actae, "grp-lease", "a", limit=100)
        assert resp.status_code == 409, resp.text
        _join(actae, "grp-lease", "a", lease_seconds=60)
        work_a2 = _claim(actae, "grp-lease", "a", limit=100).json()
        assert work_a2["count"] == 0, "B already claimed everything"

    def test_heartbeat_keeps_lease_alive(self, actae, ch_name):
        _record(actae, ch_name, 3)
        _create_group(actae, "grp-hb", ch_name)
        _join(actae, "grp-hb", "c1", lease_seconds=2)

        # Claim, then heartbeat with a long lease before it lapses.
        work = _claim(actae, "grp-hb", "c1", limit=100).json()
        assert work["count"] == 3
        assert _heartbeat(actae, "grp-hb", "c1", lease_seconds=120).status_code == 200

        # Another consumer still cannot claim (lease held by heartbeat).
        _join(actae, "grp-hb", "c2", lease_seconds=60)
        blocked = _claim(actae, "grp-hb", "c2", limit=100).json()
        assert blocked["count"] == 0

    def test_heartbeat_unknown_consumer_409(self, actae, ch_name):
        _create_group(actae, "grp-hb2", ch_name)
        resp = _heartbeat(actae, "grp-hb2", "ghost", lease_seconds=60)
        assert resp.status_code == 409

    def test_rejoin_resumes_from_last_acked_offset(self, actae, ch_name):
        """A crashed consumer re-joining must resume from its last ACKED offset,
        not from its stale claimed_cursor — otherwise its own unacked work is
        silently skipped (never redelivered)."""
        _record(actae, ch_name, 10)
        _create_group(actae, "grp-rejoin", ch_name)

        _join(actae, "grp-rejoin", "c1", lease_seconds=60)
        work = _claim(actae, "grp-rejoin", "c1", limit=10).json()
        assert work["count"] == 10
        _ack(actae, "grp-rejoin", "c1", 5)

        # Simulate a restart: same consumer id re-joins. Any unacked claims
        # (cursors 6..10) must become reclaimable, not be skipped.
        offset = _join(actae, "grp-rejoin", "c1", lease_seconds=60)
        assert offset["claimed_cursor"] == 5, "re-join must drop stale claims"

        work2 = _claim(actae, "grp-rejoin", "c1", limit=10).json()
        assert work2["count"] == 5, "unacked work must be redelivered on restart"
        assert [e["channel_cursor"] for e in work2["events"]] == [6, 7, 8, 9, 10]

        _ack(actae, "grp-rejoin", "c1", 10)
        offsets = actae.raw_get("/api/v1/groups/grp-rejoin/offsets").json()["offsets"]
        assert offsets[0]["last_cursor"] == 10


class TestParallelClaims:
    def test_parallel_claim_stress_no_double_delivery(self, actae, ch_name):
        """5 consumers × 10 rounds racing on 50 events — every event is
        claimed exactly once while leases are active."""
        _record(actae, ch_name, 50)
        _create_group(actae, "grp-race", ch_name)

        results = []
        errors = []

        def worker(consumer_id):
            try:
                _join(actae, "grp-race", consumer_id, lease_seconds=60)
                seen = []
                for _ in range(20):
                    work = _claim(actae, "grp-race", consumer_id, limit=10).json()
                    if work["count"] == 0:
                        break
                    cursors = [e["channel_cursor"] for e in work["events"]]
                    seen.extend(cursors)
                    _ack(actae, "grp-race", consumer_id, max(cursors))
                results.append(seen)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(f"c{i}",)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        assert not errors, errors
        print("PER-CONSUMER:", results)
        all_claimed = [c for seen in results for c in seen]
        assert len(all_claimed) == 50, f"claimed {len(all_claimed)} != 50"
        assert len(set(all_claimed)) == 50, "duplicate delivery detected!"

        # All work durably acked by at least one consumer.
        offsets = actae.raw_get("/api/v1/groups/grp-race/offsets").json()["offsets"]
        assert sum(o["last_cursor"] for o in offsets) >= 50

    def test_parallel_fork_same_operation_id_single_child(self, actae, ch_name):
        """10 concurrent forks with the same operation_id → exactly one
        fork.started event and one state copy."""
        for i in range(3):
            resp = actae.raw_post("/api/v1/events/record", {
                "channel_id": ch_name,
                "type": "step",
                "payload": {"i": i},
                "metadata": {"actor": "bot"},
            })
            assert resp.status_code == 201
        actae.raw_post(f"/api/v1/state/{ch_name}", {
            "channel_id": ch_name, "cursor": 3, "state": {"s": 3},
        })

        op_id = f"race-op-{int(time.time() * 1000)}"
        statuses = []
        errors = []

        def fork_worker():
            try:
                for attempt in range(5):
                    resp = actae.raw_post("/api/v1/channels/fork", {
                        "source_channel_id": ch_name,
                        "new_channel_id": f"{ch_name}-child",
                        "at_cursor": 3,
                        "operation_id": op_id,
                    })
                    if resp.status_code < 500 or attempt == 4:
                        statuses.append((resp.status_code, resp.json().get("replayed")))
                        break
                    time.sleep(0.5)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=fork_worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, errors
        assert all(code == 201 for code, _ in statuses)
        assert sum(1 for _, replayed in statuses if not replayed) == 1, \
            "exactly one fresh fork expected"
        assert sum(1 for _, replayed in statuses if replayed) == 9, \
            "nine idempotent replays expected"

        events = actae.replay(f"{ch_name}-child", cursor=0, limit=10)
        assert len(events) == 1
        assert events[0]["type"] == "fork.started"
