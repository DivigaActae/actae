# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Fork boundary semantics: exact-boundary state selection, fork.started
lineage, idempotency, and error mapping.

Covers the flagship fork runtime guarantees end-to-end over HTTP:
- at_cursor > 0 selects the latest saved snapshot at-or-before the cursor
- at_cursor = 0 forks from latest state (empty child allowed)
- missing boundary → 409 snapshot_boundary_required
- child starts with exactly one fork.started event (channel_cursor 1)
- fork.started.depends_on references the parent's boundary event
- operation_id / child_id replays never duplicate state or events
"""

import time

import pytest


def _record(actae, ch, n=3):
    for i in range(n):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": ch,
            "type": "step",
            "payload": {"seq": i},
            "metadata": {"actor": "bot"},
        })
        assert resp.status_code == 201, resp.text


def _save_state(actae, ch, cursor, state):
    resp = actae.raw_post(f"/api/v1/state/{ch}", {
        "channel_id": ch,
        "cursor": cursor,
        "state": state,
    })
    assert resp.status_code == 200, resp.text
    return resp.json()["version"]


def _fork(actae, source, child, at_cursor=0, operation_id=None, reason=None):
    body = {
        "source_channel_id": source,
        "new_channel_id": child,
        "at_cursor": at_cursor,
    }
    if operation_id:
        body["operation_id"] = operation_id
    if reason:
        body["reason"] = reason
    return actae.raw_post("/api/v1/channels/fork", body)


def _child_state(actae, child):
    resp = actae.raw_get(f"/api/v1/state/{child}")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    if data.get("cursor") is None:
        return None
    return (data["cursor"], data["state"])


class TestExactBoundary:
    def test_fork_selects_snapshot_at_or_before_cursor(self, actae, ch_name):
        _record(actae, ch_name, 5)
        _save_state(actae, ch_name, 2, {"boundary": 2})
        _save_state(actae, ch_name, 5, {"boundary": 5})

        resp = _fork(actae, ch_name, f"{ch_name}-child", at_cursor=3)
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["status"] == "forked"
        # forked_at_cursor is the RESOLVED boundary (the snapshot inherited),
        # while requested_at_cursor records what the caller asked for.
        assert body["forked_at_cursor"] == 2
        assert body["requested_at_cursor"] == 3
        assert body["resolved_cursor"] == 2
        assert body["restorable"] is True
        assert body["replayed"] is False

        cursor, state = _child_state(actae, f"{ch_name}-child")
        assert cursor == 2, "must resolve to the snapshot at cursor 2"
        assert state == {"boundary": 2}

    def test_fork_at_cursor_beyond_latest_snapshot_uses_latest(self, actae, ch_name):
        _record(actae, ch_name, 5)
        _save_state(actae, ch_name, 4, {"boundary": 4})
        resp = _fork(actae, ch_name, f"{ch_name}-child", at_cursor=100)
        assert resp.status_code == 201, resp.text
        cursor, state = _child_state(actae, f"{ch_name}-child")
        assert cursor == 4
        assert state == {"boundary": 4}

    def test_fork_missing_boundary_returns_409(self, actae, ch_name):
        _record(actae, ch_name, 3)
        # No snapshot saved at all; at_cursor > 0 must fail.
        resp = _fork(actae, ch_name, f"{ch_name}-child", at_cursor=2)
        assert resp.status_code == 409, resp.text
        assert resp.json()["status"] == "snapshot_boundary_required"

        # Still nothing written on the child.
        events = actae.replay(f"{ch_name}-child", cursor=0, limit=10)
        assert events == []

    def test_fork_zero_cursor_latest_state(self, actae, ch_name):
        # Event-only channel (no snapshots): at_cursor=0 still forks.
        _record(actae, ch_name, 3)
        resp = _fork(actae, ch_name, f"{ch_name}-child", at_cursor=0)
        assert resp.status_code == 201, resp.text
        child_events = actae.replay(f"{ch_name}-child", cursor=0, limit=10)
        assert len(child_events) == 1
        assert child_events[0]["type"] == "fork.started"

    def test_fork_negative_cursor_rejected(self, actae, ch_name):
        resp = _fork(actae, ch_name, f"{ch_name}-child", at_cursor=-1)
        assert resp.status_code == 400, resp.text


class TestForkStartedLineage:
    def test_child_starts_with_single_fork_event(self, actae, ch_name):
        _record(actae, ch_name, 3)
        _save_state(actae, ch_name, 3, {"s": 3})
        assert _fork(actae, ch_name, f"{ch_name}-child", at_cursor=3).status_code == 201

        events = actae.replay(f"{ch_name}-child", cursor=0, limit=10)
        assert len(events) == 1
        ev = events[0]
        assert ev["type"] == "fork.started"
        assert ev["channel_cursor"] == 1
        assert ev["actor"] == "system"
        assert ev["payload"]["source_channel_id"] == ch_name
        assert ev["payload"]["forked_at_cursor"] == 3

        # The child's own first event continues at channel_cursor 2.
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": f"{ch_name}-child",
            "type": "step",
            "payload": {"own": True},
            "metadata": {"actor": "bot"},
        })
        assert resp.status_code == 201
        assert resp.json()["event"]["channel_cursor"] == 2

    def test_fork_started_depends_on_parent_boundary_event(self, actae, ch_name):
        _record(actae, ch_name, 4)
        parent_events = actae.replay(ch_name, cursor=0, limit=10)
        boundary = parent_events[-1]
        # Snapshots are pinned to *global* event cursors (the space the
        # dashboard timeline and the SDK use) — save at the real event cursor,
        # not a small per-channel number.
        _save_state(actae, ch_name, boundary["cursor"], {"s": boundary["cursor"]})
        assert _fork(actae, ch_name, f"{ch_name}-child", at_cursor=boundary["cursor"]).status_code == 201

        child_events = actae.replay(f"{ch_name}-child", cursor=0, limit=10)
        assert child_events[0]["type"] == "fork.started"
        # depends_on must point at the event AT the fork boundary (global
        # cursor space), not the channel's latest event.
        assert child_events[0]["depends_on"] == boundary["id"]

    def test_nested_fork_lineage(self, actae, ch_name):
        _record(actae, ch_name, 3)
        _save_state(actae, ch_name, 3, {"s": 3})
        assert _fork(actae, ch_name, f"{ch_name}-parent", at_cursor=3).status_code == 201
        _save_state(actae, f"{ch_name}-parent", 1, {"ps": 1})
        assert _fork(actae, f"{ch_name}-parent", f"{ch_name}-child", at_cursor=1).status_code == 201

        meta = actae.raw_get(f"/api/v1/channels/{ch_name}-child/metadata").json()
        assert meta["parent_channel_id"] == f"{ch_name}-parent"
        assert meta["origin_run_id"] == ch_name
        assert meta["forked_at_cursor"] == 1


class TestForkIdempotency:
    def test_same_operation_id_replays_without_duplication(self, actae, ch_name):
        _record(actae, ch_name, 3)
        _save_state(actae, ch_name, 3, {"s": 3})
        op_id = f"op-{int(time.time() * 1000)}"

        first = _fork(actae, ch_name, f"{ch_name}-child", at_cursor=3, operation_id=op_id)
        assert first.status_code == 201
        assert first.json()["replayed"] is False

        second = _fork(actae, ch_name, f"{ch_name}-child", at_cursor=3, operation_id=op_id)
        assert second.status_code == 201
        assert second.json()["replayed"] is True
        assert second.json()["channel_id"] == first.json()["channel_id"]

        events = actae.replay(f"{ch_name}-child", cursor=0, limit=10)
        assert len(events) == 1, "replay must not duplicate fork.started"
        cursor, _ = _child_state(actae, f"{ch_name}-child")
        assert cursor == 3

    def test_same_child_id_different_operation_is_noop(self, actae, ch_name):
        _record(actae, ch_name, 3)
        _save_state(actae, ch_name, 3, {"s": 3})
        assert _fork(actae, ch_name, f"{ch_name}-child", at_cursor=3).status_code == 201
        # Same child id, different operation id (no explicit op).
        resp = _fork(actae, ch_name, f"{ch_name}-child", at_cursor=3)
        assert resp.status_code == 201
        assert resp.json()["replayed"] is True
        events = actae.replay(f"{ch_name}-child", cursor=0, limit=10)
        assert len(events) == 1

    def test_fork_different_child_ids_are_independent(self, actae, ch_name):
        _record(actae, ch_name, 3)
        _save_state(actae, ch_name, 3, {"s": 3})
        for suffix in ("a", "b"):
            resp = _fork(actae, ch_name, f"{ch_name}-{suffix}", at_cursor=3)
            assert resp.status_code == 201
            assert resp.json()["replayed"] is False

        # Events written on fork A do not leak into fork B.
        actae.raw_post("/api/v1/events/record", {
            "channel_id": f"{ch_name}-a",
            "type": "step",
            "payload": {"fork": "a"},
            "metadata": {"actor": "bot"},
        })
        events_b = actae.replay(f"{ch_name}-b", cursor=0, limit=10)
        assert len(events_b) == 1
        assert events_b[0]["type"] == "fork.started"


class TestPostAuditProvenance:
    """Post-audit hardening: resolved boundary provenance, immutable receipt,
    idempotency conflicts, per-side divergence, truncation."""

    def test_fork_receipt_reports_requested_vs_resolved(self, actae, ch_name):
        _record(actae, ch_name, 5)
        _save_state(actae, ch_name, 2, {"at": 2})
        _save_state(actae, ch_name, 5, {"at": 5})

        resp = _fork(actae, ch_name, f"{ch_name}-child", at_cursor=3)
        assert resp.status_code == 201, resp.text
        body = resp.json()
        # requested=3 resolves to the nearest snapshot at-or-before = 2.
        assert body["requested_at_cursor"] == 3
        assert body["resolved_cursor"] == 2
        assert body["restorable"] is True
        assert body["source_state_version"] >= 1
        assert body["source_state_sha256"]
        # requested != resolved → the boundary is approximate, honestly graded.
        assert body["reproducibility"] == "best_effort"
        assert body["forked_at_cursor"] == 2, "forked_at_cursor is the RESOLVED boundary"

        # An EXACT fork (requested == resolved) is graded state_exact.
        resp = _fork(actae, ch_name, f"{ch_name}-exact", at_cursor=5)
        assert resp.status_code == 201, resp.text
        assert resp.json()["resolved_cursor"] == 5
        assert resp.json()["reproducibility"] == "state_exact"

        # The immutable receipt is queryable.
        receipt = actae.raw_get(f"/api/v1/channels/{ch_name}-child/receipt")
        assert receipt.status_code == 200, receipt.text
        r = receipt.json()
        assert r["requested_cursor"] == 3
        assert r["resolved_cursor"] == 2
        assert r["child_channel_id"] == f"{ch_name}-child"
        assert r["restorable"] is True

    def test_at_cursor_zero_receipt_reports_resolved_boundary(self, actae, ch_name):
        _record(actae, ch_name, 4)
        _save_state(actae, ch_name, 4, {"at": 4})
        resp = _fork(actae, ch_name, f"{ch_name}-child", at_cursor=0)
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["requested_at_cursor"] == 0
        assert body["resolved_cursor"] == 4, "at_cursor=0 resolves to the parent's cursor, not 0"
        assert body["restorable"] is True

    def test_lineage_only_fork_is_not_restorable(self, actae, ch_name):
        _record(actae, ch_name, 3)  # events but NO snapshots
        resp = _fork(actae, ch_name, f"{ch_name}-child", at_cursor=0)
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["restorable"] is False
        assert body["resolved_cursor"] == 0
        assert body["source_state_version"] == 0
        assert body["source_state_sha256"] is None
        assert body["reproducibility"] == "best_effort"

    def test_manifest_and_reproducibility_grading(self, actae, ch_name):
        _record(actae, ch_name, 3)
        _save_state(actae, ch_name, 3, {"s": 3})
        resp = actae.raw_post("/api/v1/channels/fork", {
            "source_channel_id": ch_name,
            "new_channel_id": f"{ch_name}-ctx",
            "at_cursor": 3,
            "manifest": {
                "model": "gpt-5",
                "environment": {"os": "macos"},
                "dependencies": {"actae": "1.0"},
                "seed": 42,
            },
        })
        assert resp.status_code == 201, resp.text
        assert resp.json()["reproducibility"] == "context_exact"
        assert resp.json()["manifest"]["model"] == "gpt-5"

    def test_idempotency_conflict_on_request_drift(self, actae, ch_name):
        _record(actae, ch_name, 3)
        _save_state(actae, ch_name, 3, {"s": 3})
        op = f"op-conflict-{int(time.time() * 1000)}"
        assert _fork(actae, ch_name, f"{ch_name}-c1", at_cursor=3, operation_id=op).status_code == 201
        # Same op + different child id → 409 idempotency_conflict (never a
        # silent replay or inconsistent-state error).
        resp = _fork(actae, ch_name, f"{ch_name}-c2", at_cursor=3, operation_id=op)
        assert resp.status_code == 409, resp.text
        assert resp.json()["status"] == "idempotency_conflict"
        # Same op + different boundary → 409 too.
        resp = _fork(actae, ch_name, f"{ch_name}-c1", at_cursor=2, operation_id=op)
        assert resp.status_code == 409, resp.text
        assert resp.json()["status"] == "idempotency_conflict"

    def test_channel_conflict_on_same_child_different_source(self, actae, ch_name):
        _record(actae, ch_name, 3)
        _save_state(actae, ch_name, 3, {"s": 3})
        other = f"{ch_name}-other"
        _record(actae, other, 3)
        _save_state(actae, other, 3, {"s": 3})
        assert _fork(actae, ch_name, f"{ch_name}-child", at_cursor=3).status_code == 201
        resp = _fork(actae, other, f"{ch_name}-child", at_cursor=3)
        assert resp.status_code == 409, resp.text
        assert resp.json()["status"] == "channel_conflict"

    def test_diff_reports_per_side_divergence(self, actae, ch_name):
        _record(actae, ch_name, 2)
        _save_state(actae, ch_name, 1, {"base": True})
        _save_state(actae, ch_name, 2, {"base": True, "n": 2})
        assert _fork(actae, ch_name, f"{ch_name}-a", at_cursor=1).status_code == 201
        assert _fork(actae, ch_name, f"{ch_name}-b", at_cursor=2).status_code == 201

        diff = actae.raw_get(f"/api/v1/channels/diff?left={ch_name}-a&right={ch_name}-b")
        assert diff.status_code == 200, diff.text
        d = diff.json()
        assert d["common"]["channel_id"] == ch_name
        assert d["left_diverged_at_cursor"] == 1
        assert d["right_diverged_at_cursor"] == 2
        assert d["truncated"] is False
        assert "entry_count_total" in d
        assert "diverged_at_cursor" not in d, "old single divergence field must be gone"

    def test_decision_trail_boundary_is_immutable(self, actae, ch_name):
        _record(actae, ch_name, 2)
        _save_state(actae, ch_name, 2, {"at_fork": True})
        assert _fork(actae, ch_name, f"{ch_name}-child", at_cursor=0).status_code == 201
        # Parent advances AFTER the fork.
        _record(actae, ch_name, 2)
        _save_state(actae, ch_name, 4, {"at_fork": False, "later": True})

        trail = actae.raw_get(f"/api/v1/channels/{ch_name}-child/trail")
        assert trail.status_code == 200, trail.text
        b = trail.json()["boundary"]
        assert b["resolved_cursor"] == 2, "boundary must be the fork-time cursor, not parent's later state"
        assert b["state"] == {"at_fork": True}

    def test_experiment_group_rank_promote_delete(self, actae, ch_name):
        _record(actae, ch_name, 2)
        _save_state(actae, ch_name, 2, {"base": True})
        for suffix in ("a", "b"):
            assert _fork(actae, ch_name, f"{ch_name}-{suffix}", at_cursor=2).status_code == 201

        actae.raw_patch(f"/api/v1/channels/{ch_name}-a/outcome", {"outcome": "promoted", "score": 0.9})
        actae.raw_patch(f"/api/v1/channels/{ch_name}-b/outcome", {"outcome": "rejected", "score": 0.2})

        group = actae.raw_post("/api/v1/experiments", {
            "name": "exp-http", "baseline_channel_id": ch_name,
        })
        assert group.status_code == 201, group.text
        gid = group.json()["group_id"]
        for suffix in ("a", "b"):
            r = actae.raw_post(f"/api/v1/experiments/{gid}/members", {
                "channel_id": f"{ch_name}-{suffix}",
                "role": "variant",
                "declared_delta": {"model": suffix},
            })
            assert r.status_code == 200, r.text

        rank = actae.raw_get(f"/api/v1/experiments/{gid}/rank")
        assert rank.status_code == 200, rank.text
        assert rank.json()["ranked"][0]["channel_id"] == f"{ch_name}-a"
        assert rank.json()["ranked"][0]["outcome"] == "promoted"

        # Promote the winner into the parent.
        promo = actae.raw_post(f"/api/v1/channels/{ch_name}-a/promote")
        assert promo.status_code == 200, promo.text
        assert promo.json()["parent_channel_id"] == ch_name
        assert promo.json()["parent_cursor"] >= 3
        # run.promoted recorded on the parent.
        parent_events = actae.replay(ch_name, cursor=2, limit=10)
        assert any(e["type"] == "run.promoted" for e in parent_events)

        # Non-recursive delete of a parent with children → 409.
        d = actae.raw_delete(f"/api/v1/channels/{ch_name}", params={"recursive": "false"})
        assert d.status_code == 409, d.text
        # Recursive delete succeeds.
        d = actae.raw_delete(f"/api/v1/channels/{ch_name}", params={"recursive": "true"})
        assert d.status_code == 200, d.text

    def test_compare_channels_combines_what_changed(self, actae, ch_name):
        _record(actae, ch_name, 2)
        _save_state(actae, ch_name, 2, {"base": True})
        for suffix in ("a", "b"):
            assert _fork(actae, ch_name, f"{ch_name}-{suffix}", at_cursor=2,
                         ).status_code == 201
            actae.raw_put(f"/api/v1/channels/{ch_name}-{suffix}/metadata", {
                "channel_id": f"{ch_name}-{suffix}",
                "experiment_metadata": {"result": {"output": suffix}},
            })
        cmp = actae.raw_get(f"/api/v1/channels/compare?left={ch_name}-a&right={ch_name}-b")
        assert cmp.status_code == 200, cmp.text
        c = cmp.json()
        assert c["state_diff"]["left_channel_id"] == f"{ch_name}-a"
        assert c["output_left"] == "a"
        assert c["output_right"] == "b"
        assert c["output_changed"] is True
