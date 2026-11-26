# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Server-owned step index (resolve_step) and execution trees.

Covers (docs/FORK_RUNTIME.md):
- record with `step_number` persists a durable step → (cursor, event_id)
  mapping in the same transaction; `GET /channels/{id}/steps/{n}` resolves it
- steps that were never recorded (and step 0) → 404
- fork_at_step lineage: a fork inherits its first N steps from the ancestor
  that recorded them (resolved through `experiment_metadata.forked_at_step`);
  steps past the boundary resolve on the fork itself
- execution tree: root channel + children with event counts/cursors,
  `truncated` flag and `max_depth` cap
"""

import uuid

import pytest


def _gid(name):
    return f"{name}-{uuid.uuid4().hex[:8]}"


def _step(actae, ch, n, seq):
    resp = actae.raw_post("/api/v1/events/record", {
        "channel_id": ch,
        "type": "step",
        "payload": {"seq": seq},
        "metadata": {"actor": "bot"},
        "step_number": n,
    })
    assert resp.status_code == 201, resp.text
    return resp.json()


def _resolve_step(actae, ch, n):
    return actae.raw_get(f"/api/v1/channels/{ch}/steps/{n}")


def _save_state(actae, ch, cursor, state):
    resp = actae.raw_post(f"/api/v1/state/{ch}", {
        "channel_id": ch, "cursor": cursor, "state": state,
    })
    assert resp.status_code == 200, resp.text


def _fork_at(actae, source, child, at_cursor, forked_at_step=None):
    body = {
        "source_channel_id": source,
        "new_channel_id": child,
        "at_cursor": at_cursor,
    }
    if forked_at_step is not None:
        body["experiment_metadata"] = {"forked_at_step": forked_at_step}
    resp = actae.raw_post("/api/v1/channels/fork", body)
    assert resp.status_code == 201, resp.text


def _tree(actae, ch):
    resp = actae.raw_get(f"/api/v1/channels/{ch}/fork-tree")
    assert resp.status_code == 200, resp.text
    return resp.json()


class TestResolveStep:
    def test_record_with_step_number_resolves_exactly(self, actae, ch_name):
        ev1 = _step(actae, ch_name, 1, seq=10)
        _step(actae, ch_name, 2, seq=20)
        ev3 = _step(actae, ch_name, 3, seq=30)

        r1 = _resolve_step(actae, ch_name, 1)
        assert r1.status_code == 200, r1.text
        assert r1.json()["channel_id"] == ch_name
        assert r1.json()["step_number"] == 1
        assert r1.json()["cursor"] == ev1["event"]["cursor"]
        assert r1.json()["event_id"] == ev1["event"]["id"]

        r3 = _resolve_step(actae, ch_name, 3).json()
        assert r3["cursor"] == ev3["event"]["cursor"]
        assert r3["event_id"] == ev3["event"]["id"]

    def test_unrecorded_step_404(self, actae, ch_name):
        _step(actae, ch_name, 1, seq=0)
        resp = _resolve_step(actae, ch_name, 99)
        assert resp.status_code == 404
        assert resp.json()["error"] == "Step not found"

    def test_step_zero_404(self, actae, ch_name):
        _step(actae, ch_name, 1, seq=0)
        resp = _resolve_step(actae, ch_name, 0)
        assert resp.status_code == 404

    def test_step_index_is_idempotent_on_retry(self, actae, ch_name):
        """A retried step with the same step_number never rewrites the index
        row (ON CONFLICT DO NOTHING) — the first recorded cursor wins."""
        first = _step(actae, ch_name, 1, seq=100)
        second = _step(actae, ch_name, 1, seq=200)
        assert first["event"]["id"] != second["event"]["id"]
        resolved = _resolve_step(actae, ch_name, 1).json()
        assert resolved["cursor"] == first["event"]["cursor"]
        assert resolved["event_id"] == first["event"]["id"]

    def test_fork_inherits_steps_through_lineage(self, actae, ch_name):
        """A fork records its own steps 6+; steps 1..5 resolve through the
        lineage to the ancestor that recorded them."""
        cursors = {}
        for n in range(1, 6):
            ev = _step(actae, ch_name, n, seq=n * 10)
            cursors[n] = (ch_name, ev["event"]["cursor"], ev["event"]["id"])
        _save_state(actae, ch_name, 5, {"at_fork": True})

        child = f"{ch_name}-child"
        _fork_at(actae, ch_name, child, at_cursor=5, forked_at_step=5)
        child_ev = _step(actae, child, 6, seq=60)

        # Steps 1..5 belong to the parent channel.
        for n in range(1, 6):
            r = _resolve_step(actae, child, n)
            assert r.status_code == 200, (n, r.text)
            owner, cursor, _ = cursors[n]
            assert r.json()["channel_id"] == owner, f"step {n} must resolve to ancestor"
            assert r.json()["cursor"] == cursor, f"step {n} cursor mismatch"

        # Step 6 belongs to the fork.
        r6 = _resolve_step(actae, child, 6)
        assert r6.status_code == 200
        assert r6.json()["channel_id"] == child
        assert r6.json()["event_id"] == child_ev["event"]["id"]

        # Step 7 was never recorded anywhere → 404.
        assert _resolve_step(actae, child, 7).status_code == 404

    def test_fork_without_forked_at_step_does_not_forward(self, actae, ch_name):
        """Without `forked_at_step` in the fork metadata the server does NOT
        guess a boundary — the child's own index is authoritative."""
        _step(actae, ch_name, 1, seq=1)
        _save_state(actae, ch_name, 1, {"x": 1})
        child = f"{ch_name}-kid"
        _fork_at(actae, ch_name, child, at_cursor=1)  # no forked_at_step
        resp = _resolve_step(actae, child, 1)
        assert resp.status_code == 404

    def test_fork_of_fork_inherits_deepest_ancestor(self, actae, ch_name):
        """Fork-of-fork at an inherited step resolves through the lineage to
        the ancestor that recorded it."""
        for n in range(1, 4):
            _step(actae, ch_name, n, seq=n)
        _save_state(actae, ch_name, 3, {"a": 1})
        child = f"{ch_name}-c1"
        _fork_at(actae, ch_name, child, at_cursor=3, forked_at_step=3)
        _step(actae, child, 4, seq=4)
        _save_state(actae, child, 4, {"b": 1})

        grandchild = f"{ch_name}-c2"
        _fork_at(actae, child, grandchild, at_cursor=4, forked_at_step=4)
        # Steps 1..3 are owned by the root; step 4 by the child.
        for n in (1, 2, 3):
            r = _resolve_step(actae, grandchild, n).json()
            assert r["channel_id"] == ch_name
        r4 = _resolve_step(actae, grandchild, 4).json()
        assert r4["channel_id"] == child
        assert _resolve_step(actae, grandchild, 5).status_code == 404


class TestExecutionTree:
    def test_tree_reports_root_and_children(self, actae, ch_name):
        _step(actae, ch_name, 1, seq=0)
        _step(actae, ch_name, 2, seq=1)
        _save_state(actae, ch_name, 2, {"base": True})
        for suffix in ("a", "b"):
            _fork_at(actae, ch_name, f"{ch_name}-{suffix}", at_cursor=2)
            _step(actae, f"{ch_name}-{suffix}", 3, seq=0)

        tree = _tree(actae, ch_name)
        assert tree["channel_id"] == ch_name
        assert tree["truncated"] is False
        assert tree["max_depth"] == 10
        assert tree["latest_cursor"] >= 2
        assert tree["event_count"] >= 2
        children = {c["channel_id"]: c for c in tree["children"]}
        assert set(children) == {f"{ch_name}-a", f"{ch_name}-b"}
        for suffix in ("a", "b"):
            assert children[f"{ch_name}-{suffix}"]["event_count"] >= 2  # fork.started + step
            assert children[f"{ch_name}-{suffix}"]["latest_cursor"] >= 2
            assert children[f"{ch_name}-{suffix}"]["children"] == []

    def test_tree_leaf_channel_has_no_children(self, actae, ch_name):
        _step(actae, ch_name, 1, seq=0)
        _save_state(actae, ch_name, 1, {"x": 1})
        _fork_at(actae, ch_name, f"{ch_name}-only", at_cursor=1)
        tree = _tree(actae, f"{ch_name}-only")
        assert tree["channel_id"] == f"{ch_name}-only"
        assert tree["children"] == []

    def test_tree_nested_depth(self, actae, ch_name):
        _step(actae, ch_name, 1, seq=0)
        _save_state(actae, ch_name, 1, {"x": 1})
        _fork_at(actae, ch_name, f"{ch_name}-mid", at_cursor=1)
        _fork_at(actae, f"{ch_name}-mid", f"{ch_name}-leaf", at_cursor=1)
        tree = _tree(actae, ch_name)
        mid = tree["children"][0]
        assert mid["channel_id"] == f"{ch_name}-mid"
        assert mid["children"][0]["channel_id"] == f"{ch_name}-leaf"