# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for side-effect-aware ordinary forks.

A forked channel may declare a `tool_policies` map that controls how tool
executions are handled on the child, mirroring the group-fork policy model:

    {"tool_name": "replay" | "block" | "live" | "auto", "*": <default>}

- ``replay`` : an exact source match is materialized; a miss or mismatch is a
  persisted blocked boundary (``409 fork_tool_blocked``), never an execution.
- ``block``  : the tool never executes; every attempt is a blocked boundary.
- ``auto`` (default): an exact inherited result is replayed; new or changed work
  runs live through the normal idempotent ledger.
- ``live``   : today's behavior, even for inherited work.

The tool is never executed by Actae; these tests assert replay/block/fallthrough
decisions and the resulting immutable events.
"""

import pytest


def _record(actae, ch, event_type="step", payload=None):
    resp = actae.raw_post(
        "/api/v1/events/record",
        {
            "channel_id": ch,
            "type": event_type,
            "payload": payload or {"seq": 0},
            "metadata": {"actor": "bot"},
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def _fork(actae, source, child, at_cursor=0, tool_policies=None, reason=None):
    body = {
        "source_channel_id": source,
        "new_channel_id": child,
        "at_cursor": at_cursor,
    }
    if tool_policies is not None:
        body["tool_policies"] = tool_policies
    if reason:
        body["reason"] = reason
    return actae.raw_post("/api/v1/channels/fork", body)


def _claim(actae, ch, key, tool, params=None, **extra):
    body = {"channel_id": ch, "key_name": key, "tool_name": tool}
    if params is not None:
        body["params"] = params
    body.update(extra)
    return actae.raw_post("/api/v1/executions/claim", body)


def _complete(actae, exec_id, claim_token, result):
    return actae.raw_post(
        f"/api/v1/executions/{exec_id}/complete",
        {"claim_token": claim_token, "result": result},
    )


def _complete_on(actae, ch, key, tool, params, result):
    claim = _claim(actae, ch, key, tool, params).json()
    assert claim["status"] == "claimed", claim
    resp = _complete(actae, claim["execution"]["id"], claim["claim_token"], result)
    assert resp.status_code == 200, resp.text
    return claim


def _events(actae, ch, event_type=None):
    return actae.replay(ch, cursor=0, limit=200, event_type=event_type)


class TestAutoDefault:
    def test_inherited_exact_result_replays(self, actae, ch_name):
        _record(actae, ch_name)
        _complete_on(actae, ch_name, "k-1", "charge", {"amount": 5}, {"paid": True})

        child = f"{ch_name}-auto"
        resp = _fork(actae, ch_name, child)
        assert resp.status_code == 201, resp.text

        claim = _claim(actae, child, "k-1", "charge", {"amount": 5})
        assert claim.status_code == 201, claim.text
        body = claim.json()
        assert body["status"] == "replayed"
        assert body["result"] == {"paid": True}
        assert body["claim_token"] is None
        # The tool never started on the child; a replay marker was written.
        assert len(_events(actae, child, "tool.started")) == 0
        assert len(_events(actae, child, "tool.replayed")) == 1

    def test_new_key_runs_live(self, actae, ch_name):
        _record(actae, ch_name)
        child = f"{ch_name}-auto"
        assert _fork(actae, ch_name, child).status_code == 201

        claim = _claim(actae, child, "k-new", "charge", {"amount": 9})
        assert claim.status_code == 201, claim.text
        assert claim.json()["status"] == "claimed"
        assert len(_events(actae, child, "tool.started")) == 1

    def test_changed_params_run_live_not_blocked(self, actae, ch_name):
        _record(actae, ch_name)
        _complete_on(actae, ch_name, "k-1", "charge", {"amount": 5}, {"paid": True})
        child = f"{ch_name}-auto"
        assert _fork(actae, ch_name, child).status_code == 201

        claim = _claim(actae, child, "k-1", "charge", {"amount": 50})
        assert claim.status_code == 201, claim.text
        assert claim.json()["status"] == "claimed"
        # A mismatch under `auto` is new work, not a blocked counterfactual.
        assert len(_events(actae, child, "tool.blocked")) == 0


class TestReplayPolicy:
    def test_exact_match_replays(self, actae, ch_name):
        _record(actae, ch_name)
        _complete_on(actae, ch_name, "k-1", "charge", {"amount": 5}, {"paid": True})
        child = f"{ch_name}-rep"
        assert _fork(actae, ch_name, child, tool_policies={"charge": "replay"}).status_code == 201

        claim = _claim(actae, child, "k-1", "charge", {"amount": 5})
        assert claim.status_code == 201, claim.text
        assert claim.json()["status"] == "replayed"
        assert claim.json()["result"] == {"paid": True}

    def test_miss_blocks_and_persists_boundary(self, actae, ch_name):
        _record(actae, ch_name)
        child = f"{ch_name}-rep"
        assert _fork(actae, ch_name, child, tool_policies={"charge": "replay"}).status_code == 201

        claim = _claim(actae, child, "k-unknown", "charge", {"amount": 1})
        assert claim.status_code == 409, claim.text
        assert claim.json()["status"] == "fork_tool_blocked"
        assert len(_events(actae, child, "tool.started")) == 0
        blocked = _events(actae, child, "tool.blocked")
        assert len(blocked) == 1
        assert blocked[0]["payload"]["boundary"] == "tool:charge:replay_miss"

    def test_mismatch_blocks(self, actae, ch_name):
        _record(actae, ch_name)
        _complete_on(actae, ch_name, "k-1", "charge", {"amount": 5}, {"paid": True})
        child = f"{ch_name}-rep"
        assert _fork(actae, ch_name, child, tool_policies={"charge": "replay"}).status_code == 201

        claim = _claim(actae, child, "k-1", "charge", {"amount": 50})
        assert claim.status_code == 409, claim.text
        assert claim.json()["status"] == "fork_tool_blocked"
        blocked = _events(actae, child, "tool.blocked")
        assert len(blocked) == 1
        assert blocked[0]["payload"]["boundary"] == "tool:charge:request_mismatch"

    def test_wildcard_replay_policy(self, actae, ch_name):
        _record(actae, ch_name)
        child = f"{ch_name}-rep"
        assert _fork(actae, ch_name, child, tool_policies={"*": "replay"}).status_code == 201

        claim = _claim(actae, child, "k-unknown", "any_tool")
        assert claim.status_code == 409, claim.text
        assert claim.json()["status"] == "fork_tool_blocked"


class TestBlockPolicy:
    def test_block_never_executes_even_with_inherited_result(self, actae, ch_name):
        _record(actae, ch_name)
        _complete_on(actae, ch_name, "k-1", "email.send", {"to": "a@b.c"}, {"sent": True})
        child = f"{ch_name}-blk"
        assert _fork(actae, ch_name, child, tool_policies={"email.send": "block"}).status_code == 201

        claim = _claim(actae, child, "k-1", "email.send", {"to": "a@b.c"})
        assert claim.status_code == 409, claim.text
        assert claim.json()["status"] == "fork_tool_blocked"
        assert len(_events(actae, child, "tool.started")) == 0
        assert len(_events(actae, child, "tool.replayed")) == 0
        blocked = _events(actae, child, "tool.blocked")
        assert len(blocked) == 1
        assert blocked[0]["payload"]["policy"] == "block"


class TestLivePolicy:
    def test_live_ignores_inherited_result(self, actae, ch_name):
        _record(actae, ch_name)
        _complete_on(actae, ch_name, "k-1", "charge", {"amount": 5}, {"paid": True})
        child = f"{ch_name}-live"
        assert _fork(actae, ch_name, child, tool_policies={"charge": "live"}).status_code == 201

        claim = _claim(actae, child, "k-1", "charge", {"amount": 5})
        assert claim.status_code == 201, claim.text
        assert claim.json()["status"] == "claimed"
        assert len(_events(actae, child, "tool.started")) == 1


class TestConcurrency:
    def test_concurrent_auto_replays_never_500(self, actae, ch_name):
        # A fork with the default `auto` policy, claimed concurrently by many
        # workers, must resolve gracefully (all `replayed`) rather than racing
        # the tool_executions unique constraint into a 500.
        import concurrent.futures

        _record(actae, ch_name)
        _complete_on(actae, ch_name, "k-1", "charge", {"amount": 5}, {"paid": True})
        child = f"{ch_name}-race"
        assert _fork(actae, ch_name, child).status_code == 201

        def do_claim(_):
            return _claim(actae, child, "k-1", "charge", {"amount": 5})

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            responses = list(pool.map(do_claim, range(6)))

        codes = {r.status_code for r in responses}
        assert codes <= {201}, [(r.status_code, r.text) for r in responses]
        statuses = [r.json()["status"] for r in responses]
        assert statuses.count("replayed") == 6, statuses
        # Exactly one replay marker, no tool.started on the child.
        assert len(_events(actae, child, "tool.replayed")) == 1
        assert len(_events(actae, child, "tool.started")) == 0


class TestLineageAndScope:
    def test_fork_of_fork_replays_nearest_ancestor(self, actae, ch_name):
        _record(actae, ch_name)
        _complete_on(actae, ch_name, "k-1", "charge", {"amount": 5}, {"paid": True})
        child = f"{ch_name}-c1"
        grand = f"{ch_name}-c2"
        assert _fork(actae, ch_name, child).status_code == 201
        assert _fork(actae, child, grand, tool_policies={"charge": "replay"}).status_code == 201

        # The result lives on the root; the grandchild resolves it through lineage.
        claim = _claim(actae, grand, "k-1", "charge", {"amount": 5})
        assert claim.status_code == 201, claim.text
        assert claim.json()["status"] == "replayed"
        assert claim.json()["result"] == {"paid": True}

    def test_non_forked_channel_unaffected(self, actae, ch_name):
        # A plain channel with no parent keeps exact current behavior: a
        # completed execution replays only when the caller opts into the marker.
        _complete_on(actae, ch_name, "k-1", "charge", {"amount": 5}, {"paid": True})
        claim = _claim(actae, ch_name, "k-1", "charge", {"amount": 5})
        assert claim.status_code == 201, claim.text
        assert claim.json()["status"] == "replayed"
        # No fork replay marker is emitted for a non-forked channel.
        assert len(_events(actae, ch_name, "tool.replayed")) == 0

    def test_receipt_exposes_tool_policies(self, actae, ch_name):
        _record(actae, ch_name)
        child = f"{ch_name}-meta"
        policies = {"charge": "replay", "*": "auto"}
        assert _fork(actae, ch_name, child, tool_policies=policies).status_code == 201

        receipt = actae.raw_get(f"/api/v1/channels/{child}/receipt")
        assert receipt.status_code == 200, receipt.text
        assert receipt.json()["tool_policies"] == policies

        meta = actae.raw_get(f"/api/v1/channels/{child}/metadata")
        assert meta.status_code == 200, meta.text
        assert meta.json()["tool_policies"] == policies
