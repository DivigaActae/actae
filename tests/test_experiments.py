# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""First-class experiment groups: baseline + variant forks, ranking.

Covers the experiment-group guarantees (docs/EXPERIMENTS.md):
- create with/without a baseline channel; name validation
- list/get; missing/malformed group ids → 404
- add variant forks with declared_delta; role defaulting; upsert update
- outcome/score recording (PATCH /channels/{id}/outcome) drives ranking:
  score desc, nulls last, deterministic tie-break (created_at, then
  channel_id)
- promote the winner into the parent; non-recursive delete of a parent with
  children is refused (409); recursive delete succeeds
"""

import uuid

import pytest


def _gid(name):
    return f"{name}-{uuid.uuid4().hex[:8]}"


def _record(actae, ch, n=2):
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
        "channel_id": ch, "cursor": cursor, "state": state,
    })
    assert resp.status_code == 200, resp.text
    return resp.json()["version"]


def _fork(actae, source, child, at_cursor=0):
    resp = actae.raw_post("/api/v1/channels/fork", {
        "source_channel_id": source,
        "new_channel_id": child,
        "at_cursor": at_cursor,
    })
    assert resp.status_code == 201, resp.text


def _create_experiment(actae, name, **extra):
    body = {"name": name}
    body.update(extra)
    resp = actae.raw_post("/api/v1/experiments", body)
    assert resp.status_code == 201, resp.text
    return resp.json()


def _add_member(actae, gid, channel_id, role="variant", delta=None):
    body = {"channel_id": channel_id, "role": role}
    if delta is not None:
        body["declared_delta"] = delta
    resp = actae.raw_post(f"/api/v1/experiments/{gid}/members", body)
    assert resp.status_code == 200, resp.text
    return resp.json()


def _set_outcome(actae, ch, outcome, score=None):
    body = {"outcome": outcome}
    if score is not None:
        body["score"] = score
    resp = actae.raw_patch(f"/api/v1/channels/{ch}/outcome", body)
    assert resp.status_code == 200, resp.text
    return resp.json()


def _rank(actae, gid):
    resp = actae.raw_get(f"/api/v1/experiments/{gid}/rank")
    assert resp.status_code == 200, resp.text
    return resp.json()


class TestExperimentLifecycle:
    def test_create_returns_group_shape(self, actae, ch_name):
        _record(actae, ch_name)
        group = _create_experiment(actae, "gpt5-vs-claude",
                                   description="evals", baseline_channel_id=ch_name)
        assert group["name"] == "gpt5-vs-claude"
        assert group["description"] == "evals"
        assert group["baseline_channel_id"] == ch_name
        assert group["status"] == "running"
        assert group["group_id"]
        # Baseline member is auto-added.
        assert [m["channel_id"] for m in group["members"]] == [ch_name]
        assert group["members"][0]["role"] == "baseline"

    def test_create_without_baseline(self, actae, ch_name):
        group = _create_experiment(actae, "no-baseline")
        assert group.get("baseline_channel_id") is None
        assert group.get("members") in (None, [])

    def test_create_empty_name_400(self, actae):
        resp = actae.raw_post("/api/v1/experiments", {"name": "   "})
        assert resp.status_code == 400
        assert resp.json()["status"] == "invalid_request"

    def test_list_experiments(self, actae, ch_name):
        a = _create_experiment(actae, "list-a")
        b = _create_experiment(actae, "list-b")
        listing = actae.raw_get("/api/v1/experiments").json()
        names = [g["name"] for g in listing]
        assert "list-a" in names and "list-b" in names

    def test_get_experiment_200_and_404(self, actae, ch_name):
        group = _create_experiment(actae, "get-me")
        gid = group["group_id"]
        got = actae.raw_get(f"/api/v1/experiments/{gid}")
        assert got.status_code == 200
        assert got.json()["name"] == "get-me"

        missing = actae.raw_get(f"/api/v1/experiments/{uuid.uuid4()}")
        assert missing.status_code == 404
        assert missing.json()["error"] == "Experiment not found"

        malformed = actae.raw_get("/api/v1/experiments/not-a-uuid")
        assert malformed.status_code == 404

    def test_rank_missing_group_404(self, actae):
        resp = actae.raw_get(f"/api/v1/experiments/{uuid.uuid4()}/rank")
        assert resp.status_code == 404


class TestExperimentMembers:
    def test_add_variant_with_declared_delta(self, actae, ch_name):
        _record(actae, ch_name)
        _save_state(actae, ch_name, 2, {"base": True})
        _fork(actae, ch_name, f"{ch_name}-v1", at_cursor=2)

        group = _create_experiment(actae, "members", baseline_channel_id=ch_name)
        gid = group["group_id"]
        updated = _add_member(actae, gid, f"{ch_name}-v1",
                              delta={"model": "v1"})
        member = next(m for m in updated["members"]
                      if m["channel_id"] == f"{ch_name}-v1")
        assert member["role"] == "variant"
        assert member["declared_delta"] == {"model": "v1"}

    def test_role_defaults_to_variant(self, actae, ch_name):
        _record(actae, ch_name)
        _save_state(actae, ch_name, 2, {"base": True})
        _fork(actae, ch_name, f"{ch_name}-v2", at_cursor=2)
        group = _create_experiment(actae, "default-role")
        gid = group["group_id"]
        # No `role` field → server defaults to variant.
        resp = actae.raw_post(f"/api/v1/experiments/{gid}/members",
                              {"channel_id": f"{ch_name}-v2"})
        assert resp.status_code == 200
        member = next(m for m in resp.json()["members"]
                      if m["channel_id"] == f"{ch_name}-v2")
        assert member["role"] == "variant"

    def test_add_member_upsert_updates(self, actae, ch_name):
        _record(actae, ch_name)
        _save_state(actae, ch_name, 2, {"base": True})
        _fork(actae, ch_name, f"{ch_name}-v3", at_cursor=2)
        group = _create_experiment(actae, "upsert")
        gid = group["group_id"]
        _add_member(actae, gid, f"{ch_name}-v3", delta={"model": "v3"})
        updated = _add_member(actae, gid, f"{ch_name}-v3",
                              role="variant", delta={"model": "v3b"})
        members = [m for m in updated["members"] if m["channel_id"] == f"{ch_name}-v3"]
        assert len(members) == 1, "re-adding a member must not duplicate it"
        assert members[0]["declared_delta"] == {"model": "v3b"}

    def test_add_member_empty_channel_400(self, actae):
        group = _create_experiment(actae, "empty-ch")
        resp = actae.raw_post(f"/api/v1/experiments/{group['group_id']}/members",
                              {"channel_id": "  "})
        assert resp.status_code == 400


class TestExperimentRanking:
    def test_rank_orders_by_score_then_creation(self, actae, ch_name):
        _record(actae, ch_name, 2)
        _save_state(actae, ch_name, 2, {"base": True})
        for suffix in ("a", "b", "c"):
            _fork(actae, ch_name, f"{ch_name}-{suffix}", at_cursor=2)

        group = _create_experiment(actae, "ranking")  # no baseline member
        gid = group["group_id"]
        for suffix in ("a", "b", "c"):
            _add_member(actae, gid, f"{ch_name}-{suffix}",
                        delta={"variant": suffix})

        # No scores yet → nulls last; deterministic tie-break by insertion
        # order (created_at), then channel_id.
        ranked = _rank(actae, gid)["ranked"]
        assert [m["channel_id"] for m in ranked] == \
            [f"{ch_name}-a", f"{ch_name}-b", f"{ch_name}-c"]
        assert all(m["result_score"] is None for m in ranked)
        assert all(m["outcome"] is None for m in ranked)

        # Score c highest, a lowest → reorder.
        _set_outcome(actae, f"{ch_name}-c", "promoted", score=0.95)
        _set_outcome(actae, f"{ch_name}-a", "rejected", score=0.1)
        ranked = _rank(actae, gid)["ranked"]
        assert [m["channel_id"] for m in ranked] == \
            [f"{ch_name}-c", f"{ch_name}-a", f"{ch_name}-b"]
        assert ranked[0]["outcome"] == "promoted"
        assert ranked[0]["result_score"] == pytest.approx(0.95)
        assert ranked[0]["declared_delta"] == {"variant": "c"}
        assert ranked[1]["channel_id"] == f"{ch_name}-a"
        assert ranked[1]["outcome"] == "rejected"
        assert ranked[-1]["outcome"] is None

    def test_rank_score_ties_break_by_creation_order(self, actae, ch_name):
        _record(actae, ch_name, 2)
        _save_state(actae, ch_name, 2, {"base": True})
        for suffix in ("x", "y"):
            _fork(actae, ch_name, f"{ch_name}-{suffix}", at_cursor=2)
        group = _create_experiment(actae, "ties")  # no baseline member
        gid = group["group_id"]
        for suffix in ("x", "y"):
            _add_member(actae, gid, f"{ch_name}-{suffix}")
        _set_outcome(actae, f"{ch_name}-x", "promoted", score=0.5)
        _set_outcome(actae, f"{ch_name}-y", "promoted", score=0.5)
        ranked = _rank(actae, gid)["ranked"]
        assert [m["channel_id"] for m in ranked] == [f"{ch_name}-x", f"{ch_name}-y"]

    def test_rank_reports_count_and_group(self, actae, ch_name):
        _record(actae, ch_name, 2)
        _save_state(actae, ch_name, 2, {"base": True})
        _fork(actae, ch_name, f"{ch_name}-only", at_cursor=2)
        group = _create_experiment(actae, "count", baseline_channel_id=ch_name)
        gid = group["group_id"]
        _add_member(actae, gid, f"{ch_name}-only")
        data = _rank(actae, gid)
        assert data["count"] == 2  # baseline + variant
        assert data["group"]["group_id"] == gid


class TestExperimentPromote:
    def test_promote_winner_into_parent(self, actae, ch_name):
        _record(actae, ch_name, 2)
        _save_state(actae, ch_name, 2, {"base": True, "value": 1})
        _fork(actae, ch_name, f"{ch_name}-win", at_cursor=2)
        # The winning fork advances with a better result.
        _record(actae, f"{ch_name}-win", 1)
        actae.raw_post(f"/api/v1/state/{ch_name}-win", {
            "channel_id": f"{ch_name}-win", "cursor": 2, "state": {"base": True, "value": 99},
        })

        group = _create_experiment(actae, "promote", baseline_channel_id=ch_name)
        gid = group["group_id"]
        _add_member(actae, gid, f"{ch_name}-win", delta={"value": 99})
        _set_outcome(actae, f"{ch_name}-win", "promoted", score=0.9)

        ranked = _rank(actae, gid)["ranked"]
        assert ranked[0]["channel_id"] == f"{ch_name}-win"

        promo = actae.raw_post(f"/api/v1/channels/{ch_name}-win/promote")
        assert promo.status_code == 200, promo.text
        assert promo.json()["parent_channel_id"] == ch_name
        parent_events = actae.replay(ch_name, cursor=2, limit=10)
        assert any(e["type"] == "run.promoted" for e in parent_events)

    def test_promote_root_channel_400(self, actae, ch_name):
        _record(actae, ch_name)
        # A channel_metadata row WITHOUT a parent (e.g. created by PUT
        # metadata) is a root: promotion is refused, not a 404.
        meta = actae.raw_put(f"/api/v1/channels/{ch_name}/metadata", {
            "channel_id": ch_name,
            "display_name": "root",
        })
        assert meta.status_code == 200, meta.text
        resp = actae.raw_post(f"/api/v1/channels/{ch_name}/promote")
        assert resp.status_code == 400

    def test_promote_unknown_channel_404(self, actae):
        resp = actae.raw_post(f"/api/v1/channels/{_gid('ch-missing')}/promote")
        assert resp.status_code == 404

    def test_delete_parent_with_children_409_then_recursive(self, actae, ch_name):
        _record(actae, ch_name, 2)
        _save_state(actae, ch_name, 2, {"base": True})
        _fork(actae, ch_name, f"{ch_name}-kid", at_cursor=2)

        refused = actae.raw_delete(f"/api/v1/channels/{ch_name}",
                                   params={"recursive": "false"})
        assert refused.status_code == 409
        assert refused.json()["status"] == "channel_has_children"

        ok = actae.raw_delete(f"/api/v1/channels/{ch_name}",
                              params={"recursive": "true"})
        assert ok.status_code == 200