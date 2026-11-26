# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Durable distributed execution groups: leases, messages, causal graph, forks.

Covers the flagship guarantees (docs/EXECUTION_GROUPS.md):
- create/list groups (upsert semantics); add/list members on independent
  channels; member metadata/role round-trip
- claim → heartbeat → release lease lifecycle; exclusive ownership; a
  takeover after expiry bumps the generation; stale heartbeats/acks are
  fenced (409 member_fenced)
- frozen-fork members cannot claim (409 counterfactual_blocked)
- send/ack atomically write message.sent + message.received events across
  the two member channels; the durable inbox replays offline delivery;
  operation_id makes retries idempotent (409 idempotency_conflict on reuse)
- causal-graph traverses the cross-channel message edge
- group forks: cloned member channels, replayed inbound history with
  replayed:true envelopes, frozen boundaries, promote, idempotent creation
- live WS delivery of message.received to the recipient's channel
- parallel sends with distinct operation ids are all delivered exactly once
"""

import threading
import time
import uuid
from datetime import datetime, timezone

import pytest


def _parse_ts(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _gid(name):
    return f"{name}-{uuid.uuid4().hex[:8]}"


def _create_group(actae, group_id, metadata=None):
    body = {"group_id": group_id}
    if metadata is not None:
        body["metadata"] = metadata
    resp = actae.raw_post("/api/v1/execution-groups", body)
    assert resp.status_code == 201, resp.text
    return resp.json()


def _groups(actae):
    resp = actae.raw_get("/api/v1/execution-groups")
    assert resp.status_code == 200, resp.text
    return resp.json()["groups"]


def _add_member(actae, group_id, member_id, channel_id, role=None, metadata=None):
    body = {"member_id": member_id, "channel_id": channel_id}
    if role is not None:
        body["role"] = role
    if metadata is not None:
        body["metadata"] = metadata
    resp = actae.raw_post(f"/api/v1/execution-groups/{group_id}/members", body)
    assert resp.status_code == 201, resp.text
    return resp.json()


def _members(actae, group_id):
    resp = actae.raw_get(f"/api/v1/execution-groups/{group_id}/members")
    assert resp.status_code == 200, resp.text
    return resp.json()["members"]


def _claim(actae, group_id, member_id, owner_id, lease_seconds=60):
    return actae.raw_post(
        f"/api/v1/execution-groups/{group_id}/members/{member_id}/claim",
        {"owner_id": owner_id, "lease_seconds": lease_seconds},
    )


def _heartbeat(actae, group_id, member_id, owner_id, generation, lease_seconds=60):
    return actae.raw_post(
        f"/api/v1/execution-groups/{group_id}/members/{member_id}/heartbeat",
        {"owner_id": owner_id, "generation": generation, "lease_seconds": lease_seconds},
    )


def _release(actae, group_id, member_id, owner_id, generation):
    return actae.raw_post(
        f"/api/v1/execution-groups/{group_id}/members/{member_id}/release",
        {"owner_id": owner_id, "generation": generation},
    )


def _send(actae, group_id, from_member, to_member, message_type, payload, **extra):
    body = {
        "from_member_id": from_member,
        "to_member_id": to_member,
        "type": message_type,
        "payload": payload,
    }
    body.update(extra)
    return actae.raw_post(f"/api/v1/execution-groups/{group_id}/messages", body)


def _messages(actae, group_id, member_id, after=None, limit=100):
    params = {"limit": str(limit)}
    if after is not None:
        params["after"] = after
    resp = actae.raw_get(
        f"/api/v1/execution-groups/{group_id}/members/{member_id}/messages",
        params=params,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["messages"]


def _ack(actae, message_id, owner_id, generation):
    return actae.raw_post(
        f"/api/v1/execution-group-messages/{message_id}/ack",
        {"owner_id": owner_id, "generation": generation},
    )


def _causal_graph(actae, event_id):
    resp = actae.raw_get(f"/api/v1/events/{event_id}/causal-graph")
    assert resp.status_code == 200, resp.text
    return resp.json()


def _create_fork(actae, body):
    return actae.raw_post("/api/v1/execution-group-forks", body)


def _get_fork(actae, fork_group_id):
    resp = actae.raw_get(f"/api/v1/execution-group-forks/{fork_group_id}")
    assert resp.status_code == 200, resp.text
    return resp.json()["fork"]


def _promote(actae, fork_group_id, member_id):
    return actae.raw_post(
        f"/api/v1/execution-group-forks/{fork_group_id}/members/{member_id}/promote"
    )


def _two_member_group(actae, ch_name):
    """Create a group with a router + research member on distinct channels."""
    gid = _gid("eg")
    router_ch = f"{ch_name}-router"
    research_ch = f"{ch_name}-research"
    _create_group(actae, gid)
    _add_member(actae, gid, "router", router_ch, role="router")
    _add_member(actae, gid, "research", research_ch, role="worker",
                metadata={"lang": "py"})
    return gid, router_ch, research_ch


class TestGroupLifecycle:
    def test_create_list_get(self, actae, ch_name):
        gid = _gid("eg-lc")
        _create_group(actae, gid, metadata={"team": "support"})

        groups = _groups(actae)
        mine = next(g for g in groups if g["group_id"] == gid)
        assert mine["status"] == "active"
        assert mine["metadata"] == {"team": "support"}
        assert mine["created_at"] and mine["updated_at"]

    def test_create_is_upsert_idempotent(self, actae, ch_name):
        gid = _gid("eg-upsert")
        first = _create_group(actae, gid)
        second = _create_group(actae, gid)
        assert first["group"]["group_id"] == second["group"]["group_id"]
        # Still exactly one group in the listing.
        assert sum(1 for g in _groups(actae) if g["group_id"] == gid) == 1

    def test_create_invalid_group_id_400(self, actae):
        resp = actae.raw_post("/api/v1/execution-groups", {"group_id": "x" * 300})
        assert resp.status_code == 400
        assert resp.json()["status"] == "invalid_request"

    def test_add_list_members(self, actae, ch_name):
        gid, router_ch, research_ch = _two_member_group(actae, ch_name)

        members = {m["member_id"]: m for m in _members(actae, gid)}
        assert set(members) == {"router", "research"}
        assert members["router"]["channel_id"] == router_ch
        assert members["router"]["role"] == "router"
        assert members["research"]["role"] == "worker"
        assert members["research"]["metadata"] == {"lang": "py"}
        # Unclaimed members have no owner and generation 0.
        assert members["router"]["owner_id"] is None
        assert members["router"]["generation"] == 0
        assert members["router"]["lease_until"] is None

    def test_add_member_duplicate_updates(self, actae, ch_name):
        gid, _, _ = _two_member_group(actae, ch_name)
        _add_member(actae, gid, "router", f"{ch_name}-router2", role="changed")
        members = {m["member_id"]: m for m in _members(actae, gid)}
        assert members["router"]["channel_id"] == f"{ch_name}-router2"
        assert members["router"]["role"] == "changed"
        assert len(members) == 2

    def test_list_members_unknown_group_404(self, actae):
        resp = actae.raw_get(f"/api/v1/execution-groups/{_gid('eg-missing')}/members")
        assert resp.status_code == 404
        assert resp.json()["status"] == "execution_group_not_found"


class TestLeases:
    def test_claim_returns_lease_generation_one(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        resp = _claim(actae, gid, "router", "owner-a")
        assert resp.status_code == 200, resp.text
        lease = resp.json()["lease"]
        assert lease["member_id"] == "router"
        assert lease["owner_id"] == "owner-a"
        assert lease["generation"] == 1
        assert _parse_ts(lease["lease_until"]) > datetime.now(timezone.utc)

    def test_claim_same_owner_keeps_generation(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        first = _claim(actae, gid, "router", "owner-a").json()["lease"]
        second = _claim(actae, gid, "router", "owner-a").json()["lease"]
        assert first["generation"] == 1
        assert second["generation"] == 1
        assert first["owner_id"] == second["owner_id"] == "owner-a"

    def test_claim_other_owner_while_leased_409(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        _claim(actae, gid, "router", "owner-a")
        resp = _claim(actae, gid, "router", "owner-b")
        assert resp.status_code == 409
        assert resp.json()["status"] == "member_claimed"

    def test_claim_after_expiry_takeover_bumps_generation(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        _claim(actae, gid, "router", "owner-a", lease_seconds=1)
        assert _claim(actae, gid, "router", "owner-b").status_code == 409

        deadline = time.monotonic() + 15
        takeover = None
        while time.monotonic() < deadline:
            resp = _claim(actae, gid, "router", "owner-b")
            if resp.status_code == 200:
                takeover = resp.json()["lease"]
                break
            time.sleep(1)
        assert takeover is not None, "expired lease must be reclaimable"
        assert takeover["owner_id"] == "owner-b"
        assert takeover["generation"] == 2, "takeover must bump the generation"

    def test_claim_unknown_member_404(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        resp = _claim(actae, gid, "ghost", "owner-a")
        assert resp.status_code == 404
        assert resp.json()["status"] == "execution_group_member_not_found"

    def test_heartbeat_extends_lease(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        lease = _claim(actae, gid, "router", "owner-a", lease_seconds=1).json()["lease"]
        resp = _heartbeat(actae, gid, "router", "owner-a", lease["generation"], lease_seconds=120)
        assert resp.status_code == 200, resp.text
        # A different owner is still fenced out while the lease is held.
        assert _claim(actae, gid, "router", "owner-b").status_code == 409

    def test_heartbeat_stale_generation_409(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        _claim(actae, gid, "router", "owner-a")
        resp = _heartbeat(actae, gid, "router", "owner-a", generation=99)
        assert resp.status_code == 409
        assert resp.json()["status"] == "member_fenced"

    def test_heartbeat_unknown_member_404(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        resp = _heartbeat(actae, gid, "ghost", "owner-a", generation=1)
        assert resp.status_code == 404

    def test_release_clears_owner(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        lease = _claim(actae, gid, "router", "owner-a").json()["lease"]
        resp = _release(actae, gid, "router", "owner-a", lease["generation"])
        assert resp.status_code == 200
        assert resp.json()["status"] == "released"
        # The next owner takes over immediately (generation bumps).
        takeover = _claim(actae, gid, "router", "owner-b").json()["lease"]
        assert takeover["generation"] == 2

    def test_release_stale_owner_409(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        lease = _claim(actae, gid, "router", "owner-a").json()["lease"]
        resp = _release(actae, gid, "router", "owner-b", lease["generation"])
        assert resp.status_code == 409
        assert resp.json()["status"] == "member_fenced"


class TestMessages:
    def test_send_writes_dual_events(self, actae, ch_name):
        gid, router_ch, research_ch = _two_member_group(actae, ch_name)
        sent = _send(actae, gid, "research", "router", "research.result",
                     {"answer": 42})
        assert sent.status_code == 201, sent.text
        msg = sent.json()["message"]
        assert msg["from_member_id"] == "research"
        assert msg["to_member_id"] == "router"
        assert msg["type"] == "research.result"
        assert msg["payload"] == {"answer": 42}
        assert msg["status"] == "delivered"
        assert msg["source_event_id"] and msg["delivery_event_id"]

        # The sender channel holds the intent event; the recipient channel
        # holds the delivery event, causally linked.
        source = actae.replay(research_ch, cursor=0, limit=10)
        delivery = actae.replay(router_ch, cursor=0, limit=10)
        assert [e["type"] for e in source] == ["message.sent"]
        assert [e["type"] for e in delivery] == ["message.received"]
        assert source[0]["payload"]["message_id"] == msg["message_id"]
        assert delivery[0]["payload"]["message_id"] == msg["message_id"]
        assert delivery[0]["depends_on"] == source[0]["id"]

    def test_durable_inbox_replays_delivery(self, actae, ch_name):
        gid, router_ch, research_ch = _two_member_group(actae, ch_name)
        msg = _send(actae, gid, "research", "router", "a", {"n": 1}).json()["message"]
        _send(actae, gid, "research", "router", "b", {"n": 2})
        inbox = _messages(actae, gid, "router")
        assert [m["type"] for m in inbox] == ["a", "b"]
        assert inbox[0]["message_id"] == msg["message_id"]

        # `after=<message_id>` resumes past the anchor message.
        tail = _messages(actae, gid, "router", after=msg["message_id"])
        assert [m["type"] for m in tail] == ["b"]

    def test_send_self_400(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        resp = _send(actae, gid, "router", "router", "self", {})
        assert resp.status_code == 400

    def test_send_unknown_member_404(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        resp = _send(actae, gid, "ghost", "router", "x", {})
        assert resp.status_code == 404
        assert resp.json()["status"] == "execution_group_member_not_found"

    def test_send_empty_type_400(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        resp = _send(actae, gid, "research", "router", "", {})
        assert resp.status_code == 400

    def test_causal_context_roundtrip(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        causal = {"parent_message": str(uuid.uuid4()), "depth": 3}
        msg = _send(actae, gid, "research", "router", "x", {"n": 1},
                    causal_context=causal).json()["message"]
        assert msg["causal_context"] == causal
        inbox = _messages(actae, gid, "router")
        assert inbox[0]["causal_context"] == causal

    def test_ack_marks_acknowledged_and_idempotent(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        lease = _claim(actae, gid, "router", "owner-a").json()["lease"]
        msg = _send(actae, gid, "research", "router", "x", {"n": 1}).json()["message"]

        acked = _ack(actae, msg["message_id"], "owner-a", lease["generation"])
        assert acked.status_code == 200, acked.text
        acked = acked.json()["message"]
        assert acked["status"] == "acknowledged"
        assert acked["acknowledged_by"] == "owner-a"
        assert acked["acknowledged_at"] is not None

        # Re-acking is idempotent and preserves the original timestamp.
        again = _ack(actae, msg["message_id"], "owner-a", lease["generation"]).json()["message"]
        assert again["status"] == "acknowledged"
        assert again["acknowledged_at"] == acked["acknowledged_at"]

    def test_ack_stale_generation_409(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        _claim(actae, gid, "router", "owner-a")
        msg = _send(actae, gid, "research", "router", "x", {"n": 1}).json()["message"]
        resp = _ack(actae, msg["message_id"], "owner-a", 99)
        assert resp.status_code == 409
        assert resp.json()["status"] == "member_fenced"

    def test_ack_unknown_message_409(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        _claim(actae, gid, "router", "owner-a")
        resp = _ack(actae, str(uuid.uuid4()), "owner-a", 1)
        assert resp.status_code == 409

    def test_operation_id_makes_retries_idempotent(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        op = str(uuid.uuid4())
        first = _send(actae, gid, "research", "router", "x", {"n": 1},
                      operation_id=op).json()["message"]
        second = _send(actae, gid, "research", "router", "x", {"n": 1},
                       operation_id=op).json()["message"]
        assert first["message_id"] == second["message_id"]
        # Only one envelope and one pair of events are durable.
        assert len(_messages(actae, gid, "router")) == 1
        assert len(actae.replay(router_ch, cursor=0, limit=10)) == 1

    def test_operation_id_reuse_with_different_content_409(self, actae, ch_name):
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        op = str(uuid.uuid4())
        _send(actae, gid, "research", "router", "x", {"n": 1}, operation_id=op)
        resp = _send(actae, gid, "research", "router", "x", {"n": 2},
                     operation_id=op)
        assert resp.status_code == 409
        assert resp.json()["status"] == "idempotency_conflict"

    def test_parallel_sends_all_delivered(self, actae, ch_name):
        """8 concurrent sends with distinct operation ids → all 8 envelopes
        are durably delivered, exactly once each."""
        gid, router_ch, _ = _two_member_group(actae, ch_name)
        statuses = []
        errors = []

        def worker(i):
            try:
                resp = _send(actae, gid, "research", "router", "work",
                             {"i": i}, operation_id=str(uuid.uuid4()))
                statuses.append(resp.status_code)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, errors
        assert statuses == [201] * 8, statuses
        inbox = _messages(actae, gid, "router")
        assert len(inbox) == 8
        assert len({m["message_id"] for m in inbox}) == 8
        # Exactly one message.sent + one message.received per delivery.
        assert len(actae.replay(router_ch, cursor=0, limit=100)) == 8


class TestCausalGraph:
    def test_causal_graph_traverses_message_edge(self, actae, ch_name):
        gid, router_ch, research_ch = _two_member_group(actae, ch_name)
        msg = _send(actae, gid, "research", "router", "research.result",
                    {"answer": 42}).json()["message"]
        delivery_id = msg["delivery_event_id"]

        graph = _causal_graph(actae, delivery_id)
        assert graph["root_event_id"] == delivery_id
        assert graph["truncated"] is False
        ids = {e["id"] for e in graph["events"]}
        assert msg["source_event_id"] in ids
        assert delivery_id in ids
        # The delivery causally depends on the intent event.
        edge = next(d for d in graph["dependencies"] if d["event_id"] == delivery_id)
        assert edge["depends_on_event_id"] == msg["source_event_id"]
        assert edge["dependency_type"] == "message"

    def test_causal_graph_unknown_event_404(self, actae):
        resp = actae.raw_get(f"/api/v1/events/{uuid.uuid4()}/causal-graph")
        assert resp.status_code == 404


class TestGroupFork:
    def _setup_fork(self, actae, ch_name):
        gid, router_ch, research_ch = _two_member_group(actae, ch_name)
        # Give the intervention member some inbound history to replay.
        _send(actae, gid, "research", "router", "research.result", {"answer": 42})
        _send(actae, gid, "research", "router", "research.result", {"answer": 43})
        boundary = actae.cursor(router_ch)
        fork_id = f"{gid}-alt"
        body = {
            "fork_group_id": fork_id,
            "source_group_id": gid,
            "intervention_member_id": "router",
            "intervention_cursor": boundary,
            "member_policies": {"router": "live", "research": "frozen"},
            "tool_policies": {"web.search": "replay", "email.send": "block"},
        }
        return gid, router_ch, research_ch, fork_id, body

    def test_create_fork_clones_members_and_replays_history(self, actae, ch_name):
        gid, router_ch, research_ch, fork_id, body = self._setup_fork(actae, ch_name)
        resp = _create_fork(actae, body)
        assert resp.status_code == 201, resp.text
        fork = resp.json()["fork"]
        assert fork["fork_group_id"] == fork_id
        assert fork["source_group_id"] == gid
        assert fork["intervention_member_id"] == "router"
        assert fork["status"] == "active"
        assert fork["member_policies"] == {"router": "live", "research": "frozen"}
        assert fork["tool_policies"] == {"web.search": "replay", "email.send": "block"}
        assert len(fork["receipt"]["replayed_events"]) == 4  # 2 messages × (sent+received)

        # The fork clones member identities into isolated channels.
        members = {m["member_id"]: m for m in _members(actae, fork_id)}
        assert set(members) == {"router", "research"}
        assert members["router"]["channel_id"] == f"{fork_id}:router"
        assert members["research"]["channel_id"] == f"{fork_id}:research"

        # The intervention channel receives replayed inbound history.
        replayed = actae.replay(f"{fork_id}:router", cursor=0, limit=100)
        received = [e for e in replayed if e["type"] == "message.received"]
        assert len(received) == 2
        assert all(e["payload"]["replayed"] is True for e in received)
        assert {e["payload"]["payload"]["answer"] for e in received} == {42, 43}
        # The frozen sender's channel carries the replayed intent events.
        sent = actae.replay(f"{fork_id}:research", cursor=0, limit=100)
        assert [e["type"] for e in sent] == ["message.replayed", "message.replayed"]

        # The source history is untouched.
        assert len(actae.replay(router_ch, cursor=0, limit=100)) == 2
        assert len(actae.replay(research_ch, cursor=0, limit=100)) == 2

    def test_get_fork_returns_receipt(self, actae, ch_name):
        gid, router_ch, research_ch, fork_id, body = self._setup_fork(actae, ch_name)
        _create_fork(actae, body)
        fork = _get_fork(actae, fork_id)
        assert fork["fork_group_id"] == fork_id
        assert fork["intervention_cursor"] == body["intervention_cursor"]
        assert "replayed_events" in fork["receipt"]
        assert fork["first_divergence_event_id"] is None

    def test_get_missing_fork_404(self, actae):
        resp = actae.raw_get(f"/api/v1/execution-group-forks/{_gid('eg-nofork')}")
        assert resp.status_code == 404

    def test_fork_creation_idempotent(self, actae, ch_name):
        gid, router_ch, research_ch, fork_id, body = self._setup_fork(actae, ch_name)
        first = _create_fork(actae, body).json()["fork"]
        second = _create_fork(actae, body).json()["fork"]
        assert first["receipt"] == second["receipt"]
        assert first["first_divergence_event_id"] == second["first_divergence_event_id"]
        assert len(actae.replay(f"{fork_id}:router", cursor=0, limit=100)) == 2, \
            "idempotent replay must not duplicate fork history"

    def test_fork_creation_conflicting_params_409(self, actae, ch_name):
        gid, router_ch, research_ch, fork_id, body = self._setup_fork(actae, ch_name)
        _create_fork(actae, body)
        conflict = dict(body)
        conflict["intervention_cursor"] = body["intervention_cursor"] - 1
        resp = _create_fork(actae, conflict)
        assert resp.status_code == 409
        assert resp.json()["status"] == "channel_conflict"

    def test_fork_invalid_policies_400(self, actae, ch_name):
        gid, router_ch, research_ch, fork_id, body = self._setup_fork(actae, ch_name)

        # Every member must be covered.
        missing = dict(body, member_policies={"router": "live"})
        resp = _create_fork(actae, missing)
        assert resp.status_code == 400

        # The intervention member must be live.
        not_live = dict(body, member_policies={"router": "frozen", "research": "frozen"})
        resp = _create_fork(actae, not_live)
        assert resp.status_code == 400

        # Tool policies must be replay/live/block.
        bad_tool = dict(body, tool_policies={"web.search": "sometimes"})
        resp = _create_fork(actae, bad_tool)
        assert resp.status_code == 400

        # Intervention member must exist.
        no_member = dict(body, intervention_member_id="ghost")
        resp = _create_fork(actae, no_member)
        assert resp.status_code == 404

    def test_fork_unknown_source_404(self, actae):
        body = {
            "fork_group_id": _gid("eg-fork"),
            "source_group_id": _gid("eg-nosource"),
            "intervention_member_id": "router",
            "intervention_cursor": 0,
            "member_policies": {"router": "live"},
            "tool_policies": {},
        }
        resp = _create_fork(actae, body)
        assert resp.status_code == 404
        assert resp.json()["status"] == "execution_group_not_found"

    def test_frozen_member_cannot_claim_409(self, actae, ch_name):
        gid, router_ch, research_ch, fork_id, body = self._setup_fork(actae, ch_name)
        _create_fork(actae, body)
        resp = _claim(actae, fork_id, "research", "worker-a")
        assert resp.status_code == 409
        assert resp.json()["status"] == "counterfactual_blocked"

    def test_send_from_frozen_member_blocked_and_recorded(self, actae, ch_name):
        gid, router_ch, research_ch, fork_id, body = self._setup_fork(actae, ch_name)
        _create_fork(actae, body)
        resp = _send(actae, fork_id, "research", "router", "should.not", {"x": 1})
        assert resp.status_code == 409
        assert resp.json()["status"] == "counterfactual_blocked"

        fork = _get_fork(actae, fork_id)
        assert fork["status"] == "blocked"
        boundaries = fork["receipt"]["blocked_boundaries"]
        assert any("research" in b["boundary"] for b in boundaries)

    def test_promote_frozen_member_then_send_succeeds(self, actae, ch_name):
        gid, router_ch, research_ch, fork_id, body = self._setup_fork(actae, ch_name)
        _create_fork(actae, body)

        resp = _promote(actae, fork_id, "research")
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "promoted"
        fork = _get_fork(actae, fork_id)
        assert fork["member_policies"]["research"] == "reactive"
        assert [p["member_id"] for p in fork["receipt"]["promoted_members"]] == ["research"]

        # After promotion the (formerly frozen) member can send and claim.
        sent = _send(actae, fork_id, "research", "router", "ok.now", {"go": True})
        assert sent.status_code == 201, sent.text
        assert _claim(actae, fork_id, "research", "worker-a").status_code == 200

    def test_promote_reactive_member_is_idempotent(self, actae, ch_name):
        gid, router_ch, research_ch, fork_id, body = self._setup_fork(actae, ch_name)
        _create_fork(actae, body)
        assert _promote(actae, fork_id, "research").status_code == 200
        assert _promote(actae, fork_id, "research").status_code == 200

    def test_promote_live_member_400(self, actae, ch_name):
        gid, router_ch, research_ch, fork_id, body = self._setup_fork(actae, ch_name)
        _create_fork(actae, body)
        resp = _promote(actae, fork_id, "router")
        assert resp.status_code == 400

    def test_promote_unknown_member_404(self, actae, ch_name):
        gid, router_ch, research_ch, fork_id, body = self._setup_fork(actae, ch_name)
        _create_fork(actae, body)
        resp = _promote(actae, fork_id, "ghost")
        assert resp.status_code == 404
        assert resp.json()["status"] == "execution_group_member_not_found"


class TestLiveDelivery:
    @pytest.mark.asyncio
    async def test_ws_recipient_receives_message_live(self, actae, ws, ch_name):
        gid, router_ch, research_ch = _two_member_group(actae, ch_name)
        _claim(actae, gid, "router", "owner-a")

        async with await ws.connect() as conn:
            await conn.authenticate(actae._cfg.api_key)
            await conn.subscribe(router_ch)

            sent = _send(actae, gid, "research", "router", "research.result",
                         {"answer": 42})
            assert sent.status_code == 201, sent.text
            expected_id = sent.json()["message"]["message_id"]

            msgs = await conn.recv_all(count=1, timeout=15)
            assert msgs, "expected a live broadcast for the delivered message"
            frame = msgs[0]
            assert frame.get("type") == "broadcast"
            assert frame["topic"] == router_ch
            ev = frame["payload"]["event"]
            assert ev["event_type"] == "message.received"
            assert ev["payload"]["message_id"] == expected_id
            assert ev["payload"]["from"]["member"] == "research"