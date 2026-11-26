# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Integration tests for the idempotent tool execution API.

Covers the full lifecycle: claim (create/replay/reclaim/in_progress),
complete, fail, heartbeat, cancel, lease expiry, request-hash matching,
dedup_fields, error mapping (409/404/400), WebSocket broadcasting of
lifecycle events, and get/list/delete.
"""

import asyncio
import time
import uuid

import pytest


def _claim(actae, ch, key, tool, params=None, **extra):
    body = {"channel_id": ch, "key_name": key, "tool_name": tool}
    if params is not None:
        body["params"] = params
    body.update(extra)
    return actae.raw_post("/api/v1/executions/claim", body)


def _complete(actae, exec_id, claim_token=None, result=None):
    body = {}
    if claim_token is not None:
        body["claim_token"] = claim_token
    if result is not None:
        body["result"] = result
    return actae.raw_post(f"/api/v1/executions/{exec_id}/complete", body)


def _fail(actae, exec_id, message, claim_token=None, error_type=None, stack=None):
    body = {"message": message}
    if claim_token is not None:
        body["claim_token"] = claim_token
    if error_type is not None:
        body["error_type"] = error_type
    if stack is not None:
        body["stack"] = stack
    return actae.raw_post(f"/api/v1/executions/{exec_id}/fail", body)


def _heartbeat(actae, exec_id, claim_token=None, lease_seconds=None):
    body = {}
    if claim_token is not None:
        body["claim_token"] = claim_token
    if lease_seconds is not None:
        body["lease_seconds"] = lease_seconds
    return actae.raw_post(f"/api/v1/executions/{exec_id}/heartbeat", body)


def _cancel(actae, exec_id, claim_token=None):
    body = {}
    if claim_token is not None:
        body["claim_token"] = claim_token
    return actae.raw_post(f"/api/v1/executions/{exec_id}/cancel", body)


def _get(actae, exec_id):
    return actae.raw_get(f"/api/v1/executions/{exec_id}")


def _list(actae, channel_id, limit=50):
    return actae.raw_get("/api/v1/executions", params={"channel_id": channel_id, "limit": limit})


def _delete(actae, exec_id):
    return actae.raw_delete(f"/api/v1/executions/{exec_id}")


def _events(actae, ch, event_type=None):
    return actae.replay(ch, cursor=0, limit=100, event_type=event_type)


# ---------------------------------------------------------------------------
# Claim lifecycle
# ---------------------------------------------------------------------------

class TestClaimLifecycle:
    def test_claim_creates_running_execution(self, actae, ch_name):
        resp = _claim(actae, ch_name, "pay-1", "charge_customer", {"amount": 5})
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["status"] == "claimed"
        assert body["claim_token"] is not None
        ex = body["execution"]
        assert ex["channel_id"] == ch_name
        assert ex["key_name"] == "pay-1"
        assert ex["tool_name"] == "charge_customer"
        assert ex["status"] == "running"
        assert ex["attempts"] == 1
        assert ex["params"] == {"amount": 5}
        assert ex["started_event_id"] is not None
        assert ex["started_cursor"] is not None
        assert ex["lease_until"] is not None
        assert ex["result"] is None
        assert ex["error"] is None

    def test_claim_persists_tool_started_event(self, actae, ch_name):
        resp = _claim(actae, ch_name, "k-1", "mytool", {"x": 1})
        assert resp.status_code == 201
        started = _events(actae, ch_name, event_type="tool.started")
        assert len(started) == 1
        evt = started[0]
        assert evt["type"] == "tool.started"
        assert evt["payload"]["tool"] == "mytool"
        assert evt["payload"]["key"] == "k-1"
        assert evt["payload"]["attempt"] == 1
        assert evt["payload"]["params"] == {"x": 1}
        assert evt["payload"]["execution_id"] == resp.json()["execution"]["id"]

    def test_second_claim_while_running_returns_in_progress(self, actae, ch_name):
        first = _claim(actae, ch_name, "k-1", "t").json()
        assert first["status"] == "claimed"
        second = _claim(actae, ch_name, "k-1", "t")
        assert second.status_code == 201, second.text
        body = second.json()
        assert body["status"] == "in_progress"
        assert body["claim_token"] is None
        assert body["result"] is None
        # No additional tool.started event written.
        assert len(_events(actae, ch_name, event_type="tool.started")) == 1

    def test_complete_persists_result_and_event(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t", {"q": 7}).json()
        exec_id = claim["execution"]["id"]
        resp = _complete(actae, exec_id, claim_token=claim["claim_token"], result={"paid": True, "charge": 7})
        assert resp.status_code == 200, resp.text
        ex = resp.json()["execution"]
        assert ex["status"] == "completed"
        assert ex["result"] == {"paid": True, "charge": 7}
        assert ex["completed_event_id"] is not None
        assert "claim_token" not in ex  # field is not serialized
        events = _events(actae, ch_name, event_type="tool.completed")
        assert len(events) == 1
        assert events[0]["payload"]["params"] == {"q": 7}
        assert events[0]["payload"]["execution_id"] == exec_id

    def test_claim_after_complete_replays_result(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t", {"a": 1}).json()
        _complete(actae, claim["execution"]["id"], claim_token=claim["claim_token"], result={"done": 1})
        replay = _claim(actae, ch_name, "k-1", "t", {"a": 1})
        assert replay.status_code == 201, replay.text
        body = replay.json()
        assert body["status"] == "replayed"
        assert body["result"] == {"done": 1}
        assert body["claim_token"] is None
        # attempts did not bump on replay
        assert body["execution"]["attempts"] == 1
        # no extra started event
        assert len(_events(actae, ch_name, event_type="tool.started")) == 1

    def test_duplicate_complete_is_idempotent(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t").json()
        first = _complete(actae, claim["execution"]["id"], claim_token=claim["claim_token"], result={"ok": 1})
        assert first.status_code == 200
        second = _complete(actae, claim["execution"]["id"], claim_token=claim["claim_token"], result={"ok": 999})
        assert second.status_code == 200
        # Duplicate complete returns the STORED result, not the new payload.
        assert second.json()["execution"]["result"] == {"ok": 1}
        assert len(_events(actae, ch_name, event_type="tool.completed")) == 1

    def test_claim_mismatched_request_returns_409(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t", {"a": 1}).json()
        _complete(actae, claim["execution"]["id"], claim_token=claim["claim_token"], result={"ok": 1})
        resp = _claim(actae, ch_name, "k-1", "t", {"a": 2})
        assert resp.status_code == 409
        assert resp.json()["status"] == "idempotency_key_mismatch"

    def test_claim_mismatched_tool_returns_409(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "tool_a").json()
        _complete(actae, claim["execution"]["id"], claim_token=claim["claim_token"], result={})
        resp = _claim(actae, ch_name, "k-1", "tool_b")
        assert resp.status_code == 409
        assert resp.json()["status"] == "idempotency_key_mismatch"


# ---------------------------------------------------------------------------
# Fail / reclaim
# ---------------------------------------------------------------------------

class TestFailAndReclaim:
    def test_fail_persists_error_and_event(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t").json()
        exec_id = claim["execution"]["id"]
        resp = _fail(
            actae, exec_id, "card declined",
            claim_token=claim["claim_token"], error_type="PaymentError", stack="trace-123",
        )
        assert resp.status_code == 200, resp.text
        ex = resp.json()["execution"]
        assert ex["status"] == "failed"
        assert ex["error"]["message"] == "card declined"
        assert ex["error"]["error_type"] == "PaymentError"
        assert ex["error"]["stack"] == "trace-123"
        events = _events(actae, ch_name, event_type="tool.failed")
        assert len(events) == 1
        assert events[0]["payload"]["error"]["message"] == "card declined"

    def test_claim_after_fail_returns_claimed_with_attempt_bump(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t", {"n": 1}).json()
        _fail(actae, claim["execution"]["id"], "boom", claim_token=claim["claim_token"])
        retry = _claim(actae, ch_name, "k-1", "t", {"n": 1})
        assert retry.status_code == 201
        body = retry.json()
        assert body["status"] == "claimed"
        assert body["claim_token"] is not None
        assert body["execution"]["attempts"] == 2
        assert len(_events(actae, ch_name, event_type="tool.started")) == 2

    def test_duplicate_fail_is_idempotent(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t").json()
        exec_id = claim["execution"]["id"]
        _fail(actae, exec_id, "boom", claim_token=claim["claim_token"])
        again = _fail(actae, exec_id, "different message", claim_token=claim["claim_token"])
        assert again.status_code == 200
        # Stored error is preserved; no duplicate event.
        assert again.json()["execution"]["error"]["message"] == "boom"
        assert len(_events(actae, ch_name, event_type="tool.failed")) == 1


# ---------------------------------------------------------------------------
# Lease expiry
# ---------------------------------------------------------------------------

class TestLeaseExpiry:
    def test_expired_lease_is_reclaimed(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t", lease_seconds=1)
        assert claim.status_code == 201
        assert claim.json()["status"] == "claimed"
        time.sleep(1.6)
        reclaim = _claim(actae, ch_name, "k-1", "t")
        assert reclaim.status_code == 201, reclaim.text
        body = reclaim.json()
        assert body["status"] == "reclaimed"
        assert body["claim_token"] is not None
        assert body["execution"]["attempts"] == 2

    def test_short_lease_not_expired_within_window(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t", lease_seconds=60)
        assert claim.json()["status"] == "claimed"
        second = _claim(actae, ch_name, "k-1", "t")
        assert second.json()["status"] == "in_progress"

    def test_heartbeat_extends_lease(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t", lease_seconds=1)
        exec_id = claim.json()["execution"]["id"]
        token = claim.json()["claim_token"]
        time.sleep(0.4)
        hb = _heartbeat(actae, exec_id, claim_token=token, lease_seconds=5)
        assert hb.status_code == 200, hb.text
        # After heartbeat the lease is long enough that a claim stays in_progress.
        time.sleep(0.8)
        second = _claim(actae, ch_name, "k-1", "t")
        assert second.json()["status"] == "in_progress"
        # Original owner can still complete it.
        done = _complete(actae, exec_id, claim_token=token, result={"ok": 1})
        assert done.status_code == 200


# ---------------------------------------------------------------------------
# Cancel
# ---------------------------------------------------------------------------

class TestCancel:
    def test_cancel_marks_cancelled_and_is_reclaimable(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t").json()
        exec_id = claim["execution"]["id"]
        resp = _cancel(actae, exec_id, claim_token=claim["claim_token"])
        assert resp.status_code == 200, resp.text
        assert resp.json()["execution"]["status"] == "cancelled"
        # No terminal event emitted for cancel.
        assert len(_events(actae, ch_name, event_type="tool.cancelled")) == 0
        # Reclaim afterwards.
        reclaim = _claim(actae, ch_name, "k-1", "t")
        assert reclaim.json()["status"] == "reclaimed"
        assert reclaim.json()["execution"]["attempts"] == 2

    def test_cancel_wrong_token_conflicts(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t").json()
        resp = _cancel(actae, claim["execution"]["id"], claim_token=str(uuid.uuid4()))
        assert resp.status_code == 409
        assert resp.json()["status"] == "execution_not_owned"


# ---------------------------------------------------------------------------
# Claim token enforcement
# ---------------------------------------------------------------------------

class TestClaimTokenEnforcement:
    def test_complete_wrong_token_conflicts(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t").json()
        resp = _complete(actae, claim["execution"]["id"], claim_token=str(uuid.uuid4()), result={"x": 1})
        assert resp.status_code == 409
        assert resp.json()["status"] == "execution_not_owned"
        # Execution still owned by the original claim.
        ex = _get(actae, claim["execution"]["id"]).json()["execution"]
        assert ex["status"] == "running"

    def test_complete_malformed_token_is_400(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t").json()
        resp = _complete(actae, claim["execution"]["id"], claim_token="not-a-uuid", result={})
        assert resp.status_code == 400
        assert resp.json()["status"] == "invalid_request"

    def test_fail_wrong_token_conflicts(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t").json()
        resp = _fail(actae, claim["execution"]["id"], "boom", claim_token=str(uuid.uuid4()))
        assert resp.status_code == 409
        assert resp.json()["status"] == "execution_not_owned"

    def test_heartbeat_wrong_token_conflicts(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t").json()
        resp = _heartbeat(actae, claim["execution"]["id"], claim_token=str(uuid.uuid4()))
        assert resp.status_code == 409
        assert resp.json()["status"] == "execution_not_owned"

    def test_complete_missing_execution_is_404(self, actae, ch_name):
        resp = _complete(actae, str(uuid.uuid4()), claim_token=None, result={})
        assert resp.status_code == 404
        assert resp.json()["status"] == "execution_not_found"


# ---------------------------------------------------------------------------
# dedup_fields
# ---------------------------------------------------------------------------

class TestDedupFields:
    def test_dedup_fields_ignore_volatile_params(self, actae, ch_name):
        claim = _claim(
            actae, ch_name, "k-1", "t",
            params={"customer": "abc", "nonce": "n1"},
            dedup_fields=["customer"],
        )
        assert claim.status_code == 201
        assert claim.json()["status"] == "claimed"
        _complete(actae, claim.json()["execution"]["id"], claim_token=claim.json()["claim_token"], result={"ok": 1})

        replay = _claim(
            actae, ch_name, "k-1", "t",
            params={"customer": "abc", "nonce": "DIFFERENT"},
            dedup_fields=["customer"],
        )
        assert replay.status_code == 201, replay.text
        assert replay.json()["status"] == "replayed"
        assert replay.json()["result"] == {"ok": 1}

    def test_volatile_param_breaks_identity_without_dedup(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t", params={"customer": "abc", "nonce": "n1"})
        assert claim.json()["status"] == "claimed"
        _complete(actae, claim.json()["execution"]["id"], claim_token=claim.json()["claim_token"], result={})
        resp = _claim(actae, ch_name, "k-1", "t", params={"customer": "abc", "nonce": "n2"})
        assert resp.status_code == 409
        assert resp.json()["status"] == "idempotency_key_mismatch"

    def test_param_key_order_does_not_matter(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t", params={"a": 1, "b": 2})
        assert claim.json()["status"] == "claimed"
        _complete(actae, claim.json()["execution"]["id"], claim_token=claim.json()["claim_token"], result={"ok": 1})
        replay = _claim(actae, ch_name, "k-1", "t", params={"b": 2, "a": 1})
        assert replay.status_code == 201, replay.text
        assert replay.json()["status"] == "replayed"


# ---------------------------------------------------------------------------
# WebSocket broadcasting
# ---------------------------------------------------------------------------

async def _wait_broadcast(socket, expected_type, timeout=20.0):
    """Consume subscription acks until a broadcast with the given event type
    arrives. Raises AssertionError on a different broadcast or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        msg = await socket.recv(min(1.0, deadline - time.monotonic()))
        if msg.get("type") == "broadcast":
            evt = msg["payload"]["event"]
            assert evt.get("event_type") == expected_type, (
                f"expected {expected_type} broadcast, got {evt.get('event_type')}"
            )
            return msg
        # Subscription/CursorSync acks are consumed silently.
    pytest.fail(f"timed out waiting for {expected_type} broadcast")


async def _drain_acks(socket, quiet=0.5):
    """Consume non-broadcast messages until the socket is quiet. Fails if a
    broadcast arrives unexpectedly."""
    while True:
        try:
            msg = await socket.recv(quiet)
        except (asyncio.TimeoutError, TimeoutError):
            return
        if msg.get("type") == "broadcast":
            pytest.fail(f"unexpected broadcast: {msg.get('payload')}")


class TestWebSocketBroadcast:
    def test_claim_and_complete_broadcast_lifecycle_events(self, actae, ws, ch_name, actae_config):
        async def run():
            async with await ws.connect() as socket:
                # Authenticated subscriber: broadcast delivery is permission-
                # filtered and anonymous sockets must not rely on delivery.
                await socket.send_auth(api_key=actae_config.api_key)
                await socket.subscribe(ch_name)
                claim = _claim(actae, ch_name, "k-1", "mytool", {"v": 3})
                assert claim.status_code == 201
                exec_id = claim.json()["execution"]["id"]
                token = claim.json()["claim_token"]

                started = await _wait_broadcast(socket, "tool.started")
                evt = started["payload"]["event"]
                assert started["type"] == "broadcast"
                assert started["topic"] == ch_name
                assert evt["payload"]["tool"] == "mytool"
                assert evt["payload"]["key"] == "k-1"
                assert evt["payload"]["attempt"] == 1
                assert evt["payload"]["params"] == {"v": 3}
                assert evt["metadata"]["execution_id"] == exec_id
                assert evt["actor"] == "system"
                assert evt["channel_cursor"] is not None

                _complete(actae, exec_id, claim_token=token, result={"done": True})
                completed = await _wait_broadcast(socket, "tool.completed")
                cevt = completed["payload"]["event"]
                assert cevt["metadata"]["execution_id"] == exec_id
                assert cevt["depends_on"] == evt["id"]

        asyncio.run(run())

    def test_fail_broadcasts_tool_failed(self, actae, ws, ch_name, actae_config):
        async def run():
            async with await ws.connect() as socket:
                await socket.send_auth(api_key=actae_config.api_key)
                await socket.subscribe(ch_name)
                claim = _claim(actae, ch_name, "k-1", "t").json()
                started = await _wait_broadcast(socket, "tool.started")
                assert started["payload"]["event"]["payload"]["key"] == "k-1"
                _fail(actae, claim["execution"]["id"], "exploded", claim_token=claim["claim_token"])
                msg = await _wait_broadcast(socket, "tool.failed")
                assert msg["payload"]["event"]["payload"]["error"]["message"] == "exploded"

        asyncio.run(run())

    def test_replay_broadcasts_tool_replayed_when_opted_in(self, actae, ws, ch_name, actae_config):
        async def run():
            claim = _claim(actae, ch_name, "k-1", "t").json()
            _complete(actae, claim["execution"]["id"], claim_token=claim["claim_token"], result={"ok": 1})
            async with await ws.connect() as socket:
                await socket.send_auth(api_key=actae_config.api_key)
                await socket.subscribe(ch_name)
                replay = _claim(actae, ch_name, "k-1", "t", emit_replay_event=True)
                assert replay.json()["status"] == "replayed"
                msg = await _wait_broadcast(socket, "tool.replayed")
                assert msg["payload"]["event"]["metadata"]["execution_id"] == claim["execution"]["id"]
                assert msg["payload"]["event"]["payload"]["attempt"] == 1

        asyncio.run(run())

    def test_replay_no_event_when_not_opted_in(self, actae, ws, ch_name):
        async def run():
            claim = _claim(actae, ch_name, "k-1", "t").json()
            _complete(actae, claim["execution"]["id"], claim_token=claim["claim_token"], result={})
            async with await ws.connect() as socket:
                await socket.subscribe(ch_name)
                replay = _claim(actae, ch_name, "k-1", "t")
                assert replay.json()["status"] == "replayed"
                # Drain subscription acks; no broadcast must appear.
                await _drain_acks(socket)

        asyncio.run(run())

    def test_replay_marker_emitted_only_once(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t").json()
        _complete(actae, claim["execution"]["id"], claim_token=claim["claim_token"], result={})
        _claim(actae, ch_name, "k-1", "t", emit_replay_event=True)
        _claim(actae, ch_name, "k-1", "t", emit_replay_event=True)
        assert len(_events(actae, ch_name, event_type="tool.replayed")) == 1


# ---------------------------------------------------------------------------
# Get / list / delete
# ---------------------------------------------------------------------------

class TestGetListDelete:
    def test_get_execution(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t", {"z": 9}).json()
        resp = _get(actae, claim["execution"]["id"])
        assert resp.status_code == 200
        ex = resp.json()["execution"]
        assert ex["id"] == claim["execution"]["id"]
        assert ex["key_name"] == "k-1"
        assert ex["status"] == "running"
        assert ex["params"] == {"z": 9}

    def test_get_missing_execution_is_404(self, actae):
        resp = _get(actae, str(uuid.uuid4()))
        assert resp.status_code == 404
        assert resp.json()["error"] == "execution_not_found"

    def test_list_executions_newest_first(self, actae, ch_name):
        ids = []
        for i in range(3):
            claim = _claim(actae, ch_name, f"key-{i}", "t").json()
            ids.append(claim["execution"]["id"])
            _complete(actae, claim["execution"]["id"], claim_token=claim["claim_token"], result={"i": i})
        resp = _list(actae, ch_name)
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 3
        got = [e["id"] for e in body["executions"]]
        assert set(got) == set(ids)
        # Newest first: created later → listed earlier.
        assert got[0] == ids[-1]
        assert got[-1] == ids[0]

    def test_list_is_scoped_to_channel(self, actae, ch_name):
        _claim(actae, ch_name, "k-1", "t")
        other = _list(actae, f"{ch_name}-other")
        assert other.json()["count"] == 0

    def test_list_limit(self, actae, ch_name):
        for i in range(5):
            _claim(actae, ch_name, f"k-{i}", "t")
        resp = _list(actae, ch_name, limit=2)
        assert resp.json()["count"] == 2

    def test_delete_execution(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t").json()
        exec_id = claim["execution"]["id"]
        resp = _delete(actae, exec_id)
        assert resp.status_code == 200
        assert resp.json()["status"] == "deleted"
        assert _get(actae, exec_id).status_code == 404

    def test_delete_missing_execution_is_404(self, actae):
        assert _delete(actae, str(uuid.uuid4())).status_code == 404


# ---------------------------------------------------------------------------
# Validation & concurrency
# ---------------------------------------------------------------------------

class TestValidation:
    def test_claim_missing_fields_is_400(self, actae, ch_name):
        resp = actae.raw_post("/api/v1/executions/claim", {"channel_id": ch_name})
        assert resp.status_code == 400
        assert resp.json()["error"] == "Invalid request body"

    def test_claim_empty_body_is_400(self, actae):
        resp = actae.raw_post("/api/v1/executions/claim", {})
        assert resp.status_code == 400

    def test_claim_bad_json_is_400(self, actae):
        headers = actae._cfg.api_headers
        resp = actae._http.post(
            f"{actae._cfg.api_url}/api/v1/executions/claim",
            content=b"{not json",
            headers=headers,
        )
        assert resp.status_code == 400

    def test_invalid_execution_id_is_400(self, actae):
        assert _get(actae, "not-a-uuid").status_code == 400
        assert _complete(actae, "not-a-uuid", result={}).status_code == 400

    def test_complete_missing_body_fields_is_ok(self, actae, ch_name):
        # claim_token and result are optional; an empty body is valid for
        # completing with a NULL result.
        claim = _claim(actae, ch_name, "k-1", "t").json()
        resp = _complete(actae, claim["execution"]["id"], claim_token=claim["claim_token"])
        assert resp.status_code == 200
        assert resp.json()["execution"]["result"] is None

    def test_fail_requires_message(self, actae, ch_name):
        claim = _claim(actae, ch_name, "k-1", "t").json()
        resp = actae.raw_post(
            f"/api/v1/executions/{claim['execution']['id']}/fail",
            {"claim_token": claim["claim_token"]},
        )
        assert resp.status_code == 400  # serde requires `message`


class TestConcurrency:
    def test_parallel_claims_exactly_one_wins(self, actae, ch_name):
        import concurrent.futures

        def do_claim(_):
            return _claim(actae, ch_name, "k-1", "t")

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(do_claim, range(4)))

        statuses = [r.json()["status"] for r in results]
        assert statuses.count("claimed") == 1, statuses
        assert statuses.count("in_progress") == 3, statuses
        # Exactly one tool.started event.
        assert len(_events(actae, ch_name, event_type="tool.started")) == 1

    def test_parallel_completes_race_is_idempotent(self, actae, ch_name):
        import concurrent.futures

        claim = _claim(actae, ch_name, "k-1", "t").json()
        exec_id = claim["execution"]["id"]
        token = claim["claim_token"]

        def do_complete(_):
            return _complete(actae, exec_id, claim_token=token, result={"n": _})

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            responses = list(pool.map(do_complete, range(4)))

        codes = {r.status_code for r in responses}
        assert codes <= {200}, responses
        assert len(_events(actae, ch_name, event_type="tool.completed")) == 1
        ex = _get(actae, exec_id).json()["execution"]
        assert ex["status"] == "completed"
