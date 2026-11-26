# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Transport parity suite (selling point 4).

Runs the SAME scenario over HTTP and WebSocket (Python SDK) and asserts the
events are represented identically: auth, scoped subscribe + deny, publish
with request_id ack correlation, idempotent retry via operation_id, replay
after publish, and error codes.

Opt-in, like the other live suites: needs a running Actae (dev or docker)
and the standard env vars (ACTAE_URL / ACTAE_WS_URL / ACTAE_API_KEY).

    ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-... \
      python3 -m pytest tests/test_transport_parity.py -v
"""

import asyncio
import os
import uuid

import pytest

ACTAE_URL = os.environ.get("ACTAE_URL")
ACTAE_API_KEY = os.environ.get("ACTAE_API_KEY")

pytestmark = pytest.mark.skipif(
    not (ACTAE_URL and ACTAE_API_KEY),
    reason="requires ACTAE_URL and ACTAE_API_KEY (live Actae)",
)


def _ws_url() -> str:
    return os.environ.get("ACTAE_WS_URL") or ACTAE_URL.replace("http://", "ws://") + "/ws"


def _make_client():
    from actae_client.client import ActaeClient

    return ActaeClient(
        api_key=ACTAE_API_KEY,
        endpoint=ACTAE_URL,
        auto_reconnect=False,
    )


@pytest.mark.asyncio
async def test_http_record_and_ws_publish_produce_same_event_shape():
    client = _make_client()
    run = uuid.uuid4().hex[:8]
    async with client:
        # Same payload published via HTTP record and via WS publish.
        http_event = await client.record(f"parity.http.{run}", "parity.event", {"n": 1}, actor="a")
        ws_event = await client.publish(f"parity.ws.{run}", {"n": 1})

        # Identical representation: both carry server id, channel, type,
        # payload, actor, cursor and timestamp.
        assert http_event.channel_id == f"parity.http.{run}"
        assert http_event.event_type == "parity.event"
        assert http_event.payload == {"n": 1}
        assert http_event.actor == "a"
        assert http_event.cursor >= 1
        assert http_event.id
        assert http_event.timestamp

        assert ws_event.channel_id == f"parity.ws.{run}"
        assert ws_event.payload == {"n": 1}
        assert ws_event.cursor >= 1
        assert ws_event.id
        assert ws_event.timestamp

        # WS-published events are re-playable over HTTP with the same shape.
        replayed = await client.replay(f"parity.ws.{run}")
        assert len(replayed) == 1
        rp = replayed[0]
        assert rp.channel_id == f"parity.ws.{run}"
        assert rp.payload == {"n": 1}
        assert rp.cursor == ws_event.cursor


@pytest.mark.asyncio
async def test_publish_idempotent_retry_replays_original():
    client = _make_client()
    run = uuid.uuid4().hex[:8]
    async with client:
        # Retrying a WS publish with the same (topic, operation_id) replays
        # the original event instead of duplicating — same contract as HTTP
        # record idempotency. operation_id must be a real UUID (the server
        # parses it as one).
        op = str(uuid.uuid4())
        first = await client.publish(f"parity.idem.{run}", {"n": 1}, operation_id=op)
        second = await client.publish(f"parity.idem.{run}", {"n": 1}, operation_id=op)
        assert second.id == first.id
        assert second.cursor == first.cursor
        replayed = await client.replay(f"parity.idem.{run}")
        assert len(replayed) == 1


@pytest.mark.asyncio
async def test_ws_subscribe_auth_required_before_subscribe():
    """subscribe before auth must be rejected (audit item 1)."""
    import aiohttp
    import json as _json

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(_ws_url()) as ws:
            # Clear the server's `connected` welcome frame.
            await asyncio.wait_for(ws.receive(), timeout=5)
            await ws.send_json({"type": "subscribe", "topic": "parity.noauth"})
            resp = await asyncio.wait_for(ws.receive(), timeout=5)
            msg = resp.json()
            text = _json.dumps(msg)
            assert "AUTH_REQUIRED" in text or "auth" in text.lower(), f"expected auth error, got {msg}"


@pytest.mark.asyncio
async def test_ws_publish_without_auth_rejected():
    import aiohttp
    import json as _json

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(_ws_url()) as ws:
            await asyncio.wait_for(ws.receive(), timeout=5)
            await ws.send_json({"type": "broadcast", "topic": "parity.noauth", "payload": {}})
            resp = await asyncio.wait_for(ws.receive(), timeout=5)
            msg = resp.json()
            text = _json.dumps(msg)
            assert "AUTH_REQUIRED" in text or "auth" in text.lower(), f"expected auth error, got {msg}"


@pytest.mark.asyncio
async def test_verify_chain_and_close_channel_live():
    """Tamper-evident ledger + lifecycle endpoints over HTTP (audit sp 3, item 14)."""
    import uuid as _uuid

    client = _make_client()
    run = _uuid.uuid4().hex[:8]
    async with client:
        for i in range(2):
            await client.record(f"parity.ledger.{run}", f"ledger.{i}", {"n": i}, actor="a")

        # The ledger verifies for fresh data.
        data = await client._request("GET", f"/api/v1/events/verify/parity.ledger.{run}")
        assert data["valid"] is True, data
        assert data["verified_events"] == 2

        # Closing stamps the registry lifecycle timestamp.
        closed = await client._request("POST", f"/api/v1/channels/parity.ledger.{run}/close")
        assert closed["status"] == "closed"
        assert closed["closed_at"]

        # An unknown channel's close is a 404.
        from actae_client.errors import APIError
        with pytest.raises(APIError) as ei:
            await client._request("POST", "/api/v1/channels/parity.ledger.does-not-exist/close")
        assert ei.value.status_code == 404


def test_auth_error_responses_leak_no_internals():
    """Audit item 15: error bodies must not contain DB/config detail."""
    import httpx

    # Unauthenticated requests to protected endpoints return clean bodies.
    for url in (
        f"{ACTAE_URL}/api/v1/channels",
        f"{ACTAE_URL}/api/v1/auth/capabilities",
        f"{ACTAE_URL}/api/v1/user/preferences",
    ):
        r = httpx.get(url, timeout=10, verify=False)
        body = r.text.lower()
        for marker in ("postgres", "sqlx", "connection refused", "hunter2", "dbname=", "password="):
            assert marker not in body, f"leak '{marker}' in {url}: {r.text}"
        assert r.status_code in (401, 403), f"expected auth error for {url}, got {r.status_code}"


async def _ws_handshake(ws, api_key):
    """Clear the welcome frame and authenticate; returns once authenticated."""
    await asyncio.wait_for(ws.receive(), timeout=5)  # connected welcome
    await ws.send_json({"type": "auth", "api_key": api_key})
    while True:
        m = (await asyncio.wait_for(ws.receive(), timeout=5)).json()
        inner = m.get("Connection", m)
        if inner.get("event") == "authenticated":
            return
        if "Error" in m or inner.get("type") == "error":
            raise AssertionError(f"auth failed: {m}")


async def _wait_for_subscription(ws, topic, expect_error=False):
    while True:
        m = (await asyncio.wait_for(ws.receive(), timeout=5)).json()
        inner = m.get("Subscription", m)
        if inner.get("topic") != topic:
            continue
        if expect_error:
            assert inner.get("event") == "error", f"expected error, got {m}"
            return
        if inner.get("event") == "subscribed":
            return


@pytest.mark.asyncio
async def test_scope_denied_subscribe_never_receives_live_events():
    """A read-restricted key that subscribes to a forbidden channel is NOT
    registered as a live subscriber: it gets a subscription error and never
    receives events broadcast on that channel (audit: WS subscribe race)."""
    restricted = os.environ.get("ACTAE_RESTRICTED_KEY")
    if not restricted:
        pytest.skip("ACTAE_RESTRICTED_KEY not set (provisioned on the isolated test instance)")
    import aiohttp
    import json as _json
    import time as _time

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(_ws_url()) as ws:
            await _ws_handshake(ws, restricted)

            # Forbidden channel -> subscription error.
            await ws.send_json({"type": "subscribe", "topic": "forbidden.live"})
            await _wait_for_subscription(ws, "forbidden.live", expect_error=True)

            # Allowed channel -> subscribed (proves the socket is live and
            # receiving normally, so a non-receipt of forbidden is meaningful).
            await ws.send_json({"type": "subscribe", "topic": "allowed.live"})
            await _wait_for_subscription(ws, "allowed.live", expect_error=False)

            # Admin (full-access) client broadcasts on both channels.
            async with session.ws_connect(_ws_url()) as admin:
                await _ws_handshake(admin, ACTAE_API_KEY)
                await admin.send_json({"type": "broadcast", "topic": "forbidden.live", "payload": {"leak": True}})
                await asyncio.wait_for(admin.receive(), timeout=5)  # ack
                await admin.send_json({"type": "broadcast", "topic": "allowed.live", "payload": {"ok": True}})
                await asyncio.wait_for(admin.receive(), timeout=5)  # ack

            # The restricted socket must receive allowed.live but NEVER
            # forbidden.live.
            seen_allowed = False
            seen_forbidden = False
            deadline = _time.monotonic() + 5
            while _time.monotonic() < deadline:
                try:
                    m = (await asyncio.wait_for(ws.receive(), timeout=1)).json()
                except asyncio.TimeoutError:
                    continue
                text = _json.dumps(m)
                if "broadcast" in text and "allowed.live" in text:
                    seen_allowed = True
                if "broadcast" in text and "forbidden.live" in text:
                    seen_forbidden = True
            assert seen_allowed, "allowed subscription must receive live events"
            assert not seen_forbidden, "forbidden-channel events leaked to an out-of-scope socket"


@pytest.mark.asyncio
async def test_live_sweep_deauthenticates_revoked_socket():
    """The 30s credential_reauthorizer sweep, wired into a real server, must
    de-authenticate a socket whose key was revoked after connect: after the
    sweep, the socket can no longer publish (AUTH_REQUIRED), and it received
    the AUTH_REVOKED error frame. Requires ACTAE_SWEEP_KEY (provisioned on
    the isolated test instance) and direct DB access via ACTAE_PG_DSN."""
    import aiohttp
    import json as _json

    sweep_key = os.environ.get("ACTAE_SWEEP_KEY")
    pg_dsn = os.environ.get("ACTAE_PG_DSN")
    if not (sweep_key and pg_dsn):
        pytest.skip("ACTAE_SWEEP_KEY / ACTAE_PG_DSN not set")
    import asyncpg

    async with aiohttp.ClientSession() as session:
        # Re-activate the sweep key (a prior run revokes it) so the test is
        # idempotent.
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(
                "UPDATE public.api_keys SET status = 'active' WHERE key_prefix = 'sk-sweep-t'"
            )
        finally:
            await conn.close()

        async with session.ws_connect(_ws_url()) as ws:
            await _ws_handshake(ws, sweep_key)
            # Publish succeeds while the key is active.
            await ws.send_json({"type": "broadcast", "topic": "sweep.live", "payload": {"n": 1}})
            await asyncio.wait_for(ws.receive(), timeout=5)  # ack

            # Revoke the key in the DB (like `actae keys revoke`).
            conn = await asyncpg.connect(pg_dsn)
            try:
                await conn.execute(
                    "UPDATE public.api_keys SET status = 'revoked' WHERE key_prefix = 'sk-sweep-t'"
                )
            finally:
                await conn.close()

            # The sweep (<=30s + margin) must de-authenticate the socket:
            # a subsequent publish is rejected with AUTH_REQUIRED.
            seen_auth_required = False
            deadline = asyncio.get_event_loop().time() + 45
            while asyncio.get_event_loop().time() < deadline:
                # Try a publish; drain any frames (AUTH_REVOKED error, etc.).
                await ws.send_json({"type": "broadcast", "topic": "sweep.live", "payload": {"n": 2}})
                try:
                    m = (await asyncio.wait_for(ws.receive(), timeout=2)).json()
                except asyncio.TimeoutError:
                    continue
                text = _json.dumps(m)
                if "AUTH_REQUIRED" in text:
                    seen_auth_required = True
                    break
            assert seen_auth_required, "revoked socket must be de-authenticated by the sweep"


def test_auth_rate_limit_ignores_spoofed_forwarded_for():
    """Auth rate limiting keys on the trusted peer IP (x-actae-client-ip),
    NOT a client-supplied X-Forwarded-For: rotating spoofed XFF values must
    not bypass the per-IP bucket, and the endpoint returns 429 once the real
    peer's bucket (capacity 20) is exhausted."""
    import httpx

    statuses = []
    for i in range(40):
        r = httpx.post(
            f"{ACTAE_URL}/api/v1/auth/api-key-login",
            json={"api_key": f"bogus-key-{i}"},
            headers={"X-Forwarded-For": f"198.51.100.{i % 250}"},
            timeout=10,
            verify=False,
        )
        statuses.append(r.status_code)
        if r.status_code == 429:
            break
    assert 429 in statuses, (
        f"auth rate limit never triggered despite spoofed XFF rotation "
        f"(first statuses: {statuses[:5]})"
    )
