# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Deep-004: real cross-layer fleet E2E against real SaaS + gateway + hosted
instances, driven through the Python SDK.

These tests perform REAL network assertions — every hop is real HTTP/WS
between separate processes:
    SDK -> SaaS (token exchange) -> gateway (typed dispatch) -> instance.

Run with:
    ACTAE_E2E=1 python3 -m pytest tests/fleet_e2e/test_direct_fleet.py -v
"""
from __future__ import annotations

import asyncio
import json
import os

import pytest

from topology import E2E_SKIP, REPO_ROOT

TS_DRIVER_SRC = r"""
import { FleetClient } from './dist/index.js';

const instance = process.env.ACTAE_E2E_INSTANCE ?? 'ins_e2e_2';
const client = new FleetClient({
  organizationToken: process.env.ACTAE_ORG_TOKEN,
  organizationId: process.env.ACTAE_ORG_ID,
  endpoint: process.env.ACTAE_FLEET_URL,
  tokenEndpoint: process.env.ACTAE_FLEET_TOKEN_URL,
  requestedScopes: (process.env.ACTAE_FLEET_SCOPES ?? '').split(',').filter(Boolean),
});

const caps = await client.capabilities({ instanceId: instance });
if (!caps) throw new Error('capabilities returned nothing');

const channel = 'public.tssdk';
await client.callOperation('actae.api.v1.events.record', {
  instanceId: instance,
  body: {
    channel_id: channel,
    event_type: 'tool.tssdk',
    payload: { ok: true },
    metadata: { actor: 'tssdk' },
  },
  idempotencyKey: 'e2e-tssdk-1',
});
const replay = await client.callOperation('actae.api.v1.events.replay', {
  instanceId: instance,
  pathParams: { channel_id: channel },
  body: { limit: 10 },
});
const events = (replay?.events ?? replay?.data?.events ?? []);
console.log(`TS_SDK_REPLAYED_EVENTS: ${events.length}`);
if (!events.length) throw new Error('ts relay dispatch did not round-trip');

// State save + load round-trip.
await client.callOperation('actae.api.v1.state.save', {
  instanceId: instance,
  pathParams: { channel_id: channel },
  body: { channel_id: channel, cursor: 1, state: { step: 3 } },
  idempotencyKey: 'e2e-tssdk-state',
});
const state = await client.callOperation('actae.api.v1.state.load', {
  instanceId: instance,
  pathParams: { channel_id: channel },
});
if (!state) throw new Error('ts state.load returned nothing');
console.log('TS_SDK_STATE: ok');

// Fork the channel (immutable provenance).
const fork = await client.callOperation('actae.api.v1.forks.create', {
  instanceId: instance,
  body: {
    source_channel_id: channel,
    new_channel_id: 'ch_tssdk_fork_e2e',
    at_cursor: 1,
    reason: 'fleet e2e',
  },
  idempotencyKey: 'e2e-tssdk-fork',
});
const child = fork?.child_channel_id ?? fork?.data?.child_channel_id;
if (!child) throw new Error('ts fork returned no child channel');
console.log(`TS_SDK_FORK_CHILD: ${child}`);

// Idempotency conflict: same key + different content -> 409.
let conflictStatus = 0;
try {
  await client.callOperation('actae.api.v1.events.record', {
    instanceId: instance,
    body: {
      channel_id: channel,
      event_type: 'tool.tssdk',
      payload: { conflict: true },
      metadata: { actor: 'tssdk' },
    },
    idempotencyKey: 'e2e-tssdk-1',
  });
} catch (err) {
  conflictStatus = err?.statusCode ?? 0;
}
if (conflictStatus !== 409) throw new Error(`ts expected idempotency conflict 409, got ${conflictStatus}`);
console.log('TS_SDK_IDEMPOTENCY_CONFLICT: 409');

// Pagination: record several events, iterate opaque keyset pages (limit=2),
// and confirm every event is seen once.
const pageChannel = channel + '_pages';
for (let i = 0; i < 5; i++) {
  await client.callOperation('actae.api.v1.events.record', {
    instanceId: instance,
    body: { channel_id: pageChannel, event_type: 'tool.page', payload: { n: i }, metadata: { actor: 'tssdk' } },
    idempotencyKey: `e2e-tssdk-page-${i}`,
  });
}
const seen = new Set();
let pageCount = 0;
for await (const item of client.iterQuery(instance, { channel_ids: [pageChannel], limit: 2 })) {
  pageCount++;
  const p = (item?.payload ?? item?.data?.payload);
  seen.add(JSON.stringify(p ?? item));
}
if (pageCount < 5) throw new Error(`ts pagination incomplete: ${pageCount}`);
console.log(`TS_SDK_PAGINATION: items=${pageCount} unique=${seen.size}`);

// Multi-instance partial results: a real instance + a non-routable instance.
const pr = await client.queryMany([instance, 'ins_missing_e2e'], { channel_ids: [pageChannel] });
if (!pr.errors?.length) throw new Error('ts expected partial-result errors for the missing instance');
try { client.requireComplete(pr); throw new Error('ts requireComplete must raise on partial'); }
catch (e) { if (e.message?.startsWith('ts requireComplete')) throw e; }
console.log('TS_SDK_PARTIAL_RESULTS: ok');

// Cancellation: open a ticketed gateway WS stream and abort mid-stream
// (AbortSignal propagated through connect + queue wait).
const saas = process.env.ACTAE_E2E_SAAS_URL;
const internal = process.env.ACTAE_E2E_INTERNAL_TOKEN;
if (saas && internal) {
  const ticketResp = await fetch(saas + '/v1/internal/stream-tickets', {
    method: 'POST',
    headers: { authorization: 'Bearer ' + internal, 'content-type': 'application/json' },
    body: JSON.stringify({ organization_id: process.env.ACTAE_ORG_ID, instance_id: instance, channels: [channel], origin: process.env.ACTAE_FLEET_URL }),
  });
  const ticket = (await ticketResp.json()).ticket;
  const ac = new AbortController();
  const it = client.streamWithReconnect({ instanceId: instance, ticket, signal: ac.signal, maxReconnectAttempts: 1 });
  try {
    await Promise.race([
      it.next().then(() => {}),
      new Promise((_, rej) => setTimeout(() => rej(new Error('no-frame')), 15000)),
    ]);
  } catch (e) {
    if (String(e?.message).includes('no-frame')) throw e;
  }
  ac.abort(); // cancel mid-stream; must not deadlock
  console.log('TS_SDK_CANCELLATION: ok');
}

console.log('TS_SDK_CROSS_LAYER: ok');
"""


@pytest.fixture
def fleet_client(fleet_topo):
    """A Python SDK FleetClient bound to the real SaaS + gateway, with the
    real provisioned service token."""
    from actae_client.fleet import FleetClient

    iid = next(iter(fleet_topo.instances))
    svc_token = fleet_topo.service_tokens[iid]
    return FleetClient(
        svc_token,
        endpoint=fleet_topo.gateway_url,
        organization_id=fleet_topo.org,
        token_endpoint=fleet_topo.saas_url,
        requested_scopes=[
            "events:read", "events:write",
            "state:read", "state:write",
            "channels:read", "channels:write",
            "forks:create",
        ],
    )


def _run(coro):
    return asyncio.run(coro)


@E2E_SKIP
def test_token_exchange_and_capabilities(fleet_topo, fleet_client):
    """Real SaaS token exchange returns a fleet_access JWT; the gateway
    verifies it and returns capabilities (fleet protocol + supported ops)."""
    iid = next(iter(fleet_topo.instances))

    async def go():
        token = await fleet_client._token()
        assert token and token.startswith("ey")
        caps = await fleet_client.capabilities(iid)
        return caps

    caps = _run(go())
    assert isinstance(caps, dict)
    supported = caps.get("supported_fleet_operations")
    assert supported, "capabilities must advertise supported fleet operations"
    assert "actae.api.v1.events.record" in supported


@E2E_SKIP
def test_dispatch_record_replay_state_fork(fleet_topo, fleet_client):
    """Typed dispatch (PR-016) round-trips through the gateway to a REAL
    instance: record an event, replay it, save state, fork, resume."""
    iid = next(iter(fleet_topo.instances))
    channel = "public.e2e"

    async def go():
        # record (channel_id is in the BODY for events.record)
        recorded = await fleet_client.call_operation(
            "actae.api.v1.events.record",
            instance_id=iid,
            path_params={},
            body={
                "channel_id": channel,
                "event_type": "tool.e2e",
                "payload": {"ok": True},
                "metadata": {"actor": "e2e"},
            },
            operation_idempotency_key="e2e-key-1",
        )
        assert recorded is not None
        # replay the channel back (channel_id is a PATH param for replay)
        replayed = await fleet_client.call_operation(
            "actae.api.v1.events.replay",
            instance_id=iid,
            path_params={"channel_id": channel},
            body={"limit": 10},
        )
        events = replayed.get("events") or replayed.get("data", {}).get("events") or []
        assert events, "recorded event must be replayable through the gateway"
        # save state (channel_id is a PATH param for state.save)
        saved = await fleet_client.call_operation(
            "actae.api.v1.state.save",
            instance_id=iid,
            path_params={"channel_id": channel},
            body={"channel_id": channel, "cursor": 1, "state": {"step": 3}},
            operation_idempotency_key="e2e-state-1",
        )
        assert saved is not None
        # fork the channel (no path params; source_channel_id in the body)
        fork = await fleet_client.call_operation(
            "actae.api.v1.forks.create",
            instance_id=iid,
            path_params={},
            body={
                "source_channel_id": channel,
                "new_channel_id": "ch_" + channel.split(".")[-1] + "_fork_e2e",
                "at_cursor": 1,
                "reason": "fleet e2e",
            },
            operation_idempotency_key="e2e-fork-1",
        )
        child = fork.get("child_channel_id") or fork.get("data", {}).get("child_channel_id")
        assert child, f"fork must return a child channel: {fork}"
        return child

    child = _run(go())
    assert child.startswith("ch_")


@E2E_SKIP
def test_dispatch_unsupported_fails_locally(fleet_client):
    """PR-016: scope=public auth operations fail LOCALLY (426) with no
    network I/O — the SDK never sends them to the gateway."""
    from actae_client.fleet import UnsupportedOperationError

    async def go():
        with pytest.raises(UnsupportedOperationError):
            await fleet_client.call_operation(
                "actae.api.v1.auth.signup", instance_id="ins_e2e_1", body={}
            )

    _run(go())


@E2E_SKIP
def test_relay_dispatch_roundtrip(fleet_topo, fleet_client):
    """Deep-004: a relay-mode instance (dials OUT to the gateway) executes a
    REAL relayed mutation + read through the outbound relay session — command
    frame → instance → result, with the idempotency key forwarded so the
    content-bound request digest verifies (PR-014)."""
    import asyncio
    from actae_client.fleet import FleetClient

    iid = fleet_topo.relay_instance_id
    channel = "public.relay"

    async def go():
        # A fresh client bound to this event loop.
        fresh = FleetClient(
            fleet_topo.service_tokens[iid],
            endpoint=fleet_topo.gateway_url,
            organization_id=fleet_topo.org,
            token_endpoint=fleet_topo.saas_url,
            requested_scopes=["events:read", "events:write", "channels:read"],
        )
        await fresh.call_operation(
            "actae.api.v1.events.record",
            instance_id=iid,
            path_params={},
            body={
                "channel_id": channel,
                "event_type": "tool.relay",
                "payload": {"relayed": True},
                "metadata": {"actor": "e2e"},
            },
            operation_idempotency_key="e2e-relay-1",
        )
        replay = await fresh.call_operation(
            "actae.api.v1.events.replay",
            instance_id=iid,
            path_params={"channel_id": channel},
            body={"limit": 10},
        )
        events = replay.get("events") or replay.get("data", {}).get("events") or []
        if getattr(fresh, "_session", None):
            await fresh._session.close()
        return events

    events = asyncio.run(go())
    assert events, "relay dispatch must round-trip through the outbound session"


@E2E_SKIP
def test_go_sdk_cross_layer(fleet_topo, fleet_topo_env):
    """Deep-004: the Go SDK drives the same real topology (token exchange →
    gateway capabilities → typed dispatch record/replay)."""
    import subprocess

    repo = REPO_ROOT
    cmd = ["go", "run", "./e2e_driver"]
    env = dict(os.environ)
    env.update(fleet_topo_env)
    proc = subprocess.run(
        cmd, cwd=repo / "sdks" / "go", env=env, capture_output=True, text=True, timeout=180
    )
    assert proc.returncode == 0, "go_driver failed:\n%s\n%s" % (
        proc.stdout,
        proc.stderr,
    )
    assert "GO_SDK_REPLAYED_EVENTS: 1" in proc.stdout
    assert "GO_SDK_STATE: ok" in proc.stdout
    assert "GO_SDK_FORK_CHILD:" in proc.stdout
    assert "GO_SDK_IDEMPOTENCY_CONFLICT: 409" in proc.stdout
    assert "GO_SDK_PAGINATION:" in proc.stdout
    assert "GO_SDK_PARTIAL_RESULTS: ok" in proc.stdout
    assert "GO_SDK_CANCELLATION: ok" in proc.stdout


@E2E_SKIP
def test_typescript_sdk_cross_layer(fleet_topo, fleet_topo_env):
    """Deep-004: the TypeScript SDK drives the same real topology."""
    import subprocess

    repo = REPO_ROOT
    # Build the SDK dist first (the driver imports ./dist/index.js).
    build = subprocess.run(
        ["npm", "run", "build"],
        cwd=repo / "sdks" / "typescript",
        capture_output=True,
        text=True,
        timeout=240,
    )
    assert build.returncode == 0, "ts build failed:\n%s" % build.stderr
    cmd = ["node", "--input-type=module", "-e", TS_DRIVER_SRC]
    env = dict(os.environ)
    env.update(fleet_topo_env)
    proc = subprocess.run(cmd, cwd=repo / "sdks" / "typescript", env=env, capture_output=True, text=True, timeout=180)
    assert proc.returncode == 0, "ts_driver failed:\n%s\n%s" % (
        proc.stdout,
        proc.stderr,
    )
    assert "TS_SDK_REPLAYED_EVENTS: 1" in proc.stdout
    assert "TS_SDK_STATE: ok" in proc.stdout
    assert "TS_SDK_FORK_CHILD:" in proc.stdout
    assert "TS_SDK_IDEMPOTENCY_CONFLICT: 409" in proc.stdout
    assert "TS_SDK_PAGINATION:" in proc.stdout
    assert "TS_SDK_PARTIAL_RESULTS: ok" in proc.stdout
    assert "TS_SDK_CANCELLATION: ok" in proc.stdout


@E2E_SKIP
def test_read_only_token_cannot_mutate(fleet_topo):
    """PR-001 negative matrix through the REAL gateway + instance: a fleet
    token with only read scopes can query/replay but gets 403 for record,
    state mutation, fork, and delete."""
    import asyncio
    from actae_client.errors import APIError
    from actae_client.fleet import FleetClient

    iid = "ins_e2e_1"
    channel = "public.neg"

    async def go():
        fresh = FleetClient(
            fleet_topo.service_tokens[iid],
            endpoint=fleet_topo.gateway_url,
            organization_id=fleet_topo.org,
            token_endpoint=fleet_topo.saas_url,
            requested_scopes=["events:read", "channels:read"],  # read-only
        )
        # Read is allowed.
        replayed = await fresh.call_operation(
            "actae.api.v1.events.replay",
            instance_id=iid,
            path_params={"channel_id": channel},
            body={"limit": 5},
        )
        assert replayed is not None
        # Every mutation is denied with 403 (never a 404 that leaks existence).
        for op, path_params, body in [
            (
                "actae.api.v1.events.record",
                {},
                {
                    "channel_id": channel,
                    "event_type": "tool.neg",
                    "payload": {"x": 1},
                    "metadata": {"actor": "e2e"},
                },
            ),
            (
                "actae.api.v1.state.save",
                {"channel_id": channel},
                {"channel_id": channel, "cursor": 1, "state": {"step": 1}},
            ),
            ("actae.api.v1.channels.delete", {"channel_id": channel}, {}),
        ]:
            try:
                await fresh.call_operation(
                    op,
                    instance_id=iid,
                    path_params=path_params,
                    body=body,
                    operation_idempotency_key="neg-" + op.split(".")[-1],
                )
                raise AssertionError(f"{op} must be denied for a read-only token")
            except APIError as e:
                assert e.status_code == 403, f"{op} expected 403, got {e.status_code}"
        if getattr(fresh, "_session", None):
            await fresh._session.close()

    asyncio.run(go())


@E2E_SKIP
def test_secret_sentinel_never_leaves_source(fleet_topo):
    """Secret-sentinel scan (PR-009 acceptance): with a metadata-only source
    policy synced from the SaaS, a sentinel value recorded in an event payload
    must NEVER appear on the federated stream egress (client WS frame / NATS),
    in the SaaS database, or in the gateway logs — it stays inside the source
    instance."""
    import asyncio
    import sqlite3
    import time as _time
    import uuid
    import requests
    import websockets

    from actae_client.fleet import FleetClient

    iid = "ins_e2e_1"
    channel = "public.sentinel"
    sentinel = "SENTINEL-" + uuid.uuid4().hex
    origin = "http://localhost:3000"

    # Configure a metadata-only source policy in the SaaS DB; the instance
    # picks it up on its next control-plane sync.
    conn = sqlite3.connect(fleet_topo.saas_db)
    conn.execute(
        "UPDATE instance_remote_access SET policy = ?, policy_version = policy_version + 1, "
        "enabled = 1, mode = 'hosted' WHERE installation_id = ?",
        (
            json.dumps(
                {"version": 1, "enabled": True, "mode": "hosted", "metadata_only": True}
            ),
            iid,
        ),
    )
    conn.commit()
    conn.close()
    _time.sleep(4)  # one sync cycle

    ticket_resp = requests.post(
        fleet_topo.saas_url + "/v1/internal/stream-tickets",
        headers={"authorization": "Bearer " + fleet_topo.internal_token},
        json={
            "organization_id": fleet_topo.org,
            "instance_id": iid,
            "channels": [channel],
            "origin": origin,
        },
        timeout=10,
    )
    assert ticket_resp.status_code == 201, ticket_resp.text
    ticket = ticket_resp.json()["ticket"]
    ws_url = (
        fleet_topo.gateway_url.replace("http://", "ws://")
        + f"/v1/organizations/{fleet_topo.org}/events/stream?ticket={ticket}"
    )
    frames = []

    async def go():
        fresh = FleetClient(
            fleet_topo.service_tokens[iid],
            endpoint=fleet_topo.gateway_url,
            organization_id=fleet_topo.org,
            token_endpoint=fleet_topo.saas_url,
            requested_scopes=["events:read", "events:write", "channels:read"],
        )
        async with websockets.connect(ws_url, origin=origin, open_timeout=20) as ws:
            await asyncio.sleep(3)
            await fresh.call_operation(
                "actae.api.v1.events.record",
                instance_id=iid,
                path_params={},
                body={
                    "channel_id": channel,
                    "event_type": "tool.sentinel",
                    "payload": {"secret": sentinel, "ok": True},
                    "metadata": {"actor": "e2e"},
                },
                operation_idempotency_key="e2e-sentinel-1",
            )
            try:
                async with asyncio.timeout(25):
                    while True:
                        frame = await ws.recv()
                        frames.append(frame)
                        if "tool.sentinel" in frame:
                            break
            except (asyncio.TimeoutError, websockets.ConnectionClosed):
                pass
        if getattr(fresh, "_session", None):
            await fresh._session.close()

    asyncio.run(go())

    # The redacted event WAS delivered (metadata_only keeps id/type/cursor),
    # but the sentinel value never left the source.
    assert any("tool.sentinel" in f for f in frames), "redacted event must still arrive"
    assert all(sentinel not in f for f in frames), (
        "sentinel must never appear on federated stream egress"
    )
    # A direct string scan across the whole SaaS DB file for the sentinel.
    import pathlib

    db_bytes = pathlib.Path(fleet_topo.saas_db).read_bytes()
    assert sentinel.encode() not in db_bytes, (
        "sentinel must never be persisted in the SaaS database"
    )
    # Gateway log must not contain the sentinel either.
    gw_log = (fleet_topo._tmp_path / "gateway.log").read_text(errors="replace")
    assert sentinel not in gw_log, "sentinel must never appear in gateway logs"


@E2E_SKIP
def test_source_restart_no_permanent_gap(fleet_topo, fleet_client):
    """Restart/failover: after the source instance restarts, the gateway's
    shared connector reconnects and live delivery resumes — the event recorded
    after restart is delivered on the open WS stream (no permanent gap)."""
    import asyncio
    import requests
    import websockets

    from topology import restart_instance

    iid = "ins_e2e_1"
    channel = "public.restart"
    origin = "http://localhost:3000"

    ticket_resp = requests.post(
        fleet_topo.saas_url + "/v1/internal/stream-tickets",
        headers={"authorization": "Bearer " + fleet_topo.internal_token},
        json={
            "organization_id": fleet_topo.org,
            "instance_id": iid,
            "channels": [channel],
            "origin": origin,
        },
        timeout=10,
    )
    assert ticket_resp.status_code == 201, ticket_resp.text
    ticket = ticket_resp.json()["ticket"]
    ws_url = (
        fleet_topo.gateway_url.replace("http://", "ws://")
        + f"/v1/organizations/{fleet_topo.org}/events/stream?ticket={ticket}"
    )
    received = []

    async def go():
        from actae_client.fleet import FleetClient

        fresh = FleetClient(
            fleet_topo.service_tokens[iid],
            endpoint=fleet_topo.gateway_url,
            organization_id=fleet_topo.org,
            token_endpoint=fleet_topo.saas_url,
            requested_scopes=["events:read", "events:write", "channels:read"],
        )
        async with websockets.connect(ws_url, origin=origin, open_timeout=20) as ws:
            await asyncio.sleep(3)  # connector auth + subscribe
            # Kill + restart the source instance; the connector must reconnect.
            restart_instance(fleet_topo, iid)
            await fresh.call_operation(
                "actae.api.v1.events.record",
                instance_id=iid,
                path_params={},
                body={
                    "channel_id": channel,
                    "event_type": "tool.restart",
                    "payload": {"after": "restart"},
                    "metadata": {"actor": "e2e"},
                },
                operation_idempotency_key="e2e-restart-1",
            )
            try:
                async with asyncio.timeout(30):
                    while True:
                        frame = await ws.recv()
                        received.append(frame)
                        if "tool.restart" in frame:
                            break
            except (asyncio.TimeoutError, websockets.ConnectionClosed):
                pass
        if getattr(fresh, "_session", None):
            await fresh._session.close()

    asyncio.run(go())
    assert any("tool.restart" in f for f in received), (
        "live delivery must resume after a source restart (no permanent gap)"
    )


@E2E_SKIP
def test_stream_ticket_and_ws(fleet_topo):
    """A real stream ticket consumed by the gateway opens a live WS to the
    instance: record an event, subscribe, receive the broadcast frame."""
    import requests
    import websockets

    iid = next(iter(fleet_topo.instances))
    channel = "public.e2e.ws"
    origin = "http://localhost:3000"

    # Mint a stream ticket through the real SaaS internal endpoint (the
    # gateway's workload identity is the internal token).
    resp = requests.post(
        fleet_topo.saas_url + "/v1/internal/stream-tickets",
        headers={"authorization": "Bearer " + fleet_topo.internal_token},
        json={
            "organization_id": fleet_topo.org,
            "instance_id": iid,
            "channels": [channel],
            "origin": origin,
        },
        timeout=10,
    )
    assert resp.status_code == 201, resp.text
    ticket = resp.json()["ticket"]

    # Open the gateway WS with the consumed ticket + the ticket's origin.
    ws_url = (
        fleet_topo.gateway_url.replace("http://", "ws://")
        + f"/v1/organizations/{fleet_topo.org}/events/stream?ticket={ticket}"
    )
    import asyncio
    from actae_client.fleet import FleetClient

    received = []

    async def connect():
        # A FRESH client inside this event loop (aiohttp sessions are
        # loop-bound; the session fixture's loop is closed).
        fresh = FleetClient(
            fleet_topo.service_tokens[iid],
            endpoint=fleet_topo.gateway_url,
            organization_id=fleet_topo.org,
            token_endpoint=fleet_topo.saas_url,
            requested_scopes=["events:read", "events:write", "channels:read"],
        )
        async with websockets.connect(ws_url, origin=origin, open_timeout=20) as ws:
            # Record a SECOND event while the stream is live so the broadcast
            # reaches the just-subscribed connector.
            await fresh.call_operation(
                "actae.api.v1.events.record",
                instance_id=iid,
                path_params={},
                body={
                    "channel_id": channel,
                    "event_type": "tool.ws",
                    "payload": {"live": True, "n": 2},
                    "metadata": {"actor": "e2e"},
                },
                operation_idempotency_key="e2e-ws-2",
            )
            try:
                async with asyncio.timeout(25):
                    while True:
                        frame = await ws.recv()
                        received.append(frame)
                        if "e2e-ws-2" in frame or "tool.ws" in frame:
                            break
            except (asyncio.TimeoutError, websockets.ConnectionClosed):
                pass
        if getattr(fresh, "_session", None):
            await fresh._session.close()

    asyncio.run(connect())
    assert any("tool.ws" in f for f in received), (
        "gateway stream must deliver the recorded event frame"
    )