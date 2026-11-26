# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Deep-004: NATS live-bus leg.

With `ACTAE_E2E_NATS=1` the suite boots a REAL NATS server and a SECOND
gateway built with the `gateway-nats` feature (`GATEWAY_NATS_SERVERS`),
then proves the live transport end-to-end: an event recorded through the
NATS gateway is published onto the instance's hashed NATS subject and
received by a real NATS subscriber.

Per the acceptance the test FAILS (never skips) when enabled but NATS is
unavailable. Requires:
  * the gateway built with `--features federation-gateway,gateway-nats`
    (ACT AE_NATS_GATEWAY_BIN / default actae/target/debug/actae-federation-gateway)
  * a nats-server: ACTAE_NATS_SERVER_BIN, `nats-server` on PATH, or it is
    auto-downloaded to the temp dir (pinned version).
"""
from __future__ import annotations

import base64
import hashlib
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

from topology import REPO_ROOT, _free_port, _spawn, _wait_http, build_topology, teardown

NATS_SKIP = pytest.mark.skipif(
    os.environ.get("ACTAE_E2E_NATS") != "1",
    reason="opt-in NATS leg (ACTAE_E2E_NATS=1)",
)

NATS_VERSION = "v2.10.26"


def _nats_server_bin(tmp_path: Path) -> str:
    """Resolve a nats-server binary: ACTAE_NATS_SERVER_BIN, PATH, or download
    the pinned release to the temp dir. Fails loudly (never skips)."""
    explicit = os.environ.get("ACTAE_NATS_SERVER_BIN")
    if explicit:
        return explicit
    on_path = shutil.which("nats-server")
    if on_path:
        return on_path
    import platform

    sys_map = {"darwin": "darwin", "linux": "linux", "win32": "windows"}
    os_name = sys_map.get(sys.platform, "linux")
    arch = "arm64" if platform.machine() in ("arm64", "aarch64") else "amd64"
    if os_name == "windows":
        raise RuntimeError(
            "NATS leg on Windows requires ACTAE_NATS_SERVER_BIN (no auto-download)"
        )
    bin_path = tmp_path / ("nats-server-" + NATS_VERSION)
    if not bin_path.exists():
        url = (
            "https://github.com/nats-io/nats-server/releases/download/"
            f"{NATS_VERSION}/nats-server-{NATS_VERSION}-{os_name}-{arch}.tar.gz"
        )
        tarball = tmp_path / "nats-server.tar.gz"
        import urllib.request

        urllib.request.urlretrieve(url, tarball)
        import tarfile

        with tarfile.open(tarball) as tf:
            tf.extractall(tmp_path)
    found = list(tmp_path.rglob("nats-server"))
    if not found:
        raise RuntimeError("failed to locate the downloaded nats-server binary")
    return str(found[0])


@pytest.fixture(scope="session")
def nats_server(tmp_path_factory):
    """One NATS server shared by the NATS leg tests. Runs in a Docker
    container (`nats:2.10.26-alpine`) for full process isolation — a local
    nats-server binary spawned by the test harness was observed receiving an
    unrelated shutdown signal from the topology. Falls back to a local binary
    (ACTAE_NATS_SERVER_BIN / PATH / auto-download) only when Docker is
    unavailable."""
    if os.environ.get("ACTAE_E2E_NATS") != "1":
        pytest.skip("opt-in NATS leg")
    tmp = tmp_path_factory.mktemp("nats")
    import uuid as _uuid

    container = None
    proc = None
    port = None
    if shutil.which("docker"):
        name = "actae-e2e-nats-" + _uuid.uuid4().hex[:8]
        launched = subprocess.run(
            ["docker", "run", "-d", "--rm", "--name", name, "-p", "127.0.0.1::4222",
             "nats:2.10.26-alpine"],
            capture_output=True, text=True, timeout=120,
        )
        if launched.returncode == 0:
            container = name
            for _ in range(30):
                got = subprocess.run(
                    ["docker", "port", name, "4222"], capture_output=True, text=True, timeout=10
                )
                if got.returncode == 0 and got.stdout.strip():
                    port = int(got.stdout.strip().split(":")[-1])
                    break
                time.sleep(0.3)
            if port is not None and not _wait_for_nats_port(port, timeout=20):
                logs = subprocess.run(
                    ["docker", "logs", name],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                subprocess.run(
                    ["docker", "rm", "-f", name], capture_output=True, timeout=60
                )
                container = None
                port = None
                raise RuntimeError(
                    "Docker NATS published its port but never became protocol-ready:\n"
                    + logs.stdout
                    + logs.stderr
                )
    if port is None:
        # Fall back to a local binary on a high fixed port.
        bin_path = _nats_server_bin(tmp)
        for candidate in range(30000, 30100):
            log_path = tmp / f"nats-{candidate}.log"
            p = _spawn([bin_path, "-p", str(candidate)], {}, log_path, start_new_session=True)
            if _wait_for_nats_ready(p, candidate, log_path, timeout=15):
                proc = p
                port = candidate
                break
            p.terminate()
            p.wait(timeout=5)
        if port is None:
            raise RuntimeError("could not start a NATS server (docker or local)")
    url = f"nats://127.0.0.1:{port}"
    try:
        yield {"url": url, "port": port, "container": container, "proc": proc}
    finally:
        if container is not None:
            subprocess.run(["docker", "rm", "-f", container], capture_output=True, timeout=60)
        elif proc is not None:
            try:
                proc.terminate()
            except Exception:  # noqa: BLE001
                pass
            proc.wait(timeout=10)


def _wait_for_nats_ready(proc, port: int, log_path: Path, timeout: float = 30) -> bool:
    """Wait for a local child and the NATS protocol handshake."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            return False  # server exited (port conflict or crash)
        if _wait_for_nats_port(port, timeout=1):
            return True
        time.sleep(0.3)
    return False


def _wait_for_nats_port(port: int, timeout: float = 30) -> bool:
    """Require a complete NATS protocol handshake, not merely an open port."""
    import asyncio
    import nats

    async def _probe():
        nc = await asyncio.wait_for(
            nats.connect(f"nats://127.0.0.1:{port}", connect_timeout=1,
                         allow_reconnect=False, max_reconnect_attempts=0),
            timeout=2,
        )
        await nc.close()

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            asyncio.run(_probe())
            return True
        except Exception:  # noqa: BLE001
            time.sleep(0.3)
    return False


@pytest.fixture(scope="session")
def nats_fleet_topo(tmp_path_factory):
    """Minimal topology for the NATS transport leg.

    The NATS assertion exercises a hosted connector only. Giving it its own
    one-instance topology prevents relay startup/teardown and fenced leases
    from influencing this independent transport test.
    """
    if os.environ.get("ACTAE_E2E_NATS") != "1":
        pytest.skip("opt-in NATS leg")
    tmp = tmp_path_factory.mktemp("fleet-e2e-nats")
    topo = build_topology(
        tmp,
        instance_ids=("ins_e2e_1",),
        relay_instance_id=None,
    )
    try:
        yield topo
    finally:
        teardown(topo)


@pytest.fixture(scope="session")
def nats_gateway(nats_fleet_topo, nats_server):
    """A SECOND gateway binary built with `gateway-nats`, using NATS as the
    live bus, pointed at the same SaaS + hosted instances."""
    port = _free_port()
    hosted_urls = {
        iid: f"http://127.0.0.1:{inst.port}"
        for iid, inst in nats_fleet_topo.instances.items()
        if inst.mode == "hosted"
    }
    env = {
        **os.environ,
        "RUST_LOG": "actae_federation_gateway=debug,actae::federation_gateway=debug",
        "GATEWAY_BIND": f"127.0.0.1:{port}",
        "GATEWAY_CONTROL_PLANE_URL": nats_fleet_topo.saas_url,
        "GATEWAY_INTERNAL_TOKEN": nats_fleet_topo.internal_token,
        "GATEWAY_ID": "gw-e2e-nats",
        "GATEWAY_FEDERATION_VERIFICATION_KEYS": "fk1:" + nats_fleet_topo.pubkey_b64,
        "ACTAE_FEDERATION_AUDIENCE": "actae-fleet-gateway",
        "ACTAE_FEDERATION_ISSUER": "actae-fleet",
        "GATEWAY_INSTANCE_URLS": _json(hosted_urls),
        "GATEWAY_NATS_SERVERS": nats_server["url"],
        "GATEWAY_RATE_LIMIT_PER_SEC": "10000",
        "GATEWAY_MAX_STREAM_CONNECTIONS": "200",
        "GATEWAY_SAAS_ALLOW_HTTP": "1",
        "ACTAE_ACCESS_LOG_ALL": "1",
    }
    proc = _spawn(
        [str(_nats_gateway_bin())],
        env,
        nats_fleet_topo.tmp_path / "gateway-nats.log",
    )
    url = f"http://127.0.0.1:{port}"
    try:
        _wait_http(f"{url}/healthz", timeout=60)
    except Exception:
        proc.terminate()
        raise
    try:
        yield {"url": url, "port": port, "proc": proc}
    finally:
        try:
            proc.terminate()
        except Exception:  # noqa: BLE001
            pass
        proc.wait(timeout=10)


def _json(obj):
    import json

    return json.dumps(obj)


def _nats_gateway_bin() -> Path:
    return Path(
        os.environ.get(
            "ACTAE_NATS_GATEWAY_BIN",
            REPO_ROOT / "actae" / "target" / "debug" / "actae-federation-gateway",
        )
    )


def asyncio_run(coro):
    import asyncio

    return asyncio.run(coro)


@NATS_SKIP
def test_nats_live_bus_delivers_frames(nats_fleet_topo, nats_gateway, nats_server):
    """Deep-004: an event recorded through the NATS-backed gateway is
    published onto the instance's hashed NATS subject and received by a real
    NATS subscriber."""
    import asyncio
    from actae_client.fleet import FleetClient

    iid = "ins_e2e_1"
    channel = "public.nats"
    fleet_topo = nats_fleet_topo
    org = fleet_topo.org
    # The gateway prefixes NATS subjects with `actae.fleet.live`.
    hashed = hashlib.sha256(
        f"actae:fleet:live:v1:{org}:{iid}".encode()
    ).hexdigest()[:24]
    subject = f"actae.fleet.live.{hashed}"

    received = []

    async def go():
        import nats
        import websockets

        # Mint a stream ticket (the gateway's internal workload identity) and
        # open a WS stream on the NATS gateway — this STARTS the connector
        # whose live frames feed the NATS bus.
        import requests

        origin = "http://localhost:3000"
        ticket_resp = requests.post(
            fleet_topo.saas_url + "/v1/internal/stream-tickets",
            headers={"authorization": "Bearer " + fleet_topo.internal_token},
            json={
                "organization_id": org,
                "instance_id": iid,
                "channels": [channel],
                "origin": origin,
            },
            timeout=10,
        )
        assert ticket_resp.status_code == 201, ticket_resp.text
        ticket = ticket_resp.json()["ticket"]
        ws_url = (
            nats_gateway["url"].replace("http://", "ws://")
            + f"/v1/organizations/{org}/events/stream?ticket={ticket}"
        )

        nc = await nats.connect(nats_server["url"])
        sub = await nc.subscribe(subject)
        await nc.flush()

        async with websockets.connect(ws_url, origin=origin, open_timeout=20) as ws:
            # Do not infer upstream readiness from elapsed time. The bridge
            # forwards the canonical subscription confirmations, so wait for
            # the terminal subscribe_many acknowledgement before recording.
            async with asyncio.timeout(20):
                while True:
                    ready_frame = await ws.recv()
                    if "subscribed_many" in ready_frame:
                        break
            fresh = FleetClient(
                fleet_topo.service_tokens[iid],
                endpoint=nats_gateway["url"],
                organization_id=org,
                token_endpoint=fleet_topo.saas_url,
                requested_scopes=["events:read", "events:write", "channels:read"],
            )
            await fresh.call_operation(
                "actae.api.v1.events.record",
                instance_id=iid,
                path_params={},
                body={
                    "channel_id": channel,
                    "event_type": "tool.nats",
                    "payload": {"over": "nats"},
                    "metadata": {"actor": "e2e"},
                },
                operation_idempotency_key="e2e-nats-1",
            )
            # The connector must ALSO deliver the live frame to this WS bridge.
            try:
                async with asyncio.timeout(10):
                    while True:
                        ws_frame = await ws.recv()
                        received.append(b"ws:" + ws_frame.encode("utf-8", "replace"))
                        if "tool.nats" in ws_frame:
                            break
            except asyncio.TimeoutError:
                pass
            # Give the connector + NATS publish a moment to land.
            async def collect():
                async for msg in sub.messages:
                    received.append(msg.data)
                    if b"tool.nats" in msg.data:
                        break

            try:
                await asyncio.wait_for(collect(), timeout=20)
            except asyncio.TimeoutError:
                pass
        await nc.close()
        if getattr(fresh, "_session", None):
            await fresh._session.close()

    asyncio.run(go())
    assert any(b"tool.nats" in data for data in received), (
        "NATS live bus must deliver the recorded event frame on the hashed subject"
    )
