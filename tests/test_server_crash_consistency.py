# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Server-kill chaos consistency — SIGKILL Actae mid-run, restart, verify.

The worker-crash suites (`examples/reliability`) kill the *worker*; this opt-in
suite kills the **server process** (`SIGKILL`, no graceful shutdown) and proves
that committed state survives a hard restart: acknowledged events persist,
per-channel cursors stay gapless, the tamper-evident hash chain still verifies,
and an atomic transition's event + snapshot remain consistent.

It boots its own managed-storage `actae` instance (no external server needed), like
the lifecycle suite it reuses. Gated on `ACTAE_SERVER_CRASH=1` (needs the
`actae` binary and the embedded PostgreSQL binaries / cache).

    ACTAE_SERVER_CRASH=1 python3 -m pytest tests/test_server_crash_consistency.py -v
"""

import threading
import time
import uuid

import httpx
import pytest

from tests.test_dev_lifecycle import (  # noqa: E402 - reuse the spawn harness
    API_KEY,
    EmbeddedInstance,
    _actae_bin,
    _free_port_pair,
    _instance_env,
    _pg_port_pair,
)

pytestmark = pytest.mark.timeout(300)

SKIP_REASON = (
    "opt-in server-crash chaos suite (set ACTAE_SERVER_CRASH=1; needs an `actae` binary)"
)


def _skip_unless():
    import os

    if os.environ.get("ACTAE_SERVER_CRASH") != "1":
        pytest.skip(SKIP_REASON)
    if not _actae_bin().is_file():
        pytest.skip("actae binary not found — build with `cd actae && cargo build` first")


def _auth():
    return {"X-API-Key": API_KEY}


def _replay(instance, channel):
    resp = httpx.get(
        f"{instance.url}/api/v1/events/replay/{channel}",
        headers=_auth(),
        params={"cursor": 0, "limit": 5000},
        timeout=10.0,
    )
    resp.raise_for_status()
    return resp.json()["events"]


def _verify_chain(instance, channel):
    resp = httpx.get(f"{instance.url}/api/v1/events/verify/{channel}", headers=_auth(), timeout=10.0)
    resp.raise_for_status()
    return resp.json()


def _latest_state(instance, channel):
    resp = httpx.get(f"{instance.url}/api/v1/state/{channel}", headers=_auth(), timeout=10.0)
    resp.raise_for_status()
    return resp.json()


def _transition(instance, channel, payload, state):
    resp = httpx.post(
        f"{instance.url}/api/v1/events/transition",
        headers={**_auth(), "Content-Type": "application/json"},
        json={
            "channel_id": channel,
            "type": "agent.transition",
            "payload": payload,
            "state": state,
            "metadata": {"actor": "chaos-test"},
        },
        timeout=10.0,
    )
    resp.raise_for_status()
    return resp.json()


@pytest.fixture
def instance(tmp_path):
    _skip_unless()
    http_port, _ = _free_port_pair()
    pg_port = _pg_port_pair(http_port, http_port + 1)[0]
    data_dir = tmp_path / "data"
    env = _instance_env(http_port, pg_port, data_dir)
    inst = EmbeddedInstance(http_port, pg_port, data_dir, env)
    inst.start()
    try:
        yield inst
    finally:
        inst.cleanup()


def test_acknowledged_writes_survive_a_hard_server_kill(instance):
    channel = f"chaos-ack-{uuid.uuid4().hex[:8]}"
    acked = []
    for _ in range(40):
        event = instance.record(channel)
        acked.append(event["cursor"])

    transition = _transition(instance, channel, {"k": 1}, {"phase": "committed"})
    state_cursor = transition["event"]["cursor"]
    state_version = transition["state_version"]
    assert state_version >= 1

    # Hard kill — no graceful shutdown, no flush.
    instance.kill_hard()
    # Restart the same data dir; the orphaned postgres is reclaimed.
    instance.start()

    events = _replay(instance, channel)
    cursors = [e["cursor"] for e in events]
    assert cursors == list(range(1, len(cursors) + 1)), "cursors are not gapless after restart"
    assert set(acked).issubset(set(cursors)), "an acknowledged event was lost"
    assert state_cursor in cursors, "the transition event did not survive"

    state = _latest_state(instance, channel)
    assert state is not None and state["cursor"] == state_cursor
    assert state["state"] == {"phase": "committed"}, "snapshot did not survive with its event"

    chain = _verify_chain(instance, channel)
    assert chain["valid"] is True, f"hash chain broken after restart: {chain}"

    # The restarted server accepts new writes on the same channel.
    instance.record(channel)


def test_kill_during_concurrent_writes_leaves_no_torn_state(instance):
    channel = f"chaos-race-{uuid.uuid4().hex[:8]}"
    acked = []
    lock = threading.Lock()
    stop = threading.Event()

    def hammer():
        while not stop.is_set():
            try:
                event = instance.record(channel)
            except Exception:
                return  # connection died with the server — expected
            with lock:
                acked.append(event["cursor"])

    thread = threading.Thread(target=hammer, daemon=True)
    thread.start()
    time.sleep(1.5)
    instance.kill_hard()  # kill mid-flight
    stop.set()
    thread.join(timeout=5)

    instance.start()
    events = _replay(instance, channel)
    cursors = [e["cursor"] for e in events]
    assert cursors, "no acknowledged event survived"
    assert cursors == list(range(1, len(cursors) + 1)), "cursors are not gapless (torn write)"
    with lock:
        assert set(acked).issubset(set(cursors)), "an acknowledged event was lost"
    chain = _verify_chain(instance, channel)
    assert chain["valid"] is True, f"hash chain broken after mid-write kill: {chain}"
    # No event has a partial/absent payload (a torn row would fail to serialize).
    assert all("payload" in e for e in events)
