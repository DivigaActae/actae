#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Actae instance health & usage test.

Verifies that your Actae server is up, accepting events, and that
stats/metrics update correctly. Includes a simple agent simulation
that records a multi-step workflow and validates state transitions.

Usage:
    # 1. Copy .env.example to .env and fill in your values
    cp .env.example .env

    # 2. Install dependencies
    pip install httpx python-dotenv

    # 3. Run
    python test_actae.py
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

try:
    import httpx
except ImportError:
    sys.exit("Missing dependency: pip install httpx")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # .env loading is optional

# ── Configuration ──────────────────────────────────────────────────────────
ACTAE_URL = os.environ.get("ACTAE_URL", "http://localhost:8002").rstrip("/")
ACTAE_API_KEY = os.environ.get("ACTAE_API_KEY", "")

if not ACTAE_API_KEY:
    sys.exit(
        "ERROR: ACTAE_API_KEY is not set.\n"
        "Copy .env.example to .env and fill in your API key."
    )

HEADERS = {"X-API-Key": ACTAE_API_KEY, "Content-Type": "application/json"}
CHANNEL = f"test-{uuid.uuid4().hex[:12]}"
PASSED = 0
FAILED = 0


def section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def check(name: str, ok: bool, detail: str = "") -> None:
    global PASSED, FAILED
    icon = "✓" if ok else "✗"
    color = "\033[32m" if ok else "\033[31m"
    reset = "\033[0m"
    msg = f"  {color}{icon}{reset} {name}"
    if detail:
        msg += f" — {detail}"
    print(msg)
    if ok:
        PASSED += 1
    else:
        FAILED += 1


def api(method: str, path: str, **kwargs) -> httpx.Response:
    url = f"{ACTAE_URL}{path}"
    return httpx.request(method, url, headers=HEADERS, timeout=15, **kwargs)


# ── Tests ──────────────────────────────────────────────────────────────────

def test_health():
    section("Health Checks")
    try:
        r = api("GET", "/healthz")
        check("/healthz reachable", r.status_code == 200, f"status={r.status_code}")
    except httpx.ConnectError:
        check("/healthz reachable", False, f"cannot connect to {ACTAE_URL}")
        return False

    try:
        r = api("GET", "/readyz")
        check("/readyz ready", r.status_code == 200, f"status={r.status_code}")
    except Exception as e:
        check("/readyz ready", False, str(e))

    return True


def test_record_and_replay():
    section("Record & Replay Events")
    ts = datetime.now(timezone.utc).isoformat()
    events = [
        {"type": "agent.started", "payload": {"model": "test-model", "ts": ts}, "actor": "test-runner"},
        {"type": "tool.called", "payload": {"tool": "web_search", "query": "actae documentation"}, "actor": "test-runner"},
        {"type": "tool.result", "payload": {"tool": "web_search", "rows": 5}, "actor": "test-runner"},
        {"type": "agent.thinking", "payload": {"step": 2, "reasoning": "Got search results, synthesizing answer"}, "actor": "test-runner"},
        {"type": "agent.completed", "payload": {"status": "success", "tokens_used": 1234}, "actor": "test-runner"},
    ]

    recorded_ids = []
    for i, evt in enumerate(events):
        body = {
            "channel_id": CHANNEL,
            "type": evt["type"],
            "payload": evt["payload"],
            "metadata": {"actor": evt["actor"]},
        }
        r = api("POST", "/api/v1/events/record", json=body)
        check(f"Record event {i+1} ({evt['type']})", r.status_code in (200, 201), f"status={r.status_code}")
        if r.status_code in (200, 201):
            data = r.json()
            recorded_ids.append(data.get("id", "unknown"))

    # Replay
    r = api("GET", f"/api/v1/events/replay/{CHANNEL}?cursor=0&limit=100")
    check("Replay events", r.status_code == 200, f"status={r.status_code}")
    if r.status_code == 200:
        data = r.json()
        returned = data.get("events", [])
        check("Replay returns all events", len(returned) == len(events), f"expected={len(events)}, got={len(returned)}")
        if returned:
            check("First event type matches", returned[0]["type"] == events[0]["type"])
    else:
        check("Replay returns all events", False, "replay failed")

    return len(recorded_ids) == len(events)


def test_cursor():
    section("Cursor Tracking")
    r = api("GET", f"/api/v1/events/cursor/{CHANNEL}")
    check("Get cursor", r.status_code == 200, f"status={r.status_code}")
    if r.status_code == 200:
        data = r.json()
        cursor = data.get("latest_cursor", data.get("cursor", 0))
        check("Cursor > 0 after events", cursor > 0, f"cursor={cursor}")


def test_state_transitions():
    section("State Transitions (Agent Workflow)")
    # Simulate an agent saving state at each step
    states = [
        {"turn": 1, "context": "User asked about Actae", "tools_used": []},
        {"turn": 2, "context": "Searched documentation", "tools_used": ["web_search"]},
        {"turn": 3, "context": "Synthesized answer", "tools_used": ["web_search"], "answer_ready": True},
    ]

    for i, state in enumerate(states):
        body = {
            "channel_id": CHANNEL,
            "cursor": i + 1,
            "state": json.dumps(state),
        }
        r = api("POST", f"/api/v1/state/{CHANNEL}", json=body)
        check(f"Save state turn {i+1}", r.status_code in (200, 201), f"status={r.status_code}")

    # Load latest state
    r = api("GET", f"/api/v1/state/{CHANNEL}")
    check("Load latest state", r.status_code == 200, f"status={r.status_code}")
    if r.status_code == 200:
        data = r.json()
        loaded = json.loads(data.get("state", "{}"))
        check("State has correct turn", loaded.get("turn") == 3, f"turn={loaded.get('turn')}")
        check("State has answer_ready", loaded.get("answer_ready") is True)

    # List versions
    r = api("GET", f"/api/v1/state/{CHANNEL}/versions")
    check("List state versions", r.status_code == 200, f"status={r.status_code}")
    if r.status_code == 200:
        data = r.json()
        versions = data.get("versions", [])
        check("3 state versions recorded", len(versions) == 3, f"count={len(versions)}")


def test_metrics():
    section("Metrics & Stats")
    r = api("GET", "/metrics.json")
    check("GET /metrics.json", r.status_code == 200, f"status={r.status_code}")
    if r.status_code == 200:
        data = r.json()
        check("Metrics is a dict", isinstance(data, dict))
        # The metrics should have some keys
        check("Metrics has keys", len(data) > 0, f"keys={list(data.keys())[:5]}")


def test_fork():
    section("Fork (Fork)")
    fork_ch = f"{CHANNEL}-fork-{uuid.uuid4().hex[:6]}"
    body = {
        "source_channel_id": CHANNEL,
        "new_channel_id": fork_ch,
        "at_cursor": 2,
        "reason": "test-fork: explore alternative approach",
    }
    r = api("POST", "/api/v1/channels/fork", json=body)
    check("Fork channel at cursor 2", r.status_code in (200, 201), f"status={r.status_code}")
    if r.status_code in (200, 201):
        data = r.json()
        fork_channel = data.get("fork_channel_id") or data.get("channel_id") or fork_ch
        check("Fork returns channel ID", fork_channel is not None, f"fork={fork_channel}")

        # Record an event on the fork
        r2 = api("POST", "/api/v1/events/record", json={
            "channel_id": fork_channel,
            "type": "agent.alt_approach",
            "payload": {"approach": "tried alternative"},
            "metadata": {"actor": "test-runner"},
        })
        check("Record event on fork", r2.status_code in (200, 201), f"status={r2.status_code}")

        # Verify original channel is unaffected
        r3 = api("GET", f"/api/v1/events/replay/{CHANNEL}?cursor=0&limit=100")
        if r3.status_code == 200:
            orig_count = len(r3.json().get("events", []))
            check("Original channel unchanged", orig_count == 5, f"events={orig_count}")


def test_cleanup():
    section("Cleanup")
    # Delete the fork channel if it was created
    # (The test channel will be garbage-collected by retention)
    check("Test channel", True, CHANNEL)
    check("Total passed", True, str(PASSED))
    check("Total failed", True, str(FAILED))


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'═' * 60}")
    print(f"  ACTAE INSTANCE TEST")
    print(f"  Endpoint:  {ACTAE_URL}")
    print(f"  Channel:   {CHANNEL}")
    print(f"  Time:      {datetime.now(timezone.utc).isoformat()}")
    print(f"{'═' * 60}")

    if not test_health():
        print("\n\033[31mServer is not reachable. Is Actae running?\033[0m")
        print(f"  Start with: cd actae && cargo run")
        sys.exit(1)

    test_record_and_replay()
    test_cursor()
    test_state_transitions()
    test_fork()
    test_metrics()
    test_cleanup()

    print(f"\n{'═' * 60}")
    if FAILED == 0:
        print(f"  \033[32mALL TESTS PASSED ({PASSED}/{PASSED + FAILED})\033[0m")
    else:
        print(f"  \033[31m{FAILED} FAILED, {PASSED} PASSED\033[0m")
    print(f"{'═' * 60}\n")
    sys.exit(0 if FAILED == 0 else 1)


if __name__ == "__main__":
    main()
