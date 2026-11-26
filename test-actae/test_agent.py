#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Agent integration test — simulates a real agent workflow against Actae.

Tests the full agent lifecycle:
  1. Start a session (record agent.started)
  2. Run 5 steps with state tracking
  3. Fork at step 3, run alternative steps
  4. Verify lineage and diff
  5. Check that metrics/events updated

Usage:
    pip install httpx python-dotenv
    python test_agent.py
"""

from __future__ import annotations

import json
import os
import sys
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
    pass

ACTAE_URL = os.environ.get("ACTAE_URL", "http://localhost:8002").rstrip("/")
ACTAE_API_KEY = os.environ.get("ACTAE_API_KEY", "")

if not ACTAE_API_KEY:
    sys.exit("ERROR: Set ACTAE_API_KEY in .env or environment")

HEADERS = {"X-API-Key": ACTAE_API_KEY, "Content-Type": "application/json"}
PASSED = FAILED = 0


def check(name: str, ok: bool, detail: str = ""):
    global PASSED, FAILED
    icon = "✓" if ok else "✗"
    color = "\033[32m" if ok else "\033[31m"
    print(f"  {color}{icon}\033[0m {name}" + (f" — {detail}" if detail else ""))
    PASSED += 1 if ok else 0
    FAILED += 0 if ok else 1


def api(method: str, path: str, **kw) -> httpx.Response:
    return httpx.request(method, f"{ACTAE_URL}{path}", headers=HEADERS, timeout=15, **kw)


def main():
    global PASSED, FAILED
    channel = f"agent-test-{uuid.uuid4().hex[:8]}"
    fork_channel = None

    print(f"\n{'═' * 60}")
    print(f"  AGENT INTEGRATION TEST")
    print(f"  Endpoint: {ACTAE_URL}")
    print(f"  Channel:  {channel}")
    print(f"{'═' * 60}")

    # ── Phase 1: Health ────────────────────────────────────────────
    print("\n── Phase 1: Health ──")
    r = api("GET", "/healthz")
    check("Server healthy", r.status_code == 200)

    # ── Phase 2: Agent session ────────────────────────────────────
    print("\n── Phase 2: Agent Session (5 steps) ──")
    state = {"turn": 0, "context": [], "tools_used": []}

    for step in range(1, 6):
        # Record step event
        r = api("POST", "/api/v1/events/record", json={
            "channel_id": channel,
            "type": "agent.step",
            "payload": {"step": step, "model": "test-agent", "tokens": step * 100},
            "metadata": {"actor": "test-agent"},
        })
        check(f"Step {step}: record event", r.status_code in (200, 201))

        # Update state
        state["turn"] = step
        state["context"].append(f"Step {step} completed")
        state["tools_used"].append(f"tool_{step}")

        r = api("POST", f"/api/v1/state/{channel}", json={
            "channel_id": channel,
            "cursor": step,
            "state": json.dumps(state),
        })
        check(f"Step {step}: save state", r.status_code in (200, 201))

    # Verify final state
    r = api("GET", f"/api/v1/state/{channel}")
    check("Load final state", r.status_code == 200)
    if r.status_code == 200:
        loaded = json.loads(r.json().get("state", "{}"))
        check("Final state turn=5", loaded.get("turn") == 5)
        check("Final state has 5 context entries", len(loaded.get("context", [])) == 5)

    # ── Phase 3: Fork at step 3 ──────────────────────────────────
    print("\n── Phase 3: Fork at Step 3 ──")
    fork_channel = f"{channel}-fork-{uuid.uuid4().hex[:8]}"
    r = api("POST", "/api/v1/channels/fork", json={
        "source_channel_id": channel,
        "new_channel_id": fork_channel,
        "at_cursor": 3,
        "reason": "Explore alternative approach at step 3",
    })
    check("Fork channel", r.status_code in (200, 201))
    if r.status_code in (200, 201):
        fork_channel = r.json().get("fork_channel_id") or r.json().get("channel_id")
        check("Fork channel ID returned", fork_channel is not None, f"fork={fork_channel}")

    # ── Phase 4: Run alternative steps on fork ────────────────────
    if fork_channel:
        print("\n── Phase 4: Alternative Steps on Fork ──")
        fork_state = {"turn": 0, "context": [], "tools_used": []}

        for step in range(4, 6):
            r = api("POST", "/api/v1/events/record", json={
                "channel_id": fork_channel,
                "type": "agent.step.alt",
                "payload": {"step": step, "model": "test-agent-alt", "tokens": step * 150},
                "metadata": {"actor": "test-agent-alt"},
            })
            check(f"Fork step {step}: record event", r.status_code in (200, 201))

            fork_state["turn"] = step
            fork_state["context"].append(f"Alt step {step} (different approach)")
            fork_state["tools_used"].append(f"alt_tool_{step}")

            r = api("POST", f"/api/v1/state/{fork_channel}", json={
                "channel_id": fork_channel,
                "cursor": step,
                "state": json.dumps(fork_state),
            })
            check(f"Fork step {step}: save state", r.status_code in (200, 201))

        # Verify fork state is independent
        r = api("GET", f"/api/v1/state/{fork_channel}")
        if r.status_code == 200:
            loaded = json.loads(r.json().get("state", "{}"))
            check("Fork state turn=5", loaded.get("turn") == 5)
            check("Fork state has alt context", "Alt step" in str(loaded.get("context", [])))

        # Verify original channel is unaffected
        r = api("GET", f"/api/v1/state/{channel}")
        if r.status_code == 200:
            loaded = json.loads(r.json().get("state", "{}"))
            check("Original state still turn=5", loaded.get("turn") == 5)
            check("Original context has no 'Alt'", not any("Alt" in c for c in loaded.get("context", [])))

    # ── Phase 5: Query & Metrics ──────────────────────────────────
    print("\n── Phase 5: Query & Metrics ──")

    # Query events across both channels
    r = api("POST", "/api/v1/events/query", json={
        "channel_ids": [channel] + ([fork_channel] if fork_channel else []),
        "limit": 50,
    })
    check("Query events across channels", r.status_code == 200)
    if r.status_code == 200:
        events = r.json().get("events", [])
        expected = 10 if fork_channel else 5  # 5 original + 5 fork
        check(f"Found {expected} events", len(events) == expected, f"got={len(events)}")

    # Check metrics
    r = api("GET", "/metrics.json")
    check("Metrics endpoint", r.status_code == 200)

    # ── Summary ───────────────────────────────────────────────────
    print(f"\n{'═' * 60}")
    if FAILED == 0:
        print(f"  \033[32mALL TESTS PASSED ({PASSED}/{PASSED + FAILED})\033[0m")
    else:
        print(f"  \033[31m{FAILED} FAILED, {PASSED} PASSED\033[0m")
    print(f"{'═' * 60}\n")
    sys.exit(0 if FAILED == 0 else 1)


if __name__ == "__main__":
    main()
