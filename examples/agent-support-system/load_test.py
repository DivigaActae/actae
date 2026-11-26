# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""
Concurrent load test for Actae.

Spawns N concurrent agent conversation workers and measures:

  - events/sec throughput
  - p50/p95/p99 latency per event record
  - error rate
  - memory/reconnection behavior under load

Usage:
  python load_test.py --workers=50 --duration=120
  python load_test.py --workers=100 --duration=300 --think-min=0.01 --think-max=0.05
  python load_test.py --api-url=http://localhost:8002 --workers=200 --profile=spike
"""

import argparse
import asyncio
import json
import os
import random
import statistics
import sys
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field

import httpx

from config import ACTAE_API_URL

# ---------------------------------------------------------------------------
# Scenarios (subset for speed)
# ---------------------------------------------------------------------------

QUICK_SCENARIOS = [
    [
        ("agent.started", {"agent": "load-test"}),
        ("tool.search", {"query": "test"}),
        ("tool.search.completed", {"result": "ok"}),
        ("agent.completed", {"resolution": "test"}),
    ],
    [
        ("agent.started", {"agent": "load-test"}),
        ("tool.lookup", {"id": "123"}),
        ("tool.lookup.completed", {"found": True}),
        ("tool.process", {"action": "verify"}),
        ("tool.process.completed", {"verified": True}),
        ("agent.completed", {"resolution": "verified"}),
    ],
    [
        ("agent.started", {"agent": "load-test"}),
        ("tool.search", {"q": "customer"}),
        ("tool.search.completed", {"count": 1}),
        ("tool.update", {"field": "status"}),
        ("tool.update.completed", {"status": "updated"}),
        ("tool.notify", {"channel": "email"}),
        ("tool.notify.completed", {"sent": True}),
        ("agent.completed", {"resolution": "notified"}),
    ],
    [
        ("agent.started", {"agent": "load-test"}),
        ("tool.check", {"service": "api"}),
        ("tool.check.completed", {"healthy": True}),
        ("human.approval.requested", {"reason": "test"}),
        ("human.approval.granted", {"by": "system"}),
        ("agent.completed", {"resolution": "approved"}),
    ],
]


@dataclass
class RunResult:
    total_events: int = 0
    total_errors: int = 0
    latencies: list[float] = field(default_factory=list)
    duration: float = 0.0


# ---------------------------------------------------------------------------
# Workers
# ---------------------------------------------------------------------------

async def worker(client: httpx.AsyncClient, api_url: str,
                 scenario: list, think_time: tuple[float, float],
                 results: RunResult, headers: dict,
                 idx: int):
    channel_id = f"load-{uuid.uuid4().hex[:12]}"
    errors = 0
    latencies = []

    for event_type, payload in scenario:
        body = {
            "channel_id": channel_id,
            "type": event_type,
            "payload": payload,
            "metadata": {"actor": "load-test"},
        }

        think = random.uniform(*think_time)
        if think > 0:
            await asyncio.sleep(think)

        start = time.monotonic()
        try:
            resp = await client.post(
                f"{api_url}/api/v1/events/record",
                json=body,
                headers=headers,
                timeout=10.0,
            )
            elapsed = (time.monotonic() - start) * 1000
            latencies.append(elapsed)

            if resp.status_code != 201:
                errors += 1
        except Exception:
            errors += 1
            elapsed = (time.monotonic() - start) * 1000
            latencies.append(elapsed)

    results.total_events += len(scenario)
    results.total_errors += errors
    results.latencies.extend(latencies)


# ---------------------------------------------------------------------------
# Load profiles
# ---------------------------------------------------------------------------

def profile_config(profile: str) -> dict:
    profiles = {
        "steady": {
            "workers": 50, "duration": 120,
            "ramp_up": 10, "think_min": 0.02, "think_max": 0.08,
            "description": "50 concurrent, 2 min steady state",
        },
        "spike": {
            "workers": 200, "duration": 60,
            "ramp_up": 5, "think_min": 0.005, "think_max": 0.02,
            "description": "200 concurrent burst, 1 min",
        },
        "endurance": {
            "workers": 25, "duration": 600,
            "ramp_up": 30, "think_min": 0.05, "think_max": 0.2,
            "description": "25 concurrent, 10 min endurance",
        },
        "max": {
            "workers": 500, "duration": 120,
            "ramp_up": 15, "think_min": 0.001, "think_max": 0.01,
            "description": "500 concurrent max throughput test",
        },
    }
    return profiles.get(profile, profiles["steady"])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run_load(args):
    verify = args.api_url.startswith("https")
    headers = {"Content-Type": "application/json"}

    if args.profile:
        cfg = profile_config(args.profile)
        workers = cfg["workers"]
        duration = cfg["duration"]
        ramp_up = cfg["ramp_up"]
        think = (cfg["think_min"], cfg["think_max"])
        print(f"  Profile: {args.profile} — {cfg['description']}")
    else:
        workers = args.workers
        duration = args.duration
        ramp_up = args.ramp_up
        think = (args.think_min, args.think_max)

    print(f"  Workers: {workers}")
    print(f"  Duration: {duration}s")
    print(f"  Ramp-up: {ramp_up}s")
    print(f"  Think time: {think[0]:.3f}s - {think[1]:.3f}s")
    print(f"  Scenario steps: {len(QUICK_SCENARIOS[0])}-{len(QUICK_SCENARIOS[-1])}")
    print(f"  {'' * 60}")
    print(f"  {'time':>6s}  {'active':>6s}  {'done':>6s}  {'evts':>7s}  {'errs':>5s}  {'rate':>8s}  {'p50':>6s}  {'p95':>6s}  {'p99':>6s}")
    print(f"  {'' * 60}")

    result = RunResult()
    start_time = time.monotonic()
    next_report = 5.0
    completed = 0
    active = 0
    sem = asyncio.Semaphore(workers)

    async with httpx.AsyncClient(verify=False, timeout=30.0, limits=httpx.Limits(max_connections=workers + 10)) as client:

        async def throttled_worker(scenario):
            nonlocal completed, active
            active += 1
            async with sem:
                await worker(client, args.api_url, scenario, think, result, headers, completed)
            completed += 1
            active -= 1

        pending: list[asyncio.Task] = []
        tick = time.monotonic()

        while (elapsed := time.monotonic() - start_time) < duration:
            # Dispatch workers
            dispatch_rate = min(workers, max(1, int(workers * (elapsed / ramp_up))) if elapsed < ramp_up else workers)
            to_dispatch = max(0, dispatch_rate - len(pending))
            for _ in range(to_dispatch):
                scenario = random.choice(QUICK_SCENARIOS)
                task = asyncio.create_task(throttled_worker(scenario))
                pending.append(task)

            # Wait and report
            await asyncio.sleep(1.0)

            # Clean up completed tasks
            done = [t for t in pending if t.done()]
            for t in done:
                try:
                    t.result()
                except Exception:
                    pass
            pending = [t for t in pending if not t.done()]

            # Report
            if elapsed >= next_report or elapsed >= duration:
                next_report = elapsed + 5
                elapsed_running = elapsed
                rate = result.total_events / elapsed_running if elapsed_running > 0 else 0

                p50 = statistics.median(result.latencies[-500:]) if result.latencies else 0
                p95 = 0
                p99 = 0
                if len(result.latencies) > 20:
                    sorted_lats = sorted(result.latencies)
                    p95 = sorted_lats[int(len(sorted_lats) * 0.95)]
                    p99 = sorted_lats[int(len(sorted_lats) * 0.99)]

                print(f"  {elapsed:>5.0f}s  {active:>6d}  {completed:>6d}  {result.total_events:>7d}  {result.total_errors:>5d}  {rate:>7.1f}/s  {p50:>5.1f}  {p95:>5.1f}  {p99:>5.1f}")

        # Drain pending
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.wait(pending, timeout=5.0)

    elapsed = time.monotonic() - start_time
    rate = result.total_events / elapsed if elapsed > 0 else 0

    print(f"  {'' * 60}")
    print(f"\n  Results:")
    print(f"    Duration:        {elapsed:.1f}s")
    print(f"    Workers:         {workers}")
    print(f"    Events recorded: {result.total_events}")
    print(f"    Errors:          {result.total_errors} ({result.total_errors/max(result.total_events,1)*100:.1f}%)")
    print(f"    Throughput:      {rate:.1f} events/s")
    print(f"    Latency (ms):")
    if result.latencies:
        sorted_lats = sorted(result.latencies)
        print(f"      p50:  {statistics.median(result.latencies):.1f}")
        print(f"      p95:  {sorted_lats[int(len(sorted_lats)*0.95)]:.1f}")
        print(f"      p99:  {sorted_lats[int(len(sorted_lats)*0.99)]:.1f}")
        print(f"      max:  {max(result.latencies):.1f}")

    return 0 if result.total_errors == 0 else 1


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Actae Load Test")
    parser.add_argument("--api-url", default=ACTAE_API_URL, help="Actae API URL")

    parser.add_argument("--profile", choices=["steady", "spike", "endurance", "max"],
                        help="Use a pre-defined load profile")

    # Manual settings (ignored if --profile is set)
    parser.add_argument("--workers", type=int, default=50)
    parser.add_argument("--duration", type=int, default=120)
    parser.add_argument("--ramp-up", type=int, default=10)
    parser.add_argument("--think-min", type=float, default=0.02)
    parser.add_argument("--think-max", type=float, default=0.08)

    args = parser.parse_args()
    sys.exit(asyncio.run(run_load(args)))


if __name__ == "__main__":
    main()
