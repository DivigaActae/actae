#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Soak/load harness for the execution primitives (needs a running Actae).

Bounded, hermetic load on the paths that matter for durability — not a
throughput benchmark, but a check that correctness holds *under concurrency*
and that recovery stays cheap as a channel grows:

  * fork-replay storm   — N concurrent `auto` tool claims on a forked channel
                          must ALL resolve to `replayed` with zero errors.
  * step throughput     — one session records M steps; measure events/sec.
  * resume latency      — `latest_step_number` + `resume` on a large channel;
                          must stay O(1)-ish (index probe), not scale with M.

Writes an `actae.soak.v1` JSON report and exits non-zero if any check fails.

    cd actae && cargo run
    ACTAE_URL=http://127.0.0.1:8002 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
      python3 examples/reliability/soak.py --concurrency 8 --iterations 200
"""

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from actae_client import ActaeClient  # noqa: E402
from actae_client.session import AgentSession  # noqa: E402

SCHEMA = "actae.soak.v1"


def _percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round(q * (len(ordered) - 1)))))
    return round(ordered[idx], 2)


async def _fork_replay_storm(client: ActaeClient, concurrency: int, iterations: int) -> Dict[str, Any]:
    base = f"soak-base-{uuid.uuid4().hex[:8]}"
    session = AgentSession(client, base, state_fn=lambda: {"n": 1}, snapshot_interval=1)
    async with session:
        await session.step("research", output={"sources": 3})
        claim = await client.claim_execution(base, "charge-1", "charge", {"amount": 5})
        await client.complete_execution(claim.execution.id, claim.claim_token, {"paid": True})
        await session.step("charge", output={"paid": True})
        child = await session.fork(2, f"soak-child-{uuid.uuid4().hex[:8]}")

    latencies: List[float] = []
    statuses: Dict[str, int] = {}
    errors = 0
    sem = asyncio.Semaphore(concurrency)

    async def one() -> None:
        nonlocal errors
        async with sem:
            started = time.perf_counter()
            try:
                result = await client.claim_execution(
                    child.channel_id, "charge-1", "charge", {"amount": 5}
                )
                statuses[result.status] = statuses.get(result.status, 0) + 1
                if result.status != "replayed":
                    errors += 1
            except Exception:
                errors += 1
            finally:
                latencies.append((time.perf_counter() - started) * 1000.0)

    started = time.perf_counter()
    await asyncio.gather(*(one() for _ in range(iterations)))
    elapsed = time.perf_counter() - started

    return {
        "iterations": iterations,
        "concurrency": concurrency,
        "statuses": statuses,
        "errors": errors,
        "throughput_per_sec": round(iterations / elapsed, 1) if elapsed else 0.0,
        "latency_ms": {
            "p50": _percentile(latencies, 0.50),
            "p95": _percentile(latencies, 0.95),
            "p99": _percentile(latencies, 0.99),
        },
        "passed": errors == 0 and statuses.get("replayed", 0) == iterations,
    }


async def _step_throughput(client: ActaeClient, steps: int) -> Dict[str, Any]:
    name = f"soak-steps-{uuid.uuid4().hex[:8]}"
    session = AgentSession(client, name, state_fn=lambda: {"n": 1}, snapshot_interval=1)
    started = time.perf_counter()
    async with session:
        for i in range(steps):
            await session.step("soak.step", output={"i": i})
    elapsed = time.perf_counter() - started
    return {
        "steps": steps,
        "seconds": round(elapsed, 3),
        "steps_per_sec": round(steps / elapsed, 1) if elapsed else 0.0,
        "passed": session.step_count == steps,
        "channel_id": name,
    }


async def _resume_latency(client: ActaeClient, channel_id: str, repeats: int) -> Dict[str, Any]:
    last = await client.latest_step_number(channel_id)
    latencies: List[float] = []
    for _ in range(repeats):
        started = time.perf_counter()
        await client.latest_step_number(channel_id)
        latencies.append((time.perf_counter() - started) * 1000.0)
    return {
        "last_step_number": last,
        "repeats": repeats,
        "latency_ms": {
            "p50": _percentile(latencies, 0.50),
            "p95": _percentile(latencies, 0.95),
        },
        "passed": last is not None and last >= 1,
    }


async def run(args: argparse.Namespace) -> int:
    client = ActaeClient(api_key=args.api_key, endpoint=args.url)
    await client.connect()
    try:
        fork = await _fork_replay_storm(client, args.concurrency, args.iterations)
        steps = await _step_throughput(client, args.steps)
        resume = await _resume_latency(client, steps["channel_id"], args.resume_repeats)
    finally:
        await client.disconnect()

    report = {
        "schema": SCHEMA,
        "run_id": args.run_id or f"soak-{uuid.uuid4().hex[:8]}",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "actae_url": args.url,
        "fork_replay": fork,
        "step_throughput": steps,
        "resume_latency": resume,
    }
    report["passed"] = bool(fork["passed"] and steps["passed"] and resume["passed"])
    report["verdict"] = "PASS" if report["passed"] else "FAIL"

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{report['run_id']}.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )

    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=os.getenv("ACTAE_URL") or os.getenv("ACTAE_ENDPOINT") or "http://127.0.0.1:8002")
    parser.add_argument("--api-key", default=os.getenv("ACTAE_API_KEY") or "sk-dev-0000000000000000000000")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--iterations", type=int, default=200)
    parser.add_argument("--steps", type=int, default=500)
    parser.add_argument("--resume-repeats", type=int, default=20)
    parser.add_argument("--out", default=str(Path(__file__).resolve().parents[1] / "reliability-reports"))
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
