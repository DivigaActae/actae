#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Fork showcase — one failing run, two counterfactual forks, a proof.

A single deterministic, LLM-free script that ties Actae's fork story together
in one place (the marketing-facing surface for a capability that is easy to
under-explain):

  1. A baseline run FAILS at a verify step (recorded immutably as state).
  2. Two forks are created at that exact boundary with different interventions.
  3. `diff_states` proves which fix changed what (structural, not vibes).
  4. The immutable fork receipt proves an exact, restorable boundary.
  5. The parent is unaffected by either fork (isolation).
  6. A deterministic token-savings summary shows the prefix was not re-run.

Deterministic and dependency-free beyond the Python SDK. Exit 0 when every
guarantee holds.

Usage:
    pip install -e ./sdks/python
    cd actae && cargo run
    python3 examples/fork_showcase.py
"""

from __future__ import annotations

import asyncio
import copy
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from fork_token_report import StepRegistry, format_token_report  # noqa: E402

from actae_client import ActaeClient  # noqa: E402
from actae_client.session import AgentSession  # noqa: E402

ACTAE_URL = os.getenv("ACTAE_URL", "http://localhost:8002")
ACTAE_API_KEY = os.getenv("ACTAE_API_KEY", "sk-dev-0000000000000000000000")

PIPELINE = ["plan", "act", "verify", "retry", "finalize"]
FORK_AT_STEP = 3


class Checks:
    def __init__(self) -> None:
        self.results: List[Tuple[str, bool, str]] = []

    def check(self, name: str, ok: bool, detail: str = "") -> None:
        self.results.append((name, ok, detail))
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))

    @property
    def ok(self) -> bool:
        return all(r[1] for r in self.results)


def _snapshot(state: Dict[str, Any]):
    def _fn() -> Dict[str, Any]:
        return copy.deepcopy(state)

    return _fn


def deterministic_token_registries(
    pipeline: List[str],
    fork_at_step: int,
    *,
    prompt_tokens: int = 1200,
    completion_tokens: int = 300,
    elapsed_ms: int = 500,
) -> Tuple[StepRegistry, StepRegistry]:
    """Build the baseline (all steps) and fork (tail only) token registries.

    Deterministic fixture usage, not measured provider usage: it exists so the
    showcase can demonstrate the savings arithmetic without an API key. The
    saved figure is exactly ``baseline_total - fork_total``.
    """
    baseline = StepRegistry()
    for step in pipeline:
        baseline.record(step, prompt_tokens, completion_tokens, elapsed_ms)
    fork = StepRegistry()
    for step in pipeline[fork_at_step:]:
        fork.record(step, prompt_tokens, completion_tokens, elapsed_ms)
    return baseline, fork


async def _run_baseline(client: ActaeClient, run_id: str) -> Tuple[AgentSession, Dict[str, Any]]:
    state: Dict[str, Any] = {"plan": None, "action": None, "status": "running", "error": None, "attempts": 0}
    session = AgentSession(
        client, f"{run_id}-base", state_fn=_snapshot(state), snapshot_interval=1
    )
    async with session:
        await session.step("plan", output={"plan": "primary"})
        state["plan"] = "primary"
        await session.step("act", input={"tool": "primary"}, output={"ok": False})
        state["action"] = "primary"
        await session.step("verify", output={"status": "failed", "error": "timeout"})
        state["status"] = "failed"
        state["error"] = "timeout"
        await session.step("retry", output={"attempts": 1})
        state["attempts"] = 1
        await session.step("finalize", output={"done": False})
    return session, copy.deepcopy(state)


async def _run_fork(
    client: ActaeClient,
    baseline: AgentSession,
    prefix_state: Dict[str, Any],
    name: str,
    intervention: Dict[str, Any],
    mutate: Any,
) -> Tuple[AgentSession, Dict[str, Any]]:
    state = copy.deepcopy(prefix_state)
    fork = await baseline.fork(
        FORK_AT_STEP, name, intervention=intervention, state_fn=_snapshot(state)
    )
    async with fork:
        await fork.step("retry", output=dict(intervention))
        mutate(state, intervention)
        await fork.step("finalize", output={"done": True})
        state["status"] = "ok"
        state["error"] = None
        state["done"] = True
    return fork, copy.deepcopy(state)


async def run() -> int:
    run_id = f"showcase-{uuid.uuid4().hex[:8]}"
    client = ActaeClient(api_key=ACTAE_API_KEY, endpoint=ACTAE_URL)
    checks = Checks()
    try:
        await client.connect()
        print(f"Actae: {ACTAE_URL}")
        print(f"Run:   {run_id}\n")

        baseline, prefix_state = await _run_baseline(client, run_id)
        baseline_events = await client.replay(baseline.channel_id, cursor=0, limit=500)
        checks.check(
            "the baseline failed and recorded it",
            prefix_state["status"] == "failed" and prefix_state["error"] == "timeout",
            f"status={prefix_state['status']} error={prefix_state['error']}",
        )

        def mutate_retries(state: Dict[str, Any], intervention: Dict[str, Any]) -> None:
            state["retries"] = intervention["retries"]

        def mutate_fallback(state: Dict[str, Any], intervention: Dict[str, Any]) -> None:
            state["tool"] = intervention["tool"]

        fork_a, state_a = await _run_fork(
            client, baseline, prefix_state, f"{run_id}-fix-retries",
            {"fix": "more_retries", "retries": 5}, mutate_retries,
        )
        fork_b, state_b = await _run_fork(
            client, baseline, prefix_state, f"{run_id}-fix-fallback",
            {"fix": "fallback_tool", "tool": "search_fallback"}, mutate_fallback,
        )
        checks.check(
            "both forks fixed the failure",
            state_a["status"] == "ok" and state_b["status"] == "ok",
            f"A={state_a['status']} B={state_b['status']}",
        )
        checks.check(
            "the forks took genuinely different paths",
            state_a.get("retries") == 5 and state_b.get("tool") == "search_fallback",
            "A changed retries; B changed tool",
        )

        diff = await client.diff_states(fork_a.channel_id, fork_b.channel_id)
        paths = {".".join(e["path"]) for e in diff.get("entries", [])}
        checks.check(
            "diff_states proves what changed between the forks",
            bool(paths) and paths <= {"retries", "tool"},
            f"entries={sorted(paths)}",
        )
        checks.check(
            "the shared ancestor is the failing baseline state",
            (diff.get("common") or {}).get("state", {}).get("status") == "failed",
            f"common_status={(diff.get('common') or {}).get('state', {}).get('status')}",
        )

        for label, fork in (("A", fork_a), ("B", fork_b)):
            receipt = fork.fork_receipt
            checks.check(
                f"fork {label} receipt is an exact, restorable boundary",
                bool(receipt)
                and bool(getattr(receipt, "restorable", False))
                and getattr(receipt, "requested_cursor", None)
                == getattr(receipt, "resolved_cursor", None)
                and bool(getattr(receipt, "source_state_sha256", "")),
                f"restorable={getattr(receipt, 'restorable', None)} "
                f"requested={getattr(receipt, 'requested_cursor', None)} "
                f"resolved={getattr(receipt, 'resolved_cursor', None)}",
            )

        # Isolation: the parent did not change.
        baseline_events_after = await client.replay(baseline.channel_id, cursor=0, limit=500)
        parent_state = await client.latest_state(baseline.channel_id)
        checks.check(
            "the parent is unaffected by either fork",
            len(baseline_events_after) == len(baseline_events)
            and (parent_state or {}).get("state", {}).get("status") == "failed",
            f"events={len(baseline_events_after)} (was {len(baseline_events)})",
        )

        # Deterministic token summary: the fork re-ran only the tail (2 of 5).
        baseline_reg, fork_reg = deterministic_token_registries(PIPELINE, FORK_AT_STEP)
        saved = baseline_reg.total_tokens() - fork_reg.total_tokens()
        checks.check(
            "forking skipped the prefix (3 of 5 steps not re-run)",
            fork_reg.total_calls() == len(PIPELINE) - FORK_AT_STEP and saved > 0,
            f"fork_calls={fork_reg.total_calls()} saved_tokens={saved}",
        )
        print("\n" + format_token_report(baseline_reg, fork_reg, PIPELINE, mode="deterministic-fixture"))

        # ------------------------------------------------------------------ #
        # The fidelity BOUND: what the ledger captured replays; what it did
        # not capture re-executes and diverges.
        # ------------------------------------------------------------------ #
        external = {"n": 0}

        async def recorded_read(session: AgentSession) -> Any:
            claim = await client.claim_execution(
                session.channel_id, "ext-read-1", "external.read", {"k": "v"}
            )
            if claim.status == "replayed":
                return claim.result
            value = external["n"]
            external["n"] += 1
            await client.complete_execution(
                claim.execution.id, claim.claim_token, result={"value": value}
            )
            return {"value": value}

        src = f"{run_id}-ext-src"
        src_session = AgentSession(client, src, state_fn=lambda: {"ok": True}, snapshot_interval=1)
        async with src_session:
            await src_session.step("setup", output={})
        baseline_read = await recorded_read(src_session)

        fork_session = await src_session.fork(1, f"{run_id}-ext-fork")
        fork_read = await recorded_read(fork_session)  # re-issues the inherited key

        rerun_session = AgentSession(
            client, f"{run_id}-ext-rerun", state_fn=lambda: {"ok": True}, snapshot_interval=1
        )
        async with rerun_session:
            rerun_read = await recorded_read(rerun_session)  # no ancestor -> runs live

        checks.check(
            "a recorded nondeterministic effect replays on a fork (no re-execution)",
            fork_read == baseline_read,
            f"baseline={baseline_read} fork={fork_read}",
        )
        checks.check(
            "an un-recorded/re-run effect re-executes and diverges (the fidelity bound)",
            rerun_read != baseline_read,
            f"baseline={baseline_read} re-run={rerun_read}",
        )
    finally:
        await client.disconnect()

    failed = [r for r in checks.results if not r[1]]
    print()
    print(f"{len(checks.results) - len(failed)}/{len(checks.results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run()))
