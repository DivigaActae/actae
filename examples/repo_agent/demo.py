#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Repo-maintenance agent demo: real workflow, real crash, real fork.

This is the "realistic workload" on top of the semantic evidence suites. It is
NOT the durability proof (that is ``examples/reliability`` and
``examples/durability_benchmark.py``); it is the demo that shows the same
guarantees holding for a plausible software-engineering agent operating on a
real repository with real tools and a real external side effect.

What it runs:

  baseline   a worker runs inspect → locate → plan → edit → test → react →
             report against a copied repo. It makes a plausible first edit,
             the tests still fail, and the report is submitted exactly once.
  crash      a second worker is killed with ``os._exit(137)`` right after the
             report side effect lands but before the ledger records it. A
             resume reconciles by querying the task service and does NOT
             re-submit; steps 1..7 are not re-invoked.
  fork       the baseline is forked at the failing-test step with an
             intervention that applies the correct fix. The fork's re-issued
             report replays the parent's recorded result (``auto`` policy) —
             the inherited side effect never fires again.

Deterministic invariants gate the exit code (effect counts, re-invocation
counts, fork receipt provenance). Model task success is reported separately.

Usage:
    pip install -e ./sdks/python
    cd actae && cargo run
    python3 examples/repo_agent/demo.py                 # deterministic, no key
    DEEPSEEK_API_KEY=... python3 examples/repo_agent/demo.py --llm   # real model
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List, Tuple

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

from agent import (  # noqa: E402
    REPORT_KEY,
    STEP_REACT_EDIT,
    STEP_REPORT,
    STEP_TEST,
    DryRunPolicy,
    LLMPolicy,
    RepoAgent,
)
from tools import WRONG_EDIT, RepoWorkspace, seed_workspace  # noqa: E402

from durability_benchmark import HttpEffectService, _http_request  # noqa: E402

from actae_client import ActaeClient  # noqa: E402
from actae_client.session import AgentSession  # noqa: E402

ACTAE_URL = os.getenv("ACTAE_URL", "http://localhost:8002")
ACTAE_API_KEY = os.getenv("ACTAE_API_KEY", "sk-dev-0000000000000000000000")
BASE_REPORT_DOWNSTREAM = "repo-report-base"
DEEP_FIX_MARKER = "(ordered[mid - 1] + ordered[mid]) / 2"


class Checks:
    def __init__(self) -> None:
        self.results: List[Tuple[str, bool, str]] = []

    def check(self, name: str, ok: bool, detail: str = "") -> None:
        self.results.append((name, ok, detail))
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))

    def info(self, name: str, detail: str = "") -> None:
        print(f"  [info] {name}" + (f" — {detail}" if detail else ""))

    @property
    def ok(self) -> bool:
        return all(r[1] for r in self.results)


def _task_service_count(url: str, downstream_key: str) -> int:
    rows = _http_request(url, "GET", "/effects")["rows"]
    return sum(1 for r in rows if r["downstream_key"] == downstream_key)


def _worker_env() -> Dict[str, str]:
    env = dict(os.environ)
    env["ACTAE_URL"] = ACTAE_URL
    env["ACTAE_ENDPOINT"] = ACTAE_URL
    env["ACTAE_API_KEY"] = ACTAE_API_KEY
    return env


async def _run_baseline(
    client: ActaeClient,
    checks: Checks,
    task_url: str,
    run_id: str,
    use_llm: bool,
    reports_dir: Path,
) -> Tuple[str, str]:
    channel = f"repo-{run_id}-base"
    workspace_dir = reports_dir / f"{run_id}-base-ws"
    seed_workspace(workspace_dir)

    workspace = RepoWorkspace(workspace_dir)
    state: Dict[str, Any] = {"invoked": []}
    session = AgentSession(
        client, channel,
        state_fn=lambda: {"invoked": list(state["invoked"])},
        snapshot_interval=1,
    )
    policy = LLMPolicy() if use_llm else DryRunPolicy(correct=False)
    agent = RepoAgent(
        client, session, workspace, task_url, policy,
        report_downstream_key=BASE_REPORT_DOWNSTREAM, state_sink=state,
    )
    async with session:
        result = await agent.run(start_step=1)

    if use_llm:
        checks.info("baseline outcome (model-driven)", f"tests_passed={result.tests_passed}")
    else:
        checks.check(
            "baseline leaves the failing test unresolved (the fork point)",
            not result.tests_passed,
            f"tests_passed={result.tests_passed}",
        )
    count = _task_service_count(task_url, BASE_REPORT_DOWNSTREAM)
    checks.check(
        "baseline submitted the report exactly once",
        count == 1,
        f"applications={count}",
    )
    return channel, result.final_hash


async def _run_crash_resume(
    client: ActaeClient, checks: Checks, task_url: str, run_id: str, reports_dir: Path
) -> None:
    channel = f"repo-{run_id}-crash"
    workspace_dir = reports_dir / f"{run_id}-crash-ws"
    seed_workspace(workspace_dir)

    worker = HERE / "worker.py"
    proc = subprocess.run(
        [
            sys.executable, str(worker),
            "--channel", channel,
            "--workspace", str(workspace_dir),
            "--task-service", task_url,
            "--crash-at", str(STEP_REPORT),
            "--report-downstream", "repo-report-crash",
        ],
        env=_worker_env(),
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(HERE),
    )
    checks.check(
        "agent worker dies for real at the report step (exit 137)",
        proc.returncode == 137,
        f"rc={proc.returncode}",
    )

    # The report landed but the ledger never recorded completion: wait for the
    # lease to lapse, then resume and reconcile via the status endpoint.
    await asyncio.sleep(2.6)
    state: Dict[str, Any] = {"invoked": []}
    resumed = await AgentSession.resume(
        client, channel, state_fn=lambda: {"invoked": list(state["invoked"])}, snapshot_interval=1
    )
    checks.check(
        "resume recovers the crashed session",
        resumed.status == "started" and resumed.step_count >= STEP_REPORT - 1,
        f"status={resumed.status} last_step={resumed.step_count}",
    )

    workspace = RepoWorkspace(workspace_dir)
    agent = RepoAgent(
        client, resumed, workspace, task_url, DryRunPolicy(correct=False),
        report_downstream_key="repo-report-crash", state_sink=state,
    )
    async with resumed:
        result = await agent.run(start_step=resumed.step_count + 1)

    checks.check(
        "resume re-invoked only the crashed step (1..N-1 not re-run)",
        result.invoked_steps == [STEP_REPORT],
        f"invoked={result.invoked_steps}",
    )
    count = _task_service_count(task_url, "repo-report-crash")
    checks.check(
        "the report did not fire twice on recovery (ambiguous window reconciled)",
        count == 1,
        f"applications={count}",
    )


async def _run_fork(
    client: ActaeClient,
    checks: Checks,
    task_url: str,
    run_id: str,
    baseline_channel: str,
    baseline_hash: str,
    use_llm: bool,
    reports_dir: Path,
) -> str:
    fork_channel = f"repo-{run_id}-fork"
    workspace_dir = reports_dir / f"{run_id}-fork-ws"
    seed_workspace(workspace_dir)

    state: Dict[str, Any] = {"invoked": []}
    # Inherit the baseline's prefix state, then apply the corrective intervention.
    fork = await AgentSession.resume(
        client,
        baseline_channel,
        fork_at_step=STEP_TEST,
        name=fork_channel,
        state_fn=lambda: {"invoked": list(state["invoked"])},
        snapshot_interval=1,
        intervention={"fix": "median", "reason": "even-length median averages the middles"},
    )
    workspace = RepoWorkspace(workspace_dir)
    # Replay the inherited prefix edit so the fork operates on the same files
    # the parent had at the boundary (the prefix itself is not re-run).
    workspace.edit_file("calc/stats.py", *WRONG_EDIT)
    # The intervention is the corrective action: the fork always applies the
    # known-correct fix (the model's role in LLM mode is the baseline attempt).
    policy = DryRunPolicy(correct=True)
    agent = RepoAgent(
        client, fork, workspace, task_url, policy,
        report_downstream_key=BASE_REPORT_DOWNSTREAM, state_sink=state,
    )
    async with fork:
        result = await agent.run(start_step=STEP_REACT_EDIT)

    checks.check(
        "the fork applies the correct fix and the tests pass",
        result.tests_passed and DEEP_FIX_MARKER in workspace.read_file("calc/stats.py"),
        f"tests_passed={result.tests_passed}",
    )
    if use_llm:
        checks.info(
            "fork vs baseline outcome",
            f"fork_hash_differs={result.final_hash != baseline_hash}",
        )
    else:
        checks.check(
            "the fork changed the outcome from the baseline",
            result.final_hash != baseline_hash,
            "fork hash differs from baseline hash",
        )
    checks.check(
        "the fork re-ran only steps after the boundary (prefix not re-invoked)",
        result.invoked_steps == [STEP_REACT_EDIT, STEP_REACT_EDIT + 1, STEP_REPORT],
        f"invoked={result.invoked_steps}",
    )
    # The fork's re-issued report must replay the parent's recorded result.
    replayed = await client.replay(fork_channel, cursor=0, limit=200, event_type="tool.replayed")
    checks.check(
        "the inherited report replayed instead of firing again",
        len(replayed) >= 1,
        f"tool.replayed={len(replayed)}",
    )
    count = _task_service_count(task_url, BASE_REPORT_DOWNSTREAM)
    checks.check(
        "the task service did not receive a second report for the inherited key",
        count == 1,
        f"applications={count}",
    )

    receipt = fork.fork_receipt
    checks.check(
        "the fork receipt is an exact, restorable boundary",
        bool(receipt)
        and bool(getattr(receipt, "restorable", False))
        and getattr(receipt, "requested_cursor", None) == getattr(receipt, "resolved_cursor", None)
        and bool(getattr(receipt, "source_state_sha256", "")),
        f"restorable={getattr(receipt, 'restorable', None)} "
        f"requested={getattr(receipt, 'requested_cursor', None)} "
        f"resolved={getattr(receipt, 'resolved_cursor', None)}",
    )
    return fork_channel


async def run(use_llm: bool, reports_dir: Path) -> int:
    run_id = uuid.uuid4().hex[:8]
    task_service = HttpEffectService()
    task_url = task_service.start()
    client = ActaeClient(api_key=ACTAE_API_KEY, endpoint=ACTAE_URL)
    checks = Checks()
    baseline_channel = ""
    fork_channel = ""
    try:
        await client.connect()
        print(f"Actae:   {ACTAE_URL}")
        print(f"Run:     {run_id}")
        print(f"Mode:    {'real LLM' if use_llm else 'deterministic dry-run'}")
        print(f"Service: {task_url}\n")

        print("baseline — a real workflow that fails to fix the bug")
        baseline_channel, baseline_hash = await _run_baseline(
            client, checks, task_url, run_id, use_llm, reports_dir
        )
        print("\ncrash + resume — the ambiguous side-effect window")
        await _run_crash_resume(client, checks, task_url, run_id, reports_dir)
        print("\nfork — a corrective fork that does not re-fire the effect")
        fork_channel = await _run_fork(
            client, checks, task_url, run_id, baseline_channel, baseline_hash, use_llm, reports_dir
        )
    finally:
        await client.disconnect()
        task_service.stop()

    report = {
        "run_id": run_id,
        "actae_url": ACTAE_URL,
        "mode": "llm" if use_llm else "dry-run",
        "baseline_channel": baseline_channel,
        "fork_channel": fork_channel,
        "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in checks.results],
        "passed": sum(1 for _, ok, _ in checks.results if ok),
        "total": len(checks.results),
        "verdict": "PASS" if checks.ok else "FAIL",
    }
    reports_dir.mkdir(parents=True, exist_ok=True)
    out = reports_dir / f"repo-agent-{run_id}.json"
    out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    (reports_dir / "latest.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print()
    print(f"{report['passed']}/{report['total']} checks — {report['verdict']}")
    print(f"wrote {out}")
    return 0 if checks.ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="repo-maintenance agent demo")
    parser.add_argument("--llm", action="store_true", help="use a real model for the edit decision")
    parser.add_argument(
        "--reports-dir", default=str(HERE / "reports"), help="where to write reports/workspaces"
    )
    args = parser.parse_args()
    return asyncio.run(run(args.llm, Path(args.reports_dir)))


if __name__ == "__main__":
    sys.exit(main())
