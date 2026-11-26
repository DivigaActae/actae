#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""OSS repo agent demo: a real agent on a real public repository.

Clones a pinned public repo (default: ``itsdangerous``, BSD-3-Clause),
reintroduces a deterministic bug, and runs an 8-step software-engineering agent
with the repo's **real** test file as the oracle:

  baseline  clone → survey → locate → edit (plausible but wrong) → test (fails)
            → react (gives up) → test (fails) → report
  fork      forked at the failing-test step; applies the correct fix
            → test (passes) → report (inherited report replays, not re-fires)
  compare   server-side "what changed?" between the two runs
  promote   the winning fork is promoted into its parent

Only public, permissively licensed code is cloned — no customer/work data.

Usage:
    pip install -e ./sdks/python && pip install pytest
    cd actae && cargo run
    python3 examples/oss_repo_agent/demo.py                 # deterministic
    DEEPSEEK_API_KEY=... python3 examples/oss_repo_agent/demo.py --llm
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
from typing import Any, Dict, List, Optional, Tuple

HERE = Path(__file__).resolve().parent
_EXAMPLES = HERE.parent
for p in (str(HERE), str(_EXAMPLES)):
    if p not in sys.path:
        sys.path.insert(0, p)

from oss_agent import (  # noqa: E402
    STEP_REACT_EDIT,
    STEP_REACT_TEST,
    STEP_REPORT,
    STEP_TEST,
    DeterministicPolicy,
    LLMPolicy,
    OssRepoAgent,
)
from oss_tasks import DEFAULT_TASK, get_task, task_names  # noqa: E402
from oss_tools import RepoWorkspace, apply_mutation, clone_repo, prepare_workspace  # noqa: E402

from actae_client import ActaeClient  # noqa: E402
from actae_client.session import AgentSession  # noqa: E402

ACTAE_URL = os.getenv("ACTAE_URL", "http://localhost:8002")
ACTAE_API_KEY = os.getenv("ACTAE_API_KEY", "sk-dev-0000000000000000000000")


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


def _git_commit(repo: Path) -> str:
    try:
        return subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout.strip()
    except Exception:
        return ""


def _seed(task: Dict[str, Any], cache: Path, dest: Path) -> RepoWorkspace:
    """Fresh workspace = cached checkout + the task's reintroduced bug."""
    prepare_workspace(cache, dest)
    apply_mutation(dest, task["target_file"], task["mutation"])
    return RepoWorkspace(dest, pythonpath=task.get("pythonpath"))


async def _run_baseline(
    client: ActaeClient,
    checks: Checks,
    task: Dict[str, Any],
    cache: Path,
    commit: str,
    run_id: str,
    use_llm: bool,
    reports_dir: Path,
) -> Tuple[str, str]:
    channel = f"oss-{run_id}-base"
    workspace = _seed(task, cache, reports_dir / f"{run_id}-base-ws")
    state: Dict[str, Any] = {"invoked": []}
    session = AgentSession(client, channel, state_fn=lambda: {"invoked": list(state["invoked"])}, snapshot_interval=1)
    policy = LLMPolicy() if use_llm else DeterministicPolicy(correct=False)
    agent = OssRepoAgent(
        client, session, workspace, task, policy,
        repo_commit=commit,
        report_path=reports_dir / f"{channel}-report.json",
        state_sink=state,
    )
    async with session:
        result = await agent.run(start_step=1)

    if use_llm:
        checks.info("baseline outcome (model-driven decision)", f"tests_passed={result.tests_passed}")
    else:
        checks.check(
            "baseline leaves the failing test unresolved (the fork point)",
            not result.tests_passed,
            f"tests_passed={result.tests_passed}",
        )
    checks.check(
        "baseline recorded the report exactly once",
        (reports_dir / f"{channel}-report.json").exists(),
        "report file written",
    )
    return channel, result.final_hash


async def _run_fork(
    client: ActaeClient,
    checks: Checks,
    task: Dict[str, Any],
    cache: Path,
    commit: str,
    run_id: str,
    baseline_channel: str,
    baseline_hash: str,
    reports_dir: Path,
    use_llm: bool,
) -> Tuple[str, Any, str]:
    fork_channel = f"oss-{run_id}-fork"
    workspace = _seed(task, cache, reports_dir / f"{run_id}-fork-ws")
    # Replay the inherited prefix edit so the fork's workspace matches the
    # parent at the boundary (the prefix itself is not re-run).
    workspace.edit_file(task["target_file"], *task["wrong_fix"])

    state: Dict[str, Any] = {"invoked": []}
    fork = await AgentSession.resume(
        client,
        baseline_channel,
        fork_at_step=STEP_TEST,
        name=fork_channel,
        state_fn=lambda: {"invoked": list(state["invoked"])},
        snapshot_interval=1,
        intervention={"fix": "lstrip", "reason": "remove leading zero bytes"},
    )
    agent = OssRepoAgent(
        client, fork, workspace, task, DeterministicPolicy(correct=True),
        repo_commit=commit,
        report_path=reports_dir / f"{fork_channel}-report.json",
        state_sink=state,
    )
    async with fork:
        result = await agent.run(start_step=STEP_REACT_EDIT)

    checks.check(
        "the fork applies the correct fix and the real tests pass",
        result.tests_passed and task["correct_line"] in workspace.read_file(task["target_file"]),
        f"tests_passed={result.tests_passed}",
    )
    if use_llm:
        # The model may have already fixed the baseline, in which case the fork
        # legitimately matches it. Report rather than assert.
        checks.info(
            "fork outcome vs baseline (model-driven baseline)",
            f"baseline_passed_so_matches={result.final_hash == baseline_hash}",
        )
    else:
        checks.check(
            "the fork changed the outcome from the baseline",
            result.final_hash != baseline_hash,
            "fork target hash differs from baseline",
        )
    checks.check(
        "the fork re-ran only steps after the boundary (prefix not re-invoked)",
        result.invoked_steps == [STEP_REACT_EDIT, STEP_REACT_TEST, STEP_REPORT],
        f"invoked={result.invoked_steps}",
    )
    replayed = await client.replay(fork_channel, cursor=0, limit=200, event_type="tool.replayed")
    checks.check(
        "the inherited report replayed instead of firing again",
        len(replayed) >= 1,
        f"tool.replayed={len(replayed)}",
    )
    receipt = fork.fork_receipt
    checks.check(
        "the fork receipt is an exact, restorable boundary",
        bool(receipt)
        and bool(getattr(receipt, "restorable", False))
        and getattr(receipt, "requested_cursor", None) == getattr(receipt, "resolved_cursor", None)
        and bool(getattr(receipt, "source_state_sha256", "")),
        f"restorable={getattr(receipt, 'restorable', None)}",
    )
    return fork_channel, fork, result.final_hash


async def _compare_and_promote(
    client: ActaeClient, checks: Checks, baseline: str, fork: str, fork_session: Any
) -> Optional[Dict[str, Any]]:
    compare = await client.compare_channels(baseline, fork)
    state_diff = (compare or {}).get("state_diff") or {}
    entries = state_diff.get("entries") or []
    checks.check(
        "server-side compare returns a real diff between the two runs",
        bool(compare)
        and compare.get("left_channel_id") == baseline
        and compare.get("right_channel_id") == fork
        and (len(entries) > 0 or compare.get("output_changed") is True),
        f"state_diff_entries={len(entries)} output_changed={compare.get('output_changed') if compare else None}",
    )
    try:
        promoted = await fork_session.promote()
        ok = bool(promoted) and promoted.get("status") == "promoted"
        checks.check("the winning fork is promoted into its parent", ok, f"status={promoted.get('status') if promoted else None}")
    except Exception as err:  # pragma: no cover - surfaced, not fatal
        checks.check("the winning fork is promoted into its parent", False, f"{type(err).__name__}: {err}")
    return compare


async def run(use_llm: bool, task_name: str, refresh: bool, reports_dir: Path) -> int:
    task = get_task(task_name)
    run_id = uuid.uuid4().hex[:8]
    cache = HERE / ".cache" / task_name

    print(f"Actae:   {ACTAE_URL}")
    print(f"Task:    {task_name}")
    print(f"Repo:    {task['repo_url']} ({task['license']})")
    print(f"Mode:    {'real LLM' if use_llm else 'deterministic dry-run'}\n")

    print("clone — fetch the real public repository")
    clone_repo(task["repo_url"], cache, refresh=refresh)
    commit = _git_commit(cache)
    print(f"  checked out {task['repo_url']} @ {commit or '?'}\n")

    client = ActaeClient(api_key=ACTAE_API_KEY, endpoint=ACTAE_URL)
    checks = Checks()
    baseline_channel = fork_channel = ""
    compare: Optional[Dict[str, Any]] = None
    try:
        await client.connect()
        print("baseline — a real workflow that fails to fix the bug")
        baseline_channel, baseline_hash = await _run_baseline(
            client, checks, task, cache, commit, run_id, use_llm, reports_dir
        )
        print("\nfork — a corrective fork at the failing-test step")
        fork_channel, fork_session, _ = await _run_fork(
            client, checks, task, cache, commit, run_id, baseline_channel, baseline_hash, reports_dir, use_llm
        )
        print("\ncompare + promote — pick the winner")
        compare = await _compare_and_promote(client, checks, baseline_channel, fork_channel, fork_session)
    finally:
        await client.disconnect()

    report = {
        "run_id": run_id,
        "actae_url": ACTAE_URL,
        "mode": "llm" if use_llm else "dry-run",
        "task": task_name,
        "repo": task["repo_url"],
        "license": task["license"],
        "commit": commit,
        "baseline_channel": baseline_channel,
        "fork_channel": fork_channel,
        "compare_state_diff_entries": len(((compare or {}).get("state_diff") or {}).get("entries") or []),
        "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in checks.results],
        "passed": sum(1 for _, ok, _ in checks.results if ok),
        "total": len(checks.results),
        "verdict": "PASS" if checks.ok else "FAIL",
    }
    reports_dir.mkdir(parents=True, exist_ok=True)
    out = reports_dir / f"oss-repo-agent-{run_id}.json"
    out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    (reports_dir / "latest.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print()
    print(f"{report['passed']}/{report['total']} checks — {report['verdict']}")
    print(f"wrote {out}")
    return 0 if checks.ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="OSS repo agent demo (real public repository)")
    parser.add_argument("--llm", action="store_true", help="use a real model for the fix decision")
    parser.add_argument("--task", default=DEFAULT_TASK, choices=task_names(), help="task registry entry")
    parser.add_argument("--refresh", action="store_true", help="re-clone instead of using the cache")
    parser.add_argument("--reports-dir", default=str(HERE / "reports"), help="where to write reports/workspaces")
    args = parser.parse_args()
    return asyncio.run(run(args.llm, args.task, args.refresh, Path(args.reports_dir)))


if __name__ == "__main__":
    sys.exit(main())
