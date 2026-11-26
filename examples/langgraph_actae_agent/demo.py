#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""LangGraph ReAct agent made durable and forkable by Actae.

A real LangGraph tool-calling agent (``create_react_agent``) compiled with
Actae's ``ActaeCheckpointSaver``. Three scenarios demonstrate Actae's power on
an agent whose own control loop is the framework's:

  baseline        the agent runs, calls a side-effecting tool, and answers.
  crash + resume  a worker is killed (exit 137) inside the tool, after the
                  effect landed but before the ledger recorded it; a new process
                  resumes the graph from its Actae checkpoint. The completed
                  effect does NOT fire twice (the execution ledger reconciles).
  fork            fork the thread at the exact checkpoint after the tool, change
                  the final decision, run only the tail (the tool is not
                  re-run), compare the two runs server-side and promote.

Deterministic by default (no API key). ``--llm`` uses DeepSeek for the baseline.

Usage:
    pip install -e ./sdks/python
    pip install "langgraph>=0.6,<2" "langchain-core>=0.3,<2"   # + optional langchain-openai
    cd actae && cargo run
    python3 examples/langgraph_actae_agent/demo.py
    DEEPSEEK_API_KEY=... python3 examples/langgraph_actae_agent/demo.py --llm
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
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from langchain_core.messages import HumanMessage, ToolMessage  # noqa: E402

from actae_client import ActaeClient  # noqa: E402
from actae_client.adapters.langgraph import ActaeCheckpointSaver  # noqa: E402
from lg_agent import (  # noqa: E402
    ScriptedSurveyModel,
    build_graph,
    checkpoint_channel,
    deepseek_model,
    effect_count,
    effect_rows,
    final_reply,
    make_tools,
)

ACTAE_URL = os.getenv("ACTAE_URL", "http://localhost:8002")
ACTAE_API_KEY = os.getenv("ACTAE_API_KEY", "sk-dev-0000000000000000000000")
PROMPT = (
    "Use the record_reading tool to record a reading of 21 degrees Celsius "
    "under the label 'baseline', then summarize the result."
)
LEASE_WAIT_SECS = 3.4  # > the tool's 3s execution lease


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


def _worker_env() -> Dict[str, str]:
    env = dict(os.environ)
    env["ACTAE_URL"] = ACTAE_URL
    env["ACTAE_ENDPOINT"] = ACTAE_URL
    env["ACTAE_API_KEY"] = ACTAE_API_KEY
    return env


def _cfg(thread_id: str) -> Dict[str, Any]:
    return {"configurable": {"thread_id": thread_id}}


async def _run_baseline(
    client: ActaeClient,
    saver: ActaeCheckpointSaver,
    checks: Checks,
    effects: Path,
    channel: str,
    thread: str,
    use_llm: bool,
) -> Dict[str, Any]:
    model = deepseek_model() if use_llm else ScriptedSurveyModel(label="baseline", celsius=21.0)
    tools = make_tools(client, checkpoint_channel(saver, thread), effects)
    graph = build_graph(model, tools, saver)
    state = await graph.ainvoke({"messages": [HumanMessage(PROMPT)]}, _cfg(thread))
    reply = final_reply(state)
    if use_llm:
        checks.info("baseline outcome (model-driven)", f"reply={reply!r} effects={effect_count(effects,'baseline')}")
    else:
        checks.check(
            "baseline: agent called the side-effecting tool exactly once and answered",
            effect_count(effects, "baseline") == 1 and bool(reply),
            f"effects={effect_count(effects,'baseline')} reply={reply!r}",
        )
    return state


async def _crash_and_resume(
    client: ActaeClient,
    saver: ActaeCheckpointSaver,
    checks: Checks,
    effects: Path,
    channel: str,
    thread: str,
) -> None:
    proc = subprocess.run(
        [
            sys.executable, str(HERE / "worker.py"),
            "--thread", thread, "--channel", channel, "--effects", str(effects),
            "--label", "crash", "--celsius", "18", "--crash",
        ],
        env=_worker_env(),
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(HERE),
    )
    checks.check(
        "worker really died inside the tool (exit 137)",
        proc.returncode == 137,
        f"rc={proc.returncode}",
    )
    checks.check(
        "the side effect landed exactly once before the crash",
        effect_count(effects, "crash") == 1,
        f"effects={effect_count(effects,'crash')}",
    )

    # Let the crashed claim's lease lapse, then resume from the checkpoint.
    await asyncio.sleep(LEASE_WAIT_SECS)
    tools = make_tools(client, checkpoint_channel(saver, thread), effects)
    graph = build_graph(ScriptedSurveyModel(label="crash", celsius=18.0), tools, saver)
    resumed = await graph.ainvoke(None, _cfg(thread))
    checks.check(
        "resume continued the graph from its Actae checkpoint",
        bool(final_reply(resumed)),
        f"reply={final_reply(resumed)!r}",
    )
    checks.check(
        "the effect did NOT fire twice on resume (ledger reconciled)",
        effect_count(effects, "crash") == 1,
        f"effects={effect_count(effects,'crash')}",
    )


async def _fork_and_promote(
    client: ActaeClient,
    saver: ActaeCheckpointSaver,
    checks: Checks,
    effects: Path,
    channel: str,
    base_thread: str,
    base_state: Dict[str, Any],
    fork_thread: str,
) -> Optional[Dict[str, Any]]:
    base_graph = build_graph(ScriptedSurveyModel(label="baseline", celsius=21.0), make_tools(client, checkpoint_channel(saver, base_thread), effects), saver)
    history = [t async for t in base_graph.aget_state_history(_cfg(base_thread))]
    target = next(
        (t for t in history
         if t.next and t.values.get("messages") and isinstance(t.values["messages"][-1], ToolMessage)),
        None,
    )
    checks.check(
        "found the fork point: the checkpoint after the tool, before the final answer",
        target is not None,
    )
    if target is None:
        return None

    fork_cfg = await saver.fork_thread(
        target.config, new_thread_id=fork_thread, reason="refine the final recommendation"
    )
    fork_model = ScriptedSurveyModel(
        label="baseline", celsius=21.0,
        final_note="FORK: escalate — 21C is below the 22C target.",
    )
    fork_graph = build_graph(fork_model, make_tools(client, checkpoint_channel(saver, fork_thread), effects), saver)
    effects_before = len(effect_rows(effects))
    fork_state = await fork_graph.ainvoke(None, fork_cfg)

    checks.check(
        "the fork changed the final decision",
        final_reply(fork_state) != final_reply(base_state),
        f"fork={final_reply(fork_state)!r}",
    )
    checks.check(
        "the fork did not re-run the tool (no new side effect was fired)",
        len(effect_rows(effects)) == effects_before,
        f"effects {effects_before} -> {len(effect_rows(effects))}",
    )

    base_channel = checkpoint_channel(saver, base_thread)
    fork_channel = checkpoint_channel(saver, fork_thread)
    compare = await client.compare_channels(base_channel, fork_channel)
    entries = (((compare or {}).get("state_diff") or {}).get("entries")) or []
    checks.check(
        "server-side compare shows the divergence between baseline and fork",
        bool(compare)
        and compare.get("left_channel_id") == base_channel
        and compare.get("right_channel_id") == fork_channel
        and (len(entries) > 0 or compare.get("output_changed") is True),
        f"state_diff_entries={len(entries)}",
    )
    try:
        promoted = await client.promote_channel(fork_channel)
        checks.check(
            "the winning fork is promoted into its parent",
            bool(promoted) and promoted.get("status") == "promoted",
            f"status={promoted.get('status') if promoted else None}",
        )
    except Exception as err:  # pragma: no cover - surfaced, not fatal
        checks.check("the winning fork is promoted into its parent", False, f"{type(err).__name__}: {err}")
    return compare


async def run(use_llm: bool, reports_dir: Path) -> int:
    run_id = uuid.uuid4().hex[:8]
    effects = reports_dir / f"effects-{run_id}.jsonl"
    reports_dir.mkdir(parents=True, exist_ok=True)
    if effects.exists():
        effects.unlink()

    channel = f"lg-survey-{run_id}"
    base_thread = f"lg-survey-{run_id}-base"
    crash_thread = f"lg-survey-{run_id}-crash"
    fork_thread = f"lg-survey-{run_id}-fork"

    print(f"Actae:  {ACTAE_URL}")
    print(f"Agent:  LangGraph create_react_agent + ActaeCheckpointSaver")
    print(f"Mode:   {'real LLM (DeepSeek) baseline' if use_llm else 'deterministic dry-run'}\n")

    client = ActaeClient(api_key=ACTAE_API_KEY, endpoint=ACTAE_URL)
    checks = Checks()
    compare: Optional[Dict[str, Any]] = None
    try:
        await client.connect()
        saver = ActaeCheckpointSaver(client, channel=channel)

        print("baseline — a real LangGraph ReAct agent run")
        base_state = await _run_baseline(client, saver, checks, effects, channel, base_thread, use_llm)

        print("\ncrash + resume — durable execution across a hard process kill")
        await _crash_and_resume(client, saver, checks, effects, channel, crash_thread)

        print("\nfork — refine the final decision at the exact checkpoint")
        compare = await _fork_and_promote(
            client, saver, checks, effects, channel, base_thread, base_state, fork_thread
        )
    finally:
        await client.disconnect()

    report = {
        "run_id": run_id,
        "actae_url": ACTAE_URL,
        "mode": "llm" if use_llm else "dry-run",
        "framework": "langgraph",
        "channel": channel,
        "threads": {"baseline": base_thread, "crash": crash_thread, "fork": fork_thread},
        "events": 0,
        "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in checks.results],
        "passed": sum(1 for _, ok, _ in checks.results if ok),
        "total": len(checks.results),
        "verdict": "PASS" if checks.ok else "FAIL",
    }
    out = reports_dir / f"langgraph-actae-{run_id}.json"
    out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    (reports_dir / "latest.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print()
    print(f"{report['passed']}/{report['total']} checks — {report['verdict']}")
    print(f"wrote {out}")
    return 0 if checks.ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="LangGraph + Actae durable agent demo")
    parser.add_argument("--llm", action="store_true", help="use DeepSeek for the baseline run")
    parser.add_argument("--reports-dir", default=str(HERE / "reports"), help="where to write reports/effects")
    args = parser.parse_args()
    return asyncio.run(run(args.llm, Path(args.reports_dir)))


if __name__ == "__main__":
    sys.exit(main())
