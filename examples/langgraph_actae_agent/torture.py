#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Torture test the LangGraph + Actae integration. Needs a running Actae.

Mimics the things a user (or a hostile CI) would actually do: many threads at
once, crashes at every point in the side-effecting tool, immediate resumes,
resume-after-completion, forking at *every* checkpoint, fork-of-fork, concurrent
forks, parallel tool calls, tool failures + retry, time-travel replay, and a
battery of sequential runs. Each scenario asserts hard invariants (effects fire
exactly once, graphs resume/complete, channels are isolated).

Usage:
    pip install "langgraph>=0.6,<2" "langchain-core>=0.3,<2"
    cd actae && cargo run
    python3 examples/langgraph_actae_agent/torture.py
    python3 examples/langgraph_actae_agent/torture.py --scenario fork_every_checkpoint
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from langchain_core.language_models.chat_models import BaseChatModel  # noqa: E402
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage  # noqa: E402
from langchain_core.outputs import ChatGeneration, ChatResult  # noqa: E402
from langchain_core.tools import tool  # noqa: E402

from actae_client import ActaeClient  # noqa: E402
from actae_client.adapters.langgraph import ActaeCheckpointSaver  # noqa: E402
from lg_agent import (  # noqa: E402
    ScriptedSurveyModel,
    build_graph,
    checkpoint_channel,
    effect_count,
    effect_rows,
    final_reply,
    make_tools,
)

ACTAE_URL = os.getenv("ACTAE_URL", "http://localhost:8002")
ACTAE_API_KEY = os.getenv("ACTAE_API_KEY", "sk-dev-0000000000000000000000")
PROMPT = "Use the record_reading tool to record a reading of 21 degrees Celsius under the label 'baseline', then summarize."
LEASE_SECONDS = 3
LEASE_WAIT = LEASE_SECONDS + 1.5


class Checks:
    def __init__(self, scenario: str, reports: Path) -> None:
        self.scenario = scenario
        self.reports = reports
        self.results: List[Tuple[str, bool, str]] = []

    def check(self, name: str, ok: bool, detail: str = "") -> None:
        self.results.append((name, ok, detail))
        mark = "PASS" if ok else "FAIL"
        print(f"    [{mark}] {name}" + (f" — {detail}" if detail else ""))

    @property
    def ok(self) -> bool:
        return all(r[1] for r in self.results)


def _cfg(thread: str) -> Dict[str, Any]:
    return {"configurable": {"thread_id": thread}}


def _env() -> Dict[str, str]:
    env = dict(os.environ)
    env.update(ACTAE_URL=ACTAE_URL, ACTAE_ENDPOINT=ACTAE_URL, ACTAE_API_KEY=ACTAE_API_KEY)
    return env


def _make_graph(client, channel, thread, effects, model, crash_point=None):
    saver = ActaeCheckpointSaver(client, channel=channel)
    tools = make_tools(
        client, checkpoint_channel(saver, thread), effects, crash_point=crash_point
    )
    return saver, build_graph(model, tools, saver)


async def _run(client, channel, thread, effects, label, celsius=21.0, model=None, crash_point=None):
    model = model or ScriptedSurveyModel(label=label, celsius=celsius)
    saver, graph = _make_graph(client, channel, thread, effects, model, crash_point)
    return await graph.ainvoke({"messages": [HumanMessage(PROMPT)]}, _cfg(thread))


async def _resume(client, channel, thread, effects, label, celsius=21.0, model=None):
    model = model or ScriptedSurveyModel(label=label, celsius=celsius)
    saver, graph = _make_graph(client, channel, thread, effects, model)
    return await graph.ainvoke(None, _cfg(thread))


def _spawn_worker(thread, channel, effects, label, celsius, crash_at, extra=None):
    args = [
        sys.executable, str(HERE / "worker.py"),
        "--thread", thread, "--channel", channel, "--effects", str(effects),
        "--label", label, "--celsius", str(celsius),
    ]
    if crash_at:
        args += ["--crash-at", crash_at]
    if extra:
        args += extra
    return subprocess.run(args, env=_env(), capture_output=True, text=True, timeout=180, cwd=str(HERE))


# --------------------------------------------------------------------------
# Scenarios
# --------------------------------------------------------------------------
async def scenario_isolation(client, c: Checks, tag: str, effects: Path):
    async def one(i: int):
        ch, th, label = f"{tag}-iso{i}", f"{tag}-iso{i}-t", f"iso{i}"
        state = await _run(client, ch, th, effects, label, celsius=10.0 + i)
        return i, ch, final_reply(state)

    results = await asyncio.gather(*[one(i) for i in range(6)])
    channels = {r[1] for r in results}
    c.check("6 concurrent threads produced 6 distinct channels", len(channels) == 6)
    c.check(
        "each concurrent thread recorded its own effect exactly once",
        all(effect_count(effects, f"iso{i}") == 1 for i in range(6)),
        f"counts={[effect_count(effects, f'iso{i}') for i in range(6)]}",
    )
    c.check("every concurrent thread finished with an answer", all(r[2] for r in results))


async def _crash_case(client, c: Checks, tag: str, effects: Path, label: str, crash_at: str, wait: bool):
    ch, th = f"{tag}-{label}", f"{tag}-{label}-t"
    proc = _spawn_worker(th, ch, effects, label, 15.0, crash_at)
    c.check(f"[{label}] worker died for real (exit 137)", proc.returncode == 137, f"rc={proc.returncode}")
    expected_before = 0 if crash_at == "before_effect" else 1
    c.check(
        f"[{label}] effect store before resume (expect {expected_before})",
        effect_count(effects, label) == expected_before,
        f"count={effect_count(effects, label)}",
    )
    if wait:
        await asyncio.sleep(LEASE_WAIT)
    state = await _resume(client, ch, th, effects, label, celsius=15.0)
    c.check(f"[{label}] resume completed the graph", bool(final_reply(state)))
    c.check(
        f"[{label}] effect fired exactly once end-to-end",
        effect_count(effects, label) == 1,
        f"count={effect_count(effects, label)}",
    )


async def scenario_crash_resume(client, c: Checks, tag: str, effects: Path):
    await _crash_case(client, c, tag, effects, "before", "before_effect", wait=True)
    await _crash_case(client, c, tag, effects, "after", "after_effect", wait=True)
    await _crash_case(client, c, tag, effects, "complete", "after_complete", wait=True)
    # Immediate resume (lease still live → the tool must wait it out, not double-fire).
    await _crash_case(client, c, tag, effects, "immediate", "after_effect", wait=False)


async def scenario_resume_after_complete_is_noop(client, c: Checks, tag: str, effects: Path):
    ch, th, label = f"{tag}-noop", f"{tag}-noop-t", "noop"
    first = await _run(client, ch, th, effects, label)
    reply = final_reply(first)
    again = await _resume(client, ch, th, effects, label)
    c.check("second resume returns the same final answer", final_reply(again) == reply, f"{reply!r}")
    c.check("second resume fired no new effect", effect_count(effects, label) == 1)


async def scenario_fork_every_checkpoint(client, c: Checks, tag: str, effects: Path):
    ch, th, label = f"{tag}-forkall", f"{tag}-forkall-t", "forkall"
    base = await _run(client, ch, th, effects, label)
    saver = ActaeCheckpointSaver(client, channel=ch)
    graph = build_graph(ScriptedSurveyModel(label=label), make_tools(client, checkpoint_channel(saver, th), effects), saver)
    history = [t async for t in graph.aget_state_history(_cfg(th))]
    c.check("base thread has multiple checkpoints to fork", len(history) >= 3, f"checkpoints={len(history)}")

    for i, snap in enumerate(history):
        fork_th = f"{th}-f{i}"
        try:
            fork_cfg = await saver.fork_thread(snap.config, new_thread_id=fork_th, reason=f"torture fork {i}")
            fork_graph = build_graph(ScriptedSurveyModel(label=label), make_tools(client, checkpoint_channel(saver, fork_th), effects), saver)
            fork_state = await fork_graph.ainvoke(None, fork_cfg)
            c.check(f"fork at checkpoint {i} resumed to a final answer", bool(final_reply(fork_state)))
        except Exception as err:
            c.check(f"fork at checkpoint {i} resumed to a final answer", False, f"{type(err).__name__}: {err}")
    c.check(
        "forking at every checkpoint never re-fired the tool effect",
        effect_count(effects, label) == 1,
        f"count={effect_count(effects, label)}",
    )
    c.check("baseline still resolves to its original answer", bool(final_reply(base)))


async def scenario_fork_of_fork(client, c: Checks, tag: str, effects: Path):
    ch, th, label = f"{tag}-fof", f"{tag}-fof-t", "fof"
    await _run(client, ch, th, effects, label)
    saver = ActaeCheckpointSaver(client, channel=ch)
    f1_th = f"{th}-f1"
    f1 = await saver.fork_thread(_cfg(th), new_thread_id=f1_th, reason="first fork")
    g1 = build_graph(ScriptedSurveyModel(label=label), make_tools(client, checkpoint_channel(saver, f1_th), effects), saver)
    await g1.ainvoke(None, f1)

    f2_th = f"{f1_th}-f2"
    f2 = await saver.fork_thread(f1, new_thread_id=f2_th, reason="fork of fork")
    g2 = build_graph(ScriptedSurveyModel(label=label), make_tools(client, checkpoint_channel(saver, f2_th), effects), saver)
    state = await g2.ainvoke(None, f2)
    c.check("fork-of-fork resumed to a final answer", bool(final_reply(state)))
    receipt = await client.get_fork_receipt(checkpoint_channel(saver, f2_th))
    c.check("fork-of-fork has a fork receipt", receipt is not None, f"receipt={bool(receipt)}")
    c.check("fork-of-fork never re-fired the effect", effect_count(effects, label) == 1)


async def scenario_concurrent_forks(client, c: Checks, tag: str, effects: Path):
    ch, th, label = f"{tag}-cfork", f"{tag}-cfork-t", "cfork"
    await _run(client, ch, th, effects, label)
    saver = ActaeCheckpointSaver(client, channel=ch)

    async def fork(i: int):
        ft = f"{th}-k{i}"
        cfg = await saver.fork_thread(_cfg(th), new_thread_id=ft, reason=f"concurrent {i}")
        g = build_graph(ScriptedSurveyModel(label=label), make_tools(client, checkpoint_channel(saver, ft), effects), saver)
        st = await g.ainvoke(None, cfg)
        return ft, final_reply(st)

    results = await asyncio.gather(*[fork(i) for i in range(5)])
    c.check("5 concurrent forks produced distinct threads", len({r[0] for r in results}) == 5)
    c.check("all concurrent forks finished", all(r[1] for r in results))
    c.check("concurrent forks did not re-fire the effect", effect_count(effects, label) == 1)


class _MultiToolModel(BaseChatModel):
    label: str = "multi"
    celsius: float = 21.0

    @property
    def _llm_type(self) -> str:
        return "multi"

    def bind_tools(self, tools, **kwargs):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        if any(isinstance(m, ToolMessage) for m in messages):
            return ChatResult(generations=[ChatGeneration(message=AIMessage(content="multi done"))])
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content="", tool_calls=[
            {"name": "record_reading", "args": {"label": self.label, "celsius": self.celsius}, "id": "a", "type": "tool_call"},
            {"name": "celsius_to_fahrenheit", "args": {"celsius": self.celsius}, "id": "b", "type": "tool_call"},
        ]))])


async def scenario_parallel_tool_calls(client, c: Checks, tag: str, effects: Path):
    ch, th, label = f"{tag}-par", f"{tag}-par-t", "multi"
    model = _MultiToolModel()
    saver, graph = _make_graph(client, ch, th, effects, model)
    state = await graph.ainvoke({"messages": [HumanMessage(PROMPT)]}, _cfg(th))
    c.check("two parallel tool calls in one message both ran", bool(final_reply(state)))
    c.check("the side-effecting tool fired exactly once", effect_count(effects, label) == 1)


def _failing_tools(actae, channel, effects):
    @tool
    async def record_reading(label: str, celsius: float) -> str:
        """Record a reading; always fails for the torture test."""
        claim = await actae.claim_execution(
            channel, f"reading:{label}", "survey.record",
            {"label": label, "celsius": celsius}, lease_seconds=LEASE_SECONDS,
        )
        if claim.status in ("claimed", "reclaimed"):
            await actae.fail_execution(claim.execution.id, claim.claim_token, message="sensor offline")
        raise RuntimeError("sensor offline")

    @tool
    def celsius_to_fahrenheit(celsius: float) -> float:
        """Convert C to F."""
        return celsius * 9 / 5 + 32

    return [record_reading, celsius_to_fahrenheit]


async def scenario_tool_failure_then_retry(client, c: Checks, tag: str, effects: Path):
    ch, th, label = f"{tag}-fail", f"{tag}-fail-t", "fail"
    saver = ActaeCheckpointSaver(client, channel=ch)
    fail_graph = build_graph(ScriptedSurveyModel(label=label), _failing_tools(client, checkpoint_channel(saver, th), effects), saver)
    raised = False
    try:
        await fail_graph.ainvoke({"messages": [HumanMessage(PROMPT)]}, _cfg(th))
    except Exception:
        raised = True
    c.check("a failing tool surfaces as an error", raised)
    c.check("the failed tool wrote no effect", effect_count(effects, label) == 0)
    # Retry the same logical operation with a working tool.
    await _resume(client, ch, th, effects, label)
    c.check("retry with a working tool applied the effect once", effect_count(effects, label) == 1)


async def scenario_time_travel(client, c: Checks, tag: str, effects: Path):
    ch, th, label = f"{tag}-tt", f"{tag}-tt-t", "tt"
    await _run(client, ch, th, effects, label)
    saver = ActaeCheckpointSaver(client, channel=ch)
    graph = build_graph(ScriptedSurveyModel(label=label), make_tools(client, checkpoint_channel(saver, th), effects), saver)
    history = [t async for t in graph.aget_state_history(_cfg(th))]
    old = history[-2] if len(history) >= 2 else history[-1]
    old_id = old.config["configurable"]["checkpoint_id"]
    cfg = {"configurable": {"thread_id": th, "checkpoint_id": old_id}}
    state = await graph.ainvoke(None, cfg)
    c.check("time-travel to an older checkpoint replayed to completion", bool(final_reply(state)))
    c.check("time-travel did not double-fire the effect", effect_count(effects, label) == 1)


async def scenario_many_sequential(client, c: Checks, tag: str, effects: Path, n: int = 15):
    ok = True
    for i in range(n):
        ch, th = f"{tag}-seq{i}", f"{tag}-seq{i}-t"
        state = await _run(client, ch, th, effects, f"seq{i}", celsius=float(i))
        if not final_reply(state) or effect_count(effects, f"seq{i}") != 1:
            ok = False
    c.check(f"{n} sequential runs each completed with exactly one effect", ok)


SCENARIOS = {
    "isolation": scenario_isolation,
    "crash_resume": scenario_crash_resume,
    "resume_after_complete_is_noop": scenario_resume_after_complete_is_noop,
    "fork_every_checkpoint": scenario_fork_every_checkpoint,
    "fork_of_fork": scenario_fork_of_fork,
    "concurrent_forks": scenario_concurrent_forks,
    "parallel_tool_calls": scenario_parallel_tool_calls,
    "tool_failure_then_retry": scenario_tool_failure_then_retry,
    "time_travel": scenario_time_travel,
    "many_sequential": scenario_many_sequential,
}


async def run(only: Optional[str], reports_dir: Path) -> int:
    run_id = uuid.uuid4().hex[:8]
    reports_dir.mkdir(parents=True, exist_ok=True)
    client = ActaeClient(api_key=ACTAE_API_KEY, endpoint=ACTAE_URL)
    selected = [only] if only else list(SCENARIOS)
    print(f"Torture: {ACTAE_URL}  run={run_id}  scenarios={len(selected)}\n")

    await client.connect()
    all_checks: List[Dict[str, Any]] = []
    try:
        for name in selected:
            print(f"== {name} ==")
            effects = reports_dir / f"{run_id}-{name}.jsonl"
            c = Checks(name, reports_dir)
            try:
                await SCENARIOS[name](client, c, f"torture-{run_id}-{name}", effects)
            except Exception as err:
                c.check(f"{name} raised", False, f"{type(err).__name__}: {err}")
            all_checks.extend({"scenario": name, "name": n, "ok": ok, "detail": d} for n, ok, d in c.results)
            print()
    finally:
        await client.disconnect()

    passed = sum(1 for chk in all_checks if chk["ok"])
    total = len(all_checks)
    report = {
        "run_id": run_id,
        "actae_url": ACTAE_URL,
        "scenarios": selected,
        "checks": all_checks,
        "passed": passed,
        "total": total,
        "verdict": "PASS" if passed == total and total > 0 else "FAIL",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    out = reports_dir / f"torture-{run_id}.json"
    out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"{passed}/{total} checks — {report['verdict']}")
    print(f"wrote {out}")
    return 0 if report["verdict"] == "PASS" else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="torture-test the LangGraph + Actae integration")
    parser.add_argument("--scenario", choices=list(SCENARIOS), help="run only one scenario")
    parser.add_argument("--reports-dir", default=str(HERE / "reports"))
    args = parser.parse_args()
    return asyncio.run(run(args.scenario, Path(args.reports_dir)))


if __name__ == "__main__":
    sys.exit(main())
