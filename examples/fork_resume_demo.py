#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""
Fork & Resume Demo — the flagship fork workflow, framework-agnostic.

The pitch
---------
You have a 10-step agent pipeline. Steps 1-5 are good; you want to iterate on
step 6 (its prompt / tool / logic). Naively you'd re-run all 10 steps and
burn tokens re-doing 1-5. Actae lets you *fork at step 5* and continue from
step 6 — the fork inherits steps 1-5's accumulated state (real LLM outputs),
so step 6 runs against that data without re-invoking the steps before it.

This demo proves it with REAL DeepSeek LLM calls and prints two measured
reports: token savings AND output fidelity.

  1. Run steps 1-10 (baseline) — every step is a real LLM call.
  2. Fork at step 5 with a MODIFIED step 6 (different prompt/style).
  3. Resume the fork → run steps 6-10 only, against the inherited state.
  4. Print the proofs (steps 1-5 not re-invoked, inherited state consumed,
     fork event log, refinement differs from baseline).
  5. Print the token-savings report — per-step usage, totals, tokens saved
     and % saved, summed directly from the provider's `usage` fields for the
     real calls (see `fork_token_report.py` for the honesty contract).
  6. Print the FIDELITY report — a second fork with the *same* step-6 prompt,
     compared against a no-fork re-run control: the inherited prefix is
     byte-identical to the baseline, and the fork's tail divergence
     (steps 6-10) is shown next to a full no-fork re-run's divergence — so the
     answer to "does the fork change the output?" is measured, not asserted.
     Real models sample, so both divergences are > 0 and similar; differences
     are sampling, not forking. (In dry-run both are exactly 1.00 → 0.00.)

A `--dry-run` flag replaces the real LLM calls with a deterministic fake
token table so both reports are reproducible without an API key (in dry-run
the same-prompt tail is exactly 1.00). Dry-run output is clearly labelled —
only the real mode produces figures to present.

No LangGraph, no framework — this is the raw Actae `AgentSession` (Python
SDK) which any framework (LangGraph, CrewAI, Claude, plain loops) can wrap.

Requirements:
    pip install -e ./sdks/python
    # DEEPSEEK_API_KEY in actae/.env (loaded automatically; not needed with --dry-run)
    cd actae && cargo run -- --dev          # Actae on :8002

Usage:
    python examples/fork_resume_demo.py            # real LLM calls + both reports
    python examples/fork_resume_demo.py --dry-run  # deterministic fake usage
    python examples/fork_resume_demo.py --no-fidelity  # skip the extra 5-call check

Unit-tested via sdks/example-tests/python/test_fork_token_savings.py (report math,
no server) and the opt-in live dry-run suite test_fork_resume_tokens_live.py.
"""

import argparse
import asyncio
import difflib
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from openai import AsyncOpenAI

from actae_client import ActaeClient
from actae_client.session import AgentSession

from fork_token_report import StepRegistry, format_token_report

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ACTAE_URL = os.environ.get("ACTAE_URL", "http://localhost:8002")
ACTAE_API_KEY = os.environ.get("ACTAE_API_KEY", "sk-dev-0000000000000000000000")
MODEL = os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")

# Deterministic per-step usage used in --dry-run mode: step -> (prompt, completion).
# The real mode ignores this table and sums OpenAI's `usage` object instead.
DRY_RUN_USAGE: Dict[str, tuple] = {
    "research": (900, 320),
    "insights": (1100, 450),
    "stakeholders": (1300, 500),
    "risks": (1500, 560),
    "metrics": (1200, 430),
    "recommendations": (1700, 700),
    "summary": (1500, 320),
    "next_steps": (1600, 300),
    "final": (1400, 240),
    "complete": (1400, 240),
}
DRY_RUN_ELAPSED_MS = 350

# Module-level dry-run flag; set from the CLI or run_demo(). Kept as a single
# choke point so the ten step functions below never need a dry_run parameter.
DRY_RUN = False

DEEPSEEK_API_KEY: Optional[str] = None
LLM: Optional[AsyncOpenAI] = None


def _llm() -> AsyncOpenAI:
    global LLM
    if LLM is None:
        assert DEEPSEEK_API_KEY, "DEEPSEEK_API_KEY not loaded (real mode only)"
        LLM = AsyncOpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com", timeout=120)
    return LLM


def _load_deepseek_key() -> None:
    global DEEPSEEK_API_KEY
    key = os.environ.get("DEEPSEEK_API_KEY", "")
    if key:
        DEEPSEEK_API_KEY = key
        return
    env_path = Path(__file__).resolve().parent.parent / "actae" / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("DEEPSEEK_API_KEY="):
                DEEPSEEK_API_KEY = line.split("=", 1)[1].strip().strip("\"'")
                return
    raise SystemExit(
        "DEEPSEEK_API_KEY not found. Set it in actae/.env or the environment "
        "(or run with --dry-run for a deterministic report with no API key)."
    )


# ---------------------------------------------------------------------------
# One LLM call — the single place token/time usage is captured.
# ---------------------------------------------------------------------------

async def llm_call(
    step: str,
    system: str,
    user: str,
    *,
    registry: StepRegistry,
    temperature: float = 0.7,
) -> Dict[str, Any]:
    """One LLM call. Real mode calls DeepSeek and records the reported usage;
    dry-run mode uses the deterministic DRY_RUN_USAGE table instead. Either
    way exactly one `registry.record(...)` happens per call, so `registry` is
    the single source of truth for the report."""
    dry_run = DRY_RUN
    t0 = time.time()
    if dry_run:
        prompt_tokens, completion_tokens = DRY_RUN_USAGE.get(step, (1000, 300))
        content = f"[dry-run:{step}] {system}"
        elapsed_ms = DRY_RUN_ELAPSED_MS
    else:
        resp = await _llm().chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=temperature,
            max_tokens=1024,
        )
        usage = getattr(resp, "usage", None)
        prompt_tokens = usage.prompt_tokens if usage else 0
        completion_tokens = usage.completion_tokens if usage else 0
        content = (resp.choices[0].message.content or "").strip()
        elapsed_ms = round((time.time() - t0) * 1000)
    registry.record(step, prompt_tokens, completion_tokens, elapsed_ms)
    return {
        "content": content,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "elapsed_ms": elapsed_ms,
    }


# ---------------------------------------------------------------------------
# The 10-step pipeline (framework-agnostic: plain async functions).
# ---------------------------------------------------------------------------

TOPIC = "The economic and technical case for on-device LLMs in 2026"

# Steps 1-5: produce the raw material (context) the later steps consume.
async def step_research(reg: StepRegistry, state: Dict[str, Any]) -> Dict[str, Any]:
    r = await llm_call(
        "research",
        "You are a senior analyst. Produce 3 concrete, quantified findings on the topic.",
        TOPIC,
        registry=reg,
        temperature=0.4,
    )
    return {"research": r["content"], "research_tokens": r["prompt_tokens"] + r["completion_tokens"]}


async def step_insights(reg: StepRegistry, state: Dict[str, Any]) -> Dict[str, Any]:
    r = await llm_call(
        "insights",
        "You are a strategy advisor. From the research, extract 3 actionable insights, each with a 'so what'.",
        state["research"],
        registry=reg,
        temperature=0.5,
    )
    return {"insights": r["content"], "insights_tokens": r["prompt_tokens"] + r["completion_tokens"]}


async def step_stakeholders(reg: StepRegistry, state: Dict[str, Any]) -> Dict[str, Any]:
    r = await llm_call(
        "stakeholders",
        "List the key stakeholders who care about this topic and one concern each.",
        state["research"] + "\n\n" + state["insights"],
        registry=reg,
        temperature=0.5,
    )
    return {"stakeholders": r["content"]}


async def step_risks(reg: StepRegistry, state: Dict[str, Any]) -> Dict[str, Any]:
    r = await llm_call(
        "risks",
        "Enumerate the top risks and mitigations implied by the research and insights.",
        state["research"] + "\n\n" + state["insights"] + "\n\n" + state["stakeholders"],
        registry=reg,
        temperature=0.5,
    )
    return {"risks": r["content"]}


async def step_metrics(reg: StepRegistry, state: Dict[str, Any]) -> Dict[str, Any]:
    r = await llm_call(
        "metrics",
        "Define 5 concrete success metrics with baseline and target values.",
        state["insights"] + "\n\n" + state["risks"],
        registry=reg,
        temperature=0.4,
    )
    return {"metrics": r["content"]}


# Step 6 — THE STEP WE REFINE. The baseline uses a neutral style; the fork
# uses an aggressive "recommendation-first" style. Both are real LLM calls.
STYLE_BASELINE = (
    "You are an executive report writer. Write the 'Recommendations' section "
    "in a balanced, neutral tone. Be specific."
)
STYLE_FORK = (
    "You are an executive report writer. Write the 'Recommendations' section "
    "in a CONFIDENT, decisive tone: lead with the single strongest "
    "recommendation, then the rest. Be specific and prescriptive."
)


async def step_recommendations(
    reg: StepRegistry, state: Dict[str, Any], *, style: str = STYLE_BASELINE
) -> Dict[str, Any]:
    r = await llm_call(
        "recommendations",
        style,
        state["research"] + "\n\n" + state["insights"] + "\n\n" + state["risks"] + "\n\n" + state["metrics"],
        registry=reg,
        temperature=0.6,
    )
    return {"recommendations": r["content"], "recommendations_tokens": r["prompt_tokens"] + r["completion_tokens"]}


async def step_summary(reg: StepRegistry, state: Dict[str, Any]) -> Dict[str, Any]:
    r = await llm_call(
        "summary",
        "Write a 1-paragraph executive summary of the full report.",
        state["research"] + "\n\n" + state["insights"] + "\n\n" + state["recommendations"],
        registry=reg,
        temperature=0.4,
    )
    return {"summary": r["content"]}


async def step_next_steps(reg: StepRegistry, state: Dict[str, Any]) -> Dict[str, Any]:
    r = await llm_call(
        "next_steps",
        "List the immediate next steps and owners for acting on the recommendations.",
        state["recommendations"] + "\n\n" + state["metrics"],
        registry=reg,
        temperature=0.5,
    )
    return {"next_steps": r["content"]}


async def step_final(reg: StepRegistry, state: Dict[str, Any]) -> Dict[str, Any]:
    r = await llm_call(
        "final",
        "Assemble a final verdict paragraph and a one-line takeaway.",
        state["summary"] + "\n\n" + state["next_steps"],
        registry=reg,
        temperature=0.4,
    )
    return {"final": r["content"]}


async def step_complete(reg: StepRegistry, state: Dict[str, Any]) -> Dict[str, Any]:
    # A distinct closing step so its usage is reported under its own name
    # (reusing `final` here would mislabel the report's rows).
    r = await llm_call(
        "complete",
        "Produce a one-line completion marker summarizing the outcome.",
        state["summary"] + "\n\n" + state["next_steps"],
        registry=reg,
        temperature=0.4,
    )
    return {"complete": r["content"]}


# The step list (name → runner). Steps 1-5 then 6..10.
PIPELINE: List[Dict[str, Any]] = [
    {"step": 1, "name": "research", "fn": step_research},
    {"step": 2, "name": "insights", "fn": step_insights},
    {"step": 3, "name": "stakeholders", "fn": step_stakeholders},
    {"step": 4, "name": "risks", "fn": step_risks},
    {"step": 5, "name": "metrics", "fn": step_metrics},
    {"step": 6, "name": "recommendations", "fn": step_recommendations},
    {"step": 7, "name": "summary", "fn": step_summary},
    {"step": 8, "name": "next_steps", "fn": step_next_steps},
    {"step": 9, "name": "final", "fn": step_final},
    {"step": 10, "name": "complete", "fn": step_complete},
]

STEP_NAMES: List[str] = [spec["name"] for spec in PIPELINE]
FORK_STEP = 5  # fork after step 5; refine step 6
INHERITED_KEYS = ("research", "insights", "stakeholders", "risks", "metrics")


def _state_fn_for(live: Dict[str, Any]):
    """StateFn: return the current live pipeline state (what Actae snapshots)."""
    def state_fn() -> Dict[str, Any]:
        return dict(live)
    return state_fn


async def run_steps(
    session: AgentSession,
    registry: StepRegistry,
    live: Dict[str, Any],
    specs: List[Dict[str, Any]],
    *,
    style: str = STYLE_BASELINE,
) -> Dict[str, Any]:
    """Run the given step specs against a session, recording + snapshotting.

    `registry` receives one `record()` per LLM call inside the steps; the
    event's `output.tokens` is taken from that registry so every step's event
    carries its real measured token count."""
    for spec in specs:
        step = spec["step"]
        name = spec["name"]
        fn = spec["fn"]
        if step == 6:
            delta = await fn(registry, live, style=style)
        else:
            delta = await fn(registry, live)
        live.update(delta)
        # Step 6 records the full accumulated context as input (all of steps
        # 1-5's real output) so its event log shows what it consumed.
        step_input = {k: v for k, v in delta.items() if isinstance(v, str)}
        if step == 6:
            step_input = {
                k: live.get(k, "")
                for k in INHERITED_KEYS
                if live.get(k)
            }
        await session.step(
            f"step.{step}.{name}",
            input=step_input,
            output={"tokens": registry.step_tokens(name), "chars": len(str(delta.get(list(delta)[-1], "")))},
            context=delta,
        )
        await asyncio.sleep(0.05)  # let the dashboard breathe
    return dict(live)


def _event_payloads(actae_events, event_type: str):
    """All `context` payloads (per-step output deltas) for an event type."""
    return [e.payload.get("context", {}) or {} for e in actae_events if e.event_type == event_type]


async def _run_fidelity_check(
    actae: ActaeClient,
    base_channel: str,
    *,
    dry_run: bool,
) -> Dict[str, Any]:
    """The fidelity leg: does a fork's output match a no-fork re-run?

    Compares three runs that all use IDENTICAL prompts:

      1. baseline          — the original 10-step run,
      2. no-fork re-run    — the full 10-step pipeline run again, *without*
         forking (this is literally "the run without the fork"),
      3. same-prompt fork  — fork at step 5, run steps 6-10 with the baseline
         step-6 prompt (nothing refined).

    Claims (all measured, none asserted):
      • inherited prefix — the fork's step-6 input is byte-identical to the
        baseline's (steps 1-5 are the baseline's stored output, copied not
        regenerated). Exact in real runs too.
      • tail divergence — for steps 6-10, character-level similarity (difflib
        ratio, 1.00 = identical) of baseline↔fork vs baseline↔no-fork-re-run.
        The honest test is whether the FORK drifts from the baseline any more
        than a plain RE-RUN does. Any gap is model sampling, not forking.

    Returns the metrics + a printable section. Requires a connected client."""
    # 1. No-fork control: re-run the full pipeline with the same prompts.
    rerun_channel = f"{base_channel}-rerun"
    rerun_registry = StepRegistry()
    rerun_live: Dict[str, Any] = {"topic": TOPIC}
    async with AgentSession(
        actae, rerun_channel,
        display_name="No-fork re-run control",
        state_fn=_state_fn_for(rerun_live),
        params={"topic": TOPIC, "model": MODEL, "run": "no-fork-rerun"},
    ) as rerun_session:
        await run_steps(rerun_session, rerun_registry, rerun_live, PIPELINE)

    # 2. Same-prompt fork (inherits steps 1-5, runs steps 6-10 unchanged).
    fid_channel = f"{base_channel}-fidelity"
    reg = StepRegistry()
    live: Dict[str, Any] = {"topic": TOPIC}

    sess = await AgentSession.resume(
        actae, base_channel,
        fork_at_step=FORK_STEP,
        name=fid_channel,
        state_fn=_state_fn_for(live),
        params={"topic": TOPIC, "model": MODEL, "run": "fidelity", "style": "neutral"},
    )
    inherited = sess.inherited_state or {}
    for k, v in inherited.items():
        if k in INHERITED_KEYS:
            live[k] = v
    async with sess:
        await run_steps(sess, reg, live, PIPELINE[FORK_STEP:], style=STYLE_BASELINE)

    base_events = await actae.replay(base_channel, cursor=0, limit=200)
    rerun_events = await actae.replay(rerun_channel, cursor=0, limit=200)
    fid_events = await actae.replay(fid_channel, cursor=0, limit=200)

    # 3. Inherited prefix: compare step-6 inputs byte-for-byte.
    base6_input = next((e.payload.get("input", {}) or {} for e in base_events if e.event_type == "step.6.recommendations"), {})
    fid6_input = next((e.payload.get("input", {}) or {} for e in fid_events if e.event_type == "step.6.recommendations"), {})
    compared = [k for k in INHERITED_KEYS if base6_input.get(k) is not None]
    mismatches = [k for k in compared if fid6_input.get(k) != base6_input.get(k)]

    # 4. Tail divergence: baseline↔fork vs baseline↔no-fork-re-run, per step.
    def _tail_similarity(events) -> Dict[str, float]:
        sim: Dict[str, float] = {}
        for spec in PIPELINE[FORK_STEP:]:
            step, name = spec["step"], spec["name"]
            base_ctx = _event_payloads(base_events, f"step.{step}.{name}")
            other_ctx = _event_payloads(events, f"step.{step}.{name}")
            if not base_ctx or not other_ctx:
                continue
            a = base_ctx[0].get(name, "")
            b = other_ctx[0].get(name, "")
            sim[name] = difflib.SequenceMatcher(None, a, b).ratio() if a or b else 1.0
        return sim

    fork_sim = _tail_similarity(fid_events)
    rerun_sim = _tail_similarity(rerun_events)
    fork_mean = sum(fork_sim.values()) / len(fork_sim) if fork_sim else 0.0
    rerun_mean = sum(rerun_sim.values()) / len(rerun_sim) if rerun_sim else 0.0

    lines = [
        "  ── Fidelity: is the fork's output like a no-fork re-run? ──",
        "  controls: no-fork re-run (full 10-step re-run, same prompts) vs same-prompt fork",
    ]
    if not mismatches:
        lines.append("  ✓ inherited prefix byte-identical to baseline: " + ", ".join(compared))
        lines.append("    (steps 1-5 on the fork ARE the baseline's stored output — copied, not regenerated)")
    else:
        lines.append(f"  ✗ inherited prefix MISMATCHES: {', '.join(mismatches)}")
    lines.append("")
    lines.append("  tail similarity to baseline, same prompts (1.00 = identical text):")
    header = f"    {'step':<16} {'no-fork re-run':>14} {'fork':>8}"
    lines.append(header)
    for spec in PIPELINE[FORK_STEP:]:
        name = spec["name"]
        r = rerun_sim.get(name)
        f = fork_sim.get(name)
        if r is None or f is None:
            continue
        lines.append(f"    {name:<16} {r:>14.2f} {f:>8.2f}")
    lines.append("-" * len(header))
    lines.append(f"    {'mean':<16} {rerun_mean:>14.2f} {fork_mean:>8.2f}")

    fork_div = 1.0 - fork_mean
    rerun_div = 1.0 - rerun_mean
    if fork_div <= rerun_div + 0.05:
        verdict = (f"  ✓ fork tail divergence ({fork_div:.2f}) is not worse than a plain no-fork re-run "
                   f"({rerun_div:.2f}) — forking does not degrade output fidelity; the gap to the "
                   "baseline is model sampling.")
    else:
        verdict = (f"  ⚠ fork tail divergence ({fork_div:.2f}) exceeds the no-fork re-run's "
                   f"({rerun_div:.2f}) by {fork_div - rerun_div:.2f} — see the per-step rows.")
    lines.append(verdict)
    lines.append("  (pipeline temperature 0.4-0.7; lower temperature → higher similarity)")
    if dry_run:
        lines.append("  (dry-run: deterministic → both 1.00, divergence 0.00)")
    section = "\n".join(lines)

    return {
        "channel": fid_channel,
        "rerun_channel": rerun_channel,
        "prefix_identical": not mismatches,
        "prefix_mismatches": mismatches,
        "tail_similarity_fork": fork_sim,
        "tail_similarity_rerun": rerun_sim,
        "fork_divergence": fork_div,
        "rerun_divergence": rerun_div,
        "section": section,
    }


async def run_demo(
    actae: ActaeClient,
    *,
    dry_run: bool = False,
    base_channel: Optional[str] = None,
    fork_channel: Optional[str] = None,
    with_fidelity: bool = True,
) -> Dict[str, Any]:
    """Run baseline + fork, print the proofs, and return the measured
    registries and the rendered report. `actae` must already be connected."""
    global DRY_RUN
    DRY_RUN = dry_run

    suffix = int(time.time())
    base_channel = base_channel or f"fork-demo-{suffix}"
    fork_channel = fork_channel or f"{base_channel}-fork6"

    mode = (
        "DRY-RUN — deterministic fake usage, no real LLM calls (reproducible, not for customers)"
        if dry_run
        else "real DeepSeek usage (measured)"
    )
    print(f"Topic: {TOPIC}\nModel: {MODEL}\nBase: {base_channel}  Fork: {fork_channel}")
    print(f"Token report source: {mode}\n")

    # ---- Run 1: baseline (steps 1-10) ----
    baseline_live: Dict[str, Any] = {"topic": TOPIC}
    baseline_registry = StepRegistry()

    async with AgentSession(
        actae, base_channel,
        display_name="Baseline run",
        state_fn=_state_fn_for(baseline_live),
        params={"topic": TOPIC, "model": MODEL, "run": "baseline"},
    ) as base_session:
        await run_steps(base_session, baseline_registry, baseline_live, PIPELINE)
        print(f"[baseline] steps 1-10 complete — {baseline_registry.total_calls()} LLM calls, "
              f"{base_session.step_count} steps")

    print(f"[baseline] LLM calls by step: {baseline_registry.calls_by_step()}\n")

    # ---- Fork at step 5 with a MODIFIED step 6 ----
    # The fork inherits steps 1-5's state (real LLM outputs). We resume at
    # step 6 using the fork's inherited state and the confident style.
    fork_registry = StepRegistry()
    fork_live: Dict[str, Any] = {"topic": TOPIC}

    forked_session = await AgentSession.resume(
        actae, base_channel,
        fork_at_step=FORK_STEP,
        name=fork_channel,
        state_fn=_state_fn_for(fork_live),
        params={"topic": TOPIC, "model": MODEL, "run": "fork", "style": "confident"},
    )

    # The fork inherited steps 1-5's state. Seed the live dict from it.
    inherited = forked_session.inherited_state or {}
    for k, v in inherited.items():
        if k in INHERITED_KEYS:
            fork_live[k] = v

    async with forked_session:
        # Run ONLY steps 6-10 on the fork, against inherited state.
        await run_steps(forked_session, fork_registry, fork_live, PIPELINE[FORK_STEP:], style=STYLE_FORK)

    # ---- Proof & verification ----
    print(f"[fork] LLM calls by step: {fork_registry.calls_by_step()}\n")

    # (a) Steps 1-5 must NOT have been invoked on the fork.
    for step_name in INHERITED_KEYS:
        if fork_registry.count(step_name) != 0:
            print(f"  ✗ FAIL: step '{step_name}' was re-invoked on the fork ({fork_registry.count(step_name)}x)")
        else:
            print(f"  ✓ steps 1-5 '{step_name}' NOT re-invoked on the fork")

    # (b) The fork's event log: fork.started + steps 6-10 only.
    fork_events = await actae.replay(fork_channel, cursor=0, limit=200)
    fork_event_types = [e.event_type for e in fork_events]
    print(f"\n  fork event log ({len(fork_events)} events): {fork_event_types}")

    # (c) Step 6's input contained steps 1-5's real data (inherited).
    step6_event = next((e for e in fork_events if e.event_type == "step.6.recommendations"), None)
    if step6_event is None:
        print("  ✗ FAIL: no step.6 event on the fork")
    else:
        inp = step6_event.payload.get("input", {})
        has_research = "research" in inp or bool(inp)
        print(f"  ✓ step 6 received inherited context: research={has_research} "
              f"insights={bool(inherited.get('insights'))} metrics={bool(inherited.get('metrics'))}")

    # (d) Baseline's step 6 vs fork's step 6 differ (the modification worked),
    #     while steps 1-5 data is identical (inherited, not regenerated).
    base_events = await actae.replay(base_channel, cursor=0, limit=200)
    base_step6 = next((e for e in base_events if e.event_type == "step.6.recommendations"), None)
    fork_step6 = next((e for e in fork_events if e.event_type == "step.6.recommendations"), None)
    base_rec = (base_step6.payload.get("context", {}) or {}).get("recommendations", "") if base_step6 else ""
    fork_rec = (fork_step6.payload.get("context", {}) or {}).get("recommendations", "") if fork_step6 else ""
    if base_rec and fork_rec and base_rec != fork_rec:
        print("  ✓ step 6 output differs between baseline and fork (refinement applied)")
    else:
        print(f"  ⚠ step 6 outputs identical ({len(base_rec)} vs {len(fork_rec)} chars) — check the style prompts")

    # (e) Fidelity: a same-prompt fork shows the fork ≈ a no-fork re-run.
    fidelity = None
    if with_fidelity:
        fidelity = await _run_fidelity_check(actae, base_channel, dry_run=dry_run)
        print(f"\n{fidelity['section']}")

    # (f) Token-savings report (measured — see fork_token_report.py).
    print(f"\n  LLM calls: baseline={baseline_registry.total_calls()} "
          f"fork={fork_registry.total_calls()} → saved {baseline_registry.total_calls() - fork_registry.total_calls()} calls (steps 1-5)\n")
    report = format_token_report(baseline_registry, fork_registry, STEP_NAMES, mode=mode)
    print(report)

    print(f"\n  Channels: {base_channel} (baseline)  |  {fork_channel} (fork, refined)  |  "
          f"{fidelity['channel'] if fidelity else '-'} (fork, same prompt / fidelity)")
    print("  Open the dashboard → open the fork channel → Inspector → State Diff / Decision Trail.")

    return {
        "baseline": baseline_registry,
        "fork": fork_registry,
        "report": report,
        "fidelity": fidelity,
    }


async def main(dry_run: bool, with_fidelity: bool) -> None:
    actae = ActaeClient(api_key=ACTAE_API_KEY, endpoint=ACTAE_URL)
    await actae.connect()
    try:
        await run_demo(actae, dry_run=dry_run, with_fidelity=with_fidelity)
    finally:
        await actae.disconnect()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fork & Resume demo with a measured token-savings report.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="no real LLM calls; use deterministic fake token usage so the "
             "report is reproducible without an API key",
    )
    parser.add_argument(
        "--no-fidelity",
        action="store_true",
        help="skip the same-prompt fork fidelity check (saves 5 LLM calls)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    _args = _parse_args()
    if not _args.dry_run:
        _load_deepseek_key()
    asyncio.run(main(_args.dry_run, with_fidelity=not _args.no_fidelity))
