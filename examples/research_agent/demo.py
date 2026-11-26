#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Research agent demo — Actae's fork is the flagship, on REAL arXiv data.

The story (what the video shows):

  1. A research agent ingests a real topic from the open arXiv API, filters the
     papers, extracts the key findings, and writes a brief. The expensive part
     is loading + filtering + extracting — that is where the tokens go.
  2. We want to ADD a step: a fact-checker that verifies each finding against
     the paper it cites and flags unsupported claims (hallucinated citations).
  3. Instead of re-running the whole pipeline (and re-paying for ingest,
     filter and extract), we FORK the run at the checkpoint just after
     `extract_notes` and run only the new tail (fact_check -> write_report).
     The fork inherits the real papers and findings, so those LLM calls are
     never re-issued — the token report proves it.

Everything is real: a LangGraph `StateGraph`, the open arXiv API, OpenAI
`gpt-4o-mini-2024-07-18`, and Actae's `ActaeCheckpointSaver` as the graph's
checkpointer (which is what makes "fork after step N" a one-liner and a mid-run
kill resumable).

Requirements:
    pip install -e ./sdks/python
    pip install "langgraph>=0.6,<2" langchain-core openai

Configuration — everything is explicit; there are no hardcoded defaults. Export
the one you need (preferred) or pass the matching flag:

    export ACTAE_URL=http://127.0.0.1:8002      # or --url / --port 8002
    export ACTAE_API_KEY=sk-...                 # or --api-key
    export OPENAI_API_KEY=sk-...                # real model only; omit with --dry-run

    cd actae && cargo run                       # start Actae

`ACTAE_API_KEY` must be a key valid for the target instance (mint one with
`actae keys create demo`, or run a clean standalone instance — see the README).
A missing endpoint/key, or one Actae rejects, produces a clear actionable error
rather than a traceback.

Usage:
    python examples/research_agent/demo.py                    # real LLM + arXiv
    python examples/research_agent/demo.py --dry-run          # no API key
    python examples/research_agent/demo.py --topic "retrieval augmented generation"
    python examples/research_agent/demo.py --scenario crash|replay
"""

from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

# Sibling modules (pipeline, source) and the shared token-report library.
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parent))

from fork_token_report import format_token_report  # noqa: E402  (parent dir)

from actae_client import ActaeClient  # noqa: E402
from actae_client.adapters.langgraph import ActaeCheckpointSaver  # noqa: E402
from actae_client.errors import ActaeConnectionError, AuthError  # noqa: E402

import pipeline  # noqa: E402
import source  # noqa: E402

# The ordered pipeline steps the token report compares.
STEPS = ["filter_source", "extract_notes", "fact_check", "write_report"]


# ---------------------------------------------------------------------------
# Console helpers — the demo prints a clean, sectioned narration.
# ---------------------------------------------------------------------------
def banner(title: str) -> None:
    print()
    print("=" * 74)
    print(f"  {title}")
    print("=" * 74, flush=True)


def step(n: int, text: str) -> None:
    print(f"\n[{n}] {text}", flush=True)


def require(condition: bool, message: str) -> None:
    """Invariant check that always runs (unlike `assert`, which `-O` strips)."""
    if not condition:
        raise SystemExit(f"invariant failed: {message}")


# Approximate public list prices (USD per 1M tokens), input/output. Kept small
# and labelled "approx" — a real deployment reads this from config.
_PRICES = {
    "gpt-4o-mini": (0.15, 0.60),
    "gpt-4o": (2.50, 10.00),
}


def cost_note(ledger: "pipeline.TokenLedger", model_name: str) -> str:
    price = next((p for name, p in _PRICES.items() if name in model_name), None)
    if price is None:
        return ""
    usd = ledger.total_prompt() / 1e6 * price[0] + ledger.total_completion() / 1e6 * price[1]
    return f"  approx cost ({model_name}): ${usd:.4f} (list prices)"


def make_model(dry_run: bool, model_name: str):
    if dry_run:
        return pipeline.ScriptedModel()
    if not os.environ.get("OPENAI_API_KEY"):
        raise SystemExit(
            "OPENAI_API_KEY is not set. Export it (export OPENAI_API_KEY=sk-...) "
            "or run with --dry-run for a deterministic run with no API key."
        )
    return pipeline.OpenAIModel(model=model_name)


def ingest(topic: str, max_results: int, refresh: bool) -> dict:
    """Fetch (or load the cached) arXiv result and narrate what we ingested."""
    cached = source.load_snapshot(topic) is not None and not refresh
    print("  Ingesting from the open arXiv API ...", flush=True)
    snapshot = source.get_papers(topic, max_results=max_results, refresh=refresh)
    papers = snapshot["papers"]
    where = "cache" if cached else "arXiv"
    print(f"  source: arXiv ({where}) · topic: \"{topic}\"")
    print(f"  {len(papers)} papers · {source.word_count(snapshot):,} words of abstracts")
    for paper in papers[:5]:
        print(f"    · [{paper['id']}] {paper['title'][:66]}")
    if len(papers) > 5:
        print(f"    · … and {len(papers) - 5} more")
    return snapshot


# ---------------------------------------------------------------------------
# The flagship scenario: baseline -> add fact-checker -> fork -> savings
# ---------------------------------------------------------------------------
async def run_fork(client: ActaeClient, model: pipeline.Model, run_id: str, snapshot: dict) -> None:
    topic = snapshot["topic"]
    saver = ActaeCheckpointSaver(client, channel="research")
    baseline_thread = f"research-{run_id}"
    fork_thread = f"{baseline_thread}-factcheck"

    banner("STEP 1-2  Baseline research run (load -> filter -> extract -> brief)")
    baseline_ledger = pipeline.TokenLedger()
    baseline_graph = pipeline.build_graph(model, saver, baseline_ledger, with_fact_check=False)
    baseline_cfg = {"configurable": {"thread_id": baseline_thread}}

    t0 = time.monotonic()
    baseline_state = await baseline_graph.ainvoke({"topic": topic}, baseline_cfg)
    print(f"    wall clock: {time.monotonic() - t0:.1f}s")
    print("\n    Baseline brief:\n")
    for line in pipeline.report_of(baseline_state).splitlines():
        print("      " + line)

    step(3, "Now add a step: a fact-checker that verifies each finding against")
    print("    the paper it cites (and flags hallucinated citations).")
    print("    In code: insert fact_check between extract_notes and write_report")
    print("    and run the pipeline from the checkpoint after extract_notes —")
    print("    NOT from the start.\n")

    boundary = await pipeline.checkpoint_after(baseline_graph, baseline_cfg, "extract_notes")
    if boundary is None:
        raise SystemExit("could not find the checkpoint after extract_notes")

    await saver.fork_thread(
        boundary, new_thread_id=fork_thread, reason="add a fact-checker (citation check)"
    )
    print(f"    forked thread  {baseline_thread}  ->  {fork_thread}")
    print("    fork inherits: the filtered papers + extracted findings (no re-run)")
    print("    fork runs only: fact_check -> write_report")

    banner("STEP 3  Resume the fork: only the new tail runs")
    fork_ledger = pipeline.TokenLedger()
    fork_graph = pipeline.build_graph(model, saver, fork_ledger, with_fact_check=True)
    # Resume from the fork's own latest checkpoint and tell the ENHANCED graph
    # that `extract_notes` is the last completed step, so it schedules its NEW
    # successor (the fact-checker) instead of the baseline's recorded one.
    # (LangGraph stores each checkpoint's next step, so the re-derivation must
    # happen on the fork's latest checkpoint, without pinning the old id.)
    resume_cfg = {"configurable": {**boundary["configurable"], "thread_id": fork_thread}}
    resume_cfg["configurable"].pop("checkpoint_id", None)
    await fork_graph.aupdate_state(resume_cfg, {}, as_node="extract_notes")
    t1 = time.monotonic()
    fork_state = await fork_graph.ainvoke(None, resume_cfg)
    print(f"    wall clock: {time.monotonic() - t1:.1f}s")

    checks = fork_state.get("fact_check") or []
    if checks:
        print("\n    Fact-check results:\n")
        for row in checks:
            verdict = str(row.get("verdict", "?")).upper()
            mark = {"SUPPORTED": "ok ", "PARTIAL": "~  ", "UNSUPPORTED": "!! "}.get(verdict, "?  ")
            print(f"      {mark}{verdict:<12} {str(row.get('claim', ''))[:70]}")
            if verdict != "SUPPORTED":
                print(f"          evidence: {row.get('evidence', '')}")
    print("\n    Forked brief (fact-checked):\n")
    for line in pipeline.report_of(fork_state).splitlines():
        print("      " + line)

    # The proof that the prefix was not re-invoked: the fork ledger has zero
    # calls for filter_source/extract_notes (inherited, not re-run).
    require(fork_ledger.count("filter_source") == 0, "fork re-ran filter_source")
    require(fork_ledger.count("extract_notes") == 0, "fork re-ran extract_notes")

    banner("STEP 4  Token savings (measured, not modelled)")
    mode = getattr(model, "name", pipeline.DEFAULT_MODEL)
    print(format_token_report(baseline_ledger, fork_ledger, STEPS, mode=mode))
    if not isinstance(model, pipeline.ScriptedModel):
        print(cost_note(baseline_ledger, model.name))
        print(cost_note(fork_ledger, model.name))
    print("  The fork never called filter_source or extract_notes — the expensive")
    print("  prefix is inherited from the checkpoint. That is the whole trick.")


# ---------------------------------------------------------------------------
# Variant: fork, then change the MODEL and the PROMPT for the tail only
# ---------------------------------------------------------------------------
async def run_variant(
    client: ActaeClient,
    base_model: pipeline.Model,
    fork_model: pipeline.Model,
    run_id: str,
    snapshot: dict,
    fork_style: str,
) -> None:
    topic = snapshot["topic"]
    saver = ActaeCheckpointSaver(client, channel="research")
    base_thread = f"research-{run_id}"
    variant_thread = f"{base_thread}-variant"

    banner("STEP 1  Baseline: cheap model, default prompt")
    base_ledger = pipeline.TokenLedger()
    base_graph = pipeline.build_graph(
        base_model, saver, base_ledger, with_fact_check=False, report_style="brief"
    )
    base_cfg = {"configurable": {"thread_id": base_thread}}
    base_state = await base_graph.ainvoke({"topic": topic}, base_cfg)
    print(f"    model: {base_model.name} · prompt: 'brief'")
    print("\n    Baseline brief:\n")
    for line in pipeline.report_of(base_state).splitlines():
        print("      " + line)

    step(2, "Now fork and change ONLY the tail: a different model and a")
    print(f"    different report prompt — here model '{fork_model.name}', prompt '{fork_style}'.")
    print("    The papers, the filter and the extracted findings are inherited.")
    print("    Because the pipeline shape is unchanged, we just resume the fork —")
    print("    no need to touch the prefix.\n")

    boundary = await pipeline.checkpoint_after(base_graph, base_cfg, "extract_notes")
    if boundary is None:
        raise SystemExit("could not find the checkpoint after extract_notes")
    await saver.fork_thread(
        boundary, new_thread_id=variant_thread, reason=f"re-run the tail with model={fork_model.name}, style={fork_style}"
    )
    print(f"    forked thread  {base_thread}  ->  {variant_thread}")

    banner("STEP 3  Resume the fork: the tail re-runs with the new model + prompt")
    variant_ledger = pipeline.TokenLedger()
    variant_graph = pipeline.build_graph(
        fork_model, saver, variant_ledger, with_fact_check=False, report_style=fork_style
    )
    resume_cfg = {"configurable": {**boundary["configurable"], "thread_id": variant_thread}}
    resume_cfg["configurable"].pop("checkpoint_id", None)
    # Re-derive the next step on the fork's checkpoint (the same trick as the
    # fact-check scenario): this pins the resume to the boundary state, so the
    # tail re-runs with the new model + prompt.
    await variant_graph.aupdate_state(resume_cfg, {}, as_node="extract_notes")
    variant_state = await variant_graph.ainvoke(None, resume_cfg)
    print(f"\n    Variant brief (model '{fork_model.name}', prompt '{fork_style}'):\n")
    for line in pipeline.report_of(variant_state).splitlines():
        print("      " + line)

    require(variant_ledger.count("filter_source") == 0, "variant re-ran filter_source")
    require(variant_ledger.count("extract_notes") == 0, "variant re-ran extract_notes")

    banner("STEP 4  What it cost (measured, not modelled)")
    mode = f"baseline {base_model.name} · fork {fork_model.name} (prompt: {fork_style})"
    print(format_token_report(base_ledger, variant_ledger, STEPS, mode=mode))
    if not isinstance(base_model, pipeline.ScriptedModel):
        print(cost_note(base_ledger, base_model.name))
        print(cost_note(variant_ledger, fork_model.name))
    print("  The model and the prompt changed; the expensive prefix did not re-run.")
    print("  That is the fork: swap any downstream knob without replaying the past.")
    print("\n    Compare the two briefs above — same papers, different model, different")
    print("    prompt. Swapping either is a tail-only change.")


# ---------------------------------------------------------------------------
# Replay: the durable history Actae kept for the run
# ---------------------------------------------------------------------------
async def run_replay(client: ActaeClient, run_id: str) -> None:
    thread = f"research-{run_id}"
    banner(f"Replay — the durable event history of thread '{thread}'")
    saver = ActaeCheckpointSaver(client, channel="research")
    channel = saver.channel_for_config({"configurable": {"thread_id": thread}})
    events = await client.replay(channel)
    if not events:
        print("  (no events — run the fork scenario first)")
        return
    for event in events:
        print(f"  cursor {event.cursor:>3}  {event.event_type}")


# ---------------------------------------------------------------------------
# Durability: kill the worker mid-run, resume from the checkpoint
# ---------------------------------------------------------------------------
async def run_crash(
    client: ActaeClient,
    model: pipeline.Model,
    run_id: str,
    topic: str,
    dry_run: bool,
    endpoint: str,
    api_key: str,
) -> None:
    saver = ActaeCheckpointSaver(client, channel="research")
    thread = f"research-crash-{run_id}"
    banner("Durability — the worker is killed, the run resumes from its checkpoint")

    cmd = [
        sys.executable, str(_HERE / "worker.py"),
        "--thread", thread, "--topic", topic, "--crash-after", "extract_notes",
    ]
    if dry_run:
        cmd.append("--dry-run")
    print("    a worker process runs the pipeline to the durable checkpoint after")
    print("    extract_notes and is then hard-killed (os._exit(137)) — the same")
    print("    thing that happens when a worker dies before it finishes:\n")
    print("      " + " ".join(cmd) + "\n", flush=True)
    # The worker is a separate process: hand it the endpoint + key explicitly
    # (so `--port`/`--api-key` flags work too, not just exported env vars).
    child_env = {**os.environ, "ACTAE_URL": endpoint, "ACTAE_API_KEY": api_key}
    proc = await asyncio.to_thread(subprocess.run, cmd, env=child_env)
    print(f"    worker exit code: {proc.returncode}  (137 = hard kill)")

    print("\n    resuming the SAME thread in a new process — from the last durable")
    print("    checkpoint (after extract_notes), not from the start:\n")
    ledger = pipeline.TokenLedger()
    graph = pipeline.build_graph(model, saver, ledger, with_fact_check=False)
    state = await graph.ainvoke(None, {"configurable": {"thread_id": thread}})
    for line in pipeline.report_of(state).splitlines():
        print("      " + line)

    require(ledger.count("filter_source") == 0, "resume re-ran filter_source")
    require(ledger.count("extract_notes") == 0, "resume re-ran extract_notes")
    print("\n    The resume executed only write_report. Because Actae stored the")
    print("    retrieved papers and the extracted findings as a durable")
    print("    checkpoint, the expensive prefix was never recomputed.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main_async(args: argparse.Namespace) -> None:
    endpoint, api_key = args.endpoint, args.key
    if args.scenario == "replay":
        # Replay reads the durable history only — no model, no ingestion.
        client = ActaeClient(endpoint=endpoint, api_key=api_key)
        try:
            await run_replay(client, args.run_id)
        finally:
            await client.disconnect()
        return
    model = make_model(args.dry_run, args.model)
    snapshot = ingest(args.topic, args.max_results, args.refresh)
    # The pipeline's checkpointer uses the HTTP API only, so the client is used
    # directly (no WebSocket handshake to depend on).
    client = ActaeClient(endpoint=endpoint, api_key=api_key)
    try:
        if args.scenario == "crash":
            await run_crash(client, model, args.run_id, snapshot["topic"], args.dry_run, endpoint, api_key)
        elif args.scenario == "variant":
            fork_model = make_model(args.dry_run, args.fork_model)
            await run_variant(client, model, fork_model, args.run_id, snapshot, args.fork_style)
        else:
            await run_fork(client, model, args.run_id, snapshot)
    finally:
        await client.disconnect()


def resolve_endpoint(args: argparse.Namespace) -> str:
    """The Actae endpoint: `--port` shorthand, else `--url`, else `$ACTAE_URL`.

    No hardcoded default — the endpoint must be given explicitly (env is the
    preferred way: `export ACTAE_URL=http://127.0.0.1:8002`).
    """
    if args.port:
        return f"http://127.0.0.1:{args.port}"
    if args.url:
        return args.url
    env = os.environ.get("ACTAE_URL")
    if env:
        return env
    raise SystemExit(
        "No Actae endpoint. Export it and try again:\n"
        "    export ACTAE_URL=http://127.0.0.1:8002\n"
        "  or pass --url http://host:port  /  --port 8002"
    )


def resolve_api_key(args: argparse.Namespace) -> str:
    """The Actae API key: `--api-key`, else `$ACTAE_API_KEY`. No default."""
    key = args.api_key or os.environ.get("ACTAE_API_KEY")
    if not key:
        raise SystemExit(
            "No Actae API key. Export it and try again:\n"
            "    export ACTAE_API_KEY=sk-...\n"
            "  or pass --api-key sk-..."
        )
    return key


def _auth_help(url: str, err: object) -> str:
    return (
        f"Actae rejected the request ({err}).\n"
        f"  endpoint: {url}\n"
        "  The API key is not valid for this instance. Fix one of:\n"
        "    • mint a key against the running instance and export it:\n"
        "        cd actae && ./target/debug/actae keys create demo\n"
        "        export ACTAE_API_KEY=sk-...      # or pass --api-key\n"
        "    • or start a clean standalone instance (it seeds the dev key):\n"
        "        ACTAE_DATA_DIR=/tmp/actae-demo/data ACTAE_CONFIG_DIR=/tmp/actae-demo/cfg \\\n"
        "          ACTAE_EMBEDDED_PG_PORT=5701 ACTAE_PORT=8111 ./target/debug/actae\n"
        "        export ACTAE_URL=http://127.0.0.1:8111"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Research-agent fork demo (Actae + LangGraph + arXiv).")
    parser.add_argument("--dry-run", action="store_true", help="deterministic model, no API key")
    parser.add_argument("--model", default=pipeline.DEFAULT_MODEL, help="OpenAI model id")
    parser.add_argument("--topic", default=source.DEFAULT_TOPIC, help="arXiv search topic")
    parser.add_argument("--max-results", type=int, default=source.DEFAULT_MAX_RESULTS)
    parser.add_argument("--refresh", action="store_true", help="re-fetch from arXiv (ignore the cache)")
    parser.add_argument("--scenario", choices=["fork", "variant", "replay", "crash"], default="fork")
    parser.add_argument("--fork-model", default="gpt-4o", help="model for the variant scenario's fork")
    parser.add_argument("--fork-style", choices=list(pipeline.REPORT_STYLES), default="exec", help="report prompt for the fork")
    parser.add_argument("--url", default=None, help="Actae endpoint (default: $ACTAE_URL)")
    parser.add_argument("--port", type=int, default=None, help="Actae port shorthand -> http://127.0.0.1:<port>")
    parser.add_argument("--api-key", default=None, help="Actae API key (default: $ACTAE_API_KEY)")
    parser.add_argument("--run-id", default=str(int(time.time())), help="unique run id")
    args = parser.parse_args()
    # Resolve the Actae endpoint + key up front (no hardcoded defaults): flags
    # win, otherwise $ACTAE_URL / $ACTAE_API_KEY, otherwise a clear error.
    args.endpoint = resolve_endpoint(args)
    args.key = resolve_api_key(args)
    try:
        asyncio.run(main_async(args))
    except AuthError as err:
        sys.exit(_auth_help(args.endpoint, err))
    except ActaeConnectionError as err:
        sys.exit(f"Could not reach Actae at {args.endpoint}: {err}\n  Start it with: cd actae && cargo run")
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
