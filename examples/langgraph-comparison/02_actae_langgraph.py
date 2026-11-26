#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""
02 — Actae-Enhanced LangGraph Research Pipeline
====================================================

Same 4-agent pipeline as 01_plain_langgraph.py, but with Actae for
persistence, observability, and execution management.

What Actae adds (vs the plain version):
  
   Capability        Plain LangGraph        With Actae           
  
   State             In-memory (lost)       Postgres (persisted)
   Visibility        stdout only            Real-time dashboard 
   Replay            Not possible           From any cursor     
   Forking           Not possible           At any step         
   Crash recovery    Start over             Resume from cursor  
   Versioning        None                   Every N steps       
  

Architecture difference:
  - Plain:  Agent functions → LangGraph → stdout
  - Actae:   Agent functions → LangGraph → Actae events → Dashboard

The agent logic is identical to the plain version (both use src/agents.py),
except that the researcher agent uses function calling to invoke real tools —
the builtin web_search / fetch_page / calculate, plus any MCP server tools
configured via the MCP_SERVERS env var. Every tool call and its full result
is recorded to Actae as ``tool.<name>.started`` / ``tool.<name>.completed``
events so you can inspect them in the dashboard.
Actae is an observability layer, not a rewrite.

Usage:
    # Terminal 1: Start Actae
    cd actae && cargo run -- --dev

    # Terminal 2: Run pipeline
    export DEEPSEEK_API_KEY=sk-...
    python 02_actae_langgraph.py "Your research topic"

    # Browser: http://localhost:8002 → channel "langgraph-research"
"""

import asyncio
import sys
import time
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents import (
    analyst_agent,
    critic_agent,
    researcher_agent,
    should_continue,
    writer_agent,
)
from src.config import ConfigLoader
from src.logging_setup import get_logger, reset_correlation_id
from src.mcp_bridge import MCPTools
from src.telemetry import Tracer
from src.actae_integration import ActaeIntegration, AgentSession
from src.tools import TOOLDEF_RESEARCH

logger = get_logger(__name__)


# 
# 1. STATE SCHEMA (identical to plain version)
# 

class ResearchState(TypedDict):
    topic: str
    research_findings: str
    key_insights: str
    draft: str
    revision_notes: str
    review_score: int
    review_details: Dict[str, Any]
    current_agent: str
    agent_status: str
    revision_count: int


# 
# 2. GRAPH CONSTRUCTION
# 
# Identical topology to the plain version. The only difference is that
# each node wraps the agent call in Actae event recording.

def build_graph(
    config: Any,
    session: AgentSession,
    extra_tools: Optional[List[Dict[str, Any]]] = None,
    execute_tool=None,
) -> StateGraph:
    """Build the pipeline graph with Actae instrumentation.

    Each node wrapper captures:
      - The full input state (what the agent received)
      - The agent's output (findings, insights, draft, scores)
      - A versioned state snapshot after each step

    The researcher agent calls tools — the builtin ``web_search`` /
    ``fetch_page`` / ``calculate`` plus any MCP server tools passed via
    ``extra_tools`` (run through ``execute_tool``). Every tool call and its
    result is recorded as a ``tool.<name>.started`` / ``tool.<name>.completed``
    event pair, so tool activity is fully visible in the Actae dashboard.
    """
    deepseek_cfg = config.deepseek

    researcher_tools = list(TOOLDEF_RESEARCH) + list(extra_tools or [])
    if extra_tools:
        logger.info("Researcher toolset extended with %d MCP tools", len(extra_tools))

    async def on_tool_event(event_type: str, payload: Dict[str, Any]) -> None:
        await _record_tool_event(session, event_type, payload)

    async def wrapped_researcher(s: Dict) -> Dict:
        async with session.trace_step("researcher", input=_state_preview(s)) as result:
            output = await researcher_agent(
                s,
                deepseek_cfg,
                on_tool_event=on_tool_event,
                tools=researcher_tools,
                execute_tool=execute_tool,
            )
        merged = {**s, **output}
        await session.save_state(merged)
        logger.info(
            "Agent researcher completed: %d chars findings",
            len(merged.get("research_findings", "")),
        )
        return output

    async def wrapped_analyst(s: Dict) -> Dict:
        async with session.trace_step("analyst", input=_state_preview(s)) as result:
            output = await analyst_agent(s, deepseek_cfg)
        merged = {**s, **output}
        await session.save_state(merged)
        logger.info(
            "Agent analyst completed: %d chars insights",
            len(merged.get("key_insights", "")),
        )
        return output

    async def wrapped_writer(s: Dict) -> Dict:
        async with session.trace_step("writer", input=_state_preview(s)) as result:
            output = await writer_agent(s, deepseek_cfg)
        merged = {**s, **output}
        await session.save_state(merged)
        logger.info(
            "Agent writer completed: %d chars draft (revision %d)",
            len(merged.get("draft", "")),
            merged.get("revision_count", 0),
        )
        return output

    async def wrapped_critic(s: Dict) -> Dict:
        async with session.trace_step("critic", input=_state_preview(s)) as result:
            output = await critic_agent(s, deepseek_cfg)
        merged = {**s, **output}
        await session.save_state(merged)
        score = output.get("review_score", 0)
        logger.info(
            "Agent critic completed: score %d/10 (accuracy=%d, clarity=%d, depth=%d, actionability=%d)",
            score,
            output.get("review_details", {}).get("accuracy", 0),
            output.get("review_details", {}).get("clarity", 0),
            output.get("review_details", {}).get("depth", 0),
            output.get("review_details", {}).get("actionability", 0),
        )
        return output

    graph = StateGraph(ResearchState)
    graph.add_node("researcher", wrapped_researcher)
    graph.add_node("analyst", wrapped_analyst)
    graph.add_node("writer", wrapped_writer)
    graph.add_node("critic", wrapped_critic)

    graph.add_edge(START, "researcher")
    graph.add_edge("researcher", "analyst")
    graph.add_edge("analyst", "writer")
    graph.add_edge("writer", "critic")

    graph.add_conditional_edges(
        "critic",
        lambda s: should_continue(
            s,
            max_revisions=config.pipeline.max_revisions,
            min_pass_score=config.pipeline.min_pass_score,
        ),
        {"end": END, "revise": "writer"},
    )

    return graph.compile()


async def _record_tool_event(
    session: AgentSession,
    event_type: str,
    payload: Dict[str, Any],
) -> None:
    """Record a tool call or result as an Actae event.

    ``tool.<name>.started`` carries the raw arguments as ``input`` and
    ``tool.<name>.completed`` carries the bounded result as ``output``, so
    both the call and its result render as distinct dashboard nodes.
    """
    is_completed = event_type.endswith(".completed")
    await session.record_step(
        event_type,
        input=payload if not is_completed else None,
        output=payload if is_completed else None,
    )


def _state_preview(state: Dict[str, Any]) -> Dict[str, Any]:
    """Build a preview of the state for event payloads.

    Values are passed through in full — no truncation — so the dashboard
    shows the complete input context of every agent step.
    """
    result: Dict[str, Any] = {}
    for k, v in state.items():
        if k in ("topic", "current_agent", "agent_status",
                 "research_findings", "key_insights", "draft", "revision_notes",
                 "review_score", "revision_count", "review_details"):
            result[k] = v
    return result


# 
# 3. MAIN — with Actae lifecycle
# 

async def main() -> None:
    """Execute the research pipeline with full Actae observability."""
    correlation_id = reset_correlation_id()
    config = ConfigLoader.load()
    tracer = Tracer()

    topic = sys.argv[1] if len(sys.argv) > 1 else config.pipeline.topic
    channel_id = f"{config.actae.channel_prefix}-{int(time.time())}"
    fork_channel_id = f"{channel_id}-fork"

    logger.info(
        "Pipeline starting",
        extra={
            "correlation_id": correlation_id,
            "topic": topic[:80],
            "channel": channel_id,
            "mode": "actae",
        },
    )

    async with await ActaeIntegration.create(config.actae, correlation_id) as actae:
        await actae.ensure_channel(channel_id, metadata={"topic": topic, "pipeline": "langgraph-research"})

        session = AgentSession(
            actae=actae,
            channel_id=channel_id,
            params={"topic": topic, "model": config.deepseek.model},
        )

        await session.record_step(
            "pipeline.start",
            input={"topic": topic, "agent_count": 4},
        )

        # Optional MCP servers expose tools the researcher can call. Every
        # call + result is recorded as tool.<name>.started / .completed events.
        mcp_tools = await MCPTools.start(config.mcp_servers) if config.mcp_servers else None
        try:
            with tracer.span("graph.build"):
                app = build_graph(
                    config,
                    session,
                    extra_tools=mcp_tools.schemas if mcp_tools and mcp_tools.schemas else None,
                    execute_tool=mcp_tools.execute if mcp_tools else None,
                )

            initial_state: Dict[str, Any] = {
                "topic": topic,
                "research_findings": "",
                "key_insights": "",
                "draft": "",
                "revision_notes": "",
                "review_score": 0,
                "review_details": {},
                "current_agent": "",
                "agent_status": "",
                "revision_count": 0,
            }

            logger.info("Executing pipeline (dashboard: %s)", config.actae.http_url)
            elapsed = 0.0
            final_state: Dict[str, Any] = {}

            with tracer.span("pipeline.execute") as sp:
                try:
                    final_state = await app.ainvoke(
                        initial_state,
                        config={"configurable": {"thread_id": f"actae-{correlation_id}"}},
                    )
                    elapsed = time.time() - sp.start_time
                    sp.attributes["revisions"] = final_state.get("revision_count", 0)
                    sp.attributes["final_score"] = final_state.get("review_score", 0)
                except Exception as exc:
                    await session.record_step("pipeline.failed", output={"error": str(exc)})
                    logger.error("Pipeline failed: %s", exc)
                    raise

            await session.record_step(
                "pipeline.complete",
                output={
                    "revisions": final_state.get("revision_count", 0),
                    "final_score": final_state.get("review_score", 0),
                    "elapsed_seconds": round(elapsed, 1),
                },
            )
        finally:
            if mcp_tools is not None:
                await mcp_tools.aclose()

        revision_count = final_state.get("revision_count", 0)
        final_score = final_state.get("review_score", 0)

        logger.info("Pipeline complete", extra={
            "revisions": revision_count, "final_score": final_score,
            "elapsed_seconds": round(elapsed, 1),
            "channel": channel_id,
        })

        #  Save final state snapshot 
        await session.save_state(final_state)

        #  Summary 
        print(f"\n{'=' * 60}")
        print(f"  RESEARCH PIPELINE RESULTS  (correlation: {correlation_id})")
        print(f"{'=' * 60}")
        print(f"  Topic:       {topic[:80]}...")
        print(f"  Revisions:   {revision_count}")
        print(f"  Final score: {final_score}/10")
        print(f"  Time:        {elapsed:.1f}s")
        print(f"  Mode:        Actae-enhanced")
        print(f"  Channel:     {channel_id}")
        print(f"  Dashboard:   {config.actae.http_url}")
        print()

        report = final_state.get("draft", "")
        if report:
            print(f"  {'' * 40}")
            print(f"  Final Report ({len(report)} chars)")
            print(f"  {'' * 40}")
            print(f"  {report[:1200]}")
            print()

        #  0. Show tool calls with their arguments and results 
        all_events = await actae.replay_events(channel_id, cursor=0, limit=200)
        tool_events = [e for e in all_events if e["event_type"].startswith("tool.")]
        if tool_events:
            print(f"  {'' * 40}")
            print(f"  Tool Calls ({len(tool_events)} events)")
            print(f"  {'' * 40}")
            for ev in tool_events:
                summary = _event_summary(ev["event_type"], ev.get("payload") or {})
                print(f"    [{ev['cursor']:>3}] {ev['event_type']:<34} {summary}")
            print()

        events = all_events[:50]

        #  1. Show recorded events with payload summaries 
        if events:
            print(f"  {'' * 40}")
            print(f"  Actae Event Log ({len(events)} events)")
            print(f"  {'' * 40}")
            for ev in events:
                label = ev["event_type"][:32]
                payload = ev.get("payload") or {}
                summary = _event_summary(ev["event_type"], payload)
                ts = ev["timestamp"][:19] if ev["timestamp"] else ""
                print(f"    [{ev['cursor']:>3}] {label:<34} {ts}")
                if summary:
                    print(f"          {summary}")
            print()

        #  2. Show saved state snapshots 
        state_versions = await actae.list_state_versions(channel_id)
        print(f"  {'' * 40}")
        print(f"  State Snapshots ({len(state_versions)} versions)")
        print(f"  {'' * 40}")
        if state_versions:
            for sv in state_versions:
                snap = sv.get("state", {})
                score = snap.get("review_score", "?")
                rev = snap.get("revision_count", "?")
                findings_len = len(str(snap.get("research_findings", "")))
                draft_len = len(str(snap.get("draft", "")))
                print(f"    [v{sv.get('version', '?')}] cursor={sv.get('cursor', '?'):>3} "
                      f"score={score}/10 rev={rev} "
                      f"findings={findings_len}c draft={draft_len}c")
                if snap.get("topic"):
                    print(f"          topic: {snap['topic'][:60]}")
        else:
            print(f"    (no state snapshots saved)")
        print()

        #  3. Fork the channel at the writer step for demo 
        print(f"  {'' * 40}")
        print(f"  Fork Demonstration")
        print(f"  {'' * 40}")
        fork_at = None
        if state_versions:
            # The fork API requires a saved state snapshot at-or-before the
            # fork cursor. `at_cursor` is in per-channel cursor space (the
            # snapshot cursors returned by list_state_versions are per-channel
            # too), so forking at the middle snapshot's cursor works
            # regardless of how many events exist on the channel.
            fork_at = state_versions[len(state_versions) // 2].get("cursor")
        if fork_at is not None and fork_at > 0:
            try:
                fork_result = await actae.fork_channel(
                    source_channel=channel_id,
                    new_channel=fork_channel_id,
                    at_cursor=fork_at,
                    reason=f"Experimental fork at cursor {fork_at}",
                    metadata={"experiment": "prompt_variant", "forked_from": channel_id},
                )
                print(f"    Forked channel at cursor {fork_at}")
                print(f"    Source: {channel_id}")
                print(f"    Fork:   {fork_channel_id}")
                print(f"    Origin run: {fork_result.origin_run_id[:12]}...")

                fork_events = await actae.replay_events(fork_channel_id, cursor=0, limit=5)
                print(f"    Fork inherited {len(fork_events)} events from source")
            except Exception as exc:
                logger.warning("Fork demonstration skipped: %s", exc)
                print(f"    Fork not available: {exc}")
        else:
            print(f"    (no midpoint cursor available for fork)")
        print()

        #  4. Show execution tree 
        print(f"  {'' * 40}")
        print(f"  Execution Tree")
        print(f"  {'' * 40}")
        try:
            tree = await actae.get_fork_tree(channel_id)
            if tree:
                _print_tree(tree, indent=4)
            else:
                print(f"    (no execution tree data)")
        except Exception as exc:
            logger.warning("Execution tree unavailable: %s", exc)
            print(f"    (execution tree not available: {exc})")
        print()

        #  5. Resumable state info 
        print(f"  {'' * 40}")
        print(f"  Resume / Replay Commands")
        print(f"  {'' * 40}")
        print(f"  # Load latest saved state:")
        print(f"  state = await actae.load_latest_state(\"{channel_id[:20]}...\")")
        print(f"  # Replay all events:")
        print(f"  events = await actae.replay_events(\"{channel_id[:20]}...\", cursor=0)")
        if state_versions:
            print(f"  # Fork at version {state_versions[-1].get('version', '?')}:")
            print(f"  await actae.fork_channel(\"{channel_id[:20]}...\", \"my-fork\", "
                  f"at_cursor={state_versions[-1].get('cursor', '?')})")
        print()

        print(f"{'=' * 60}")
        print(f"  Pipeline complete — all events + state persisted in Actae.")
        print(f"  Open the dashboard to explore the full execution trace.")
        print(f"  Run 03_actae_websocket.py for real-time streaming.")
        print(f"{'=' * 60}\n")


def _event_summary(event_type: str, payload: Dict[str, Any]) -> str:
    """Build a one-line summary of an event's payload."""
    if event_type.startswith("tool."):
        started = event_type.endswith(".started")
        body = payload.get("input") if started else payload.get("output", payload)
        body = body or payload
        name = body.get("name", "?")
        args = body.get("arguments", {})
        if started:
            arg_str = str(args)[:80]
            return f"{name}({arg_str[1:-1]})" if arg_str and arg_str != "{}" else f"{name}()"
        result = body.get("result", "")
        duration = body.get("duration_ms")
        prefix = f"{name} -> {result[:100]}"
        if duration is not None:
            prefix += f" ({duration}ms)"
        return prefix[:140]
    if event_type.endswith(".started"):
        inp = payload.get("input", {})
        if inp:
            parts = []
            if "topic" in inp:
                parts.append(f"topic={inp['topic'][:40]}")
            if "current_agent" in inp:
                parts.append(f"prior={inp['current_agent']}")
            return " | ".join(parts)
    if event_type.endswith(".completed"):
        out = payload.get("output", {})
        if out:
            parts = []
            if "elapsed_ms" in out:
                parts.append(f"{out['elapsed_ms']:.0f}ms")
            if "error" in out:
                parts.append(f"ERROR: {out['error'][:40]}")
            return " | ".join(parts)
    if event_type == "critic.score":
        p = payload
        return f"score={p.get('score', '?')}/10 details={p.get('details', {})}"
    if event_type == "pipeline.complete":
        out = payload.get("output", {})
        if out:
            return f"score={out.get('final_score', '?')}/10 {out.get('elapsed_seconds', '?')}s"
    if event_type == "pipeline.start":
        inp = payload.get("input", {})
        return f"agents={inp.get('agent_count', '?')}"
    return ""


def _print_tree(node: Any, indent: int = 0) -> None:
    """Recursively print an execution tree node."""
    prefix = " " * indent
    ch = (node.channel_id if hasattr(node, "channel_id") else node.get("channel_id", "?"))[:48]
    ev = node.event_count if hasattr(node, "event_count") else node.get("event_count", 0)
    print(f"{prefix}\u2514 {ch}  ({ev} events)")
    children = node.children if hasattr(node, "children") else node.get("children", [])
    for child in children:
        _print_tree(child, indent + 2)


if __name__ == "__main__":
    asyncio.run(main())
