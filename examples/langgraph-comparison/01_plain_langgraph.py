#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""
01 — Pure LangGraph Research Pipeline (no Actae)
====================================================

Enterprise-grade multi-agent research pipeline using LangGraph.
This version has NO Actae — state is in-memory, no observability.

Compare with 02_actae_langgraph.py which adds:
  - Persistent state snapshots
  - Real-time dashboard
  - Replay and fork capabilities
  - Crash recovery via cursor-based resume

Architecture:
  StateGraph (Researcher → Analyst → Writer → Critic → [loop|end])
  Each node is a pure async function: state_in → partial_state_out
  The graph engine merges outputs into shared state.

Usage:
    export DEEPSEEK_API_KEY=sk-...
    python 01_plain_langgraph.py "Your research topic"
"""

import asyncio
import sys
import time
from typing import Annotated, Any, Dict, TypedDict

from langgraph.graph import END, START, StateGraph

from src.agents import (
    analyst_agent,
    critic_agent,
    researcher_agent,
    should_continue,
    writer_agent,
)
from src.config import ConfigLoader
from src.logging_setup import get_correlation_id, get_logger, reset_correlation_id
from src.telemetry import Tracer

logger = get_logger(__name__)


# 
# 1. STATE SCHEMA
# 
# Every agent reads from and writes to this shared dict.
# LangGraph merges each node's output into the global state.
# WITHOUT Actae: This dict exists only in memory — lost on crash/restart.
# WITH Actae:    Each state transition is persisted as a versioned snapshot.

class ResearchState(TypedDict):
    #  Input 
    topic: str

    #  Agent outputs (accumulated by LangGraph's merge) 
    research_findings: str
    key_insights: str
    draft: str
    revision_notes: str
    review_score: int
    review_details: Dict[str, Any]

    #  Control flow 
    current_agent: str
    agent_status: str
    revision_count: int


# 
# 2. GRAPH CONSTRUCTION
# 
# The graph topology matches the Actae version exactly.
# The ONLY difference is the lack of Actae instrumentation.

def build_graph(config: Any) -> StateGraph:
    """Build the agent pipeline graph.

    Enterprise note: The graph is a pure configuration object.
    It can be serialized, versioned, and tested in isolation.
    """
    deepseek_cfg = config.deepseek

    #  Node functions (bound to config via closure) 
    # In production, you'd use dependency injection containers
    # (e.g., FastAPI's Depends, or a custom DI framework).
    async def researcher(s: Dict) -> Dict:
        return await researcher_agent(s, deepseek_cfg)

    async def analyst(s: Dict) -> Dict:
        return await analyst_agent(s, deepseek_cfg)

    async def writer(s: Dict) -> Dict:
        return await writer_agent(s, deepseek_cfg)

    async def critic(s: Dict) -> Dict:
        return await critic_agent(s, deepseek_cfg)

    #  Wire the graph 
    graph = StateGraph(ResearchState)
    graph.add_node("researcher", researcher)
    graph.add_node("analyst", analyst)
    graph.add_node("writer", writer)
    graph.add_node("critic", critic)

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

    #  Compile 
    # The checkpointer would be MemorySaver() or None for in-memory.
    # WITH Actae: we'd pass an ActaeCheckpointSaver here.
    return graph.compile()


# 
# 3. MAIN
# 

async def main() -> None:
    """Execute the research pipeline with enterprise observability."""
    #  Bootstrap 
    correlation_id = reset_correlation_id()
    config = ConfigLoader.load()
    tracer = Tracer()

    topic = sys.argv[1] if len(sys.argv) > 1 else config.pipeline.topic

    logger.info(
        "Pipeline starting",
        extra={
            "correlation_id": correlation_id,
            "topic": topic[:80],
            "version": "1.0.0",
            "mode": "plain",
        },
    )

    #  Build graph 
    with tracer.span("graph.build"):
        app = build_graph(config)

    #  Initial state 
    initial_state = {
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

    #  Execute 
    # ainvoke() returns the FULL final state (merged from all nodes).
    logger.info("Executing pipeline: Researcher → Analyst → Writer → Critic")

    elapsed: float = 0.0
    final_state: Dict[str, Any] = {}

    with tracer.span("pipeline.execute") as sp:
        try:
            final_state = await app.ainvoke(
                initial_state,
                config={"configurable": {"thread_id": f"plain-{correlation_id}"}},
            )
            elapsed = time.time() - sp.start_time
            sp.attributes["revisions"] = final_state.get("revision_count", 0)
            sp.attributes["final_score"] = final_state.get("review_score", 0)
        except Exception as exc:
            logger.error("Pipeline failed: %s", exc)
            sp.attributes["error"] = str(exc)
            raise

    #  Report 
    revision_count = final_state.get("revision_count", 0)
    final_score = final_state.get("review_score", 0)

    logger.info(
        "Pipeline complete",
        extra={
            "revisions": revision_count,
            "final_score": final_score,
            "elapsed_seconds": round(elapsed, 1),
            "correlation_id": correlation_id,
        },
    )

    #  Print summary 
    # In production this would go to a report generator or API response.
    print(f"\n{'=' * 60}")
    print(f"  RESEARCH PIPELINE RESULTS  (correlation: {correlation_id})")
    print(f"{'=' * 60}")
    print(f"  Topic:       {topic[:80]}...")
    print(f"  Revisions:   {revision_count}")
    print(f"  Final score: {final_score}/10")
    print(f"  Time:        {elapsed:.1f}s")
    print(f"  Mode:        Plain LangGraph (no Actae)")
    print()

    print(f"  {'' * 40}")
    print(f"  Key Findings")
    print(f"  {'' * 40}")
    text = final_state.get("research_findings", "")[:600]
    print(f"  {text}" if text else "  (none)")
    print()

    print(f"  {'' * 40}")
    print(f"  Final Report ({len(final_state.get('draft', ''))} chars)")
    print(f"  {'' * 40}")
    print(f"  {final_state.get('draft', '(none)')[:1200]}")
    print()

    print(f"{'=' * 60}")
    print(f"    No Actae — this execution trace is lost on exit.")
    print(f"    Run 02_actae_langgraph.py to see this in a dashboard.")
    print(f"    Run 03_actae_websocket.py for real-time streaming.")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    asyncio.run(main())
