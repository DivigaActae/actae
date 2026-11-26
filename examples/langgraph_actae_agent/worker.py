#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Run the LangGraph ReAct agent once against Actae.

With ``--crash`` the process is killed (exit 137) inside the side-effecting tool
*after* the effect lands but *before* the ledger records completion — the
ambiguous window a durable resume must reconcile.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from langchain_core.messages import HumanMessage  # noqa: E402

from actae_client import ActaeClient  # noqa: E402
from actae_client.adapters.langgraph import ActaeCheckpointSaver  # noqa: E402
from lg_agent import ScriptedSurveyModel, build_graph, checkpoint_channel, make_tools  # noqa: E402


async def main() -> int:
    parser = argparse.ArgumentParser(description="run the LangGraph agent once")
    parser.add_argument("--thread", required=True)
    parser.add_argument("--channel", required=True)
    parser.add_argument("--effects", required=True)
    parser.add_argument("--label", default="baseline")
    parser.add_argument("--celsius", type=float, default=21.0)
    parser.add_argument("--prompt", default="Take a temperature reading and summarize the result.")
    parser.add_argument(
        "--crash-at",
        choices=["before_effect", "after_effect", "after_complete"],
        help="kill the process at this point inside the tool",
    )
    parser.add_argument("--crash", action="store_true", help="shorthand for --crash-at after_effect")
    args = parser.parse_args()

    crash_point = args.crash_at or ("after_effect" if args.crash else None)

    client = ActaeClient(
        api_key=os.environ["ACTAE_API_KEY"], endpoint=os.environ["ACTAE_URL"]
    )
    await client.connect()
    try:
        saver = ActaeCheckpointSaver(client, channel=args.channel)
        tools = make_tools(
            client,
            checkpoint_channel(saver, args.thread),
            Path(args.effects),
            crash_point=crash_point,
        )
        graph = build_graph(
            ScriptedSurveyModel(label=args.label, celsius=args.celsius), tools, saver
        )
        await graph.ainvoke(
            {"messages": [HumanMessage(args.prompt)]},
            {"configurable": {"thread_id": args.thread}},
        )
    finally:
        await client.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
