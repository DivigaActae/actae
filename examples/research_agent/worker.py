#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Worker process for the durability scenario.

Runs the research pipeline and stops at a durable checkpoint, then hard-kills
itself (`os._exit(137)`) — exactly what happens when a worker is killed before
it finishes. The run's completed steps are already durable in Actae, so the
parent process resumes the same thread from that checkpoint and continues the
tail; the expensive prefix is not recomputed.

A separate process is used on purpose: a real crash is `os._exit`, not an
exception the same process can catch.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parent))

from actae_client import ActaeClient  # noqa: E402
from actae_client.adapters.langgraph import ActaeCheckpointSaver  # noqa: E402

import pipeline  # noqa: E402
import source  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(description="research-agent worker (durability scenario)")
    parser.add_argument("--thread", required=True)
    parser.add_argument("--topic", default=source.DEFAULT_TOPIC)
    parser.add_argument("--crash-after", default="extract_notes")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--model", default=pipeline.DEFAULT_MODEL)
    parser.add_argument("--url", default=None)
    parser.add_argument("--api-key", default=None)
    args = parser.parse_args()

    # No hardcoded endpoint/key: the parent process passes them via env.
    endpoint = args.url or os.environ.get("ACTAE_URL")
    api_key = args.api_key or os.environ.get("ACTAE_API_KEY")
    if not endpoint or not api_key:
        sys.exit("worker: ACTAE_URL and ACTAE_API_KEY must be set (env or --url/--api-key)")

    model = pipeline.ScriptedModel() if args.dry_run else pipeline.OpenAIModel(model=args.model)
    async with ActaeClient(endpoint=endpoint, api_key=api_key) as client:
        saver = ActaeCheckpointSaver(client, channel="research")
        graph = pipeline.build_graph(model, saver, pipeline.TokenLedger(), with_fact_check=False)
        cfg = {"configurable": {"thread_id": args.thread}}
        # Run to the chosen checkpoint and stop there (the run is now durable).
        await graph.ainvoke({"topic": args.topic}, cfg, interrupt_after=[args.crash_after])
        print(f"    [worker] paused at the durable checkpoint after '{args.crash_after}'")
        print("    [worker] simulating a hard kill (os._exit(137))")
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(137)


if __name__ == "__main__":
    asyncio.run(main())
