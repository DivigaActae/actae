#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Repo-agent worker — runs the maintenance agent as a real subprocess.

Used by ``demo.py`` for the crash scenario: it runs the agent and, when
``--crash-at N`` is given, calls ``os._exit(137)`` immediately after step N's
side effect (before the step is recorded), so recovery is tested against a
genuinely dead process.

    python worker.py --channel CH --workspace DIR --task-service URL [--crash-at N] [--correct]

Environment: ``ACTAE_URL`` / ``ACTAE_ENDPOINT`` and ``ACTAE_API_KEY``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from agent import DryRunPolicy, LLMPolicy, RepoAgent  # noqa: E402
from tools import RepoWorkspace  # noqa: E402


async def main() -> int:
    parser = argparse.ArgumentParser(prog="repo_agent worker")
    parser.add_argument("--channel", required=True)
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--task-service", required=True)
    parser.add_argument("--crash-at", type=int, default=None)
    parser.add_argument("--llm", action="store_true")
    parser.add_argument("--correct", action="store_true")
    parser.add_argument("--report-downstream", default="repo-report-1")
    args = parser.parse_args()

    from actae_client import ActaeClient
    from actae_client.session import AgentSession

    api_key = os.environ["ACTAE_API_KEY"]
    endpoint = os.environ.get("ACTAE_URL") or os.environ["ACTAE_ENDPOINT"]

    client = ActaeClient(api_key=api_key, endpoint=endpoint)
    await client.connect()
    try:
        workspace = RepoWorkspace(Path(args.workspace))
        state = {"invoked": []}
        session = AgentSession(
            client,
            args.channel,
            state_fn=lambda: {"invoked": list(state["invoked"])},
            snapshot_interval=1,
        )
        policy = LLMPolicy() if args.llm else DryRunPolicy(correct=args.correct)
        agent = RepoAgent(
            client, session, workspace, args.task_service, policy,
            report_downstream_key=args.report_downstream, state_sink=state,
        )
        async with session:
            result = await agent.run(start_step=1, crash_at=args.crash_at)
        print(json.dumps({"final_hash": result.final_hash, "tests_passed": result.tests_passed}))
        return 0
    finally:
        await client.disconnect()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
