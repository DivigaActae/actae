#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Crash worker for the reliability suite — dies for real, on purpose.

Run as a subprocess by ``scenarios.crash_suite``. It records a couple of agent
steps, performs one external side effect through Actae's idempotent execution
ledger, then calls ``os._exit(137)`` *without any cleanup* so the parent can
prove crash recovery against a genuinely dead process (no finally blocks, no
flushed buffers, no graceful session close).

    python worker.py <channel_id> <effect_file>

The external system is a plain file: each successful application appends one
line. Only the ledger strategy can keep that at exactly one line, because the
ledger gives the side effect a stable identity across the crash and the resume.
"""

import asyncio
import os
import sys


async def main() -> None:
    channel_id = sys.argv[1]
    effect_file = sys.argv[2]

    from actae_client import ActaeClient
    from actae_client.session import AgentSession

    api_key = os.environ["ACTAE_API_KEY"]
    endpoint = os.environ.get("ACTAE_URL") or os.environ["ACTAE_ENDPOINT"]

    client = ActaeClient(api_key=api_key, endpoint=endpoint)
    await client.connect()

    session = AgentSession(
        client,
        channel_id,
        state_fn=lambda: {"step": session.step_count},
        snapshot_interval=1,
    )
    # `async with` starts the session; os._exit() below bypasses __aexit__, so
    # the session is genuinely interrupted rather than completed.
    async with session:
        await session.step("research", output={"sources": 3})

        # The external side effect, coordinated through the ledger with a stable
        # key. The claim must complete before the crash so the resume can replay
        # the recorded result instead of applying the effect twice.
        claim = await client.claim_execution(channel_id, "effect-1", "charge", {"amount": 5})
        if claim.status in ("claimed", "reclaimed"):
            with open(effect_file, "a", encoding="utf-8") as handle:
                handle.write("charged\n")
            await client.complete_execution(
                claim.execution.id, claim.claim_token, result={"paid": True}
            )

        await session.step("charge", output={"paid": True})

        # Hard crash: kill the process immediately with exit code 137
        # (128+SIGKILL) so no cleanup, flush or graceful close runs.
        sys.stdout.flush()
        os._exit(137)


if __name__ == "__main__":
    asyncio.run(main())
