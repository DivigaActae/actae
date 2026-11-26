#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Atomicity worker for the reliability suite — commits a transition, then dies.

Run as a subprocess by ``scenarios.atomicity_suite``. It sends
``POST /api/v1/events/transition`` with a deterministic ``operation_id`` (so the
recovery re-send is provably the same logical transition), then calls
``os._exit(137)`` *without any cleanup*. The parent then proves that:

  * the event and its state snapshot committed together (one ``agent_events``
    row and a snapshot whose cursor equals the event's cursor), and
  * re-sending the identical transition with the same ``operation_id`` replays
    the original instead of writing a second event + snapshot.

    python atomicity_worker.py <channel_id> <step>

The worker never reads the response — the kill happens immediately after the
request is committed, so the parent observes the state a recovering worker
would find after an ambiguous response loss.
"""

import asyncio
import os
import sys
import uuid


def _op_id(channel: str, step: int) -> str:
    # Deterministic: the recovery re-send derives the same id.
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"atomicity|{channel}|{step}"))


def operation_id_for(channel: str, step: int) -> str:
    """Public helper reused by ``scenarios.atomicity_suite``."""
    return _op_id(channel, step)


async def main() -> None:
    channel_id = sys.argv[1]
    step = int(sys.argv[2])

    from actae_client import ActaeClient

    api_key = os.environ["ACTAE_API_KEY"]
    endpoint = os.environ.get("ACTAE_URL") or os.environ["ACTAE_ENDPOINT"]

    client = ActaeClient(api_key=api_key, endpoint=endpoint)
    await client.connect()

    await client.transition(
        channel_id,
        "atomic.step",
        {"step": step},
        {"step": step, "done": True},
        actor="atomicity-worker",
        operation_id=_op_id(channel_id, step),
    )

    # Hard crash immediately after the commit: no response consumed, no
    # finally block, no flushed buffers.
    sys.stdout.flush()
    os._exit(137)


if __name__ == "__main__":
    asyncio.run(main())
