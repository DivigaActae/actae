#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Fork intervention demo — change one decision without re-firing an effect.

The pitch
---------
Your agent made the wrong decision at step N. You want to try a different
model/prompt/action from that exact boundary. Two things must be true for that
to be safe (and both are Actae primitives):

  1. **The fork must not re-run the prefix.** Forking at step N inherits the
     real saved state, so the application continues at N+1. (See
     ``fork_resume_demo.py`` for the token-savings side of this.)
  2. **The fork must not silently re-fire a side effect the parent already
     performed.** The fork's ``tool_policies`` decides what happens when the
     child reaches a tool call: ``auto`` replays an exact inherited result,
     ``replay``/``block`` refuse, ``live`` is today's behavior.

This demo is framework- and LLM-free (deterministic, no API key) and proves
the whole interaction end to end against a running Actae:

  A. ``auto`` (default): the child re-issues an inherited tool call with
     identical params → the server replays the parent's recorded result and
     the tool body never runs again (no duplicate email/charge/write).
  B. ``block``: the fork refuses a *new* irreversible tool call and persists
     an immutable ``tool.blocked`` boundary (``409 fork_tool_blocked``).
  C. ``intervention``: the intended change (e.g. a different model) is stored
     on the fork metadata; **the application** reads ``session.intervention``
     and applies it. Actae never interprets or executes it.

Requirements:
    pip install -e ./sdks/python
    cd actae && cargo run        # Actae on :8002

Usage:
    python examples/fork_intervention_demo.py

Exit code 0 when every guarantee holds, 1 otherwise.
"""

import asyncio
import os
import sys
import uuid
from typing import Any, Dict, List, Optional

from actae_client import ActaeClient
from actae_client.errors import ForkToolBlockedError
from actae_client.session import AgentSession

ACTAE_URL = os.getenv("ACTAE_URL", "http://localhost:8002")
ACTAE_API_KEY = os.getenv("ACTAE_API_KEY", "sk-dev-0000000000000000000000")


class Report:
    """Tiny PASS/FAIL accumulator with a non-zero exit on any failure."""

    def __init__(self) -> None:
        self.results: List[tuple[str, bool, str]] = []

    def check(self, name: str, ok: bool, detail: str = "") -> None:
        self.results.append((name, ok, detail))
        mark = "PASS" if ok else "FAIL"
        print(f"  [{mark}] {name}" + (f" — {detail}" if detail else ""))

    def finish(self) -> int:
        failed = [r for r in self.results if not r[1]]
        print()
        print(f"{len(self.results) - len(failed)}/{len(self.results)} checks passed")
        return 1 if failed else 0


def _state_fn(steps: List[Dict[str, Any]]):
    """State snapshot callback: the accumulated decision history."""

    def _fn() -> Dict[str, Any]:
        return {"decisions": list(steps)}

    return _fn


async def _run_baseline(client: ActaeClient, name: str) -> AgentSession:
    """Run a 3-step pipeline, including a side effect at step 2.

    Returns the completed baseline session (valid fork source).
    """
    decisions: List[Dict[str, Any]] = []
    session = AgentSession(
        client,
        name,
        params={"model": "baseline-model"},
        state_fn=_state_fn(decisions),
        snapshot_interval=1,
    )
    async with session:
        # Step 1: research.
        decisions.append({"step": 1, "action": "research"})
        await session.step("research", input={"q": "durable agents"}, output={"sources": 3})

        # Step 2: a real, irreversible side effect, recorded through the
        # idempotent execution ledger (the tool is "sent" exactly once).
        decisions.append({"step": 2, "action": "send_email"})
        claim = await client.claim_execution(
            session.channel_id,
            "email-1",
            "email.send",
            {"to": "alice@example.com", "subject": "Report"},
        )
        assert claim.status == "claimed", claim.status
        await client.complete_execution(
            claim.execution.id, claim.claim_token, result={"message_id": "msg-aaa"}
        )
        await session.step("send_email", input={"to": "alice@example.com"}, output={"message_id": "msg-aaa"})

        # Step 3: finalize.
        decisions.append({"step": 3, "action": "finalize"})
        await session.step("finalize", output={"summary": "done"})
    return session


async def demonstrate_auto_replay(
    client: ActaeClient, baseline: AgentSession, report: Report
) -> str:
    """auto: an inherited tool call with identical params replays, never re-runs."""
    print("\nA. auto — inherited side effect replays instead of re-firing")
    forked = await _fork(baseline, f"{baseline.channel_id}-auto", at_step=2, tool_policies=None)

    # The application continues at step 3 on the child. It re-issues the same
    # logical tool call (same key, same params) — e.g. a retry or a rewritten
    # step that still needs the receipt.
    claim = await client.claim_execution(
        forked.channel_id,
        "email-1",
        "email.send",
        {"to": "alice@example.com", "subject": "Report"},
    )
    report.check(
        "auto replays the inherited result",
        claim.status == "replayed" and claim.result == {"message_id": "msg-aaa"},
        f"status={claim.status} result={claim.result}",
    )
    started = await client.replay(forked.channel_id, cursor=0, limit=200, event_type="tool.started")
    report.check("auto did not start the tool on the child", len(started) == 0, f"{len(started)} tool.started events")
    return forked.channel_id


async def demonstrate_block(client: ActaeClient, baseline: AgentSession, report: Report) -> str:
    """block: a fork refuses a new irreversible call and records the boundary."""
    print("\nB. block — a fork refuses an irreversible tool and records the boundary")
    forked = await _fork(
        baseline,
        f"{baseline.channel_id}-block",
        at_step=2,
        tool_policies={"email.send": "block"},
    )
    blocked = False
    try:
        await client.claim_execution(
            forked.channel_id,
            "email-2",
            "email.send",
            {"to": "bob@example.com", "subject": "Report"},
        )
    except ForkToolBlockedError as exc:
        blocked = True
        report.check("block refuses the call (409 fork_tool_blocked)", True, str(exc))
    report.check("block did not execute the tool", blocked)

    started = await client.replay(forked.channel_id, cursor=0, limit=200, event_type="tool.started")
    boundaries = await client.replay(forked.channel_id, cursor=0, limit=200, event_type="tool.blocked")
    report.check("no tool.started on the blocked child", len(started) == 0)
    report.check(
        "blocked boundary persisted as an immutable event",
        len(boundaries) == 1 and boundaries[0].payload.get("policy") == "block",
        boundaries[0].payload.get("boundary") if boundaries else "none",
    )
    return forked.channel_id


async def demonstrate_intervention(
    client: ActaeClient, baseline: AgentSession, report: Report
) -> str:
    """intervention: Actae stores the descriptor; the app applies it."""
    print("\nC. intervention — the application, not Actae, applies the change")
    forked = await _fork(
        baseline,
        f"{baseline.channel_id}-intervention",
        at_step=1,
        tool_policies={"email.send": "block"},
        intervention={"model": "candidate-model", "temperature": 0.2},
    )
    report.check(
        "intervention survives on the fork session",
        forked.intervention == {"model": "candidate-model", "temperature": 0.2},
        str(forked.intervention),
    )
    # The application reads the descriptor and chooses behaviour. Actae does
    # not run the model; this line stands in for the caller's own logic.
    chosen_model = forked.intervention.get("model", "baseline-model")
    report.check("application applies the intervention", chosen_model == "candidate-model", chosen_model)

    meta = await client.get_channel_metadata(forked.channel_id)
    stored = (meta.experiment_metadata or {}).get("intervention")
    report.check("intervention recorded on fork metadata", stored == forked.intervention, str(stored))
    return forked.channel_id


async def _fork(
    parent: AgentSession,
    name: str,
    *,
    at_step: int,
    tool_policies: Optional[Dict[str, Any]],
    intervention: Optional[Dict[str, Any]] = None,
) -> AgentSession:
    """Fork the completed baseline and return the primed (unstarted) child."""
    return await parent.fork(
        at_step,
        name,
        tool_policies=tool_policies,
        intervention=intervention,
    )


async def main() -> int:
    client = ActaeClient(api_key=ACTAE_API_KEY, endpoint=ACTAE_URL)
    await client.connect()
    report = Report()
    run_id = f"fork-intervention-{uuid.uuid4().hex[:8]}"
    try:
        print(f"Actae: {ACTAE_URL}")
        print(f"Run:   {run_id}")
        baseline = await _run_baseline(client, f"{run_id}-base")
        report.check("baseline recorded the email side effect", True, "email-1 completed once")

        await demonstrate_auto_replay(client, baseline, report)
        await demonstrate_block(client, baseline, report)
        await demonstrate_intervention(client, baseline, report)
    finally:
        await client.disconnect()
    return report.finish()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
