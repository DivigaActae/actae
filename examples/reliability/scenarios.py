# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Live reliability scenarios: crash recovery, side-effect-safe forks, and
durable consumer coordination.

Each ``*_suite`` coroutine runs against a real Actae instance and returns a
:class:`report.Suite` of pass/fail checks. Exceptions are captured on the
suite (reported as a failure) so one broken suite does not hide the others.
"""

import asyncio
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List

from actae_client import ActaeClient
from actae_client.errors import (
    ExecutionNotOwnedError,
    ForkToolBlockedError,
    IdempotencyConflictError,
)
from actae_client.session import AgentSession, SessionLockedError

from atomicity_worker import operation_id_for
from report import Suite


def _events_of_type(client_events: List, event_type: str) -> List:
    return [e for e in client_events if e.event_type == event_type]


async def _replay(client: ActaeClient, channel: str, event_type: str) -> List:
    return await client.replay(channel, cursor=0, limit=500, event_type=event_type)


# ---------------------------------------------------------------------------
# Crash recovery (real process kill via worker.py)
# ---------------------------------------------------------------------------


async def crash_suite(client: ActaeClient, run_id: str, workdir: Path) -> Suite:
    suite = Suite("crash")
    tmpdir = Path(tempfile.mkdtemp(prefix="actae-reliability-"))
    try:
        channel = f"{run_id}-crash"
        effect_file = tmpdir / f"{run_id}-effects.txt"
        effect_file.write_text("", encoding="utf-8")

        worker = Path(__file__).resolve().parent / "worker.py"
        env = dict(os.environ)
        env.setdefault("ACTAE_API_KEY", "")
        env["ACTAE_URL"] = getattr(client, "_endpoint", env.get("ACTAE_URL", ""))
        env["ACTAE_ENDPOINT"] = env["ACTAE_URL"]

        proc = subprocess.run(
            [sys.executable, str(worker), channel, str(effect_file)],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        suite.add(
            "worker dies for real (exit 137)",
            proc.returncode == 137,
            f"returncode={proc.returncode}",
        )

        effect_count = len(effect_file.read_text(encoding="utf-8").splitlines())
        suite.add("the external effect applied exactly once", effect_count == 1, f"count={effect_count}")

        # Resume the crashed session from durable state. The authoritative
        # progress is the server-side event history + step index (session
        # metadata is written on lifecycle transitions, not every step), so the
        # evidence is that every recorded step survived the crash.
        history = await client.replay(channel, cursor=0, limit=500)
        step_events = [e for e in history if e.event_type in ("research", "charge")]
        suite.add(
            "both recorded steps survived the crash",
            len(step_events) == 2,
            f"n={len(step_events)}",
        )
        resumed = await AgentSession.resume(client, channel)
        suite.add(
            "resume returns a started session",
            resumed.status == "started",
            f"status={resumed.status}",
        )
        # The hard kill skipped the SDK's metadata write; resume must
        # reconstruct progress from the durable event log (not report 1).
        suite.add(
            "resume recovers the real step count after a hard kill",
            resumed.step_count == 2,
            f"step_count={resumed.step_count} (want 2)",
        )

        # Re-claim the same logical effect: the ledger must replay, not re-fire.
        replayed = await client.claim_execution(channel, "effect-1", "charge", {"amount": 5})
        suite.add(
            "re-claim after crash replays the recorded result",
            replayed.status == "replayed" and replayed.result == {"paid": True},
            f"status={replayed.status}",
        )
        effect_count_after = len(effect_file.read_text(encoding="utf-8").splitlines())
        suite.add(
            "the effect did not fire again on recovery",
            effect_count_after == 1,
            f"count={effect_count_after}",
        )
    except Exception as exc:  # noqa: BLE001 - reported, not swallowed
        suite.error = f"{type(exc).__name__}: {exc}"
    return suite


# ---------------------------------------------------------------------------
# Side-effect-safe forks
# ---------------------------------------------------------------------------


async def fork_suite(client: ActaeClient, run_id: str, workdir: Path) -> Suite:
    suite = Suite("fork")
    try:
        base = f"{run_id}-fork-base"
        session = AgentSession(
            client,
            base,
            state_fn=lambda: {"decisions": decisions},
            snapshot_interval=1,
        )
        decisions = [{"step": 1}]
        async with session:
            await session.step("research", output={"sources": 3})
            claim = await client.claim_execution(base, "email-1", "email.send", {"to": "a@b.c"})
            await client.complete_execution(
                claim.execution.id, claim.claim_token, result={"message_id": "m1"}
            )
            decisions.append({"step": 2})
            await session.step("send_email", output={"message_id": "m1"})

        # A. auto (default): an inherited identical call replays.
        auto = await session.fork(2, f"{run_id}-fork-auto")
        auto_claim = await client.claim_execution(
            auto.channel_id, "email-1", "email.send", {"to": "a@b.c"}
        )
        suite.add(
            "auto replays an inherited effect instead of re-firing it",
            auto_claim.status == "replayed" and auto_claim.result == {"message_id": "m1"},
            f"status={auto_claim.status}",
        )
        suite.add(
            "auto does not start the tool on the child",
            len(await _replay(client, auto.channel_id, "tool.started")) == 0,
        )

        # B. block: an irreversible call in the fork is refused + recorded.
        blocked = await session.fork(2, f"{run_id}-fork-block", tool_policies={"email.send": "block"})
        refused = False
        try:
            await client.claim_execution(blocked.channel_id, "email-2", "email.send", {"to": "x@y.z"})
        except ForkToolBlockedError:
            refused = True
        suite.add("block refuses a new irreversible call", refused)
        blocked_events = await _replay(client, blocked.channel_id, "tool.blocked")
        suite.add(
            "the blocked boundary is persisted as an event",
            len(blocked_events) == 1 and blocked_events[0].payload.get("policy") == "block",
        )

        # C. intervention is stored and exposed (application-applied).
        intervention = {"model": "candidate-model"}
        inter = await session.fork(1, f"{run_id}-fork-intervention", intervention=intervention)
        suite.add("intervention is exposed on the fork session", inter.intervention == intervention)

        meta = await client.get_channel_metadata(inter.channel_id)
        stored = (meta.experiment_metadata or {}).get("intervention")
        suite.add("intervention is recorded on fork metadata", stored == intervention)
    except Exception as exc:  # noqa: BLE001
        suite.error = f"{type(exc).__name__}: {exc}"
    return suite


# ---------------------------------------------------------------------------
# Durable consumer coordination
# ---------------------------------------------------------------------------


async def coordination_suite(client: ActaeClient, run_id: str, workdir: Path) -> Suite:
    suite = Suite("coordination")
    try:
        channel = f"{run_id}-coord"
        for i in range(5):
            await client.record(channel, "work", {"i": i}, actor="producer")

        group = f"{run_id}-group"
        await client.create_group(group, channel)

        await client.join_group(group, "consumer-a", lease_seconds=60)
        work_a = await client.claim_work(group, "consumer-a", limit=10)
        suite.add("consumer A claims the whole batch", len(work_a.events) == 5, f"n={len(work_a.events)}")

        await client.join_group(group, "consumer-b", lease_seconds=60)
        work_b = await client.claim_work(group, "consumer-b", limit=10)
        suite.add(
            "consumer B cannot double-claim A's active range",
            len(work_b.events) == 0,
            f"n={len(work_b.events)}",
        )

        if work_a.events:
            await client.ack_work(group, "consumer-a", work_a.events[-1].cursor)
        more = await client.claim_work(group, "consumer-a", limit=10)
        suite.add("acked work is not redelivered", len(more.events) == 0, f"n={len(more.events)}")

        # Lease expiry: a short lease lets a crashed consumer's range be
        # reclaimed by the same consumer id after it lapses.
        exp_channel = f"{run_id}-coord-exp"
        for i in range(3):
            await client.record(exp_channel, "work", {"i": i}, actor="producer")
        exp_group = f"{run_id}-group-exp"
        await client.create_group(exp_group, exp_channel)
        await client.join_group(exp_group, "worker-1", lease_seconds=1)
        first = await client.claim_work(exp_group, "worker-1", limit=10)
        await asyncio.sleep(2.2)
        await client.join_group(exp_group, "worker-1", lease_seconds=60)
        reclaimed = await client.claim_work(exp_group, "worker-1", limit=10)
        suite.add(
            "a lapsed lease's unacked work is redelivered",
            len(first.events) == 3 and len(reclaimed.events) == len(first.events),
            f"first={len(first.events)} reclaimed={len(reclaimed.events)}",
        )
    except Exception as exc:  # noqa: BLE001
        suite.error = f"{type(exc).__name__}: {exc}"
    return suite


# ---------------------------------------------------------------------------
# Stale-worker / split-brain fencing
# ---------------------------------------------------------------------------


async def stale_worker_suite(client: ActaeClient, run_id: str, workdir: Path) -> Suite:
    """Prove where ownership IS fenced and document where it is not.

    Actae fences *tool executions* (claim_token) and *consumer groups*
    (leases). A bare channel write is not fenced: the server authenticates the
    caller, it does not own the agent loop. This suite asserts both facts so a
    future change that adds session-level fencing breaks the test loudly and
    forces the docs to be updated.
    """
    suite = Suite("stale_worker")
    try:
        channel = f"{run_id}-stale"

        # 1. A lapsed lease is reclaimable, and the original owner's token no
        #    longer works once another owner reclaims it (fencing).
        stale = await client.claim_execution(channel, "charge-1", "charge", {"amount": 5}, lease_seconds=1)
        suite.add("worker A claims the effect", stale.status == "claimed", f"status={stale.status}")
        await asyncio.sleep(1.6)
        reclaimed = await client.claim_execution(channel, "charge-1", "charge", {"amount": 5}, lease_seconds=30)
        suite.add(
            "worker B reclaims after the lease lapses",
            reclaimed.status == "reclaimed",
            f"status={reclaimed.status}",
        )
        fenced = False
        try:
            await client.complete_execution(
                stale.execution.id, stale.claim_token, result={"paid": True}
            )
        except ExecutionNotOwnedError:
            fenced = True
        suite.add(
            "the stale owner's complete is fenced (execution_not_owned)",
            fenced,
        )

        # 2. An active heartbeat keeps the lease, so a would-be reclaim sees
        #    in_progress instead of stealing the run.
        held = await client.claim_execution(channel, "charge-2", "charge", {"amount": 9}, lease_seconds=1)
        await asyncio.sleep(0.5)
        await client.heartbeat_execution(held.execution.id, held.claim_token, lease_seconds=5)
        await asyncio.sleep(0.8)
        contender = await client.claim_execution(channel, "charge-2", "charge", {"amount": 9}, lease_seconds=30)
        suite.add(
            "a heartbeated lease is not stolen (in_progress)",
            contender.status == "in_progress",
            f"status={contender.status}",
        )
        await client.complete_execution(held.execution.id, held.claim_token, result={"paid": True})

        # 3. The honest boundary: a bare channel write from any authenticated
        #    actor is accepted. Actae does not fence the agent loop, so side
        #    effects must be owned through claim_execution / consumer groups.
        bare = f"{run_id}-bare"
        await client.record(bare, "worker.step", {"who": "worker-a"}, actor="worker-a")
        await client.record(bare, "worker.step", {"who": "stale-worker"}, actor="stale-worker")
        events = await client.replay(bare, cursor=0, limit=100)
        actors = sorted({e.actor for e in events})
        suite.add(
            "bare channel writes are not fenced (documented boundary)",
            len(events) == 2 and actors == ["stale-worker", "worker-a"],
            f"n={len(events)} actors={actors} — fence side effects via claim_execution",
        )
    except Exception as exc:  # noqa: BLE001
        suite.error = f"{type(exc).__name__}: {exc}"
    return suite


# ---------------------------------------------------------------------------
# Atomic event + state transitions across a crash
# ---------------------------------------------------------------------------


async def atomicity_suite(client: ActaeClient, run_id: str, workdir: Path) -> Suite:
    """Prove event+snapshot atomicity and transition idempotency across a kill.

    A worker commits a ``transition`` (event AND state in one server-side
    transaction) then dies before reading the response. On recovery the same
    ``operation_id`` must replay the original — never a second event or a
    snapshot without its event.
    """
    suite = Suite("atomicity")
    tmpdir = Path(tempfile.mkdtemp(prefix="actae-atomicity-"))
    try:
        channel = f"{run_id}-atomic"
        step = 3
        op_id = operation_id_for(channel, step)

        worker = Path(__file__).resolve().parent / "atomicity_worker.py"
        env = dict(os.environ)
        env.setdefault("ACTAE_API_KEY", "")
        env["ACTAE_URL"] = getattr(client, "_endpoint", env.get("ACTAE_URL", ""))
        env["ACTAE_ENDPOINT"] = env["ACTAE_URL"]

        proc = subprocess.run(
            [sys.executable, str(worker), channel, str(step)],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        suite.add(
            "transition worker dies for real (exit 137)",
            proc.returncode == 137,
            f"returncode={proc.returncode}",
        )

        history = await client.replay(channel, cursor=0, limit=500)
        step_events = [e for e in history if e.event_type == "atomic.step"]
        suite.add(
            "exactly one event committed",
            len(step_events) == 1,
            f"n={len(step_events)}",
        )

        state = await client.latest_state(channel)
        suite.add(
            "state snapshot committed with the event",
            state is not None and state.get("state") == {"step": step, "done": True},
            f"state={state}",
        )
        suite.add(
            "snapshot cursor equals the event cursor",
            bool(step_events)
            and state is not None
            and state.get("cursor") == step_events[0].cursor,
            f"event_cursor={step_events[0].cursor if step_events else None} "
            f"state_cursor={state.get('cursor') if state else None}",
        )

        # Recovery: re-send the identical transition with the same operation_id.
        replay = await client.transition(
            channel,
            "atomic.step",
            {"step": step},
            {"step": step, "done": True},
            actor="atomicity-worker",
            operation_id=op_id,
        )
        suite.add(
            "the recovery re-send replays the original event",
            bool(step_events) and replay.event.id == step_events[0].id,
            f"replayed_id={replay.event.id} original_id={step_events[0].id if step_events else None}",
        )
        after = await client.replay(channel, cursor=0, limit=500)
        suite.add(
            "no duplicate event was written on recovery",
            len([e for e in after if e.event_type == "atomic.step"]) == 1,
            f"n={len(after)}",
        )

        # A reused operation_id with different content is a loud conflict.
        conflict = False
        try:
            await client.transition(
                channel,
                "atomic.step",
                {"step": step, "tampered": True},
                {"step": step, "done": True},
                actor="atomicity-worker",
                operation_id=op_id,
            )
        except IdempotencyConflictError:
            conflict = True
        suite.add(
            "a reused operation_id with different content is rejected",
            conflict,
        )

        # Negative case: a channel that never received a transition has neither
        # an event nor a snapshot.
        empty = f"{run_id}-atomic-empty"
        empty_events = await client.replay(empty, cursor=0, limit=10)
        empty_state = await client.latest_state(empty)
        suite.add(
            "no transition leaves neither event nor snapshot",
            len(empty_events) == 0 and empty_state is None,
            f"events={len(empty_events)} state={empty_state}",
        )
    except Exception as exc:  # noqa: BLE001
        suite.error = f"{type(exc).__name__}: {exc}"
    return suite


# ---------------------------------------------------------------------------
# Long-running tool vs lease: reclaim, fencing and duplication
# ---------------------------------------------------------------------------


async def long_tool_suite(client: ActaeClient, run_id: str, workdir: Path) -> Suite:
    """A tool that outlives its lease, with and without heartbeating.

    This is the end-to-end lease contract the primitive tests only imply:

      * NO heartbeat: the lease lapses, a second worker reclaims and runs the
        tool, and the original owner's completion is fenced. Against a
        non-idempotent downstream the effect therefore fires twice — the
        documented cost of a tool that outlives its lease.
      * heartbeat: the owner renews across the wait, the second worker sees
        ``in_progress`` and does not run the tool, and the effect fires once.

    The downstream is a local list (non-idempotent): every apply appends.
    """
    suite = Suite("long_tool")
    try:
        # --- A. no heartbeat: lease lapses -> reclaim -> duplicate ---
        channel = f"{run_id}-long-nohb"
        effects: List[str] = []

        claim = await client.claim_execution(
            channel, "long-1", "slow.tool", {"op": 1}, lease_seconds=1
        )
        suite.add("the owner claims the long tool", claim.status == "claimed", f"status={claim.status}")

        owner_outcome: List[str] = []

        async def slow_owner_no_heartbeat() -> None:
            effects.append("long-1")  # the tool body starts (effect #1)
            await asyncio.sleep(2.0)  # outlives the 1s lease
            try:
                await client.complete_execution(
                    claim.execution.id, claim.claim_token, result={"ok": True}
                )
                owner_outcome.append("completed")
            except ExecutionNotOwnedError:
                owner_outcome.append("fenced")

        task = asyncio.create_task(slow_owner_no_heartbeat())
        await asyncio.sleep(1.4)  # past the lease
        contender = await client.claim_execution(
            channel, "long-1", "slow.tool", {"op": 1}, lease_seconds=30
        )
        suite.add(
            "a tool that outlives its lease is reclaimable",
            contender.status == "reclaimed",
            f"status={contender.status}",
        )
        effects.append("long-1")  # the contender's tool body runs (effect #2)
        await task
        suite.add(
            "the stale owner's completion is fenced",
            owner_outcome == ["fenced"],
            f"owner={owner_outcome}",
        )
        suite.add(
            "the non-idempotent downstream fired twice (the documented lease cost)",
            len(effects) == 2,
            f"applications={len(effects)}",
        )

        # --- B. heartbeat: the owner keeps the lease -> no reclaim, one effect ---
        hb_channel = f"{run_id}-long-hb"
        hb_effects: List[str] = []

        hb_claim = await client.claim_execution(
            hb_channel, "long-2", "slow.tool", {"op": 2}, lease_seconds=1
        )
        hb_outcome: List[str] = []

        async def slow_owner_heartbeat() -> None:
            hb_effects.append("long-2")
            for _ in range(4):
                await asyncio.sleep(0.4)
                await client.heartbeat_execution(
                    hb_claim.execution.id, hb_claim.claim_token, lease_seconds=5
                )
            await client.complete_execution(
                hb_claim.execution.id, hb_claim.claim_token, result={"ok": True}
            )
            hb_outcome.append("completed")

        hb_task = asyncio.create_task(slow_owner_heartbeat())
        await asyncio.sleep(1.4)
        hb_contender = await client.claim_execution(
            hb_channel, "long-2", "slow.tool", {"op": 2}, lease_seconds=30
        )
        suite.add(
            "a heartbeated long tool is not reclaimed",
            hb_contender.status == "in_progress",
            f"status={hb_contender.status}",
        )
        await hb_task
        suite.add(
            "the heartbeating owner completes normally",
            hb_outcome == ["completed"],
            f"owner={hb_outcome}",
        )
        suite.add(
            "the downstream fired once when the lease was held",
            len(hb_effects) == 1,
            f"applications={len(hb_effects)}",
        )
    except Exception as exc:  # noqa: BLE001
        suite.error = f"{type(exc).__name__}: {exc}"
    return suite


# ---------------------------------------------------------------------------
# Session ownership fence (channel-level, opt-in)
# ---------------------------------------------------------------------------


async def session_fence_suite(client: ActaeClient, run_id: str, workdir: Path) -> Suite:
    """A fenced AgentSession owns its channel; stale owners are rejected.

    ``AgentSession(fence_owner=...)`` claims a per-channel lease through the
    idempotent execution ledger. This proves the three properties that matter:
    a second live owner cannot start, a lapsed lease can be reclaimed, and the
    original owner's next step is rejected (no silent append) once reclaimed.
    """
    suite = Suite("session_fence")
    try:
        channel = f"{run_id}-fence"
        owner = AgentSession(
            client, channel, fence_owner="owner-A", fence_lease_seconds=1, snapshot_interval=1
        )
        await owner.__aenter__()
        try:
            await owner.step("work", output={"who": "A"})

            # 1. A second live owner cannot start.
            contender = AgentSession(
                client, channel, fence_owner="owner-B", fence_lease_seconds=30, snapshot_interval=1
            )
            blocked = False
            try:
                await contender.__aenter__()
            except SessionLockedError:
                blocked = True
            suite.add("a second live owner cannot start", blocked)

            # 2. After the lease lapses, a fresh owner reclaims the channel.
            await asyncio.sleep(1.4)
            taker = AgentSession(
                client, channel, fence_owner="owner-B", fence_lease_seconds=30, snapshot_interval=1
            )
            await taker.__aenter__()
            await taker.step("work", output={"who": "B"})
            suite.add("a lapsed lease can be reclaimed by a new owner", True)

            # 3. The original owner's next step is rejected with no append.
            before = len(await client.replay(channel, cursor=0, limit=500))
            stale = False
            try:
                await owner.step("work", output={"who": "A2"})
            except SessionLockedError:
                stale = True
            after = len(await client.replay(channel, cursor=0, limit=500))
            suite.add("the stale owner's step is rejected", stale)
            suite.add("the rejected step appended no event", before == after, f"{before}->{after}")

            await taker.__aexit__(None, None, None)
            await owner.__aexit__(None, None, None)
        except Exception:
            await owner.__aexit__(None, None, None)
            raise
    except Exception as exc:  # noqa: BLE001
        suite.error = f"{type(exc).__name__}: {exc}"
    return suite


SUITES = {
    "crash": crash_suite,
    "fork": fork_suite,
    "coordination": coordination_suite,
    "stale_worker": stale_worker_suite,
    "atomicity": atomicity_suite,
    "long_tool": long_tool_suite,
    "session_fence": session_fence_suite,
}
