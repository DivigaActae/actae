#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Actae coordination demo — durable worker subprocess.

Joins a consumer group bound to a **work channel** (which carries only
``invoice.created`` events), claims batches, processes each invoice through the
idempotent tool-executions ledger (a completed side effect is replayed, never
re-executed, even when the group redelivers work after a crash), and acks
durable progress.

Two channels, two jobs:

* *work channel* — the group's queue. It must stay free of non-work events so
  claim/ack/redelivery semantics stay exact (acking past foreign in-flight
  events would skip them permanently).
* *audit channel* (``--audit-channel``) — every ledger execution writes its
  ``tool.*`` receipts here, plus ``worker.started``. This is the replayable,
  coordinated-retry audit trail: completed results are replayed, never
  re-executed.

Two run modes:

* ``--drain`` (default) — claim until the work channel has no work left, then
  exit 0.
* ``--crash`` — deterministic crash for the crash-recovery phase: fully process
  the first invoice (ledger + ack), ledger-complete the second invoice but die
  before acking it. The second invoice's side effect is already committed, so
  when the group redelivers it, the next worker must *replay* it — proving the
  "no completed side effect is re-executed" guarantee (within this demo's
  controlled scenario; the side effect itself is deterministic and the crash
  occurs after the ledger commit).

Every emitted line is ``KEY<TAB>JSON``; the orchestrator parses these.

Environment:
    ACTAE_URL       HTTP base URL (default http://localhost:8002)
    ACTAE_API_KEY   API key (default sk-dev-0000000000000000000000)

Usage:
    worker.py <work_channel> <group> <consumer_id> [--audit-channel CH]
              [--lease N] [--batch-limit N] [--crash] [--out FILE]
"""

import argparse
import asyncio
import json
import os
import sys
import time

from actae_client import ActaeClient

DEFAULT_URL = os.environ.get("ACTAE_URL", "http://localhost:8002")
DEFAULT_KEY = os.environ.get("ACTAE_API_KEY", "sk-dev-0000000000000000000000")

WORK_TYPE = "invoice.created"
TOOL_NAME = "process_invoice"


def _emit(out, key, **data):
    line = key + "\t" + json.dumps(data, default=str)
    if out:
        with open(out, "a") as fh:
            fh.write(line + "\n")
    else:
        print(line, flush=True)


async def _side_effect(invoice_id, amount):
    """Simulate the external side effect. Deterministic, so the demo can prove
    a completed result is replayed rather than re-executed. In a real system
    the same guarantee additionally depends on the downstream operation being
    idempotent or queryable."""
    await asyncio.sleep(0.05)
    return {"processed": True, "invoice_id": invoice_id,
            "amount": amount, "ts": time.time()}


async def _process_invoice(client, work_channel, audit_channel, group,
                           consumer_id, ev, out, actor):
    """Process one invoice through the idempotent execution ledger.

    The ledger execution lives on the *audit* channel, so its ``tool.*``
    receipts never pollute the group's work channel.
    """
    payload = ev.payload or {}
    invoice_id = payload.get("invoice_id")
    amount = payload.get("amount", 0)

    claim = await client.claim_execution(
        audit_channel,
        key_name=f"invoice:{invoice_id}",
        tool_name=TOOL_NAME,
        params={"invoice_id": invoice_id, "amount": amount},
        lease_seconds=60,
        emit_replay_event=True,
        actor=actor,
    )
    if claim.status == "replayed":
        _emit(out, "SIDE_EFFECT", invoice_id=invoice_id, cursor=ev.cursor,
              status="replayed", ran=False, execution_id=claim.execution.id)
        return
    if claim.status == "in_progress":
        _emit(out, "SIDE_EFFECT", invoice_id=invoice_id, cursor=ev.cursor,
              status="in_progress", ran=False)
        return

    result = await _side_effect(invoice_id, amount)
    await client.complete_execution(claim.execution.id, claim.claim_token,
                                    result=result)
    _emit(out, "SIDE_EFFECT", invoice_id=invoice_id, cursor=ev.cursor,
          status=claim.status, ran=True, execution_id=claim.execution.id)


async def _run_drain(client, work_channel, audit_channel, group, consumer_id,
                     lease, limit, empty_grace, out, actor):
    first_empty = None
    while True:
        work = await client.claim_work(group, consumer_id, limit=limit)
        if not work.events:
            if first_empty is None:
                first_empty = time.monotonic()
            elif time.monotonic() - first_empty >= empty_grace:
                _emit(out, "DRAINED", consumer=consumer_id)
                return
            await asyncio.sleep(1.0)
            continue
        first_empty = None
        invoices = [e for e in work.events if e.event_type == WORK_TYPE]
        for ev in invoices:
            await _process_invoice(client, work_channel, audit_channel, group,
                                   consumer_id, ev, out, actor)
        last = work.events[-1].cursor
        await client.ack_work(group, consumer_id, last)
        _emit(out, "ACKED", consumer=consumer_id, cursor=last, invoices=len(invoices))


async def _run_crash(client, work_channel, audit_channel, group, consumer_id,
                     lease, out, actor):
    """Claim everything; fully process invoice #1; ledger-complete invoice #2
    and die before acking it. Redelivery must replay, not re-run, invoice #2."""
    work = await client.claim_work(group, consumer_id, limit=100)
    invoices = [e for e in work.events if e.event_type == WORK_TYPE]
    if len(invoices) < 2:
        _emit(out, "CRASH_ABORT", consumer=consumer_id,
              reason=f"need >=2 invoices, got {len(invoices)}")
        return
    _emit(out, "CLAIMED", consumer=consumer_id, count=len(invoices),
          cursors=[e.cursor for e in invoices])

    first = invoices[0]
    await _process_invoice(client, work_channel, audit_channel, group,
                           consumer_id, first, out, actor)
    await client.ack_work(group, consumer_id, first.cursor)
    _emit(out, "ACKED", consumer=consumer_id, cursor=first.cursor)

    second = invoices[1]
    await _process_invoice(client, work_channel, audit_channel, group,
                           consumer_id, second, out, actor)
    _emit(out, "CRASH", consumer=consumer_id,
          ledger_completed_unacked=second.cursor)
    os._exit(1)


async def _heartbeat_loop(client, group, consumer_id, lease, out):
    while True:
        await asyncio.sleep(max(1.0, lease / 3))
        try:
            await client.heartbeat(group, consumer_id, lease_seconds=lease)
        except Exception as exc:  # pragma: no cover
            _emit(out, "HEARTBEAT_FAILED", consumer=consumer_id, error=str(exc))


async def _main(args):
    actor = args.consumer_id
    audit_channel = args.audit_channel or args.channel
    async with ActaeClient(api_key=DEFAULT_KEY, endpoint=DEFAULT_URL) as client:
        await client.record(channel_id=audit_channel, event_type="worker.started",
                            payload={"consumer": args.consumer_id, "kind": "python"},
                            actor=actor)
        _emit(args.out, "READY", consumer=args.consumer_id)
        await client.join_group(args.group, args.consumer_id, lease_seconds=args.lease)
        _emit(args.out, "JOINED", consumer=args.consumer_id, lease=args.lease)

        hb = asyncio.create_task(
            _heartbeat_loop(client, args.group, args.consumer_id, args.lease, args.out))
        try:
            if args.crash:
                await _run_crash(client, args.channel, audit_channel, args.group,
                                 args.consumer_id, args.lease, args.out, actor)
            else:
                await _run_drain(client, args.channel, audit_channel, args.group,
                                 args.consumer_id, args.lease, args.batch_limit,
                                 args.empty_grace, args.out, actor)
        finally:
            hb.cancel()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("channel", help="work channel to consume")
    parser.add_argument("group", help="consumer group id")
    parser.add_argument("consumer_id", help="stable consumer id")
    parser.add_argument("--audit-channel", default="",
                        help="channel for ledger receipts + worker lifecycle "
                             "(default: the work channel; the demo always passes "
                             "a separate audit channel so the work channel stays clean)")
    parser.add_argument("--lease", type=int, default=8, help="lease seconds (default 8)")
    parser.add_argument("--batch-limit", type=int, default=8, help="claim limit (default 8)")
    parser.add_argument("--empty-grace", type=float, default=5.0,
                        help="keep retrying empty claims for this many seconds before "
                             "draining (covers the lease-reaper window after a crash)")
    parser.add_argument("--crash", action="store_true", help="deterministic crash mode")
    parser.add_argument("--out", default="", help="append KEY<TAB>JSON lines to this file")
    args = parser.parse_args()
    try:
        asyncio.run(_main(args))
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
