#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Refund agent demo: a real crash inside the ambiguous window, then recovery.

The agent processes a customer refund. The refund is an external side effect
(the mock payment provider), guarded by Actae's execution ledger with a stable
idempotency key.

Sequence:

    run --crash     the provider applies the refund, then the worker is killed
                    (exit 137) before the result is recorded
    recover         the ledger sees the unfinished claim, the lease is
                    reclaimed, and the retry presents the same idempotency key
                    — the provider deduplicates, so the customer is charged once
    fork            a second continuation from the same saved prefix, with a
                    different refund amount, compared against the first

Usage:
    python agent.py --endpoint http://127.0.0.1:8022 --api-key sk-... --mode run --crash
    python agent.py ... --mode recover
    python agent.py ... --mode fork
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import urllib.request
from typing import Any, Dict, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "sdks", "python", "src"))

from actae_client import ActaeClient  # noqa: E402


PROVIDER = "http://127.0.0.1:9099"


def post(path: str, body: Dict[str, Any], idempotency_key: str) -> Dict[str, Any]:
    request = urllib.request.Request(
        f"{PROVIDER}{path}",
        data=json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key,
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read())


def stats(order: str) -> Dict[str, Any]:
    with urllib.request.urlopen(f"{PROVIDER}/stats?order={order}", timeout=5) as response:
        return json.loads(response.read())


async def claim_with_reclaim(
    client: ActaeClient, channel: str, key: str, tool: str, params: Dict[str, Any]
):
    """Claim, waiting out a short lease if the previous worker still holds it."""
    for _ in range(30):
        claim = await client.claim_execution(
            channel,
            key,
            tool,
            params,
            lease_seconds=1,
            actor="refund-agent",
        )
        if claim.status != "in_progress":
            return claim
        await asyncio.sleep(1)
    raise RuntimeError("could not reclaim the execution")


async def refund_step(
    client: ActaeClient,
    channel: str,
    order: str,
    amount_cents: int,
    *,
    crash: bool,
    key_suffix: str = "",
) -> Dict[str, Any]:
    key = f"refund:{order}{key_suffix}"
    params = {"order": order, "amount_cents": amount_cents, "currency": "EUR"}

    claim = await claim_with_reclaim(client, channel, key, "payment.refund", params)

    if claim.status == "replayed":
        print("[ledger] replayed — recorded refund result reused")
        print("[provider] not called again")
        return claim.result or {}

    print(f"[ledger] claim {claim.status} (key={key})")
    if claim.status == "reclaimed":
        print("[ledger] previous worker died mid-call; ownership moved with a fencing token")

    record = post(
        "/refund",
        {"order": order, "amount_cents": amount_cents, "currency": "EUR"},
        idempotency_key=key,
    )
    amount = record.get("amount_cents", amount_cents) / 100
    print(f"[provider] refund {record.get('refund_id')} applied: €{amount:.2f}")

    if crash:
        print("[worker] killed (exit 137) before the result was recorded")
        sys.stdout.flush()
        os._exit(137)

    await client.complete_execution(
        claim.execution.id, claim.claim_token, result=record
    )
    print("[ledger] completed — result recorded")
    return record


async def email_step(client: ActaeClient, channel: str, order: str) -> None:
    key = f"email:{order}"
    params = {"order": order, "to": "customer@example.com"}
    claim = await claim_with_reclaim(client, channel, key, "email.send", params)
    if claim.status == "replayed":
        print("[ledger] email already sent — replayed, not resent")
        return
    record = post("/email", params, idempotency_key=key)
    await client.complete_execution(
        claim.execution.id, claim.claim_token, result=record
    )
    print(f"[provider] confirmation email {record.get('message_id')} sent")


async def run(args: argparse.Namespace) -> None:
    channel = f"refund-agent:{args.order}"
    async with ActaeClient(api_key=args.api_key, endpoint=args.endpoint) as client:
        read_event = await client.record(
            channel,
            "order.read",
            {"order": args.order, "amount_cents": args.amount, "currency": "EUR"},
            actor="refund-agent",
        )
        await client.save_state(
            channel,
            read_event.cursor,
            {"order": args.order, "decision": "refund", "amount_cents": args.amount},
        )

        refund = await refund_step(
            client, channel, args.order, args.amount, crash=args.crash
        )
        await email_step(client, channel, args.order)
        await client.record(
            channel,
            "order.completed",
            {"order": args.order, "refund_id": refund.get("refund_id")},
            actor="refund-agent",
        )

        totals = stats(args.order)
        print()
        print(f"[audit] charges applied for #{args.order}: {totals['orders_charged']}")
        print(f"[audit] provider deduplicated retries:       {totals['deduplicated_retries']}")
        print(f"[audit] emails sent:                         {len(totals['emails'])}")
        print(f"[audit] channel: {channel}")
        print(f"[audit] dashboard: {args.endpoint}")


async def fork(args: argparse.Namespace) -> None:
    source = f"refund-agent:{args.order}"
    target = f"refund-agent:{args.order}:partial"
    async with ActaeClient(api_key=args.api_key, endpoint=args.endpoint) as client:
        events = await client.replay(source)
        boundary = next(
            (e.cursor for e in events if getattr(e, "event_type", "") == "order.read"),
            0,
        )
        receipt = await client.fork(
            source,
            target,
            at_cursor=boundary,
            display_name="partial refund",
            reason="customer asked for a partial refund after the full refund ran",
        )
        print(f"[fork] {source} → {target} at cursor {boundary}")
        print(
            f"[fork] resolved cursor {receipt.resolved_cursor} · state v{receipt.source_state_version}"
            f" · restorable={receipt.restorable}"
        )

        refund = await refund_step(
            client, target, args.order, args.amount, crash=False, key_suffix=":partial"
        )
        await email_step(client, target, args.order)
        await client.record(
            target,
            "order.completed",
            {"order": args.order, "refund_id": refund.get("refund_id")},
            actor="refund-agent",
        )

        totals = stats(args.order)
        print()
        print(f"[compare] full refund:    €{args.full / 100:.2f} on {source}")
        print(f"[compare] partial refund: €{args.amount / 100:.2f} on {target}")
        print(f"[audit] charges applied for #{args.order}: {totals['orders_charged']}")
        print(f"[audit] dashboard: {args.endpoint}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="http://127.0.0.1:8022")
    parser.add_argument("--api-key", default=os.environ.get("ACTAE_API_KEY", ""))
    parser.add_argument("--order", default="4471")
    parser.add_argument("--amount", type=int, default=4900)
    parser.add_argument("--full", type=int, default=4900, help="baseline amount for fork compare")
    parser.add_argument("--mode", choices=["run", "recover", "fork"], default="run")
    parser.add_argument("--crash", action="store_true")
    args = parser.parse_args()

    print("=== Actae refund agent ===")
    print(f"order #{args.order}  €{args.amount / 100:.2f}  mode={args.mode}")
    print()

    if args.mode == "fork":
        asyncio.run(fork(args))
    else:
        asyncio.run(run(args))


if __name__ == "__main__":
    main()
