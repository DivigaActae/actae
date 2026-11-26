#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Refund agent demo: a real crash inside the ambiguous window, recovery, fork.

Paced and narrated for screen recording. Every step prints what it is doing,
the kill is explicit, and the audit at the end is read back from the mock
payment provider.

    ./run --mode run --crash      # provider applies the refund, then SIGKILL
    ./run --mode recover          # reclaim, replay, no second charge
    ./run --mode fork --amount 1225 --full 4900
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
import urllib.request
from typing import Any, Dict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "sdks", "python", "src"))

from actae_client import ActaeClient  # noqa: E402


PROVIDER = "http://127.0.0.1:9099"
DIM, BOLD, GREEN, RED, YELLOW, CYAN, RESET = (
    "\033[2m", "\033[1m", "\033[32m", "\033[31m", "\033[33m", "\033[36m", "\033[0m"
)

START = time.time()
PACE = 1.6


def log(tag: str, message: str, color: str = "") -> None:
    elapsed = int(time.time() - START)
    stamp = f"{elapsed // 60:02d}:{elapsed % 60:02d}"
    print(f"{DIM}[{stamp}]{RESET} {color}{tag:<12}{RESET} {message}", flush=True)


def post(path: str, body: Dict[str, Any], idempotency_key: str) -> Dict[str, Any]:
    request = urllib.request.Request(
        f"{PROVIDER}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Idempotency-Key": idempotency_key},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read())


def stats(order: str) -> Dict[str, Any]:
    with urllib.request.urlopen(f"{PROVIDER}/stats?order={order}", timeout=5) as response:
        return json.loads(response.read())


async def claim_with_reclaim(client: ActaeClient, channel: str, key: str, tool: str, params: Dict[str, Any]):
    for _ in range(30):
        claim = await client.claim_execution(
            channel, key, tool, params, lease_seconds=1, actor="refund-agent"
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

    log("agent", f"tool call · payment.refund · €{amount_cents / 100:.2f}", CYAN)
    log("ledger", f"claim key={key} (lease 1s)", DIM)
    claim = await claim_with_reclaim(client, channel, key, "payment.refund", params)
    time.sleep(PACE)

    if claim.status == "replayed":
        log("ledger", "replayed · recorded result reused", GREEN)
        log("provider", "not called again — no second charge", GREEN)
        return claim.result or {}

    log("ledger", f"claim {claim.status} · fencing token issued", GREEN)
    if claim.status == "reclaimed":
        log("ledger", "previous worker died mid-call; ownership moved", YELLOW)

    log("provider", f"POST /refund key={key}", DIM)
    record = post(
        "/refund",
        {"order": order, "amount_cents": amount_cents, "currency": "EUR"},
        idempotency_key=key,
    )
    log("provider", f"refund {record.get('refund_id')} · €{record['amount_cents'] / 100:.2f} applied", YELLOW)
    time.sleep(PACE)

    if crash:
        log("worker", f"kill -9 {os.getpid()}", RED)
        print(f"{RED}{BOLD}         the effect landed; the result was never recorded{RESET}", flush=True)
        time.sleep(1.2)
        os.kill(os.getpid(), signal.SIGKILL)
        raise SystemExit(137)

    await client.complete_execution(claim.execution.id, claim.claim_token, result=record)
    log("ledger", "completed · result recorded", GREEN)
    time.sleep(PACE)
    return record


async def email_step(client: ActaeClient, channel: str, order: str) -> None:
    key = f"email:{order}"
    params = {"order": order, "to": "customer@example.com"}
    log("agent", "tool call · email.send · confirmation", CYAN)
    claim = await claim_with_reclaim(client, channel, key, "email.send", params)
    if claim.status == "replayed":
        log("ledger", "email inherited from the parent · replayed, not resent", GREEN)
        time.sleep(PACE)
        return
    record = post("/email", params, idempotency_key=key)
    await client.complete_execution(claim.execution.id, claim.claim_token, result=record)
    log("provider", f"confirmation {record.get('message_id')} sent", YELLOW)
    time.sleep(PACE)


async def run(args: argparse.Namespace) -> None:
    channel = f"refund-agent:{args.order}"
    async with ActaeClient(api_key=args.api_key or None, endpoint=args.endpoint) as client:
        log("order", f"#{args.order} · €{args.amount / 100:.2f} · customer@example.com")
        time.sleep(PACE)
        log("policy", "refund allowed (delivered 6 days ago, under €200)", DIM)
        time.sleep(PACE)
        log("agent", "plan: refund → notify → close", CYAN)

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
        log("actae", f"state saved at cursor {read_event.cursor}", GREEN)
        time.sleep(PACE)

        refund = await refund_step(client, channel, args.order, args.amount, crash=args.crash)
        await email_step(client, channel, args.order)
        await client.record(
            channel,
            "order.completed",
            {"order": args.order, "refund_id": refund.get("refund_id")},
            actor="refund-agent",
        )

        totals = stats(args.order)
        print()
        log("audit", f"charges applied for #{args.order}: {totals['orders_charged']}", GREEN)
        log("audit", f"provider deduplicated retries: {totals['deduplicated_retries']}", GREEN)
        log("audit", f"emails sent: {len(totals['emails'])}", GREEN)
        log("audit", f"channel {channel} · dashboard: {args.endpoint}/dashboard/flow/?channel={channel}", DIM)


async def fork(args: argparse.Namespace) -> None:
    source = f"refund-agent:{args.order}"
    target = f"refund-agent:{args.order}:partial"
    async with ActaeClient(api_key=args.api_key or None, endpoint=args.endpoint) as client:
        log("agent", "reconsider the decision: partial refund instead of full", CYAN)
        time.sleep(PACE)
        events = await client.replay(source)
        boundary = next(
            (e.cursor for e in events if getattr(e, "event_type", "") == "order.read"), 0
        )
        receipt = await client.fork(
            source,
            target,
            at_cursor=boundary,
            display_name="partial refund",
            reason="customer asked for a partial refund",
        )
        log("fork", f"{source} → {target}", YELLOW)
        log("fork", f"cursor {receipt.resolved_cursor} · state v{receipt.source_state_version} · restorable={receipt.restorable}", DIM)
        time.sleep(PACE)

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

        print()
        log("compare", f"full    €{args.full / 100:.2f} · {source}", DIM)
        log("compare", f"partial €{args.amount / 100:.2f} · {target}", DIM)
        totals = stats(args.order)
        log("audit", f"total sandbox charges: {totals['orders_charged']}", GREEN)
        log("audit", f"dashboard: {args.endpoint}/dashboard/flow/?channel={target}", DIM)


def main() -> None:
    global PACE
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="http://127.0.0.1:8022")
    parser.add_argument("--api-key", default=os.environ.get("ACTAE_API_KEY", ""))
    parser.add_argument("--order", default="4471")
    parser.add_argument("--amount", type=int, default=4900)
    parser.add_argument("--full", type=int, default=4900)
    parser.add_argument("--mode", choices=["run", "recover", "fork"], default="run")
    parser.add_argument("--crash", action="store_true")
    parser.add_argument("--pace", type=float, default=1.6, help="seconds between steps")
    args = parser.parse_args()
    PACE = args.pace

    print(f"{BOLD}=== Actae refund agent ==={RESET}")
    print(f"order #{args.order} · €{args.amount / 100:.2f} · mode={args.mode}\n")
    if args.mode == "fork":
        asyncio.run(fork(args))
    else:
        asyncio.run(run(args))


if __name__ == "__main__":
    main()
