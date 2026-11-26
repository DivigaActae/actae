# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""
AI Support Agent Simulator.

Generates realistic customer support conversation traces and records
every agent action as Actae events. Can run in three modes:

  single   - One conversation, detailed output
  stream   - Continuous random conversations  
  load     - N concurrent conversations with configurable think time

Usage:
  python simulator.py single          # Run one conversation
  python simulator.py stream          # Continuous random conversations
  python simulator.py load --concurrent=20 --rate=5
"""

import argparse
import asyncio
import json
import random
import sys
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

import httpx


# ---------------------------------------------------------------------------
# Scenario definitions — realistic customer issues
# ---------------------------------------------------------------------------

SCENARIOS = [
    {
        "name": "order_status",
        "summary": "Customer asks about delayed order",
        "steps": [
            ("agent.started", {"agent": "support-v1", "user_id": "cust_001"}),
            ("tool.search_orders", {"tool": "orders_api", "query": "order-12345"}),
            ("tool.search_orders.completed", {"orders_found": 1, "status": "shipped"}),
            ("tool.check_carrier", {"carrier": "FedEx", "tracking": "FX123456789"}),
            ("tool.check_carrier.completed", {"estimated_delivery": "2026-06-27"}),
            ("message.sent", {"text": "Your order #12345 is on its way. FedEx tracking FX123456789, estimated delivery June 27th."}),
            ("agent.completed", {"resolution": "informed", "satisfaction": "positive"}),
        ],
    },
    {
        "name": "refund_request",
        "summary": "Customer wants a refund for defective item",
        "steps": [
            ("agent.started", {"agent": "support-v1", "user_id": "cust_042"}),
            ("tool.search_orders", {"tool": "orders_api", "query": "order-67890"}),
            ("tool.search_orders.completed", {"orders_found": 1, "status": "delivered"}),
            ("tool.check_return_policy", {"item_category": "electronics", "days_since_delivery": 14}),
            ("tool.check_return_policy.completed", {"eligible": True, "window_remaining": 16}),
            ("human.approval.requested", {"reason": "refund > $200", "refund_amount": 249.99}),
            ("human.approval.granted", {"approved_by": "supervisor_bob", "note": "Customer has history"}),
            ("tool.process_refund", {"amount": 249.99, "method": "original_payment"}),
            ("tool.process_refund.completed", {"refund_id": "RF-20260625-001", "status": "processed"}),
            ("message.sent", {"text": "Your refund of $249.99 for order #67890 has been processed. It will appear in 3-5 business days."}),
            ("agent.completed", {"resolution": "refunded", "satisfaction": "positive"}),
        ],
    },
    {
        "name": "account_access",
        "summary": "Customer locked out of account",
        "steps": [
            ("agent.started", {"agent": "support-v1", "user_id": "cust_089"}),
            ("tool.check_account_status", {"account": "cust_089"}),
            ("tool.check_account_status.completed", {"status": "locked", "reason": "failed_login", "attempts": 5}),
            ("tool.verify_identity", {"methods": ["email_otp", "security_question"]}),
            ("tool.verify_identity.completed", {"verified": True, "method": "email_otp"}),
            ("tool.unlock_account", {"account": "cust_089"}),
            ("tool.unlock_account.completed", {"status": "unlocked", "timestamp": "2026-06-25T10:15:00Z"}),
            ("message.sent", {"text": "Your account has been unlocked. I recommend enabling 2FA for extra security."}),
            ("agent.completed", {"resolution": "unlocked", "satisfaction": "positive"}),
        ],
    },
    {
        "name": "billing_dispute",
        "summary": "Customer disputes a charge",
        "steps": [
            ("agent.started", {"agent": "support-v2", "user_id": "cust_156"}),
            ("tool.search_invoices", {"customer": "cust_156", "period": "2026-06"}),
            ("tool.search_invoices.completed", {"invoices_found": 3, "total": 89.97}),
            ("tool.get_invoice_detail", {"invoice": "INV-202606-003"}),
            ("tool.get_invoice_detail.completed", {"items": [{"name": "Premium Plan", "amount": 49.99, "billing_period": "2026-06-01 to 2026-06-30"}]}),
            ("tool.check_discount_eligibility", {"customer_tier": "gold", "tenure_months": 18}),
            ("tool.check_discount_eligibility.completed", {"eligible": True, "discount": "15%", "reason": "loyalty"}),
            ("human.approval.requested", {"reason": "goodwill_discount", "discount": "15%"}),
            ("human.approval.granted", {"approved_by": "supervisor_alice", "note": "Loyal customer since 2025"}),
            ("tool.apply_discount", {"invoice": "INV-202606-003", "discount": "15%"}),
            ("tool.apply_discount.completed", {"adjusted_amount": 42.49, "savings": 7.50}),
            ("message.sent", {"text": "I've applied a 15% loyalty discount to your Premium Plan. Your next invoice will be $42.49 instead of $49.99."}),
            ("agent.completed", {"resolution": "discount_applied", "satisfaction": "positive"}),
        ],
    },
    {
        "name": "technical_issue",
        "summary": "Customer reports integration failure",
        "steps": [
            ("agent.started", {"agent": "support-v2", "user_id": "cust_203"}),
            ("tool.check_service_health", {"service": "api-gateway"}),
            ("tool.check_service_health.completed", {"status": "degraded", "incident": "INC-20260625-002"}),
            ("tool.check_integration_logs", {"integration": "salesforce", "time_range": "last_1h"}),
            ("tool.check_integration_logs.completed", {"errors": 47, "error_type": "rate_limit", "last_error": "2026-06-25T10:05:00Z"}),
            ("tool.escalate_to_engineering", {"severity": "high", "team": "integrations"}),
            ("tool.escalate_to_engineering.completed", {"ticket": "TKT-20260625-891", "priority": "P1"}),
            ("message.sent", {"text": "We're experiencing a degraded service on our API gateway affecting Salesforce integration. Engineering has been notified (P1 ticket TKT-20260625-891). I'll follow up as soon as it's resolved."}),
            ("checkpoint.created", {"type": "followup", "scheduled": "2026-06-25T11:00:00Z"}),
            ("agent.completed", {"resolution": "escalated", "satisfaction": "neutral"}),
        ],
    },
    {
        "name": "multi_tool_workflow",
        "summary": "Complex workflow across 5 tools",
        "steps": [
            ("agent.started", {"agent": "support-v3", "user_id": "cust_312"}),
            ("tool.search_customer", {"query": "cust_312"}),
            ("tool.search_customer.completed", {"tier": "enterprise", "account_manager": "sarah_j"}),
            ("tool.check_active_subscriptions", {"customer": "cust_312"}),
            ("tool.check_active_subscriptions.completed", {"subscriptions": ["pro-annual", "addon-storage"], "monthly_total": 299.00}),
            ("tool.search_ticket_history", {"customer": "cust_312", "limit": 5}),
            ("tool.search_ticket_history.completed", {"open_tickets": 2, "recent": ["TKT-202606-100", "TKT-202606-101"]}),
            ("tool.get_ticket_detail", {"ticket": "TKT-202606-101"}),
            ("tool.get_ticket_detail.completed", {"status": "waiting_on_customer", "age_days": 3}),
            ("tool.get_conversation_context", {"ticket": "TKT-202606-101", "include_notes": True}),
            ("tool.get_conversation_context.completed", {"thread_length": 12, "last_message_from": "customer", "pending_action": "customer_reply"}),
            ("message.sent", {"text": "I see you have an open ticket (TKT-202606-101) where our team asked for additional logs. Could you provide those? Also, I noticed your account manager Sarah J. is copied on that thread."}),
            ("agent.completed", {"resolution": "followup_requested", "satisfaction": "neutral"}),
        ],
    },
]


# ---------------------------------------------------------------------------
# Event recording
# ---------------------------------------------------------------------------

class ActaeEventClient:
    """Records events to Actae via HTTP API."""

    def __init__(self, api_url: str, api_key: str = ""):
        self.api_url = api_url.rstrip("/")
        self.headers = {"Content-Type": "application/json"}
        if api_key:
            self.headers["X-API-Key"] = api_key
        self._client: httpx.AsyncClient | None = None
        self._verify = api_url.startswith("https")

    async def __aenter__(self):
        self._client = httpx.AsyncClient(verify=False)
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    async def record(self, channel_id: str, event_type: str, payload: dict,
                     actor: str = "simulator") -> dict | None:
        body = {
            "channel_id": channel_id,
            "type": event_type,
            "payload": payload,
            "metadata": {"actor": actor},
        }

        url = f"{self.api_url}/api/v1/events/record"
        for attempt in range(3):
            try:
                resp = await self._client.post(url, json=body, headers=self.headers, timeout=10.0)
                if resp.status_code == 201:
                    return resp.json().get("event")
                if resp.status_code in (502, 503):
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                print(f"  [!] record failed: {resp.status_code} {resp.text[:200]}")
                return None
            except (httpx.ConnectError, httpx.TimeoutException) as e:
                if attempt < 2:
                    await asyncio.sleep(1.0)
                    continue
                print(f"  [!] connection failed: {e}")
                return None
        return None

    async def replay(self, channel_id: str, cursor: int = 0, limit: int = 100) -> list:
        url = f"{self.api_url}/api/v1/events/replay/{channel_id}?cursor={cursor}&limit={limit}"
        try:
            resp = await self._client.get(url, headers=self.headers, timeout=10.0)
            if resp.status_code == 200:
                return resp.json().get("events", [])
        except Exception:
            pass
        return []

    async def latest_cursor(self, channel_id: str) -> int | None:
        url = f"{self.api_url}/api/v1/events/cursor/{channel_id}"
        try:
            resp = await self._client.get(url, headers=self.headers, timeout=10.0)
            if resp.status_code == 200:
                return resp.json().get("latest_cursor")
        except Exception:
            pass
        return None


# ---------------------------------------------------------------------------
# Conversation simulator
# ---------------------------------------------------------------------------

@dataclass
class Conversation:
    channel_id: str
    scenario: dict
    events_recorded: int = 0
    started_at: float = field(default_factory=time.time)

    @property
    def duration(self) -> float:
        return time.time() - self.started_at

    @property
    def summary(self) -> str:
        return f"{self.channel_id[:8]} | {self.scenario['name']:20s} | {self.events_recorded:2d} events | {self.duration:.1f}s"


async def run_conversation(client: ActaeEventClient, scenario: dict,
                           think_time: tuple[float, float] = (0.05, 0.3),
                           channel_id: str | None = None,
                           verbose: bool = False) -> Conversation:
    """Execute a single support conversation, recording events to Actae."""
    conv = Conversation(
        channel_id=channel_id or f"conv-{uuid.uuid4().hex[:12]}",
        scenario=scenario,
    )

    if verbose:
        print(f"\n{'='*60}")
        print(f"Conversation: {conv.channel_id}")
        print(f"Scenario:     {scenario['name']}")
        print(f"Summary:      {scenario['summary']}")
        print(f"{'='*60}")

    for step_idx, (event_type, payload) in enumerate(scenario["steps"]):
        think = random.uniform(*think_time)
        if verbose and step_idx > 0:
            await asyncio.sleep(think)

        recorded = await client.record(
            channel_id=conv.channel_id,
            event_type=event_type,
            payload=payload,
            actor=payload.get("agent", "simulator"),
        )

        if recorded:
            conv.events_recorded += 1

        if verbose:
            cursor = recorded["cursor"] if recorded else "?"
            marker = "" if recorded else ""
            print(f"  [{cursor:>4}] {marker} {event_type:35s} {json.dumps(payload, default=str)[:80]}")

    if verbose:
        print(f"\n  Done. {conv.events_recorded}/{len(scenario['steps'])} events recorded in {conv.duration:.1f}s")

    return conv


# ---------------------------------------------------------------------------
# Mode: single — one detailed conversation
# ---------------------------------------------------------------------------

async def mode_single(args):
    scenario = random.choice(SCENARIOS)
    async with ActaeEventClient(args.api_url) as client:
        conv = await run_conversation(client, scenario, think_time=(0.1, 0.5), verbose=True)

        print(f"\nReplaying conversation {conv.channel_id}...")
        events = await client.replay(conv.channel_id)
        print(f"  Replayed {len(events)} events")

        cursor = await client.latest_cursor(conv.channel_id)
        print(f"  Latest cursor: {cursor}")
    return 0


# ---------------------------------------------------------------------------
# Mode: stream — continuous random conversations
# ---------------------------------------------------------------------------

async def mode_stream(args):
    tick_names = ["", "", "", "", "", "", "", ""]
    tick_idx = 0
    total_events = 0
    total_convos = 0
    start = time.time()

    async with ActaeEventClient(args.api_url) as client:
        while True:
            scenario = random.choice(SCENARIOS)
            conv = await run_conversation(client, scenario, think_time=(0.02, 0.15), verbose=False)
            total_convos += 1
            total_events += conv.events_recorded
            elapsed = time.time() - start
            rate = total_events / elapsed if elapsed > 0 else 0

            tick = tick_names[tick_idx % len(tick_names)]
            tick_idx += 1
            print(f"\r  {tick} {total_convos:4d} conversations | {total_events:5d} events | {rate:.1f} evt/s   ", end="", flush=True)
            await asyncio.sleep(random.uniform(0.1, 0.5))
    return 0


# ---------------------------------------------------------------------------
# Mode: load — N concurrent conversations
# ---------------------------------------------------------------------------

async def mode_load(args):
    import math

    concurrent = args.concurrent
    rate = args.rate
    duration = args.duration
    think = (args.min_think, args.max_think)

    print(f"Load test: {concurrent} concurrent, {rate}/s rate, {duration}s duration")
    print(f"Think time: {think[0]:.2f}s - {think[1]:.2f}s")
    print()

    sem = asyncio.Semaphore(concurrent)
    total_events = 0
    total_convos = 0
    start = time.time()
    batch = 0

    async with ActaeEventClient(args.api_url) as client:

        async def throttled_worker(scenario):
            async with sem:
                conv = await run_conversation(client, scenario, think_time=think, verbose=False)
                return conv

        pending = []
        while time.time() - start < duration:
            elapsed = time.time() - start
            remaining = duration - elapsed
            if remaining <= 0:
                break

            # Dispatch a batch
            batch_size = min(rate, concurrent - len(pending))
            scenarios = [random.choice(SCENARIOS) for _ in range(batch_size)]
            for s in scenarios:
                task = asyncio.create_task(throttled_worker(s))
                pending.append(task)

            # Resolve completed
            done, pending = await asyncio.wait(pending, timeout=1.0, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                conv = task.result()
                total_convos += 1
                total_events += conv.events_recorded

            batch += 1
            if batch % 5 == 0:
                elapsed_running = time.time() - start
                rate_actual = total_events / elapsed_running if elapsed_running > 0 else 0
                active = len(pending)
                print(f"  convos={total_convos:5d}  events={total_events:6d}  rate={rate_actual:5.1f}/s  active_workers={active:3d}")

        # Drain remaining
        if pending:
            done, _ = await asyncio.wait(pending, timeout=30.0)
            for task in done:
                conv = task.result()
                total_convos += 1
                total_events += conv.events_recorded

    elapsed = time.time() - start
    print(f"\nResults:")
    print(f"  Conversations: {total_convos}")
    print(f"  Events recorded: {total_events}")
    print(f"  Duration: {elapsed:.1f}s")
    print(f"  Avg rate: {total_events/elapsed:.1f} events/s")
    print(f"  Avg events/conversation: {total_events/max(total_convos,1):.1f}")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="AI Support Agent Simulator")
    parser.add_argument("--api-url", default="http://localhost:8002",
                        help="Actae API URL (default: http://localhost:8002)")
    parser.add_argument("--api-key", default="", help="Actae API key")
    sub = parser.add_subparsers(dest="mode", required=True)

    # single
    p_single = sub.add_parser("single", help="Run one conversation with detailed output")

    # stream
    sub.add_parser("stream", help="Continuous random conversations")

    # load
    p_load = sub.add_parser("load", help="Concurrent load test")
    p_load.add_argument("--concurrent", type=int, default=20, help="Max concurrent conversations")
    p_load.add_argument("--rate", type=int, default=10, help="Target conversations per second")
    p_load.add_argument("--duration", type=int, default=60, help="Test duration (seconds)")
    p_load.add_argument("--min-think", type=float, default=0.02, help="Min think time between steps")
    p_load.add_argument("--max-think", type=float, default=0.1, help="Max think time between steps")

    args = parser.parse_args()
    args.api_url = args.api_url or "http://localhost:8002"

    if args.mode == "single":
        exit_code = asyncio.run(mode_single(args))
    elif args.mode == "stream":
        exit_code = asyncio.run(mode_stream(args))
    elif args.mode == "load":
        exit_code = asyncio.run(mode_load(args))
    else:
        parser.print_help()
        exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
