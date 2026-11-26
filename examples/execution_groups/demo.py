#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""End-to-end Execution Groups + Group Fork demonstration.

Run against a real Actae instance (``cargo run`` by default)::

    pip install -e sdks/python
    python examples/execution_groups/demo.py

The demo creates two agents, claims fenced leases, sends a durable causal
message, observes it over the WebSocket convenience API, inspects its causal
graph, forks at the delivery boundary, proves frozen writes are blocked, then
promotes the frozen agent to reactive and records a live divergence.
"""

import argparse
import asyncio
import os
import time
import uuid

from actae_client import ActaeClient, CounterfactualBlockedError


def line(title: str) -> None:
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}", flush=True)


async def main(args: argparse.Namespace) -> None:
    url = args.url or os.environ.get("ACTAE_URL", "http://localhost:8002")
    key = args.api_key or os.environ.get("ACTAE_API_KEY", "sk-dev-0000000000000000000000")
    ws_url = args.ws_url or os.environ.get("ACTAE_WS_URL")
    suffix = f"{int(time.time())}-{uuid.uuid4().hex[:6]}"
    group_id = f"demo-group-{suffix}"
    router_channel = f"demo-{suffix}-router"
    research_channel = f"demo-{suffix}-research"
    fork_id = f"demo-fork-{suffix}"

    async with ActaeClient(api_key=key, endpoint=url, ws_endpoint=ws_url or "", timeout=10) as client:
        line("1. Create an execution group and fenced agent leases")
        await client.create_execution_group(group_id, metadata={"demo": True, "purpose": "support triage"})
        await client.add_execution_group_member(group_id, "router", router_channel, role="router")
        await client.add_execution_group_member(group_id, "research", research_channel, role="research")
        router = client.execution_group(group_id).member("router")
        research = client.execution_group(group_id).member("research")
        router_lease = await router.claim("demo-router", lease_seconds=60)
        research_lease = await research.claim("demo-research", lease_seconds=60)
        print(f"router generation={router_lease.generation}; research generation={research_lease.generation}")

        line("2. Send a durable causal message and acknowledge it")
        message = await router.send(
            "research", "ticket.inspect", {"ticket_id": 42, "priority": "high"},
            causal_context={"request_id": "req-42", "parents": []},
            operation_id=str(uuid.uuid4()),
        )
        print(f"message={message.message_id} delivery_event={message.delivery_event_id}")
        received = await research.wait_for("ticket.inspect", timeout=5)
        await research.acknowledge(received)
        print(f"durable wait received={received.message_id}; status=acknowledged")

        line("3. Inspect the persisted causal DAG")
        graph = await client.causal_graph(message.delivery_event_id)
        print(f"events={len(graph.get('events', []))} dependencies={len(graph.get('dependencies', []))}")

        line("4. Observe a new delivery through the group WebSocket API")
        live = ActaeClient(api_key=key, endpoint=url, ws_endpoint=ws_url or "", timeout=10)
        await live.connect()
        live_research = live.execution_group(group_id).member("research")
        prior = await client.replay(research_channel, limit=10000)
        cursor = max((event.cursor for event in prior), default=0)
        stream = live_research.stream_ws(cursor=cursor)
        next_delivery = asyncio.create_task(stream.__anext__())
        await asyncio.sleep(0.1)  # allow the subscribe-with-cursor handshake to complete
        ws_message = await router.send("research", "ticket.answer", {"ticket_id": 42, "answer": "ready"})
        streamed = await asyncio.wait_for(next_delivery, timeout=5)
        print(f"streamed message={streamed.message_id} (sent={ws_message.message_id})")
        await stream.aclose()
        await live.disconnect()

        line("5. Fork at the delivery boundary")
        router_history = await client.replay(router_channel, limit=10000)
        boundary_event = next(event for event in router_history if event.event_type == "message.sent")
        fork = await client.fork_execution_group(
            fork_id, group_id, "router", boundary_event.cursor,
            member_policies={"router": "live", "research": "frozen"},
            tool_policies={"ticket.lookup": "replay", "ticket.answer": "block"},
        )
        print(f"fork={fork['fork_group_id']} status={fork['status']} receipt={fork['receipt']}")

        line("6. Prove frozen writes are blocked, then promote the agent")
        fork_router = client.execution_group(fork_id).member("router")
        try:
            await fork_router.send("research", "ticket.answer", {"ticket_id": 42, "answer": "counterfactual"})
        except CounterfactualBlockedError as exc:
            print(f"blocked as expected: {exc}")
        promoted = await client.promote_execution_group_fork_member(fork_id, "research")
        print(f"promoted policy={promoted['member_policies']['research']}")
        live_message = await fork_router.send("research", "ticket.answer", {"ticket_id": 42, "answer": "live"})
        receipt = await client.get_execution_group_fork(fork_id)
        print(f"live divergence message={live_message.message_id}; receipt={receipt['receipt']}")

        await router.release()
        await research.release()
        line("Demo complete")
        print("The durable message, causal graph, frozen boundary, promotion, and live divergence are all persisted in Actae.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", help="Actae HTTP URL (default: ACTAE_URL or http://localhost:8002)")
    parser.add_argument("--ws-url", help="Actae WebSocket URL (default: ACTAE_WS_URL or derived URL)")
    parser.add_argument("--api-key", help="API key (default: ACTAE_API_KEY or dev key)")
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
