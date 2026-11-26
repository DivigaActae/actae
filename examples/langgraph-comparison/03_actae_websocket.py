#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""
03 — Actae WebSocket Real-Time Event Streaming
====================================================

Demonstrates Actae's pub/sub architecture for real-time agent event
streaming — the backbone of reactive agent systems.

Key enterprise patterns demonstrated:
  
   Pattern                  How Actae implements it                
  
   Event-driven agents      WebSocket pub/sub with topic matching 
   Cursor-based replay      Exactly-once processing semantics     
   Dual-write (HTTP + WS)   record() for audit, publish() for    
                            real-time                             
   Subscribe with cursor    Resume from last processed event      
   Channel forking          Fork execution for A/B testing      
  

Compare:
  - HTTP record():  Creates an event with structured metadata.
                    Returns cursor. Good for audit trails.
  - WS publish():   Creates an event AND broadcasts to subscribers.
                    Same persistence + real-time delivery.
  - WS subscribe(): Receive events as they happen. No polling.

Usage:
    # Terminal 1: Actae running
    cd actae && cargo run -- --dev

    # Terminal 2: This script
    python 03_actae_websocket.py

    # Open dashboard: http://localhost:8002
"""

import asyncio
import json
import signal
import sys
import time
from typing import Any, Dict, List

from src.config import ConfigLoader
from src.logging_setup import get_logger, reset_correlation_id
from src.resilience import retry_async
from src.actae_integration import ActaeIntegration

logger = get_logger(__name__)


#  Global state (for signal handler) 
_shutdown_requested = False


def _handle_signal(signum: int, frame: Any) -> None:
    global _shutdown_requested
    _shutdown_requested = True
    logger.warning("Shutdown requested (signal %d)", signum)


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# 
# REAL-TIME EVENT STREAMING
# 
# Enterprise patterns:
#   - Topic isolation: Each agent/component publishes to its own topic.
#     No polling, no shared state, no race conditions.
#   - Cursor tracking: Every event has a globally unique, monotonic cursor.
#     Clients track "last processed cursor" for exactly-once semantics.
#   - Graceful degradation: If Actae is down, circuit breaker prevents
#     cascading failures in downstream components.

class EventCounter:
    """Tracks throughput metrics for production monitoring."""

    def __init__(self) -> None:
        self.received: List[Dict[str, Any]] = []
        self.published: int = 0
        self.start_time: float = time.time()

    def on_message(self, topic: str, event: Any) -> None:
        """Callback for incoming WS messages."""
        self.received.append({
            "topic": topic,
            "cursor": event.cursor,
            "event_type": event.event_type,
            "payload": event.payload,
            "timestamp": event.timestamp,
        })
        preview = json.dumps(event.payload)[:100] if event.payload else "(empty)"
        logger.info(
            "WS received [%s] cursor=%d: %s",
            topic.split(".")[-1], event.cursor, preview,
        )

    def on_subscribed(self, topic: str, cursor: int) -> None:
        logger.info("Subscribed to '%s' at cursor=%s", topic, cursor)

    def on_error(self, msg: str) -> None:
        logger.error("WS error: %s", msg)

    @property
    def rate_per_second(self) -> float:
        elapsed = time.time() - self.start_time
        return len(self.received) / elapsed if elapsed > 0 else 0

    @property
    def report(self) -> str:
        return (
            f"Received {len(self.received)} events "
            f"({self.rate_per_second:.1f}/s), "
            f"published {self.published}"
        )


# 
# DEMO SCENARIO
# 
# Simulates a 3-agent system where agents communicate via Actae events.
# Each agent subscribes to its input topic and publishes to its output
# topic — forming a reactive pipeline with zero coupling.

async def run_demo(actae: ActaeIntegration, topic: str, counter: EventCounter) -> None:
    """Execute the WebSocket demo scenario."""
    client = actae.client
    client.on_message(counter.on_message)
    client.on_subscribed(counter.on_subscribed)
    client.on_error(counter.on_error)

    #  1. Subscribe with cursor=0 (replay all past events) 
    # In production, you'd track the last processed cursor and
    # subscribe from that point for exactly-once semantics.
    logger.info("Subscribing to %s from cursor 0", topic)
    await client.subscribe(topic, cursor=0, wait=True)

    #  2. Publish events via WebSocket (real-time) 
    # WS publish() sends the event to ALL subscribers immediately.
    # The callback fires on the same connection (loopback).
    logger.info("Publishing 3 events via WebSocket...")
    for i in range(3):
        if _shutdown_requested:
            break
        ev = await client.publish(topic, {
            "agent": ["researcher", "analyst", "writer"][i],
            "step": i + 1,
            "status": "completed",
            "duration_ms": round((i + 1) * 150.0, 1),
        })
        counter.published += 1
        logger.info("Published [%d/3] cursor=%d", i + 1, ev.cursor)
        await asyncio.sleep(0.3)

    #  3. Record events via HTTP API 
    # HTTP record() creates events with richer metadata (actor, tags).
    # Actae also broadcasts these to WS subscribers automatically.
    logger.info("Recording 3 events via HTTP API...")
    for i in range(3):
        if _shutdown_requested:
            break
        await actae.record_event(
            channel_id=topic,
            event_type=f"demo.step.{i + 1}",
            payload={
                "agent": ["researcher", "analyst", "writer"][i],
                "phase": "http_record",
                "timestamp": time.time(),
            },
            actor=f"agent-{i + 1}",
            metadata={"demo": "websocket", "step": i + 1},
        )
        logger.info("HTTP record [%d/3]", i + 1)
        await asyncio.sleep(0.2)

    #  4. Wait for remaining callbacks 
    await asyncio.sleep(1)

    #  5. Replay all events 
    events = await actae.replay_events(topic, cursor=0, limit=20)
    logger.info("Replay: %d events in '%s'", len(events), topic)

    #  6. Show fork capability 
    cursor = await actae.get_latest_cursor(topic)
    logger.info(
        "Ready to fork at cursor=%d. "
        "Example: actae.fork('%s', 'my-fork', at_cursor=%d)",
        cursor, topic, cursor,
    )


# 
# MAIN
# 

async def main() -> None:
    """Run the WebSocket demo with graceful shutdown."""
    correlation_id = reset_correlation_id()
    config = ConfigLoader.load()

    topic = f"public.demo-{int(time.time())}"
    counter = EventCounter()

    logger.info(
        "WebSocket demo starting",
        extra={
            "correlation_id": correlation_id,
            "topic": topic,
            "actae_url": config.actae.http_url,
            "ws_url": config.actae.ws_endpoint,
            "shutdown_timeout": 5,
        },
    )

    #  Connect to Actae with retry 
    try:
        async with await ActaeIntegration.create(config.actae, correlation_id) as actae:
            await run_demo(actae, topic, counter)

            #  Final report 
            duration = time.time() - counter.start_time
            print(f"\n{'=' * 60}")
            print(f"  WEBSOCKET DEMO COMPLETE")
            print(f"{'=' * 60}")
            print(f"  Topic:      {topic}")
            print(f"  Duration:   {duration:.1f}s")
            print(f"  Published:  {counter.published} events via WS")
            print(f"  Received:   {len(counter.received)} via subscription")
            print(f"  Rate:       {counter.rate_per_second:.1f} events/s")
            print()

            if counter.received:
                print(f"  {'' * 40}")
                print(f"  Real-Time Events")
                print(f"  {'' * 40}")
                for ev in counter.received:
                    agent = (ev["payload"] or {}).get("agent", "?")
                    print(f"    [{ev['cursor']:>3}] topic={ev['topic'][:30]:<30} agent={agent}")
                print()

            print(f"  {'' * 40}")
            print(f"  Next Steps")
            print(f"  {'' * 40}")
            print(f"  Replay:     python3 -c \"from actae_client import ActaeClient...\"")
            print(f"  Dashboard:  {config.actae.http_url}")
            print(f"  Fork:       Fork this topic to experiment with A/B variants")
            print()
            print(f"{'=' * 60}")
            print(f"    Real-time event streaming via WebSocket complete.")
            print(f"    All events persisted — visible in the dashboard.")
            print(f"{'=' * 60}\n")

    except Exception as exc:
        logger.error("Demo failed: %s", exc)
        if counter.received:
            print(f"\nPartial results: {counter.report}")
        raise
    finally:
        logger.info("Shutdown complete. Correlation ID: %s", correlation_id)


if __name__ == "__main__":
    asyncio.run(main())
