# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""
Actae Execution Debugger CLI.

Inspect, replay, and debug agent execution traces from the terminal.

Usage:
  python cli.py channels                       # List recent conversations
  python cli.py show <channel_id>              # Show full timeline
  python cli.py replay <channel_id>            # Step through events
  python cli.py search --type tool.completed   # Search events
  python cli.py inspect <channel_id> <cursor>  # Inspect specific event
  python cli.py watch <channel_id>             # Live tail events
"""

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime

import httpx

from config import ACTAE_API_URL


# ---------------------------------------------------------------------------
# Rich output helpers (no external deps)
# ---------------------------------------------------------------------------

def _style(text: str, color: str = "") -> str:
    colors = {
        "green": "\033[92m",
        "yellow": "\033[93m",
        "red": "\033[91m",
        "blue": "\033[94m",
        "purple": "\033[95m",
        "cyan": "\033[96m",
        "gray": "\033[90m",
        "bold": "\033[1m",
        "reset": "\033[0m",
    }
    if not color or not sys.stdout.isatty():
        return text
    return f"{colors.get(color, '')}{text}{colors['reset']}"


def _event_dot(event_type: str) -> str:
    if event_type.startswith("agent.started"):
        return _style("", "green")
    if event_type.startswith("agent.completed") or event_type.endswith(".completed"):
        return _style("", "blue")
    if event_type.startswith("tool."):
        return _style("", "yellow")
    if event_type.startswith("human."):
        return _style("", "purple")
    if event_type.startswith("message."):
        return _style("", "cyan")
    if event_type.startswith("checkpoint."):
        return _style("", "gray")
    if event_type.startswith("error"):
        return _style("", "red")
    return _style("·", "gray")


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

class Client:
    def __init__(self, api_url: str):
        self.api_url = api_url.rstrip("/")
        self._verify = api_url.startswith("https")

    async def _get(self, path: str) -> dict | list | None:
        async with httpx.AsyncClient(verify=False, timeout=15.0) as c:
            try:
                resp = await c.get(f"{self.api_url}{path}")
                if resp.status_code == 200:
                    return resp.json()
            except Exception:
                pass
        return None

    async def _post(self, path: str, body: dict) -> dict | None:
        async with httpx.AsyncClient(verify=False, timeout=15.0) as c:
            try:
                resp = await c.post(f"{self.api_url}{path}", json=body,
                                    headers={"Content-Type": "application/json"})
                if resp.status_code == 200:
                    return resp.json()
            except Exception:
                pass
        return None


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

async def cmd_channels(args):
    client = Client(args.api_url)
    data = await client._get("/api/v1/events/query")
    if not data:
        print("No channels found or Actae unreachable.")
        return

    # We'll query to find recent channel IDs
    query_result = await client._post("/api/v1/events/query", {"limit": 200})
    if not query_result:
        print("No events found.")
        return

    events = query_result.get("events", [])
    channels: dict[str, list] = {}
    for e in events:
        ch = e.get("channel_id", "?")
        if ch not in channels:
            channels[ch] = []
        channels[ch].append(e)

    print(f"\n  {_style('Recent Agent Conversations', 'bold')}")
    print(f"  {'' * 60}")
    for ch_id, evts in sorted(channels.items(), key=lambda x: max(e.get("cursor", 0) for e in x[1]), reverse=True)[:20]:
        types = set(e.get("type", "") for e in evts)
        has_completed = any("completed" in t for t in types)
        status = _style("", "green") if has_completed else _style("…", "yellow")
        start = next((e.get("timestamp", "") for e in evts if "started" in e.get("type", "")), "")
        cursor = max(e.get("cursor", 0) for e in evts)
        print(f"  {status} {_style(ch_id[:20], 'cyan'):20s}  {len(evts):3d} events  cursor={cursor}  {start[:19] if start else ''}")

    print(f"  {'' * 60}")
    print(f"  {len(channels)} channels, {sum(len(v) for v in channels.values())} total events\n")


async def cmd_show(args):
    client = Client(args.api_url)
    cursor = getattr(args, "cursor", 0)
    data = await client._get(f"/api/v1/events/replay/{args.channel_id}?cursor={cursor}&limit=200")
    if not data or not data.get("events"):
        print(f"No events found for channel: {args.channel_id}")
        return

    events = data["events"]
    print(f"\n  {_style('Execution Timeline', 'bold')}")
    print(f"  Channel: {_style(args.channel_id, 'cyan')}")
    print(f"  Events:  {len(events)}")
    print(f"  {'' * 70}")

    for e in events:
        dot = _event_dot(e.get("type", ""))
        cursor_str = _style(f"#{e.get('cursor', '?'):>4}", "gray")
        ts = e.get("timestamp", "")[11:19] if e.get("timestamp") else ""
        label = e.get("type", "").replace(".", " · ")
        payload = e.get("payload", {})
        summary = ""
        if isinstance(payload, dict):
            for key in ("resolution", "status", "tool", "text", "reason"):
                if key in payload:
                    val = str(payload[key])[:60]
                    summary = f"  {_style(f'({val})', 'gray')}"
                    break
            if not summary:
                vals = list(payload.values())
                if vals:
                    first = str(vals[0])[:60]
                    summary = f"  {_style(f'({first})', 'gray')}"

        print(f"  {dot} {cursor_str}  {_style(label, 'bold'):35s} {_style(ts, 'gray'):8s}{summary}")

    print(f"  {'' * 70}\n")


async def cmd_replay(args):
    """Interactive step-through replay."""
    client = Client(args.api_url)
    cursor = 0

    while True:
        data = await client._get(f"/api/v1/events/replay/{args.channel_id}?cursor={cursor}&limit=1")
        if not data or not data.get("events"):
            if cursor == 0:
                print(f"No events for channel: {args.channel_id}")
            else:
                print(f"\n{_style('End of execution.', 'gray')}")
            break

        e = data["events"][0]
        cursor = e.get("cursor", cursor) + 1
        dot = _event_dot(e.get("type", ""))
        label = e.get("type", "").replace(".", " · ")
        payload = json.dumps(e.get("payload", {}), indent=2)
        ts = e.get("timestamp", "")[11:19] if e.get("timestamp") else ""

        print(f"\n  {dot}  {_style(label, 'bold')}  {_style(ts, 'gray')}")
        print(f"  {_style('' * 50, 'gray')}")
        print(f"  {payload}")
        print()

        if not getattr(args, "auto", False):
            try:
                inp = input(f"  {_style('[Enter=next, q=quit]', 'gray')} ")
                if inp.lower() == "q":
                    break
            except (EOFError, KeyboardInterrupt):
                break


async def cmd_search(args):
    client = Client(args.api_url)
    body = {"limit": args.limit}
    if args.type:
        body["event_type"] = args.type
    if args.actor:
        body["actor"] = args.actor

    data = await client._post("/api/v1/events/query", body)
    if not data or not data.get("events"):
        print("No matching events found.")
        return

    events = data["events"]
    print(f"\n  {_style(f'Found {len(events)} matching events', 'bold')}")
    print(f"  {'' * 70}")

    for e in events:
        dot = _event_dot(e.get("type", ""))
        ch = e.get("channel_id", "")[:16]
        cursor = e.get("cursor", "?")
        label = e.get("type", "").replace(".", " · ")
        print(f"  {dot}  {_style(ch, 'cyan'):16s}  #{cursor:<4}  {_style(label, 'bold')}")


async def cmd_inspect(args):
    client = Client(args.api_url)
    data = await client._get(f"/api/v1/events/replay/{args.channel_id}?cursor={args.cursor}&limit=1")
    if not data or not data.get("events"):
        print(f"No event at cursor {args.cursor} for {args.channel_id}")
        return
    e = data["events"][0]
    print(json.dumps(e, indent=2, default=str))


async def cmd_watch(args):
    """Live tail of a channel."""
    client = Client(args.api_url)
    last_cursor = 0

    print(f"  Watching {args.channel_id}... (Ctrl+C to stop)\n")

    try:
        while True:
            data = await client._get(f"/api/v1/events/replay/{args.channel_id}?cursor={last_cursor}&limit=50")
            if data and data.get("events"):
                for e in data["events"]:
                    cursor = e.get("cursor", 0)
                    if cursor > last_cursor:
                        dot = _event_dot(e.get("type", ""))
                        ts = e.get("timestamp", "")[11:19] if e.get("timestamp") else ""
                        label = e.get("type", "").replace(".", " · ")
                        print(f"  {dot}  #{cursor:<4}  {_style(label, 'bold'):35s}  {ts}")
                        last_cursor = cursor
            await asyncio.sleep(0.5)
    except KeyboardInterrupt:
        print(f"\n  Stopped.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Actae Execution Debugger")
    parser.add_argument("--api-url", default=ACTAE_API_URL, help="Actae API URL")

    sub = parser.add_subparsers(dest="command", required=True)

    p_channels = sub.add_parser("channels", help="List recent conversations")

    p_show = sub.add_parser("show", help="Show execution timeline")
    p_show.add_argument("channel_id")
    p_show.add_argument("--cursor", type=int, default=0)

    p_replay = sub.add_parser("replay", help="Step through execution")
    p_replay.add_argument("channel_id")
    p_replay.add_argument("--auto", action="store_true", help="Auto-play")

    p_search = sub.add_parser("search", help="Search events")
    p_search.add_argument("--type", help="Filter by event type")
    p_search.add_argument("--actor", help="Filter by actor")
    p_search.add_argument("--limit", type=int, default=50)

    p_inspect = sub.add_parser("inspect", help="Inspect a specific event")
    p_inspect.add_argument("channel_id")
    p_inspect.add_argument("cursor", type=int)

    p_watch = sub.add_parser("watch", help="Live tail a channel")
    p_watch.add_argument("channel_id")

    args = parser.parse_args()

    command_map = {
        "channels": cmd_channels,
        "show": cmd_show,
        "replay": cmd_replay,
        "search": cmd_search,
        "inspect": cmd_inspect,
        "watch": cmd_watch,
    }

    fn = command_map.get(args.command)
    if fn:
        asyncio.run(fn(args))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
