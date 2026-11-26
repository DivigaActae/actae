#!/usr/bin/env python3
# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Actae coordination demo — WebSocket watcher subprocess.

Connects to Actae over WebSocket, subscribes to a channel, and records every
broadcast it receives — in arrival order — to an output file. The orchestrator
compares these records across watchers and against the durable log to prove
multi-subscriber real-time fan-out: every subscriber sees the identical,
gapless, ordered stream, live.

Every emitted line is ``KEY<TAB>JSON``:

* ``READY`` — WebSocket connected and subscription acknowledged.
* ``EVENT`` — one broadcast ``{"cursor": N, "type": ...}``.
* ``DONE`` — reached ``--count`` events or ``--timeout`` seconds.

Environment:
    ACTAE_URL       HTTP base URL (default http://localhost:8002)
    ACTAE_WS_URL    WebSocket URL (default derived from ACTAE_URL)
    ACTAE_API_KEY   API key (default sk-dev-0000000000000000000000)

Usage:
    watcher.py <channel> [--count N] [--timeout S] [--out FILE]
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
DEFAULT_WS = os.environ.get("ACTAE_WS_URL", "")


def _emit(out, key, **data):
    line = key + "\t" + json.dumps(data, default=str)
    if out:
        with open(out, "a") as fh:
            fh.write(line + "\n")
    else:
        print(line, flush=True)


async def _main(args):
    if not DEFAULT_WS:
        ws = DEFAULT_URL.replace("https://", "wss://").replace("http://", "ws://") + "/ws"
    else:
        ws = DEFAULT_WS

    async with ActaeClient(api_key=DEFAULT_KEY, endpoint=DEFAULT_URL,
                           ws_endpoint=ws) as client:
        seen = []

        def on_message(topic, event):
            seen.append((event.cursor, event.event_type))
            _emit(args.out, "EVENT", cursor=event.cursor, type=event.event_type,
                  topic=topic)

        client.on_message(on_message)
        await client.connect()
        await client.subscribe(args.channel)
        _emit(args.out, "READY", channel=args.channel)

        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            if args.count and len(seen) >= args.count:
                break
            await asyncio.sleep(0.1)

        _emit(args.out, "DONE", count=len(seen))
        await client.disconnect()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("channel", help="channel to subscribe to")
    parser.add_argument("--count", type=int, default=0, help="exit after N events")
    parser.add_argument("--timeout", type=int, default=120, help="exit after S seconds")
    parser.add_argument("--out", default="", help="append KEY<TAB>JSON lines to this file")
    args = parser.parse_args()
    try:
        asyncio.run(_main(args))
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
