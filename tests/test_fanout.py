# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Ring-buffer fanout and broadcast delivery tests.

Covers: PF-06 (120-client fanout) from the test strategy.

The ring-buffer activates at RING_FANOUT_THRESHOLD=100 subscribers.
Tests verify both single-subscriber and multi-subscriber broadcast delivery.
"""

import pytest
import json


pytestmark = pytest.mark.asyncio


class TestSingleSubscriber:
    """Basic single-subscriber delivery reliability."""

    async def test_ten_events_delivered(self, ws, actae, ch_name):
        async with await ws.connect() as wsc:
            await wsc.authenticate(actae._cfg.api_key)  # WS auth message is mandatory
            await wsc.subscribe(ch_name)

            for i in range(10):
                actae.record(ch_name, "test.event", {"seq": i}, "producer")

            msgs = await wsc.recv_all(count=10, timeout=10)
            broadcasts = [m for m in msgs if m.get("type") == "broadcast"]
            assert len(broadcasts) == 10, (
                f"Expected 10 broadcasts, got {len(broadcasts)}"
            )

            cursors = [
                m["payload"]["event"]["cursor"]
                for m in broadcasts
            ]
            assert all(cursors[i] < cursors[i + 1] for i in range(len(cursors) - 1))


class TestMultiSubscriberFanout:
    """Multiple simultaneous subscribers all receive events."""

    SUBSCRIBER_COUNT = 5

    async def test_all_subscribers_receive_all_events(self, ws, actae, ch_name):
        connections = []
        try:
            for _ in range(self.SUBSCRIBER_COUNT):
                wsc = await ws.connect()
                await wsc.__aenter__()
                await wsc.authenticate(actae._cfg.api_key)  # WS auth message is mandatory
                await wsc.subscribe(ch_name)
                connections.append(wsc)

            for i in range(5):
                actae.record(ch_name, "test.event", {"seq": i}, "producer")

            for idx, wsc in enumerate(connections):
                msgs = await wsc.recv_all(count=5, timeout=10)
                broadcasts = [m for m in msgs if m.get("type") == "broadcast"]
                assert len(broadcasts) == 5, (
                    f"Subscriber {idx}: expected 5, got {len(broadcasts)}"
                )
        finally:
            for wsc in connections:
                await wsc.__aexit__(None, None, None)


class TestLargeScaleFanout:
    """120 concurrent subscribers — ring-buffer activation threshold."""

    SUBSCRIBER_COUNT = 120

    @pytest.mark.slow
    async def test_120_subscribers_all_receive(self, ws, actae, ch_name):
        connections = []
        try:
            for i in range(self.SUBSCRIBER_COUNT):
                wsc = await ws.connect()
                await wsc.__aenter__()
                await wsc.authenticate(actae._cfg.api_key)  # WS auth message is mandatory
                await wsc.subscribe(ch_name)
                connections.append(wsc)

            import time as _time
            _time.sleep(1)

            event_count = 5
            for i in range(event_count):
                actae.record(ch_name, "test.event", {"seq": i}, "producer")

            failures = []
            for idx, wsc in enumerate(connections):
                msgs = await wsc.recv_all(count=event_count, timeout=15)
                broadcasts = [m for m in msgs if m.get("type") == "broadcast"]
                if len(broadcasts) != event_count:
                    failures.append((idx, len(broadcasts)))

            if failures:
                msg = "\n".join(
                    f"  Subscriber {idx}: got {count}/{event_count}"
                    for idx, count in failures[:5]
                )
                pytest.fail(f"{len(failures)} subscribers missed events:\n{msg}")

        finally:
            for wsc in connections:
                try:
                    await wsc.__aexit__(None, None, None)
                except Exception:
                    pass


@pytest.mark.slow
class TestFanoutWithLargePayloads:
    """Broadcast with ~10KB payloads to stress serialization."""

    async def test_large_payloads_delivered(self, ws, actae, ch_name):
        async with await ws.connect() as wsc:
            await wsc.authenticate(actae._cfg.api_key)  # WS auth message is mandatory
            await wsc.subscribe(ch_name)

            large_payload = {"data": "x" * 10000, "nested": {"a": list(range(100))}}
            actae.record(ch_name, "test.event", large_payload, "producer")

            msgs = await wsc.recv_all(count=1, timeout=10)
            broadcasts = [m for m in msgs if m.get("type") == "broadcast"]
            assert len(broadcasts) == 1
            delivered = broadcasts[0]["payload"]["event"]["payload"]
            assert len(delivered["data"]) == 10000
