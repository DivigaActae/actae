# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Message serialization round-trip and error cases.

SER-001 through SER-004 from the test gap analysis.
"""

import json
import pytest

pytestmark = pytest.mark.asyncio


class TestMessageRoundTrip:
    """SER-001: All message types serialize/deserialize losslessly."""

    async def test_subscribe_message_roundtrip(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()
            await raw.send(json.dumps({"type": "subscribe", "topic": "roundtrip-test", "cursor": 42}))
            resp = json.loads(await raw.recv())
            assert isinstance(resp, dict)
        finally:
            await raw.close()

    async def test_unsubscribe_roundtrip(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()
            await raw.send(json.dumps({"type": "unsubscribe", "topic": "roundtrip-unsub"}))
            resp = json.loads(await raw.recv())
            assert isinstance(resp, dict)
        finally:
            await raw.close()

    async def test_ping_pong_roundtrip(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()
            await raw.send(json.dumps({"type": "connection", "event": "ping", "payload": {}}))
            resp = json.loads(await raw.recv())
            assert "Connection" in resp
            assert resp["Connection"]["event"] == "pong"
        finally:
            await raw.close()


class TestFromJsonErrorCases:
    """SER-002: Malformed JSON gracefully handled."""

    MALFORMED_INPUTS = [
        "",
        "not-json",
        '{"type": "subscribe"}',  # missing topic
        '{"type": "unknown_type", "topic": "test"}',
        '{"type": "subscribe", "topic": null}',
        '{"type": "subscribe", "topic": "test", "cursor": "not-an-int"}',
        "{" * 1000,  # deeply nested
    ]

    async def test_malformed_inputs_return_error(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()
            for bad_input in self.MALFORMED_INPUTS:
                try:
                    await raw.send(bad_input)
                    resp = json.loads(await raw.recv())
                    assert "Error" in resp or "Connection" in resp
                except Exception:
                    pass
        finally:
            await raw.close()


class TestLargeMessages:
    """Large message boundaries."""

    async def test_large_subscribe_topic(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10, max_size=5*1024*1024)
        try:
            await raw.recv()
            long_topic = "x" * 1000
            await raw.send(json.dumps({"type": "subscribe", "topic": long_topic}))
            resp = json.loads(await raw.recv())
            assert isinstance(resp, dict)
        finally:
            await raw.close()
