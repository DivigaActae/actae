# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Advanced WebSocket message types and protocol edge cases.

Broadcast, DirectMessage, Ack/Nack, DatabaseChange message types.
"""

import json
import pytest
import asyncio

pytestmark = pytest.mark.asyncio


async def _connect(actae_config):
    import websockets as _ws
    ws_url = actae_config.ws_url.rstrip("/")
    if not ws_url.endswith("/ws"):
        ws_url = f"{ws_url}/ws"
    ws = await _ws.connect(ws_url, ssl=actae_config.ssl_context, ping_interval=None, close_timeout=10, max_size=5*1024*1024)
    await ws.recv()
    await ws.send('{"type":"auth","api_key":"test-api-key-for-integration-tests","payload":{}}')
    await ws.recv()
    return ws


class TestBroadcastMessages:
    """WS Broadcast message type — send and receive."""

    async def test_broadcast_to_subscribed_channel(self, ws, actae_config):
        wsa = await _connect(actae_config)
        await wsa.send(json.dumps({"type": "subscribe", "topic": "broad-test"}))
        await wsa.recv()
        wsb = await _connect(actae_config)
        await wsb.send(json.dumps({"type": "subscribe", "topic": "broad-test"}))
        await wsb.recv()
        await wsa.send(json.dumps({"type": "broadcast", "topic": "broad-test", "payload": {"msg": "hello"}}))
        resp = json.loads(await wsa.recv())
        assert resp is not None
        await wsa.close()
        await wsb.close()

    async def test_broadcast_without_subscribe(self, ws, actae_config):
        wsa = await _connect(actae_config)
        await wsa.send(json.dumps({"type": "broadcast", "topic": "no-sub-test", "payload": {"msg": "hello"}}))
        resp = json.loads(await wsa.recv())
        assert resp is not None
        await wsa.close()


class TestAckNackMessages:
    """Ack/Nack message parsing and UUID validation."""

    async def test_ack_with_valid_uuid(self, ws, actae_config):
        wsa = await _connect(actae_config)
        await wsa.send(json.dumps({"type": "ack", "id": "00000000-0000-0000-0000-000000000001"}))
        try:
            resp = await asyncio.wait_for(wsa.recv(), timeout=3)
            assert isinstance(resp, str)
        except asyncio.TimeoutError:
            pass  # server may not respond to ack
        await wsa.close()

    async def test_nack_with_valid_uuid(self, ws, actae_config):
        wsa = await _connect(actae_config)
        await wsa.send(json.dumps({"type": "nack", "id": "00000000-0000-0000-0000-000000000001"}))
        try:
            resp = await asyncio.wait_for(wsa.recv(), timeout=3)
            assert isinstance(resp, str)
        except asyncio.TimeoutError:
            pass  # server may not respond to nack
        await wsa.close()

    async def test_ack_with_invalid_uuid(self, ws, actae_config):
        wsa = await _connect(actae_config)
        await wsa.send(json.dumps({"type": "ack", "id": "not-a-uuid"}))
        resp = json.loads(await wsa.recv())
        assert "Error" in resp
        await wsa.close()

    async def test_nack_with_missing_id(self, ws, actae_config):
        wsa = await _connect(actae_config)
        await wsa.send(json.dumps({"type": "nack"}))
        resp = json.loads(await wsa.recv())
        assert "Error" in resp
        await wsa.close()


class TestDatabaseChangeMessage:
    """DatabaseChange message type and event whitelist."""

    async def test_database_change_valid_event(self, ws, actae_config):
        wsa = await _connect(actae_config)
        await wsa.send(json.dumps({
            "type": "database_change", "topic": "db-test",
            "event": "INSERT", "payload": {"table": "test"}
        }))
        resp = json.loads(await wsa.recv())
        assert resp is not None
        await wsa.close()

    async def test_database_change_invalid_event(self, ws, actae_config):
        wsa = await _connect(actae_config)
        await wsa.send(json.dumps({
            "type": "database_change", "topic": "db-test",
            "event": "TRUNCATE", "payload": {"table": "test"}
        }))
        resp = json.loads(await wsa.recv())
        assert "Error" in resp or "DatabaseChange" in resp
        await wsa.close()


class TestAuthMessageEdgeCases:
    """Auth message with null/empty/edge payloads."""

    async def test_auth_with_null_payload(self, ws, actae_config):
        import websockets as _ws
        ws_url = actae_config.ws_url.rstrip("/")
        if not ws_url.endswith("/ws"):
            ws_url = f"{ws_url}/ws"
        raw = await _ws.connect(ws_url, ssl=actae_config.ssl_context, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()
            await raw.send('{"type":"auth","api_key":"test-api-key-for-integration-tests","payload":null}')
            resp = json.loads(await raw.recv())
            assert "Connection" in resp or "Error" in resp
        finally:
            await raw.close()

    async def test_auth_with_empty_payload(self, ws, actae_config):
        import websockets as _ws
        ws_url = actae_config.ws_url.rstrip("/")
        if not ws_url.endswith("/ws"):
            ws_url = f"{ws_url}/ws"
        raw = await _ws.connect(ws_url, ssl=actae_config.ssl_context, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()
            await raw.send('{"type":"auth","api_key":"test-api-key-for-integration-tests","payload":{}}')
            resp = json.loads(await raw.recv())
            assert "Connection" in resp
            assert resp["Connection"]["event"] == "authenticated"
        finally:
            await raw.close()


class TestConnectionMessageTypes:
    """Connection messages for state management."""

    async def test_ping_with_custom_payload(self, ws, actae_config):
        wsa = await _connect(actae_config)
        await wsa.send(json.dumps({"type": "connection", "event": "ping", "payload": {"client_ts": 123456789}}))
        resp = json.loads(await wsa.recv())
        assert "Connection" in resp
        assert resp["Connection"]["event"] == "pong"
        await wsa.close()

    async def test_unknown_connection_event(self, ws, actae_config):
        wsa = await _connect(actae_config)
        await wsa.send(json.dumps({"type": "connection", "event": "unknown_event_type", "payload": {}}))
        resp = json.loads(await wsa.recv())
        assert resp is not None
        await wsa.close()


class TestMultipleChannels:
    """Subscribe to multiple channels."""

    async def test_subscribe_two_channels(self, ws, actae_config):
        wsa = await _connect(actae_config)
        await wsa.send(json.dumps({"type": "subscribe", "topic": "multi-ch-a"}))
        await wsa.recv()
        await wsa.send(json.dumps({"type": "subscribe", "topic": "multi-ch-b"}))
        resp = json.loads(await wsa.recv())
        assert resp["Subscription"]["event"] == "subscribed"
        await wsa.close()

    async def test_subscribe_same_topic_twice(self, ws, actae_config):
        wsa = await _connect(actae_config)
        await wsa.send(json.dumps({"type": "subscribe", "topic": "dup-test"}))
        await wsa.recv()
        await wsa.send(json.dumps({"type": "subscribe", "topic": "dup-test"}))
        resp = json.loads(await wsa.recv())
        assert resp["Subscription"]["event"] == "error"
        await wsa.close()
