# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""WebSocket connection lifecycle: connect, welcome, auth, subscribe, send/receive.

Covers: WS-01 through WS-08 from the test strategy.
"""

import pytest
import json

pytestmark = pytest.mark.asyncio


class TestConnectAndWelcome:
    async def test_welcome_on_connect(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            msg = json.loads(await raw.recv())
            assert "Connection" in msg
            assert msg["Connection"]["event"] == "connected"
        finally:
            await raw.close()

    async def test_welcome_has_valid_timestamp(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            msg = json.loads(await raw.recv())
            assert isinstance(msg["Connection"]["payload"]["timestamp"], int)
        finally:
            await raw.close()


class TestAuth:
    async def test_anonymous_auth_succeeds(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()
            await raw.send('{"type":"auth","payload":{}}')
            resp = json.loads(await raw.recv())
            assert resp is not None
        finally:
            await raw.close()

    async def test_api_key_auth_returns_user(self, ws, dev_mode):
        # Dev mode does not seed API keys into the auth DB, so the key falls
        # back to anonymous — this assertion only holds against docker.
        if dev_mode:
            pytest.skip("dev mode does not seed API keys")
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()
            await raw.send('{"type":"auth","api_key":"test-api-key-for-integration-tests","payload":{}}')
            resp = json.loads(await raw.recv())
            # The docker stack seeds the key via ACTAE_INITIAL_API_KEY, which
            # provision_initial_keys assigns to the "admin" principal.
            assert resp["Connection"]["payload"]["user_id"] == "admin"
        finally:
            await raw.close()


class TestSubscribe:
    async def test_subscribe_returns_confirmation(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()
            await raw.send('{"type":"auth","api_key":"test-api-key-for-integration-tests","payload":{}}')
            await raw.recv()
            await raw.send('{"type":"subscribe","topic":"ws-test-1"}')
            resp = json.loads(await raw.recv())
            assert resp["Subscription"]["event"] == "subscribed"
        finally:
            await raw.close()

    async def test_subscribe_contains_cursor_and_count(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()
            await raw.send('{"type":"auth","api_key":"test-api-key-for-integration-tests","payload":{}}')
            await raw.recv()
            await raw.send('{"type":"subscribe","topic":"ws-test-2"}')
            resp = json.loads(await raw.recv())
            p = resp["Subscription"]["payload"]
            assert "cursor" in p
            assert "subscriber_count" in p
        finally:
            await raw.close()

    async def test_duplicate_subscribe_errors(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()
            await raw.send('{"type":"auth","api_key":"test-api-key-for-integration-tests","payload":{}}')
            await raw.recv()
            await raw.send('{"type":"subscribe","topic":"ws-test-3"}')
            await raw.recv()
            await raw.send('{"type":"subscribe","topic":"ws-test-3"}')
            resp2 = json.loads(await raw.recv())
            assert resp2["Subscription"]["event"] == "error"
        finally:
            await raw.close()

    async def test_unsubscribe_returns_confirmation(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()
            await raw.send('{"type":"auth","api_key":"test-api-key-for-integration-tests","payload":{}}')
            await raw.recv()
            await raw.send('{"type":"subscribe","topic":"ws-test-sub-unsub"}')
            # Drain subscribe response messages (CursorSync + confirmation)
            import asyncio as _asyncio
            for _ in range(3):
                try:
                    await _asyncio.wait_for(raw.recv(), timeout=1)
                except _asyncio.TimeoutError:
                    break
            await raw.send('{"type":"unsubscribe","topic":"ws-test-sub-unsub"}')
            resp = json.loads(await raw.recv())
            assert resp["Subscription"]["event"] == "unsubscribed"
        finally:
            await raw.close()

    async def test_unsubscribe_nonsubscribed_errors(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()
            await raw.send('{"type":"auth","api_key":"test-api-key-for-integration-tests","payload":{}}')
            await raw.recv()
            await raw.send('{"type":"unsubscribe","topic":"never-subscribed"}')
            resp = json.loads(await raw.recv())
            assert resp["Subscription"]["event"] == "error"
        finally:
            await raw.close()


class TestSendReceive:
    async def test_invalid_json_returns_error(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()
            await raw.send("this is not json")
            resp = json.loads(await raw.recv())
            assert "Error" in resp
        finally:
            await raw.close()

    async def test_ping_returns_pong(self, ws):
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
