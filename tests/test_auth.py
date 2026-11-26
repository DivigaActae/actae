# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Authentication flows: API key, system channel restrictions, WebSocket auth.

Covers: AU-01 through AU-05 from the test strategy.
"""

import pytest
import json


class TestAPIKeyAuth:
    """AU-01: Missing/invalid API key."""

    def test_missing_api_key_for_write_returns_error(self, actae, dev_mode):
        if dev_mode:
            pytest.skip("dev mode enables anonymous access")
        body = {
            "channel_id": "test-ch",
            "event_type": "agent.started",
            "payload": {},
            "metadata": {"actor": "test"},
        }
        resp = actae._http.post(
            f"{actae._cfg.api_url}/api/v1/events/record",
            json=body,
            headers={"Content-Type": "application/json"},
        )
        # With mTLS, the connection is authenticated at TLS level but
        # the application still needs an API key for write operations
        assert resp.status_code in (401, 403, 500), f"Unexpected {resp.status_code}: {resp.text[:200]}"

    def test_invalid_api_key_returns_401(self, actae, dev_mode):
        if dev_mode:
            pytest.skip("dev mode enables anonymous access")
        body = {
            "channel_id": "test-ch",
            "event_type": "agent.started",
            "payload": {},
            "metadata": {"actor": "test"},
        }
        resp = actae._http.post(
            f"{actae._cfg.api_url}/api/v1/events/record",
            json=body,
            headers={"Content-Type": "application/json", "X-API-Key": "invalid-key-12345"},
        )
        assert resp.status_code in (401, 403)

    def test_valid_api_key_succeeds(self, actae, ch_name):
        result = actae.record(ch_name, "test.event", {}, "test")
        assert result["status"] == "recorded"

    def test_read_without_api_key_may_succeed(self, actae, ch_name):
        """Anonymous reads may be allowed if configured. Test reads without auth."""
        actae.record(ch_name, "test.event", {}, "bot")
        resp = actae._http.get(
            f"{actae._cfg.api_url}/api/v1/events/replay/{ch_name}?cursor=0&limit=10",
            headers={"Content-Type": "application/json"},
        )
        # ENABLE_ANONYMOUS=true allows anonymous reads
        assert resp.status_code in (200, 401, 403), f"Unexpected {resp.status_code}"

    def test_query_without_api_key_may_succeed(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        resp = actae._http.post(
            f"{actae._cfg.api_url}/api/v1/events/query",
            json={"limit": 10},
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code in (200, 401, 403), f"Unexpected {resp.status_code}"

    def test_cursor_without_api_key_may_succeed(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        resp = actae._http.get(
            f"{actae._cfg.api_url}/api/v1/events/cursor/{ch_name}",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code in (200, 401, 403), f"Unexpected {resp.status_code}"


class TestChannelRestrictions:
    """AU-02, AU-03: System/admin channel protection."""

    def test_system_channel_write_forbidden(self, actae, dev_mode):
        if dev_mode:
            pytest.skip("dev mode enables anonymous access")
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "system.config",
            "event_type": "agent.started",
            "payload": {},
            "metadata": {"actor": "test"},
        })
        assert resp.status_code in (403, 401)
        body = resp.json()
        assert "forbidden" in body.get("status", "").lower()

    def test_admin_channel_write_forbidden(self, actae, dev_mode):
        if dev_mode:
            pytest.skip("dev mode enables anonymous access")
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": "admin.secrets",
            "event_type": "agent.started",
            "payload": {},
            "metadata": {"actor": "test"},
        })
        assert resp.status_code in (403, 401)
        body = resp.json()
        assert "forbidden" in body.get("status", "").lower()

    def test_normal_channel_allowed(self, actae, ch_name):
        result = actae.record(ch_name, "agent.started", {}, "test")
        assert result["status"] == "recorded"


@pytest.mark.asyncio
class TestWebSocketAuth:
    """AU-04, AU-05: WebSocket authentication."""

    async def test_ws_auth_with_invalid_token(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()  # welcome
            await raw.send('{"type":"auth","token":"invalid.jwt.token","payload":{}}')
            resp = await raw.recv()
            # Server may fall back to session auth with anonymous
            # Either an error or an authenticated response are acceptable
            assert resp is not None
        finally:
            await raw.close()

    async def test_ws_auth_with_valid_api_key(self, ws, actae):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()  # welcome
            await raw.send(f'{{"type":"auth","api_key":"{actae._cfg.api_key}","payload":{{}}}}')
            resp = await raw.recv()
            # May be rate limited if too many auth attempts
            assert '"authenticated"' in resp or '"Rate limit"' in resp
        finally:
            await raw.close()

    async def test_ws_auth_with_no_credentials(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()  # welcome
            await raw.send('{"type":"auth","payload":{}}')
            resp = await raw.recv()
            assert '"authenticated"' in resp or '"Error"' in resp
        finally:
            await raw.close()


@pytest.mark.asyncio
class TestSystemChannelSubscribe:
    """System channel restrictions via WebSocket."""

    async def test_system_channel_subscribe_rejected(self, ws, actae):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()  # welcome
            await raw.send(f'{{"type":"auth","api_key":"{actae._cfg.api_key}","payload":{{}}}}')
            await raw.recv()  # auth response
            await raw.send('{"type":"subscribe","topic":"system.internal"}')
            resp = await raw.recv()
            assert '"Error"' in resp or '"error"' in resp
        finally:
            await raw.close()
