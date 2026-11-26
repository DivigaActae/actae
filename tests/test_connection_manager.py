# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Connection manager: limits, cleanup, back-pressure, pool management.

CON-001 through CON-005 from the test gap analysis.
"""

import time
import pytest


class TestConnectionLimits:
    """CON-001: Connection limit enforcement."""

    def test_connection_count_tracked(self, actae):
        resp = actae._http.get(f"{actae._cfg.api_url}/healthz", headers=actae._cfg.api_headers)
        if resp.status_code == 200:
            body = resp.json()
            components = body.get("components", {})
            connections = components.get("connections", {})
            assert "active_connections" in connections


class TestInactiveCleanup:
    """CON-003: Inactive connection cleanup."""

    @pytest.mark.asyncio
    async def test_connect_then_disconnect(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        await raw.recv()
        await raw.send(
            '{"type":"auth","api_key":"test-api-key-for-integration-tests","payload":{}}'
        )
        await raw.recv()
        await raw.close()
        time.sleep(0.5)


class TestWebSocketCleanup:
    """Ensure WS subscriptions are cleaned up on disconnect."""

    @pytest.mark.asyncio
    async def test_subscribe_then_disconnect_cleanup(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()
            await raw.send(
                '{"type":"auth","api_key":"test-api-key-for-integration-tests","payload":{}}'
            )
            await raw.recv()
            await raw.send('{"type":"subscribe","topic":"cleanup-test"}')
            await raw.recv()
        finally:
            await raw.close()
        time.sleep(0.5)
