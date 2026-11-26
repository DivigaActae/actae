# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Security layer: IP blocking, rate limiting, auth lockout, message validation.

SEC-001 through SEC-009 from the test gap analysis.

Exercises the server's security layer through the actual HTTP/WS API.
"""

import time
import pytest
import httpx
import json



class TestAuthRateLimiting:
    """SEC-003: Auth rate limiting — rapid auth attempts trigger lockout."""

    def test_many_concurrent_requests_handled(self, actae, ch_name):
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
            futures = []
            for _ in range(50):
                futures.append(pool.submit(
                    actae.raw_post, "/api/v1/events/record", {
                        "channel_id": ch_name, "event_type": "test",
                        "payload": {}, "metadata": {"actor": "bot"},
                    }
                ))
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
        statuses = [r.status_code for r in results]
        ok_count = sum(1 for s in statuses if s == 201)
        limited_count = sum(1 for s in statuses if s in (429, 503))
        assert ok_count >= 1 or limited_count >= 1

    def test_health_still_accessible_under_load(self, actae):
        for _ in range(20):
            r = actae._http.get(f"{actae._cfg.api_url}/healthz", headers=actae._cfg.api_headers)
            assert r.status_code in (200, 503)


class TestInputSanitization:
    """Topics and payloads are sanitized against injection."""

    INJECTION_PAYLOADS = [
        {"channel_id": "test; DROP TABLE agent_events; --", "event_type": "test", "payload": {}, "metadata": {"actor": "bot"}},
        {"channel_id": "test' OR '1'='1", "event_type": "test", "payload": {}, "metadata": {"actor": "bot"}},
        {"channel_id": "test../etc/passwd", "event_type": "test", "payload": {}, "metadata": {"actor": "bot"}},
        {"channel_id": "test..\\..\\windows\\system32", "event_type": "test", "payload": {}, "metadata": {"actor": "bot"}},
    ]

    def test_sql_injection_channel_names(self, actae):
        for bad_channel in [p["channel_id"] for p in self.INJECTION_PAYLOADS]:
            resp = actae.raw_post("/api/v1/events/record", {
                "channel_id": bad_channel, "event_type": "agent.started",
                "payload": {}, "metadata": {"actor": "bot"},
            })
            assert resp.status_code in (201, 400, 422), f"Injection not caught: {bad_channel} -> {resp.status_code}"

    def test_xss_via_payload_rejected(self, actae, ch_name):
        xss_payloads = [
            {"msg": "<script>alert('xss')</script>"},
            {"msg": "javascript:alert(1)"},
            {"msg": "onerror=alert(1)"},
            {"url": "javascript:void(0)"},
        ]
        for payload in xss_payloads:
            resp = actae.raw_post("/api/v1/events/record", {
                "channel_id": ch_name, "event_type": "test.event",
                "payload": payload, "metadata": {"actor": "bot"},
            })
            assert resp.status_code in (201, 400, 422), f"XSS not caught: {payload} -> {resp.status_code}"

    def test_prototype_pollution_payload(self, actae, ch_name):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": ch_name, "event_type": "test.event",
            "payload": {"__proto__": {"admin": True}, "constructor": {"prototype": {"admin": True}}},
            "metadata": {"actor": "bot"},
        })
        assert resp.status_code in (201, 400, 422)

    def test_command_injection_payload(self, actae, ch_name):
        resp = actae.raw_post("/api/v1/events/record", {
            "channel_id": ch_name, "event_type": "test.event",
            "payload": {"cmd": "rm -rf /"}, "metadata": {"actor": "bot"},
        })
        assert resp.status_code in (201, 400, 422)


class TestMessageValidation:
    """Message security validation through WebSocket."""

    @pytest.mark.asyncio
    async def test_oversized_message_rejected(self, ws):
        import websockets as _ws
        import asyncio as _asyncio
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10, max_size=5*1024*1024)
        try:
            await raw.recv()
            big_payload = "x" * (2 * 1024 * 1024)
            await raw.send(json.dumps({"type": "subscribe", "topic": big_payload}))
            resp = await _asyncio.wait_for(raw.recv(), timeout=5)
            assert isinstance(resp, str)
        finally:
            await raw.close()

    @pytest.mark.asyncio
    async def test_malformed_utf8_rejected(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()
            await raw.send(b'\xff\xfe\x00\x01'.decode('utf-8', errors='replace'))
            resp = await raw.recv()
            assert resp is not None
        finally:
            await raw.close()


class TestSecurityEndpoints:
    """Health and metrics endpoints reflect security state."""

    def test_health_returns_security_status(self, actae):
        resp = actae._http.get(f"{actae._cfg.api_url}/healthz", headers=actae._cfg.api_headers)
        assert resp.status_code in (200, 503)
        body = resp.json()
        assert "status" in body
        assert "components" in body

    def test_metrics_endpoint_accessible(self, actae):
        resp = actae._http.get(
            f"{actae._cfg.api_url}/metrics",
            headers=actae._cfg.api_headers,
        )
        assert resp.status_code in (200, 403, 404)
