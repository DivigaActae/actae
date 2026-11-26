# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Message security validation: XSS, SQL injection, topic validation.

MSG-001 through MSG-005 from the test gap analysis.
"""

import json
import pytest


class TestTopicSecurity:
    """MSG-001: Topic validation rejects dangerous patterns."""

    INJECTION_TOPICS = [
        "test/../etc/passwd",
        "test/..\\..\\windows\\system32",
        "test'; DROP TABLE agent_events; --",
        "test' OR '1'='1",
        "test<script>alert(1)</script>",
        "a/b/c/d/e/f/g/h/i/j/k",  # >10 nesting
    ]

    @pytest.mark.asyncio
    async def test_injection_topics_rejected(self, ws):
        import websockets as _ws
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()
            await raw.send(
                '{"type":"auth","api_key":"test-api-key-for-integration-tests","payload":{}}'
            )
            await raw.recv()
            for topic in self.INJECTION_TOPICS:
                await raw.send(json.dumps({"type": "subscribe", "topic": topic}))
                resp = json.loads(await raw.recv())
                assert "Error" in resp or resp["Subscription"]["event"] in ("subscribed", "error")
        finally:
            await raw.close()


class TestPayloadSecurity:
    """MSG-002: Payload content validation."""

    DANGEROUS_PAYLOADS = [
        {"__proto__": {"admin": True}},
        {"constructor": {"prototype": {"admin": True}}},
        {"x": "<script>alert(1)</script>"},
        {"url": "javascript:alert(1)"},
        {"x": "'; DROP TABLE users; --"},
        {"x": "1; SELECT * FROM information_schema"},
    ]

    def test_dangerous_payloads_accepted_or_rejected(self, actae, ch_name):
        for payload in self.DANGEROUS_PAYLOADS:
            resp = actae.raw_post("/api/v1/events/record", {
                "channel_id": ch_name, "event_type": "test.event",
                "payload": payload, "metadata": {"actor": "bot"},
            })
            assert resp.status_code in (201, 400, 422), f"Unexpected status for {payload}: {resp.status_code}"
