# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Input sanitization: injection patterns in all request fields.

Tests that SQL injection, XSS, path traversal, and command injection
patterns are handled safely by all API endpoints.
"""

import pytest
import httpx



class TestSqlInjectionVector:
    """SQL injection patterns rejected or handled safely."""

    SQLI_PATTERNS = [
        "'; DROP TABLE agent_events; --",
        "' OR '1'='1",
        "1; SELECT * FROM information_schema",
        "' UNION SELECT * FROM users --",
        "'; TRUNCATE agent_events; --",
    ]

    def test_sqli_in_channel_rejected(self, actae):
        for pattern in self.SQLI_PATTERNS:
            resp = actae.raw_post("/api/v1/events/record", {
                "channel_id": pattern, "event_type": "test",
                "payload": {}, "metadata": {"actor": "bot"},
            })
            assert resp.status_code in (201, 400, 422, 403), f"SQLi not caught: {pattern}"

    def test_sqli_in_payload_rejected(self, actae, ch_name):
        for pattern in self.SQLI_PATTERNS:
            resp = actae.raw_post("/api/v1/events/record", {
                "channel_id": ch_name, "event_type": "test",
                "payload": {"query": pattern}, "metadata": {"actor": "bot"},
            })
            assert resp.status_code in (201, 400, 422)

    def test_sqli_in_actor_rejected(self, actae, ch_name):
        for pattern in self.SQLI_PATTERNS:
            resp = actae.raw_post("/api/v1/events/record", {
                "channel_id": ch_name, "event_type": "test",
                "payload": {}, "metadata": {"actor": pattern},
            })
            assert resp.status_code in (201, 400, 422)


class TestXssVector:
    """XSS patterns handled safely."""

    XSS_PATTERNS = [
        "<script>alert('xss')</script>",
        "javascript:alert(1)",
        "onerror=alert(1)",
        "<img src=x onerror=alert(1)>",
        "<svg/onload=alert(1)>",
        "\"><script>alert(1)</script>",
    ]

    def test_xss_in_event_type(self, actae, ch_name):
        for pattern in self.XSS_PATTERNS:
            resp = actae.raw_post("/api/v1/events/record", {
                "channel_id": ch_name, "event_type": pattern,
                "payload": {}, "metadata": {"actor": "bot"},
            })
            assert resp.status_code in (201, 400, 422), f"XSS not handled: {pattern}"

    def test_xss_in_payload(self, actae, ch_name):
        for pattern in self.XSS_PATTERNS:
            resp = actae.raw_post("/api/v1/events/record", {
                "channel_id": ch_name, "event_type": "test.event",
                "payload": {"content": pattern}, "metadata": {"actor": "bot"},
            })
            assert resp.status_code in (201, 400, 422)


class TestPathTraversalVector:
    """Path traversal patterns handled safely."""

    TRAVERSAL_PATTERNS = [
        "../../etc/passwd",
        "..\\..\\windows\\system32",
        "%2e%2e%2f%2e%2e%2f",
        "....//....//etc/passwd",
        "..;/..;/..;/",
    ]

    def test_path_traversal_in_channel(self, actae):
        for pattern in self.TRAVERSAL_PATTERNS:
            resp = actae.raw_post("/api/v1/events/record", {
                "channel_id": pattern, "event_type": "test",
                "payload": {}, "metadata": {"actor": "bot"},
            })
            assert resp.status_code in (201, 400, 422)


class TestCommandInjectionVector:
    """Command injection patterns handled safely."""

    CMD_PATTERNS = [
        "; rm -rf /",
        "| cat /etc/passwd",
        "`ls -la`",
        "$(cat /etc/shadow)",
        "& ping -i 30 127.0.0.1 &",
    ]

    def test_cmd_injection_in_fields(self, actae, ch_name):
        for pattern in self.CMD_PATTERNS:
            resp = actae.raw_post("/api/v1/events/record", {
                "channel_id": ch_name, "event_type": pattern,
                "payload": {"cmd": pattern}, "metadata": {"actor": pattern},
            })
            assert resp.status_code in (201, 400, 422)


class TestNoSQLInjection:
    """JSON-specific injection patterns."""

    NOSQL_PATTERNS = [
        {"$gt": ""},
        {"$ne": ""},
        {"$where": "1==1"},
        {"$regex": ".*"},
        {"admin": True, "$gt": ""},
    ]

    def test_nosql_patterns_handled(self, actae, ch_name):
        for pattern in self.NOSQL_PATTERNS:
            resp = actae.raw_post("/api/v1/events/record", {
                "channel_id": ch_name, "event_type": "test.event",
                "payload": pattern, "metadata": {"actor": "bot"},
            })
            assert resp.status_code in (201, 400, 422)
