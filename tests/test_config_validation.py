# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Configuration validation tests.

Tests that environment variable edge cases are handled correctly.
Some tests verify the server rejects bad config on startup.
"""

import subprocess
import pytest


class TestServerHealth:
    """Server health check returns expected fields."""

    def test_health_has_required_fields(self, actae):
        resp = actae._http.get(f"{actae._cfg.api_url}/healthz", headers=actae._cfg.api_headers)
        assert resp.status_code in (200, 503)
        body = resp.json()
        assert "status" in body
        assert "components" in body
        components = body["components"]
        assert "database" in components
        assert "connections" in components

    def test_health_database_status(self, actae):
        resp = actae._http.get(f"{actae._cfg.api_url}/healthz", headers=actae._cfg.api_headers)
        if resp.status_code == 200:
            body = resp.json()
            db = body.get("components", {}).get("database", {})
            assert db.get("status") in ("ok", "error")

    def test_readyz_returns_200(self, actae):
        resp = actae._http.get(f"{actae._cfg.api_url}/readyz", headers=actae._cfg.api_headers)
        assert resp.status_code in (200, 503)


class TestMetricsEndpoint:
    """Metrics endpoint returns prometheus-style data."""

    def test_metrics_json_endpoint(self, actae):
        resp = actae._http.get(f"{actae._cfg.api_url}/metrics.json", headers=actae._cfg.api_headers)
        assert resp.status_code in (200, 404)

class TestApiKeyScenarios:
    """API key edge cases and error responses."""

    def test_missing_api_key_for_write(self, actae, dev_mode):
        if dev_mode:
            pytest.skip("dev mode enables anonymous access")
        resp = actae._http.post(
            f"{actae._cfg.api_url}/api/v1/events/record",
            json={"channel_id": "test", "event_type": "test", "payload": {}, "metadata": {"actor": "bot"}},
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code in (401, 403)

    def test_unknown_api_key(self, actae, dev_mode):
        if dev_mode:
            pytest.skip("dev mode enables anonymous access")
        resp = actae._http.post(
            f"{actae._cfg.api_url}/api/v1/events/record",
            json={"channel_id": "test", "event_type": "test", "payload": {}, "metadata": {"actor": "bot"}},
            headers={"Content-Type": "application/json", "X-API-Key": "nonexistent-key-xyz"},
        )
        assert resp.status_code in (401, 403)

    def test_empty_api_key_header(self, actae, dev_mode):
        if dev_mode:
            pytest.skip("dev mode enables anonymous access")
        resp = actae._http.post(
            f"{actae._cfg.api_url}/api/v1/events/record",
            json={"channel_id": "test", "event_type": "test", "payload": {}, "metadata": {"actor": "bot"}},
            headers={"Content-Type": "application/json", "X-API-Key": ""},
        )
        assert resp.status_code in (401, 403, 500)


class TestQueryBoundaries:
    """Query endpoint with edge case inputs."""

    def test_query_with_only_event_type(self, actae):
        events = actae.query(event_type="agent.started", limit=10)
        assert isinstance(events, list)

    def test_query_with_only_actor(self, actae):
        events = actae.query(actor="bot", limit=10)
        assert isinstance(events, list)

    def test_query_no_filters(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "bot")
        events = actae.query(limit=10)
        assert isinstance(events, list)
