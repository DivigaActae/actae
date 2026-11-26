# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Server resilience: restart persistence, DB outage recovery, graceful shutdown.

Covers: RS-01, RS-02, RS-04 from the test strategy. (RS-03 was removed with
the external broadcast backplane.)

NOTE: These tests modify Docker container state and should be run in a
dedicated environment (the --all suite) or with --runslow.
"""

import time
import subprocess
import pytest
import httpx


pytestmark = pytest.mark.slow


@pytest.fixture(autouse=True)
def _skip_in_dev_mode(dev_mode):
    if dev_mode:
        pytest.skip("resilience tests manage the docker compose stack")


def _docker_compose(cmd, timeout=60):
    result = subprocess.run(
        ["docker", "compose"] + cmd,
        capture_output=True, text=True, timeout=timeout,
    )
    return result.returncode, result.stdout, result.stderr


class TestServerRestart:
    """RS-01: Server restart does not drop events."""

    def test_events_survive_restart(self, actae, ch_name):
        for i in range(10):
            actae.record(ch_name, "test.event", {"seq": i}, "resilience")
        before = actae.cursor(ch_name)

        ret, out, err = _docker_compose(["restart", "actae"])
        assert ret == 0, f"Restart failed: {err}"

        deadline = time.monotonic() + 30
        healthy = False
        while time.monotonic() < deadline:
            try:
                resp = httpx.get(
                    f"{actae._cfg.api_url}/healthz",
                    verify=actae._cfg.ssl_context,
                    timeout=5.0,
                )
                if resp.status_code == 200:
                    healthy = True
                    break
            except Exception:
                pass
            time.sleep(1)
        assert healthy, "Server did not become healthy after restart"

        after = actae.cursor(ch_name)
        assert after == before, f"Cursor changed from {before} to {after}"

        events = actae.replay(ch_name, cursor=0, limit=100)
        assert len(events) == 10

        actae.record(ch_name, "test.event", {"after_restart": True}, "resilience")
        assert actae.cursor(ch_name) == before + 1


class TestDatabaseOutage:
    """RS-02: Mid-stream DB outage with recovery."""

    def test_write_fails_during_db_outage(self, actae, ch_name):
        actae.record(ch_name, "test.event", {"step": 1}, "agent")

        _docker_compose(["stop", "postgres"])
        time.sleep(2)

        try:
            resp = actae.raw_post("/api/v1/events/record", {
                "channel_id": ch_name,
                "event_type": "test.event",
                "payload": {"step": 2},
                "metadata": {"actor": "agent"},
            })
            assert resp.status_code in (500, 502, 503), (
                f"Expected 5xx during DB outage, got {resp.status_code}"
            )
        finally:
            _docker_compose(["start", "postgres"])
            time.sleep(5)

        actae.record(ch_name, "test.event", {"step": 3, "recovered": True}, "agent")
        events = actae.replay(ch_name, cursor=0, limit=10)
        assert len(events) >= 2


class TestGracefulShutdown:
    """RS-04: Graceful shutdown drains connections."""

    def test_health_unhealthy_after_stop(self, actae, ch_name):
        actae.record(ch_name, "test.event", {}, "test")
        _docker_compose(["stop", "actae"])
        time.sleep(2)

        try:
            import httpx as _httpx
            with pytest.raises((_httpx.ConnectError, _httpx.ReadTimeout, ConnectionError)):
                _httpx.get(
                    f"{actae._cfg.api_url}/healthz",
                    verify=actae._cfg.ssl_context,
                    timeout=5.0,
                )
        finally:
            _docker_compose(["start", "actae"])
            time.sleep(5)
