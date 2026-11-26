# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""`actae keys` CLI lifecycle against the docker stack.

Runs the `actae` binary inside the actae-server container (it shares the
server's database environment) and verifies create → use → list → revoke.

NOTE: these tests modify/manage the docker stack — marked `slow` and skipped
in dev mode, like tests/test_resilience.py.
"""

import json
import subprocess
import pytest
import httpx


pytestmark = pytest.mark.slow


@pytest.fixture(autouse=True)
def _skip_in_dev_mode(dev_mode):
    if dev_mode:
        pytest.skip("CLI keys tests manage the docker compose stack")


def _actae_cli(*args, timeout=60):
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "actae", "actae"] + list(args),
        capture_output=True, text=True, timeout=timeout,
    )
    return result.returncode, result.stdout, result.stderr


def _extract_key(stdout: str) -> str:
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("sk-"):
            return line
    raise AssertionError(f"no raw key in CLI output: {stdout[:400]}")


class TestCliKeys:
    def test_cli_key_lifecycle(self, actae):
        # 1. create
        rc, out, err = _actae_cli("keys", "create", "cli-test-key", "--user-id", "pytest")
        assert rc == 0, f"create failed: {out} {err}"
        key = _extract_key(out)
        assert key.startswith("sk-") and len(key) > 20
        prefix = key[:12]

        # 2. the key works over HTTP
        resp = actae.record("cli-keys-ch", "cli.key", {"who": "pytest"}, "cli-test")
        assert resp["status"] == "recorded"

        # 3. list shows it as active
        rc, out, err = _actae_cli("keys", "list")
        assert rc == 0
        assert prefix in out
        assert "cli-test-key" in out

        # 4. revoke by prefix
        rc, out, err = _actae_cli("keys", "revoke", prefix)
        assert rc == 0, f"revoke failed: {out} {err}"

        # 5. revoked key is rejected (401/403 on write)
        resp = actae.raw_post(
            "/api/v1/events/record",
            {
                "channel_id": "cli-keys-ch",
                "event_type": "cli.key",
                "payload": {"who": "pytest"},
                "metadata": {"actor": "cli-test"},
            },
            headers={"Content-Type": "application/json", "X-API-Key": key},
        )
        assert resp.status_code in (401, 403), f"revoked key accepted: {resp.status_code}"

        # 6. list shows it as revoked
        rc, out, err = _actae_cli("keys", "list")
        assert "revoked" in out
