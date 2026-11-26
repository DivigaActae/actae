# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Auth-abuse limiting separation: failed attempts count, valid traffic never does.

Regression suite for the server fix where the per-IP auth rate-limit bucket
(`ACTAE_IP_AUTH_RATE` / `ACTAE_IP_AUTH_CAPACITY`) was consumed on EVERY
authenticated request — a valid workload bursting above the failure threshold
was throttled as authentication abuse (429 "Too many attempts").

The fix: in the HTTP API-key path the bucket is consumed ONLY on authentication
FAILURE (invalid key, invalid session, missing credentials). Successful
authentication never touches it. Connection-based auth (WebSocket) keeps
per-attempt consumption.

Verified invariants (all deterministic — the instance runs with a rate of 0
tokens/s so the bucket never refills):

1. A valid-key burst (well above the bucket capacity) is never 429'd.
2. An invalid-key burst gets plain 401s for the first `capacity` attempts,
   then a steady 429 once the bucket is exhausted.
3. A valid-key request immediately after an exhausted bucket still succeeds:
   an attacker burning the bucket can never block a legitimate client.

Opt-in: set ACTAE_LIFECYCLE=1 (same infra as the embedded-lifecycle suite —
needs an `actae` binary and the pg-embed binaries).
"""

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

SKIP_REASON = (
    "opt-in embedded-lifecycle suite (set ACTAE_LIFECYCLE=1; needs an `actae` binary)"
)

API_KEY = "sk-dev-0000000000000000000000"

# Rate 0 tokens/s: the bucket never refills, so the test is immune to timing.
AUTH_RATE = "0"
AUTH_CAPACITY = "2"


def _actae_bin():
    override = os.environ.get("ACTAE_BIN")
    if override:
        return Path(override)
    bin_path = REPO_ROOT / "actae" / "target" / "debug" / "actae"
    if sys.platform == "win32":
        return Path(str(bin_path) + ".exe")
    return bin_path


def _pg_bin_dir():
    """Reuse the pg-embed cache binaries when present (avoids re-downloading
    the same zonky package on every fresh data dir)."""
    override = os.environ.get("ACTAE_PG_BIN_DIR")
    if override:
        path = Path(override)
        if not path.is_dir():
            return None
        if (path / "bin" / "initdb").is_file():
            return path / "bin"
        return path
    home = Path.home()
    candidates = [
        home / "Library" / "Caches" / "pg-embed",
        home / ".cache" / "pg-embed",
        Path(os.environ.get("XDG_CACHE_HOME", str(home / ".cache"))) / "pg-embed",
    ]
    for root in candidates:
        if root.is_dir():
            hits = sorted(root.glob("*/*/*/bin/postgres"))
            if hits:
                return hits[-1].parent
    return None


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _instance_env(http_port, pg_port, data_dir):
    env = dict(os.environ)
    for key in (
        "ACTAE_PORT", "ACTAE_HOST", "ACTAE_DATA_DIR", "ACTAE_EMBEDDED_PG_PORT",
        "ACTAE_EMBEDDED_PG_PASSWORD", "ACTAE_EMBEDDED_PG_WIPE", "ACTAE_AUTO_BUMP",
        "ACTAE_PG_NO_HOST", "ACTAE_PG_BIN_DIR", "ACTAE_IP_AUTH_RATE",
        "ACTAE_IP_AUTH_CAPACITY",
        "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT",
        "POSTGRES_DB", "CLIENT_POSTGRES_USER", "CLIENT_POSTGRES_PASSWORD",
        "CLIENT_POSTGRES_HOST", "CLIENT_POSTGRES_PORT", "CLIENT_POSTGRES_DB",
        "DATABASE_URL", "CLIENT_DATABASE_URL", "ACTAE_STORAGE",
        "ACTAE_DEPLOYMENT_MODE", "HTTPS_PORT",
    ):
        env.pop(key, None)
    env["ACTAE_PORT"] = str(http_port)
    env["ACTAE_HOST"] = "127.0.0.1"
    env["ACTAE_EMBEDDED_PG_PORT"] = str(pg_port)
    env["ACTAE_DATA_DIR"] = str(data_dir)
    env["ACTAE_INITIAL_API_KEY"] = API_KEY
    env["ACTAE_NO_BROWSER"] = "1"
    env["ACTAE_IP_AUTH_RATE"] = AUTH_RATE
    env["ACTAE_IP_AUTH_CAPACITY"] = AUTH_CAPACITY
    bin_dir = _pg_bin_dir()
    if bin_dir is not None:
        env["ACTAE_PG_BIN_DIR"] = str(bin_dir)
    return env


def _wait_for_health(url, timeout=120):
    deadline = time.monotonic() + timeout
    with httpx.Client(timeout=2.0) as client:
        while time.monotonic() < deadline:
            try:
                resp = client.get(f"{url}/healthz")
                if resp.status_code == 200 and resp.json().get("status") == "ok":
                    return
            except Exception:
                pass
            time.sleep(0.5)
    raise AssertionError(f"server at {url} did not become healthy")


def _pytest_skip_unless_available():
    if os.environ.get("ACTAE_LIFECYCLE") != "1":
        pytest.skip(SKIP_REASON)
    if not _actae_bin().is_file():
        pytest.skip("actae binary not found — build with `cd actae && cargo build` first")


@pytest.fixture
def instance(tmp_path):
    _pytest_skip_unless_available()
    http_port = _free_port()
    pg_port = _free_port()
    data_dir = tmp_path / "actae-rate"
    data_dir.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(
        [str(_actae_bin())],
        stdout=open(data_dir / "actae.log", "w"),
        stderr=subprocess.STDOUT,
        text=True,
        env=_instance_env(http_port, pg_port, data_dir),
    )
    # The instance may have auto-bumped; re-read the actual URL.
    url = f"http://127.0.0.1:{http_port}"
    try:
        _wait_for_health(url, timeout=120)
        info_file = data_dir / "url"
        if info_file.exists():
            try:
                info = json.loads(info_file.read_text())
                url = info["url"]
            except Exception:
                pass
        yield url
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=40)
            except Exception:
                proc.kill()


def _valid_request(url, client):
    return client.get(f"{url}/api/v1/channels", headers={"X-API-Key": API_KEY})


def _invalid_request(url, client):
    return client.get(f"{url}/api/v1/channels", headers={"X-API-Key": "sk-invalid-0000000000000000000"})


class TestAuthRateLimitSeparation:
    def test_valid_burst_never_limited(self, instance):
        """A valid-key burst far above the auth bucket capacity (2) must
        never be throttled — the bucket counts failures only."""
        with httpx.Client(timeout=10.0) as client:
            for i in range(20):
                resp = _valid_request(instance, client)
                assert resp.status_code == 200, (
                    f"valid request {i} throttled: {resp.status_code} {resp.text}"
                )

    def test_invalid_burst_exhausts_bucket_then_429(self, instance):
        """Failed attempts consume the bucket: the first `capacity` get plain
        401s, everything after gets a 429 while the bucket is empty."""
        with httpx.Client(timeout=10.0) as client:
            statuses = []
            for _ in range(6):
                resp = _invalid_request(instance, client)
                statuses.append(resp.status_code)
        assert statuses[:2] == [401, 401], f"first attempts must be 401, got {statuses}"
        assert all(s == 429 for s in statuses[2:]), (
            f"attempts after bucket exhaustion must be 429, got {statuses}"
        )

    def test_valid_request_unblocked_after_exhausted_bucket(self, instance):
        """An attacker burning the auth bucket must never block a legitimate
        client: a valid request right after bucket exhaustion still succeeds.

        Failures stay under the lockout threshold (AUTH_FAILURE_LOCKOUT_
        THRESHOLD default is 5) so this assertion isolates the bucket, not
        the lockout cache."""
        with httpx.Client(timeout=10.0) as client:
            for _ in range(4):
                resp = _invalid_request(instance, client)
                assert resp.status_code in (401, 429)
            resp = _valid_request(instance, client)
            assert resp.status_code == 200, (
                f"valid request throttled after failed-attempt burst: "
                f"{resp.status_code} {resp.text}"
            )