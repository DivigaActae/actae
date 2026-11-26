# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Opt-in end-to-end suite for self-hosted dashboard auth (first-run setup +
admin user management).

Boots dedicated managed-storage `actae` instances (like
`test_dev_lifecycle.py`, which this module reuses for the spawn machinery) and
drives the real HTTP endpoints. Opt-in because each test pays a full embedded
PostgreSQL cold start:

    ACTAE_SELF_HOSTED_AUTH=1 python3 -m pytest tests/test_self_hosted_auth.py -v

Needs the debug binary (`cd actae && cargo build`); PG binaries are reused from
the pg-embed cache / ACTAE_PG_BIN_DIR when present.
"""

import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx
import pytest

# Reuse the lifecycle spawn machinery (same directory).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_dev_lifecycle import (  # noqa: E402
    EmbeddedInstance,
    _actae_bin,
    _free_port,
    _instance_env,
)

pytestmark = pytest.mark.timeout(300)

ADMIN_EMAIL = "admin@actae.local"
# Policy-compliant (mixed case + digit + symbol, no common words/sequences).
ADMIN_PW = "Zx9-Qp4m-Vt7k-Lr2w"
MEMBER_PW = "Qw8-Zx3m-Pl6v-Rt9k"
SETUP_PW = "Xq7vLm2pRt9Zk4!B"
TOKEN_RE = re.compile(r"setup token:\s+([0-9a-f]{32,})")

SKIP_REASON = (
    "opt-in self-hosted-auth suite (set ACTAE_SELF_HOSTED_AUTH=1; needs an "
    "`actae` binary)"
)


def _skip_unless_available():
    if os.environ.get("ACTAE_SELF_HOSTED_AUTH") != "1":
        pytest.skip(SKIP_REASON)
    if not _actae_bin().is_file():
        pytest.skip("actae binary not found — build with `cd actae && cargo build` first")


@pytest.fixture(autouse=True)
def _require_env():
    _skip_unless_available()


@pytest.fixture
def boot(tmp_path):
    """Factory that boots isolated instances and tears them down at test end."""
    started = []

    def _boot(password=None):
        http_port = _free_port()
        pg_port = _free_port()
        while pg_port == http_port:
            pg_port = _free_port()
        data_dir = tmp_path / f"inst-{len(started)}"
        env = _instance_env(http_port, pg_port, data_dir)
        env["ACTAE_CONFIG_DIR"] = str(data_dir / "cfg")
        env["ACTAE_AUTO_BUMP"] = "0"
        if password is not None:
            env["ACTAE_INITIAL_ADMIN_PASSWORD"] = password
        inst = EmbeddedInstance(http_port, pg_port, data_dir, env)
        inst.start()
        started.append(inst)
        return inst

    yield _boot
    for inst in started:
        try:
            inst.cleanup()
        except Exception:  # noqa: BLE001 - best-effort teardown
            pass


def _post(inst, path, body):
    return httpx.post(
        f"{inst.url}{path}",
        headers={"Content-Type": "application/json"},
        json=body,
        timeout=10.0,
    )


def _login(inst, email, password):
    return _post(inst, "/api/v1/auth/login", {"email": email, "password": password})


def _bearer(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _token(resp):
    assert resp.status_code == 200, resp.text
    return resp.json()["token"]


def _setup_token(inst):
    # The banner (and its setup token) is flushed just after healthz comes up,
    # so poll briefly instead of racing the flush.
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        match = TOKEN_RE.search(inst.proc_stdout())
        if match:
            return match.group(1)
        time.sleep(0.25)
    raise AssertionError("no setup token in the startup banner")


# --------------------------------------------------------------------------- #
# First-run setup (item 2)
# --------------------------------------------------------------------------- #

def test_setup_token_flow(boot):
    inst = boot()  # no ACTAE_INITIAL_ADMIN_PASSWORD -> setup token
    token = _setup_token(inst)

    cfg = httpx.get(f"{inst.url}/api/v1/auth/config").json()
    assert cfg["setup_required"] is True

    # Login is impossible until setup completes.
    assert _login(inst, ADMIN_EMAIL, SETUP_PW).status_code == 401

    # Wrong token is rejected; a weak password is rejected.
    assert _post(inst, "/api/v1/auth/setup", {"token": "deadbeef", "password": SETUP_PW}).status_code == 401
    assert _post(inst, "/api/v1/auth/setup", {"token": token, "password": "password"}).status_code == 400

    # Correct token completes setup and mints a session.
    resp = _post(inst, "/api/v1/auth/setup", {"token": token, "password": SETUP_PW, "name": "Ada"})
    assert resp.status_code == 200, resp.text
    session = resp.json()["token"]

    me = httpx.get(f"{inst.url}/api/v1/auth/me", headers=_bearer(session)).json()
    assert me["user"]["role"] == "admin"
    assert me["user"]["name"] == "Ada"

    assert httpx.get(f"{inst.url}/api/v1/auth/config").json()["setup_required"] is False

    # Single-use: replaying the token is refused.
    assert _post(inst, "/api/v1/auth/setup", {"token": token, "password": SETUP_PW}).status_code == 409

    # The chosen password now works.
    assert _login(inst, ADMIN_EMAIL, SETUP_PW).status_code == 200


def test_setup_is_single_winner_under_concurrency(boot):
    inst = boot()
    token = _setup_token(inst)

    pw_a, pw_b = SETUP_PW, "Nv4-Kr8m-Wq2p-Zx7t"
    results = {}

    def submit(key, pw):
        results[key] = _post(
            inst, "/api/v1/auth/setup", {"token": token, "password": pw}
        ).status_code

    threads = [
        threading.Thread(target=submit, args=("a", pw_a)),
        threading.Thread(target=submit, args=("b", pw_b)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(results.values()) == [200, 409], results

    # Exactly one password was set — the winner's — and it is the effective one.
    winner_pw = pw_a if results["a"] == 200 else pw_b
    loser_pw = pw_b if results["a"] == 200 else pw_a
    assert _login(inst, ADMIN_EMAIL, winner_pw).status_code == 200
    assert _login(inst, ADMIN_EMAIL, loser_pw).status_code == 401
    assert httpx.get(f"{inst.url}/api/v1/auth/config").json()["setup_required"] is False


# --------------------------------------------------------------------------- #
# Admin user management (item 4)
# --------------------------------------------------------------------------- #

def test_admin_user_management(boot):
    inst = boot(password=ADMIN_PW)
    admin = _token(_login(inst, ADMIN_EMAIL, ADMIN_PW))

    # Preset password => setup already satisfied.
    assert httpx.get(f"{inst.url}/api/v1/auth/config").json()["setup_required"] is False

    me = httpx.get(f"{inst.url}/api/v1/auth/me", headers=_bearer(admin)).json()
    admin_id = me["user"]["id"]
    assert me["user"]["role"] == "admin"

    users = httpx.get(f"{inst.url}/api/v1/users", headers=_bearer(admin)).json()["users"]
    assert len(users) == 1

    # Create a member; duplicate email is a conflict.
    created = httpx.post(
        f"{inst.url}/api/v1/users",
        headers=_bearer(admin),
        json={"email": "dev@actae.local", "name": "Dev", "password": MEMBER_PW, "role": "member"},
    )
    assert created.status_code == 201, created.text
    dev_id = created.json()["user"]["id"]
    assert (
        httpx.post(
            f"{inst.url}/api/v1/users",
            headers=_bearer(admin),
            json={"email": "dev@actae.local", "password": MEMBER_PW},
        ).status_code
        == 409
    )

    # A member cannot administer users.
    member = _token(_login(inst, "dev@actae.local", MEMBER_PW))
    assert httpx.get(f"{inst.url}/api/v1/users", headers=_bearer(member)).status_code == 403

    # Admin promotes the member.
    assert (
        httpx.patch(
            f"{inst.url}/api/v1/users/{dev_id}",
            headers=_bearer(admin),
            json={"role": "admin"},
        ).status_code
        == 200
    )

    # Reset the member's password: old session is revoked, new password works.
    assert (
        httpx.post(
            f"{inst.url}/api/v1/users/{dev_id}/password",
            headers=_bearer(admin),
            json={"password": "Rt5-Pm9v-Kw3q-Zx8n"},
        ).status_code
        == 200
    )
    assert httpx.get(f"{inst.url}/api/v1/auth/me", headers=_bearer(member)).status_code == 401
    assert _login(inst, "dev@actae.local", "Rt5-Pm9v-Kw3q-Zx8n").status_code == 200

    # Delete the (now demoted-back) member.
    assert (
        httpx.patch(
            f"{inst.url}/api/v1/users/{dev_id}",
            headers=_bearer(admin),
            json={"role": "member"},
        ).status_code
        == 200
    )
    assert httpx.delete(f"{inst.url}/api/v1/users/{dev_id}", headers=_bearer(admin)).status_code == 200
    assert len(httpx.get(f"{inst.url}/api/v1/users", headers=_bearer(admin)).json()["users"]) == 1

    # Lockout guards: cannot demote or delete the last admin (self).
    assert (
        httpx.patch(
            f"{inst.url}/api/v1/users/{admin_id}",
            headers=_bearer(admin),
            json={"role": "member"},
        ).status_code
        == 409
    )
    assert httpx.delete(f"{inst.url}/api/v1/users/{admin_id}", headers=_bearer(admin)).status_code == 409


def test_invalid_preset_password_aborts_with_guidance(tmp_path):
    """A policy-violating ACTAE_INITIAL_ADMIN_PASSWORD fails loudly, with the
    new actionable message (rather than a bare bcrypt-policy error)."""
    http_port = _free_port()
    pg_port = _free_port()
    while pg_port == http_port:
        pg_port = _free_port()
    data_dir = tmp_path / "inst"
    env = _instance_env(http_port, pg_port, data_dir)
    env["ACTAE_CONFIG_DIR"] = str(data_dir / "cfg")
    env["ACTAE_AUTO_BUMP"] = "0"
    env["ACTAE_INITIAL_ADMIN_PASSWORD"] = "password"  # common word -> rejected

    data_dir.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(
        [str(_actae_bin())],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )
    try:
        out, _ = proc.communicate(timeout=180)
    finally:
        if proc.poll() is None:
            proc.kill()
    assert proc.returncode != 0
    assert "ACTAE_INITIAL_ADMIN_PASSWORD is not acceptable" in out
