# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Hermetic fleet E2E topology (Deep-004).

Boots a REAL SaaS backend, REAL `actae-federation-gateway` binary and TWO
real embedded `actae` instances on ephemeral ports, wires them together
(the SaaS is the control plane + fleet-token issuer; instances enroll and
sync federation keys; the gateway routes to them), provisions an org +
instance + service token, and drives REAL network assertions through the
Python SDK.

Everything here is real HTTP/WS between processes — no mocks, no fakes.

Usage:
    ACTAE_E2E=1 python3 -m pytest tests/fleet_e2e/ -v
    ACTAE_BIN=/path/to/actae/target/debug/actae \
    ACTAE_GATEWAY_BIN=/path/to/actae/target/debug/actae-federation-gateway \
    ACTAE_E2E=1 python3 -m pytest tests/fleet_e2e/ -v
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import shlex
import sqlite3
import subprocess
import sys
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "saas" / "backend" / "tests"))
# helpers.py (saas/backend/tests) imports actae_saas at module load; make the
# SaaS backend importable even when it is not pip-installed (the fleet CI job
# only installs tests/requirements.txt).
sys.path.insert(0, str(REPO_ROOT / "saas" / "backend" / "src"))

import requests  # noqa: E402

from helpers import signup_with_install  # noqa: E402  (reused: real HTTP signup)

E2E_SKIP = pytest_skip = None
try:
    import pytest

    E2E_SKIP = pytest.mark.skipif(
        os.environ.get("ACTAE_E2E") != "1",
        reason="opt-in hermetic fleet E2E (ACTAE_E2E=1)",
    )
except ImportError:  # pragma: no cover
    pass


def _free_port() -> int:
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _actae_bin() -> Path:
    return Path(os.environ.get("ACTAE_BIN", REPO_ROOT / "actae" / "target" / "debug" / "actae"))


def _gateway_bin() -> Path:
    return Path(
        os.environ.get(
            "ACTAE_GATEWAY_BIN",
            REPO_ROOT / "actae" / "target" / "debug" / "actae-federation-gateway",
        )
    )


def _wait_http(url: str, timeout: float = 60.0, method: str = "GET") -> requests.Response:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            r = requests.request(method, url, timeout=2)
            if r.status_code < 500:
                return r
            last = r
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(0.3)
    raise RuntimeError(f"endpoint {url} not ready: {last}")


def _wait_file(path: Path, timeout: float = 60.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists() and path.stat().st_size > 0:
            return
        time.sleep(0.3)
    raise RuntimeError(f"file {path} not produced")


def _spawn(cmd, env, log_path: Path, **kw):
    return subprocess.Popen(
        cmd,
        env=env,
        stdout=open(log_path, "w"),
        stderr=subprocess.STDOUT,
        **kw,
    )


def _start_saas(tmp_path: Path, db_path: Path, port: int, federation_key: str):
    """Boot the REAL SaaS backend in real (non-test-driver) mode so it seeds
    the federation signing key (kid `fk1`) and distributes its public half to
    enrolled instances through the control-plane sync."""
    env = {
        **os.environ,
        "SAAS_KEY_ENCRYPTION_KEY": "TWRkIG_NGeSAhp-9iWnP15ziQiFzwXnDHXueCAUUXx8=",
        "SAAS_DB_PATH": str(db_path),
        "SAAS_FEDERATION_SIGNING_KEY": federation_key,
        "SAAS_LICENSE_SIGNING_KEY": "8HNMd025tSUQmt0LiWwTelfPj39Fb29LtOAKgbgYnsc",
        "SAAS_INTERNAL_TOKEN": "e2e-internal-token-0123456789abcdef",
        "SAAS_SIGNUP_ENABLED": "1",
        "SAAS_PORT": str(port),
    }
    cmd = [sys.executable, "-m", "actae_saas", "--port", str(port)]
    return _spawn(cmd, env, tmp_path / "saas.log")


def _provision_org_instance(db_path, email, instance_id, mode="hosted", org_id=None):
    """Seed a customer + org + installation (secret-enrollable, active,
    remote access in the given mode) + a valid organization service token in
    the REAL SaaS database. `org_id` reuses an existing org so multiple
    instances share one organization. Returns (org_id, instance_id, svc)."""
    sys.path.insert(0, str(REPO_ROOT / "saas" / "backend" / "src"))
    from actae_saas import crypto  # noqa: E402

    conn = sqlite3.connect(db_path)
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ")
    customer_id = "cus_e2e_" + uuid.uuid4().hex[:16]
    org_id = org_id or ("org_" + hashlib.sha256(email.encode()).hexdigest()[:32])
    raw = crypto.mint_service_token()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO customers (id, email, password_hash, name, created_at) "
            "VALUES (?, ?, ?, 'Fleet E2E', ?)",
            (customer_id, email, "e2e-not-used", now),
        )
        conn.execute(
            "INSERT OR IGNORE INTO organizations (id, name, slug, created_at) "
            "VALUES (?, ?, ?, ?)",
            (org_id, "Fleet E2E Org", "fleet-e2e-" + org_id[-16:], now),
        )
        conn.execute(
            "INSERT OR IGNORE INTO organization_members "
            "(organization_id, customer_id, role, created_at) VALUES (?, ?, 'owner', ?)",
            (org_id, customer_id, now),
        )
        # Installation: secret-enrollable, active (trial), remote access mode.
        enroll_secret = "es_e2e_" + instance_id
        conn.execute(
            "INSERT OR IGNORE INTO installations "
            "(id, customer_id, name, status, trial_ends_at, enrollment_secret_hash, rev, created_at, "
            " organization_id, deployment_type, desired_state) "
            "VALUES (?, ?, 'fleet-e2e-host', 'active', ?, ?, 1, ?, ?, 'hosted', 'running')",
            (
                instance_id,
                customer_id,
                "2030-01-01T00:00:00Z",
                crypto.sha256_hex(enroll_secret),
                now,
                org_id,
            ),
        )
        conn.execute(
            "INSERT OR IGNORE INTO instance_remote_access "
            "(installation_id, enabled, mode, policy, policy_version, actor, created_at, updated_at) "
            "VALUES (?, 1, ?, ?, 1, 'e2e', ?, ?)",
            (
                instance_id,
                mode,
                json.dumps({"allowed_channels": ["public.*"]}),
                now,
                now,
            ),
        )
        conn.execute(
            "INSERT OR IGNORE INTO instance_fleet_status "
            "(installation_id, protocol_version, connected, safe_position, updated_at) "
            "VALUES (?, 1, 0, 0, ?)",
            (instance_id, now),
        )
        # Organization service token: store the correct HMAC, keep the raw.
        key_enc_key = "TWRkIG_NGeSAhp-9iWnP15ziQiFzwXnDHXueCAUUXx8="
        token_hash = _compute_service_token_hash(raw, key_enc_key)
        conn.execute(
            "INSERT INTO organization_api_tokens "
            "(id, organization_id, name, token_hash, hash_version, grants, created_at) "
            "VALUES (?, ?, 'fleet-e2e', ?, 1, ?, ?)",
            (
                "st_" + uuid.uuid4().hex[:24],
                org_id,
                token_hash,
                json.dumps(
                    [
                        "organization:read",
                        "instances:read",
                        "events:read",
                        "events:write",
                        "state:read",
                        "state:write",
                        "channels:read",
                        "channels:write",
                        "forks:create",
                        "tokens:manage",
                    ],
                    separators=(",", ":"),
                ),
                now,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return org_id, instance_id, raw


def _compute_service_token_hash(raw: str, key_encryption_key: str) -> str:
    """Mirror config.service_token_hmac_secret() + crypto.hmac_service_token()"""
    import hashlib
    import hmac as hmac_mod

    secret = hashlib.sha256(key_encryption_key.encode("utf-8")).digest()
    return hmac_mod.new(secret, raw.encode("utf-8"), hashlib.sha256).hexdigest()




def _start_instance(tmp_path, saas_url, port, install_id, *, relay_url=None, relay_credential=None):
    """Boot a real embedded `actae` instance enrolled with the SaaS control
    plane via ACTAE_ENROLL_SECRET (matching the seeded enrollment_secret_hash)
    so it syncs the federation verification keys (kid `fk1`). When
    `relay_url` is set the instance dials OUT to the gateway as a relay-mode
    instance (ACTAE_RELAY_ENABLED)."""
    env = {
        **os.environ,
        "ACTAE_CONTROL_PLANE_URL": saas_url,
        "ACTAE_INSTALL_ID": install_id,
        "ACTAE_ENROLL_SECRET": "es_e2e_" + install_id,
        "ACTAE_PORT": str(port),
        "ACTAE_SUBSCRIPTION_GRACE_SECS": "7200",
        "ACTAE_SYNC_INTERVAL_SECS": "3",
        "ACTAE_AUTO_BUMP": "0",
        "ACTAE_STORAGE": "managed",
        "ACTAE_IP_AUTH_RATE": "10000",
        "ACTAE_IP_AUTH_CAPACITY": "20000",
        "ACTAE_INITIAL_API_KEY": "sk-e2e-%s-00000000000000000000000000000000" % install_id,
        "ACTAE_DATA_DIR": str(tmp_path / ("data-" + install_id)),
        "ACTAE_EMBEDDED_PG_PORT": str(_free_port()),
        "ACTAE_NO_BROWSER": "1",
    }
    if relay_url is not None:
        env["ACTAE_RELAY_ENABLED"] = "1"
        env["ACTAE_RELAY_URL"] = relay_url
        env["ACTAE_RELAY_CREDENTIAL"] = relay_credential or "e2e-relay-shared-secret"
    return _spawn([str(_actae_bin())], env, tmp_path / ("instance-%s.log" % install_id))


def _start_gateway(tmp_path, saas_url, port, instance_urls, federation_pubkey_b64, *, relay_credential=None):
    env = {
        **os.environ,
        "GATEWAY_BIND": f"127.0.0.1:{port}",
        "GATEWAY_CONTROL_PLANE_URL": saas_url,
        "GATEWAY_INTERNAL_TOKEN": "e2e-internal-token-0123456789abcdef",
        "GATEWAY_ID": "gw-e2e",
        "GATEWAY_FEDERATION_VERIFICATION_KEYS": "fk1:" + federation_pubkey_b64,
        "ACTAE_FEDERATION_AUDIENCE": "actae-fleet-gateway",
        "ACTAE_FEDERATION_ISSUER": "actae-fleet",
        "GATEWAY_INSTANCE_URLS": json.dumps(instance_urls),
        "GATEWAY_RATE_LIMIT_PER_SEC": "10000",
        "GATEWAY_MAX_STREAM_CONNECTIONS": "200",
        "GATEWAY_SAAS_ALLOW_HTTP": "1",
    }
    if relay_credential is not None:
        # Credential relay (dev path): the relay route is served on the main
        # gateway port; relay-mode instances dial OUT and authenticate with the
        # shared credential.
        env["GATEWAY_RELAY_CREDENTIAL"] = relay_credential
    return _spawn([str(_gateway_bin())], env, tmp_path / "gateway.log")


def _make_federation_pubkey_b64(seed_b64: str) -> str:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives import serialization

    key = Ed25519PrivateKey.from_private_bytes(base64.b64decode(seed_b64))
    pub = key.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    )
    return base64.b64encode(pub).decode()


def _new_federation_key() -> str:
    return base64.b64encode(os.urandom(32)).decode()


def build_topology(tmp_path, *, instance_ids=("ins_e2e_1", "ins_e2e_2"),
                   relay_instance_id="ins_e2e_2"):
    """Boot the full topology and return a namespace of live handles. Callers
    MUST call `.teardown()`. `relay_instance_id` boots as a relay-mode instance
    that dials OUT to the gateway (credential relay, dev path); the rest are
    hosted instances."""
    for name in (_actae_bin(), _gateway_bin()):
        if not name.exists():
            raise RuntimeError(f"binary not built: {name} (cd actae && cargo build --bin ...)")

    saas_port = _free_port()
    gateway_port = _free_port()
    saas_db = tmp_path / "saas.db"
    federation_key = _new_federation_key()
    saas_url = f"http://127.0.0.1:{saas_port}"
    pubkey_b64 = _make_federation_pubkey_b64(federation_key)
    relay_credential = "e2e-relay-shared-secret"
    gateway_url = f"http://127.0.0.1:{gateway_port}"

    saas_proc = _start_saas(tmp_path, saas_db, saas_port, federation_key)
    _wait_http(f"{saas_url}/healthz", timeout=60)

    # Provision org + instance(s) + service token in the real SaaS DB. All
    # instances share ONE org so a single org-scoped service token / routing
    # manifest covers the whole fleet.
    org = None
    service_tokens = {}
    instances = {}
    hosted_urls = {}
    for iid in instance_ids:
        mode = "relay" if iid == relay_instance_id else "hosted"
        oid, iid_ret, svc_token = _provision_org_instance(
            saas_db, f"fleet-e2e-{iid}@example.com", iid, mode=mode, org_id=org
        )
        org = org or oid
        service_tokens[iid] = svc_token
        port = _free_port()
        if mode == "relay":
            proc = _start_instance(
                tmp_path, saas_url, port, iid,
                relay_url=gateway_url,
                relay_credential=relay_credential,
            )
        else:
            proc = _start_instance(tmp_path, saas_url, port, iid)
            hosted_urls[iid] = f"http://127.0.0.1:{port}"
        instances[iid] = SimpleNamespace(proc=proc, port=port, mode=mode)
        _wait_http(f"http://127.0.0.1:{port}/healthz", timeout=120)
        # Give the control-plane sync time to distribute the federation keys.
        time.sleep(2)

    gateway_proc = _start_gateway(
        tmp_path, saas_url, gateway_port, hosted_urls, pubkey_b64,
        relay_credential=relay_credential,
    )
    _wait_http(f"{gateway_url}/healthz", timeout=60)
    # Give the relay-mode instance time to dial OUT and register its session.
    if relay_instance_id:
        _wait_for_relay_session(tmp_path, relay_instance_id, timeout=45)

    return SimpleNamespace(
        saas_url=saas_url,
        saas_port=saas_port,
        saas_db=saas_db,
        saas_proc=saas_proc,
        gateway_url=gateway_url,
        gateway_port=gateway_port,
        gateway_proc=gateway_proc,
        instances=instances,
        _tmp_path=tmp_path,
        _saas_url=saas_url,
        _relay_credential=relay_credential,
        org=org,
        service_tokens=service_tokens,
        relay_instance_id=relay_instance_id,
        federation_key=federation_key,
        pubkey_b64=pubkey_b64,
        relay_credential=relay_credential,
        internal_token="e2e-internal-token-0123456789abcdef",
        tmp_path=tmp_path,
    )


def _wait_for_relay_session(tmp_path, relay_instance_id, timeout=45):
    """Wait until the relay-mode instance's outbound session is established
    (the instance reconnects with backoff until the gateway accepts). Fails
    loudly — the relay leg is mandatory in this topology."""
    log = tmp_path / ("instance-%s.log" % relay_instance_id)
    deadline = time.time() + timeout
    while time.time() < deadline:
        if log.exists():
            text = log.read_text(errors="replace")
            if "relay session established" in text or "relay session accepted" in text:
                return
        time.sleep(0.5)
    tail = log.read_text(errors="replace")[-2000:] if log.exists() else "(no log)"
    raise RuntimeError(
        f"relay-mode instance {relay_instance_id} never established an outbound "
        f"session:\n{tail}"
    )


def restart_instance(topo, instance_id, timeout=120):
    """Stop a hosted instance process and boot it again with the SAME data dir
    + env (persistent embedded PostgreSQL), so the fleet connector can
    reconnect and resume after a source restart. Returns the new handle and
    updates `topo.instances`."""
    inst = topo.instances[instance_id]
    assert inst.mode == "hosted", "restart helper is for hosted instances"
    try:
        inst.proc.terminate()
    except Exception:  # noqa: BLE001
        pass
    inst.proc.wait(timeout=15)
    port = inst.port
    proc = _start_instance(topo._tmp_path, topo._saas_url, port, instance_id)
    topo.instances[instance_id] = SimpleNamespace(proc=proc, port=port, mode="hosted")
    _wait_http(f"http://127.0.0.1:{port}/healthz", timeout=timeout)
    # Control-plane sync + federation keys re-distribution.
    time.sleep(2)
    return topo.instances[instance_id]


def teardown(topo):
    procs = []
    if getattr(topo, "gateway_proc", None):
        procs.append(("gateway", topo.gateway_proc))
    for iid, inst in getattr(topo, "instances", {}).items():
        procs.append((f"instance-{iid}", inst.proc))
    if getattr(topo, "saas_proc", None):
        procs.append(("saas", topo.saas_proc))
    for name, proc in procs:
        try:
            proc.terminate()
        except Exception:  # noqa: BLE001
            pass
    for name, proc in procs:
        try:
            proc.wait(timeout=15)
        except Exception:  # noqa: BLE001
            proc.kill()
            proc.wait(timeout=5)