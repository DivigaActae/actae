# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Embedded-mode lifecycle tests — managed PostgreSQL process management.

Spawns dedicated managed-storage `actae` instances (the SAME code path as
production) and verifies the production-grade invariants of the managed
PostgreSQL child:

1. Data survives graceful restarts (SIGINT / SIGTERM).
2. A SIGKILLed Actae leaves an orphaned postgres that the next start claims
   (only after proving ownership via postmaster.pid + `pg_ctl status`) and
   reuses (data intact).
3. Starting a second instance while one runs bumps to the next free HTTP
   port, pairs it with a distinct pg port and data directory, and never
   touches the first instance's postgres (isolation).
4. `ACTAE_EMBEDDED_PG_WIPE` recreates the database from scratch.
5. The data-dir lock fails a concurrent start of the same identity (with
   `ACTAE_AUTO_BUMP=0`) instead of racing for the pg port.
6. A PG major-version mismatch fails with an actionable wipe hint.
7. Unix sockets are disabled: no `.s.PGSQL.*` artifacts anywhere.
8. The generated postgres password is printed exactly once (first boot only),
   the persisted credential is authoritative, and a conflicting
   `ACTAE_EMBEDDED_PG_PASSWORD` is a loud, actionable error.
9. `actae doctor` lists running instances via the data-dir convention.

The lock test uses Unix `fcntl` and is skipped on Windows (the fs2 lock
itself is cross-platform; only the test harness is not).

Opt-in: set ACTAE_LIFECYCLE=1 (like the conformance suite). Needs the
`actae` binary (ACTAE_BIN, default `actae/target/debug/actae`); the embedded
PostgreSQL binaries are auto-downloaded on first managed-storage start (or reuse
the pg-embed cache via ACTAE_PG_BIN_DIR when present).
"""

import json
import os
import shutil
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

# Each lifecycle test boots a full embedded PostgreSQL (cold CI runners and
# Windows Defender scans make first startup slow); the pytest.ini global 60s
# timeout kills fixture setup long before the 120s health wait fires.
pytestmark = pytest.mark.timeout(300)
REPO_ROOT = Path(__file__).resolve().parent.parent

SKIP_REASON = (
    "opt-in embedded-lifecycle suite (set ACTAE_LIFECYCLE=1; needs an `actae` binary)"
)

API_KEY = "sk-dev-0000000000000000000000"


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
    the same zonky package on every fresh data dir). ACTAE_PG_BIN_DIR may
    point at a zonky tree root (bin/lib/share — the fetch-pg-binaries.sh
    layout) or directly at the bin dir; both are normalized to the bin dir."""
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


def _free_port_pair():
    """Two consecutive free ports (the second instance bumps by one)."""
    for _ in range(200):
        base = _free_port()
        if base + 1 > 65535:
            continue
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", base + 1))
                return base, base + 1
            except OSError:
                continue
    raise RuntimeError("could not find two consecutive free ports")


def _pg_port_pair(http_a, http_b):
    """Two consecutive free pg ports that cannot collide with the http ports."""
    used = {http_a, http_b}
    for _ in range(200):
        base = _free_port()
        if base + 1 > 65535:
            continue
        if base in used or base + 1 in used:
            continue
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", base + 1))
                return base, base + 1
            except OSError:
                continue
    raise RuntimeError("could not find two consecutive free pg ports")


class EmbeddedInstance:
    """A spawned managed-storage `actae` instance with a deterministic identity."""

    def __init__(self, http_port, pg_port, data_dir, env):
        self.http_port = http_port
        self.pg_port = pg_port
        self.data_dir = data_dir
        self.env = env
        self.proc = None
        self.log_file = None

    @property
    def url(self):
        return f"http://127.0.0.1:{self.http_port}"

    def start(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = self.data_dir / "actae.log"
        # On Windows the child must live in its own console process group so
        # a Ctrl+C delivered to it does not also interrupt the test runner.
        creationflags = (
            subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
        )
        self.proc = subprocess.Popen(
            [str(_actae_bin())],
            stdout=open(self.log_file, "w"),
            stderr=subprocess.STDOUT,
            text=True,
            env=self.env,
            creationflags=creationflags,
        )
        try:
            wait_for_health(self.url, timeout=120)
        except AssertionError:
            # Surface the child's own output — without it a Windows/CI
            # startup failure is undebuggable from CI logs alone.
            tail = ""
            if self.log_file is not None and self.log_file.exists():
                try:
                    lines = self.log_file.read_text(errors="replace").splitlines()
                    tail = "\n".join(lines[-40:])
                except OSError:
                    pass
            raise AssertionError(
                f"embedded actae at {self.url} not healthy after 120s\n"
                f"--- actae.log (tail) ---\n{tail or '(no log output)'}"
            ) from None
        # The instance may have auto-bumped to a different port; re-read the
        # actual identity from its url file.
        self._reload_identity()

    def _reload_identity(self):
        info_file = self.data_dir / "url"
        if info_file.exists():
            try:
                info = json.loads(info_file.read_text())
                self.http_port = int(info["url"].rsplit(":", 1)[1])
                self.pg_port = int(info["pg_port"])
            except Exception:
                pass

    def proc_stdout(self):
        if not self.log_file or not self.log_file.exists():
            return ""
        return self.log_file.read_text()

    def stop_graceful(self):
        # Windows: SIGTERM is a hard TerminateProcess, so use the console
        # Ctrl+C event (actae's graceful-shutdown path) via the child's own
        # process group. Unix: plain SIGINT.
        if sys.platform == "win32":
            self.proc.send_signal(signal.CTRL_C_EVENT)
        else:
            self.proc.send_signal(signal.SIGINT)
        self.proc.wait(timeout=40)

    def kill_hard(self):
        self.proc.kill()
        self.proc.wait(timeout=10)

    def cleanup(self):
        if self.proc and self.proc.poll() is None:
            try:
                self.stop_graceful()
            except Exception:
                self.kill_hard()

    def _auth(self):
        return {"X-API-Key": API_KEY}

    def channels(self):
        resp = httpx.get(f"{self.url}/api/v1/channels", headers=self._auth())
        resp.raise_for_status()
        return resp.json()["channels"]

    def record(self, channel_id):
        resp = httpx.post(
            f"{self.url}/api/v1/events/record",
            headers={**self._auth(), "Content-Type": "application/json"},
            json={
                "channel_id": channel_id,
                "type": "agent.started",
                "payload": {"lifecycle": True},
                "metadata": {"actor": "lifecycle-test"},
            },
        )
        resp.raise_for_status()
        return resp.json()["event"]


def wait_for_health(url, timeout=120):
    deadline = time.monotonic() + timeout
    last_error = None
    with httpx.Client(timeout=2.0) as client:
        while time.monotonic() < deadline:
            try:
                resp = client.get(f"{url}/healthz")
                if resp.status_code == 200 and resp.json().get("status") == "ok":
                    return
            except Exception as exc:  # noqa: BLE001 - connection refused etc.
                last_error = exc
            time.sleep(0.5)
    raise AssertionError(f"server at {url} did not become healthy: {last_error}")


def _instance_env(http_port, pg_port, data_dir):
    env = dict(os.environ)
    for key in (
        "ACTAE_PORT", "ACTAE_HOST", "ACTAE_DATA_DIR", "ACTAE_EMBEDDED_PG_PORT",
        "ACTAE_EMBEDDED_PG_PASSWORD", "ACTAE_EMBEDDED_PG_WIPE", "ACTAE_AUTO_BUMP",
        "ACTAE_PG_NO_HOST", "ACTAE_PG_BIN_DIR", "ACTAE_CONFIG_DIR",
        "ACTAE_CONTROL_PLANE_URL", "ACTAE_INSTALL_ID", "ACTAE_ENROLL_SECRET",
        "ACTAE_ENROLL_KEY",
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
    # Hermetic config dir: without this, a developer's real
    # `~/.actae/enrollment.json` leaks into the spawned instance and silently
    # enrolls it, defeating the standalone expectation. Persists across a
    # restart within one test (same data_dir), so the JWT secret is stable.
    env["ACTAE_CONFIG_DIR"] = str(data_dir / "cfg")
    env["ACTAE_INITIAL_API_KEY"] = API_KEY
    env["ACTAE_NO_BROWSER"] = "1"
    bin_dir = _pg_bin_dir()
    if bin_dir is not None:
        env["ACTAE_PG_BIN_DIR"] = str(bin_dir)
    return env


def _pytest_skip_unless_available():
    if os.environ.get("ACTAE_LIFECYCLE") != "1":
        pytest.skip(SKIP_REASON)
    if not _actae_bin().is_file():
        pytest.skip("actae binary not found — build with `cd actae && cargo build` first")


@pytest.fixture
def embedded_instance(tmp_path):
    _pytest_skip_unless_available()
    http_port, _ = _free_port_pair()
    pg_port = _free_port()
    data_dir = tmp_path / "actae-embedded"
    instance = EmbeddedInstance(
        http_port, pg_port, data_dir, _instance_env(http_port, pg_port, data_dir)
    )
    instance.start()
    try:
        yield instance
    finally:
        instance.cleanup()


class TestEmbeddedLifecycle:
    def test_graceful_restart_preserves_data(self, embedded_instance):
        """SIGINT shuts down the embedded postgres; a restart reuses the
        cluster and all recorded events survive."""
        first = embedded_instance.record(f"ch-{uuid.uuid4().hex[:8]}")
        embedded_instance.stop_graceful()
        assert embedded_instance.proc.returncode == 0, "graceful shutdown must exit 0"

        embedded_instance.start()
        assert first["channel_id"] in embedded_instance.channels()
        events = httpx.get(
            f"{embedded_instance.url}/api/v1/events/replay/{first['channel_id']}",
            headers=embedded_instance._auth(),
        ).json()["events"]
        assert [e["id"] for e in events] == [first["id"]]

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="on Windows SIGTERM is TerminateProcess (a hard kill), not a \
                graceful signal — actae's Windows graceful path is Ctrl+C, \
                covered by the SIGINT restart test",
    )
    def test_sigterm_restart_preserves_data(self, embedded_instance):
        """`kill` (SIGTERM) is a graceful shutdown, not a hard kill."""
        first = embedded_instance.record(f"ch-{uuid.uuid4().hex[:8]}")
        embedded_instance.proc.send_signal(signal.SIGTERM)
        embedded_instance.proc.wait(timeout=40)
        assert embedded_instance.proc.returncode == 0, "SIGTERM shutdown must exit 0"

        embedded_instance.start()
        assert first["channel_id"] in embedded_instance.channels()

    def test_embedded_pg_wipe_starts_fresh(self, embedded_instance):
        """ACTAE_EMBEDDED_PG_WIPE=1 recreates the database from scratch."""
        first = embedded_instance.record(f"ch-{uuid.uuid4().hex[:8]}")
        embedded_instance.stop_graceful()

        env = _instance_env(
            embedded_instance.http_port,
            embedded_instance.pg_port,
            embedded_instance.data_dir,
        )
        env["ACTAE_EMBEDDED_PG_WIPE"] = "1"
        embedded_instance.env = env
        embedded_instance.start()
        assert first["channel_id"] not in embedded_instance.channels()

    def test_sigkill_orphan_recovery(self, embedded_instance):
        """A SIGKILLed Actae leaves an orphaned postgres; the next start
        claims it (never a foreign process) and the data survives."""
        first = embedded_instance.record(f"ch-{uuid.uuid4().hex[:8]}")
        embedded_instance.kill_hard()
        time.sleep(2)

        # Orphan check: a postgres must still be listening on the pg port.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            assert s.connect_ex(("127.0.0.1", embedded_instance.pg_port)) == 0, (
                "expected an orphaned postgres after SIGKILL"
            )

        embedded_instance.start()
        assert first["channel_id"] in embedded_instance.channels()

    def test_second_instance_bumps_port_and_isolates(self, tmp_path):
        """A second managed-storage instance with the same configuration bumps to
        the next free HTTP port, pairs it with a distinct pg port + data dir,
        and never touches the first instance's postgres."""
        # Direct EmbeddedInstance use: still opt-in (no embedded_instance fixture).
        _pytest_skip_unless_available()
        http_a, http_b = _free_port_pair()
        pg_a, pg_b = _pg_port_pair(http_a, http_b)
        dir_a = tmp_path / "actae-embedded"
        dir_b = Path(f"{dir_a}-{http_b}")

        a = EmbeddedInstance(http_a, pg_a, dir_a, _instance_env(http_a, pg_a, dir_a))
        b = EmbeddedInstance(http_b, pg_b, dir_b, _instance_env(http_a, pg_a, dir_a))
        a.start()
        try:
            b.start()
            # B bumps to the next free http port and pairs it with the next
            # pg port and its own data dir (verified from B's url file).
            assert b.http_port == http_b, "second instance must bump to the next port"
            assert b.pg_port == pg_a + 1, "pg port must move in lockstep"
            assert os.path.isdir(dir_b), "bumped instance must use its own data dir"

            # Isolation: an event recorded on B is invisible to A.
            channel_b = f"ch-{uuid.uuid4().hex[:8]}"
            b.record(channel_b)
            assert channel_b in b.channels()
            assert channel_b not in a.channels()

            # A's postgres was never touched and A keeps serving normally.
            channel_a = f"ch-{uuid.uuid4().hex[:8]}"
            a.record(channel_a)
            assert channel_a in a.channels()
            assert channel_a not in b.channels()
        finally:
            b.cleanup()
            a.cleanup()

    def test_healthz_reports_embedded_mode(self, embedded_instance):
        resp = httpx.get(f"{embedded_instance.url}/healthz")
        resp.raise_for_status()
        body = resp.json()
        assert body["mode"] == "embedded"
        assert body["embedded_pg"] is True

    def test_banner_and_url_file(self, embedded_instance):
        """The instance writes its identity to <data-dir>/url and the banner
        prints the listening port."""
        info = json.loads((embedded_instance.data_dir / "url").read_text())
        assert info["url"] == f"http://127.0.0.1:{embedded_instance.http_port}"
        assert info["pg_port"] == embedded_instance.pg_port
        assert "managed storage" in embedded_instance.proc_stdout()

    def test_no_unix_sockets_anywhere(self, embedded_instance):
        """Unix sockets are disabled entirely: no .s.PGSQL.* artifact in the
        OS temp dir (the stale-socket class of bugs) and none under the data
        dir either."""
        port = embedded_instance.pg_port
        for name in (f".s.PGSQL.{port}", f".s.PGSQL.{port}.lock"):
            tmp_artifact = Path(tempfile.gettempdir()) / name
            assert not tmp_artifact.exists(), (
                f"embedded PG must not create {name} in the shared temp dir"
            )
        assert not list(
            (embedded_instance.data_dir / "pg").glob(".s.PGSQL.*")
        ), "embedded PG must not create unix sockets under the cluster"

    def test_uses_host_postgres_instead_of_download(self, tmp_path):
        """A host-installed PostgreSQL on PATH is adopted for the managed
        cluster instead of downloading — the exact 'my machine already has
        PostgreSQL' scenario. The existing pg binaries are re-used as a fake
        host install on PATH; ACTAE_PG_BIN_DIR is unset so host detection is
        what kicks in (never a download). A graceful restart must keep using
        the matching host PG and preserve data."""
        # Direct EmbeddedInstance use: still opt-in (no embedded_instance fixture).
        _pytest_skip_unless_available()
        bin_dir = _pg_bin_dir()
        if bin_dir is None:
            pytest.skip("no pg binaries available to simulate a host install")
        host_bin = Path(bin_dir)
        if not (host_bin.parent / "lib").is_dir():
            pytest.skip("pg tree lacks lib/ — need the full zonky tree to simulate a host install")

        http_port, _ = _free_port_pair()
        pg_port = _free_port()
        data_dir = tmp_path / "actae-embedded"
        env = _instance_env(http_port, pg_port, data_dir)
        env.pop("ACTAE_PG_BIN_DIR", None)  # host detection must kick in, not the override
        env["PATH"] = str(host_bin) + os.pathsep + env.get("PATH", "")
        instance = EmbeddedInstance(http_port, pg_port, data_dir, env)
        instance.start()
        try:
            log = instance.proc_stdout()
            assert "Using host PostgreSQL from" in log, "host PG was not adopted"
            assert "Downloading PostgreSQL" not in log, "must not download when a host PG exists"
            channel = f"ch-{uuid.uuid4().hex[:8]}"
            instance.record(channel)

            # Restart with the same data dir: the existing cluster's major
            # must match the host PG, so the same host PG is used again and
            # the recorded event survives.
            instance.stop_graceful()
            instance.start()
            assert "Using host PostgreSQL from" in instance.proc_stdout()
            assert "Downloading PostgreSQL" not in instance.proc_stdout()
            assert channel in instance.channels()
        finally:
            instance.cleanup()

    def test_uses_host_postgres_via_pg_config(self, tmp_path):
        """The `pg_config --bindir` tier: a pg_config on PATH reporting a
        real PostgreSQL bin dir must be parsed and adopted. Deterministic
        everywhere (a shim stands in for the real pg_config binary)."""
        # Direct EmbeddedInstance use: still opt-in (no embedded_instance fixture).
        _pytest_skip_unless_available()
        bin_dir = _pg_bin_dir()
        if bin_dir is None:
            pytest.skip("no pg binaries available to simulate a host install")
        host_bin = Path(bin_dir)
        if not (host_bin.parent / "lib").is_dir():
            pytest.skip("pg tree lacks lib/ — need the full zonky tree to simulate a host install")

        shim_dir = tmp_path / "pgconfig-shim"
        shim_dir.mkdir()
        pg_config_shim = shim_dir / "pg_config"
        pg_config_shim.write_text(f"#!/bin/sh\necho {host_bin}\n")
        pg_config_shim.chmod(0o755)

        http_port, _ = _free_port_pair()
        pg_port = _free_port()
        data_dir = tmp_path / "actae-embedded"
        env = _instance_env(http_port, pg_port, data_dir)
        env.pop("ACTAE_PG_BIN_DIR", None)
        env["PATH"] = str(shim_dir) + os.pathsep + env.get("PATH", "")
        instance = EmbeddedInstance(http_port, pg_port, data_dir, env)
        instance.start()
        try:
            log = instance.proc_stdout()
            assert "Using host PostgreSQL from" in log, "pg_config tier did not adopt the host PG"
            assert "Downloading PostgreSQL" not in log
            channel = f"ch-{uuid.uuid4().hex[:8]}"
            instance.record(channel)
            assert channel in instance.channels()
        finally:
            instance.cleanup()

    def test_uses_real_host_postgres_if_available(self, tmp_path):
        """End-to-end against a REAL host PostgreSQL when one is installed
        (`pg_config` resolving to a usable initdb/postgres/pg_ctl trio).
        Skips on machines without one (e.g. CI). This is the definitive
        check that a standard Homebrew/apt/EDB install is detected."""
        # Direct EmbeddedInstance use: still opt-in (no embedded_instance fixture).
        _pytest_skip_unless_available()
        pg_config = shutil.which("pg_config")
        if not pg_config:
            pytest.skip("no pg_config on PATH — no real host PostgreSQL to verify")
        bindir = subprocess.run(
            [pg_config, "--bindir"], capture_output=True, text=True, timeout=30
        ).stdout.strip()
        host_bin = Path(bindir)
        if not all((host_bin / f).is_file() for f in ("initdb", "postgres", "pg_ctl")):
            pytest.skip(f"pg_config bindir {bindir} does not contain the server trio")

        http_port, _ = _free_port_pair()
        pg_port = _free_port()
        data_dir = tmp_path / "actae-embedded"
        env = _instance_env(http_port, pg_port, data_dir)
        env.pop("ACTAE_PG_BIN_DIR", None)
        # Ensure the real pg_config is reachable (keg-only installs link it
        # but the server binaries may live in the Cellar bin dir).
        env["PATH"] = str(host_bin) + os.pathsep + env.get("PATH", "")
        instance = EmbeddedInstance(http_port, pg_port, data_dir, env)
        instance.start()
        try:
            log = instance.proc_stdout()
            assert "Using host PostgreSQL from" in log, "real host PG was not adopted"
            assert "Downloading PostgreSQL" not in log
            channel = f"ch-{uuid.uuid4().hex[:8]}"
            instance.record(channel)
            assert channel in instance.channels()
        finally:
            instance.cleanup()

    def test_password_printed_only_on_first_boot(self, embedded_instance):
        """The generated postgres password is surfaced exactly once (first
        boot); a restart from the persisted pwfile must not leak it again.
        Note: `start()` truncates the log, so the post-restart file contains
        only the restart's output."""
        first_boot = embedded_instance.proc_stdout()
        assert first_boot.count("Postgres password:") == 1
        embedded_instance.stop_graceful()
        embedded_instance.start()
        assert "Postgres password:" not in embedded_instance.proc_stdout()

    def test_password_env_conflict_is_actionable(self, tmp_path):
        """Changing ACTAE_EMBEDDED_PG_PASSWORD on an existing cluster is a
        loud, actionable error — never a silent 60s timeout-and-fail."""
        # Direct EmbeddedInstance use: still opt-in (no embedded_instance fixture).
        _pytest_skip_unless_available()
        http_port, _ = _free_port_pair()
        pg_port = _free_port()
        data_dir = tmp_path / "actae-embedded"
        instance = EmbeddedInstance(
            http_port, pg_port, data_dir, _instance_env(http_port, pg_port, data_dir)
        )
        instance.start()
        instance.stop_graceful()

        bad_env = _instance_env(http_port, pg_port, data_dir)
        bad_env["ACTAE_EMBEDDED_PG_PASSWORD"] = "a-totally-different-password"
        proc = subprocess.Popen(
            [str(_actae_bin())],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=bad_env,
        )
        try:
            out, _ = proc.communicate(timeout=90)
            assert proc.returncode != 0
            assert "ACTAE_EMBEDDED_PG_PASSWORD" in out
            assert "ACTAE_EMBEDDED_PG_WIPE" in out
        finally:
            if proc.poll() is None:
                proc.kill()

    def test_password_env_matching_is_fine(self, tmp_path):
        """The persisted password is authoritative; re-supplying the SAME
        value is a normal restart."""
        # Direct EmbeddedInstance use: still opt-in (no embedded_instance fixture).
        _pytest_skip_unless_available()
        http_port, _ = _free_port_pair()
        pg_port = _free_port()
        data_dir = tmp_path / "actae-embedded"
        env = _instance_env(http_port, pg_port, data_dir)
        env["ACTAE_EMBEDDED_PG_PASSWORD"] = "operator-pinned-secret-123"
        instance = EmbeddedInstance(http_port, pg_port, data_dir, env)
        instance.start()
        first = instance.record(f"ch-{uuid.uuid4().hex[:8]}")
        instance.stop_graceful()

        same_env = _instance_env(http_port, pg_port, data_dir)
        same_env["ACTAE_EMBEDDED_PG_PASSWORD"] = "operator-pinned-secret-123"
        instance.env = same_env
        instance.start()
        assert first["channel_id"] in instance.channels()

    def test_version_mismatch_error_is_actionable(self, tmp_path):
        """A cluster initialized with a different major version fails with a
        wipe hint instead of a cryptic error."""
        # Direct EmbeddedInstance use: still opt-in (no embedded_instance fixture).
        _pytest_skip_unless_available()
        http_port, _ = _free_port_pair()
        pg_port = _free_port()
        data_dir = tmp_path / "actae-embedded"
        cluster = data_dir / "pg"
        cluster.mkdir(parents=True)
        (cluster / "PG_VERSION").write_text("17.9\n")

        env = _instance_env(http_port, pg_port, data_dir)
        proc = subprocess.Popen(
            [str(_actae_bin())],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )
        try:
            out, _ = proc.communicate(timeout=90)
            assert proc.returncode != 0
            assert "ACTAE_EMBEDDED_PG_WIPE" in out
        finally:
            if proc.poll() is None:
                proc.kill()

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="the lock is taken with Unix flock (fcntl); Windows has no fcntl \
                module — the fs2 lock itself is cross-platform, only this harness is not",
    )
    def test_second_start_same_identity_fails_cleanly(self, tmp_path):
        """With ACTAE_AUTO_BUMP=0, a concurrent start of the same identity
        fails fast with a clear error instead of racing for the pg port."""
        # Direct EmbeddedInstance use: still opt-in (no embedded_instance fixture).
        _pytest_skip_unless_available()
        http_port, _ = _free_port_pair()
        pg_port = _free_port()
        data_dir = tmp_path / "actae-embedded"
        data_dir.mkdir(parents=True)
        lock_path = data_dir / ".actae.lock"

        import fcntl

        with open(lock_path, "w") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            env = _instance_env(http_port, pg_port, data_dir)
            env["ACTAE_AUTO_BUMP"] = "0"
            proc = subprocess.Popen(
                [str(_actae_bin())],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
            )
            try:
                out, _ = proc.communicate(timeout=90)
                assert proc.returncode != 0
                assert "another Actae instance is already using data dir" in out
            finally:
                if proc.poll() is None:
                    proc.kill()

    def test_doctor_lists_running_embedded_instances(self, tmp_path):
        """`actae doctor` finds running instances via the ACTAE_DATA_DIR
        data-dir convention."""
        # Direct EmbeddedInstance use: still opt-in (no embedded_instance fixture).
        _pytest_skip_unless_available()
        base = tmp_path / "doctor-data"
        http_port, _ = _free_port_pair()
        pg_port, _ = _pg_port_pair(http_port, http_port + 1)
        env = _instance_env(http_port, pg_port, base)
        instance = EmbeddedInstance(http_port, pg_port, base, env)
        instance.start()
        try:
            result = subprocess.run(
                [str(_actae_bin()), "doctor"],
                capture_output=True,
                text=True,
                timeout=60,
                env=env,
            )
            assert instance.url in result.stdout
            assert "[up]" in result.stdout
        finally:
            instance.cleanup()


def _wait_for_log(instance, needle, timeout=20):
    """Poll the child's log until `needle` appears (the HTTP listener can
    accept healthz a moment before the startup banner is flushed to stdout)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if needle in instance.proc_stdout():
            return
        time.sleep(0.25)
    # Surface the tail so a missing banner is debuggable.
    tail = "\n".join(instance.proc_stdout().splitlines()[-40:])
    raise AssertionError(f"did not see {needle!r} in actae output\n--- tail ---\n{tail}")


class TestCommunityEditionFirstRun:
    """The free community build's first-run journey (the binary side of the
    `docs/EDITIONS.md` runbook): download → run → the banner's next steps →
    record/replay.

    The anonymous SaaS download channel itself (`GET /v1/community/*`) is
    covered by `saas/backend/tests/test_downloads.py::TestCommunityDownloads`;
    this suite proves the artifact a user downloads actually starts, prints the
    onboarding a first-time user needs (dashboard URL + the SDK installs, since
    a running server alone is useless), and serves a first event.
    """

    def test_banner_prints_the_sdk_next_steps(self, embedded_instance):
        _wait_for_log(embedded_instance, "Next steps")
        out = embedded_instance.proc_stdout()
        assert "Actae is fully operational" in out
        # The dashboard URL the user is told to open.
        assert f"Dashboard:   {embedded_instance.url}" in out
        # A running server alone is useless — the SDK installs must be shown.
        assert "pip install actae-client" in out
        assert "go get github.com/divigaactae/actae/sdks/go" in out
        assert "npm install @actae/sdk" in out
        assert "actae add" in out
        # The one-time API key is surfaced in the next-steps block.
        assert API_KEY in out
        # Standalone nudge + the optional, card-free account (unlinked
        # instances send nothing; linking is opt-in).
        assert "running standalone" in out
        assert "https://actae.dev/signup/" in out

    def test_first_event_round_trip(self, embedded_instance):
        """The quickstart's core loop against the downloaded artifact."""
        channel = f"quickstart-{uuid.uuid4().hex[:8]}"
        recorded = embedded_instance.record(channel)
        events = httpx.get(
            f"{embedded_instance.url}/api/v1/events/replay/{channel}",
            headers=embedded_instance._auth(),
        ).json()["events"]
        assert [e["id"] for e in events] == [recorded["id"]]
        # The API event DTO carries the type as `type` (see handlers/events.rs).
        assert events[0]["type"] == "agent.started"
        assert events[0]["payload"] == {"lifecycle": True}
        assert events[0]["actor"] == "lifecycle-test"
        # The channel is discoverable, so the dashboard would list the run.
        assert channel in embedded_instance.channels()


class TestLocalDashboardAutoLogin:
    """The local self-hosted dashboard never shows a login/setup form: a
    loopback browser gets an admin session with no credential. Opt-in with the
    rest of this suite."""

    def test_config_advertises_local_auto_login(self, embedded_instance):
        cfg = httpx.get(f"{embedded_instance.url}/api/v1/auth/config").json()
        assert cfg["sso"] is False
        assert cfg["local_auto_login"] is True

    def test_local_session_issues_an_admin_session(self, embedded_instance):
        resp = httpx.post(f"{embedded_instance.url}/api/v1/auth/local-session")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["token"]
        assert body["user"]["email"] == "admin@actae.local"
        assert body["user"]["role"] == "admin"

        # The session is a real dashboard session: /auth/me resolves it and it
        # grants full access to a normal API call.
        me = httpx.get(
            f"{embedded_instance.url}/api/v1/auth/me",
            headers={"Authorization": f"Bearer {body['token']}"},
        )
        assert me.status_code == 200
        assert me.json()["user"]["email"] == "admin@actae.local"
        channels = httpx.get(
            f"{embedded_instance.url}/api/v1/channels",
            headers={"Authorization": f"Bearer {body['token']}"},
        )
        assert channels.status_code == 200

        # First-run setup is retired — the operator is already signed in.
        cfg = httpx.get(f"{embedded_instance.url}/api/v1/auth/config").json()
        assert cfg["setup_required"] is False

    def test_local_session_refuses_a_non_loopback_host(self, embedded_instance):
        # DNS-rebinding guard: loopback peer but a public Host header.
        resp = httpx.post(
            f"{embedded_instance.url}/api/v1/auth/local-session",
            headers={"Host": "evil.example"},
        )
        assert resp.status_code == 403

    def test_local_session_can_be_disabled(self, tmp_path):
        _pytest_skip_unless_available()
        http_port, _ = _free_port_pair()
        pg_port = _free_port()
        data_dir = tmp_path / "actae-local-login-off"
        env = _instance_env(http_port, pg_port, data_dir)
        env["ACTAE_LOCAL_AUTO_LOGIN"] = "0"
        instance = EmbeddedInstance(http_port, pg_port, data_dir, env)
        instance.start()
        try:
            cfg = httpx.get(f"{instance.url}/api/v1/auth/config").json()
            assert cfg["local_auto_login"] is False
            resp = httpx.post(f"{instance.url}/api/v1/auth/local-session")
            assert resp.status_code == 403
        finally:
            instance.cleanup()


class TestClientCredentialsFile:
    """The zero-copy SDK onboarding path (`docs/CLIENT_CREDENTIALS.md`): the
    boot that mints a key publishes `~/.actae/client.json`, and `actae login`
    can (re)publish it. Proves the "run it and your SDKs are configured" claim
    against the real binary. Opt-in with the rest of this suite."""

    def _creds_path(self, instance):
        return instance.data_dir / "cfg" / "client.json"

    def test_boot_publishes_client_credentials(self, embedded_instance):
        # The banner (and the publish) run after healthz comes up; wait for it.
        _wait_for_log(embedded_instance, "SDK setup")
        path = self._creds_path(embedded_instance)
        assert path.is_file(), "first boot must publish the SDK credentials file"
        if sys.platform != "win32":
            assert oct(path.stat().st_mode & 0o777) == "0o600", "credentials must be owner-only"
        creds = json.loads(path.read_text())
        assert creds["endpoint"] == embedded_instance.url
        assert creds["api_key"] == API_KEY
        # The published key authenticates against the live server.
        resp = httpx.get(
            f"{embedded_instance.url}/api/v1/auth/capabilities",
            headers={"X-API-Key": creds["api_key"]},
        )
        assert resp.status_code == 200

    def test_login_local_mints_and_logout_removes(self, embedded_instance):
        _wait_for_log(embedded_instance, "SDK setup")
        path = self._creds_path(embedded_instance)
        # Remove the boot-published file, then re-establish it via `actae login`
        # (the recovery path) — no --url, so it auto-detects the local instance.
        path.unlink()
        env = _instance_env(
            embedded_instance.http_port, embedded_instance.pg_port, embedded_instance.data_dir
        )
        res = subprocess.run(
            [str(_actae_bin()), "login"],
            env=env,
            # Same cwd as the spawned server so a debug build's dotenv (.env)
            # JWT_SECRET is identical in both processes (release builds ignore .env).
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert res.returncode == 0, f"actae login failed:\n{res.stdout}\n{res.stderr}"
        assert path.is_file()
        creds = json.loads(path.read_text())
        assert creds["endpoint"] == embedded_instance.url
        assert creds["api_key"] != API_KEY  # a freshly minted key
        assert (
            httpx.get(
                f"{embedded_instance.url}/api/v1/auth/capabilities",
                headers={"X-API-Key": creds["api_key"]},
            ).status_code
            == 200
        )

        res = subprocess.run(
            [str(_actae_bin()), "logout"],
            env=env,
            # Same cwd as the spawned server so a debug build's dotenv (.env)
            # JWT_SECRET is identical in both processes (release builds ignore .env).
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert res.returncode == 0, res.stderr
        assert not path.exists(), "logout must remove the credentials file"

    def test_login_rotates_the_previous_key(self, embedded_instance):
        """Re-running `actae login` rotates: the replaced key is revoked, not
        left active forever (no key accumulation)."""
        _wait_for_log(embedded_instance, "SDK setup")
        path = self._creds_path(embedded_instance)
        first = json.loads(path.read_text())["api_key"]  # the boot key (API_KEY)

        env = _instance_env(
            embedded_instance.http_port, embedded_instance.pg_port, embedded_instance.data_dir
        )
        res = subprocess.run(
            [str(_actae_bin()), "login"],
            env=env,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert res.returncode == 0, f"{res.stdout}\n{res.stderr}"
        second = json.loads(path.read_text())["api_key"]
        assert second != first

        def capabilities(key):
            return httpx.get(
                f"{embedded_instance.url}/api/v1/auth/capabilities",
                headers={"X-API-Key": key},
            ).status_code

        assert capabilities(second) == 200
        assert capabilities(first) == 401, "the replaced key must be revoked"

    def test_sdk_authenticates_from_the_file_alone(self, embedded_instance, monkeypatch):
        """The zero-copy claim end-to-end: the real Python SDK, configured only
        by the file the binary wrote (no env credentials), records an event."""
        _wait_for_log(embedded_instance, "SDK setup")
        pytest.importorskip("actae_client")
        import asyncio

        from actae_client import ActaeClient

        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(embedded_instance.data_dir / "cfg"))
        for var in ("ACTAE_API_KEY", "ACTAE_URL", "ACTAE_ENDPOINT", "ACTAE_WS_URL"):
            monkeypatch.delenv(var, raising=False)

        channel = f"sdk-creds-{uuid.uuid4().hex[:8]}"

        async def _run():
            async with ActaeClient() as client:
                return await client.record(
                    channel, "sdk.credentials", {"ok": True}, actor="lifecycle-test"
                )

        event = asyncio.run(_run())
        assert event.channel_id == channel
        assert event.event_type == "sdk.credentials"
        assert event.payload == {"ok": True}
