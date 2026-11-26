# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

import os
import ssl
import uuid
import json
import time
from typing import Generator, Optional
from dataclasses import dataclass

import pytest
import httpx
import websockets


# ---------------------------------------------------------------------------
# Dev mode detection
# ---------------------------------------------------------------------------

def _is_dev_mode(api_url: str, api_key: str) -> bool:
    """Detect if we're running against dev mode (no TLS, embedded PG)."""
    if api_url.startswith("http://"):
        return True
    if api_key and api_key.startswith("sk-dev-"):
        return True
    return False


# ---------------------------------------------------------------------------
# mTLS paths — overridable via environment
# ---------------------------------------------------------------------------

_CERT_DIR = os.path.join(os.path.dirname(__file__), "..", "certs", "client")
_DEFAULT_CLIENT_CERT = os.getenv("ACTAE_CLIENT_CERT", os.path.join(_CERT_DIR, "actae-test-client.crt"))
_DEFAULT_CLIENT_KEY = os.getenv("ACTAE_CLIENT_KEY", os.path.join(_CERT_DIR, "actae-test-client.key"))
_DEFAULT_CA_CERT = os.getenv("ACTAE_CA_CERT", os.path.join(os.path.dirname(__file__), "..", "certs", "ca.crt"))


def _build_ssl_context(cert_path: str, key_path: str, ca_cert_path: str) -> ssl.SSLContext:
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.load_cert_chain(cert_path, key_path)
    ctx.load_verify_locations(ca_cert_path)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


# ---------------------------------------------------------------------------
# Configuration — all overridable via environment variables
# ---------------------------------------------------------------------------

@dataclass
class ActaeConfig:
    api_url: str
    ws_url: str
    api_key: str
    postgres_dsn: str
    health_timeout_seconds: int
    client_cert_path: str
    client_key_path: str
    ca_cert_path: str
    dev_mode: bool = False

    @property
    def api_headers(self) -> dict:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    @property
    def ssl_context(self) -> Optional[ssl.SSLContext]:
        if self.dev_mode:
            return None
        return _build_ssl_context(
            self.client_cert_path,
            self.client_key_path,
            self.ca_cert_path,
        )


def pytest_addoption(parser):
    parser.addoption(
        "--api-url",
        default=os.getenv("ACTAE_API_URL", "https://localhost:8002"),
        help="Actae HTTP API base URL",
    )
    parser.addoption(
        "--ws-url",
        default=os.getenv("ACTAE_WS_URL", "wss://localhost:8002"),
        help="Actae WebSocket URL",
    )
    parser.addoption(
        "--api-key",
        default=os.getenv("ACTAE_API_KEY", "test-api-key-for-integration-tests"),
        help="X-API-Key header value",
    )
    parser.addoption(
        "--pg-dsn",
        default=os.getenv(
            "ACTAE_PG_DSN",
            "postgresql://actae:actae_dev_password@localhost:5432/actae",
        ),
        help="PostgreSQL DSN for direct DB verification",
    )
    parser.addoption(
        "--client-cert",
        default=_DEFAULT_CLIENT_CERT,
        help="Client TLS certificate path for mTLS",
    )
    parser.addoption(
        "--client-key",
        default=_DEFAULT_CLIENT_KEY,
        help="Client TLS private key path for mTLS",
    )
    parser.addoption(
        "--ca-cert",
        default=_DEFAULT_CA_CERT,
        help="CA certificate path for TLS verification",
    )
    parser.addoption(
        "--embedded",
        action="store_true",
        default=False,
        help="Managed-storage dev instance: no TLS, no mTLS, dev API key defaults",
    )


@pytest.fixture(scope="session")
def actae_config(request) -> ActaeConfig:
    dev_forced = request.config.getoption("--embedded")
    api_url = request.config.getoption("--api-url").rstrip("/")
    api_key = request.config.getoption("--api-key")
    dev_detected = _is_dev_mode(api_url, api_key)

    return ActaeConfig(
        api_url=api_url,
        ws_url=request.config.getoption("--ws-url").rstrip("/"),
        api_key=api_key,
        postgres_dsn=request.config.getoption("--pg-dsn") or "",
        health_timeout_seconds=30,
        client_cert_path=request.config.getoption("--client-cert"),
        client_key_path=request.config.getoption("--client-key"),
        ca_cert_path=request.config.getoption("--ca-cert"),
        dev_mode=dev_forced or dev_detected,
    )


# ---------------------------------------------------------------------------
# HTTP client — adapts to dev or prod mode
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def http_client(actae_config) -> httpx.Client:
    if actae_config.dev_mode:
        # Dev mode: plain HTTP, no TLS/mTLS
        return httpx.Client(
            timeout=httpx.Timeout(30.0, connect=10.0),
            limits=httpx.Limits(max_keepalive_connections=50, max_connections=200),
        )

    # Production mode: mTLS (client cert + CA are baked into the context —
    # httpx `verify=` accepts a context directly; `cert=` is deprecated).
    ssl_ctx = actae_config.ssl_context
    return httpx.Client(
        verify=ssl_ctx,
        timeout=httpx.Timeout(30.0, connect=10.0),
        limits=httpx.Limits(max_keepalive_connections=50, max_connections=200),
    )


# ---------------------------------------------------------------------------
# Dev mode fixture — tests can use this to skip features not available in dev
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def dev_mode(actae_config) -> bool:
    return actae_config.dev_mode


# ---------------------------------------------------------------------------
# Health check — verify server is alive before any tests
# ---------------------------------------------------------------------------

def pytest_sessionstart(session):
    config = session.config
    dev_forced = config.getoption("--embedded")
    api_url = config.getoption("--api-url").rstrip("/")
    api_key = config.getoption("--api-key")
    is_dev = dev_forced or _is_dev_mode(api_url, api_key)

    # The control-plane conformance suite (ACTAE_CONFORMANCE=1), the dev-mode
    # lifecycle suite (ACTAE_LIFECYCLE=1), and the static API-contract suite
    # (ACTAE_CONTRACT=1) are self-contained — they do not need (and must not
    # require) an externally running server.
    if (
        os.environ.get("ACTAE_CONFORMANCE") == "1"
        or os.environ.get("ACTAE_LIFECYCLE") == "1"
        or os.environ.get("ACTAE_SELF_HOSTED_AUTH") == "1"
        or os.environ.get("ACTAE_SERVER_CRASH") == "1"
        or os.environ.get("ACTAE_CONTRACT") == "1"
        or os.environ.get("ACTAE_E2E") == "1"
    ):
        print("Self-contained suite: skipping session server health check")
        return

    timeout = 30
    deadline = time.monotonic() + timeout
    last_error = None

    client_kwargs = {"timeout": httpx.Timeout(5.0)}

    if not is_dev:
        client_cert = config.getoption("--client-cert")
        client_key = config.getoption("--client-key")
        ca_cert = config.getoption("--ca-cert")
        ssl_ctx = _build_ssl_context(client_cert, client_key, ca_cert)
        client_kwargs["verify"] = ssl_ctx

    with httpx.Client(**client_kwargs) as hc:
        while time.monotonic() < deadline:
            try:
                resp = hc.get(f"{api_url}/healthz", headers={"Content-Type": "application/json"})
                if resp.status_code == 200:
                    body = resp.json()
                    if body.get("status") == "ok":
                        mode = "dev" if is_dev else "production"
                        print(f"Server healthy ({mode} mode) at {api_url}")
                        return
            except Exception as exc:
                last_error = exc
            time.sleep(1)

    raise RuntimeError(
        f"Server at {api_url} did not become healthy within {timeout}s "
        f"(dev_mode={is_dev}): {last_error}"
    )


# ---------------------------------------------------------------------------
# Unique channel per test
# ---------------------------------------------------------------------------

@pytest.fixture
def channel_id() -> Generator[str, None, None]:
    ch = f"test-{uuid.uuid4().hex[:12]}"
    yield ch


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

class ActaeClient:
    def __init__(self, http_client: httpx.Client, config: ActaeConfig):
        self._http = http_client
        self._cfg = config

    def record(
        self,
        channel_id: str,
        event_type: str,
        payload: dict,
        actor: str,
    ) -> dict:
        body = {
            "channel_id": channel_id,
            "event_type": event_type,
            "payload": payload,
            "metadata": {"actor": actor},
        }
        headers = self._cfg.api_headers
        resp = self._http.post(
            f"{self._cfg.api_url}/api/v1/events/record",
            json=body,
            headers=headers,
        )
        resp.raise_for_status()
        return resp.json()

    def replay(
        self,
        channel_id: str,
        cursor: int = 0,
        limit: int = 100,
        event_type: Optional[str] = None,
    ) -> list[dict]:
        params = {"cursor": str(cursor), "limit": str(limit)}
        if event_type is not None:
            params["event_type"] = event_type
        headers = self._cfg.api_headers
        resp = self._http.get(
            f"{self._cfg.api_url}/api/v1/events/replay/{channel_id}",
            params=params,
            headers=headers,
        )
        resp.raise_for_status()
        return resp.json().get("events", [])

    def query(self, **filters) -> list[dict]:
        param_map = {
            "channel_ids": "channel_ids",
            "event_type": "event_type",
            "actor": "actor",
            "cursor_start": "cursor_start",
            "cursor_end": "cursor_end",
            "from_time": "from",
            "to_time": "to",
            "limit": "limit",
            "offset": "offset",
        }
        body = {}
        for k, v in filters.items():
            api_key = param_map.get(k, k)
            if v is not None:
                body[api_key] = v
        headers = self._cfg.api_headers
        resp = self._http.post(
            f"{self._cfg.api_url}/api/v1/events/query",
            json=body,
            headers=headers,
        )
        resp.raise_for_status()
        return resp.json().get("events", [])

    def cursor(self, channel_id: str) -> Optional[int]:
        params = {}
        headers = self._cfg.api_headers
        resp = self._http.get(
            f"{self._cfg.api_url}/api/v1/events/cursor/{channel_id}",
            params=params,
            headers=headers,
        )
        resp.raise_for_status()
        return resp.json().get("latest_cursor")

    def raw_post(self, path: str, body: dict = None, headers: dict = None) -> httpx.Response:
        h = {**self._cfg.api_headers, **(headers or {})}
        return self._http.post(
            f"{self._cfg.api_url}{path}",
            json=body or {},
            headers=h,
        )

    def raw_get(self, path: str, params: dict = None, headers: dict = None) -> httpx.Response:
        h = {**self._cfg.api_headers, **(headers or {})}
        # `params or None` preserves query strings already embedded in `path`
        # (httpx merges path-query with `params`; an empty dict would discard it).
        return self._http.get(
            f"{self._cfg.api_url}{path}",
            params=params or None,
            headers=h,
        )

    def raw_put(self, path: str, body: dict = None, headers: dict = None) -> httpx.Response:
        h = {**self._cfg.api_headers, **(headers or {})}
        return self._http.put(
            f"{self._cfg.api_url}{path}",
            json=body or {},
            headers=h,
        )

    def raw_patch(self, path: str, body: dict = None, headers: dict = None) -> httpx.Response:
        h = {**self._cfg.api_headers, **(headers or {})}
        return self._http.patch(
            f"{self._cfg.api_url}{path}",
            json=body or {},
            headers=h,
        )

    def raw_delete(self, path: str, headers: dict = None, params: dict = None) -> httpx.Response:
        h = {**self._cfg.api_headers, **(headers or {})}
        return self._http.delete(
            f"{self._cfg.api_url}{path}",
            headers=h,
            params=params or {},
        )

    def save_state(self, channel_id: str, cursor: int, state) -> dict:
        return self.raw_post(
            f"/api/v1/state/{channel_id}",
            {"cursor": cursor, "state": state},
        ).json()

    def load_state(self, channel_id: str) -> dict:
        return self.raw_get(f"/api/v1/state/{channel_id}").json()

    def list_states(self, channel_id: str, limit: int = 100, offset: int = 0) -> dict:
        return self.raw_get(
            f"/api/v1/state/{channel_id}/versions",
            params={"limit": str(limit), "offset": str(offset)},
        ).json()

    def load_state_version(self, channel_id: str, version: int) -> dict:
        return self.raw_get(
            f"/api/v1/state/{channel_id}/version/{version}"
        ).json()

    def delete_state_version(self, channel_id: str, version: int) -> httpx.Response:
        return self.raw_delete(f"/api/v1/state/{channel_id}/version/{version}")

    def healthz(self) -> dict:
        return self.raw_get("/healthz").json()

    def readyz(self) -> dict:
        return self.raw_get("/readyz").json()

    def metrics_json(self) -> dict:
        return self.raw_get("/metrics.json").json()


@pytest.fixture
def actae(http_client, actae_config) -> ActaeClient:
    return ActaeClient(http_client, actae_config)


# ---------------------------------------------------------------------------
# WebSocket helpers (adapts TLS for dev mode)
# ---------------------------------------------------------------------------

class ActaeWebSocket:
    def __init__(self, config: ActaeConfig):
        self._cfg = config
        self._ws = None

    async def __aenter__(self):
        use_ssl = self._cfg.ws_url.startswith("wss://")
        kwargs = {
            "ping_interval": None,
            "close_timeout": 10,
        }
        if use_ssl and not self._cfg.dev_mode:
            kwargs["ssl"] = self._cfg.ssl_context
        elif use_ssl and self._cfg.dev_mode:
            # Dev mode with WSS: create a permissive context
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            kwargs["ssl"] = ctx

        self._ws = await websockets.connect(
            self._ws_endpoint(),
            **kwargs,
        )
        welcome = json.loads(await self._ws.recv())
        assert "Connection" in welcome, f"Expected welcome, got {welcome}"
        return self

    def _ws_endpoint(self) -> str:
        """The WebSocket upgrade endpoint. Actae serves WS at `/ws`; append
        the path when the configured URL is a bare origin."""
        url = self._cfg.ws_url.rstrip("/")
        if url.endswith("/ws"):
            return url
        return f"{url}/ws"

    async def __aexit__(self, *exc):
        if self._ws:
            await self._ws.close()
            self._ws = None

    async def subscribe(self, topic: str, cursor: Optional[int] = None) -> dict:
        msg = {"type": "subscribe", "topic": topic}
        if cursor is not None:
            msg["cursor"] = cursor
        await self._ws.send(json.dumps(msg))
        return json.loads(await self._ws.recv())

    async def unsubscribe(self, topic: str) -> dict:
        await self._ws.send(json.dumps({"type": "unsubscribe", "topic": topic}))
        return json.loads(await self._ws.recv())

    async def send_auth(self, token: str = None, api_key: str = None) -> dict:
        msg = {"type": "auth", "payload": {}}
        if token:
            msg["token"] = token
        if api_key:
            msg["api_key"] = api_key
        await self._ws.send(json.dumps(msg))
        return json.loads(await self._ws.recv())

    async def send_ping(self) -> dict:
        await self._ws.send(json.dumps({"type": "connection", "event": "ping", "payload": {}}))
        return json.loads(await self._ws.recv())

    async def send_raw(self, text: str) -> dict:
        await self._ws.send(text)
        return json.loads(await self._ws.recv())

    async def authenticate(self, api_key: str) -> dict:
        return await self.send_auth(api_key=api_key)

    async def recv(self, timeout: float = 5.0) -> dict:
        import asyncio
        return json.loads(
            await asyncio.wait_for(self._ws.recv(), timeout=timeout)
        )

    async def recv_all(self, count: int, timeout: float = 10.0) -> list[dict]:
        import asyncio
        messages = []
        deadline = time.monotonic() + timeout
        while len(messages) < count and time.monotonic() < deadline:
            remaining = max(0.1, deadline - time.monotonic())
            try:
                msg = await asyncio.wait_for(self._ws.recv(), timeout=remaining)
                messages.append(json.loads(msg))
            except asyncio.TimeoutError:
                break
        return messages


@pytest.fixture
def ws(actae_config):
    class WsFactory:
        @property
        def url(self) -> str:
            """The WS upgrade endpoint (``ws(s)://host[:port]/ws``)."""
            url = actae_config.ws_url.rstrip("/")
            return url if url.endswith("/ws") else f"{url}/ws"

        @property
        def ssl(self) -> Optional[ssl.SSLContext]:
            """TLS context for direct websocket connections in tests."""
            return actae_config.ssl_context

        @staticmethod
        async def connect() -> ActaeWebSocket:
            return ActaeWebSocket(actae_config)

    return WsFactory()


# ---------------------------------------------------------------------------
# Postgres direct access (for verifications) — skipped in dev mode
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def pg_cursor(actae_config):
    if actae_config.dev_mode or not actae_config.postgres_dsn:
        pytest.skip("Direct Postgres access not available in dev mode")
    import psycopg2
    conn = psycopg2.connect(actae_config.postgres_dsn)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            yield cur
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Unique channel name per test function
# ---------------------------------------------------------------------------

@pytest.fixture
def ch_name(request) -> str:
    test_name = request.node.name.replace("[", "-").replace("]", "")
    return f"ch-{test_name[:48]}-{uuid.uuid4().hex[:8]}"
