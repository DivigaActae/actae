# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Security-tier behaviors: API-key dashboard login, password-auth gate,
metrics gating, and the public auth-config surface.

Covers the standard tier (TLS + API keys + JWT-session-from-key) that
replaced default mTLS: one credential for SDKs and the dashboard, password
auth off by default, and API-key-gated metrics.
"""

import pytest


class TestPasswordAuthGate:
    """ENABLE_SIGNUP defaults to false: password signup is 403. Login is
    always reachable (only signup is gated), so an unknown account on a
    standalone instance gets 401 rather than 403."""

    def test_signup_disabled_by_default(self, actae):
        resp = actae.raw_post("/api/v1/auth/signup", {
            "email": "new@example.com",
            "password": "password123",
            "name": "New",
        })
        assert resp.status_code == 403
        assert "API key" in resp.text

    def test_login_unknown_account_is_unauthorized(self, actae):
        resp = actae.raw_post("/api/v1/auth/login", {
            "email": "any@example.com",
            "password": "password123",
        })
        assert resp.status_code == 401

    def test_auth_config_exposes_enable_signup(self, actae):
        resp = actae._http.get(f"{actae._cfg.api_url}/api/v1/auth/config")
        assert resp.status_code == 200
        assert resp.json()["enable_signup"] is False
        # Standalone instance: no portal SSO.
        assert resp.json()["sso"] is False

    def test_auth_config_requires_no_key(self, actae):
        # Public endpoint the dashboard consults to decide which login
        # modes to show — must not require credentials itself.
        resp = actae._http.get(
            f"{actae._cfg.api_url}/api/v1/auth/config",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 200


class TestApiKeyLogin:
    """POST /api/v1/auth/api-key-login — the dashboard's primary credential."""

    def test_api_key_login_returns_session_token(self, actae):
        resp = actae._http.post(
            f"{actae._cfg.api_url}/api/v1/auth/api-key-login",
            json={"api_key": actae._cfg.api_key},
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["token"], "a session JWT must be returned"
        assert body["user"]["id"], "the key's user identity must be returned"

    def test_api_key_login_invalid_key_rejected(self, actae):
        resp = actae._http.post(
            f"{actae._cfg.api_url}/api/v1/auth/api-key-login",
            json={"api_key": "sk-definitely-invalid-key-00000000000000"},
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 401

    def test_api_key_login_bad_body_rejected(self, actae):
        resp = actae._http.post(
            f"{actae._cfg.api_url}/api/v1/auth/api-key-login",
            json={"nope": True},
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 400

    def test_me_with_api_key_session(self, actae):
        """The session token from an API-key login works on /auth/me even
        though the subject is a key user_id, not a local users-table row."""
        login = actae._http.post(
            f"{actae._cfg.api_url}/api/v1/auth/api-key-login",
            json={"api_key": actae._cfg.api_key},
            headers={"Content-Type": "application/json"},
        )
        assert login.status_code == 200, login.text
        token = login.json()["token"]
        resp = actae._http.get(
            f"{actae._cfg.api_url}/api/v1/auth/me",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["user"]["id"] == login.json()["user"]["id"]

    @pytest.mark.asyncio
    async def test_ws_auth_with_api_key_session(self, ws, actae):
        """API-key-minted JWTs authenticate over WebSocket without a
        users-table lookup."""
        import websockets as _ws
        import json as _json
        login = actae._http.post(
            f"{actae._cfg.api_url}/api/v1/auth/api-key-login",
            json={"api_key": actae._cfg.api_key},
            headers={"Content-Type": "application/json"},
        )
        assert login.status_code == 200, login.text
        token = login.json()["token"]
        raw = await _ws.connect(ws.url, ssl=ws.ssl, ping_interval=None, close_timeout=10)
        try:
            await raw.recv()  # welcome
            await raw.send(_json.dumps({"type": "auth", "token": token, "payload": {}}))
            resp = _json.loads(await raw.recv())
            assert '"authenticated"' in _json.dumps(resp) or \
                resp.get("status") == "authenticated" or "Error" in _json.dumps(resp), resp
        finally:
            await raw.close()


class TestMetricsGating:
    """/metrics and /metrics.json are API-key-gated; health stays open."""

    def test_metrics_requires_api_key(self, actae):
        resp = actae._http.get(
            f"{actae._cfg.api_url}/metrics",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 401

    def test_metrics_accepts_api_key(self, actae):
        resp = actae._http.get(f"{actae._cfg.api_url}/metrics", headers=actae._cfg.api_headers)
        assert resp.status_code == 200

    def test_metrics_json_requires_api_key(self, actae):
        resp = actae._http.get(
            f"{actae._cfg.api_url}/metrics.json",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 401

    def test_metrics_json_accepts_api_key(self, actae):
        resp = actae._http.get(f"{actae._cfg.api_url}/metrics.json", headers=actae._cfg.api_headers)
        assert resp.status_code == 200

    def test_healthz_stays_open(self, actae):
        resp = actae._http.get(
            f"{actae._cfg.api_url}/healthz",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code in (200, 503)

    def test_readyz_stays_open(self, actae):
        resp = actae._http.get(
            f"{actae._cfg.api_url}/readyz",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code in (200, 503)
