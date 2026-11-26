# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""User preferences persistence: GET/PUT /api/v1/user/preferences.

The preferences endpoint stores an opaque JSON object per resolved identity
(dashboard session JWT or X-API-Key, shared namespace). These tests cover
the HTTP contract, the security posture, and the identity sharing between
the API-key and JWT paths.
"""

import json

import pytest


def _prefs_path():
    return "/api/v1/user/preferences"


def _prefs_url(cfg):
    return f"{cfg.api_url}{_prefs_path()}"


class TestPreferencesHttpContract:
    def test_get_returns_empty_object_when_unset(self, actae):
        # Reset to a known-empty state first: dev mode uses a single fixed
        # API key, so other tests may have populated the store.
        actae.raw_put(_prefs_path(), {"prefs": {}})
        resp = actae.raw_get(_prefs_path())
        assert resp.status_code == 200
        assert resp.json() == {"prefs": {}}

    def test_put_then_get_round_trip(self, actae):
        body = {"prefs": {"flow": {"layoutMode": "compact", "visibleEventTypes": ["agent.step"]}}}
        put = actae.raw_put(_prefs_path(), body)
        assert put.status_code == 200
        assert put.json() == body

        get = actae.raw_get(_prefs_path())
        assert get.status_code == 200
        assert get.json() == body

    def test_put_replaces_entire_object(self, actae):
        actae.raw_put(_prefs_path(), {"prefs": {"a": 1}})
        actae.raw_put(_prefs_path(), {"prefs": {"b": 2}})
        get = actae.raw_get(_prefs_path())
        assert get.json() == {"prefs": {"b": 2}}

    def test_put_non_object_envelope_rejected(self, actae):
        resp = actae.raw_put(_prefs_path(), [1, 2, 3])
        assert resp.status_code == 400
        assert "object" in resp.text.lower()

    def test_put_non_object_prefs_value_rejected(self, actae):
        resp = actae.raw_put(_prefs_path(), {"prefs": [1, 2]})
        assert resp.status_code == 400

    def test_put_invalid_json_rejected(self, actae):
        resp = actae._http.put(
            _prefs_url(actae._cfg),
            content=b"{not json",
            headers={"Content-Type": "application/json", **({"X-API-Key": actae._cfg.api_key} if actae._cfg.api_key else {})},
        )
        assert resp.status_code == 400

    def test_put_oversized_object_rejected(self, actae):
        blob = "x" * (64 * 1024 + 1)
        body = {"prefs": {"blob": blob}}
        resp = actae.raw_put(_prefs_path(), body)
        assert resp.status_code in (413, 400)

    def test_nested_prefs_preserved_exactly(self, actae):
        deep = {"flow": {"fields": ["uuid"], "layout": {"direction": "TB", "spacing": 40}}}
        actae.raw_put(_prefs_path(), {"prefs": deep})
        get = actae.raw_get(_prefs_path())
        assert get.json()["prefs"] == deep


class TestPreferencesAuth:
    def test_missing_credentials_401(self, actae):
        resp = actae._http.get(_prefs_url(actae._cfg), headers={"Content-Type": "application/json"})
        assert resp.status_code == 401

    def test_invalid_api_key_401(self, actae):
        resp = actae._http.get(
            _prefs_url(actae._cfg),
            headers={"Content-Type": "application/json", "X-API-Key": "sk-invalid-key"},
        )
        assert resp.status_code == 401

    def test_invalid_bearer_token_401(self, actae):
        resp = actae._http.get(
            _prefs_url(actae._cfg),
            headers={"Content-Type": "application/json", "Authorization": "Bearer not-a-jwt"},
        )
        assert resp.status_code == 401

    def test_api_key_and_jwt_share_identity(self, actae):
        """The dashboard session (JWT) and HTTP SDK clients (X-API-Key) share
        the same preference store when they resolve to the same user."""
        if not actae._cfg.api_key:
            pytest.skip("no API key configured")
        key = actae._cfg.api_key
        login = actae._http.post(
            f"{actae._cfg.api_url}/api/v1/auth/api-key-login",
            json={"api_key": key},
            headers={"Content-Type": "application/json"},
        )
        if login.status_code != 200:
            pytest.skip("api-key-login unavailable (e.g. dev mode without JWT minting)")
        token = login.json().get("token")
        if not token:
            pytest.skip("no JWT minted")

        body = {"prefs": {"via": "jwt"}}
        put = actae._http.put(
            _prefs_url(actae._cfg),
            json=body,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        )
        assert put.status_code == 200

        # Same identity via X-API-Key sees the same object.
        get = actae._http.get(
            _prefs_url(actae._cfg),
            headers={"Content-Type": "application/json", "X-API-Key": key},
        )
        assert get.status_code == 200
        assert get.json() == body
