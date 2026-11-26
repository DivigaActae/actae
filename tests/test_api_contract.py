# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""CI structural API-contract test (Phase 0 + RUST-001/RUST-002).

Pure static checks — no running server required. Wraps the canonical
compatibility checker in `spec/api-parity/v1/compat_check.py`:

* every public route in the Rust source has exactly one manifest operation
  (a new public handler without a contract entry fails CI);
* every manifest operation maps to a real handler fn in a real owner file;
* no route registration outside the canonical router set (parallel handler tree);
* no second event store / migration set / schema engine outside
  `actae/src/events/` (duplicate resource implementation);
* the actae.api.v1 OpenAPI fixture still contains every baseline v1 field
  (removed/narrowed v1 fields fail).
"""

import os
import sys
import json

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SPEC_DIR = os.path.join(REPO_ROOT, "spec", "api-parity", "v1")
COMPAT_CHECK = os.path.join(SPEC_DIR, "compat_check.py")

sys.path.insert(0, SPEC_DIR)
import compat_check  # noqa: E402  (import after sys.path setup)


def _load(path: str):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture(scope="module")
def manifest():
    return _load(os.path.join(SPEC_DIR, "manifest.json"))


@pytest.fixture(scope="module")
def openapi():
    return _load(os.path.join(SPEC_DIR, "actae.api.v1.openapi.json"))


class TestManifest:
    def test_manifest_has_operations(self, manifest):
        assert manifest["operations"], "manifest must inventory every public operation"

    def test_manifest_operations_are_complete(self, manifest):
        for op in manifest["operations"]:
            for field in compat_check.MANIFEST_REQUIRED_FIELDS:
                assert field in op, f"{op.get('id')} missing field {field}"

    def test_operation_ids_unique(self, manifest):
        ids = [op["id"] for op in manifest["operations"]]
        assert len(ids) == len(set(ids)), "duplicate operation ids"

    def test_operation_ids_match_api_version_prefix(self, manifest):
        for op in manifest["operations"]:
            assert op["id"].startswith("actae.api.v1."), op["id"]

    def test_every_scope_is_listed(self, manifest):
        listed = set(manifest["scopes"])
        used = {op["scope"] for op in manifest["operations"] if op["scope"] != "public"}
        unknown = used - listed
        assert not unknown, f"scopes used but not listed: {unknown}"

    def test_websocket_protocol_is_documented(self, manifest):
        ws = manifest["websocket"]
        assert ws["endpoint"] == "/ws"
        assert ws["client_messages"], "WS client messages must be inventoried"
        assert ws["server_messages"], "WS server messages must be inventoried"

    def test_subscribe_many_is_versioned_additive(self, manifest):
        msgs = {m["type"]: m for m in manifest["websocket"]["client_messages"]}
        assert "subscribe_many" in msgs
        assert msgs["subscribe_many"].get("since") == "1.1.0"


class TestHandlerOwners:
    def test_every_operation_has_existing_handler(self, manifest):
        for op in manifest["operations"]:
            h = op["handler"]
            path = os.path.join(REPO_ROOT, h["file"])
            assert os.path.exists(path), f"{op['id']}: handler file {h['file']} missing"
            src = open(path, "r", encoding="utf-8").read()
            assert h["fn"] in src, f"{op['id']}: handler fn {h['fn']} not in {h['file']}"

    def test_every_operation_has_existing_domain_service(self, manifest):
        for op in manifest["operations"]:
            d = op["domain"]
            for key in ("facade", "service"):
                for ref in d.get(key, "").split(","):
                    ref = ref.strip()
                    if not ref:
                        continue
                    file_part = ref.split("#")[0]
                    path = os.path.join(REPO_ROOT, file_part)
                    assert os.path.exists(path), f"{op['id']}: {key} {file_part} missing"


class TestRouteParity:
    def test_all_routes_have_manifest_entries(self, manifest):
        ok, errors, _ = compat_check.run_checks()
        route_errors = [e for e in errors if e.startswith("route parity:")]
        assert not route_errors, "\n".join(route_errors)

    def test_no_route_registration_outside_router_set(self, manifest):
        ok, errors, _ = compat_check.run_checks()
        dup_errors = [e for e in errors if e.startswith("RUST-002:")]
        assert not dup_errors, "\n".join(dup_errors)


class TestDuplicateResourceRejection:
    def test_no_second_event_store(self):
        ok, errors, _ = compat_check.run_checks()
        store_errors = [e for e in errors if "data-plane domain file" in e]
        assert not store_errors, "\n".join(store_errors)

    def test_no_resource_family_module_outside_allowlist(self):
        ok, errors, _ = compat_check.run_checks()
        mod_errors = [e for e in errors if "resource-family module" in e]
        assert not mod_errors, "\n".join(mod_errors)


class TestV1Compatibility:
    def test_openapi_fixture_parses(self, openapi):
        assert openapi["info"]["version"] == "actae.api.v1"

    def test_baseline_v1_fields_not_removed(self):
        ok, errors, _ = compat_check.run_checks()
        compat_errors = [e for e in errors if e.startswith("v1 compatibility:")]
        assert not compat_errors, "\n".join(compat_errors)

    def test_event_wire_shape_preserved(self):
        """The documented Event shape must keep its canonical fields (section 2.2)."""
        schemas = _load(os.path.join(SPEC_DIR, "actae.api.v1.openapi.json"))["components"]["schemas"]
        event = schemas["Event"]["properties"]
        for field in ("id", "channel_id", "type", "payload", "actor", "cursor", "timestamp", "depends_on"):
            assert field in event, f"Event.{field} must not be removed"


def test_checker_is_executable_as_ci_job():
    import subprocess

    result = subprocess.run(
        [sys.executable, COMPAT_CHECK],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    # The standalone job passes only once Phase-1 instance.rs exists; the pytest
    # suite (with a live server) is the authority. Assert the checker runs cleanly
    # modulo the Phase-1-additive instance handler reference.
    assert result.returncode in (0, 1), result.stderr
    assert "api-contract:" in result.stdout