# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

import json
from pathlib import Path

import pytest

from actae_client.fleet import FleetClient
from actae_client.manifest import MANIFEST_OPERATIONS, manifest_operation
from actae_client.session import _canonical_step_content, _step_operation_id


def _repo_file(name: str) -> Path:
    return Path(__file__).resolve().parents[3] / "spec" / "api-parity" / "v1" / name


def test_generated_manifest_registry_matches_source_manifest():
    source = json.loads(_repo_file("manifest.json").read_text())["operations"]
    assert len(MANIFEST_OPERATIONS) == len(source) == 76
    assert [op.id for op in MANIFEST_OPERATIONS] == [op["id"] for op in source]
    for expected in source:
        actual = manifest_operation(expected["id"])
        assert (actual.method, actual.path, actual.action) == (expected["method"], expected["path"], expected["action"])


def test_cross_sdk_golden_vectors_are_shared():
    vectors = json.loads(_repo_file("golden_vectors.json").read_text())["deterministic_operation_keys"]
    for vector in vectors:
        canonical = _canonical_step_content(vector["payload"], vector["metadata"])
        assert canonical == vector["canonical"]
        assert _step_operation_id(vector["channel"], vector["type"], int(vector["step"]), vector["payload"], vector["metadata"]) == vector["operation_id"]


def test_manifest_operation_rejects_path_typos(monkeypatch):
    client = FleetClient("org", "https://fleet.example", "org-1")
    called = False

    async def no_network(*args, **kwargs):
        nonlocal called
        called = True
        return {"ok": True}

    monkeypatch.setattr(client, "call", no_network)
    with pytest.raises(ValueError, match="missing path parameter"):
        import asyncio
        asyncio.run(client.call_operation("actae.api.v1.events.replay", path_params={}))
    assert not called


def test_manifest_operation_uses_typed_gateway_dispatch(monkeypatch):
    client = FleetClient("org", "https://fleet.example", "org-1")
    seen = {}

    async def request(method, path, **kwargs):
        seen.update(method=method, path=path, kwargs=kwargs)
        return {"ok": True}

    monkeypatch.setattr(client, "_request", request)
    import asyncio
    asyncio.run(client.call_operation("actae.api.v1.events.replay", instance_id="inst-1",
                                      path_params={"channel_id": "run"}, query={"limit": 2}))
    assert seen["method"] == "POST"
    assert seen["path"].endswith("/instances/inst-1/ops/actae.api.v1.events.replay")
    assert seen["kwargs"]["body"] == {"path_params": {"channel_id": "run"}, "query": {"limit": 2}, "body": {}}


@pytest.mark.asyncio
async def test_fleet_supported_operations_dispatch(monkeypatch):
    """Fleet-supported operations dispatch through the gateway."""
    client = FleetClient("org", "https://fleet.example", "org-1")
    calls = []

    async def request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        return {"data": {}}

    monkeypatch.setattr(client, "_request", request)
    import re
    fleet_ops = [op for op in MANIFEST_OPERATIONS if op.fleet_supported]
    for op in fleet_ops:
        names = re.findall(r"\{([^}]+)\}", op.path)
        path_params = {name: f"value-{i}" for i, name in enumerate(names)}
        confirm = next(iter(path_params.values()), "*") if op.action == "delete" else None
        await client.call_operation(op.id, instance_id="inst-1", path_params=path_params,
                                    body={}, operation_idempotency_key="stable-key",
                                    confirm_target=confirm)
    assert len(calls) == len(fleet_ops)
    assert all(path.startswith("/v1/organizations/org-1/instances/inst-1/ops/") for _, path, _ in calls)


@pytest.mark.asyncio
async def test_unsupported_operations_fail_locally():
    """Auth/dashboard operations (scope=public) fail with 426 before network I/O."""
    from actae_client.fleet import UnsupportedOperationError
    client = FleetClient("org", "https://fleet.example", "org-1")
    unsupported = [op for op in MANIFEST_OPERATIONS if not op.fleet_supported]
    for op in unsupported:
        with pytest.raises(UnsupportedOperationError, match="unsupported API operation"):
            await client.call_operation(op.id, instance_id="inst-1", body={})
    assert len(unsupported) == 5  # auth.signup, auth.login, auth.api_key_login, auth.config.get, auth.logout
