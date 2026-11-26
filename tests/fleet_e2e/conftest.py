# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Hermetic fleet E2E fixtures (Deep-004)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# `tests/` is a package (it ships an `__init__.py`), so `topology` is not a
# top-level module on sys.path when pytest imports this conftest. Prepending
# the directory here (before any test module import) keeps `from topology
# import ...` working in this package and its test modules.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from topology import E2E_SKIP, build_topology, teardown


@pytest.fixture(scope="session")
def fleet_topo(tmp_path_factory):
    """One shared topology for the whole fleet E2E session (booted once, torn
    down once). Fails loudly — never silently skips — when the binaries are
    missing or a service does not become healthy."""
    if os.environ.get("ACTAE_E2E") != "1":
        pytest.skip("opt-in hermetic fleet E2E (ACTAE_E2E=1)")
    tmp = tmp_path_factory.mktemp("fleet-e2e")
    topo = build_topology(tmp)
    try:
        yield topo
    finally:
        teardown(topo)


@pytest.fixture
def fleet_topo_env(fleet_topo):
    """Environment for driving the Go/TypeScript SDK drivers against the
    real topology."""
    iid = fleet_topo.relay_instance_id
    return {
        "ACTAE_FLEET_URL": fleet_topo.gateway_url,
        "ACTAE_FLEET_TOKEN_URL": fleet_topo.saas_url,
        "ACTAE_ORG_ID": fleet_topo.org,
        "ACTAE_ORG_TOKEN": fleet_topo.service_tokens[iid],
        "ACTAE_E2E_INSTANCE": iid,
        "ACTAE_E2E_SAAS_URL": fleet_topo.saas_url,
        "ACTAE_E2E_INTERNAL_TOKEN": fleet_topo.internal_token,
        "ACTAE_E2E_CHANNEL": "public.sdkmatrix",
        "ACTAE_FLEET_SCOPES": (
            "events:read,events:write,state:read,state:write,"
            "channels:read,channels:write,forks:create"
        ),
    }


@pytest.fixture
def saas(fleet_topo):
    return fleet_topo


@pytest.fixture
def gateway(fleet_topo):
    return fleet_topo


@pytest.fixture
def instances(fleet_topo):
    return fleet_topo.instances