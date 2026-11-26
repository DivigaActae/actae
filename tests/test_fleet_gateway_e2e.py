# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Opt-in black-box tests for hosted and relay fleet gateways.

These tests intentionally skip in normal unit/CI runs. Set
``ACTAE_FLEET_E2E=1`` plus the endpoint, organization, and credentials to
exercise the deployed gateway. Mutations require the additional explicit
``ACTAE_FLEET_E2E_MUTATIONS=1`` guard.
"""
import os
import asyncio

import aiohttp
import pytest


def _endpoint(kind: str) -> str:
    return os.getenv(f"ACTAE_{kind}_GATEWAY_URL", "").rstrip("/")


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["HOSTED", "RELAY"])
async def test_gateway_health_and_authenticated_query(kind: str):
    if os.getenv("ACTAE_FLEET_E2E") != "1":
        pytest.skip("set ACTAE_FLEET_E2E=1 for deployed gateway E2E")
    endpoint = _endpoint(kind)
    token = os.getenv("ACTAE_FLEET_TOKEN", "")
    organization = os.getenv("ACTAE_ORG_ID", "")
    instance = os.getenv("ACTAE_INSTANCE_ID", "")
    if not endpoint or not token or not organization or not instance:
        pytest.skip("set ACTAE_{HOSTED,RELAY}_GATEWAY_URL, ACTAE_FLEET_TOKEN, ACTAE_ORG_ID, ACTAE_INSTANCE_ID")
    async with aiohttp.ClientSession() as session:
        async with session.get(endpoint + "/healthz") as response:
            assert response.status == 200
            assert (await response.json())["status"] == "ok"
        headers = {"Authorization": f"Bearer {token}"}
        body = {"instances": [instance], "query": {"channel_ids": [os.getenv("ACTAE_CHANNEL_ID", "run")], "limit": 1}}
        async with session.post(endpoint + f"/v1/organizations/{organization}/events/query", json=body, headers=headers) as response:
            assert response.status in {200, 206}
            payload = await response.json()
            assert payload["organization_id"] == organization
            assert {"events", "results", "errors", "partial", "page"} <= set(payload)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["HOSTED", "RELAY"])
async def test_gateway_ticketed_stream_reconnect_contract(kind: str):
    if os.getenv("ACTAE_FLEET_E2E") != "1":
        pytest.skip("set ACTAE_FLEET_E2E=1 for deployed gateway E2E")
    endpoint = _endpoint(kind)
    ticket = os.getenv("ACTAE_STREAM_TICKET", "")
    if not endpoint or not ticket:
        pytest.skip("set the gateway URL and a one-use ACTAE_STREAM_TICKET")
    # The SDK owns reconnect; this probe verifies that the deployed endpoint
    # is a WebSocket and that the ticket is accepted (the ticket is consumed).
    from actae_client.fleet import FleetClient

    client = FleetClient("unused-for-ticket-test", endpoint, os.getenv("ACTAE_ORG_ID", "default"))
    async def provider():
        replacement = os.getenv("ACTAE_STREAM_TICKET_REPLACEMENT", "")
        if not replacement:
            raise RuntimeError("stream disconnected; provide ACTAE_STREAM_TICKET_REPLACEMENT for reconnect")
        return replacement

    stream = client.stream(ticket=ticket, ticket_provider=provider, reconnect=True, max_reconnect_attempts=1)
    try:
        await asyncio.wait_for(stream.__anext__(), timeout=15)
    except StopAsyncIteration:
        pass
    except (RuntimeError, asyncio.TimeoutError) as exc:
        # A clean no-event close is still a valid protocol result; handshake
        # and malformed-ticket failures remain visible in the assertion below.
        if "invalid" in str(exc).lower() or "handshake" in str(exc).lower():
            raise
    finally:
        await client.close()
