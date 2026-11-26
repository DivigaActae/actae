# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from actae_client import Actae, channel_for_run, deterministic_operation_key


def _claim(status="claimed", result=None):
    return SimpleNamespace(
        status=status,
        result=result,
        claim_token=None if status == "in_progress" else "token-1",
        execution=SimpleNamespace(id="execution-1"),
    )


def _client():
    return SimpleNamespace(
        record=AsyncMock(return_value=SimpleNamespace(id="event-1", cursor=1)),
        claim_execution=AsyncMock(return_value=_claim()),
        complete_execution=AsyncMock(return_value=SimpleNamespace()),
        fail_execution=AsyncMock(return_value=SimpleNamespace()),
        heartbeat_execution=AsyncMock(return_value=SimpleNamespace()),
        cancel_execution=AsyncMock(return_value=SimpleNamespace()),
    )


def test_channel_for_run_is_readable_stable_and_collision_safe():
    assert channel_for_run("ticket-42", "langgraph") == "langgraph:ticket-42"
    first = channel_for_run("tenant/a workflow", "temporal")
    assert first == channel_for_run("tenant/a workflow", "temporal")
    assert first.startswith("temporal:tenant-a-workflow:")
    assert first != channel_for_run("tenant:a workflow", "temporal")
    assert len(channel_for_run("x" * 1000).encode("utf-8")) <= 256


def test_deterministic_operation_key_matches_existing_cross_sdk_vector():
    assert deterministic_operation_key(
        "agent-session", "tool", "channel-1", 7, "payload"
    ) == "f059dcde-a40f-5156-9ba8-ad9246cf929d"


@pytest.mark.asyncio
async def test_activity_restores_carrier_lineage_and_propagates_attempt():
    client = _client()
    actae = Actae(client)
    temporal = actae.orchestrator("temporal")
    carrier = temporal.carrier("wf/42")

    async with temporal.activity("agent-0", parent=carrier, attempt=2) as activity:
        assert actae.current() is activity
        assert activity.workflow_id == "wf/42"
        assert activity.attempt == 2
        async with activity.child("agent-1") as child:
            assert child.parent_run_id == "agent-0"
            assert actae.current() is child
        assert actae.current() is activity

    assert actae.current(required=False) is None
    kinds = [call.args[1] for call in client.record.await_args_list]
    assert kinds == [
        "framework.run.started",
        "framework.run.started",
        "framework.run.completed",
        "framework.run.completed",
    ]
    lifecycle_calls = client.record.await_args_list
    assert lifecycle_calls[0].kwargs["operation_id"] == lifecycle_calls[0].kwargs["operation_id"]
    assert lifecycle_calls[0].args[2]["data"]["attempt"] == 2


def test_carrier_fixture_is_cross_sdk_safe():
    import pathlib

    fixture = pathlib.Path(__file__).parents[3] / "tests" / "fixtures" / "run_carrier_v1.json"
    from actae_client import ActaeCarrier

    carrier = ActaeCarrier.from_dict(__import__("json").loads(fixture.read_text()))
    assert carrier.to_dict()["workflow_id"] == "workflow-42"
    with pytest.raises(ValueError, match="unsupported fields"):
        ActaeCarrier.from_dict({**carrier.to_dict(), "api_key": "never-accepted"})


@pytest.mark.asyncio
async def test_run_failure_preserves_application_error_when_failure_record_fails():
    client = _client()
    client.record.side_effect = [SimpleNamespace(id="started"), RuntimeError("storage down")]
    actae = Actae(client)

    with pytest.raises(ValueError, match="application failed"):
        async with actae.run("r1"):
            raise ValueError("application failed")


@pytest.mark.asyncio
async def test_run_cancellation_is_not_recorded_as_failure():
    client = _client()
    actae = Actae(client)
    with pytest.raises(asyncio.CancelledError):
        async with actae.run("r1"):
            raise asyncio.CancelledError()
    assert [call.args[1] for call in client.record.await_args_list] == [
        "framework.run.started", "framework.run.cancelled",
    ]


@pytest.mark.asyncio
async def test_warn_mode_is_fail_open_for_lifecycle_only():
    client = _client()
    client.record.side_effect = RuntimeError("offline")
    actae = Actae(client, lifecycle_errors="warn")
    async with actae.run("r1") as scope:
        assert scope.run_id == "r1"


@pytest.mark.asyncio
async def test_effect_decorator_claims_once_and_replays_without_invocation():
    client = _client()
    actae = Actae(client)
    calls = 0

    @actae.effect("order:{order_id}:charge")
    async def charge(order_id: str, amount: int):
        nonlocal calls
        calls += 1
        return {"receipt": "r-1", "amount": amount}

    async with actae.run("checkout-1"):
        assert await charge("o-1", 25) == {"receipt": "r-1", "amount": 25}
        client.claim_execution.return_value = _claim(
            "replayed", {"receipt": "r-1", "amount": 25}
        )
        assert await charge("o-1", 25) == {"receipt": "r-1", "amount": 25}

    assert calls == 1
    first_claim = client.claim_execution.await_args_list[0]
    assert first_claim.args[:4] == (
        "custom:checkout-1",
        "order:o-1:charge",
        "charge",
        {"order_id": "o-1", "amount": 25},
    )


@pytest.mark.asyncio
async def test_effect_requires_scope_and_json_safe_parameters():
    client = _client()
    actae = Actae(client)

    @actae.effect("unsafe")
    async def unsafe(value):
        return value

    with pytest.raises(RuntimeError, match="no active Actae run"):
        await unsafe(1)
    async with actae.run("r1"):
        with pytest.raises(TypeError, match="effect parameters"):
            await unsafe(object())


@pytest.mark.asyncio
async def test_context_is_isolated_between_concurrent_tasks():
    client = _client()
    actae = Actae(client)
    ready = asyncio.Event()
    seen = []

    async def worker(run_id):
        async with actae.run(run_id):
            ready.set()
            await asyncio.sleep(0)
            seen.append(actae.current().run_id)

    await asyncio.gather(worker("a"), worker("b"))
    assert sorted(seen) == ["a", "b"]
