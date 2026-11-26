# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from actae_client.adapters.contract import (
    CHECKPOINT_METADATA_KEY,
    FULL_FEATURE_SET,
    ActaeFeature,
    ActaeRunContext,
    ActaeToolExecutor,
    AdapterCapabilities,
    AdapterContractError,
    AdapterSupportLevel,
    CheckpointEnvelope,
    FrameworkEvent,
    ResumeFidelity,
    ToolExecutionInProgressError,
    embed_checkpoint_metadata,
    extract_checkpoint_envelope,
    strip_checkpoint_metadata,
)
from actae_client.adapters.profiles import (
    ADAPTER_CAPABILITIES,
    common_surface_capabilities,
    get_adapter_capabilities,
)


def _capabilities(**overrides):
    values = {
        "framework": "test",
        "adapter_version": "1",
        "support_level": AdapterSupportLevel.CERTIFIED,
        "resume_fidelity": ResumeFidelity.CHECKPOINT_EXACT,
        "features": FULL_FEATURE_SET,
        "native_checkpoint": True,
    }
    values.update(overrides)
    return AdapterCapabilities(**values)


def _envelope(**overrides):
    values = {
        "framework": "test",
        "adapter_version": "1",
        "channel_id": "run:test",
        "portable_state": {"messages": [{"role": "user", "content": "hello"}]},
        "native_checkpoint": {"checkpoint_id": "cp-1"},
        "application_state": {"customer": 42},
        "pending_work": {"tool": "lookup"},
        "manifest": {"model": "test-model"},
        "event_cursor": 7,
        "event_id": "evt-7",
    }
    values.update(overrides)
    return CheckpointEnvelope(**values)


def _claim(status, *, result=None, token="token-1"):
    return SimpleNamespace(
        status=status,
        result=result,
        claim_token=token,
        execution=SimpleNamespace(id="execution-1"),
    )


def test_capabilities_are_machine_readable_and_validate_dependencies():
    profile = _capabilities()
    assert profile.full_parity
    assert profile.missing_features == frozenset()
    assert profile.supports(ActaeFeature.TOOL_EXECUTIONS)
    assert profile.to_dict()["features"] == sorted(f.value for f in FULL_FEATURE_SET)

    with pytest.raises(AdapterContractError, match="resume support requires"):
        _capabilities(features={ActaeFeature.RESUME}, native_checkpoint=False)


def test_every_python_adapter_publishes_an_honest_native_and_common_profile():
    assert set(ADAPTER_CAPABILITIES) == {
        "langgraph", "claude-agent-sdk", "langchain", "crewai", "openai-agents", "codex",
    }
    assert get_adapter_capabilities("langgraph").resume_fidelity is ResumeFidelity.CHECKPOINT_EXACT
    assert get_adapter_capabilities("codex").resume_fidelity is ResumeFidelity.OBSERVE_ONLY
    for framework in ADAPTER_CAPABILITIES:
        assert common_surface_capabilities(framework).full_parity
    with pytest.raises(KeyError, match="available"):
        get_adapter_capabilities("unknown")


def test_checkpoint_envelope_round_trips_and_preserves_framework_state():
    envelope = _envelope()
    restored = CheckpointEnvelope.from_dict(envelope.to_dict())
    assert restored == envelope

    state = embed_checkpoint_metadata({"framework_owned": [1, 2]}, envelope)
    assert state["framework_owned"] == [1, 2]
    assert state[CHECKPOINT_METADATA_KEY]["schema"] == envelope.schema
    assert extract_checkpoint_envelope(state) == envelope
    assert strip_checkpoint_metadata(state) == {"framework_owned": [1, 2]}


def test_checkpoint_matches_cross_sdk_conformance_fixture():
    fixture = Path(__file__).parents[3] / "tests/fixtures/framework_checkpoint_v1.json"
    value = json.loads(fixture.read_text(encoding="utf-8"))
    assert CheckpointEnvelope.from_dict(value).to_dict() == value


def test_checkpoint_rejects_invalid_or_non_json_state():
    with pytest.raises(AdapterContractError, match="unsupported checkpoint schema"):
        CheckpointEnvelope.from_dict({**_envelope().to_dict(), "schema": "future/v9"})

    with pytest.raises(AdapterContractError, match="JSON-serializable"):
        _envelope(native_checkpoint={"bad": object()})

    with pytest.raises(AdapterContractError, match="must be an object"):
        extract_checkpoint_envelope({CHECKPOINT_METADATA_KEY: "bad"})


def test_framework_event_has_stable_schema_and_rejects_nan():
    payload = FrameworkEvent("step.completed", "test", "run-1", {"answer": 42}).to_payload()
    assert payload["schema"] == "actae.framework-event/v1"
    assert payload["kind"] == "step.completed"

    with pytest.raises(AdapterContractError, match="JSON-serializable"):
        FrameworkEvent("step", "test", "run-1", {"score": float("nan")}).to_payload()


@pytest.mark.asyncio
async def test_tool_executor_claims_runs_completes_and_replays():
    client = SimpleNamespace(
        claim_execution=AsyncMock(return_value=_claim("claimed")),
        complete_execution=AsyncMock(),
        heartbeat_execution=AsyncMock(),
        fail_execution=AsyncMock(),
        cancel_execution=AsyncMock(),
    )
    executor = ActaeToolExecutor(client, "run:test")
    invoked = 0

    async def invoke():
        nonlocal invoked
        invoked += 1
        return {"ok": True}

    result = await executor.execute(
        "step-1:email", "send_email", {"to": "a@example.com"}, invoke
    )
    assert result == {"ok": True}
    assert invoked == 1
    client.complete_execution.assert_awaited_once_with(
        "execution-1", "token-1", {"ok": True}
    )

    client.claim_execution.return_value = _claim("replayed", result={"ok": True})
    assert await executor.execute("step-1:email", "send_email", {}, invoke) == {"ok": True}
    assert invoked == 1


@pytest.mark.asyncio
async def test_tool_executor_heartbeats_and_supports_sync_functions():
    client = SimpleNamespace(
        claim_execution=AsyncMock(return_value=_claim("reclaimed")),
        complete_execution=AsyncMock(),
        heartbeat_execution=AsyncMock(),
        fail_execution=AsyncMock(),
        cancel_execution=AsyncMock(),
    )
    executor = ActaeToolExecutor(client, "run:test")

    async def invoke():
        await asyncio.sleep(0.025)
        return "done"

    assert await executor.execute(
        "slow-1", "slow", {}, invoke, heartbeat_interval=0.005
    ) == "done"
    assert client.heartbeat_execution.await_count >= 1

    client.claim_execution.return_value = _claim("claimed")
    assert await executor.execute("sync-1", "sync", {}, lambda: 3) == 3


@pytest.mark.asyncio
async def test_tool_executor_stops_waiting_when_heartbeat_loses_lease():
    client = SimpleNamespace(
        claim_execution=AsyncMock(return_value=_claim("claimed")),
        complete_execution=AsyncMock(),
        heartbeat_execution=AsyncMock(side_effect=RuntimeError("lease lost")),
        fail_execution=AsyncMock(),
        cancel_execution=AsyncMock(),
    )
    executor = ActaeToolExecutor(client, "run:test")
    cancelled = asyncio.Event()

    async def ignores_until_cancelled():
        await cancelled.wait()
        return "too late"

    with pytest.raises(RuntimeError, match="lease lost"):
        await asyncio.wait_for(
            executor.execute(
                "lease-1", "slow", {}, ignores_until_cancelled,
                heartbeat_interval=0.01, cancellation_event=cancelled,
            ),
            timeout=1,
        )
    client.fail_execution.assert_awaited_once()
    client.complete_execution.assert_not_awaited()


@pytest.mark.asyncio
async def test_tool_executor_reports_failure_cancellation_and_contention():
    client = SimpleNamespace(
        claim_execution=AsyncMock(return_value=_claim("claimed")),
        complete_execution=AsyncMock(),
        heartbeat_execution=AsyncMock(),
        fail_execution=AsyncMock(),
        cancel_execution=AsyncMock(),
    )
    executor = ActaeToolExecutor(client, "run:test")

    with pytest.raises(ValueError, match="heartbeat_interval"):
        await executor.execute("invalid", "invalid", {}, lambda: None, heartbeat_interval=0)
    client.claim_execution.assert_not_awaited()

    async def fail():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await executor.execute("fail-1", "failing", {}, fail)
    assert client.fail_execution.await_args.kwargs["error_type"] == "RuntimeError"

    client.claim_execution.return_value = _claim("in_progress", token=None)
    with pytest.raises(ToolExecutionInProgressError) as error:
        await executor.execute("busy-1", "busy", {}, lambda: None)
    assert error.value.execution_id == "execution-1"

    client.claim_execution.return_value = _claim("claimed")
    started = asyncio.Event()

    async def wait_forever():
        started.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(executor.execute("cancel-1", "waiting", {}, wait_forever))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    client.cancel_execution.assert_awaited_once_with("execution-1", "token-1")


@pytest.mark.asyncio
async def test_run_context_records_and_loads_standard_checkpoints():
    envelope = _envelope()
    client = SimpleNamespace(
        record=AsyncMock(return_value=SimpleNamespace(id="event-1")),
        latest_cursor=AsyncMock(return_value=7),
        save_state=AsyncMock(return_value=11),
        latest_state=AsyncMock(return_value={"state": envelope.to_dict()}),
        replay=AsyncMock(return_value=[]),
        fork=AsyncMock(return_value=SimpleNamespace(child_channel_id="run:fork")),
        create_experiment=AsyncMock(return_value={"id": "experiment-1"}),
        add_experiment_member=AsyncMock(return_value={"ok": True}),
        create_execution_group=AsyncMock(return_value=SimpleNamespace(group_id="group-1")),
        schedule_wakeup=AsyncMock(return_value=SimpleNamespace(id="wakeup-1")),
    )
    context = ActaeRunContext(client, "run:test", "test", "run-1")
    await context.record("step.completed", {"answer": 42})
    recorded = client.record.await_args.args
    assert recorded[0:2] == ("run:test", "framework.step.completed")
    assert recorded[2]["framework"] == "test"
    assert await context.save_checkpoint(envelope) == 11
    assert await context.load_checkpoint() == envelope
    assert await context.replay(cursor=2, limit=10) == []
    fork = await context.fork("run:fork", reason="variant")
    assert fork.channel_id == "run:fork"
    assert fork.parent_run_id == "run-1"
    await context.create_experiment("models")
    client.create_experiment.assert_awaited_once_with(
        "models", description=None, baseline_channel_id="run:test"
    )
    await context.add_to_experiment("experiment-1")
    await context.create_execution_group("group-1")
    await context.schedule_wakeup("2026-08-28T10:00:00Z", payload={"retry": 1})
