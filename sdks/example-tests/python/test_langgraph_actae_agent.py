# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Offline unit tests for examples/langgraph_actae_agent (no server, no network).

Covers the deterministic scripted model, the effect-store helpers, and the
idempotent ledger-guarded tool against a fake Actae. Building an actual
LangGraph graph + ActaeCheckpointSaver is covered by the demo and by
``test_adapters_extended.py``; these tests keep the example's logic pinned.

Skipped when langchain-core is not installed (the example's optional dep).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("langchain_core")

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage  # noqa: E402

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT / "examples" / "langgraph_actae_agent"))

import lg_agent  # noqa: E402


class _Execution:
    id = "ex-1"


class _Claim:
    def __init__(self, status: str, result=None):
        self.status = status
        self.result = result
        self.execution = _Execution()
        self.claim_token = "tok"


class FakeActae:
    def __init__(self, statuses):
        self._statuses = list(statuses)
        self.calls = []

    async def claim_execution(self, channel, key, tool, params, lease_seconds=None):
        self.calls.append(("claim", key))
        status = self._statuses.pop(0) if self._statuses else "claimed"
        return _Claim(status)

    async def complete_execution(self, execution_id, token, result=None):
        self.calls.append(("complete", result))


def test_effect_store_round_trip(tmp_path):
    path = tmp_path / "effects.jsonl"
    assert lg_agent.effect_rows(path) == []
    assert lg_agent.effect_count(path, "a") == 0
    lg_agent.apply_effect(path, {"label": "a", "celsius": 1})
    lg_agent.apply_effect(path, {"label": "b", "celsius": 2})
    assert lg_agent.effect_count(path, "a") == 1
    assert lg_agent.effect_count(path, "b") == 1
    assert len(lg_agent.effect_rows(path)) == 2


def test_scripted_model_emits_tool_then_final():
    model = lg_agent.ScriptedSurveyModel(label="baseline", celsius=21.0, final_note="done")
    first = model.invoke([HumanMessage("go")])
    assert first.tool_calls and first.tool_calls[0]["name"] == "record_reading"
    assert first.tool_calls[0]["args"] == {"label": "baseline", "celsius": 21.0}

    history = [
        HumanMessage("go"),
        AIMessage(content="", tool_calls=[{"name": "record_reading", "args": {}, "id": "c", "type": "tool_call"}]),
        ToolMessage(content="baseline: 21C", tool_call_id="c"),
    ]
    second = model.invoke(history)
    assert second.content == "done"


def test_final_reply_returns_last_ai_message():
    state = {"messages": [HumanMessage("x"), AIMessage(content="first"), AIMessage(content="last")]}
    assert lg_agent.final_reply(state) == "last"
    assert lg_agent.final_reply({"messages": [HumanMessage("x")]}) == ""


async def _call_tool(tool, **kwargs):
    return await tool.ainvoke(kwargs)


def test_tool_applies_effect_once(tmp_path):
    import asyncio

    effects = tmp_path / "e.jsonl"
    fake = FakeActae(["claimed"])
    tool = lg_agent.make_tools(fake, "ch", effects)[0]
    out = asyncio.run(_call_tool(tool, label="x", celsius=10.0))
    assert "10.0C = 50.0F" in out
    assert lg_agent.effect_count(effects, "x") == 1
    assert ("complete", {"summary": "x: 10.0C = 50.0F"}) in fake.calls


def test_tool_reconciles_a_reclaimed_effect(tmp_path):
    import asyncio

    effects = tmp_path / "e.jsonl"
    lg_agent.apply_effect(effects, {"label": "x", "celsius": 10, "fahrenheit": 50})
    fake = FakeActae(["reclaimed"])
    tool = lg_agent.make_tools(fake, "ch", effects)[0]
    out = asyncio.run(_call_tool(tool, label="x", celsius=10.0))
    assert "already applied" in out
    assert lg_agent.effect_count(effects, "x") == 1  # not re-applied


def test_tool_replays_a_recorded_result(tmp_path):
    import asyncio

    effects = tmp_path / "e.jsonl"
    fake = FakeActae(["replayed"])
    tool = lg_agent.make_tools(fake, "ch", effects)[0]
    # A replayed claim carries the recorded result on the claim object.
    orig = FakeActae.claim_execution

    async def replay_claim(self, channel, key, tool_name, params, lease_seconds=None):
        claim = _Claim("replayed", result={"summary": "x: recorded"})
        return claim

    FakeActae.claim_execution = replay_claim  # type: ignore[assignment]
    try:
        out = asyncio.run(_call_tool(tool, label="x", celsius=10.0))
    finally:
        FakeActae.claim_execution = orig  # type: ignore[assignment]
    assert out == "x: recorded"
    assert lg_agent.effect_rows(effects) == []  # nothing applied
