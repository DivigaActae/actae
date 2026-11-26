# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""A real LangGraph ReAct agent, made durable and forkable by Actae.

This is *not* a hand-rolled pipeline: it is LangGraph's canonical tool-calling
agent (``create_react_agent``), compiled with Actae's ``ActaeCheckpointSaver``
as its checkpointer. Actae is the agent's durable state store, so the same
thread can be resumed after a hard process kill and forked at an exact
checkpoint.

The side-effecting tool is guarded by Actae's execution ledger (claim/complete
with a stable key), so a resume or a fork that re-enters the tool replays the
recorded result instead of firing the effect twice.

Deterministic by default (a scripted chat model, no API key); ``--llm`` swaps in
a real DeepSeek model that must decide to call the tool itself.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import tool

from actae_client.adapters.langgraph import ActaeCheckpointSaver


# --------------------------------------------------------------------------
# External side effect: a real, observable effect the agent performs.
# --------------------------------------------------------------------------
def effect_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def effect_count(path: Path, label: str) -> int:
    return sum(1 for r in effect_rows(path) if r.get("label") == label)


def apply_effect(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=True) + "\n")


# --------------------------------------------------------------------------
# Tools: one side-effecting (ledger-guarded), one pure.
# --------------------------------------------------------------------------
def _die() -> None:
    sys.stdout.flush()
    os._exit(137)


_LEASE_SECONDS = 3


async def _claim_owned(actae: Any, channel: str, key: str, params: Dict[str, Any], label: str):
    """Claim an execution, waiting out a live lease owned by another worker.

    ``claim_execution`` returns ``in_progress`` when a live lease already owns
    the key; applying the effect then would double-fire it. Wait (bounded) for
    the lease to lapse and re-claim, preferring ``reclaimed`` ownership.
    """
    wait_budget = _LEASE_SECONDS + 3
    waited = 0.0
    claim = await actae.claim_execution(
        channel, key, "survey.record", params, lease_seconds=_LEASE_SECONDS
    )
    while getattr(claim, "status", "") == "in_progress" and waited < wait_budget:
        await asyncio.sleep(0.5)
        waited += 0.5
        claim = await actae.claim_execution(
            channel, key, "survey.record", params, lease_seconds=_LEASE_SECONDS
        )
    return claim


def make_tools(
    actae: Any,
    channel: str,
    effects_path: Path,
    *,
    crash_point: Optional[str] = None,
    crash_after_effect: bool = False,
) -> list:
    """Build the agent's tools.

    ``crash_point`` (for the durable-execution torture tests) kills the process
    at a precise point inside the side-effecting tool:
    ``"before_effect"`` | ``"after_effect"`` | ``"after_complete"``.
    ``crash_after_effect=True`` is shorthand for ``"after_effect"``.
    """
    point = crash_point or ("after_effect" if crash_after_effect else None)

    @tool
    async def record_reading(label: str, celsius: float) -> str:
        """Record a sensor reading to the survey log (an external side effect).
        `label` is a stable identifier for the reading."""
        key = f"reading:{label}"
        claim = await _claim_owned(actae, channel, key, {"label": label, "celsius": celsius}, label)
        if claim.status == "replayed":
            saved = claim.result if isinstance(claim.result, dict) else {}
            return str(saved.get("summary", f"{label}: replayed"))
        if claim.status not in ("claimed", "reclaimed"):
            return f"{label}: busy (another worker owns this execution; status={claim.status})"
        # Reclaimed after a crash: the effect may already have landed. Reconcile
        # by checking the effect store instead of blindly re-applying.
        if effect_count(effects_path, label) > 0:
            summary = f"{label}: {celsius}C (already applied)"
            await actae.complete_execution(
                claim.execution.id, claim.claim_token, result={"summary": summary}
            )
            return summary
        if point == "before_effect":
            _die()
        fahrenheit = celsius * 9 / 5 + 32
        apply_effect(effects_path, {"label": label, "celsius": celsius, "fahrenheit": fahrenheit})
        if point == "after_effect":
            _die()  # after the effect, before completion
        summary = f"{label}: {celsius}C = {fahrenheit:.1f}F"
        await actae.complete_execution(
            claim.execution.id, claim.claim_token, result={"summary": summary}
        )
        if point == "after_complete":
            _die()  # after completion, before returning to the graph
        return summary

    @tool
    def celsius_to_fahrenheit(celsius: float) -> float:
        """Convert a temperature from Celsius to Fahrenheit."""
        return celsius * 9 / 5 + 32

    return [record_reading, celsius_to_fahrenheit]


# --------------------------------------------------------------------------
# Deterministic, history-aware scripted ReAct model (no API key).
# --------------------------------------------------------------------------
class ScriptedSurveyModel(BaseChatModel):
    """Emits one tool call, then a final answer once a tool result is present."""

    label: str = "baseline"
    celsius: float = 21.0
    final_note: str = "Survey complete: 21C is within range."

    @property
    def _llm_type(self) -> str:
        return "scripted-survey"

    def bind_tools(self, tools: Any, **kwargs: Any) -> "ScriptedSurveyModel":
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):  # type: ignore[override]
        used_tool = any(isinstance(m, ToolMessage) for m in messages)
        if used_tool:
            message = AIMessage(content=self.final_note)
        else:
            message = AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "record_reading",
                        "args": {"label": self.label, "celsius": self.celsius},
                        "id": f"call-{self.label}",
                        "type": "tool_call",
                    }
                ],
            )
        return ChatResult(generations=[ChatGeneration(message=message)])


def deepseek_model() -> BaseChatModel:
    """A real DeepSeek model (OpenAI-compatible). Requires langchain-openai."""
    try:
        from langchain_openai import ChatOpenAI  # type: ignore
    except Exception as err:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "LLM mode needs `pip install langchain-openai` and DEEPSEEK_API_KEY"
        ) from err
    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError("DEEPSEEK_API_KEY is required for --llm mode")
    return ChatOpenAI(
        model=os.environ.get("DEEPSEEK_MODEL", "deepseek-chat"),
        base_url=os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
        api_key=api_key,
        temperature=0,
    )


# --------------------------------------------------------------------------
# Graph
# --------------------------------------------------------------------------
def build_graph(model: BaseChatModel, tools: list, saver: ActaeCheckpointSaver):
    """Compile LangGraph's ReAct agent against the Actae checkpointer."""
    from langgraph.prebuilt import create_react_agent

    with warnings.catch_warnings():
        # langgraph 1.x deprecates create_react_agent in favour of
        # langchain.agents.create_agent; the LangGraph prebuilt is stable here.
        warnings.simplefilter("ignore")
        return create_react_agent(model, tools, checkpointer=saver)


def final_reply(state: Dict[str, Any]) -> str:
    for message in reversed(state.get("messages", [])):
        if isinstance(message, AIMessage) and message.content:
            return str(message.content)
    return ""


def checkpoint_channel(saver: ActaeCheckpointSaver, thread_id: str) -> str:
    return saver.channel_for_config({"configurable": {"thread_id": thread_id}})
