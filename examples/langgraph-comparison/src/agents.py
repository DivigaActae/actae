# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""
Agent definitions — the core business logic of the research pipeline.

Each agent is a pure function that takes state in and returns state out.
This module contains NO framework code (no LangGraph, no Actae).
Agents are framework-agnostic — the same functions work in both
the plain LangGraph and Actae-instrumented versions.

Enterprise patterns:
  - Input validation via pydantic-style assertions
  - Structured error reporting (error codes, context)
  - Logging with correlation IDs
  - Configurable via dependency injection (config object)
"""

import json
import logging
import time as _time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from openai import AsyncOpenAI

from src.config import DeepSeekConfig
from src.logging_setup import get_correlation_id
from src.tools import TOOLDEF_RESEARCH, run_tool

logger = logging.getLogger(__name__)

MAX_TOOL_ROUNDS = 10


#  LLM result type 
@dataclass
class LLMResult:
    """Captures the full context of an LLM call for observability.

    When tools are used, ``tool_calls`` holds one dict per executed tool
    with the tool name, arguments, bounded result, and elapsed time.
    """
    content: str
    system_prompt: str
    user_message: str
    model: str
    prompt_tokens: int
    completion_tokens: int
    duration_ms: float
    tool_calls: Optional[list] = None


#  Agent errors 
class AgentError(Exception):
    """Base error for agent failures."""
    def __init__(self, message: str, agent: str, error_code: str, details: Optional[Dict] = None):
        self.agent = agent
        self.error_code = error_code
        self.details = details or {}
        super().__init__(f"[{error_code}] {agent}: {message}")


class LLMConnectionError(AgentError):
    """DeepSeek API connection or rate-limit failure."""


class LLMOutputError(AgentError):
    """LLM returned unparseable or invalid output."""


#  DeepSeek client (lazy singleton per config) 
_client_registry: Dict[str, AsyncOpenAI] = {}


def get_llm(config: DeepSeekConfig) -> AsyncOpenAI:
    """Get or create a cached DeepSeek client for the given config.

    In production, clients are pooled and reused to avoid connection
    overhead and stay within API rate limits.
    """
    key = f"{config.base_url}:{config.model}"
    if key not in _client_registry:
        _client_registry[key] = AsyncOpenAI(
            api_key=config.api_key,
            base_url=config.base_url,
            timeout=config.request_timeout_seconds,
        )
    return _client_registry[key]


async def _call_llm(
    config: DeepSeekConfig,
    system_prompt: str,
    user_message: str,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
) -> LLMResult:
    """Execute an LLM call with structured error handling.

    Returns an LLMResult containing the response content plus full
    prompt/response metadata for observability.

    Production concerns addressed:
      - Timeout: configurable per request
      - Rate limiting: caught and wrapped in LLMConnectionError
      - Empty responses: caught and wrapped in LLMOutputError
      - Token tracking: logged for cost monitoring
    """
    llm = get_llm(config)
    t0 = _time.time()

    try:
        resp = await llm.chat.completions.create(
            model=config.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            temperature=temperature if temperature is not None else config.default_temperature,
            max_tokens=max_tokens or config.max_tokens,
        )
    except Exception as exc:
        raise LLMConnectionError(
            message=str(exc),
            agent="llm",
            error_code="LLM_CONNECTION_ERROR",
            details={"model": config.model},
        ) from exc

    elapsed_ms = (_time.time() - t0) * 1000
    content = resp.choices[0].message.content or ""
    if not content.strip():
        raise LLMOutputError(
            message="LLM returned empty response",
            agent="llm",
            error_code="LLM_EMPTY_RESPONSE",
        )

    usage = getattr(resp, "usage", None)
    prompt_tokens = usage.prompt_tokens if usage else 0
    completion_tokens = usage.completion_tokens if usage else 0

    if usage:
        logger.info(
            "LLM call: model=%s prompt_tokens=%d completion_tokens=%d cost=~$%.4f elapsed=%.0fms",
            config.model,
            prompt_tokens,
            completion_tokens,
            (prompt_tokens * 0.00000015 + completion_tokens * 0.00000060),
            elapsed_ms,
        )

    return LLMResult(
        content=content,
        system_prompt=system_prompt,
        user_message=user_message,
        model=config.model,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        duration_ms=elapsed_ms,
    )


async def _call_llm_with_tools(
    config: DeepSeekConfig,
    system_prompt: str,
    user_message: str,
    tools: Optional[list] = None,
    execute_tool=None,
    on_tool_event=None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
) -> LLMResult:
    """Execute an LLM call that can invoke tools, looping until a final answer.

    The loop follows the OpenAI/DeepSeek function-calling protocol:
      1. Send messages with ``tools`` attached.
      2. If the response contains ``tool_calls``, execute each tool, append the
         assistant's tool-call message plus ``role=tool`` results, and re-ask.
      3. Once the model returns plain content, that is the final answer.

    ``execute_tool`` is an optional async ``(name, arguments) -> str`` callable
    that actually runs the tool. It defaults to ``run_tool`` (builtin tools);
    pass a custom one to run MCP-backed tools via ``src.mcp_bridge``.

    Every executed tool is reported to ``on_tool_event(event_type, payload)``
    (an optional async callable) as ``tool.<name>.started`` then
    ``tool.<name>.completed``, so observability layers (e.g. Actae) can record
    tool calls and their results without the agent knowing about them.

    Returns:
        ``LLMResult`` with ``tool_calls`` populated if any tools ran.
    """
    llm = get_llm(config)
    t0 = _time.time()
    dispatcher = execute_tool or run_tool

    messages: list = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_message},
    ]
    tool_calls: list = []
    prompt_tokens = 0
    completion_tokens = 0

    try:
        for _round in range(MAX_TOOL_ROUNDS):
            resp = await llm.chat.completions.create(
                model=config.model,
                messages=messages,
                temperature=temperature if temperature is not None else config.default_temperature,
                max_tokens=max_tokens or config.max_tokens,
                tools=tools or [],
            )
            usage = getattr(resp, "usage", None)
            if usage:
                prompt_tokens += usage.prompt_tokens
                completion_tokens += usage.completion_tokens

            msg = resp.choices[0].message
            calls = getattr(msg, "tool_calls", None) or []
            if not calls:
                break

            messages.append({
                "role": "assistant",
                "content": msg.content or "",
                "tool_calls": [
                    {
                        "id": c.id,
                        "type": "function",
                        "function": {
                            "name": c.function.name,
                            "arguments": c.function.arguments,
                        },
                    }
                    for c in calls
                ],
            })

            for c in calls:
                name = c.function.name
                try:
                    arguments = json.loads(c.function.arguments or "{}")
                except json.JSONDecodeError:
                    arguments = {}

                if on_tool_event is not None:
                    await _safe_emit(on_tool_event, f"tool.{name}.started", {
                        "name": name,
                        "arguments": arguments,
                        "tool_call_id": c.id,
                    })

                tc_t0 = _time.time()
                result = await dispatcher(name, arguments)
                tc_elapsed_ms = round((_time.time() - tc_t0) * 1000)

                if on_tool_event is not None:
                    await _safe_emit(on_tool_event, f"tool.{name}.completed", {
                        "name": name,
                        "arguments": arguments,
                        "result": result,
                        "duration_ms": tc_elapsed_ms,
                        "tool_call_id": c.id,
                    })

                tool_calls.append({
                    "tool": name,
                    "arguments": arguments,
                    "result": result,
                    "duration_ms": tc_elapsed_ms,
                })
                messages.append({
                    "role": "tool",
                    "tool_call_id": c.id,
                    "content": result,
                })
        else:
            raise LLMOutputError(
                message=f"No final answer after {MAX_TOOL_ROUNDS} tool rounds",
                agent="llm",
                error_code="LLM_TOOL_LOOP_LIMIT",
                details={"tool_calls": tool_calls},
            )
    except (LLMConnectionError, LLMOutputError):
        raise
    except Exception as exc:
        raise LLMConnectionError(
            message=str(exc),
            agent="llm",
            error_code="LLM_CONNECTION_ERROR",
            details={"model": config.model, "tools": len(tools or [])},
        ) from exc

    elapsed_ms = (_time.time() - t0) * 1000
    content = resp.choices[0].message.content or ""
    if not content.strip():
        raise LLMOutputError(
            message="LLM returned empty response after tool calls",
            agent="llm",
            error_code="LLM_EMPTY_RESPONSE",
        )

    if usage:
        logger.info(
            "LLM tool call: model=%s tools=%d prompt_tokens=%d completion_tokens=%d elapsed=%.0fms",
            config.model, len(tools or []), prompt_tokens, completion_tokens, elapsed_ms,
        )

    return LLMResult(
        content=content,
        system_prompt=system_prompt,
        user_message=user_message,
        model=config.model,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        duration_ms=elapsed_ms,
        tool_calls=tool_calls,
    )


async def _safe_emit(on_tool_event, event_type: str, payload: dict) -> None:
    """Await the optional observer, swallowing errors so tools always run."""
    try:
        await on_tool_event(event_type, payload)
    except Exception as exc:
        logger.warning("tool event observer failed for %s: %s", event_type, exc)


#  Agent prompts 
# In production, these would live in a prompts/ directory or a prompt
# management system (e.g., LangSmith Hub, custom prompt registry).

RESEARCHER_SYSTEM_PROMPT = (
    "You are a senior research analyst at a technology advisory firm. "
    "Given a research topic, produce 5-7 key findings with detailed "
    "explanations and supporting evidence. Be specific, cite real-world "
    "examples, and quantify claims where possible. "
    "Format: plain text, one finding per paragraph.\n\n"
    "Your job is to ground your findings in current facts. Use the "
    "web_search tool to gather up-to-date information (2-4 well-chosen "
    "queries covering different angles of the topic). You may follow up "
    "with fetch_page on a promising source URL and use calculate for any "
    "numeric analysis. Cite the sources the tools return.\n\n"
    "IMPORTANT — when to stop: once the searches have given you enough "
    "material to support your findings (typically after 2-4 queries), "
    "STOP calling tools and write the findings as your final answer. "
    "Do not keep searching — the pipeline ends the moment you reply "
    "without tool calls."
)

ANALYST_SYSTEM_PROMPT = (
    "You are a strategic analyst specializing in technology assessment. "
    "Given research findings, extract 3-5 actionable insights that would "
    "be valuable for a CTO or VP of Engineering making adoption decisions. "
    "For EACH insight, explain: (1) what it means, (2) why it matters, "
    "(3) what to do about it. Format: plain text."
)

WRITER_SYSTEM_PROMPT = (
    "You are a senior technical writer producing executive reports. "
    "Given research findings and key insights, produce a well-structured "
    "report (600-900 words) in markdown format with these sections: "
    "Executive Summary, Key Findings, Strategic Implications, "
    "Recommendations. Use concrete numbers and specific examples. "
    "If revision notes are provided, address ALL feedback points."
)

CRITIC_SYSTEM_PROMPT = (
    "You are a rigorous editor reviewing a technical report. Score it "
    "on four criteria (each 1-10): accuracy, clarity, depth, actionability. "
    "Provide specific, actionable revision notes for any score below 8. "
    "Return valid JSON only: {\"score\": N, \"accuracy\": N, \"clarity\": N, "
    "\"depth\": N, \"actionability\": N, \"notes\": \"...\"}"
)


#  Agent 1: Researcher 
async def researcher_agent(
    state: Dict[str, Any],
    config: DeepSeekConfig,
    on_tool_event=None,
    tools: Optional[list] = None,
    execute_tool=None,
) -> Dict[str, Any]:
    """Generate research findings for the given topic.

    Uses the DeepSeek function-calling protocol via ``_call_llm_with_tools``
    so the researcher can ground findings by calling tools — the builtin
    ``web_search`` / ``fetch_page`` / ``calculate`` plus any extra tools
    passed in ``tools`` (e.g. MCP server tools). Each tool call is forwarded
    to the optional ``on_tool_event`` observer for observability (Actae events).
    ``execute_tool`` overrides the builtin runner, e.g. with an MCP dispatcher.
    """
    topic = state.get("topic", "")
    if not topic:
        raise AgentError("No topic provided", "researcher", "INVALID_INPUT")

    logger.info("Researcher analyzing topic: %s", topic[:80])
    user_msg = f"Research topic: {topic}"
    result = await _call_llm_with_tools(
        config,
        RESEARCHER_SYSTEM_PROMPT,
        user_msg,
        tools=tools or TOOLDEF_RESEARCH,
        execute_tool=execute_tool,
        on_tool_event=on_tool_event,
    )

    return {
        "research_findings": result.content,
        "current_agent": "researcher",
        "agent_status": "completed",
        "llm_prompt": {"system": RESEARCHER_SYSTEM_PROMPT, "user": user_msg},
        "llm_response": {
            "model": result.model,
            "prompt_tokens": result.prompt_tokens,
            "completion_tokens": result.completion_tokens,
            "duration_ms": result.duration_ms,
            "tool_call_count": len(result.tool_calls or []),
            "tool_calls": result.tool_calls or [],
        },
    }


#  Agent 2: Analyst 
async def analyst_agent(state: Dict[str, Any], config: DeepSeekConfig) -> Dict[str, Any]:
    """Extract strategic insights from research findings."""
    research = state.get("research_findings", "")
    if not research:
        raise AgentError(
            "No research findings to analyze", "analyst", "MISSING_INPUT"
        )

    logger.info("Analyst extracting insights from %d chars of research", len(research))
    user_msg = f"Research:\n{research}"
    result = await _call_llm(config, ANALYST_SYSTEM_PROMPT, user_msg)

    return {
        "key_insights": result.content,
        "current_agent": "analyst",
        "agent_status": "completed",
        "llm_prompt": {"system": ANALYST_SYSTEM_PROMPT, "user": user_msg},
        "llm_response": {
            "model": result.model,
            "prompt_tokens": result.prompt_tokens,
            "completion_tokens": result.completion_tokens,
            "duration_ms": result.duration_ms,
        },
    }


#  Agent 3: Writer 
async def writer_agent(state: Dict[str, Any], config: DeepSeekConfig) -> Dict[str, Any]:
    """Write or revise the report."""
    research = state.get("research_findings", "")
    insights = state.get("key_insights", "")
    revision = state.get("revision_notes", "")
    revision_count = state.get("revision_count", 0)

    if not research or not insights:
        raise AgentError(
            "Missing research or insights for writing",
            "writer", "MISSING_INPUT",
        )

    if revision:
        logger.info("Writer revising (v%d) with %d chars of feedback", revision_count + 1, len(revision))
        user_msg = (
            f"Original research:\n{research}\n\n"
            f"Key insights:\n{insights}\n\n"
            f"Current draft:\n{state.get('draft', '')}\n\n"
            f"Critic feedback:\n{revision}\n\n"
            f"Please revise the report addressing all feedback."
        )
        result = await _call_llm(
            config, WRITER_SYSTEM_PROMPT, user_msg, temperature=0.5
        )
    else:
        logger.info("Writer producing first draft")
        user_msg = f"Research:\n{research}\n\nInsights:\n{insights}"
        result = await _call_llm(config, WRITER_SYSTEM_PROMPT, user_msg)

    return {
        "draft": result.content,
        "current_agent": "writer",
        "revision_count": revision_count + 1,
        "agent_status": "completed",
        "llm_prompt": {"system": WRITER_SYSTEM_PROMPT, "user": user_msg},
        "llm_response": {
            "model": result.model,
            "prompt_tokens": result.prompt_tokens,
            "completion_tokens": result.completion_tokens,
            "duration_ms": result.duration_ms,
        },
    }


#  Agent 4: Critic 
async def critic_agent(state: Dict[str, Any], config: DeepSeekConfig) -> Dict[str, Any]:
    """Score the draft and provide revision feedback."""
    draft = state.get("draft", "")
    if not draft:
        raise AgentError("No draft to review", "critic", "MISSING_INPUT")

    logger.info("Critic reviewing draft (%d chars)", len(draft))
    user_msg = f"Report:\n{draft}"
    result = await _call_llm(config, CRITIC_SYSTEM_PROMPT, user_msg, temperature=0.3)
    raw = result.content

    try:
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        parsed = json.loads(cleaned)
        score = int(parsed.get("score", 5))
        notes = parsed.get("notes", raw)
        accuracy_score = int(parsed.get("accuracy", score))
        clarity_score = int(parsed.get("clarity", score))
        depth_score = int(parsed.get("depth", score))
        actionability_score = int(parsed.get("actionability", score))
    except (json.JSONDecodeError, ValueError, KeyError) as exc:
        logger.warning("Failed to parse critic JSON: %s. Falling back to text.", exc)
        score = 5
        for word in raw.split():
            if word.isdigit() and 1 <= int(word) <= 10:
                score = int(word)
                break
        notes = raw
        accuracy_score = clarity_score = depth_score = actionability_score = score

    return {
        "review_score": score,
        "revision_notes": notes,
        "review_details": {
            "accuracy": accuracy_score,
            "clarity": clarity_score,
            "depth": depth_score,
            "actionability": actionability_score,
        },
        "current_agent": "critic",
        "agent_status": "completed",
        "llm_prompt": {"system": CRITIC_SYSTEM_PROMPT, "user": user_msg},
        "llm_response": {
            "model": result.model,
            "prompt_tokens": result.prompt_tokens,
            "completion_tokens": result.completion_tokens,
            "duration_ms": result.duration_ms,
        },
    }


#  Routing logic 
def should_continue(
    state: Dict[str, Any],
    max_revisions: int = 2,
    min_pass_score: int = 7,
) -> str:
    """Decision: should the pipeline continue to revision or finish?

    This routing function is shared between plain and Actae versions.
    It's a stateless pure function — testable with zero dependencies.
    """
    score = state.get("review_score", 0)
    revision_count = state.get("revision_count", 0)

    if score >= min_pass_score:
        logger.info("Pipeline ending: score %d >= %d threshold", score, min_pass_score)
        return "end"

    if revision_count >= max_revisions:
        logger.info("Pipeline ending: max revisions (%d) reached", max_revisions)
        return "end"

    logger.info("Pipeline revising: score %d < %d (revision %d/%d)",
                score, min_pass_score, revision_count + 1, max_revisions)
    return "revise"
