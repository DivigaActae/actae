# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""A real, durable, forkable research-agent pipeline — built on LangGraph.

The graph is a small, explicit pipeline (not a ReAct loop), because the whole
point of the demo is that the *steps are visible* and the *fork boundary is a
specific step*:

    load_source -> filter_source -> extract_notes -> [fact_check] -> write_report

`load_source` reads the papers fetched from the open **arXiv** API (see
`source.py`); the rest are LLM steps. `ActaeCheckpointSaver` is the graph's
checkpointer, so every super-step is a durable Actae checkpoint — which is what
makes "fork after `extract_notes`" a one-liner and a mid-run kill resumable.

The model is swappable behind a tiny `Model` interface: the real OpenAI model
(`gpt-4o-mini-2024-07-18` by default) or a deterministic, data-driven
`ScriptedModel` for offline runs and tests (it extracts real claims from the
real abstracts with no API key). Every node records provider-reported token
usage into a `TokenLedger`, the basis of the savings report.
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, TypedDict

from langgraph.graph import END, START, StateGraph

from actae_client.adapters.langgraph import ActaeCheckpointSaver

import source as _source

DEFAULT_MODEL = os.environ.get("RESEARCH_MODEL", "gpt-4o-mini-2024-07-18")

# The report prompt is a named style so a fork can change it without touching
# the rest of the pipeline (the fork's tail uses a different `report_style`).
REPORT_STYLES = {
    "brief": (
        "Write a concise research brief for a busy reader: 4-6 bullet findings "
        "(each citing its arXiv id), then one sentence of outlook. Be factual "
        "and cite ids."
    ),
    "exec": (
        "Write an executive one-pager: a one-sentence 'Bottom line', then 3 "
        "punchy bullet findings (each citing its arXiv id), then a "
        "'Recommendation' line. Be decisive."
    ),
    "skeptical": (
        "Write a skeptical review: 3-5 bullet findings (each citing its arXiv "
        "id), each with a short caveat about methodology, then a one-sentence "
        "verdict on how much to trust this literature."
    ),
}


# ---------------------------------------------------------------------------
# State + token ledger
# ---------------------------------------------------------------------------
class ResearchState(TypedDict, total=False):
    topic: str
    papers: List[Dict[str, Any]]        # the whole arXiv result
    chunks: List[Dict[str, str]]        # addressable (id, title, abstract)
    selected: List[Dict[str, str]]      # papers the filter kept
    notes: List[str]                    # extracted claims
    fact_check: List[Dict[str, str]]    # [{claim, verdict, evidence}]
    report: str


class TokenLedger:
    """Per-step prompt/completion token sums, from the provider's own numbers."""

    def __init__(self) -> None:
        self._calls: Dict[str, int] = {}
        self._prompt: Dict[str, int] = {}
        self._completion: Dict[str, int] = {}
        self._elapsed_ms: Dict[str, int] = {}

    def record(self, step: str, prompt: int, completion: int, elapsed_ms: int) -> None:
        self._calls[step] = self._calls.get(step, 0) + 1
        self._prompt[step] = self._prompt.get(step, 0) + max(0, prompt)
        self._completion[step] = self._completion.get(step, 0) + max(0, completion)
        self._elapsed_ms[step] = self._elapsed_ms.get(step, 0) + max(0, elapsed_ms)

    def count(self, step: str) -> int:
        return self._calls.get(step, 0)

    def step_tokens(self, step: str) -> int:
        return self._prompt.get(step, 0) + self._completion.get(step, 0)

    def total_tokens(self) -> int:
        return sum(self.step_tokens(s) for s in self._calls)

    def total_prompt(self) -> int:
        return sum(self._prompt.values())

    def total_completion(self) -> int:
        return sum(self._completion.values())

    def total_elapsed_ms(self) -> int:
        return sum(self._elapsed_ms.values())

    def steps(self) -> List[str]:
        return list(self._calls)


# ---------------------------------------------------------------------------
# Model interface (real OpenAI, or a deterministic data-driven model)
# ---------------------------------------------------------------------------
@dataclass
class Completion:
    text: str
    prompt_tokens: int
    completion_tokens: int
    elapsed_ms: int
    model: str = DEFAULT_MODEL


class Model(Protocol):
    async def complete(
        self, *, system: str, user: str, step: str, json_mode: bool = False
    ) -> Completion: ...


class OpenAIModel:
    """The real model: OpenAI's chat completions API.

    JSON steps use the API's structured `json_object` response format (the
    model cannot wrap the JSON in prose), and transient failures (connection,
    timeout, rate limit, 5xx) are retried with exponential backoff + jitter.
    """

    _MAX_ATTEMPTS = 4

    def __init__(self, model: str = DEFAULT_MODEL, api_key: Optional[str] = None, temperature: float = 0.0):
        from openai import AsyncOpenAI

        key = api_key or os.environ.get("OPENAI_API_KEY")
        if not key:
            raise RuntimeError(
                "OPENAI_API_KEY is required for the real run (or use --dry-run)."
            )
        self._client = AsyncOpenAI(api_key=key, timeout=120)
        self._model = model
        self._temperature = temperature

    @property
    def name(self) -> str:
        return self._model

    async def complete(self, *, system: str, user: str, step: str, json_mode: bool = False) -> Completion:
        import asyncio
        import random

        from openai import APIConnectionError, APIStatusError, APITimeoutError, RateLimitError

        request: Dict[str, Any] = {
            "model": self._model,
            "temperature": self._temperature,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }
        if json_mode:
            request["response_format"] = {"type": "json_object"}

        delay = 0.5
        last_error: Exception = RuntimeError("unreachable")
        for attempt in range(self._MAX_ATTEMPTS):
            try:
                started = time.monotonic()
                response = await self._client.chat.completions.create(**request)
            except (APIConnectionError, APITimeoutError, RateLimitError) as err:
                last_error = err
            except APIStatusError as err:
                if err.status_code != 429 and err.status_code < 500:
                    raise  # 4xx (bad request, auth, …) is not retryable
                last_error = err
            else:
                elapsed_ms = int((time.monotonic() - started) * 1000)
                usage = response.usage
                return Completion(
                    text=(response.choices[0].message.content or "").strip(),
                    prompt_tokens=int(getattr(usage, "prompt_tokens", 0) or 0),
                    completion_tokens=int(getattr(usage, "completion_tokens", 0) or 0),
                    elapsed_ms=elapsed_ms,
                    model=self._model,
                )
            if attempt < self._MAX_ATTEMPTS - 1:
                await asyncio.sleep(delay + random.random() * 0.2)
                delay *= 2
        raise last_error


# A rendered source line: `[2401.12345] Some Title :: abstract text`.
_LINE_RE = re.compile(r"^\[([^\]]+)\]\s*(.*?)\s*::\s*(.*)$", re.M)
# An arXiv id referenced inside a claim: `(2401.12345)` or `arXiv:2401.12345`.
_CLAIM_ID_RE = re.compile(r"(?:arXiv:)?\(?([0-9]{4}\.[0-9]{4,5})\)?")


def rendered_ids(text: str) -> List[str]:
    return [m[0] for m in _LINE_RE.findall(text)]


def _first_sentence(text: str, limit: int = 200) -> str:
    text = text.strip()
    cut = re.split(r"(?<=[.!?])\s", text, maxsplit=1)[0]
    return cut if len(cut) <= limit else cut[: limit - 1] + "…"


class ScriptedModel:
    """Deterministic, real-data-driven model for offline runs and tests.

    It is *extractive*, not generative: it parses the real arXiv abstracts out
    of each prompt and returns real claims — no API key, no invented content —
    with token counts derived from the prompt/response length (clearly labelled
    synthetic in the report). It also injects ONE fabricated citation so the
    fact-checker has a hallucination to catch, exactly like a real model run.

    The parsing helpers are pure functions so the honesty of the numbers and the
    prompt contract are unit-tested offline.
    """

    SELECT_K = 8       # papers the filter keeps
    EXTRACT_M = 6      # claims the extractor emits (plus the injected one)
    FABRICATED = "0000.00000"

    @property
    def name(self) -> str:
        return "dry-run (scripted)"

    async def complete(self, *, system: str, user: str, step: str, json_mode: bool = False) -> Completion:
        text = self._respond(step, user, system)
        return Completion(
            text=text,
            prompt_tokens=max(1, len(user) // 4),
            completion_tokens=max(1, len(text) // 4),
            elapsed_ms=40,
        )

    def _respond(self, step: str, user: str, system: str) -> str:
        # JSON steps return an object (matching the real model's json_object
        # response format); the node's `coerce` reads the single key.
        if step == "filter_source":
            return json.dumps({"ids": rendered_ids(user)[: self.SELECT_K]})
        if step == "extract_notes":
            return json.dumps({"findings": self._claims(user)})
        if step == "fact_check":
            return json.dumps({"verdicts": self._verdicts(user)})
        if step == "write_report":
            return self._report(user, _style_of(system))
        return ""

    def _claims(self, user: str) -> List[str]:
        claims: List[str] = []
        for paper_id, title, abstract in _LINE_RE.findall(user)[: self.EXTRACT_M]:
            claims.append(f"{title} ({paper_id}) reports: {_first_sentence(abstract)}")
        # One fabricated citation — the hallucination the checker must catch.
        claims.append(
            f"A single approach now dominates the field ({self.FABRICATED})."
        )
        return claims

    def _verdicts(self, user: str) -> List[Dict[str, str]]:
        section = user.split("Findings:", 1)[-1].split("Papers:", 1)[0]
        try:
            claims = _extract_json(section)
        except ValueError:
            claims = []
        present = set(rendered_ids(user))
        verdicts: List[Dict[str, str]] = []
        for claim in claims:
            match = _CLAIM_ID_RE.search(str(claim))
            cited = match.group(1) if match else None
            if cited and cited in present:
                verdicts.append({"claim": str(claim), "verdict": "supported", "evidence": cited})
            else:
                verdicts.append(
                    {
                        "claim": str(claim),
                        "verdict": "unsupported",
                        "evidence": f"no retrieved paper {cited or '(none cited)'}",
                    }
                )
        return verdicts

    def _report(self, user: str, style: str) -> str:
        section = user.split("Findings:", 1)[-1]
        verification = None
        if "Verification results" in section:
            section, verification = section.split("Verification results", 1)
        findings = [line[2:].strip() for line in section.splitlines() if line.strip().startswith("- ")]
        bullets = []
        for finding in findings:
            clean = re.sub(r"\s*\([0-9]{4}\.[0-9]{4,5}\)", "", finding).strip()
            bullets.append(clean)
        cited_ids = set(_CLAIM_ID_RE.findall(section))

        if style == "exec":
            body = ["Executive one-pager", ""]
            body.append("Bottom line: the retrieved literature converges on a small set of operational themes.")
            body.append("")
            body += [f"- {b}" for b in bullets[:3]]
            body.append("")
            body.append("Recommendation: adopt the cited methods, but verify the citations before trusting the numbers.")
        elif style == "skeptical":
            body = ["Skeptical review", ""]
            for b in bullets[:5]:
                body.append(f"- {b}")
                body.append("    caveat: single-paper evidence; no independent replication shown here.")
            body.append("")
            body.append("Verdict: promising but thin — treat these findings as directional, not settled.")
        else:
            body = ["Research brief", ""]
            body += [f"- {b}" for b in bullets[: self.EXTRACT_M + 1]]

        if verification is not None:
            unsupported = re.findall(
                r'"claim":\s*"([^"]+)"[^}]*?"verdict":\s*"unsupported"', verification
            )
            if unsupported:
                body.append("")
                body.append(
                    "Corrections: %d finding(s) were not supported by a retrieved paper "
                    "and were removed (%s)." % (len(unsupported), ", ".join(_short(c) for c in unsupported))
                )
        body.append("")
        body.append("Based on %d retrieved arXiv papers." % len(cited_ids))
        return "\n".join(body)


def _style_of(system: str) -> str:
    """Which report style a system prompt asks for (drives the scripted render)."""
    lowered = system.lower()
    if "one-pager" in lowered:
        return "exec"
    if "skeptical" in lowered:
        return "skeptical"
    return "brief"


def _short(text: str, limit: int = 50) -> str:
    text = text.strip()
    return text if len(text) <= limit else text[: limit - 1] + "…"


# ---------------------------------------------------------------------------
# Prompt/parse helpers
# ---------------------------------------------------------------------------
class PipelineError(RuntimeError):
    """A pipeline step produced no usable output (loud, not silent)."""


def _first_json(text: str) -> Any:
    """The first complete JSON value in `text`, with a balanced scan.

    Scans from the first `{`/`[` for the matching close, honouring string
    literals and escapes — so a brace inside a string, or a second JSON value
    later in the text, cannot confuse it (the old greedy regex could).
    """
    start = next((i for i, ch in enumerate(text) if ch in "[{"), None)
    if start is None:
        raise ValueError("model reply contained no JSON")
    opener = text[start]
    closer = "]" if opener == "[" else "}"
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(text)):
        ch = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == opener:
            depth += 1
        elif ch == closer:
            depth -= 1
            if depth == 0:
                return json.loads(text[start : index + 1])
    raise ValueError("model reply contained unterminated JSON")


def _extract_json(text: str) -> Any:
    """Pull the first JSON value out of a model reply (fences tolerated)."""
    cleaned = text.strip()
    fenced = re.search(r"```(?:json)?\s*(.+?)\s*```", cleaned, re.DOTALL)
    if fenced:
        cleaned = fenced.group(1).strip()
    try:
        return json.loads(cleaned)
    except ValueError:
        return _first_json(cleaned)


def _render_chunks(chunks: List[Dict[str, str]], limit: int = 220) -> str:
    lines = []
    for chunk in chunks:
        text = chunk["text"]
        if len(text) > limit:
            text = text[: limit - 1] + "…"
        lines.append(f"[{chunk['id']}] {chunk['section']} :: {text}")
    return "\n".join(lines)


def _items(value: Any, key: str) -> List[Any]:
    """The array under `key` in a model's JSON object (loud on a bad shape)."""
    if isinstance(value, dict):
        value = value.get(key)
    if not isinstance(value, list):
        raise PipelineError(f"expected a JSON array under '{key}', got {type(value).__name__}")
    return value


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------
def build_graph(
    model: Model,
    saver: ActaeCheckpointSaver,
    ledger: TokenLedger,
    *,
    with_fact_check: bool,
    report_style: str = "brief",
):
    """Compile the pipeline.

    `with_fact_check` inserts the verification step; `report_style` selects the
    `write_report` prompt from `REPORT_STYLES` (so a fork can change the prompt
    without touching the rest of the pipeline).
    """
    report_system = REPORT_STYLES.get(report_style, REPORT_STYLES["brief"])

    async def _call(step: str, system: str, user: str, json_mode: bool = False) -> Completion:
        completion = await model.complete(system=system, user=user, step=step, json_mode=json_mode)
        ledger.record(step, completion.prompt_tokens, completion.completion_tokens, completion.elapsed_ms)
        return completion

    async def _call_json(step: str, system: str, user: str, coerce) -> Any:
        """A model call that must return JSON (structured output), retried once.

        The real model is asked for `json_object`; the reply is parsed with a
        balanced scan. A malformed reply is re-asked once, then surfaces
        loudly — a silently empty result is worse than a visible failure.
        """
        completion = await _call(step, system, user, json_mode=True)
        try:
            return coerce(_extract_json(completion.text))
        except (ValueError, TypeError):
            retry = await _call(
                step,
                system + " Return ONLY a valid JSON object, no prose or code fences.",
                user,
                json_mode=True,
            )
            return coerce(_extract_json(retry.text))

    async def load_source(state: ResearchState) -> Dict[str, Any]:
        # Ingests the real source: the cached arXiv result for the topic.
        snapshot = _source.get_papers(state["topic"])
        return {"papers": snapshot["papers"], "chunks": _source.chunks(snapshot)}

    async def filter_source(state: ResearchState) -> Dict[str, Any]:
        chunks = state["chunks"]
        ids = await _call_json(
            "filter_source",
            system=(
                "You are a research assistant. Select only the papers relevant "
                'to the question. Reply with a JSON object of the form '
                '{"ids": ["<arxiv id>", ...]}.'
            ),
            user=(
                f"Question: {state['topic']}\n\n"
                f"Papers:\n{_render_chunks(chunks, 220)}\n\n"
                "Reply with the JSON object of relevant arXiv ids."
            ),
            coerce=lambda value: [str(i) for i in _items(value, "ids")],
        )
        by_id = {c["id"]: c for c in chunks}
        selected = [by_id[i] for i in ids if i in by_id]
        if not selected:
            raise PipelineError(
                "filter_source kept no known papers (model returned no valid ids) — "
                "refusing to continue with an empty source"
            )
        return {"selected": selected}

    async def extract_notes(state: ResearchState) -> Dict[str, Any]:
        notes = await _call_json(
            "extract_notes",
            system=(
                "Extract the key findings from these papers. Each finding is a "
                "single sentence and MUST cite the paper's arXiv id in "
                'parentheses, e.g. "X improves Y (2401.12345)." Reply with a JSON '
                'object of the form {"findings": ["<finding>", ...]}.'
            ),
            user=(
                f"Question: {state['topic']}\n\n"
                f"Papers:\n{_render_chunks(state['selected'], 600)}\n\n"
                "Reply with the JSON object of findings."
            ),
            coerce=lambda value: [str(n) for n in _items(value, "findings")],
        )
        if not notes:
            raise PipelineError("extract_notes produced no findings")
        return {"notes": notes}

    async def fact_check(state: ResearchState) -> Dict[str, Any]:
        verdicts = await _call_json(
            "fact_check",
            system=(
                "You are a strict fact-checker. For each finding, decide whether "
                "the cited arXiv paper (by id) actually supports it. Reply with a "
                'JSON object of the form {"verdicts": [{"claim": "...", '
                '"verdict": "supported|partial|unsupported", "evidence": "..."}]}.'
            ),
            user=(
                f"Findings:\n{json.dumps(state['notes'], indent=2)}\n\n"
                f"Papers:\n{_render_chunks(state['selected'], 600)}\n\n"
                "Reply with the JSON object of verdicts."
            ),
            coerce=lambda value: _items(value, "verdicts"),
        )
        if not verdicts or not all(isinstance(v, dict) for v in verdicts):
            raise PipelineError("fact_check produced no usable verdicts")
        return {"fact_check": verdicts}

    async def write_report(state: ResearchState) -> Dict[str, Any]:
        checked = state.get("fact_check")
        verification = ""
        if checked:
            verification = (
                "\n\nVerification results (drop or correct any finding that is "
                "not fully supported by its cited paper, and add a short "
                "corrections note):\n" + json.dumps(checked, indent=2)
            )
        completion = await _call(
            "write_report",
            system=report_system,
            user=(
                f"Question: {state['topic']}\n\n"
                f"Findings:\n" + "\n".join(f"- {n}" for n in state["notes"]) + verification
            ),
        )
        return {"report": completion.text}

    graph = StateGraph(ResearchState)
    graph.add_node("load_source", load_source)
    graph.add_node("filter_source", filter_source)
    graph.add_node("extract_notes", extract_notes)
    graph.add_node("write_report", write_report)
    graph.add_edge(START, "load_source")
    graph.add_edge("load_source", "filter_source")
    graph.add_edge("filter_source", "extract_notes")
    if with_fact_check:
        graph.add_node("fact_check", fact_check)
        graph.add_edge("extract_notes", "fact_check")
        graph.add_edge("fact_check", "write_report")
    else:
        graph.add_edge("extract_notes", "write_report")
    graph.add_edge("write_report", END)
    return graph.compile(checkpointer=saver)


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------
_NODE_OUTPUT = {
    "load_source": "chunks",
    "filter_source": "selected",
    "extract_notes": "notes",
    "fact_check": "fact_check",
    "write_report": "report",
}


async def checkpoint_after(graph: Any, config: dict, node: str) -> Optional[dict]:
    """The resume config for the checkpoint produced by `node` (fork boundary)."""
    key = _NODE_OUTPUT[node]
    snapshot = await graph.aget_state(config)
    while snapshot is not None:
        values = snapshot.values or {}
        if key in values and snapshot.next:
            return snapshot.config
        if snapshot.parent_config is None:
            return None
        snapshot = await graph.aget_state(snapshot.parent_config)
    return None


def report_of(state: Dict[str, Any]) -> str:
    return str(state.get("report") or "").strip()
