# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Repository-maintenance agent for the Actae demo.

The agent runs a real software-engineering workflow against a copied repo:
inspect → locate → plan → edit → run tests → react → submit a report. Every
step is recorded on an ``AgentSession``; the side-effecting operations (edits
and the report submission) are guarded by Actae's idempotent execution ledger,
so a crash cannot fire them twice.

Two policies:

* ``DryRunPolicy`` — a deterministic scripted trajectory (no API key). This is
  the reproducibility/evidence path.
* ``LLMPolicy`` — the model chooses the fix given the real test output over an
  OpenAI-compatible chat endpoint (DeepSeek). The scaffolding and verification
  stay deterministic; only the edit decision is model-driven.

The report submission targets a local HTTP task service (reused from
``examples/durability_benchmark.py``) so an ambiguous outcome can be
reconciled by querying the service instead of blindly re-submitting.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_BENCH = Path(__file__).resolve().parent
_EXAMPLES = _BENCH.parent
if str(_EXAMPLES) not in sys.path:
    sys.path.insert(0, str(_EXAMPLES))

from durability_benchmark import TransportError, apply_effect, status_effect  # noqa: E402

from actae_client import ActaeClient  # noqa: E402
from actae_client.session import AgentSession  # noqa: E402

from tools import CORRECT_MEDIAN_FIX, WRONG_EDIT, RepoWorkspace  # noqa: E402

STATS = "calc/stats.py"

# Step numbers (1-based). Steps 1..5 are shared prefix; the fork diverges at 6.
STEP_INSPECT = 1
STEP_LOCATE = 2
STEP_PLAN = 3
STEP_EDIT = 4
STEP_TEST = 5
STEP_REACT_EDIT = 6
STEP_REACT_TEST = 7
STEP_REPORT = 8
PIPELINE_STEPS = 8

REPORT_KEY = "report-1"
REPORT_DOWNSTREAM_KEY = "repo-report-1"


@dataclass
class RunResult:
    channel_id: str
    final_hash: str
    tests_passed: bool
    report_status: str
    invoked_steps: List[int] = field(default_factory=list)
    test_outputs: List[str] = field(default_factory=list)


def _wrong_edit(workspace: RepoWorkspace) -> bool:
    old, new = WRONG_EDIT
    return workspace.edit_file(STATS, old, new)


def _correct_edit(workspace: RepoWorkspace) -> bool:
    old, new = CORRECT_MEDIAN_FIX
    return workspace.edit_file(STATS, old, new)


class DryRunPolicy:
    """Deterministic trajectory: a no-op baseline, or the correct fix."""

    name = "dry-run"

    def __init__(self, correct: bool = True) -> None:
        self.correct = correct
        self.variant = "median-fix" if correct else "baseline-giveup"

    def react_edit(self, workspace: RepoWorkspace, last_test_output: str) -> Tuple[str, bool]:
        if not self.correct:
            return "baseline gives up (no further change)", False
        return "apply median even-length fix", _correct_edit(workspace)


class LLMPolicy:
    """Real model chooses the fix from the failing test output."""

    name = "llm"
    variant = "llm-react"

    def __init__(self, model: Optional[str] = None, chat_fn: Optional[Any] = None) -> None:
        _load_deepseek_key()
        self.model = model or os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")
        self.base_url = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
        # Injectable transport: tests pass a deterministic responder so the
        # model decision is reproducible without a network call.
        self._chat_fn = chat_fn

    def _chat(self, system: str, user: str) -> str:
        if self._chat_fn is not None:
            return self._chat_fn(system, user)
        import urllib.error
        import urllib.request

        api_key = os.environ.get("DEEPSEEK_API_KEY")
        if not api_key:
            raise RuntimeError(
                "DEEPSEEK_API_KEY is required for LLM mode (run with --dry-run otherwise)"
            )
        body = json.dumps(
            {
                "model": self.model,
                "temperature": 0,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            }
        ).encode()
        request = urllib.request.Request(
            self.base_url.rstrip("/") + "/chat/completions",
            data=body,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            payload = json.loads(response.read())
        return payload["choices"][0]["message"]["content"] or ""

    def react_edit(self, workspace: RepoWorkspace, last_test_output: str) -> Tuple[str, bool]:
        source = workspace.read_file(STATS)
        prompt = (
            "A failing pytest run and the source of calc/stats.py follow. "
            "Choose the single correct fix. Reply with STRICT JSON only: "
            '{"fix": "median" | "average", "reason": "..."}.\n\n'
            f"TEST OUTPUT:\n{last_test_output}\n\nSOURCE:\n{source}\n"
        )
        try:
            raw = self._chat(
                "You are a careful Python maintainer. Answer with JSON only.", prompt
            )
            choice = json.loads(_extract_json(raw)).get("fix", "median")
        except Exception:
            choice = "median"
        if choice == "average":
            return "model chose average fix", _wrong_edit(workspace)
        return "model chose median fix", _correct_edit(workspace)


def _extract_json(text: str) -> str:
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError(f"no JSON object in model output: {text!r}")
    return text[start : end + 1]


def _load_deepseek_key() -> None:
    """Populate DEEPSEEK_API_KEY from the dev checkout's ``actae/.env`` if set."""
    if os.environ.get("DEEPSEEK_API_KEY"):
        return
    env_file = _EXAMPLES.parent / "actae" / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        if line.startswith("DEEPSEEK_API_KEY="):
            os.environ["DEEPSEEK_API_KEY"] = line.split("=", 1)[1].strip().strip('"').strip("'")
            return


def _ledger_result(claim: Any) -> Optional[Dict[str, Any]]:
    if claim.status == "replayed":
        return claim.result if isinstance(claim.result, dict) else {}
    return None


class RepoAgent:
    """Runs the maintenance pipeline against one channel/session."""

    def __init__(
        self,
        client: ActaeClient,
        session: AgentSession,
        workspace: RepoWorkspace,
        task_service_url: str,
        policy: Any,
        report_key: str = REPORT_KEY,
        report_downstream_key: str = REPORT_DOWNSTREAM_KEY,
        state_sink: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.client = client
        self.session = session
        self.workspace = workspace
        self.task_service_url = task_service_url
        self.policy = policy
        self.report_key = report_key
        self.report_downstream_key = report_downstream_key
        self.state_sink = state_sink
        self.invoked: List[int] = []
        self.test_outputs: List[str] = []

    async def _record(self, step: int, step_type: str, **kwargs: Any) -> None:
        self.invoked.append(step)
        if self.state_sink is not None:
            self.state_sink["invoked"] = list(self.invoked)
        await self.session.step(step_type, **kwargs)

    async def _claim(self, step: int, key: str, tool: str, params: Dict[str, Any]):
        return await self.client.claim_execution(
            self.session.channel_id, key, tool, params, lease_seconds=2
        )

    async def run(
        self,
        *,
        start_step: int = 1,
        crash_at: Optional[int] = None,
        report_already_recorded: bool = False,
        stop_after: Optional[int] = None,
    ) -> RunResult:
        """Execute steps ``start_step``..``PIPELINE_STEPS``.

        ``crash_at`` kills the process (exit 137) immediately after that step's
        side effect, before the step is recorded — the ambiguous window.
        ``report_already_recorded`` lets a resume skip re-issuing a report whose
        live outcome was already reconciled.
        """
        tests_passed = False

        for step in range(start_step, PIPELINE_STEPS + 1):
            if stop_after is not None and step > stop_after:
                break

            if step == STEP_INSPECT:
                source = self.workspace.read_file(STATS)
                await self._record(step, "inspect", input={"file": STATS}, output={"chars": len(source)})

            elif step == STEP_LOCATE:
                hits = self.workspace.search("median")
                await self._record(step, "locate", input={"needle": "median"}, output={"hits": len(hits)})

            elif step == STEP_PLAN:
                await self._record(
                    step, "plan", output={"plan": ["edit median", "run tests", "submit report"]}
                )

            elif step == STEP_EDIT:
                key = f"edit-{step}"
                claim = await self._claim(step, key, "repo.edit", {"file": STATS, "variant": "first"})
                if claim.status == "replayed":
                    changed = bool((_ledger_result(claim) or {}).get("changed"))
                else:
                    changed = _wrong_edit(self.workspace)
                    await self.client.complete_execution(
                        claim.execution.id, claim.claim_token,
                        result={"changed": changed, "sha256": self.workspace.file_hash(STATS)},
                    )
                    if crash_at == step:
                        _die()
                await self._record(
                    step, "edit", input={"file": STATS}, output={"changed": changed}
                )

            elif step in (STEP_TEST, STEP_REACT_TEST):
                key = f"test-{step}"
                # The verification identity includes the code under test, so a
                # fork that changed the code re-runs (rather than replaying a
                # stale inherited result).
                claim = await self._claim(
                    step, key, "repo.test",
                    {"suite": "tests", "subject_sha256": self.workspace.file_hash(STATS)},
                )
                if claim.status == "replayed":
                    saved = _ledger_result(claim) or {}
                    rc, output = int(saved.get("returncode", 1)), str(saved.get("output", ""))
                else:
                    rc, output = self.workspace.run_tests()
                    await self.client.complete_execution(
                        claim.execution.id, claim.claim_token,
                        result={"returncode": rc, "output": output[-500:]},
                    )
                    if crash_at == step:
                        _die()
                tests_passed = rc == 0
                self.test_outputs.append(output)
                await self._record(
                    step, "test", output={"returncode": rc, "passed": tests_passed}
                )

            elif step == STEP_REACT_EDIT:
                key = f"edit-{step}"
                # Fork-specific work must be request-distinct: a fork's edit
                # carries its own variant, so it does not replay the parent's.
                claim = await self._claim(
                    step, key, "repo.edit",
                    {"file": STATS, "variant": getattr(self.policy, "variant", "react")},
                )
                if claim.status == "replayed":
                    changed = bool((_ledger_result(claim) or {}).get("changed"))
                    reason = "replayed"
                else:
                    reason, changed = self.policy.react_edit(self.workspace, self.test_outputs[-1] if self.test_outputs else "")
                    await self.client.complete_execution(
                        claim.execution.id, claim.claim_token,
                        result={"changed": changed, "sha256": self.workspace.file_hash(STATS)},
                    )
                    if crash_at == step:
                        _die()
                await self._record(
                    step, "edit", input={"file": STATS}, output={"changed": changed, "note": reason}
                )

            elif step == STEP_REPORT:
                if report_already_recorded:
                    await self._record(
                        step, "submit_report", output={"status": "already_reconciled"}
                    )
                    continue
                claim = await self._claim(
                    step, self.report_key, "repo.report",
                    {"task": "repo-maintenance", "version": 1},
                )
                if claim.status == "replayed":
                    status = "replayed"
                elif claim.status == "reclaimed" and status_effect(
                    "http", self.task_service_url, self.report_key
                ).get("applied"):
                    # Ambiguous prior attempt: the report already landed;
                    # reconcile instead of re-submitting (durable_query).
                    await self.client.complete_execution(
                        claim.execution.id, claim.claim_token,
                        result={"report": self.report_key, "reconciled": True},
                    )
                    status = "reconciled"
                else:
                    try:
                        apply_effect(
                            "http", self.task_service_url,
                            self.report_key, self.report_downstream_key, True,
                        )
                    except TransportError:
                        # Ambiguous: the report may or may not have landed.
                        # Reconcile by querying the task service (the
                        # durable_query pattern); do NOT blindly re-submit.
                        prior = status_effect("http", self.task_service_url, self.report_key)
                        if not prior.get("applied"):
                            apply_effect(
                                "http", self.task_service_url,
                                self.report_key, self.report_downstream_key, True,
                            )
                    if crash_at == step:
                        _die()
                    await self.client.complete_execution(
                        claim.execution.id, claim.claim_token,
                        result={"report": self.report_key, "sha256": self.workspace.file_hash(STATS)},
                    )
                    status = "submitted"
                await self._record(step, "submit_report", output={"status": status})

        return RunResult(
            channel_id=self.session.channel_id,
            final_hash=self.workspace.file_hash(STATS),
            tests_passed=tests_passed,
            report_status="submitted",
            invoked_steps=list(self.invoked),
            test_outputs=list(self.test_outputs),
        )


def _die() -> None:
    sys.stdout.flush()
    os._exit(137)
