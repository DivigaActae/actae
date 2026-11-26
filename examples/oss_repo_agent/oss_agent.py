# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""OSS repo agent: a real agent on a real public repository.

Pipeline (8 steps), all recorded on an ``AgentSession``:

    clone → survey → locate → plan? → edit → test → react(edit) → test → report

Concretely: ``clone(1) survey(2) locate(3) edit(4) test(5) react_edit(6)
react_test(7) report(8)``. The side-effecting edits, test runs and the report
are guarded by Actae's idempotent execution ledger. The baseline makes a
plausible-but-wrong edit; the fork at the failing-test step applies the correct
fix. Two policies:

* ``DeterministicPolicy`` — scripted (no API key): the evidence path.
* ``LLMPolicy`` — DeepSeek chooses between the two candidate fixes from the
  real pytest output over an OpenAI-compatible endpoint.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from oss_tools import RepoWorkspace  # noqa: E402

from actae_client import ActaeClient  # noqa: E402
from actae_client.session import AgentSession  # noqa: E402

STEP_CLONE = 1
STEP_SURVEY = 2
STEP_LOCATE = 3
STEP_EDIT = 4
STEP_TEST = 5
STEP_REACT_EDIT = 6
STEP_REACT_TEST = 7
STEP_REPORT = 8
PIPELINE_STEPS = 8
REPORT_KEY = "oss-report-1"


@dataclass
class RunResult:
    channel_id: str
    final_hash: str
    tests_passed: bool
    report_status: str
    invoked_steps: List[int] = field(default_factory=list)
    test_outputs: List[str] = field(default_factory=list)


def _ledger_result(claim: Any) -> Optional[Dict[str, Any]]:
    if claim.status == "replayed":
        return claim.result if isinstance(claim.result, dict) else {}
    return None


class DeterministicPolicy:
    name = "dry-run"

    def __init__(self, correct: bool = True) -> None:
        self.correct = correct
        self.variant = "correct-fix" if correct else "baseline-giveup"

    def react_edit(
        self, workspace: RepoWorkspace, task: Dict[str, Any], last_test_output: str
    ) -> Tuple[str, bool]:
        if not self.correct:
            return "baseline gives up (no further change)", False
        return "apply the correct fix", workspace.edit_file(task["target_file"], *task["correct_fix"])


class LLMPolicy:
    name = "llm"
    variant = "llm-react"

    def __init__(self, model: Optional[str] = None, chat_fn: Optional[Any] = None) -> None:
        _load_deepseek_key()
        self.model = model or os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")
        self.base_url = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
        self._chat_fn = chat_fn

    def _chat(self, system: str, user: str) -> str:
        if self._chat_fn is not None:
            return self._chat_fn(system, user)
        import urllib.request

        api_key = os.environ.get("DEEPSEEK_API_KEY")
        if not api_key:
            raise RuntimeError("DEEPSEEK_API_KEY is required for --llm mode")
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

    def react_edit(
        self, workspace: RepoWorkspace, task: Dict[str, Any], last_test_output: str
    ) -> Tuple[str, bool]:
        source = workspace.read_file(task["target_file"])
        prompt = (
            "A failing pytest run and the source of the offending file follow. "
            "This repository serializes integers; the test expects the minimal "
            "big-endian representation (leading zero bytes removed). Choose the "
            "single correct fix. Reply with STRICT JSON only: "
            '{"fix": "lstrip" | "rstrip", "reason": "..."}.\n\n'
            f"TEST OUTPUT:\n{last_test_output}\n\nSOURCE:\n{source}\n"
        )
        try:
            raw = self._chat("You are a careful Python maintainer. Answer with JSON only.", prompt)
            choice = json.loads(_extract_json(raw)).get("fix", "lstrip")
        except Exception:
            choice = "lstrip"
        if choice == "rstrip":
            # The workspace already carries the baseline's wrong `.rstrip`
            # attempt, so choosing rstrip means "leave it wrong" — no edit.
            return "model chose rstrip (wrong — inherited wrong fix stays)", False
        return "model chose lstrip (correct)", workspace.edit_file(
            task["target_file"], *task["correct_fix"]
        )


def _extract_json(text: str) -> str:
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError(f"no JSON object in model output: {text!r}")
    return text[start : end + 1]


def _load_deepseek_key() -> None:
    if os.environ.get("DEEPSEEK_API_KEY"):
        return
    env_file = _HERE.parent.parent / "actae" / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        if line.startswith("DEEPSEEK_API_KEY="):
            os.environ["DEEPSEEK_API_KEY"] = line.split("=", 1)[1].strip().strip('"').strip("'")
            return


class OssRepoAgent:
    """Runs the pipeline for one task against one channel/session."""

    def __init__(
        self,
        client: ActaeClient,
        session: AgentSession,
        workspace: RepoWorkspace,
        task: Dict[str, Any],
        policy: Any,
        repo_commit: str = "",
        report_path: Optional[Path] = None,
        state_sink: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.client = client
        self.session = session
        self.workspace = workspace
        self.task = task
        self.policy = policy
        self.repo_commit = repo_commit
        self.report_path = report_path
        self.state_sink = state_sink
        self.invoked: List[int] = []
        self.test_outputs: List[str] = []

    async def _record(self, step: int, step_type: str, **kwargs: Any) -> None:
        self.invoked.append(step)
        if self.state_sink is not None:
            self.state_sink["invoked"] = list(self.invoked)
        await self.session.step(step_type, **kwargs)

    async def _claim(self, key: str, tool: str, params: Dict[str, Any]):
        return await self.client.claim_execution(
            self.session.channel_id, key, tool, params, lease_seconds=5
        )

    async def run(self, *, start_step: int = 1) -> RunResult:
        target = self.task["target_file"]
        test_path = self.task["test_path"]
        tests_passed = False

        for step in range(start_step, PIPELINE_STEPS + 1):
            if step == STEP_CLONE:
                await self._record(
                    step,
                    "clone",
                    input={"repo": self.task["repo_url"]},
                    output={"commit": self.repo_commit, "license": self.task.get("license")},
                )

            elif step == STEP_SURVEY:
                py = self.workspace.search("def ", "*.py")
                await self._record(
                    step, "survey", output={"python_defs": len(py), "root": str(self.workspace.root.name)}
                )

            elif step == STEP_LOCATE:
                hits = self.workspace.search(self.task["locate_needle"])
                await self._record(
                    step,
                    "locate",
                    input={"needle": self.task["locate_needle"]},
                    output={"hits": [h["file"] for h in hits]},
                )

            elif step == STEP_EDIT:
                key = f"edit-{step}"
                claim = await self._claim(key, "repo.edit", {"file": target, "variant": "first"})
                if claim.status == "replayed":
                    changed = bool((_ledger_result(claim) or {}).get("changed"))
                else:
                    changed = self.workspace.edit_file(target, *self.task["wrong_fix"])
                    await self.client.complete_execution(
                        claim.execution.id,
                        claim.claim_token,
                        result={"changed": changed, "sha256": self.workspace.file_hash(target)},
                    )
                await self._record(step, "edit", input={"file": target}, output={"changed": changed})

            elif step in (STEP_TEST, STEP_REACT_TEST):
                key = f"test-{step}"
                claim = await self._claim(
                    key,
                    "repo.test",
                    {"suite": test_path, "subject_sha256": self.workspace.file_hash(target)},
                )
                if claim.status == "replayed":
                    saved = _ledger_result(claim) or {}
                    rc = int(saved.get("returncode", 1))
                    output = str(saved.get("output", ""))
                else:
                    rc, output = self.workspace.run_tests(test_path)
                    await self.client.complete_execution(
                        claim.execution.id,
                        claim.claim_token,
                        result={"returncode": rc, "output": output[-500:]},
                    )
                tests_passed = rc == 0
                self.test_outputs.append(output)
                await self._record(
                    step, "test", input={"suite": test_path}, output={"returncode": rc, "passed": tests_passed}
                )

            elif step == STEP_REACT_EDIT:
                key = f"edit-{step}"
                claim = await self._claim(
                    key,
                    "repo.edit",
                    {"file": target, "variant": getattr(self.policy, "variant", "react")},
                )
                if claim.status == "replayed":
                    changed = bool((_ledger_result(claim) or {}).get("changed"))
                    note = "replayed"
                else:
                    note, changed = self.policy.react_edit(
                        self.workspace, self.task, self.test_outputs[-1] if self.test_outputs else ""
                    )
                    await self.client.complete_execution(
                        claim.execution.id,
                        claim.claim_token,
                        result={"changed": changed, "sha256": self.workspace.file_hash(target)},
                    )
                await self._record(
                    step, "edit", input={"file": target}, output={"changed": changed, "note": note}
                )

            elif step == STEP_REPORT:
                claim = await self._claim(
                    REPORT_KEY, "repo.report", {"task": self.task["name"], "version": 1}
                )
                if claim.status == "replayed":
                    status = "replayed"
                else:
                    if self.report_path is not None:
                        self.report_path.parent.mkdir(parents=True, exist_ok=True)
                        self.report_path.write_text(
                            json.dumps(
                                {
                                    "task": self.task["name"],
                                    "repo": self.task["repo_url"],
                                    "commit": self.repo_commit,
                                    "channel": self.session.channel_id,
                                    "tests_passed": tests_passed,
                                    "target_sha256": self.workspace.file_hash(target),
                                },
                                indent=2,
                            )
                            + "\n",
                            encoding="utf-8",
                        )
                    await self.client.complete_execution(
                        claim.execution.id,
                        claim.claim_token,
                        result={"report": REPORT_KEY, "sha256": self.workspace.file_hash(target)},
                    )
                    status = "submitted"
                await self._record(step, "submit_report", output={"status": status})

        return RunResult(
            channel_id=self.session.channel_id,
            final_hash=self.workspace.file_hash(target),
            tests_passed=tests_passed,
            report_status="submitted",
            invoked_steps=list(self.invoked),
            test_outputs=list(self.test_outputs),
        )
