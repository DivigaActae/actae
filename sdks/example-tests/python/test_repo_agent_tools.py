# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the repo-agent demo's tools and deterministic policy.

No server and no LLM: these pin the real filesystem/test behavior and the
scripted trajectory so the demo's deterministic evidence path cannot silently
break.

    python3 -m pytest sdks/example-tests/python/test_repo_agent_tools.py -v
"""

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO / "examples"))
sys.path.insert(0, str(_REPO / "examples" / "repo_agent"))

from tools import (  # noqa: E402
    CORRECT_MEDIAN_FIX,
    FIXTURE,
    WRONG_EDIT,
    RepoWorkspace,
    expected_fixed_hash,
    seed_workspace,
)


def test_fixture_starts_with_a_failing_test(tmp_path):
    workspace = RepoWorkspace(seed_workspace(tmp_path / "ws"))
    rc, output = workspace.run_tests()
    assert rc != 0
    assert "test_median_even" in output


def test_correct_median_fix_makes_the_suite_pass(tmp_path):
    workspace = RepoWorkspace(seed_workspace(tmp_path / "ws"))
    old, new = CORRECT_MEDIAN_FIX
    assert workspace.edit_file("calc/stats.py", old, new) is True
    rc, output = workspace.run_tests()
    assert rc == 0, output
    assert workspace.file_hash("calc/stats.py") == expected_fixed_hash(FIXTURE)


def test_wrong_edit_changes_the_file_but_leaves_tests_failing(tmp_path):
    workspace = RepoWorkspace(seed_workspace(tmp_path / "ws"))
    old, new = WRONG_EDIT
    assert workspace.edit_file("calc/stats.py", old, new) is True
    rc, _ = workspace.run_tests()
    assert rc != 0
    # The known-correct fix is NOT present.
    assert CORRECT_MEDIAN_FIX[1] not in workspace.read_file("calc/stats.py")


def test_edit_is_a_noop_when_anchor_is_absent(tmp_path):
    workspace = RepoWorkspace(seed_workspace(tmp_path / "ws"))
    assert workspace.edit_file("calc/stats.py", "not present", "x") is False


def test_search_finds_the_median_definition(tmp_path):
    workspace = RepoWorkspace(seed_workspace(tmp_path / "ws"))
    hits = workspace.search("def median")
    assert len(hits) == 1
    assert hits[0]["file"] == "calc/stats.py"


def test_seed_workspace_does_not_mutate_the_fixture(tmp_path):
    fixture = _REPO / "examples" / "repo_agent" / "fixture_repo" / "calc" / "stats.py"
    before = fixture.read_bytes()
    seed_workspace(tmp_path / "ws")
    assert fixture.read_bytes() == before


def test_dry_run_policy_correct_and_baseline(tmp_path):
    from agent import DryRunPolicy  # noqa: E402

    correct_ws = RepoWorkspace(seed_workspace(tmp_path / "correct"))
    _, changed = DryRunPolicy(correct=True).react_edit(correct_ws, "")
    assert changed is True
    assert correct_ws.file_hash("calc/stats.py") == expected_fixed_hash(FIXTURE)

    baseline_ws = RepoWorkspace(seed_workspace(tmp_path / "baseline"))
    _, changed = DryRunPolicy(correct=False).react_edit(baseline_ws, "")
    assert changed is False


# --------------------------------------------------------------------------- #
# RepoAgent orchestration (fake ledger, real task service, no Actae server)
# --------------------------------------------------------------------------- #


class _FakeClaim:
    def __init__(self, status, execution_id, token, result=None):
        self.status = status
        self.execution = type("Exec", (), {"id": execution_id})()
        self.claim_token = token
        self.result = result


class _FakeLedgerClient:
    """In-memory stand-in for ActaeClient's execution ledger."""

    def __init__(self):
        self.executions = {}
        self.applied_channels = set()

    async def claim_execution(self, channel_id, key_name, tool_name, params, *, lease_seconds=None):
        record = self.executions.get((channel_id, key_name))
        if record is not None and record["status"] == "completed":
            return _FakeClaim("replayed", key_name, None, result=record["result"])
        token = f"{key_name}#{len(self.executions)}"
        self.executions[(channel_id, key_name)] = {
            "status": "running", "token": token, "result": None
        }
        return _FakeClaim("claimed", f"{channel_id}:{key_name}", token)

    async def complete_execution(self, execution_id, claim_token=None, result=None):
        for (channel_id, key_name), record in self.executions.items():
            if f"{channel_id}:{key_name}" == execution_id:
                assert record["token"] == claim_token
                record["status"] = "completed"
                record["result"] = result
                return record
        raise AssertionError(f"unknown execution {execution_id}")


class _FakeSession:
    def __init__(self, channel_id):
        self.channel_id = channel_id
        self.steps = []

    async def step(self, step_type, **kwargs):
        self.steps.append((step_type, kwargs))


def _run_agent(tmp_path, channel, correct, task_url, reuse=None):
    import asyncio

    from agent import DryRunPolicy, RepoAgent  # noqa: E402

    workspace_dir = tmp_path / f"ws-{channel}"
    if reuse is None:
        workspace = RepoWorkspace(seed_workspace(workspace_dir))
        client = _FakeLedgerClient()
        session = _FakeSession(channel)
    else:
        workspace, client, session = reuse
    state = {"invoked": []}
    agent = RepoAgent(
        client, session, workspace, task_url, DryRunPolicy(correct=correct),
        report_downstream_key="unit-report", state_sink=state,
    )
    result = asyncio.run(agent.run(start_step=1))
    return result, (workspace, client, session)


def test_repo_agent_full_pipeline_baseline_and_correct_fix(tmp_path):
    from durability_benchmark import HttpEffectService, _http_request  # noqa: E402

    service = HttpEffectService()
    url = service.start()
    try:
        baseline, _ = _run_agent(tmp_path, "unit-base", correct=False, task_url=url)
        assert baseline.tests_passed is False
        assert baseline.invoked_steps == [1, 2, 3, 4, 5, 6, 7, 8]
        apps = sum(1 for r in _http_request(url, "GET", "/effects")["rows"] if r["downstream_key"] == "unit-report")
        assert apps == 1

        forked, _ = _run_agent(tmp_path, "unit-fork", correct=True, task_url=url)
        assert forked.tests_passed is True
        assert forked.invoked_steps == [1, 2, 3, 4, 5, 6, 7, 8]
    finally:
        service.stop()


def test_repo_agent_report_is_idempotent_across_reruns(tmp_path):
    from durability_benchmark import HttpEffectService, _http_request  # noqa: E402

    service = HttpEffectService()
    url = service.start()
    try:
        _, reuse = _run_agent(tmp_path, "unit-idem", correct=False, task_url=url)
        # Re-run the same channel: the report key replays, the effect does not fire again.
        second, _ = _run_agent(tmp_path, "unit-idem", correct=False, task_url=url, reuse=reuse)
        assert second.invoked_steps == [1, 2, 3, 4, 5, 6, 7, 8]
        apps = sum(1 for r in _http_request(url, "GET", "/effects")["rows"] if r["downstream_key"] == "unit-report")
        assert apps == 1
    finally:
        service.stop()


# --------------------------------------------------------------------------- #
# LLMPolicy determinism (injected responder, no network)
# --------------------------------------------------------------------------- #


def test_llm_policy_median_choice_is_deterministic(tmp_path):
    from agent import LLMPolicy  # noqa: E402

    workspace = RepoWorkspace(seed_workspace(tmp_path / "llm-median"))
    policy = LLMPolicy(chat_fn=lambda _s, _u: 'Sure: {"fix": "median", "reason": "even"}')
    _, changed = policy.react_edit(workspace, "1 failed")
    assert changed is True
    assert workspace.file_hash("calc/stats.py") == expected_fixed_hash(FIXTURE)


def test_llm_policy_average_choice_applies_the_wrong_edit(tmp_path):
    from agent import LLMPolicy  # noqa: E402

    workspace = RepoWorkspace(seed_workspace(tmp_path / "llm-average"))
    policy = LLMPolicy(chat_fn=lambda _s, _u: '{"fix": "average"}')
    _, changed = policy.react_edit(workspace, "1 failed")
    assert changed is True
    assert CORRECT_MEDIAN_FIX[1] not in workspace.read_file("calc/stats.py")


def test_llm_policy_defaults_to_median_on_malformed_output(tmp_path):
    from agent import LLMPolicy  # noqa: E402

    workspace = RepoWorkspace(seed_workspace(tmp_path / "llm-bad"))
    policy = LLMPolicy(chat_fn=lambda _s, _u: "I am not going to answer in JSON")
    _, changed = policy.react_edit(workspace, "fail")
    assert changed is True
    assert CORRECT_MEDIAN_FIX[1] in workspace.read_file("calc/stats.py")
