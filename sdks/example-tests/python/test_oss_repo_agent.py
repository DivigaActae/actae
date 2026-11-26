# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Offline unit tests for examples/oss_repo_agent (no network, no Actae server).

Covers the pure logic: task-anchor integrity, workspace tools, the deterministic
and LLM policies. The live end-to-end run needs a running Actae + a network
clone and is exercised by the demo, not here.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT / "examples" / "oss_repo_agent"))

import oss_agent  # noqa: E402
import oss_tasks  # noqa: E402
from oss_tools import RepoWorkspace, apply_mutation, prepare_workspace  # noqa: E402

CORRECT_SOURCE = (
    "def int_to_bytes(num: int) -> bytes:\n"
    '    return _int_to_bytes(num).lstrip(b"\\x00")\n'
    "\n"
    "def other() -> int:\n"
    "    return 1\n"
)


def _fake_cache(tmp_path: Path) -> Path:
    repo = tmp_path / "cache_repo"
    target = repo / "src" / "itsdangerous" / "encoding.py"
    target.parent.mkdir(parents=True)
    target.write_text(CORRECT_SOURCE, encoding="utf-8")
    return repo


def _seeded_workspace(tmp_path: Path) -> tuple[RepoWorkspace, dict]:
    task = oss_tasks.get_task(oss_tasks.DEFAULT_TASK)
    ws_dir = tmp_path / "ws"
    prepare_workspace(_fake_cache(tmp_path), ws_dir)
    apply_mutation(ws_dir, task["target_file"], task["mutation"])
    return RepoWorkspace(ws_dir, pythonpath=task.get("pythonpath")), task


def _react_workspace(tmp_path: Path) -> tuple[RepoWorkspace, dict]:
    """Workspace at the fork boundary: mutation + the baseline's wrong edit."""
    ws, task = _seeded_workspace(tmp_path)
    assert ws.edit_file(task["target_file"], *task["wrong_fix"]) is True
    return ws, task


def test_every_task_has_consistent_anchors():
    for name in oss_tasks.task_names():
        assert oss_tasks.anchors_are_consistent(oss_tasks.get_task(name))


def test_unknown_task_raises():
    with pytest.raises(KeyError):
        oss_tasks.get_task("does-not-exist")


def test_mutation_applies_the_bug():
    task = oss_tasks.get_task(oss_tasks.DEFAULT_TASK)
    assert task["mutation"][1] == "return _int_to_bytes(num)"
    assert task["correct_line"].endswith('lstrip(b"\\x00")')


def test_chain_mutation_wrong_then_correct(tmp_path):
    ws, task = _seeded_workspace(tmp_path)
    assert task["mutation"][1] in ws.read_file(task["target_file"])
    assert ws.edit_file(task["target_file"], *task["wrong_fix"]) is True
    assert task["wrong_fix"][1] in ws.read_file(task["target_file"])
    assert ws.edit_file(task["target_file"], *task["correct_fix"]) is True
    assert task["correct_line"] in ws.read_file(task["target_file"])


def test_prepare_workspace_isolates_from_cache(tmp_path):
    cache = _fake_cache(tmp_path)
    ws_dir = tmp_path / "ws"
    prepare_workspace(cache, ws_dir)
    ws = RepoWorkspace(ws_dir)
    ws.edit_file("src/itsdangerous/encoding.py", "return 1", "return 2")
    # The cache is untouched.
    assert "return 2" not in (cache / "src" / "itsdangerous" / "encoding.py").read_text()


def test_workspace_search_hash_and_diff(tmp_path):
    ws, task = _seeded_workspace(tmp_path)
    hits = ws.search("def int_to_bytes")
    assert hits and hits[0]["file"] == task["target_file"]
    before = ws.file_hash(task["target_file"])
    ws.edit_file(task["target_file"], *task["wrong_fix"])
    assert ws.file_hash(task["target_file"]) != before
    assert "---" in ws.diff(task["target_file"])


def test_deterministic_policy_correct(tmp_path):
    ws, task = _react_workspace(tmp_path)
    policy = oss_agent.DeterministicPolicy(correct=True)
    note, changed = policy.react_edit(ws, task, "1 failed")
    assert changed is True
    assert task["correct_line"] in ws.read_file(task["target_file"])
    assert "correct" in note


def test_deterministic_policy_gives_up(tmp_path):
    ws, task = _react_workspace(tmp_path)
    before = ws.file_hash(task["target_file"])
    policy = oss_agent.DeterministicPolicy(correct=False)
    note, changed = policy.react_edit(ws, task, "1 failed")
    assert changed is False
    assert ws.file_hash(task["target_file"]) == before
    assert "gives up" in note


def test_llm_policy_chooses_lstrip(tmp_path):
    ws, task = _react_workspace(tmp_path)
    policy = oss_agent.LLMPolicy(chat_fn=lambda system, user: '{"fix": "lstrip", "reason": "strips leading"}')
    note, changed = policy.react_edit(ws, task, "1 failed")
    assert changed is True
    assert task["correct_line"] in ws.read_file(task["target_file"])


def test_llm_policy_chooses_rstrip_stays_wrong(tmp_path):
    ws, task = _react_workspace(tmp_path)
    policy = oss_agent.LLMPolicy(chat_fn=lambda system, user: '```json\n{"fix": "rstrip"}\n```')
    note, changed = policy.react_edit(ws, task, "1 failed")
    assert changed is False
    assert task["wrong_fix"][1] in ws.read_file(task["target_file"])
    assert task["correct_line"] not in ws.read_file(task["target_file"])


def test_llm_policy_defaults_to_correct_on_bad_output(tmp_path):
    ws, task = _react_workspace(tmp_path)
    policy = oss_agent.LLMPolicy(chat_fn=lambda system, user: "not json at all")
    note, changed = policy.react_edit(ws, task, "1 failed")
    assert changed is True
    assert task["correct_line"] in ws.read_file(task["target_file"])
