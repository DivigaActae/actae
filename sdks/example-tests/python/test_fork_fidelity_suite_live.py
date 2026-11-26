# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Opt-in live tests for the LLM-judged fidelity suite.

Two tests against a real Actae server:

  1. `test_fidelity_suite_dry_run_plumbing` — needs only ACTAE_URL/ACTAE_API_KEY.
     Runs the full suite in dry-run (deterministic pipeline + deterministic
     judge, no DeepSeek key) and asserts the plumbing is sound: every scenario
     passes, the inherited prefix is byte-identical, and the report structure
     is complete.

  2. `test_fidelity_suite_real_judge` — additionally needs DEEPSEEK_API_KEY.
     Runs the REAL suite: real pipeline LLM calls + a real DeepSeek judge
     (temperature 0). This is the marketing-evidence run. It asserts the
     claim under test — for every unrelated scenario, fork fidelity is above
     the threshold and not worse than the no-fork re-run control — and writes
     the JSON evidence report to `examples/fidelity-reports/`.

The real test is deliberately slow (dozens of real LLM calls); it is opt-in.

    ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
        python3 -m pytest sdks/example-tests/python/test_fork_fidelity_suite_live.py -v

    # + DEEPSEEK_API_KEY (env or actae/.env) for the real-judge test.
"""

import asyncio
import os
import sys
from pathlib import Path

import pytest

_EXAMPLES = Path(__file__).resolve().parents[3] / "examples"
sys.path.insert(0, str(_EXAMPLES))

from actae_client import ActaeClient  # noqa: E402
from fork_fidelity_lib import evaluate_suite  # noqa: E402
from fork_fidelity_scenarios import ALL_SCENARIOS  # noqa: E402

_ENDPOINT = os.environ.get("ACTAE_URL") or os.environ.get("ACTAE_ENDPOINT")
_API_KEY = os.environ.get("ACTAE_API_KEY")

pytestmark = pytest.mark.skipif(
    not (_ENDPOINT and _API_KEY), reason="ACTAE_URL/ACTAE_API_KEY not set"
)


def _deepseek_key() -> str:
    key = os.environ.get("DEEPSEEK_API_KEY", "")
    if key:
        return key
    env_path = Path(__file__).resolve().parents[3] / "actae" / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("DEEPSEEK_API_KEY="):
                return line.split("=", 1)[1].strip().strip("\"'")
    return ""


def _report_dir() -> Path:
    return _EXAMPLES / "fidelity-reports"


def _assert_report_structure(report) -> None:
    d = report.to_dict()
    assert "generated_at" in d
    assert "scenarios" in d and len(d["scenarios"]) == len(ALL_SCENARIOS)
    overall = d["overall"]
    for k in ("pass", "passed", "scenarios", "mean_fork_fidelity",
              "mean_rerun_fidelity", "worst_fork_fidelity", "total_llm_calls", "total_tokens"):
        assert k in overall
    for sc in d["scenarios"]:
        for k in ("name", "fork_fidelity", "rerun_fidelity", "prefix_identical", "pass", "fail_reason"):
            assert k in sc
        assert "score" in sc["fork_fidelity"]
        assert "rationale" in sc["fork_fidelity"]
        assert sc["repeats"], "raw repeat evidence must be preserved"
        for repeat in sc["repeats"]:
            assert set(repeat["observations"]) == {"baseline", "rerun", "fork"}
            for run in repeat["observations"].values():
                assert "state" in run and "trace" in run and "tail" in run
            provenance = repeat["fork_provenance"]
            assert provenance["boundary_restorable"] is True
            assert provenance["requested_boundary_cursor"] == provenance["resolved_boundary_cursor"]
            assert provenance["source_state_sha256"]


@pytest.mark.timeout(600)
def test_fidelity_suite_dry_run_plumbing():
    async def _main():
        actae = ActaeClient(api_key=_API_KEY, endpoint=_ENDPOINT)
        await actae.connect()
        try:
            return await evaluate_suite(
                actae,
                ALL_SCENARIOS,
                chat=None,
                pipeline_model="dry-run",
                judge_model="dry-run",
                dry_run=True,
                judge_reps=1,
            )
        finally:
            await actae.disconnect()

    report = asyncio.run(_main())

    # Every scenario must pass the plumbing (deterministic → exact).
    assert report.passed, [r.fail_reason for r in report.scenarios]
    assert report.passed_count == len(ALL_SCENARIOS)
    for r in report.scenarios:
        assert r.prefix_identical, f"{r.name}: inherited prefix mismatch {r.prefix_mismatches}"
        assert r.fork.score == 10.0, f"{r.name}: dry-run fork must be exactly equivalent"
        assert r.rerun.score == 10.0
    _assert_report_structure(report)


@pytest.mark.timeout(1500)
def test_fidelity_suite_real_judge():
    """The evidence run: real pipeline + real DeepSeek judge. Writes the JSON
    report to examples/fidelity-reports/."""
    pytest.importorskip("openai", reason="openai not installed (needed for the AsyncOpenAI judge client)")
    from openai import AsyncOpenAI

    key = _deepseek_key()
    if not key:
        pytest.skip("DEEPSEEK_API_KEY not set")
    chat = AsyncOpenAI(api_key=key, base_url="https://api.deepseek.com", timeout=180)

    async def _main():
        actae = ActaeClient(api_key=_API_KEY, endpoint=_ENDPOINT)
        await actae.connect()
        try:
            return await evaluate_suite(
                actae,
                ALL_SCENARIOS,
                chat=chat,
                pipeline_model=os.environ.get("DEEPSEEK_MODEL", "deepseek-chat"),
                judge_model=os.environ.get("DEEPSEEK_MODEL", "deepseek-chat"),
                dry_run=False,
                judge_reps=int(os.environ.get("FIDELITY_JUDGE_REPS", "1")),
                repeats=int(os.environ.get("FIDELITY_REPEATS", "2")),
                threshold=7.0,
                tolerance=1.5,
            )
        finally:
            await actae.disconnect()

    report = asyncio.run(_main())

    # The marketing claim: every unrelated scenario has mean fork fidelity >=
    # threshold AND fork is not worse than the no-fork re-run control. Single
    # LLM samples are noisy, so the default uses `repeats` independent runs
    # and judges the MEAN (FIDELITY_REPEATS overrides).
    failures = [r for r in report.scenarios if not r.passed]
    for r in report.scenarios:
        print(
            f"[{'PASS' if r.passed else 'FAIL'}] {r.name}: "
            f"fork={r.fork.score:.2f} rerun={r.rerun.score:.2f} "
            f"prefix_identical={r.prefix_identical} {r.fail_reason or ''}"
        )
        print(f"      fork judge: {r.fork.verdict} — {r.fork.rationale[:160]}")
    assert not failures, f"scenarios failed: {[(f.name, f.fail_reason) for f in failures]}"
    assert report.passed

    # Persist the evidence.
    _report_dir().mkdir(parents=True, exist_ok=True)
    import json
    import time as _time

    run_id = _time.strftime("live-%Y%m%d-%H%M%S")
    path = _report_dir() / f"fork-fidelity-{run_id}.json"
    path.write_text(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))
    print(f"\nevidence report: {path}")
    print(f"overall: {report.passed_count}/{report.scenario_count} passed, "
          f"mean fork {report.mean_fork_fidelity:.2f}, worst fork {report.worst_fork_fidelity:.2f}")
