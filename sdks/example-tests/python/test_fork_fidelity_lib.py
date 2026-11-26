# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the LLM-judged fidelity infrastructure (no server, no LLM).

Covers the pure logic in `examples/fork_fidelity_lib.py` — judge JSON
parsing, deterministic judge, tail/prefix extraction, pass/fail thresholds,
aggregation, report serialization — and validates the five scenarios in
`examples/fork_fidelity_scenarios.py` are well-formed and genuinely
unrelated. The end-to-end runs (real Actae + real DeepSeek judge) live in the
opt-in live suite `test_fork_fidelity_suite_live.py`.

    python3 -m pytest sdks/example-tests/python/test_fork_fidelity_lib.py -v
"""

import asyncio
import sys
from pathlib import Path

import pytest

_EXAMPLES = Path(__file__).resolve().parents[3] / "examples"
sys.path.insert(0, str(_EXAMPLES))

from fork_fidelity_lib import (  # noqa: E402
    DeterministicJudge,
    FidelityComparison,
    FidelityError,
    LLMJudge,
    Scenario,
    Step,
    aggregate,
    inherited_keys,
    parse_judge_json,
    prefix_identical,
    scenario_passed,
    tail_keys,
    tail_text,
    validate_scenario,
)
from fork_fidelity_scenarios import ALL_SCENARIOS, scenarios_by_name  # noqa: E402


def _scenario(**kw):
    base = dict(
        name="unit-test",
        description="test scenario",
        topic="A test topic",
        steps=[
            Step(key="a", system="sys a", user=lambda s: "u a"),
            Step(key="b", system="sys b", user=lambda s: "u b"),
            Step(key="c", system="sys c", user=lambda s: "u c"),
        ],
        fork_at_step=1,
    )
    base.update(kw)
    return Scenario(**base)


# ---------------------------------------------------------------------------
# Scenario validation
# ---------------------------------------------------------------------------

def test_validate_scenario_ok():
    validate_scenario(_scenario())


def test_validate_scenario_rejects_bad_config():
    with pytest.raises(FidelityError, match="slug"):
        validate_scenario(_scenario(name="Bad Name!"))
    with pytest.raises(FidelityError, match="no steps"):
        validate_scenario(_scenario(steps=[]))
    with pytest.raises(FidelityError, match="duplicate"):
        validate_scenario(_scenario(steps=[Step("a", "s", lambda s: "u"), Step("a", "s", lambda s: "u")]))
    with pytest.raises(FidelityError, match="fork_at_step"):
        validate_scenario(_scenario(fork_at_step=0))
    with pytest.raises(FidelityError, match="fork_at_step"):
        validate_scenario(_scenario(fork_at_step=3))  # == len(steps) → no tail


def test_inherited_and_tail_keys():
    sc = _scenario(fork_at_step=1)
    assert inherited_keys(sc) == ["a"]
    assert tail_keys(sc) == ["b", "c"]
    sc2 = _scenario(fork_at_step=2)
    assert inherited_keys(sc2) == ["a", "b"]
    assert tail_keys(sc2) == ["c"]


# ---------------------------------------------------------------------------
# Judge JSON parsing
# ---------------------------------------------------------------------------

def test_parse_judge_json_plain():
    out = parse_judge_json('{"score": 9, "verdict": "equivalent", "rationale": "same facts"}')
    assert out["score"] == 9
    assert out["verdict"] == "equivalent"


def test_parse_judge_json_fence():
    out = parse_judge_json('```json\n{"score": 7, "verdict": "minor_differences", "rationale": "ok"}\n```')
    assert out["score"] == 7


def test_parse_judge_json_loose_fallback():
    out = parse_judge_json('Here is the result: {"score": 8, "verdict": "equivalent", "rationale": "fine"}')
    assert out["score"] == 8


def test_parse_judge_json_unparseable_raises():
    with pytest.raises(FidelityError, match="unparseable"):
        parse_judge_json("the outputs are clearly different")
    with pytest.raises(FidelityError, match="out of range"):
        parse_judge_json('{"score": 11, "verdict": "equivalent"}')


# ---------------------------------------------------------------------------
# Judges
# ---------------------------------------------------------------------------

def test_deterministic_judge_plumbing():
    judge = DeterministicJudge()
    identical = asyncio.run(judge.score("same text", "same text"))
    assert identical.score == 10.0
    assert identical.verdict == "equivalent"
    different = asyncio.run(judge.score("completely different content here", "nothing in common at all"))
    assert different.score < 5.0


class _FakeChat:
    """Simulates an OpenAI chat client: `chat.completions.create(...)`.

    `contents` is a FIFO of raw assistant replies; each `create` consumes one."""

    def __init__(self, contents):
        self._contents = list(contents)
        self.calls = []
        self.chat = type(
            "Chat",
            (),
            {"completions": type("Completions", (), {"create": self._create})},
        )()

    async def _create(self, **kwargs):
        self.calls.append(kwargs)
        if not self._contents:
            raise AssertionError("no fake reply left")
        content = self._contents.pop(0)

        class _Msg:
            pass

        _Msg.content = content

        class _Choice:
            message = _Msg()

        class _Resp:
            choices = [_Choice()]
            usage = None

        return _Resp()


def test_llm_judge_averages_reps_and_parses():
    chat = _FakeChat([
        '{"score": 8, "verdict": "minor_differences", "rationale": "r1"}',
        '{"score": 9, "verdict": "equivalent", "rationale": "r2"}',
    ])
    judge = LLMJudge(chat, model="fake-model", reps=2)
    result = asyncio.run(judge.score("reference", "candidate"))
    assert result.score == 8.5
    assert len(result.samples) == 2
    assert result.rationale == "r1 | r2"
    assert chat.calls[0]["temperature"] == 0.0  # judge is deterministic-config


def test_llm_judge_retries_on_unparseable_then_succeeds():
    chat = _FakeChat([
        "not json at all",
        '{"score": 6, "verdict": "some_substantive_differences", "rationale": "recovered"}',
    ])
    judge = LLMJudge(chat, model="fake-model", reps=1)
    result = asyncio.run(judge.score("ref", "cand"))
    assert result.score == 6
    assert chat.calls[0] == chat.calls[1]  # same prompt retried


def test_llm_judge_gives_up_after_retries():
    chat = _FakeChat(["garbage", "garbage", "garbage"])
    judge = LLMJudge(chat, model="fake-model", reps=1, retries=3)
    with pytest.raises(FidelityError, match="failed after 3"):
        asyncio.run(judge.score("ref", "cand"))


# ---------------------------------------------------------------------------
# Extraction + pass/fail logic
# ---------------------------------------------------------------------------

def test_tail_text_joins_only_tail_steps():
    sc = _scenario(fork_at_step=1)
    state = {"topic": "T", "a": "AAA", "b": "BBB", "c": "CCC"}
    text = tail_text(state, sc)
    assert "AAA" not in text  # inherited step excluded
    assert "[b]" in text and "BBB" in text
    assert "[c]" in text and "CCC" in text


def test_prefix_identical_compares_inherited_keys():
    sc = _scenario(fork_at_step=1)
    base = {"a": "same", "b": "x", "c": "y"}
    fork = {"a": "same", "b": "x", "c": "y"}
    ok, mism = prefix_identical(base, fork, sc)
    assert ok and mism == []
    fork_bad = dict(fork, a="DIFFERENT")
    ok2, mism2 = prefix_identical(base, fork_bad, sc)
    assert not ok2 and mism2 == ["a"]


def test_scenario_passed_logic():
    def cmp(score):
        return FidelityComparison(score=score, verdict="x", rationale="", samples=[])

    ok, reason = scenario_passed(True, [], cmp(8.0), cmp(8.5), threshold=7.0, tolerance=1.5)
    assert ok and reason is None
    ok2, r2 = scenario_passed(False, ["a"], cmp(9.0), cmp(9.0), threshold=7.0, tolerance=1.5)
    assert not ok2 and "prefix mismatch" in r2
    ok3, r3 = scenario_passed(True, [], cmp(6.0), cmp(9.0), threshold=7.0, tolerance=1.5)
    assert not ok3 and "threshold" in r3
    ok4, r4 = scenario_passed(True, [], cmp(7.5), cmp(9.5), threshold=7.0, tolerance=1.5)
    assert not ok4 and "below the no-fork re-run" in r4
    # equal or within tolerance passes even if rerun higher
    ok5, _ = scenario_passed(True, [], cmp(8.0), cmp(9.4), threshold=7.0, tolerance=1.5)
    assert ok5


def test_aggregate_report():
    def cmp(score):
        return FidelityComparison(score=score, verdict="x", rationale="", samples=[])

    from fork_fidelity_lib import ScenarioResult

    def result(name, fork_score, rerun_score, passed):
        return ScenarioResult(
            name=name, description="", topic="", step_count=3, fork_at_step=1,
            inherited_steps=["a"], tail_steps=["b", "c"],
            prefix_identical=True, prefix_mismatches=[],
            fork=cmp(fork_score), rerun=cmp(rerun_score), repeats=[],
            passed=passed, fail_reason=None, calls=10, tokens=500, channels=[],
        )

    rep = aggregate(
        [result("x", 8.0, 7.5, True), result("y", 6.0, 9.0, False)],
        threshold=7.0, tolerance=1.5, dry_run=False,
        pipeline_model="m", judge_model="j", judge_reps=2,
    )
    assert rep.passed is False
    assert rep.passed_count == 1
    assert rep.scenario_count == 2
    assert rep.mean_fork_fidelity == 7.0
    assert rep.mean_rerun_fidelity == 8.25
    assert rep.worst_fork_fidelity == 6.0
    assert rep.total_llm_calls == 20
    assert rep.total_tokens == 1000
    d = rep.to_dict()
    assert d["overall"]["pass"] is False
    assert set(d["overall"]) >= {"mean_fork_fidelity", "worst_fork_fidelity", "total_tokens"}
    assert d["scenarios"][0]["fork_fidelity"]["score"] == 8.0


def test_aggregate_comparison_means_repeats():
    from fork_fidelity_lib import aggregate_comparison

    c1 = FidelityComparison(score=6.0, verdict="x", rationale="r1", samples=[])
    c2 = FidelityComparison(score=9.0, verdict="x", rationale="r2", samples=[])
    agg = aggregate_comparison([c1, c2])
    assert agg.score == 7.5
    assert agg.rationale == "r1 | r2"


def test_is_transient_classification():
    from actae_client.errors import ActaeConnectionError, APIError
    from actae_client.session import SessionError
    from fork_fidelity_lib import _is_transient

    assert _is_transient(ActaeConnectionError("drop")) is True
    assert _is_transient(APIError(500)) is True
    assert _is_transient(APIError(400)) is False
    assert _is_transient(SessionError("Channel 'x' not found")) is True
    assert _is_transient(ValueError("config bug")) is False


# ---------------------------------------------------------------------------
# Scenario catalogue
# ---------------------------------------------------------------------------

def test_all_scenarios_valid_and_unrelated():
    assert len(ALL_SCENARIOS) >= 5, "need multiple independent scenarios"
    names = [sc.name for sc in ALL_SCENARIOS]
    assert len(set(names)) == len(names), "scenario names must be unique"
    domains = [sc.params.get("domain") for sc in ALL_SCENARIOS]
    assert len(set(domains)) == len(domains), "scenarios must be from unrelated domains"
    fork_points = {sc.fork_at_step for sc in ALL_SCENARIOS}
    assert len(fork_points) >= 2, "fork points should vary across scenarios"
    for sc in ALL_SCENARIOS:
        validate_scenario(sc)
        assert len(sc.steps) >= 3
        assert sc.description


def test_scenarios_by_name_filter():
    sel = scenarios_by_name(["s3-localization", "s5-product-brief"])
    assert [s.name for s in sel] == ["s3-localization", "s5-product-brief"]
    with pytest.raises(ValueError, match="unknown"):
        scenarios_by_name(["does-not-exist"])
