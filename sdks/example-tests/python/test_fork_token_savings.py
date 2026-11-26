# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the fork-resume token-savings report (no server, no LLM).

These pin the *arithmetic* the demo prints to customers: every report number
is a plain sum of the usage a provider reported for real calls, "saved" is
exactly `baseline_total - fork_total`, percentages and wall-clock deltas are
correct, and the rendered text is stable. The library under test is
`examples/fork_token_report.py` — deliberately standalone (stdlib only) so
these tests run in any environment.

The demo itself (`examples/fork_resume_demo.py`) is exercised end-to-end in
dry-run mode by the opt-in live suite `test_fork_resume_tokens_live.py`.

    python3 -m pytest sdks/example-tests/python/test_fork_token_savings.py -v
"""

import sys
from pathlib import Path

import pytest

_EXAMPLES = Path(__file__).resolve().parents[3] / "examples"
sys.path.insert(0, str(_EXAMPLES))

from fork_token_report import StepRegistry, format_token_report  # noqa: E402


def _record(reg, step, prompt, completion, ms=100):
    reg.record(step, prompt, completion, ms)


# ---------------------------------------------------------------------------
# Registry: pure accumulation
# ---------------------------------------------------------------------------

def test_registry_sums_per_step_and_total():
    reg = StepRegistry()
    _record(reg, "research", 900, 320, ms=110)
    _record(reg, "research", 10, 20, ms=5)          # a second call to the same step
    _record(reg, "insights", 1100, 450, ms=200)

    assert reg.count("research") == 2
    assert reg.count("insights") == 1
    assert reg.total_calls() == 3
    assert reg.prompt_tokens("research") == 910
    assert reg.completion_tokens("research") == 340
    assert reg.step_tokens("research") == 1250
    assert reg.step_tokens("insights") == 1550
    assert reg.total_tokens() == 1250 + 1550
    assert reg.elapsed_ms("research") == 115
    assert reg.total_elapsed_ms() == 115 + 200
    assert reg.calls_by_step() == {"research": 2, "insights": 1}


def test_registry_step_order_is_first_recorded():
    reg = StepRegistry()
    _record(reg, "b", 1, 1)
    _record(reg, "a", 1, 1)
    assert reg.steps() == ("b", "a")


def test_registry_unknown_step_is_zero():
    reg = StepRegistry()
    assert reg.count("nope") == 0
    assert reg.step_tokens("nope") == 0
    assert reg.total_tokens() == 0
    assert reg.total_calls() == 0


def test_registry_rejects_negative_usage():
    reg = StepRegistry()
    for bad in ((-1, 1, 1), (1, -1, 1), (1, 1, -1)):
        with pytest.raises(ValueError, match="negative"):
            _record(reg, "x", *bad)
    assert reg.total_tokens() == 0, "rejected records must not be counted"


def test_mark_counts_an_attempt_without_usage():
    reg = StepRegistry()
    reg.mark("retry")
    reg.record("ok", 10, 5, 50)
    assert reg.count("retry") == 1
    assert reg.count("ok") == 1
    assert reg.total_calls() == 2
    assert reg.step_tokens("retry") == 0, "mark() adds no token usage"


# ---------------------------------------------------------------------------
# Savings arithmetic: baseline vs fork
# ---------------------------------------------------------------------------

def test_savings_math_is_exact_sum():
    """saved must equal the summed usage of the steps the fork never ran."""
    base = StepRegistry()
    fork = StepRegistry()
    for n, (p, c) in enumerate([(10, 5), (20, 10), (30, 15), (40, 20)], start=1):
        _record(base, f"s{n}", p, c)
    for n, (p, c) in [(3, (30, 15)), (4, (40, 20))]:
        _record(fork, f"s{n}", p, c)

    # baseline = s1..s4 = 15+30+45+60 = 150; fork = s3+s4 = 105; saved = 45.
    assert base.total_tokens() == 150
    assert fork.total_tokens() == 105
    assert base.total_tokens() - fork.total_tokens() == 45


def test_percentage_is_saved_over_baseline():
    base = StepRegistry()
    fork = StepRegistry()
    _record(base, "s1", 10, 5)
    _record(base, "s2", 20, 10)
    _record(fork, "s2", 20, 10)
    # saved 15 of 45 → 33.333%
    report = format_token_report(base, fork, ["s1", "s2"], mode="measured")
    assert "33.3%" in report


def test_empty_baseline_does_not_divide_by_zero():
    base = StepRegistry()
    fork = StepRegistry()
    _record(fork, "s1", 10, 5)
    report = format_token_report(base, fork, ["s1"], mode="measured")
    assert "tokens saved on the fork run: 0 (no baseline usage recorded)" in report


# ---------------------------------------------------------------------------
# Report rendering (stability — the demo prints exactly this text)
# ---------------------------------------------------------------------------

def test_skipped_steps_render_as_dash():
    base = StepRegistry()
    fork = StepRegistry()
    _record(base, "research", 900, 320)
    _record(base, "insights", 1100, 450)
    _record(fork, "insights", 1100, 450)

    report = format_token_report(base, fork, ["research", "insights"], mode="measured")
    # The skipped step's fork cell is a dash and its tokens are not counted.
    row = next(l for l in report.splitlines() if l.strip().startswith("research"))
    assert "—" in row
    assert "1,220" in row
    # Fork total is only the step it ran.
    assert "TOTAL" in report
    total_row = next(l for l in report.splitlines() if l.strip().startswith("TOTAL"))
    assert total_row.endswith("1,550")


def test_report_contains_measured_totals_and_savings():
    base = StepRegistry()
    fork = StepRegistry()
    _record(base, "s1", 100, 50)
    _record(base, "s2", 200, 100)
    _record(fork, "s2", 200, 100)

    report = format_token_report(base, fork, ["s1", "s2"], mode="measured")
    assert "source: measured" in report
    assert "TOTAL" in report
    assert "150" in report.split("tokens saved on the fork run: ")[1].split(" ")[0]
    assert "33.3%" in report
    assert "wall-clock" in report


def test_report_note_appears_only_when_fork_tail_is_heavier():
    base = StepRegistry()
    fork = StepRegistry()
    _record(base, "s1", 100, 50)
    _record(base, "s2", 100, 50)
    _record(fork, "s2", 300, 50)  # fork tail step costs MORE than baseline's

    report = format_token_report(base, fork, ["s1", "s2"], mode="measured")
    assert "ran with the inherited prefix" in report
    assert "200 tokens more" in report

    # When the tail is equal, the note is absent (nothing to disclose).
    base2 = StepRegistry()
    fork2 = StepRegistry()
    _record(base2, "s1", 100, 50)
    _record(base2, "s2", 100, 50)
    _record(fork2, "s2", 100, 50)
    report2 = format_token_report(base2, fork2, ["s1", "s2"], mode="measured")
    assert "ran with the inherited prefix" not in report2


def test_report_survives_negative_savings_honestly():
    """If the fork tail somehow cost more than the whole baseline, the report
    must show a negative saving rather than clamp to zero."""
    base = StepRegistry()
    fork = StepRegistry()
    _record(base, "s1", 10, 5)          # baseline total 15
    _record(fork, "s1", 10, 5)
    _record(fork, "s2", 1000, 500)      # fork total 1515
    report = format_token_report(base, fork, ["s1", "s2"], mode="measured")
    saved_line = next(l for l in report.splitlines() if "tokens saved on the fork run:" in l)
    assert "-1,500" in saved_line
    assert "-10000.0%" in saved_line  # honest: fork cost 100x the baseline
