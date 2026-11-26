# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the reliability evidence runner's report model (no server).

The live harness (real process kills, forks, consumer groups) is run manually
and its committed report lives in ``examples/reliability-reports/``. These
tests pin the aggregation, verdict, exit-code and Markdown rendering so a
regression cannot silently turn a failing run into a green report.

    python3 -m pytest sdks/example-tests/python/test_reliability_harness.py -v
"""

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO / "examples"))
sys.path.insert(0, str(_REPO / "examples" / "reliability"))

from report import SCHEMA, Check, Report, Suite, render_markdown  # noqa: E402


def test_check_records_name_ok_detail():
    c = Check("worker dies", True, "returncode=137")
    assert c.to_dict() == {"name": "worker dies", "ok": True, "detail": "returncode=137"}


def test_suite_counts_and_ok():
    s = Suite("crash")
    s.add("a", True)
    s.add("b", False, "boom")
    assert s.passed == 1
    assert s.total == 2
    assert s.ok is False


def test_empty_suite_is_not_evidence():
    # A suite with no checks must not count as a pass.
    assert Suite("crash").ok is False


def test_suite_with_error_is_failed_even_with_passing_checks():
    s = Suite("fork")
    s.add("a", True)
    s.error = "ConnectionError: refused"
    assert s.ok is False
    assert s.to_dict()["error"] == "ConnectionError: refused"


def test_report_verdict_and_exit_code():
    good = Suite("crash")
    good.add("a", True)
    bad = Suite("fork")
    bad.add("b", False)

    passing = Report("run-1", "http://localhost:8002", [good, good])
    assert passing.verdict == "PASS"
    assert passing.exit_code() == 0
    assert (passing.passed, passing.total) == (2, 2)

    failing = Report("run-2", "http://localhost:8002", [good, bad])
    assert failing.verdict == "FAIL"
    assert failing.exit_code() == 1


def test_report_with_no_suites_fails():
    assert Report("run", "http://x").verdict == "FAIL"


def test_report_to_dict_shape():
    suite = Suite("coordination")
    suite.add("no double-claim", True)
    report = Report("run-3", "http://x", [suite], server_version="0.1.0")
    data = report.to_dict()
    assert data["schema"] == SCHEMA
    assert data["verdict"] == "PASS"
    assert data["server_version"] == "0.1.0"
    assert data["suites"][0]["name"] == "coordination"
    assert data["suites"][0]["checks"][0]["ok"] is True


def test_render_markdown_includes_verdict_and_failures():
    suite = Suite("fork")
    suite.add("auto replays", True)
    suite.add("block refuses", False, "not blocked")
    report = Report("run-4", "http://x", [suite])
    md = render_markdown(report)
    assert "**FAIL**" in md
    assert "- [x] auto replays" in md
    assert "- [ ] block refuses — not blocked" in md
