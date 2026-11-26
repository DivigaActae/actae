# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Pure report model for the Actae reliability evidence suite.

No server and no I/O here on purpose: the runner and the unit tests both import
this module, so the aggregation, verdict and exit-code logic is testable in
isolation.

Schema (``actae.reliability.v1``)::

    {
      "schema": "actae.reliability.v1",
      "run_id": "...",
      "generated_at": "2026-09-21T08:00:00+00:00",
      "actae_url": "http://localhost:8002",
      "server_version": "0.1.0",
      "suites": [
        {"name": "fork", "checks": [{"name": "...", "ok": true, "detail": ""}],
         "passed": 4, "total": 4, "ok": true, "error": null}
      ],
      "passed": 12, "total": 12, "verdict": "PASS"
    }
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

SCHEMA = "actae.reliability.v1"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Check:
    """One asserted reliability property."""

    __slots__ = ("name", "ok", "detail")

    def __init__(self, name: str, ok: bool, detail: str = "") -> None:
        self.name = name
        self.ok = bool(ok)
        self.detail = detail

    def to_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "ok": self.ok, "detail": self.detail}


class Suite:
    """A named group of checks (e.g. "crash", "fork", "coordination").

    A suite that raised before/while running records ``error`` and is
    reported as failed rather than silently omitted.
    """

    __slots__ = ("name", "checks", "error")

    def __init__(self, name: str, checks: Optional[List[Check]] = None, error: Optional[str] = None) -> None:
        self.name = name
        self.checks: List[Check] = list(checks or [])
        self.error = error

    def add(self, name: str, ok: bool, detail: str = "") -> None:
        self.checks.append(Check(name, ok, detail))

    @property
    def passed(self) -> int:
        return sum(1 for c in self.checks if c.ok)

    @property
    def total(self) -> int:
        return len(self.checks)

    @property
    def ok(self) -> bool:
        # A suite with no checks is not evidence of anything: require at least
        # one check and no suite-level error.
        return self.error is None and self.total > 0 and self.passed == self.total

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "checks": [c.to_dict() for c in self.checks],
            "passed": self.passed,
            "total": self.total,
            "ok": self.ok,
            "error": self.error,
        }


class Report:
    """The full evidence run."""

    def __init__(
        self,
        run_id: str,
        actae_url: str,
        suites: Optional[List[Suite]] = None,
        server_version: Optional[str] = None,
        generated_at: Optional[str] = None,
    ) -> None:
        self.schema = SCHEMA
        self.run_id = run_id
        self.generated_at = generated_at or _now_iso()
        self.actae_url = actae_url
        self.server_version = server_version
        self.suites: List[Suite] = list(suites or [])

    @property
    def passed(self) -> int:
        return sum(s.passed for s in self.suites)

    @property
    def total(self) -> int:
        return sum(s.total for s in self.suites)

    @property
    def verdict(self) -> str:
        return "PASS" if (self.suites and all(s.ok for s in self.suites)) else "FAIL"

    def exit_code(self) -> int:
        return 0 if self.verdict == "PASS" else 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema": self.schema,
            "run_id": self.run_id,
            "generated_at": self.generated_at,
            "actae_url": self.actae_url,
            "server_version": self.server_version,
            "suites": [s.to_dict() for s in self.suites],
            "passed": self.passed,
            "total": self.total,
            "verdict": self.verdict,
        }


def render_markdown(report: Report) -> str:
    """Render the report as a human-readable Markdown summary."""
    lines: List[str] = []
    lines.append(f"# Actae reliability evidence — {report.run_id}")
    lines.append("")
    lines.append(f"- **Schema:** `{report.schema}`")
    lines.append(f"- **Generated:** {report.generated_at}")
    lines.append(f"- **Endpoint:** {report.actae_url}")
    if report.server_version:
        lines.append(f"- **Server version:** {report.server_version}")
    lines.append(f"- **Verdict:** **{report.verdict}** ({report.passed}/{report.total} checks)")
    lines.append("")
    for suite in report.suites:
        status = "PASS" if suite.ok else "FAIL"
        lines.append(f"## {suite.name} — {status} ({suite.passed}/{suite.total})")
        if suite.error:
            lines.append(f"> error: {suite.error}")
        for check in suite.checks:
            mark = "x" if check.ok else " "
            suffix = f" — {check.detail}" if check.detail else ""
            lines.append(f"- [{mark}] {check.name}{suffix}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
