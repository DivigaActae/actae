# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Live dry-run test for the fork-resume token-savings report (opt-in).

Runs the actual `examples/fork_resume_demo.py` pipeline (baseline + fork)
against a real Actae server in `--dry-run` mode — deterministic fake token
usage, no API key — and asserts the report's headline figures and the
measured registries.

Dry-run tokens are fixed per step (`DRY_RUN_USAGE`), so the numbers are exact
and reproducible:

    baseline = sum of all 10 steps  = 17,660 tokens
    fork     = sum of steps 6-10    =  9,400 tokens
    saved    = 8,260 tokens (46.8%)  — the steps 1-5 work that never re-runs

The demo's own proofs (steps 1-5 not re-invoked, inherited state, fork event
log) are covered in depth by `test_fork_resume_live.py`; this test pins the
token dimension end-to-end. The pure report arithmetic is unit-tested in
`test_fork_token_savings.py` (no server needed).

    ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
        python3 -m pytest sdks/example-tests/python/test_fork_resume_tokens_live.py -v

Needs the demo's imports available (openai + actae_client installed).
"""

import asyncio
import os
import sys
import uuid
from pathlib import Path

import pytest

_EXAMPLES = Path(__file__).resolve().parents[3] / "examples"
sys.path.insert(0, str(_EXAMPLES))

from actae_client import ActaeClient  # noqa: E402
fork_resume_demo = pytest.importorskip(
    "fork_resume_demo", reason="openai (demo dependency) not installed"
)
DRY_RUN = fork_resume_demo.DRY_RUN
DRY_RUN_USAGE = fork_resume_demo.DRY_RUN_USAGE
PIPELINE = fork_resume_demo.PIPELINE
run_demo = fork_resume_demo.run_demo

_ENDPOINT = os.environ.get("ACTAE_URL") or os.environ.get("ACTAE_ENDPOINT")
_API_KEY = os.environ.get("ACTAE_API_KEY")

pytestmark = pytest.mark.skipif(
    not (_ENDPOINT and _API_KEY), reason="ACTAE_URL/ACTAE_API_KEY not set"
)


def _expected_totals():
    """Plain sums of the dry-run table — independent of the report library."""
    total = 0
    for spec in PIPELINE:
        prompt, completion = DRY_RUN_USAGE[spec["name"]]
        total += prompt + completion
    prefix = sum(
        DRY_RUN_USAGE[spec["name"]][0] + DRY_RUN_USAGE[spec["name"]][1]
        for spec in PIPELINE[:5]
    )
    return total, total - prefix  # (baseline_total, fork_total)


def test_dry_run_demo_reports_exact_token_savings():
    BASE_TOTAL, FORK_TOTAL = _expected_totals()
    SAVED = BASE_TOTAL - FORK_TOTAL
    PCT = SAVED / BASE_TOTAL * 100.0

    async def _main():
        actae = ActaeClient(api_key=_API_KEY, endpoint=_ENDPOINT)
        await actae.connect()
        try:
            suffix = uuid.uuid4().hex[:8]
            return await run_demo(
                actae,
                dry_run=True,
                base_channel=f"fork-tokens-{suffix}",
                fork_channel=f"fork-tokens-{suffix}-fork",
            )
        finally:
            await actae.disconnect()

    try:
        result = asyncio.run(_main())
    finally:
        import fork_resume_demo as _demo
        _demo.DRY_RUN = DRY_RUN  # restore whatever the import default was

    base, fork = result["baseline"], result["fork"]
    report = result["report"]

    # (a) Call counts: baseline ran all 10 steps, the fork only 6-10.
    assert base.total_calls() == len(PIPELINE) == 10
    assert fork.total_calls() == 5
    for step in [spec["name"] for spec in PIPELINE[:5]]:
        assert fork.count(step) == 0, f"fork must not invoke {step}"

    # (b) Measured totals match the plain sums of the deterministic table.
    assert base.total_tokens() == BASE_TOTAL
    assert fork.total_tokens() == FORK_TOTAL

    # (c) The headline figures the demo prints for customers.
    assert f"tokens saved on the fork run: {SAVED:,} ({PCT:.1f}% of the baseline)" in report

    # (d) The fork's own steps recorded usage; inherited steps are "—" and free.
    assert "DRY-RUN" in report
    assert "source: DRY-RUN" in report
    for name in [spec["name"] for spec in PIPELINE[:5]]:
        row = next(l for l in report.splitlines() if l.strip().startswith(name))
        assert "—" in row, f"inherited step {name} should show a dash"

    # (e) Fidelity: a same-prompt fork must reproduce the baseline exactly in
    # dry-run (deterministic content → prefix byte-identical, both divergences 0).
    assert result["fidelity"] is not None
    fidelity = result["fidelity"]
    assert fidelity["prefix_identical"], f"inherited prefix mismatch: {fidelity['prefix_mismatches']}"
    assert "byte-identical" in fidelity["section"]
    assert fidelity["tail_similarity_fork"], "no tail steps compared for the fork"
    assert fidelity["tail_similarity_rerun"], "no tail steps compared for the no-fork control"
    for name, ratio in fidelity["tail_similarity_fork"].items():
        assert ratio == 1.0, f"same-prompt fork tail step {name} similarity {ratio:.3f}, want 1.0 in dry-run"
    for name, ratio in fidelity["tail_similarity_rerun"].items():
        assert ratio == 1.0, f"no-fork re-run tail step {name} similarity {ratio:.3f}, want 1.0 in dry-run"
    assert fidelity["fork_divergence"] == 0.0
    assert fidelity["rerun_divergence"] == 0.0
    assert "forking does not degrade output fidelity" in fidelity["section"]
