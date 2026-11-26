# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the fork-showcase helpers (no server).

Pins the deterministic token-savings arithmetic and the small pure helpers so
the showcase's reported numbers cannot silently drift.

    python3 -m pytest sdks/example-tests/python/test_fork_showcase.py -v
"""

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO / "examples"))

from fork_showcase import deterministic_token_registries  # noqa: E402


def test_fork_registry_only_covers_the_tail():
    baseline, fork = deterministic_token_registries(["a", "b", "c", "d", "e"], 3)
    assert baseline.total_calls() == 5
    assert fork.total_calls() == 2
    assert fork.steps() == ("d", "e")


def test_savings_is_exact_difference():
    baseline, fork = deterministic_token_registries(["a", "b", "c"], 2, prompt_tokens=100, completion_tokens=50)
    assert baseline.total_tokens() == 3 * 150
    assert fork.total_tokens() == 1 * 150
    assert baseline.total_tokens() - fork.total_tokens() == 300


def test_fork_at_zero_reruns_everything():
    baseline, fork = deterministic_token_registries(["a", "b"], 0)
    assert fork.total_tokens() == baseline.total_tokens()
    assert baseline.total_tokens() - fork.total_tokens() == 0
