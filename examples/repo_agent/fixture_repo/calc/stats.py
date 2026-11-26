# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

"""Small statistics helpers — the fixture package for the repo-agent demo.

Deliberately contains a subtle bug in ``median`` so the agent has a real,
testable task.
"""


def average(values):
    """Arithmetic mean of ``values`` (0.0 for an empty sequence)."""
    if not values:
        return 0.0
    return sum(values) / len(values)


def median(values):
    """Median of ``values`` (0.0 for an empty sequence)."""
    if not values:
        return 0.0
    ordered = sorted(values)
    # BUG: for an even-length sequence the median is the mean of the two
    # middle values, not the upper-middle element.
    return ordered[len(ordered) // 2]
