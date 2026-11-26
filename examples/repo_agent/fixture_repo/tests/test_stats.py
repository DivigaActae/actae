# Copyright 2026 Diviga
# SPDX-License-Identifier: Apache-2.0

from calc.stats import average, median


def test_average_simple():
    assert average([1, 2, 3, 4]) == 2.5


def test_average_empty():
    assert average([]) == 0.0


def test_median_odd():
    assert median([1, 2, 3]) == 2


def test_median_even():
    # The failing test: an even-length median is the mean of the two middles.
    assert median([1, 2, 3, 4]) == 2.5


def test_median_empty():
    assert median([]) == 0.0
