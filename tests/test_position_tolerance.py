"""Boundaries for comparing the durable fill ledger with REST positions."""

from __future__ import annotations

import pytest

from polymaker.position_tolerance import authoritative_position_matches


@pytest.mark.parametrize(
    ("ledger_size", "rest_size", "expected"),
    [
        (0.0, 0.0, True),
        (0.002919, 0.0, True),
        (0.009999, 0.0, True),
        (0.01, 0.0, False),
        (0.1, 0.0, False),
        (-0.000001, 0.0, False),
        (45.102919, 45.1029, True),
        (2.287254, 2.2872, True),
        (45.102999, 45.1029, True),
        (45.103, 45.1029, False),
        (45.102919, 45.1028, False),
    ],
)
def test_authoritative_position_match_boundaries(
    ledger_size: float, rest_size: float, expected: bool,
) -> None:
    assert authoritative_position_matches(ledger_size, rest_size) is expected


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_authoritative_position_match_rejects_nonfinite_values(value: float) -> None:
    assert not authoritative_position_matches(value, 0.0)
    assert not authoritative_position_matches(0.0, value)
