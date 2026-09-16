"""Shared rules for comparing the fill ledger with Data API positions."""

from __future__ import annotations

import math

REST_POSITION_ABS_TOL = 0.00005
STRICT_POSITION_ABS_TOL = 0.000001
MAX_OMITTED_DUST_SHARES = 0.01


def authoritative_position_matches(ledger_size: float, rest_size: float) -> bool:
    """Return whether a REST position can authoritatively explain ledger inventory.

    The Data API may omit positive positions below 0.01 shares and displays
    reported positions to four decimal places. The upper dust boundary is
    intentionally exclusive; negative or non-finite inventory never matches.
    """
    if not math.isfinite(ledger_size) or not math.isfinite(rest_size):
        return False
    if ledger_size < 0.0 or rest_size < 0.0:
        return False
    if rest_size == 0.0:
        return ledger_size < MAX_OMITTED_DUST_SHARES
    tolerance = (
        REST_POSITION_ABS_TOL
        if ledger_size > 0.0
        else STRICT_POSITION_ABS_TOL
    )
    return math.isclose(ledger_size, rest_size, rel_tol=0.0, abs_tol=tolerance)
