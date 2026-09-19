"""Shared rules for comparing the fill ledger with Data API positions."""

from __future__ import annotations

import math

# Data API position sizes are truncated to four decimal places.  Keep the
# boundary exclusive so a full 0.0001-share discrepancy still fails closed.
REST_POSITION_ABS_TOL = 0.0001
STRICT_POSITION_ABS_TOL = 0.000001
MAX_OMITTED_DUST_SHARES = 0.01


def authoritative_position_matches(ledger_size: float, rest_size: float) -> bool:
    """Return whether a REST position can authoritatively explain ledger inventory.

    The Data API may omit positive positions below 0.01 shares and truncates
    reported positions to four decimal places. The upper boundaries are
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
    return abs(ledger_size - rest_size) < tolerance
