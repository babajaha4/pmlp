"""Pure reward-capacity allocation helpers.

The live engine must not spend scarce reservation headroom on markets that
cannot produce a reward-eligible two-sided quote. This module keeps that
choice deterministic and independent of network or engine state.
"""

from __future__ import annotations

from collections.abc import Iterable

from polymaker.config import StrategyProfile
from polymaker.domain import MarketMeta


def reward_entry_markets(
    markets: Iterable[tuple[MarketMeta, StrategyProfile]],
    *,
    max_total_notional: float,
    max_market_notional: float,
    max_event_notional: float,
) -> frozenset[str]:
    """Select reward-only markets that fit worst-case reservation caps.

    A binary market needs one minimum-size order on each outcome. Since the
    two token prices sum to approximately one, ``rewards_min_size`` shares on
    both sides reserve roughly that many pUSD. Candidates are ranked by reward
    dollars per reserved dollar and admitted greedily under global and event
    group caps.
    """
    candidates: list[tuple[float, float, str, str | None, float]] = []
    for meta, profile in markets:
        if not profile.reward_only_entries:
            continue
        if (
            meta.rewards_daily_rate <= 0
            or meta.rewards_max_spread <= 0
            or meta.rewards_min_size <= 0
        ):
            continue
        required = meta.rewards_min_size * max(profile.reward_size_mult, 0.0)
        if required <= 0 or required > max_market_notional:
            continue
        candidates.append((
            meta.rewards_daily_rate / required,
            meta.rewards_daily_rate,
            meta.condition_id,
            meta.event_id,
            required,
        ))

    selected: set[str] = set()
    used_total = 0.0
    used_events: dict[str, float] = {}
    for _, _, cid, event_id, required in sorted(
        candidates, key=lambda x: (-x[0], -x[1], x[2])
    ):
        if used_total + required > max_total_notional + 1e-9:
            continue
        event_used = used_events.get(event_id, 0.0) if event_id else 0.0
        if event_id and event_used + required > max_event_notional + 1e-9:
            continue
        selected.add(cid)
        used_total += required
        if event_id:
            used_events[event_id] = event_used + required
    return frozenset(selected)
