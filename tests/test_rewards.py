"""Deterministic reward-capacity allocation tests."""

from __future__ import annotations

from dataclasses import replace

from polymaker.config import StrategyProfile
from polymaker.strategy.rewards import reward_entry_markets


def test_reward_markets_rank_by_reward_per_reserved_dollar(meta) -> None:
    first = replace(meta, condition_id="first", rewards_daily_rate=20, rewards_min_size=10)
    second = replace(meta, condition_id="second", rewards_daily_rate=30, rewards_min_size=30)
    selected = reward_entry_markets(
        [(first, StrategyProfile(reward_only_entries=True)),
         (second, StrategyProfile(reward_only_entries=True))],
        max_total_notional=30,
        max_market_notional=30,
        max_event_notional=30,
    )
    assert selected == frozenset({"first"})


def test_zero_reward_and_over_cap_markets_are_not_selected(meta) -> None:
    zero = replace(meta, condition_id="zero", rewards_daily_rate=0)
    too_large = replace(meta, condition_id="too-large", rewards_min_size=50)
    selected = reward_entry_markets(
        [(zero, StrategyProfile(reward_only_entries=True)),
         (too_large, StrategyProfile(reward_only_entries=True))],
        max_total_notional=100,
        max_market_notional=15,
        max_event_notional=100,
    )
    assert not selected


def test_total_and_event_caps_limit_selected_markets(meta) -> None:
    one = replace(meta, condition_id="one", event_id="event-a", rewards_daily_rate=20)
    two = replace(meta, condition_id="two", event_id="event-a", rewards_daily_rate=19)
    three = replace(meta, condition_id="three", event_id="event-b", rewards_daily_rate=18)
    profile = StrategyProfile(reward_only_entries=True)
    selected = reward_entry_markets(
        [(one, profile), (two, profile), (three, profile)],
        max_total_notional=20,
        max_market_notional=10,
        max_event_notional=10,
    )
    assert selected == frozenset({"one", "three"})


def test_live_cap_cannot_fit_fifty_share_reward_floor(meta) -> None:
    live = replace(meta, rewards_min_size=50)
    selected = reward_entry_markets(
        [(live, StrategyProfile(reward_only_entries=True))],
        max_total_notional=60,
        max_market_notional=15,
        max_event_notional=30,
    )
    assert not selected
