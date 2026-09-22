from __future__ import annotations

from polymaker.config import Config


def test_live_config_fits_current_reward_floor_without_changing_total_cap() -> None:
    cfg = Config.load("livecfg", load_env=False)

    assert cfg.risk.max_total_exposure_usdc == 60.0
    assert cfg.risk.max_event_group_loss_usdc == 55.0
    assert cfg.risk.max_market_notional_usdc == 55.0
    assert cfg.risk.daily_loss_kill_usdc == 12.0
    assert cfg.execution.post_only is True
    assert cfg.merge.enabled is False
    assert cfg.engine.reconcile_interval_s == 60.0

    markets = cfg.enabled_markets
    assert len(markets) == 6
    assert len({market.ref for market in markets}) == len(markets)
    assert {market.profile for market in markets} == {"live-tiny"}
    assert {market.ref for market in markets} == {
        "will-gavin-newsom-win-the-2028-democratic-presidential-nomination-568",
        "will-jd-vance-win-the-2028-republican-presidential-nomination",
        "will-russia-invade-another-country-in-2026",
        "china-x-taiwan-military-clash-before-2027",
        "iran-nuke-before-2027",
        "will-the-us-acquire-any-part-of-greenland-in-2026",
    }

    profile = cfg.profiles["live-tiny"]
    assert profile.base_size_usdc == 5.0
    assert profile.q_max_usdc == 12.0
    assert profile.layers == 1
    assert profile.reward_only_entries is True
