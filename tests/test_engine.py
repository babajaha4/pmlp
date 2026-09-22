"""Integration test: one full engine recompute cycle in paper mode (no network)."""

from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from unittest.mock import AsyncMock

from polymaker.catalog.rewards import RewardMarketSnapshot, RewardMarketsReadError
from polymaker.config import Config, PathsConfig, StrategyProfile
from polymaker.domain import Side
from polymaker.engine import Engine
from polymaker.strategy.regime import RegimeMachine


def _engine_with_market(tmp_path, meta) -> Engine:
    cfg = Config(paths=PathsConfig(db=str(tmp_path / "state.db"),
                                   journal_dir=str(tmp_path / "j"),
                                   log_dir=str(tmp_path / "l")))
    cfg.engine.journal = False
    eng = Engine(cfg, paper=True)
    cid = meta.condition_id
    # inject one market directly, bypassing network resolution
    eng.metas[cid] = meta
    eng.profiles[cid] = StrategyProfile()
    eng.est[cid] = Engine._make_estimators(eng.profiles[cid])
    eng.regime_m[cid] = RegimeMachine()
    eng._dirty[cid] = asyncio.Event()
    eng._locks[cid] = asyncio.Lock()
    for tok in (meta.yes.token_id, meta.no.token_id):
        eng._token_cid[tok] = cid
    eng.md.set_markets([(cid, [meta.yes.token_id, meta.no.token_id])])
    eng._running = True
    return eng


def _feed_book(eng, meta):
    now = time.time()  # fresh ts so the ws_stale guard doesn't HALT the market
    yb = eng.md.book(meta.yes.token_id)
    yb.apply_snapshot(bids=[(0.48, 500), (0.49, 500)], asks=[(0.51, 500), (0.52, 500)], ts=now)
    nb = eng.md.book(meta.no.token_id)
    nb.apply_snapshot(bids=[(0.48, 500), (0.49, 500)], asks=[(0.51, 500), (0.52, 500)], ts=now)


async def test_recompute_places_two_sided_paper_quotes(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    _feed_book(eng, meta)
    await eng._recompute(meta.condition_id)

    yes_orders = eng.state.orders_for(meta.yes.token_id)
    no_orders = eng.state.orders_for(meta.no.token_id)
    assert yes_orders, "no YES quotes placed"
    assert no_orders, "no NO quotes placed"
    # entry quotes are BUYs on both tokens (the canonical two-sided quote)
    assert all(o.side is Side.BUY for o in yes_orders)
    assert all(o.side is Side.BUY for o in no_orders)
    eng.state.close()
    eng.catalog.close()


async def test_recompute_is_idempotent_within_tolerance(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    _feed_book(eng, meta)
    await eng._recompute(meta.condition_id)
    n_after_first = len(eng.state.orders)
    # same book -> reconcile should be a no-op, order count unchanged
    await eng._recompute(meta.condition_id)
    assert len(eng.state.orders) == n_after_first
    eng.state.close()
    eng.catalog.close()


async def test_risk_fitted_quotes_are_stable_across_recomputes(tmp_path, meta):
    """A quote shrunk to remaining headroom must not cancel/replace itself."""
    eng = _engine_with_market(tmp_path, meta)
    eng.cfg.risk.max_total_exposure_usdc = 10.0
    _feed_book(eng, meta)

    await eng._recompute(meta.condition_id)
    first_orders = {
        order.order_id: (order.token_id, order.side, order.price, order.size)
        for order in eng.state.orders.values()
    }
    assert first_orders
    assert sum(order.notional for order in eng.state.orders.values()) <= 10.0 + 1e-9

    await eng._recompute(meta.condition_id)

    second_orders = {
        order.order_id: (order.token_id, order.side, order.price, order.size)
        for order in eng.state.orders.values()
    }
    assert second_orders == first_orders
    eng.state.close()
    eng.catalog.close()


async def test_recompute_skips_when_book_empty(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    # no book fed
    await eng._recompute(meta.condition_id)
    assert len(eng.state.orders) == 0
    eng.state.close()
    eng.catalog.close()


async def test_shutdown_closes_streams_and_is_idempotent(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.md.close = AsyncMock()
    eng.user = AsyncMock()
    eng.gateway.cancel_asset = AsyncMock(return_value=True)

    await eng.shutdown()
    await eng.shutdown()

    eng.md.close.assert_awaited_once()
    eng.user.close.assert_awaited_once()
    assert eng.gateway.cancel_asset.await_count == 2


async def test_metadata_refresh_applies_official_reward_competition(
    tmp_path, meta, monkeypatch
):
    eng = _engine_with_market(tmp_path, replace(meta, reward_competitiveness=999.0))

    class FakeGamma:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return None

        async def markets_by_condition(self, _condition_ids):
            return {meta.condition_id: {"acceptingOrders": True, "closed": False}}

    monkeypatch.setattr("polymaker.engine.GammaClient", lambda *_args, **_kwargs: FakeGamma())
    monkeypatch.setattr(
        "polymaker.engine.fetch_reward_markets",
        AsyncMock(return_value={
            meta.condition_id: RewardMarketSnapshot(
                meta.condition_id, 80.0, 20.0, 4.0, 12.0
            )
        }),
    )

    await eng.refresh_market_metadata()

    refreshed = eng.metas[meta.condition_id]
    assert refreshed.rewards_daily_rate == 80.0
    assert refreshed.rewards_min_size == 20.0
    assert refreshed.rewards_max_spread == 4.0
    assert refreshed.reward_competitiveness == 12.0
    assert eng.catalog.get(meta.condition_id).reward_competitiveness == 12.0
    eng.state.close()
    eng.catalog.close()


async def test_reward_api_failure_preserves_last_valid_competition(
    tmp_path, meta, monkeypatch
):
    original = replace(meta, rewards_daily_rate=70.0, reward_competitiveness=33.0)
    eng = _engine_with_market(tmp_path, original)

    class FakeGamma:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return None

        async def markets_by_condition(self, _condition_ids):
            return {meta.condition_id: {"acceptingOrders": True, "closed": False}}

    async def fail_rewards(*_args, **_kwargs):
        raise RewardMarketsReadError("down")

    monkeypatch.setattr("polymaker.engine.GammaClient", lambda *_args, **_kwargs: FakeGamma())
    monkeypatch.setattr("polymaker.engine.fetch_reward_markets", fail_rewards)

    await eng.refresh_market_metadata()

    assert eng.metas[meta.condition_id].rewards_daily_rate == 70.0
    assert eng.metas[meta.condition_id].reward_competitiveness == 33.0
    eng.state.close()
    eng.catalog.close()


async def test_slug_cold_start_resolves_before_reward_refresh(tmp_path, meta):
    cfg = Config(paths=PathsConfig(
        db=str(tmp_path / "cold.db"),
        journal_dir=str(tmp_path / "j"),
        log_dir=str(tmp_path / "l"),
    ))
    engine = Engine(cfg, paper=True)
    raw = {
        "conditionId": meta.condition_id,
        "question": meta.question,
        "slug": meta.slug,
        "clobTokenIds": '["yes-token", "no-token"]',
        "outcomes": '["Yes", "No"]',
        "acceptingOrders": True,
        "rewardsMinSize": 10,
        "rewardsMaxSpread": 3,
    }

    class FakeGamma:
        async def resolve_tag_id(self, _slug):
            return "tag"

        async def iter_markets(self, **_kwargs):
            yield raw

    resolved = await engine._fetch_meta(FakeGamma(), meta.slug, None)

    assert resolved is not None
    assert resolved.condition_id == meta.condition_id
    assert resolved.reward_competitiveness is None
    engine.state.close()
    engine.catalog.close()
