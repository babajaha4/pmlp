"""Offline journal replay and conservative fill-model tests."""

from __future__ import annotations

import json

import pytest

from polymaker.backtest import (
    BacktestError,
    JournalBacktester,
    JournalEvent,
    ReplayOptions,
    load_journal,
)
from polymaker.config import Config, RiskConfig, StrategyProfile


def _book(ts: float, token: str, cid: str, bid: float, ask: float) -> JournalEvent:
    return JournalEvent(
        ts,
        "book",
        {
            "event_type": "book",
            "market": cid,
            "asset_id": token,
            "timestamp": str(int(ts * 1000)),
            "tick_size": "0.01",
            "bids": [{"price": str(bid), "size": "100"}],
            "asks": [{"price": str(ask), "size": "100"}],
        },
        0,
    )


def _trade(ts: float, token: str, cid: str, price: float, size: float) -> JournalEvent:
    return JournalEvent(
        ts,
        "last_trade_price",
        {
            "event_type": "last_trade_price",
            "market": cid,
            "asset_id": token,
            "timestamp": str(int(ts * 1000)),
            "price": str(price),
            "size": str(size),
            "side": "SELL",
        },
        0,
    )


def _simulator(meta, profile, **options) -> JournalBacktester:
    cfg = Config(risk=RiskConfig(
        max_total_exposure_usdc=10_000,
        max_market_notional_usdc=10_000,
        max_event_group_loss_usdc=10_000,
    ))
    return JournalBacktester(
        cfg,
        {meta.condition_id: meta},
        {meta.condition_id: profile},
        ReplayOptions(**options),
    )


def test_load_journal_sorts_and_counts_bad_lines(tmp_path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text(
        json.dumps({"ts": 2, "kind": "book", "data": {}}) + "\n"
        + "not-json\n"
        + json.dumps({"ts": 1.5, "kind": "orders_out", "data": []}) + "\n"
        + json.dumps({"ts": 1, "kind": "price_change", "data": {}}) + "\n",
        encoding="utf-8",
    )
    events, malformed = load_journal(path)
    assert [event.ts for event in events] == [1.0, 1.5, 2.0]
    assert malformed == 1


def test_replay_requires_l2_events(meta, profile) -> None:
    simulator = _simulator(meta, profile)
    try:
        with pytest.raises(BacktestError):
            simulator.run([JournalEvent(1, "orders_out", {}, 0)])
    finally:
        simulator.close()


def test_visible_queue_prevents_optimistic_fill(meta, profile) -> None:
    events = [
        _book(1, meta.yes.token_id, meta.condition_id, 0.49, 0.51),
        _book(1, meta.no.token_id, meta.condition_id, 0.49, 0.51),
        _trade(2, meta.yes.token_id, meta.condition_id, 0.49, 50),
    ]
    simulator = _simulator(meta, profile, queue_ahead_fraction=1.0, quote_latency_ms=0)
    try:
        result = simulator.run(events)
    finally:
        simulator.close()
    assert result.placed_orders > 0
    assert result.fills == 0
    assert result.fill_probability == 0


def test_trade_through_fills_and_records_adverse_markout(meta) -> None:
    profile = StrategyProfile(
        base_size_usdc=10,
        layers=1,
        reward_size_mult=0,
        event_jump_ticks=100,
        min_edge_ticks=1,
    )
    events = [
        _book(1, meta.yes.token_id, meta.condition_id, 0.49, 0.51),
        _book(1, meta.no.token_id, meta.condition_id, 0.49, 0.51),
        _trade(2, meta.yes.token_id, meta.condition_id, 0.48, 100),
        _book(3, meta.yes.token_id, meta.condition_id, 0.39, 0.41),
    ]
    simulator = _simulator(
        meta,
        profile,
        queue_ahead_fraction=1.0,
        quote_latency_ms=0,
        markout_seconds=1,
    )
    try:
        result = simulator.run(events)
    finally:
        simulator.close()
    assert result.fills == 1
    assert result.filled_notional > 0
    assert result.trading_mtm_pnl < 0
    assert result.mean_markout_bps is not None
    assert result.mean_markout_bps < 0
    assert result.max_capital_at_risk <= 10_000


def test_replay_is_deterministic(meta, profile) -> None:
    events = [
        _book(1, meta.yes.token_id, meta.condition_id, 0.49, 0.51),
        _book(1, meta.no.token_id, meta.condition_id, 0.49, 0.51),
        _trade(2, meta.yes.token_id, meta.condition_id, 0.48, 20),
    ]
    results = []
    for _ in range(2):
        simulator = _simulator(meta, profile, queue_ahead_fraction=0, quote_latency_ms=0)
        try:
            results.append(simulator.run(events).to_dict())
        finally:
            simulator.close()
    assert results[0] == results[1]
