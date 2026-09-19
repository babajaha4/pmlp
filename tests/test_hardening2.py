"""Hardening batch 2: Tier 0-3 fixes — inflight expiry, crossed-book guard,
metadata halt, load shed, exit floor, divergence correction, per-market lock,
CSV export, WAL/pnl."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock

import pytest

from polymaker.domain import Fill, Position, Regime, Side
from polymaker.execution.gateway import GatewayReadError
from polymaker.state.store import StateStore
from polymaker.strategy.quoting import QuoteInputs, construct_quotes
from polymaker.userstream.parse import normalize_trade
from tests.conftest import view
from tests.test_engine import _engine_with_market, _feed_book
from tests.test_userstream_parse import FUNDER, _production_trade_payload


def _confirmed_trade(meta):
    payload = _production_trade_payload(status="CONFIRMED")
    payload["timestamp"] = time.time()
    payload["asset_id"] = meta.no.token_id
    payload["maker_orders"] = [mo for mo in payload["maker_orders"]
                               if mo["maker_address"] == FUNDER]
    payload["maker_orders"][0]["asset_id"] = meta.yes.token_id
    return payload


async def test_first_trade_sync_repairs_rest_position_without_cash(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.state.set_position(meta.yes.token_id, 50, 0.123)
    eng.gateway._funder = FUNDER
    eng.gateway.trades = AsyncMock(return_value=[_confirmed_trade(meta)])
    eng.gateway.positions = AsyncMock(return_value={meta.yes.token_id: (50, 0.123)})
    eng.gateway.open_orders = AsyncMock(return_value=[])
    await eng._reconcile_authoritative_state(startup=True)
    assert eng.state.position(meta.yes.token_id).size == 50
    assert eng.state.fill_count() == 1
    assert eng.risk.net_cash == pytest.approx(-6.15)
    assert eng.state.get_sync_value("confirmed_trade_sync_initialized") == "1"


async def test_initialized_position_mismatch_remains_state_unknown(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.state.set_sync_value("confirmed_trade_sync_initialized", "1")
    eng.gateway.trades = AsyncMock(return_value=[])
    eng.gateway.positions = AsyncMock(return_value={meta.yes.token_id: (50, 0.123)})
    eng.gateway.open_orders = AsyncMock(return_value=[])
    for _ in range(2):
        with pytest.raises(GatewayReadError, match="do not explain positions"):
            await eng._reconcile_authoritative_state()
        assert eng._state_unknown
        assert eng.state.position(meta.yes.token_id).size == 50
    eng.state.close()
    eng.catalog.close()
    restarted = _engine_with_market(tmp_path, meta)
    restarted.gateway.trades = AsyncMock(return_value=[])
    restarted.gateway.positions = AsyncMock(return_value={meta.yes.token_id: (50, 0.123)})
    restarted.gateway.open_orders = AsyncMock(return_value=[])
    with pytest.raises(GatewayReadError, match="do not explain positions"):
        await restarted._reconcile_authoritative_state(startup=True)


def test_ledger_inventory_and_identity_economics(tmp_path):
    store = StateStore(tmp_path / "ledger.db")
    fill = Fill("token", Side.BUY, 0.2, 10, "canonical", 123)
    store.apply_fill(fill, aliases=("legacy",))
    store.apply_fill(Fill("token", Side.SELL, 0.3, 4, "sell", 124))
    assert store.fill_position_sizes() == {"token": 6.0}
    assert store.fill_identity_matches(fill, aliases=("legacy",))
    assert not store.fill_identity_matches(Fill("token", Side.BUY, 0.4, 10, "canonical", 123))
    assert not store.fill_identity_matches(fill, aliases=("sell",))
    assert not store.fill_identity_matches(Fill("token", Side.BUY, 0.2, 10, "missing", 123))


@pytest.mark.parametrize("field,value", [("price", "nan"), ("matched_amount", "bad"),
                                         ("side", "invalid")])
async def test_malformed_owned_trade_cannot_checkpoint(tmp_path, meta, field, value):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    payload = _confirmed_trade(meta)
    payload["maker_orders"][0][field] = value
    eng.gateway.trades = AsyncMock(return_value=[payload])
    with pytest.raises(GatewayReadError, match="invalid confirmed trade"):
        await eng._sync_confirmed_trades()
    assert eng.state.get_sync_value("confirmed_trade_sync_ts") is None
    assert eng.state.fill_count() == 0


async def test_rest_identity_conflict_is_quarantined(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    payload = _confirmed_trade(meta)
    ev = normalize_trade(payload, FUNDER, eng._other_token)[0]
    eng.user_proc.on_trade(ev, meta.condition_id)
    payload["maker_orders"][0]["price"] = "0.4"
    eng.gateway.trades = AsyncMock(return_value=[payload])
    with pytest.raises(GatewayReadError, match="identity conflict"):
        await eng._sync_confirmed_trades()
    assert eng.state.get_sync_value("confirmed_trade_sync_ts") is None
    assert eng.risk.net_cash == pytest.approx(-6.15)


async def test_startup_reads_trades_positions_orders_in_order(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    reads = []

    async def trades(**kwargs):
        reads.append("trades")
        return [_confirmed_trade(meta)]

    async def positions():
        reads.append("positions")
        return {meta.yes.token_id: (50, 0.123)}

    async def orders():
        reads.append("orders")
        return []

    eng.gateway.trades = trades
    eng.gateway.positions = positions
    eng.gateway.open_orders = orders
    await eng._startup_reconcile()
    assert reads == ["trades", "positions", "orders"]
    assert eng.risk.net_cash == pytest.approx(-6.15)


async def test_trade_read_failure_sets_state_unknown_and_places_nothing(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway.trades = AsyncMock(side_effect=GatewayReadError("trade read failed"))
    eng.gateway.place = AsyncMock()
    with pytest.raises(GatewayReadError, match="trade read failed"):
        await eng._startup_reconcile()
    assert eng._state_unknown
    await eng._recompute(meta.condition_id)
    eng.gateway.place.assert_not_awaited()


async def test_periodic_sync_applies_confirmed_fill_before_positions(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    eng.state.set_sync_value("confirmed_trade_sync_initialized", "1")
    eng.gateway.trades = AsyncMock(return_value=[_confirmed_trade(meta)])
    eng.gateway.positions = AsyncMock(return_value={meta.yes.token_id: (50, 0.123)})

    async def orders():
        eng._running = False
        return []

    eng.gateway.open_orders = orders
    eng._reconcile_now.set()
    await eng._reconcile_loop()
    assert eng.risk.net_cash == pytest.approx(-6.15)
    assert eng.state.position(meta.yes.token_id).size == 50


async def test_authoritative_failure_immediately_cancels_configured_assets(tmp_path, meta, monkeypatch):
    eng = _engine_with_market(tmp_path, meta)
    _feed_book(eng, meta)
    await eng._recompute(meta.condition_id)
    assert eng.state.orders
    eng.gateway.trades = AsyncMock(side_effect=GatewayReadError("trade read failed"))
    eng.gateway.place = AsyncMock()
    cancelled = []

    async def cancel_asset(tok):
        assert eng._state_unknown
        cancelled.append(tok)
        return True

    async def stop_on_backoff(delay):
        eng._running = False

    eng.gateway.cancel_asset = cancel_asset
    monkeypatch.setattr("polymaker.engine.asyncio.sleep", stop_on_backoff)
    eng._reconcile_now.set()
    await eng._reconcile_loop()
    assert set(cancelled) == {meta.yes.token_id, meta.no.token_id}
    assert eng._state_unknown
    eng.gateway.place.assert_not_awaited()


async def test_rest_match_time_and_legacy_id_are_replayable(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    payload = _confirmed_trade(meta)
    payload["match_time"] = payload.pop("timestamp")
    payload["maker_orders"][0].pop("order_id")
    eng.gateway.trades = AsyncMock(return_value=[payload])
    assert await eng._sync_confirmed_trades(full_day=True) == 1
    assert await eng._sync_confirmed_trades(full_day=True) == 0
    assert eng.state.fill_count() == 1
    assert eng.risk.net_cash == pytest.approx(-6.15)


async def test_pre_day_inventory_baseline_survives_rest_retry(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    eng.gateway.trades = AsyncMock(return_value=[])
    eng.gateway.positions = AsyncMock(return_value={meta.yes.token_id: (20, 0.4)})
    eng.gateway.open_orders = AsyncMock(return_value=[])
    await eng._reconcile_authoritative_state(startup=True)
    eng.gateway.trades = AsyncMock(return_value=[_confirmed_trade(meta)])
    eng.gateway.positions = AsyncMock(return_value={meta.yes.token_id: (70, 0.2)})
    await eng._reconcile_authoritative_state()
    assert eng.state.position(meta.yes.token_id).size == 70
    eng._state_unknown = True
    await eng._reconcile_authoritative_state()
    assert not eng._state_unknown
    assert eng.risk.net_cash == pytest.approx(-6.15)


async def test_ws_and_rest_same_leg_is_counted_once(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    payload = _confirmed_trade(meta)
    payload["status"] = "MATCHED"
    eng.user_proc.on_trade(normalize_trade(payload, FUNDER, eng._other_token)[0], meta.condition_id)
    payload["status"] = "CONFIRMED"
    eng.gateway.trades = AsyncMock(return_value=[payload])
    assert await eng._sync_confirmed_trades(full_day=True) == 0
    assert eng.state.fill_count() == 1
    assert eng.state.inflight(meta.yes.token_id) == 0
    assert eng.risk.net_cash == pytest.approx(-6.15)


async def test_trade_window_overlap_is_clamped_to_utc_day(tmp_path, meta, monkeypatch):
    eng = _engine_with_market(tmp_path, meta)
    monkeypatch.setattr("polymaker.engine.time.time", lambda: 1700000000.0)
    eng.gateway.trades = AsyncMock(return_value=[])
    await eng._sync_confirmed_trades()
    eng.gateway.trades.assert_awaited_with(after=1699920000)
    eng.state.set_sync_value("confirmed_trade_sync_initialized", "1")
    await eng._sync_confirmed_trades()
    eng.gateway.trades.assert_awaited_with(after=1699999700)
    eng.state.set_sync_value("confirmed_trade_sync_ts", "1699920001")
    await eng._sync_confirmed_trades()
    eng.gateway.trades.assert_awaited_with(after=1699920000)
    await eng._sync_confirmed_trades(full_day=True)
    eng.gateway.trades.assert_awaited_with(after=1699920000)


async def test_rest_sync_only_applies_confirmed_configured_tokens(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    matched = _confirmed_trade(meta)
    matched["id"] = "not-confirmed"
    matched["status"] = "MATCHED"
    untracked = _confirmed_trade(meta)
    untracked["id"] = "manual"
    untracked["maker_orders"][0]["asset_id"] = "manual-token"
    eng.gateway.trades = AsyncMock(return_value=[matched, untracked, _confirmed_trade(meta)])
    assert await eng._sync_confirmed_trades() == 1
    assert eng.state.fill_count() == 1
    assert eng.state.position("manual-token").size == 0
    assert eng.risk.net_cash == pytest.approx(-6.15)


async def test_unknown_recovery_does_not_skip_inflight_inventory_proof(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.state.set_sync_value("confirmed_trade_sync_initialized", "1")
    eng.state.mark_inflight(meta.yes.token_id)
    eng._state_unknown = True
    eng.gateway.trades = AsyncMock(return_value=[])
    eng.gateway.positions = AsyncMock(return_value={meta.yes.token_id: (50, 0.123)})
    eng.gateway.open_orders = AsyncMock(return_value=[])
    with pytest.raises(GatewayReadError, match="do not explain positions"):
        await eng._reconcile_authoritative_state()
    assert eng._state_unknown


@pytest.mark.parametrize("lifecycle", ["confirmed", "matched-confirmed"])
@pytest.mark.parametrize("boundary", ["orders", "lock"])
async def test_fill_during_snapshot_is_not_overwritten(tmp_path, meta, lifecycle, boundary):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    eng.state.set_sync_value("confirmed_trade_sync_initialized", "1")
    eng.gateway.trades = AsyncMock(return_value=[])
    eng.gateway.positions = AsyncMock(return_value={})
    reached = asyncio.Event()
    proceed = asyncio.Event()

    async def orders():
        reached.set()
        if boundary == "orders":
            await proceed.wait()
        return []

    eng.gateway.open_orders = orders
    lock = eng._locks[meta.condition_id]
    if boundary == "lock":
        await lock.acquire()
    task = asyncio.create_task(eng._reconcile_authoritative_state())
    await reached.wait()
    payload = _confirmed_trade(meta)
    if lifecycle == "matched-confirmed":
        payload["status"] = "MATCHED"
        eng.user_proc.on_trade(normalize_trade(payload, FUNDER, eng._other_token)[0], meta.condition_id)
    payload["status"] = "CONFIRMED"
    eng.user_proc.on_trade(normalize_trade(payload, FUNDER, eng._other_token)[0], meta.condition_id)
    proceed.set()
    if boundary == "lock":
        lock.release()
    with pytest.raises(GatewayReadError, match="ledger changed"):
        await task
    assert eng.state.position(meta.yes.token_id).size == 50
    assert eng.state.fill_position_sizes()[meta.yes.token_id] == 50
    assert eng._state_unknown


async def test_negative_signed_inventory_cannot_be_proven_by_zero_rest(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.state.set_sync_value("confirmed_trade_sync_initialized", "1")
    eng.state.apply_fill(Fill(meta.yes.token_id, Side.SELL, 0.3, 50, "unexplained-sell"))
    eng.gateway.trades = AsyncMock(return_value=[])
    eng.gateway.positions = AsyncMock(return_value={})
    eng.gateway.open_orders = AsyncMock(return_value=[])
    with pytest.raises(GatewayReadError, match="do not explain positions"):
        await eng._reconcile_authoritative_state()
    assert eng._state_unknown
    assert eng.state.position(meta.yes.token_id).size == 0
    assert eng.state.fill_position_sizes()[meta.yes.token_id] == -50


@pytest.mark.parametrize("recovery", ["overlap", "restart", "utc-rollover"])
async def test_pending_net_flat_roundtrip_stays_in_replay_window(tmp_path, meta, monkeypatch, recovery):
    clock = [1700000000.0]
    monkeypatch.setattr("polymaker.engine.time.time", lambda: clock[0])
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    eng.state.set_sync_value("confirmed_trade_sync_initialized", "1")
    buy = _confirmed_trade(meta)
    buy["id"] = "pending-buy"
    buy["status"] = "MATCHED"
    buy["maker_orders"][0].update(price="0.2", matched_amount="10")
    sell = _confirmed_trade(meta)
    sell["id"] = "pending-sell"
    sell["status"] = "MINED"
    sell["maker_orders"][0].update(side="SELL", price="0.3", matched_amount="10")
    rows = [buy, sell]
    requested = []

    async def trades(*, after):
        requested.append(after)
        return [row for row in rows if float(row["timestamp"]) > after]

    eng.gateway.trades = trades
    eng.gateway.positions = AsyncMock(return_value={})
    eng.gateway.open_orders = AsyncMock(return_value=[])
    await eng._reconcile_authoritative_state()
    clock[0] += 1000
    await eng._reconcile_authoritative_state()
    if recovery != "overlap":
        eng.state.close()
        eng.catalog.close()
        eng = _engine_with_market(tmp_path, meta)
        eng.gateway._funder = FUNDER
        eng.gateway.trades = trades
        eng.gateway.positions = AsyncMock(return_value={})
        eng.gateway.open_orders = AsyncMock(return_value=[])
    if recovery == "utc-rollover":
        clock[0] = 1700093000.0
    buy["status"] = sell["status"] = "CONFIRMED"
    await eng._reconcile_authoritative_state()
    assert eng.state.fill_count() == 2
    assert eng.risk.net_cash == pytest.approx(1.0)
    assert eng.state.position(meta.yes.token_id).size == 0
    assert requested[-1] <= 1699999700
    await eng._sync_confirmed_trades()
    assert requested[-1] > 1700000000


async def test_failed_pending_trade_is_not_released_with_unreversed_fill(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    payload = _confirmed_trade(meta)
    payload["status"] = "MATCHED"
    event = normalize_trade(payload, FUNDER, eng._other_token)[0]
    eng.user_proc.on_trade(event, meta.condition_id)
    eng.gateway.trades = AsyncMock(return_value=[payload])
    await eng._sync_confirmed_trades()
    eng.state.close()
    eng.catalog.close()
    restarted = _engine_with_market(tmp_path, meta)
    restarted.gateway._funder = FUNDER
    payload["status"] = "FAILED"
    restarted.gateway.trades = AsyncMock(return_value=[payload])
    with pytest.raises(GatewayReadError, match="durable reversal"):
        await restarted._sync_confirmed_trades()


def test_failure_identity_requires_equal_durable_reversal(tmp_path):
    store = StateStore(tmp_path / "failed.db")
    original = Fill("token", Side.BUY, 0.2, 10, "legacy", 123)
    canonical = Fill("token", Side.BUY, 0.2, 10, "canonical", 123)
    assert store.fill_failure_settled(canonical, aliases=("legacy",))
    store.apply_fill(original, aliases=("canonical",))
    assert not store.fill_failure_settled(canonical, aliases=("legacy",))
    store.apply_fill(Fill("token", Side.SELL, 0.2, 10, "legacy:reverse", 123))
    assert store.fill_failure_settled(canonical, aliases=("legacy",))
    assert not store.fill_failure_settled(Fill("token", Side.BUY, 0.4, 10, "canonical", 123))


async def test_pending_recovery_cap_keeps_metadata_and_fails_closed(tmp_path, meta, monkeypatch):
    clock = [1700000000.0]
    monkeypatch.setattr("polymaker.engine.time.time", lambda: clock[0])
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    payload = _confirmed_trade(meta)
    payload["status"] = "MATCHED"
    old_buy = _confirmed_trade(meta)
    old_buy.update(id="older-settled-buy", timestamp=1699999900)
    old_sell = _confirmed_trade(meta)
    old_sell.update(id="older-settled-sell", timestamp=1699999901)
    old_sell["maker_orders"][0]["side"] = "SELL"
    eng.gateway.trades = AsyncMock(return_value=[old_buy, old_sell, payload])
    await eng._sync_confirmed_trades()
    pending = eng.state.get_sync_value("confirmed_trade_pending")
    clock[0] += 7 * 86400
    await eng._sync_confirmed_trades()
    assert eng.gateway.trades.call_args.kwargs["after"] == 1699999700
    clock[0] += 1
    with pytest.raises(GatewayReadError, match="7-day recovery"):
        await eng._reconcile_authoritative_state()
    assert eng._state_unknown
    assert eng.state.get_sync_value("confirmed_trade_pending") == pending
    assert eng.state.get_sync_value("confirmed_trade_sync_ts") == "1700604800"


@pytest.mark.parametrize("pending", ["", "not json", "[]", '{"leg": -1}', '{"leg": "123"}',
                                      '{"leg": NaN}', '{"leg": true}', '{"leg": ' + "9" * 400 + "}"])
async def test_invalid_pending_metadata_cannot_advance_snapshot(tmp_path, meta, pending):
    eng = _engine_with_market(tmp_path, meta)
    eng.state.set_sync_value("confirmed_trade_pending", pending)
    eng.gateway.trades = AsyncMock(return_value=[])
    with pytest.raises(GatewayReadError, match="pending trade metadata"):
        await eng._reconcile_authoritative_state()
    assert eng._state_unknown
    assert eng.state.get_sync_value("confirmed_trade_sync_ts") is None
    eng.gateway.trades.assert_not_awaited()


# ── T0-1: inflight expiry ────────────────────────────────────────────────
def test_inflight_expires_after_max_age(tmp_path):
    s = StateStore(tmp_path / "s.db")
    s.mark_inflight("tok")
    assert s.inflight("tok") == 1
    # not yet stale
    assert s.expire_inflight(max_age_s=100) == []
    assert s.inflight("tok") == 1
    # force age by rewriting the stored ts
    s._inflight_ts["tok"] = time.time() - 999
    cleared = s.expire_inflight(max_age_s=100)
    assert cleared == ["tok"]
    assert s.inflight("tok") == 0
    s.close()


# ── T0-7: exit sizing floors (never over-sell) ───────────────────────────
def test_exit_size_is_floored(meta, profile):
    # hold a fractional position; the SELL must be floored so size <= held
    tq = construct_quotes(QuoteInputs(
        meta=meta, regime=Regime.REDUCE_ONLY, fv=0.5, vol_short=0.0, toxicity=0.0,
        yes_view=view(0.49, 0.51), no_view=view(0.49, 0.51),
        pos_yes=Position("yes-token", 17.999, 0.4), pos_no=Position("no-token"),
        profile=profile, now=1000.0,
    ))
    sells = [q for q in tq.quotes if q.side == Side.SELL]
    assert sells
    assert sells[0].size <= 17.999  # floored, never rounded up past the holding
    assert sells[0].size == 17.99


# ── T0-5: crossed-book guard ─────────────────────────────────────────────
async def test_crossed_book_skips_quoting(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    now = time.time()
    # crossed: best bid (0.55) above best ask (0.45)
    eng.md.book(meta.yes.token_id).apply_snapshot(bids=[(0.55, 100)], asks=[(0.45, 100)], ts=now)
    eng.md.book(meta.no.token_id).apply_snapshot(bids=[(0.45, 100)], asks=[(0.55, 100)], ts=now)
    await eng._recompute(meta.condition_id)
    assert eng.state.orders == {}  # no quotes on a nonsensical book
    eng.state.close()
    eng.catalog.close()


# ── T0-2: metadata halt pulls quotes ─────────────────────────────────────
async def test_halted_market_pulls_quotes(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    _feed_book(eng, meta)
    await eng._recompute(meta.condition_id)
    assert len(eng.state.orders) > 0  # quoting normally
    # market flagged closed/not-accepting by the metadata refresh
    eng._halted.add(meta.condition_id)
    await eng._recompute(meta.condition_id)
    assert eng.state.orders == {}  # all pulled
    eng.state.close()
    eng.catalog.close()


# ── T1: load shedding under order pressure ───────────────────────────────
async def test_load_shed_skips_new_quotes_under_pressure(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.paper = False  # shed only applies live
    _feed_book(eng, meta)
    placed_calls: list[int] = []

    async def spy_place(quotes, m):
        placed_calls.append(len(quotes))
        return []

    async def no_cancel(ids):
        return True

    # force high pressure
    for _ in range(1000):
        eng.gateway._order_bucket._tokens = 0.0
    eng.gateway.place = spy_place  # type: ignore[method-assign]
    eng.gateway.cancel = no_cancel  # type: ignore[method-assign]
    await eng._recompute(meta.condition_id)
    assert eng.gateway.order_pressure > 0.85
    assert placed_calls == []  # new quotes shed, not placed
    eng.state.close()
    eng.catalog.close()


# ── per-market lock serializes recompute vs reconcile ────────────────────
async def test_recompute_holds_market_lock(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    _feed_book(eng, meta)
    lock = eng._locks[meta.condition_id]
    await lock.acquire()  # simulate reconcile holding it

    async def try_recompute():
        await eng._recompute(meta.condition_id)

    task = asyncio.create_task(try_recompute())
    await asyncio.sleep(0.05)
    assert not task.done()  # blocked on the lock
    lock.release()
    await task  # now proceeds
    eng.state.close()
    eng.catalog.close()


# ── T1: on-chain divergence correction ───────────────────────────────────
async def test_divergence_corrects_to_onchain(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    tok = meta.yes.token_id
    eng.state.apply_fill(Fill(tok, Side.BUY, 0.5, 100, "phantom"))  # internal says 100
    assert eng.state.position(tok).size == 100

    async def fake_balances(tokens):
        return {t: (5.0 if t == tok else 0.0) for t in tokens}  # chain says 5

    eng.gateway.token_balances = fake_balances  # type: ignore[method-assign]
    await eng._check_position_divergence()
    assert eng.state.position(tok).size == 5.0  # corrected to on-chain truth
    eng.state.close()
    eng.catalog.close()


# ── churn bug: resting orders must NOT shrink the size taper ─────────────
def test_open_orders_are_hard_reservations_without_taper_churn(tmp_path, meta):
    """Resting BUYs reserve capital; they do not repeatedly taper themselves.

    A new quote batch is rejected once reservations plus inventory exceed a cap.
    """
    from polymaker.config import RiskConfig
    from polymaker.domain import OpenOrder, OrderState
    from polymaker.risk.manager import RiskManager

    store = StateStore(tmp_path / "s.db")
    rm = RiskManager(RiskConfig(max_market_notional_usdc=15.0), store)
    rm.update_mark(meta.yes.token_id, 0.2)
    rm.update_mark(meta.no.token_id, 0.8)
    # rest ~$14 of BUY orders (near cap) but hold NO inventory
    store.upsert_order(OpenOrder("y", meta.yes.token_id, Side.BUY, 0.2, 50, OrderState.LIVE))
    store.upsert_order(OpenOrder("n", meta.no.token_id, Side.BUY, 0.79, 6, OrderState.LIVE))
    d = rm.evaluate(meta, ws_stale=False, event_group_cost=0.0)
    assert not d.reduce_only
    assert d.size_scale == 1.0  # resting orders do not taper -> no churn
    # but FILLED inventory near cap DOES taper without treating replaceable
    # resting orders as another copy of the desired target
    store.apply_fill(Fill(meta.yes.token_id, Side.BUY, 0.2, 70, "f"))  # $14 position
    d2 = rm.evaluate(meta, ws_stale=False, event_group_cost=0.0)
    assert not d2.reduce_only
    assert 0.0 < d2.size_scale < 1.0
    store.close()


# ── operator positions in other markets must not leak into bot state ─────
def test_untracked_positions_are_dropped_and_filtered(tmp_path, meta):
    """Manual UI bets in markets the bot doesn't trade must not enter state,
    exposure caps, or PnL — neither from the DB (stale) nor from the API."""
    eng = _engine_with_market(tmp_path, meta)
    # stale DB leak: a sports position from an earlier unscoped reconcile
    eng.state.set_position("sports-token", 370.0, 0.54)
    dropped = eng.state.drop_untracked_positions(set(eng._token_cid))
    assert dropped == ["sports-token"]
    assert eng.state.position("sports-token").size == 0
    # API filter: only traded tokens survive _only_traded
    api = {"sports-token": (370.0, 0.54), meta.yes.token_id: (10.0, 0.2)}
    filtered = eng._only_traded(api)
    assert "sports-token" not in filtered
    assert meta.yes.token_id in filtered
    eng.state.close()
    eng.catalog.close()


def test_per_layer_reward_floor(meta, profile):
    """Every resting ORDER must meet the rewards min size (scoring is per order).
    NO at ~0.80 with $100 base -> layers bump to the 100-share floor."""
    from dataclasses import replace

    m = replace(meta, rewards_min_size=100.0)
    p = profile.with_overrides({"base_size_usdc": 100.0, "layers": 2})
    tq = construct_quotes(QuoteInputs(
        meta=m, regime=Regime.QUIET, fv=0.20, vol_short=0.0, toxicity=0.0,
        yes_view=view(0.195, 0.197), no_view=view(0.802, 0.805),
        pos_yes=Position("yes-token"), pos_no=Position("no-token"),
        profile=p, now=1000.0,
    ))
    buys = [q for q in tq.quotes if q.side == Side.BUY]
    assert buys
    assert all(q.size >= 100.0 for q in buys), [q.size for q in buys]


# ── quoter wake cadence: slow baseline, precise cool-off re-entry ────────
async def test_quoter_wake_cadence(tmp_path, meta):
    from polymaker.domain import Fill
    from polymaker.strategy.regime import RegimeInputs

    eng = _engine_with_market(tmp_path, meta)
    # flat + QUIET -> slow baseline tick
    assert eng._next_wake_s(meta.condition_id, 60.0) == 60.0
    # in an EVENT cool-off -> wake right when it ends, not a full minute later
    p = eng.profiles[meta.condition_id]
    eng.regime_m[meta.condition_id].decide(
        RegimeInputs(now=time.time(), tick=0.001, fv=0.2, prev_fv=0.2, vol_ratio=1.0,
                     flow_z=0.0, inventory_util=0.0, hours_to_end=999.0, sweep_flagged=True), p)
    w = eng._next_wake_s(meta.condition_id, 60.0)
    assert 0 < w <= p.event_cooloff_s + 1
    # holding inventory -> fast tick to manage exits
    eng.state.apply_fill(Fill(meta.yes.token_id, Side.BUY, 0.2, 50, "f"))
    assert eng._next_wake_s(meta.condition_id, 60.0) <= 10.0
    eng.state.close()
    eng.catalog.close()


async def test_aged_inventory_exit_walks_to_maker_touch(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    _feed_book(eng, meta)
    profile = eng.profiles[meta.condition_id]
    eng.state.apply_fill(Fill(
        meta.yes.token_id,
        Side.BUY,
        0.2,
        10,
        "aged-fill",
        ts=time.time() - profile.exit_urgency_s - 1,
    ))

    await eng._recompute(meta.condition_id)

    sells = [
        order for order in eng.state.orders.values()
        if order.token_id == meta.yes.token_id and order.side is Side.SELL
    ]
    assert len(sells) == 1
    best_bid = eng.md.book(meta.yes.token_id).best_bid()
    assert best_bid is not None
    assert sells[0].price == pytest.approx(best_bid.price + meta.tick_size)
    eng.state.close()
    eng.catalog.close()


# ── a quiet market with a live WS link must NOT false-halt ───────────────
async def test_quiet_market_with_live_link_is_not_stale(tmp_path, meta):
    """Thin/quiet markets go long stretches with no book mutation. Halting on
    book-recency would zero their rewards. With the link up we must keep quoting;
    only a genuinely DOWN link past the grace window halts."""
    eng = _engine_with_market(tmp_path, meta)
    _feed_book(eng, meta)
    eng.md.connected = True
    eng.md.disconnected_since = 0.0
    # backdate the book so a book-recency check would (wrongly) read stale
    eng.md.book(meta.yes.token_id).local_ts = time.time() - 9999
    eng.md.book(meta.no.token_id).local_ts = time.time() - 9999
    await eng._recompute(meta.condition_id)
    assert len(eng.state.orders) > 0  # still quoting despite a silent book
    # a genuinely dead link past the grace window DOES halt
    eng.md.connected = False
    eng.md.disconnected_since = time.time() - 9999
    await eng._recompute(meta.condition_id)
    assert eng.state.orders == {}
    eng.state.close()
    eng.catalog.close()


# ── stale/past end-date must not halt a still-trading market ─────────────
def test_past_end_date_is_treated_as_unknown():
    """"Next PM" appointment markets carry a stale past endDate while still
    accepting orders. A past date must read as None (unknown), not 0 hours,
    else the regime machine HALTs a live market and never quotes."""
    from polymaker.engine import _hours_to_end

    now = time.time()
    assert _hours_to_end("2020-01-01T00:00:00Z", now) is None  # past -> unknown
    assert _hours_to_end(None, now) is None
    future = _hours_to_end("2099-01-01T00:00:00Z", now)
    assert future is not None and future > 0  # genuine future still measured


# ── T2: PnL snapshot + CSV export smoke ──────────────────────────────────
def test_pnl_snapshot_and_wal(tmp_path):
    s = StateStore(tmp_path / "s.db")
    s.record_pnl(100.0, 50.0, 50.0, 1.5)
    s.checkpoint_wal()  # must not raise
    row = s._conn.execute("SELECT equity, daily_pnl FROM pnl_snapshots").fetchone()
    assert row["equity"] == 100.0 and row["daily_pnl"] == 1.5
    s.close()


def test_catalog_csv_export(tmp_path):
    from polymaker.catalog.gamma import parse_market
    from polymaker.catalog.store import CatalogStore
    from tests.test_catalog import RAW

    store = CatalogStore(tmp_path / "c.db")
    store.upsert_market(parse_market(RAW, {"0xabc": 42.0}))
    out = tmp_path / "markets.csv"
    n = store.export_csv(out)
    assert n == 1
    text = out.read_text()
    assert "slug" in text and "will-x-win" in text and "condition_id" in text
    store.close()
