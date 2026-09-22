"""Final reconciliation review regressions, using only offline exchange doubles."""

from __future__ import annotations

import asyncio
import sqlite3
from unittest.mock import AsyncMock

import pytest

from polymaker.domain import Fill, OpenOrder, OrderState, Side
from polymaker.execution.gateway import GatewayReadError
from polymaker.userstream.parse import normalize_trade
from tests.test_engine import _engine_with_market, _feed_book
from tests.test_hardening2 import _confirmed_trade
from tests.test_userstream_parse import FUNDER


@pytest.mark.parametrize("path", ["matched", "confirmed", "rest"])
@pytest.mark.parametrize("side", ["BUY", "SELL"])
@pytest.mark.parametrize("restart_before", [False, True])
@pytest.mark.parametrize("loss", [False, True])
async def test_first_new_day_fill_uses_pretrade_equity(
    tmp_path, meta, monkeypatch, path, side, restart_before, loss,
):
    day = ["2026-09-13"]
    monkeypatch.setattr("polymaker.risk.manager._day_key", lambda: day[0])
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    old = _confirmed_trade(meta)
    old["id"] = "previous-day-holding"
    old["maker_orders"][0].update(price="0.4", matched_amount="10")
    eng.user_proc.on_trade(normalize_trade(old, FUNDER, eng._other_token)[0], meta.condition_id)
    eng.state.set_sync_value("confirmed_trade_sync_initialized", "1")
    eng.risk.update_mark(meta.yes.token_id, 0.5)
    eng.risk.reset_day()
    day[0] = "2026-09-14"
    if restart_before:
        eng.state.close()
        eng.catalog.close()
        eng = _engine_with_market(tmp_path, meta)
        eng.gateway._funder = FUNDER
    eng.risk.update_mark(meta.yes.token_id, 0.5)
    eng.cfg.risk.daily_loss_kill_usdc = 0.3
    payload = _confirmed_trade(meta)
    payload["id"] = "first-new-day-fill"
    price = (0.6 if side == "BUY" else 0.4) if loss else 0.5
    payload["maker_orders"][0].update(side=side, price=price, matched_amount="4")
    if path == "rest":
        eng.gateway.trades = AsyncMock(return_value=[payload])
        eng.gateway.positions = AsyncMock(return_value={meta.yes.token_id: (14 if side == "BUY" else 6, 0.4)})
        eng.gateway.open_orders = AsyncMock(return_value=[])
        await eng._startup_reconcile()
    else:
        payload["status"] = path.upper()
        eng.user_proc.on_trade(normalize_trade(payload, FUNDER, eng._other_token)[0], meta.condition_id)
    assert eng.state.load_risk_state(day[0])["day_start_equity"] == pytest.approx(1)
    assert eng.risk.equity == pytest.approx(0.6 if loss else 1)
    assert eng.risk.daily_pnl == pytest.approx(-0.4 if loss else 0)
    assert eng.risk.global_halt()[0] is loss
    eng.state.close()
    eng.catalog.close()
    restored = _engine_with_market(tmp_path, meta)
    restored.cfg.risk.daily_loss_kill_usdc = 0.3
    assert restored.state.load_risk_state(day[0])["day_start_equity"] == pytest.approx(1)
    assert restored.risk.daily_pnl == pytest.approx(-0.4 if loss else 0)
    assert restored.risk.global_halt()[0] is loss


@pytest.mark.parametrize("failure", ["trade-failed", "cash-callback"])
async def test_new_day_baseline_precedes_reversal_or_cash_callback_failure(
    tmp_path, meta, monkeypatch, failure,
):
    day = ["2026-09-13"]
    monkeypatch.setattr("polymaker.risk.manager._day_key", lambda: day[0])
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    payload = _confirmed_trade(meta)
    payload["status"] = "MATCHED"
    payload["maker_orders"][0].update(price="0.4", matched_amount="10")
    eng.user_proc.on_trade(normalize_trade(payload, FUNDER, eng._other_token)[0], meta.condition_id)
    eng.state.set_sync_value("confirmed_trade_sync_initialized", "1")
    eng.risk.update_mark(meta.yes.token_id, 0.5)
    eng.risk.reset_day()
    day[0] = "2026-09-14"
    if failure == "trade-failed":
        payload["status"] = "FAILED"
        eng.user_proc.on_trade(normalize_trade(payload, FUNDER, eng._other_token)[0], meta.condition_id)
    else:
        payload["id"] = "callback-fails"
        payload["maker_orders"][0].update(price="0.6", matched_amount="4")

        def fail_cash_callback(_fill):
            raise RuntimeError("crash after fill commit")

        monkeypatch.setattr(eng.risk, "note_fill", fail_cash_callback)
        with pytest.raises(RuntimeError, match="crash after fill commit"):
            eng.user_proc.on_trade(normalize_trade(payload, FUNDER, eng._other_token)[0], meta.condition_id)
    saved = eng.state.load_risk_state(day[0])
    assert saved is not None
    assert saved["day_start_equity"] == pytest.approx(1)
    eng.state.close()
    eng.catalog.close()
    restored = _engine_with_market(tmp_path, meta)
    restored.risk.update_mark(meta.yes.token_id, 0.5)
    assert restored.risk.daily_pnl == pytest.approx(-1 if failure == "trade-failed" else -0.4)


@pytest.mark.parametrize("source", ["rest", "websocket"])
async def test_pending_migration_cannot_create_baseline_and_confirmation_recovers(
    tmp_path, meta, source,
):
    eng = _engine_with_market(tmp_path, meta)
    _feed_book(eng, meta)
    eng.gateway._funder = FUNDER
    payload = _confirmed_trade(meta)
    payload["status"] = "MINED" if source == "rest" else "MATCHED"
    if source == "websocket":
        eng.user_proc.on_trade(normalize_trade(payload, FUNDER, eng._other_token)[0],
                               meta.condition_id)
    eng.gateway.trades = AsyncMock(return_value=[payload] if source == "rest" else [])
    eng.gateway.positions = AsyncMock(return_value={meta.yes.token_id: (50, 0.123)})
    eng.gateway.open_orders = AsyncMock(return_value=[])
    cancelled = []

    async def cancel_asset(token):
        cancelled.append(token)
        return True

    eng.gateway.cancel_asset = cancel_asset
    with pytest.raises(GatewayReadError, match="pending"):
        await eng._reconcile_authoritative_state(startup=True)
    assert eng._state_unknown
    assert eng.state.get_sync_value("confirmed_trade_sync_initialized") is None
    assert eng.state.get_sync_value(f"confirmed_trade_baseline:{meta.yes.token_id}") is None
    assert set(cancelled) == {meta.yes.token_id, meta.no.token_id}
    await eng._recompute(meta.condition_id)
    assert eng.state.orders == {}

    payload["status"] = "CONFIRMED"
    eng.gateway.trades = AsyncMock(return_value=[payload])
    await eng._reconcile_authoritative_state(startup=True)
    assert not eng._state_unknown
    assert eng.state.get_sync_value("confirmed_trade_sync_initialized") == "1"
    assert float(eng.state.get_sync_value(f"confirmed_trade_baseline:{meta.yes.token_id}")) == 0
    assert eng.state.position(meta.yes.token_id).size == 50
    assert eng.state.fill_count() == 1
    assert eng.risk.net_cash == pytest.approx(-6.15)


async def test_older_snapshot_cannot_undo_onchain_quarantine(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.state.set_sync_value("confirmed_trade_sync_initialized", "1")
    reached = asyncio.Event()
    proceed = asyncio.Event()

    async def orders():
        reached.set()
        await proceed.wait()
        return []

    eng.gateway.trades = AsyncMock(return_value=[])
    eng.gateway.positions = AsyncMock(return_value={})
    eng.gateway.open_orders = orders
    eng.gateway.token_balances = AsyncMock(return_value={meta.yes.token_id: 50.0})
    older = asyncio.create_task(eng._reconcile_authoritative_state())
    await asyncio.wait_for(reached.wait(), timeout=1)
    await eng._check_position_divergence()
    proceed.set()
    with pytest.raises(GatewayReadError, match="changed"):
        await asyncio.wait_for(older, timeout=1)
    assert eng._state_unknown
    assert eng.state.position(meta.yes.token_id).size == 50


@pytest.mark.parametrize("initialized", [False, True])
async def test_onchain_inventory_requires_ledger_proof_before_recovery(tmp_path, meta, initialized):
    eng = _engine_with_market(tmp_path, meta)
    _feed_book(eng, meta)
    eng.gateway._funder = FUNDER
    if initialized:
        eng.state.set_sync_value("confirmed_trade_sync_initialized", "1")
    await eng._recompute(meta.condition_id)
    assert eng.state.orders
    eng._dirty[meta.condition_id].clear()
    eng.gateway.token_balances = AsyncMock(return_value={meta.yes.token_id: 50.0})
    eng.gateway.trades = AsyncMock(return_value=[])
    eng.gateway.positions = AsyncMock(return_value={meta.yes.token_id: (50, 0.123)})
    eng.gateway.open_orders = AsyncMock(return_value=[])
    await eng._check_position_divergence()
    assert eng.state.position(meta.yes.token_id).size == 50
    assert eng._state_unknown
    assert eng._reconcile_now.is_set()
    assert not eng._dirty[meta.condition_id].is_set()
    assert eng.state.orders == {}
    assert eng.risk.net_cash == 0
    with pytest.raises(GatewayReadError, match="do not explain positions"):
        await eng._reconcile_authoritative_state()
    await eng._recompute(meta.condition_id)
    assert eng.state.orders == {}

    eng.state.close()
    eng.catalog.close()
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    eng.gateway.trades = AsyncMock(return_value=[])
    eng.gateway.positions = AsyncMock(return_value={meta.yes.token_id: (50, 0.123)})
    eng.gateway.open_orders = AsyncMock(return_value=[])
    with pytest.raises(GatewayReadError, match="do not explain positions"):
        await eng._reconcile_authoritative_state(startup=True)
    eng.gateway.trades = AsyncMock(return_value=[_confirmed_trade(meta)])
    await eng._reconcile_authoritative_state(startup=True)
    assert not eng._state_unknown
    assert eng.risk.net_cash == pytest.approx(-6.15)
    assert eng.state.position(meta.yes.token_id).size == 50
    assert eng.state.fill_count() == 1


async def test_quarantine_cleans_orders_from_already_submitted_placement(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    _feed_book(eng, meta)
    submitted = asyncio.Event()
    complete_post = asyncio.Event()
    read_started = asyncio.Event()
    manual = OpenOrder("manual", "other-token", Side.BUY, 0.2, 10, OrderState.LIVE)
    exchange_orders = {manual.order_id: manual}

    async def place(quotes, _meta, **_kwargs):
        submitted.set()
        await complete_post.wait()
        orders = [eng.gateway._paper_order(q) for q in quotes]
        exchange_orders.update((o.order_id, o) for o in orders)
        return orders

    async def cancel_asset(token):
        for oid, order in list(exchange_orders.items()):
            if order.token_id == token:
                del exchange_orders[oid]
        return True

    async def trades(**_kwargs):
        read_started.set()
        raise GatewayReadError("lost authoritative trades")

    eng.gateway.place = place
    eng.gateway.cancel_asset = cancel_asset
    eng.gateway.trades = trades
    quote = asyncio.create_task(eng._recompute(meta.condition_id))
    await asyncio.wait_for(submitted.wait(), timeout=1)
    cleanup = asyncio.create_task(eng._reconcile_authoritative_state())
    await asyncio.wait_for(read_started.wait(), timeout=1)
    complete_post.set()
    await asyncio.wait_for(quote, timeout=1)
    with pytest.raises(GatewayReadError):
        await asyncio.wait_for(cleanup, timeout=1)
    assert exchange_orders == {"manual": manual}
    assert eng.state.orders == {}

@pytest.mark.parametrize("boundary", ["reservation", "rate-budget"])
@pytest.mark.parametrize("halt", ["quarantine", "kill"])
async def test_waiting_quote_cannot_place_after_halt(tmp_path, meta, boundary, halt):
    eng = _engine_with_market(tmp_path, meta)
    _feed_book(eng, meta)
    reached = asyncio.Event()
    proceed = asyncio.Event()
    read_started = asyncio.Event()
    issued = []
    cancelled = []
    original_paper_order = eng.gateway._paper_order

    class PausedReservation:
        async def __aenter__(self):
            reached.set()
            await proceed.wait()

        async def __aexit__(self, *_args):
            return None

    async def rate_budget(_size):
        reached.set()
        await proceed.wait()

    def paper_order(quote):
        issued.append(quote)
        return original_paper_order(quote)

    async def trades(**_kwargs):
        read_started.set()
        raise GatewayReadError("lost authoritative trades")

    async def cancel_asset(token):
        cancelled.append(token)
        return True

    if boundary == "reservation":
        eng._reservation_lock = PausedReservation()
    else:
        eng.gateway._order_bucket.acquire = rate_budget
    eng.gateway._paper_order = paper_order
    eng.gateway.cancel_asset = cancel_asset
    eng.gateway.trades = trades
    quote = asyncio.create_task(eng._recompute(meta.condition_id))
    await asyncio.wait_for(reached.wait(), timeout=1)
    cleanup = None
    if halt == "kill":
        eng.risk.kill()
    else:
        cleanup = asyncio.create_task(eng._reconcile_authoritative_state())
        await asyncio.wait_for(read_started.wait(), timeout=1)
        assert eng._state_unknown
        if boundary == "reservation":
            with pytest.raises(GatewayReadError):
                await asyncio.wait_for(cleanup, timeout=1)
    proceed.set()
    await asyncio.wait_for(quote, timeout=1)
    if cleanup is not None:
        with pytest.raises(GatewayReadError):
            await asyncio.wait_for(cleanup, timeout=1)
        assert set(cancelled) == {meta.yes.token_id, meta.no.token_id}
    assert issued == []
    assert eng.state.orders == {}


@pytest.mark.parametrize("entry", ["startup", "authoritative", "loop"])
async def test_authoritative_task_cancellation_propagates(tmp_path, meta, entry):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway.trades = AsyncMock(side_effect=asyncio.CancelledError)
    eng._reconcile_now.set()
    method = {"startup": eng._startup_reconcile, "authoritative": eng._reconcile_authoritative_state,
              "loop": eng._reconcile_loop}[entry]
    with pytest.raises(asyncio.CancelledError):
        await method()


async def test_unexpected_cancel_exception_keeps_quarantine_and_logs(
    tmp_path, meta, capsys, caplog
):
    eng = _engine_with_market(tmp_path, meta)
    _feed_book(eng, meta)
    await eng._recompute(meta.condition_id)
    eng.gateway.trades = AsyncMock(side_effect=sqlite3.OperationalError("database unavailable"))

    async def cancel_asset(token):
        if token == meta.yes.token_id:
            raise RuntimeError("cancel transport failed")
        return True

    eng.gateway.cancel_asset = cancel_asset
    with pytest.raises(sqlite3.OperationalError):
        await eng._reconcile_authoritative_state()
    assert eng._state_unknown
    assert eng.state.orders_for(meta.yes.token_id)
    assert not eng.state.orders_for(meta.no.token_id)
    captured = capsys.readouterr()
    assert "managed_cancel_failed" in captured.out or any(
        "managed_cancel_failed" in record.getMessage() for record in caplog.records
    )


async def test_repeated_snapshot_failures_do_not_repeat_confirmed_quarantine(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway.trades = AsyncMock(side_effect=GatewayReadError("positions unavailable"))
    eng.gateway.cancel_asset = AsyncMock(return_value=True)

    with pytest.raises(GatewayReadError):
        await eng._reconcile_authoritative_state()
    with pytest.raises(GatewayReadError):
        await eng._reconcile_authoritative_state()

    assert eng._state_unknown
    assert eng._state_unknown_quarantined
    assert eng.gateway.cancel_asset.await_count == 2


async def test_failed_quarantine_is_retried_on_next_snapshot_failure(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway.trades = AsyncMock(side_effect=GatewayReadError("positions unavailable"))
    eng.gateway.cancel_asset = AsyncMock(side_effect=[False, True, True, True])

    with pytest.raises(GatewayReadError):
        await eng._reconcile_authoritative_state()
    assert not eng._state_unknown_quarantined
    with pytest.raises(GatewayReadError):
        await eng._reconcile_authoritative_state()

    assert eng._state_unknown
    assert eng._state_unknown_quarantined
    assert eng.gateway.cancel_asset.await_count == 4


async def test_authoritative_recovery_resets_quarantine_for_future_failure(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway.trades = AsyncMock(side_effect=GatewayReadError("positions unavailable"))
    eng.gateway.cancel_asset = AsyncMock(return_value=True)

    with pytest.raises(GatewayReadError):
        await eng._reconcile_authoritative_state()
    assert eng._state_unknown_quarantined

    eng.gateway.trades = AsyncMock(return_value=[])
    eng.gateway.positions = AsyncMock(return_value={})
    eng.gateway.open_orders = AsyncMock(return_value=[])
    await eng._reconcile_authoritative_state()
    assert not eng._state_unknown
    assert not eng._state_unknown_quarantined

    eng.gateway.trades = AsyncMock(side_effect=GatewayReadError("positions unavailable again"))
    with pytest.raises(GatewayReadError):
        await eng._reconcile_authoritative_state()
    assert eng.gateway.cancel_asset.await_count == 4


@pytest.mark.parametrize("field", ["price", "matched_amount", "timestamp"])
async def test_huge_trade_number_is_a_failed_snapshot(tmp_path, meta, field):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    payload = _confirmed_trade(meta)
    target = payload if field == "timestamp" else payload["maker_orders"][0]
    target[field] = 10 ** 400
    eng.gateway.trades = AsyncMock(return_value=[payload])
    with pytest.raises(GatewayReadError, match="invalid confirmed trade"):
        await eng._sync_confirmed_trades()
    assert eng.state.fill_count() == 0
    assert eng.state.get_sync_value("confirmed_trade_sync_ts") is None


@pytest.mark.parametrize("boundary", ["initial-metadata", "startup-position-drop", "loop-expire", "loop-pnl"])
async def test_store_exception_quarantines_every_authoritative_entry(
    tmp_path, meta, monkeypatch, boundary,
):
    eng = _engine_with_market(tmp_path, meta)
    _feed_book(eng, meta)
    await eng._recompute(meta.condition_id)
    assert eng.state.orders
    eng.gateway.trades = AsyncMock(return_value=[])
    eng.gateway.positions = AsyncMock(return_value={})
    eng.gateway.open_orders = AsyncMock(return_value=[])
    cancelled = []
    backoffs = []

    def broken_store(*_args, **_kwargs):
        eng._running = False
        raise sqlite3.OperationalError("injected database failure")

    async def cancel_asset(token):
        cancelled.append(token)
        return True

    async def stop_on_backoff(delay):
        backoffs.append(delay)
        eng._running = False

    eng.gateway.cancel_asset = cancel_asset
    method = {
        "initial-metadata": "get_sync_value", "startup-position-drop": "drop_untracked_positions",
        "loop-expire": "expire_inflight", "loop-pnl": "record_pnl",
    }[boundary]
    monkeypatch.setattr(eng.state, method, broken_store)
    if boundary.startswith("loop"):
        monkeypatch.setattr("polymaker.engine.asyncio.sleep", stop_on_backoff)
        eng._reconcile_now.set()
        await eng._reconcile_loop()
        assert backoffs
    else:
        entry = eng._startup_reconcile if boundary.startswith("startup") else eng._reconcile_authoritative_state
        with pytest.raises(sqlite3.OperationalError, match="injected database failure"):
            await entry()
    assert eng._state_unknown
    assert set(cancelled) == {meta.yes.token_id, meta.no.token_id}
    assert eng.state.orders == {}


@pytest.mark.parametrize("snapshot", ["positions", "orders"])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), 10 ** 400], ids=["nan", "inf", "huge"])
async def test_nonfinite_authoritative_numbers_cannot_initialize(tmp_path, meta, snapshot, value):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway.trades = AsyncMock(return_value=[])
    eng.gateway.positions = AsyncMock(return_value={})
    eng.gateway.open_orders = AsyncMock(return_value=[])
    if snapshot == "positions":
        eng.gateway.positions.return_value = {meta.yes.token_id: (value, 0.123)}
    else:
        eng.gateway.open_orders.return_value = [
            OpenOrder("broken", meta.yes.token_id, Side.BUY, value, 10, OrderState.LIVE),
        ]
    with pytest.raises(GatewayReadError):
        await eng._reconcile_authoritative_state()
    assert eng._state_unknown
    assert eng.state.get_sync_value("confirmed_trade_sync_initialized") is None


@pytest.mark.parametrize(
    ("ledger_size", "rest_size"),
    [(45.102919, 45.1029), (45.102951, 45.1030), (2.287254, 2.2872)],
)
async def test_rest_position_display_rounding_does_not_quarantine_confirmed_ledger(
    tmp_path, meta, ledger_size, rest_size,
):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    eng.state.set_sync_value("confirmed_trade_sync_initialized", "1")
    for token in (meta.yes.token_id, meta.no.token_id):
        eng.state.set_sync_value(f"confirmed_trade_baseline:{token}", "0")
    payload = _confirmed_trade(meta)
    payload["maker_orders"][0].update(price="0.124", matched_amount=str(ledger_size))
    eng.gateway.trades = AsyncMock(return_value=[payload])
    eng.gateway.positions = AsyncMock(
        return_value={meta.yes.token_id: (rest_size, 0.124)}
    )
    eng.gateway.open_orders = AsyncMock(return_value=[])

    await eng._reconcile_authoritative_state()

    assert not eng._state_unknown
    assert eng.state.fill_position_sizes()[meta.yes.token_id] == pytest.approx(ledger_size)
    assert eng.state.position(meta.yes.token_id).size == pytest.approx(rest_size)
    assert eng.risk.net_cash == pytest.approx(-0.124 * ledger_size)


async def test_rest_position_difference_beyond_display_rounding_stays_quarantined(
    tmp_path, meta,
):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    eng.state.set_sync_value("confirmed_trade_sync_initialized", "1")
    for token in (meta.yes.token_id, meta.no.token_id):
        eng.state.set_sync_value(f"confirmed_trade_baseline:{token}", "0")
    payload = _confirmed_trade(meta)
    payload["maker_orders"][0].update(price="0.124", matched_amount="45.102919")
    eng.gateway.trades = AsyncMock(return_value=[payload])
    eng.gateway.positions = AsyncMock(
        return_value={meta.yes.token_id: (45.1028, 0.124)}
    )
    eng.gateway.open_orders = AsyncMock(return_value=[])

    with pytest.raises(GatewayReadError, match="confirmed trades do not explain positions"):
        await eng._reconcile_authoritative_state()

    assert eng._state_unknown
    eng.gateway.open_orders.assert_not_awaited()


async def test_rest_omitted_positive_dust_does_not_quarantine_confirmed_ledger(
    tmp_path, meta,
):
    eng = _engine_with_market(tmp_path, meta)
    eng.state.set_sync_value("confirmed_trade_sync_initialized", "1")
    for token in (meta.yes.token_id, meta.no.token_id):
        eng.state.set_sync_value(f"confirmed_trade_baseline:{token}", "0")
    eng.state.apply_fill(Fill(
        meta.yes.token_id, Side.BUY, 0.13, 45.102919, "dust-buy", is_maker=True,
    ))
    eng.state.apply_fill(Fill(
        meta.yes.token_id, Side.SELL, 0.13, 45.1, "dust-sell", is_maker=True,
    ))
    eng.gateway.trades = AsyncMock(return_value=[])
    eng.gateway.positions = AsyncMock(return_value={})
    eng.gateway.open_orders = AsyncMock(return_value=[])

    await eng._reconcile_authoritative_state()

    assert not eng._state_unknown
    assert eng.state.fill_position_sizes()[meta.yes.token_id] == pytest.approx(0.002919)
    eng.gateway.open_orders.assert_awaited_once()
