"""Durable on-chain exposure must survive lagging authoritative REST snapshots."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from unittest.mock import AsyncMock

import pytest

from polymaker.domain import OpenOrder, OrderState, Side
from polymaker.execution.gateway import GatewayReadError
from tests.test_engine import _engine_with_market, _feed_book
from tests.test_hardening2 import _confirmed_trade
from tests.test_userstream_parse import FUNDER

PROOF_KEY = "confirmed_trade_requires_proof"


@pytest.mark.parametrize("initialized", [False, True])
@pytest.mark.parametrize("restart", [False, True])
async def test_lagging_rest_preserves_onchain_exposure_until_confirmation(
    tmp_path, meta, initialized, restart,
):
    eng = _engine_with_market(tmp_path, meta)
    _feed_book(eng, meta)
    if initialized:
        eng.state.set_sync_value("confirmed_trade_sync_initialized", "1")
    await eng._recompute(meta.condition_id)
    assert eng.state.orders
    eng.gateway.token_balances = AsyncMock(return_value={meta.yes.token_id: 50.0})
    await eng._check_position_divergence()
    proof = eng.state.get_sync_value(PROOF_KEY)
    if restart:
        eng.state.close()
        eng.catalog.close()
        eng.gateway.close()
        eng = _engine_with_market(tmp_path, meta)
        _feed_book(eng, meta)
    eng.gateway._funder = FUNDER
    eng.gateway.trades = AsyncMock(return_value=[])
    eng.gateway.positions = AsyncMock(return_value={})
    eng.gateway.open_orders = AsyncMock(return_value=[])

    with pytest.raises(GatewayReadError, match="on-chain proof"):
        await eng._reconcile_authoritative_state(startup=restart)
    assert eng.state.position(meta.yes.token_id).size == 50
    assert eng._state_unknown
    assert eng.state.get_sync_value(PROOF_KEY) == proof
    assert eng.state.fill_count() == 0
    assert eng.risk.net_cash == 0
    if not initialized:
        assert eng.state.get_sync_value("confirmed_trade_sync_initialized") is None
        assert eng.state.get_sync_value(f"confirmed_trade_baseline:{meta.yes.token_id}") is None
    await eng._recompute(meta.condition_id)
    assert eng.state.orders == {}

    eng.gateway.trades = AsyncMock(return_value=[_confirmed_trade(meta)])
    eng.gateway.positions = AsyncMock(return_value={meta.yes.token_id: (50, 0.123)})
    await eng._reconcile_authoritative_state()
    assert not eng._state_unknown
    assert eng.state.get_sync_value(PROOF_KEY) == "0"
    assert eng.state.position(meta.yes.token_id).size == 50
    assert eng.state.fill_count() == 1
    assert eng.risk.net_cash == pytest.approx(-6.15)


@pytest.mark.parametrize("initialized", [False, True])
@pytest.mark.parametrize("raw", [
    "1", "", "{", "null", "[]", "{}", '{"yes-token": -1}', '{"yes-token": NaN}',
    '{"yes-token": Infinity}', '{"yes-token": 1e400}', '{"yes-token": true}',
    '{"yes-token": "50"}', '{"manual-token": 50}', '{"yes-token": 50, "yes-token": 0}',
    '{"yes-token": ' + "9" * 400 + '}',
], ids=["legacy-missing", "empty", "invalid-json", "null", "list", "empty-map", "negative",
        "nan", "inf", "float-overflow", "boolean", "string", "unconfigured", "duplicate", "huge-int"])
async def test_missing_or_malformed_required_proof_cannot_resume(tmp_path, meta, initialized, raw):
    eng = _engine_with_market(tmp_path, meta)
    if initialized:
        eng.state.set_sync_value("confirmed_trade_sync_initialized", "1")
    eng.state.set_position(meta.yes.token_id, 50, 0.123)
    eng.state.set_sync_value(PROOF_KEY, raw)
    eng.gateway.trades = AsyncMock(return_value=[])
    eng.gateway.positions = AsyncMock(return_value={})
    eng.gateway.open_orders = AsyncMock(return_value=[])
    with pytest.raises(GatewayReadError, match="on-chain proof"):
        await eng._reconcile_authoritative_state(startup=True)
    assert eng._state_unknown
    assert eng.state.position(meta.yes.token_id).size == 50
    assert eng.state.get_sync_value(PROOF_KEY) == raw
    assert eng.state.get_sync_value("confirmed_trade_sync_ts") is None
    if not initialized:
        assert eng.state.get_sync_value("confirmed_trade_sync_initialized") is None
        assert eng.state.get_sync_value(f"confirmed_trade_baseline:{meta.yes.token_id}") is None


async def test_partial_later_onchain_read_updates_size_without_dropping_other_proof(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway._funder = FUNDER
    eng.gateway.token_balances = AsyncMock(return_value={meta.yes.token_id: 50.0, meta.no.token_id: 20.0})
    await eng._check_position_divergence()
    eng.gateway.token_balances = AsyncMock(return_value={meta.yes.token_id: 49.5})
    await eng._check_position_divergence()
    assert eng.state.position(meta.yes.token_id).size == 49.5
    assert json.loads(eng.state.get_sync_value(PROOF_KEY)) == {
        meta.yes.token_id: 49.5, meta.no.token_id: 20,
    }
    yes_fill = _confirmed_trade(meta)
    yes_fill["maker_orders"][0]["matched_amount"] = "49.5"
    no_fill = _confirmed_trade(meta)
    no_fill["id"] = "no-token-fill"
    no_fill["maker_orders"][0].update(asset_id=meta.no.token_id, matched_amount="20", price="0.4")
    eng.gateway.trades = AsyncMock(return_value=[yes_fill, no_fill])
    eng.gateway.positions = AsyncMock(return_value={meta.yes.token_id: (49.5, 0.123)})
    eng.gateway.open_orders = AsyncMock(return_value=[])
    with pytest.raises(GatewayReadError, match="on-chain proof"):
        await eng._reconcile_authoritative_state()
    assert eng.state.position(meta.yes.token_id).size == 49.5
    assert eng.state.position(meta.no.token_id).size == 20
    eng.gateway.positions.return_value[meta.no.token_id] = (20, 0.4)
    await eng._reconcile_authoritative_state()
    assert not eng._state_unknown
    assert eng.state.get_sync_value(PROOF_KEY) == "0"
    assert eng.risk.net_cash == pytest.approx(-14.0885)


@pytest.mark.parametrize("size", [-1, float("nan"), float("inf"), 10 ** 400, True, "50"],
                         ids=["negative", "nan", "inf", "huge", "boolean", "string"])
async def test_invalid_onchain_size_cannot_publish_partial_proof(tmp_path, meta, size):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway.token_balances = AsyncMock(return_value={meta.yes.token_id: 50.0, meta.no.token_id: size})
    with pytest.raises(GatewayReadError, match="on-chain"):
        await eng._check_position_divergence()
    assert eng._state_unknown
    assert eng.state.get_sync_value(PROOF_KEY) is None
    assert eng.state.position(meta.yes.token_id).size == 0
    assert eng.state.position(meta.no.token_id).size == 0


async def test_complete_proof_precedes_position_write_and_survives_restart(tmp_path, meta, monkeypatch):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway.token_balances = AsyncMock(return_value={meta.yes.token_id: 50.0, meta.no.token_id: 20.0})

    def crash_position_write(*_args, **_kwargs):
        raise sqlite3.OperationalError("position write interrupted")

    monkeypatch.setattr(eng.state, "force_set_position", crash_position_write)
    with pytest.raises(sqlite3.OperationalError, match="position write interrupted"):
        await eng._check_position_divergence()
    assert eng._state_unknown
    eng.state.close()
    eng.catalog.close()
    eng.gateway.close()
    restored = _engine_with_market(tmp_path, meta)
    restored.gateway.trades = AsyncMock(return_value=[])
    restored.gateway.positions = AsyncMock(return_value={})
    restored.gateway.open_orders = AsyncMock(return_value=[])
    with pytest.raises(GatewayReadError, match="on-chain proof"):
        await restored._reconcile_authoritative_state(startup=True)
    assert restored.state.position(meta.yes.token_id).size == 50
    assert restored.state.position(meta.no.token_id).size == 20
    assert restored.state.get_sync_value("confirmed_trade_sync_initialized") is None


async def test_zero_onchain_proof_is_retained_against_stale_positive_rest(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.state.set_position(meta.yes.token_id, 50, 0.123)
    eng.gateway.token_balances = AsyncMock(return_value={meta.yes.token_id: 0.0})
    await eng._check_position_divergence()
    proof = eng.state.get_sync_value(PROOF_KEY)
    eng.gateway.trades = AsyncMock(return_value=[])
    eng.gateway.positions = AsyncMock(return_value={meta.yes.token_id: (50, 0.123)})
    eng.gateway.open_orders = AsyncMock(return_value=[])
    with pytest.raises(GatewayReadError):
        await eng._reconcile_authoritative_state()
    assert eng._state_unknown
    assert eng.state.position(meta.yes.token_id).size == 0
    assert eng.state.get_sync_value(PROOF_KEY) == proof
    eng.gateway.positions = AsyncMock(return_value={})
    await eng._reconcile_authoritative_state()
    assert not eng._state_unknown
    assert eng.state.position(meta.yes.token_id).size == 0
    assert eng.state.get_sync_value(PROOF_KEY) == "0"


async def test_newer_proof_is_not_overwritten_while_older_positions_read_waits(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    eng.gateway.token_balances = AsyncMock(return_value={meta.yes.token_id: 50.0})
    await eng._check_position_divergence()
    reached = asyncio.Event()
    proceed = asyncio.Event()

    async def positions():
        reached.set()
        await proceed.wait()
        return {}

    eng.gateway.trades = AsyncMock(return_value=[])
    eng.gateway.positions = positions
    eng.gateway.open_orders = AsyncMock(return_value=[])
    older = asyncio.create_task(eng._reconcile_authoritative_state())
    await asyncio.wait_for(reached.wait(), timeout=1)
    eng.gateway.token_balances = AsyncMock(return_value={meta.yes.token_id: 49.5, meta.no.token_id: 20})
    await eng._check_position_divergence()
    proceed.set()
    with pytest.raises(GatewayReadError, match="changed"):
        await asyncio.wait_for(older, timeout=1)
    assert eng._state_unknown
    assert eng.state.position(meta.yes.token_id).size == 49.5
    assert eng.state.position(meta.no.token_id).size == 20
    assert json.loads(eng.state.get_sync_value(PROOF_KEY)) == {
        meta.yes.token_id: 49.5, meta.no.token_id: 20,
    }


async def test_overlapping_onchain_reads_merge_against_latest_durable_proof(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    reached = asyncio.Event()
    proceed = asyncio.Event()

    async def first_balances(_tokens):
        reached.set()
        await proceed.wait()
        return {meta.yes.token_id: 50.0}

    eng.gateway.token_balances = first_balances
    first = asyncio.create_task(eng._check_position_divergence())
    await asyncio.wait_for(reached.wait(), timeout=1)
    eng.gateway.token_balances = AsyncMock(return_value={meta.no.token_id: 20})
    await eng._check_position_divergence()
    proceed.set()
    await asyncio.wait_for(first, timeout=1)
    assert eng._state_unknown
    assert eng.state.position(meta.yes.token_id).size == 50
    assert eng.state.position(meta.no.token_id).size == 20
    assert json.loads(eng.state.get_sync_value(PROOF_KEY)) == {
        meta.yes.token_id: 50, meta.no.token_id: 20,
    }
    assert eng.state.fill_count() == 0
    assert eng.risk.net_cash == 0


async def test_orders_failure_retains_proof_and_recovery_preserves_manual_kill(tmp_path, meta):
    eng = _engine_with_market(tmp_path, meta)
    _feed_book(eng, meta)
    eng.gateway._funder = FUNDER
    manual = OpenOrder("manual", "manual-token", Side.BUY, 0.2, 10, OrderState.LIVE)
    exchange_orders = {manual.order_id: manual}
    cancelled = []

    async def cancel_asset(token):
        cancelled.append(token)
        for oid, order in list(exchange_orders.items()):
            if order.token_id == token:
                del exchange_orders[oid]
        return True

    eng.gateway.cancel_asset = cancel_asset
    eng.gateway.token_balances = AsyncMock(return_value={meta.yes.token_id: 50.0})
    await eng._check_position_divergence()
    eng.risk.kill()
    proof = eng.state.get_sync_value(PROOF_KEY)
    eng.gateway.trades = AsyncMock(return_value=[_confirmed_trade(meta)])
    eng.gateway.positions = AsyncMock(return_value={})
    with pytest.raises(GatewayReadError, match="on-chain proof"):
        await eng._reconcile_authoritative_state()
    eng.gateway.positions = AsyncMock(return_value={meta.yes.token_id: (50, 0.123)})
    eng.gateway.open_orders = AsyncMock(side_effect=GatewayReadError("orders unavailable"))
    with pytest.raises(GatewayReadError, match="orders unavailable"):
        await eng._reconcile_authoritative_state()
    assert eng._state_unknown
    assert eng.state.get_sync_value(PROOF_KEY) == proof
    assert eng.state.position(meta.yes.token_id).size == 50
    assert eng.state.get_sync_value("confirmed_trade_sync_initialized") is None
    assert eng.state.fill_count() == 1
    assert eng.risk.net_cash == pytest.approx(-6.15)

    eng.gateway.open_orders = AsyncMock(return_value=list(exchange_orders.values()))
    await eng._reconcile_authoritative_state()
    assert not eng._state_unknown
    assert eng.state.get_sync_value(PROOF_KEY) == "0"
    assert eng.risk.global_halt() == (True, "manual_kill")
    assert exchange_orders == {"manual": manual}
    assert set(cancelled) == {meta.yes.token_id, meta.no.token_id}
    await eng._recompute(meta.condition_id)
    assert eng.state.orders == {}
