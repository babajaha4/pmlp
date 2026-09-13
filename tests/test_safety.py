"""Regression tests for live-mode safety gates and fail-closed state handling."""

from __future__ import annotations

import dataclasses

import pytest
from typer.testing import CliRunner

from polymaker.cli import app
from polymaker.config import Config, PathsConfig, RiskConfig
from polymaker.domain import Fill, OpenOrder, OrderState, Quote, Side, TokenMeta
from polymaker.engine import Engine
from polymaker.execution.gateway import ExecutionGateway, GatewayReadError
from polymaker.merge import Merger
from polymaker.risk.manager import RiskManager
from polymaker.state.store import StateStore


def test_run_live_requires_explicit_confirmation() -> None:
    result = CliRunner().invoke(app, ["run", "--live"])
    assert result.exit_code == 2
    assert "--confirm-live" in result.stdout


def test_run_defaults_to_paper(monkeypatch) -> None:
    modes: list[bool] = []

    class FakeEngine:
        def __init__(self, _cfg, *, paper: bool = False) -> None:
            modes.append(paper)

        async def run_forever(self) -> None:
            return None

        async def shutdown(self) -> None:
            return None

    monkeypatch.setattr("polymaker.engine.Engine", FakeEngine)
    result = CliRunner().invoke(app, ["run"])
    assert result.exit_code == 0
    assert modes == [True]
    assert "PAPER" in result.stdout


def test_live_self_tests_require_explicit_confirmation() -> None:
    runner = CliRunner()
    assert runner.invoke(app, ["livetest"]).exit_code == 2
    assert runner.invoke(app, ["moneydoctor"]).exit_code == 2


@pytest.mark.asyncio
async def test_open_orders_snapshot_failure_is_not_empty(monkeypatch) -> None:
    gw = ExecutionGateway(Config(), paper=False)
    gw._client = object()

    async def fail(*_args, **_kwargs):
        raise OSError("network down")

    monkeypatch.setattr(gw, "_io", fail)
    with pytest.raises(GatewayReadError):
        await gw.open_orders()
    gw.close()


@pytest.mark.asyncio
async def test_positions_snapshot_failure_is_not_empty(monkeypatch) -> None:
    gw = ExecutionGateway(Config(), paper=False)
    gw._funder = "0x0000000000000000000000000000000000000001"

    async def fail(*_args, **_kwargs):
        raise OSError("network down")

    monkeypatch.setattr("httpx.AsyncClient.get", fail)
    with pytest.raises(GatewayReadError):
        await gw.positions()
    gw.close()


def test_risk_state_survives_restart_and_kill(tmp_path, meta) -> None:
    path = tmp_path / "state.db"
    store = StateStore(path)
    rm = RiskManager(RiskConfig(), store)
    fill = Fill(meta.yes.token_id, Side.BUY, 0.5, 10, "risk-fill")
    store.apply_fill(fill)
    rm.note_fill(fill)
    rm.note_order_result(False)
    rm.kill()
    store.close()

    store2 = StateStore(path)
    restored = RiskManager(RiskConfig(), store2)
    assert restored.net_cash == pytest.approx(-5.0)
    assert restored.error_rate == 0.0  # one attempt is below the breaker threshold
    assert restored.global_halt() == (True, "manual_kill")
    store2.close()


def test_daily_loss_kill_is_persisted(tmp_path, meta) -> None:
    path = tmp_path / "state.db"
    store = StateStore(path)
    rm = RiskManager(RiskConfig(daily_loss_kill_usdc=1), store)
    fill = Fill(meta.yes.token_id, Side.BUY, 0.5, 10, "daily-fill")
    store.apply_fill(fill)
    rm.note_fill(fill)
    rm.reset_day()
    rm.update_mark(meta.yes.token_id, 0.2)
    assert rm.global_halt()[0] is True
    store.close()

    store2 = StateStore(path)
    restored = RiskManager(RiskConfig(daily_loss_kill_usdc=1), store2)
    assert restored.daily_pnl == pytest.approx(-3.0)
    assert restored.global_halt() == (True, "manual_kill")
    store2.close()


def test_kill_does_not_clear_at_utc_day_boundary(tmp_path, monkeypatch) -> None:
    path = tmp_path / "state.db"
    monkeypatch.setattr("polymaker.risk.manager._day_key", lambda: "2026-09-12")
    store = StateStore(path)
    rm = RiskManager(RiskConfig(), store)
    rm.kill()
    store.close()

    monkeypatch.setattr("polymaker.risk.manager._day_key", lambda: "2026-09-13")
    store2 = StateStore(path)
    restored = RiskManager(RiskConfig(), store2)
    assert restored.global_halt() == (True, "manual_kill")
    store2.close()


def test_daily_loss_kill_clears_on_new_utc_day(tmp_path, monkeypatch, meta) -> None:
    path = tmp_path / "state.db"
    monkeypatch.setattr("polymaker.risk.manager._day_key", lambda: "2026-09-12")
    store = StateStore(path)
    rm = RiskManager(RiskConfig(daily_loss_kill_usdc=1), store)
    fill = Fill(meta.yes.token_id, Side.BUY, 0.5, 10, "day-boundary")
    store.apply_fill(fill)
    rm.note_fill(fill)
    rm.reset_day()
    rm.update_mark(meta.yes.token_id, 0.2)
    assert rm.global_halt()[0]
    store.close()

    monkeypatch.setattr("polymaker.risk.manager._day_key", lambda: "2026-09-13")
    store2 = StateStore(path)
    restored = RiskManager(RiskConfig(daily_loss_kill_usdc=1), store2)
    restored.establish_daily_baseline()
    assert restored.global_halt()[0] is False
    store2.close()


def test_resting_buy_orders_are_reserved(tmp_path, meta) -> None:
    store = StateStore(tmp_path / "state.db")
    rm = RiskManager(RiskConfig(max_market_notional_usdc=10, max_total_exposure_usdc=10), store)
    store.upsert_order(
        OpenOrder("resting", meta.yes.token_id, Side.BUY, 0.5, 18, OrderState.LIVE)
    )
    assert not rm.can_reserve(meta, [Quote(meta.yes.token_id, Side.BUY, 0.5, 4)])
    store.close()


def test_buy_batch_is_scaled_to_remaining_headroom(tmp_path, meta) -> None:
    store = StateStore(tmp_path / "state.db")
    rm = RiskManager(RiskConfig(
        max_market_notional_usdc=10,
        max_total_exposure_usdc=10,
        max_event_group_loss_usdc=10,
    ), store)
    store.upsert_order(
        OpenOrder("resting", meta.yes.token_id, Side.BUY, 0.5, 14, OrderState.LIVE)
    )
    fitted = rm.fit_reservation(meta, [Quote(meta.no.token_id, Side.BUY, 0.5, 10)])
    assert len(fitted) == 1
    assert fitted[0].size == pytest.approx(6.0)
    assert rm.can_reserve(meta, fitted)
    store.close()


def test_global_reservation_counts_orders_without_positions(tmp_path, meta) -> None:
    store = StateStore(tmp_path / "state.db")
    rm = RiskManager(RiskConfig(
        max_market_notional_usdc=100,
        max_total_exposure_usdc=10,
        max_event_group_loss_usdc=100,
    ), store)
    store.upsert_order(
        OpenOrder("first-market", meta.yes.token_id, Side.BUY, 0.5, 18, OrderState.LIVE)
    )
    second = dataclasses.replace(
        meta,
        condition_id="second-condition",
        tokens=(TokenMeta("second-yes", "Yes"), TokenMeta("second-no", "No")),
        event_id="second-event",
        min_order_size=1,
    )
    quote = Quote(second.yes.token_id, Side.BUY, 0.5, 4)
    assert not rm.can_reserve(second, [quote])
    fitted = rm.fit_reservation(second, [quote])
    assert fitted[0].size == pytest.approx(2.0)
    store.close()


def test_merge_is_disabled_by_default() -> None:
    assert Merger(Config()).can_merge is False


def test_paper_state_is_isolated_from_live_state(tmp_path) -> None:
    live_path = tmp_path / "state.db"
    live = StateStore(live_path)
    live.set_position("live-token", 10, 0.4)
    live.close()
    cfg = Config(paths=PathsConfig(
        db=str(live_path),
        journal_dir=str(tmp_path / "journal"),
        log_dir=str(tmp_path / "logs"),
    ))
    eng = Engine(cfg, paper=True)
    assert eng.state.position("live-token").size == 0
    eng.gateway.close()
    eng.journal.close()
    eng.state.close()
    eng.catalog.close()

    restored = StateStore(live_path)
    assert restored.position("live-token").size == 10
    restored.close()


@pytest.mark.asyncio
async def test_engine_cancels_only_managed_tokens(tmp_path, meta) -> None:
    cfg = Config(
        paths=PathsConfig(
            db=str(tmp_path / "state.db"),
            journal_dir=str(tmp_path / "journal"),
            log_dir=str(tmp_path / "logs"),
        )
    )
    eng = Engine(cfg, paper=True)
    eng.metas[meta.condition_id] = meta
    seen: list[str] = []

    async def cancel(token: str) -> bool:
        seen.append(token)
        return True

    eng.gateway.cancel_asset = cancel  # type: ignore[method-assign]
    assert await eng._cancel_managed_assets() is True
    assert set(seen) == {meta.yes.token_id, meta.no.token_id}
    eng.state.close()
    eng.catalog.close()


@pytest.mark.asyncio
async def test_startup_refuses_to_quote_when_managed_cancel_fails(tmp_path, meta) -> None:
    cfg = Config(
        paths=PathsConfig(
            db=str(tmp_path / "state.db"),
            journal_dir=str(tmp_path / "journal"),
            log_dir=str(tmp_path / "logs"),
        )
    )
    eng = Engine(cfg, paper=True)
    eng.metas[meta.condition_id] = meta

    async def fail(_token: str) -> bool:
        return False

    eng.gateway.cancel_asset = fail  # type: ignore[method-assign]
    with pytest.raises(GatewayReadError):
        await eng._startup_reconcile()
    assert eng._state_unknown is True
    eng.state.close()
    eng.catalog.close()
