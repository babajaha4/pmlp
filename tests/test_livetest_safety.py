"""Safety boundaries for the one-order live wallet test."""

from __future__ import annotations

import asyncio
import dataclasses
import io
from types import SimpleNamespace

import pytest
from rich.console import Console
from typer.testing import CliRunner

from polymaker.catalog.store import CatalogStore
from polymaker.cli import app
from polymaker.config import Config, ExecutionConfig, MarketEntry, PathsConfig, Secrets
from polymaker.domain import OpenOrder, OrderState, Side
from polymaker.execution.gateway import GatewayReadError


def _cfg(tmp_path, meta, *, slug: str | None = None) -> Config:
    db = tmp_path / "state.db"
    store = CatalogStore(db)
    store.upsert_market(meta)
    store.close()
    return Config(
        paths=PathsConfig(
            db=str(db),
            journal_dir=str(tmp_path / "journal"),
            log_dir=str(tmp_path / "logs"),
        ),
        markets=[MarketEntry(slug=slug or meta.slug)],
        secrets=Secrets(PK="test-private-key", BROWSER_ADDRESS="0x" + "1" * 40),
    )


def _console() -> Console:
    return Console(file=io.StringIO(), force_terminal=False)


def test_livetest_cli_requires_explicit_market(monkeypatch) -> None:
    seen: list[tuple[str, float]] = []

    async def fake_run(_cfg, _console, notional_usdc: float, *, market_slug: str) -> bool:
        seen.append((market_slug, notional_usdc))
        return True

    monkeypatch.setattr("polymaker.livetest.run_livetest", fake_run)
    monkeypatch.setattr("polymaker.cli.Config.load", lambda _path: Config())

    result = CliRunner().invoke(
        app,
        ["livetest", "--confirm-live", "--market", "will-x-happen", "--notional", "5"],
    )

    assert result.exit_code == 0
    assert seen == [("will-x-happen", 5.0)]


@pytest.mark.asyncio
async def test_livetest_rejects_unconfigured_market_before_gateway(
    tmp_path, meta, monkeypatch
) -> None:
    from polymaker import livetest

    cfg = _cfg(tmp_path, meta)

    class ForbiddenGateway:
        def __init__(self, _cfg) -> None:
            raise AssertionError("gateway must not be created for an unconfigured market")

    monkeypatch.setattr(livetest, "ExecutionGateway", ForbiddenGateway)

    ok = await livetest.run_livetest(
        cfg,
        _console(),
        5.0,
        market_slug="not-configured",
    )

    assert ok is False


@pytest.mark.asyncio
async def test_livetest_rejects_order_above_requested_notional(
    tmp_path, meta, monkeypatch
) -> None:
    from polymaker import livetest

    expensive_minimum = dataclasses.replace(meta, min_order_size=100.0)
    cfg = _cfg(tmp_path, expensive_minimum)
    placed: list[object] = []
    closed: list[bool] = []

    class FakeGateway:
        def __init__(self, _cfg) -> None:
            pass

        async def connect(self) -> None:
            pass

        async def balance_allowance(self) -> dict[str, str]:
            return {"balance": "100000000"}

        async def get_book(self, _token: str) -> dict[str, float]:
            return {"best_bid": 0.20, "best_ask": 0.21, "bid_depth": 100, "ask_depth": 100}

        async def open_orders(self) -> list[OpenOrder]:
            return []

        async def place(self, quotes, _meta) -> list[OpenOrder]:
            placed.extend(quotes)
            return []

        def close(self) -> None:
            closed.append(True)

    monkeypatch.setattr(livetest, "ExecutionGateway", FakeGateway)

    ok = await livetest.run_livetest(
        cfg,
        _console(),
        5.0,
        market_slug=meta.slug,
    )

    assert ok is False
    assert placed == []
    assert closed == [True]


@pytest.mark.asyncio
async def test_livetest_requires_post_only_execution(tmp_path, meta, monkeypatch) -> None:
    from polymaker import livetest

    cfg = _cfg(tmp_path, meta)
    cfg.execution = ExecutionConfig(post_only=False)

    class ForbiddenGateway:
        def __init__(self, _cfg) -> None:
            raise AssertionError("gateway must not be created when post-only is disabled")

    monkeypatch.setattr(livetest, "ExecutionGateway", ForbiddenGateway)

    ok = await livetest.run_livetest(
        cfg,
        _console(),
        5.0,
        market_slug=meta.slug,
    )

    assert ok is False


def test_livetest_rejects_market_without_legal_deep_price(meta) -> None:
    from polymaker.livetest import _test_quote

    assert _test_quote(meta, best_bid=0.01, max_notional=5.0) is None


@pytest.mark.asyncio
async def test_order_readback_retries_until_single_order_is_live(meta, monkeypatch) -> None:
    from polymaker.livetest import _observe_resting_order

    order = OpenOrder("test-order", meta.yes.token_id, Side.BUY, 0.39, 10, OrderState.LIVE)
    sleeps: list[float] = []

    class FakeGateway:
        def __init__(self) -> None:
            self.reads = 0

        async def get_order(self, order_id: str) -> OpenOrder | None:
            assert order_id == "test-order"
            self.reads += 1
            return None if self.reads == 1 else order

        async def open_orders(self) -> list[OpenOrder]:
            return []

    async def record_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr("polymaker.livetest.asyncio.sleep", record_sleep)
    gateway = FakeGateway()

    observed = await _observe_resting_order(gateway, "test-order", _console())

    assert observed is True
    assert gateway.reads == 2
    assert sleeps == [0.5, 1.0]


@pytest.mark.asyncio
async def test_gateway_get_order_parses_authoritative_partial_fill() -> None:
    from polymaker.execution.gateway import ExecutionGateway

    raw = {
        "id": "test-order",
        "status": "LIVE",
        "owner": "owner-id",
        "maker_address": "0x" + "1" * 40,
        "market": "0x" + "2" * 64,
        "asset_id": "yes-token",
        "side": "BUY",
        "original_size": "100",
        "size_matched": "25",
        "price": "0.4",
        "outcome": "YES",
        "expiration": "0",
        "order_type": "GTC",
        "associate_trades": ["trade-1"],
        "created_at": 1_700_000_000,
    }
    gateway = ExecutionGateway(Config())
    gateway._client = SimpleNamespace(get_order=lambda _order_id: raw)

    order = await gateway.get_order("test-order")

    assert order == OpenOrder(
        "test-order",
        "yes-token",
        Side.BUY,
        0.4,
        75.0,
        OrderState.PARTIALLY_FILLED,
        created_ts=1_700_000_000,
    )


@pytest.mark.asyncio
async def test_order_readback_stops_on_authoritative_terminal_state(meta, monkeypatch) -> None:
    from polymaker.livetest import _observe_resting_order

    order = OpenOrder(
        "test-order", meta.yes.token_id, Side.BUY, 0.39, 10, OrderState.CANCELED
    )
    sleeps: list[float] = []

    class FakeGateway:
        async def get_order(self, _order_id: str) -> OpenOrder:
            return order

        async def open_orders(self) -> list[OpenOrder]:
            raise AssertionError("terminal single-order state is authoritative")

    async def record_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr("polymaker.livetest.asyncio.sleep", record_sleep)

    observed = await _observe_resting_order(FakeGateway(), "test-order", _console())

    assert observed is False
    assert sleeps == [0.5]


@pytest.mark.asyncio
async def test_gateway_get_order_fails_closed_on_unknown_status() -> None:
    from polymaker.execution.gateway import ExecutionGateway

    gateway = ExecutionGateway(Config())
    gateway._client = SimpleNamespace(
        get_order=lambda _order_id: {
            "id": "test-order",
            "status": "NEW_UNDOCUMENTED_STATE",
            "asset_id": "yes-token",
            "side": "BUY",
            "original_size": "100",
            "size_matched": "0",
            "price": "0.4",
        }
    )

    with pytest.raises(GatewayReadError, match="snapshot unavailable"):
        await gateway.get_order("test-order")


@pytest.mark.asyncio
async def test_livetest_waits_for_cancelled_placement_before_cleanup(
    tmp_path, meta, monkeypatch
) -> None:
    from polymaker import livetest

    cfg = _cfg(tmp_path, meta)
    order = OpenOrder("late-order", meta.yes.token_id, Side.BUY, 0.39, 10, OrderState.LIVE)
    placement_started = asyncio.Event()
    release_placement = asyncio.Event()
    gateways: list[object] = []

    class FakeGateway:
        def __init__(self, _cfg) -> None:
            self.live: list[OpenOrder] = []
            self.cancelled: list[list[str]] = []
            self.closed = False
            gateways.append(self)

        async def connect(self) -> None:
            pass

        async def balance_allowance(self) -> dict[str, str]:
            return {"balance": "100000000"}

        async def get_book(self, _token: str) -> dict[str, float]:
            return {"best_bid": 0.50, "best_ask": 0.51, "bid_depth": 100, "ask_depth": 100}

        async def open_orders(self) -> list[OpenOrder]:
            return list(self.live)

        async def place(self, _quotes, _meta) -> list[OpenOrder]:
            placement_started.set()
            await release_placement.wait()
            self.live = [order]
            return [order]

        async def cancel(self, order_ids: list[str]) -> bool:
            self.cancelled.append(order_ids)
            self.live = []
            return True

        async def cancel_asset(self, _token: str) -> bool:
            self.live = []
            return True

        def close(self) -> None:
            self.closed = True

    async def no_sleep(_seconds: float) -> None:
        pass

    monkeypatch.setattr(livetest, "ExecutionGateway", FakeGateway)
    monkeypatch.setattr(livetest.asyncio, "sleep", no_sleep)

    task = asyncio.create_task(
        livetest.run_livetest(cfg, _console(), 5.0, market_slug=meta.slug)
    )
    await placement_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    release_placement.set()

    with pytest.raises(asyncio.CancelledError):
        await task

    gw = gateways[0]
    assert gw.live == []
    assert gw.cancelled == [["late-order"]]
    assert gw.closed is True


@pytest.mark.asyncio
async def test_livetest_cleans_up_when_readback_raises(tmp_path, meta, monkeypatch) -> None:
    from polymaker import livetest

    cfg = _cfg(tmp_path, meta)
    order = OpenOrder("test-order", meta.yes.token_id, Side.BUY, 0.39, 10, OrderState.LIVE)
    cancelled: list[list[str]] = []
    closed: list[bool] = []

    class FakeGateway:
        def __init__(self, _cfg) -> None:
            self.snapshots = 0

        async def connect(self) -> None:
            pass

        async def balance_allowance(self) -> dict[str, str]:
            return {"balance": "100000000"}

        async def get_book(self, _token: str) -> dict[str, float]:
            return {"best_bid": 0.50, "best_ask": 0.51, "bid_depth": 100, "ask_depth": 100}

        async def open_orders(self) -> list[OpenOrder]:
            self.snapshots += 1
            if self.snapshots == 1:
                return []
            if self.snapshots == 2:
                raise GatewayReadError("readback unavailable")
            return []

        async def place(self, _quotes, _meta) -> list[OpenOrder]:
            return [order]

        async def cancel(self, order_ids: list[str]) -> bool:
            cancelled.append(order_ids)
            return True

        async def cancel_asset(self, _token: str) -> bool:
            raise AssertionError("asset fallback is not needed when id cancellation confirms")

        def close(self) -> None:
            closed.append(True)

    async def no_sleep(_seconds: float) -> None:
        pass

    monkeypatch.setattr(livetest, "ExecutionGateway", FakeGateway)
    monkeypatch.setattr(livetest.asyncio, "sleep", no_sleep)

    ok = await livetest.run_livetest(
        cfg,
        _console(),
        5.0,
        market_slug=meta.slug,
    )

    assert ok is False
    assert cancelled == [["test-order"]]
    assert closed == [True]


@pytest.mark.asyncio
async def test_livetest_falls_back_to_configured_asset_after_cancel_failure(
    tmp_path, meta, monkeypatch
) -> None:
    from polymaker import livetest

    cfg = _cfg(tmp_path, meta)
    order = OpenOrder("test-order", meta.yes.token_id, Side.BUY, 0.39, 10, OrderState.LIVE)
    asset_cancels: list[str] = []

    class FakeGateway:
        def __init__(self, _cfg) -> None:
            self.snapshots = 0

        async def connect(self) -> None:
            pass

        async def balance_allowance(self) -> dict[str, str]:
            return {"balance": "100000000"}

        async def get_book(self, _token: str) -> dict[str, float]:
            return {"best_bid": 0.50, "best_ask": 0.51, "bid_depth": 100, "ask_depth": 100}

        async def open_orders(self) -> list[OpenOrder]:
            self.snapshots += 1
            if self.snapshots == 1:
                return []
            if self.snapshots <= 4:
                return [order]
            return []

        async def place(self, _quotes, _meta) -> list[OpenOrder]:
            return [order]

        async def cancel(self, _order_ids: list[str]) -> bool:
            return False

        async def cancel_asset(self, token: str) -> bool:
            asset_cancels.append(token)
            return True

        def close(self) -> None:
            pass

    async def no_sleep(_seconds: float) -> None:
        pass

    monkeypatch.setattr(livetest, "ExecutionGateway", FakeGateway)
    monkeypatch.setattr(livetest.asyncio, "sleep", no_sleep)

    ok = await livetest.run_livetest(
        cfg,
        _console(),
        5.0,
        market_slug=meta.slug,
    )

    assert ok is True
    assert asset_cancels == [meta.yes.token_id]


@pytest.mark.asyncio
async def test_livetest_closes_gateway_when_cleanup_read_fails(
    tmp_path, meta, monkeypatch
) -> None:
    from polymaker import livetest

    cfg = _cfg(tmp_path, meta)
    order = OpenOrder("test-order", meta.yes.token_id, Side.BUY, 0.39, 10, OrderState.LIVE)
    closed: list[bool] = []

    class FakeGateway:
        def __init__(self, _cfg) -> None:
            self.snapshots = 0

        async def connect(self) -> None:
            pass

        async def balance_allowance(self) -> dict[str, str]:
            return {"balance": "100000000"}

        async def get_book(self, _token: str) -> dict[str, float]:
            return {"best_bid": 0.50, "best_ask": 0.51, "bid_depth": 100, "ask_depth": 100}

        async def open_orders(self) -> list[OpenOrder]:
            self.snapshots += 1
            if self.snapshots == 1:
                return []
            raise RuntimeError("REST unavailable")

        async def place(self, _quotes, _meta) -> list[OpenOrder]:
            return [order]

        async def cancel(self, _order_ids: list[str]) -> bool:
            return False

        async def cancel_asset(self, _token: str) -> bool:
            return False

        def close(self) -> None:
            closed.append(True)

    async def no_sleep(_seconds: float) -> None:
        pass

    monkeypatch.setattr(livetest, "ExecutionGateway", FakeGateway)
    monkeypatch.setattr(livetest.asyncio, "sleep", no_sleep)

    ok = await livetest.run_livetest(
        cfg,
        _console(),
        5.0,
        market_slug=meta.slug,
    )

    assert ok is False
    assert closed == [True]


@pytest.mark.asyncio
async def test_doctor_prefers_configured_market_token(monkeypatch) -> None:
    from polymaker import doctor

    cfg = Config(
        markets=[MarketEntry(slug="configured-market")],
        secrets=Secrets(PK="test-private-key", BROWSER_ADDRESS="0x" + "1" * 40),
    )
    probed: list[str] = []

    class FakeResponse:
        status_code = 200

    class FakeHttpClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args) -> None:
            pass

        async def get(self, *_args, **_kwargs) -> FakeResponse:
            return FakeResponse()

    class FakeGateway:
        def __init__(self, _cfg) -> None:
            self.creds = SimpleNamespace()
            self.funder = "0x" + "1" * 40
            self.address = "0x" + "2" * 40

        async def connect(self) -> None:
            pass

        async def balance_allowance(self) -> dict[str, str]:
            return {"balance": "100000000"}

        async def positions(self) -> dict[str, tuple[float, float]]:
            return {"historical-position-token": (15.0, 0.5)}

        async def open_orders(self) -> list[OpenOrder]:
            return []

        def close(self) -> None:
            pass

    async def configured_market_token(_cfg) -> str:
        return "configured-token"

    async def tradeable(_cfg) -> tuple[bool, str]:
        return True, "one market checked"

    async def market_ws(token: str, _proxy) -> tuple[bool, str]:
        probed.append(token)
        return True, "book received"

    async def user_ws(*_args) -> tuple[bool, str]:
        return True, "connected"

    monkeypatch.setattr(doctor.httpx, "AsyncClient", FakeHttpClient)
    monkeypatch.setattr("polymaker.execution.gateway.ExecutionGateway", FakeGateway)
    monkeypatch.setattr(doctor, "_configured_market_token", configured_market_token, raising=False)
    monkeypatch.setattr(doctor, "_configured_markets_tradeable", tradeable)
    monkeypatch.setattr(doctor, "_market_ws_book", market_ws)
    monkeypatch.setattr(doctor, "_user_ws_auth", user_ws)

    ok = await doctor.run_doctor(cfg, _console())

    assert ok is True
    assert probed == ["configured-token"]
