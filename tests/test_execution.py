"""Tests for the rate budgeter and the paper-mode gateway."""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import httpx
import pytest
import respx

from polymaker.config import Config, WalletConfig
from polymaker.domain import Quote, Side
from polymaker.execution.gateway import ExecutionGateway, GatewayReadError, _tick_str
from polymaker.execution.ratelimit import TokenBucket


def test_tick_str_formats():
    assert _tick_str(0.01) == "0.01"
    assert _tick_str(0.001) == "0.001"
    assert _tick_str(0.0025) == "0.0025"
    assert _tick_str(0.1) == "0.1"


async def test_token_bucket_limits_rate():
    bucket = TokenBucket(rate_per_s=100.0, burst=5.0)
    start = time.monotonic()
    # burst of 5 is instant; the next 5 must wait ~ (5/100)s = 50ms
    for _ in range(10):
        await bucket.acquire(1)
    elapsed = time.monotonic() - start
    assert elapsed >= 0.04  # had to wait for refill


async def test_token_bucket_pressure_rises_when_drained():
    bucket = TokenBucket(rate_per_s=10.0, burst=10.0)
    assert bucket.pressure == pytest.approx(0.0, abs=0.01)
    for _ in range(10):
        await bucket.acquire(1)
    assert bucket.pressure > 0.8


async def test_paper_gateway_places_and_cancels_without_wallet(meta):
    cfg = Config()  # defaults, no secrets
    gw = ExecutionGateway(cfg, paper=True)
    quotes = [
        Quote(meta.yes.token_id, Side.BUY, 0.49, 100),
        Quote(meta.no.token_id, Side.BUY, 0.48, 100),
    ]
    placed = await gw.place(quotes, meta)
    assert len(placed) == 2
    assert all(o.order_id.startswith("paper-") for o in placed)
    # cancel is a no-op in paper mode but must not raise
    await gw.cancel([o.order_id for o in placed])
    assert await gw.open_orders() == []


async def test_paper_gateway_heartbeat_and_cancel_all_noop():
    gw = ExecutionGateway(Config(), paper=True)
    assert await gw.heartbeat() is True  # paper: healthy no-op
    assert gw.heartbeat_failures == 0
    await gw.cancel_all()  # no client, must not raise


def test_gateway_requires_wallet_for_live_connect(monkeypatch):
    from polymaker.config import Secrets

    # Explicit values override process credentials as well as disabling dotenv.
    cfg = Config(secrets=Secrets(PK="", BROWSER_ADDRESS="", _env_file=None))
    gw = ExecutionGateway(cfg, paper=False)

    async def deny_io(*_args, **_kwargs):
        pytest.fail("missing-wallet test attempted a network connection")

    monkeypatch.setattr(gw, "_io", deny_io)
    try:
        assert not cfg.secrets.has_wallet
        with pytest.raises(RuntimeError, match="no wallet"):
            asyncio.run(gw.connect())
        assert gw._client is None
    finally:
        gw.close()


@pytest.mark.asyncio
async def test_gateway_trades_reads_valid_snapshot():
    row = {"id": "t", "status": "CONFIRMED", "maker_orders": []}
    seen: dict[str, object] = {}

    def get_trades(params, *, only_first_page):
        seen["after"] = params.after
        seen["only_first_page"] = only_first_page
        return [row]

    gateway = ExecutionGateway(Config())
    gateway._client = SimpleNamespace(get_trades=get_trades)
    assert await gateway.trades(after=123) == [row]
    assert seen == {"after": 123, "only_first_page": False}
    gateway.close()


@pytest.mark.parametrize("mode", ["empty-live", "disconnected-live", "paper-no-client"])
async def test_gateway_trade_snapshot_empty_and_disconnected_boundaries(mode):
    gateway = ExecutionGateway(Config(), paper=mode == "paper-no-client")
    if mode == "empty-live":
        gateway._client = SimpleNamespace(get_trades=lambda *_args, **_kwargs: [])
    try:
        if mode == "disconnected-live":
            with pytest.raises(GatewayReadError, match="not connected"):
                await gateway.trades(after=123)
        else:
            assert await gateway.trades(after=123) == []
    finally:
        gateway.close()


@pytest.mark.parametrize("field", ["size", "avgPrice"])
@pytest.mark.parametrize("value", ["nan", "inf", 10 ** 400], ids=["nan", "inf", "huge"])
@respx.mock
async def test_gateway_positions_rejects_nonfinite_economics(field, value):
    gateway = ExecutionGateway(Config())
    gateway._funder = "0x0000000000000000000000000000000000000001"
    row = {"asset": "token", "size": "10", "avgPrice": "0.5", field: value}
    respx.get(f"{gateway._data_host}/positions").mock(return_value=httpx.Response(200, json=[row]))
    try:
        with pytest.raises(GatewayReadError):
            await gateway.positions()
    finally:
        gateway.close()


@respx.mock
async def test_gateway_positions_retries_429_using_retry_after(monkeypatch):
    gateway = ExecutionGateway(Config())
    gateway._funder = "0x0000000000000000000000000000000000000001"
    sleeps: list[float] = []
    monkeypatch.setattr("polymaker.execution.gateway.time.sleep", sleeps.append)
    route = respx.get(f"{gateway._data_host}/positions").mock(side_effect=[
        httpx.Response(429, headers={"Retry-After": "2"}),
        httpx.Response(200, json=[]),
    ])
    try:
        assert await gateway.positions() == {}
        assert route.call_count == 2
        assert sleeps == [2.0]
    finally:
        gateway.close()


@respx.mock
async def test_gateway_positions_exhausted_retry_remains_fail_closed(monkeypatch):
    gateway = ExecutionGateway(Config())
    gateway._funder = "0x0000000000000000000000000000000000000001"
    monkeypatch.setattr("polymaker.execution.gateway.time.sleep", lambda _delay: None)
    route = respx.get(f"{gateway._data_host}/positions").mock(
        return_value=httpx.Response(503)
    )
    try:
        with pytest.raises(GatewayReadError, match="positions snapshot unavailable"):
            await gateway.positions()
        assert route.call_count == 3
    finally:
        gateway.close()


@respx.mock
async def test_gateway_positions_does_not_retry_nonretryable_status(monkeypatch):
    gateway = ExecutionGateway(Config())
    gateway._funder = "0x0000000000000000000000000000000000000001"
    monkeypatch.setattr(
        "polymaker.execution.gateway.time.sleep",
        lambda _delay: pytest.fail("401 response was retried"),
    )
    route = respx.get(f"{gateway._data_host}/positions").mock(
        return_value=httpx.Response(401)
    )
    try:
        with pytest.raises(GatewayReadError, match="positions snapshot unavailable"):
            await gateway.positions()
        assert route.call_count == 1
    finally:
        gateway.close()


@respx.mock
async def test_gateway_positions_reuses_client_and_rejects_malformed_success():
    gateway = ExecutionGateway(Config())
    gateway._funder = "0x0000000000000000000000000000000000000001"
    route = respx.get(f"{gateway._data_host}/positions").mock(
        return_value=httpx.Response(200, json={"positions": []})
    )
    client = gateway._data_client
    try:
        with pytest.raises(GatewayReadError, match="positions snapshot unavailable"):
            await gateway.positions()
        assert gateway._data_client is client
        assert route.call_count == 1
    finally:
        gateway.close()
    assert client.is_closed


@respx.mock
async def test_official_liquidity_rewards_use_funder_and_l2_auth(monkeypatch):
    gateway = ExecutionGateway(Config(wallet=WalletConfig(signature_type=3)))
    gateway._funder = "0x0000000000000000000000000000000000000001"
    gateway._client = SimpleNamespace(signer=object())
    gateway._creds = object()
    signed: list[tuple[str, str, object]] = []

    def headers(_signer, _creds, request):
        signed.append((request.method, request.request_path, request.serialized_body))
        return {"POLY_API_KEY": "test"}

    monkeypatch.setattr("polymaker.execution.gateway.create_level_2_headers", headers)
    total = respx.get(f"{gateway._cfg.wallet.clob_host}/rewards/user/total").mock(
        return_value=httpx.Response(200, json=[{
            "date": "2026-09-22T00:00:00Z",
            "asset_address": "0xpUSD",
            "maker_address": gateway.funder,
            "earnings": 1.25,
            "asset_rate": 1.0,
        }])
    )
    percentages = respx.get(
        f"{gateway._cfg.wallet.clob_host}/rewards/user/percentages"
    ).mock(return_value=httpx.Response(200, json={"0xcondition": 12.5}))
    scoring = respx.post(f"{gateway._cfg.wallet.clob_host}/orders-scoring").mock(
        return_value=httpx.Response(200, json={"order-1": True, "order-2": False})
    )
    try:
        result = await gateway.official_liquidity_rewards(
            "2026-09-22", ["order-1", "order-2"]
        )
    finally:
        gateway.close()

    assert result["date"] == "2026-09-22"
    assert result["earnings_value"] == pytest.approx(1.25)
    assert result["payout_eligible"] is True
    assert result["percentages"] == {"0xcondition": 12.5}
    assert result["order_scoring"] == {"order-1": True, "order-2": False}
    assert result["scoring_order_count"] == 1
    assert result["checked_order_count"] == 2
    assert total.calls[0].request.url.params["maker_address"] == gateway.funder
    assert total.calls[0].request.url.params["signature_type"] == "3"
    assert percentages.calls[0].request.url.params["maker_address"] == gateway.funder
    assert percentages.calls[0].request.url.params["signature_type"] == "3"
    assert scoring.calls[0].request.content == b'["order-1","order-2"]'
    assert signed == [
        ("GET", "/rewards/user/total", None),
        ("GET", "/rewards/user/percentages", None),
        ("POST", "/orders-scoring", '["order-1","order-2"]'),
    ]


@pytest.mark.parametrize(
    ("endpoint", "payload"),
    [
        ("total", {"earnings": []}),
        ("percentages", {"0xcondition": "nan"}),
        ("scoring", {"wrong-order": True}),
    ],
)
@respx.mock
async def test_official_liquidity_rewards_reject_malformed_snapshots(
    monkeypatch, endpoint, payload,
):
    gateway = ExecutionGateway(Config())
    gateway._funder = "0x0000000000000000000000000000000000000001"
    gateway._client = SimpleNamespace(signer=object())
    gateway._creds = object()
    monkeypatch.setattr(
        "polymaker.execution.gateway.create_level_2_headers",
        lambda *_args: {"POLY_API_KEY": "test"},
    )
    total_payload: object = [{
        "date": "2026-09-22T00:00:00Z",
        "asset_address": "0xpUSD",
        "maker_address": gateway.funder,
        "earnings": 0.5,
        "asset_rate": 1.0,
    }]
    percentage_payload: object = {"0xcondition": 10.0}
    scoring_payload: object = {"order-1": True}
    if endpoint == "total":
        total_payload = payload
    elif endpoint == "percentages":
        percentage_payload = payload
    else:
        scoring_payload = payload
    respx.get(f"{gateway._cfg.wallet.clob_host}/rewards/user/total").mock(
        return_value=httpx.Response(200, json=total_payload)
    )
    respx.get(f"{gateway._cfg.wallet.clob_host}/rewards/user/percentages").mock(
        return_value=httpx.Response(200, json=percentage_payload)
    )
    respx.post(f"{gateway._cfg.wallet.clob_host}/orders-scoring").mock(
        return_value=httpx.Response(200, json=scoring_payload)
    )
    try:
        with pytest.raises(GatewayReadError, match="official liquidity rewards unavailable"):
            await gateway.official_liquidity_rewards("2026-09-22", ["order-1"])
    finally:
        gateway.close()


@pytest.mark.parametrize("field", ["price", "original_size", "size_matched"])
async def test_gateway_orders_rejects_nonfinite_economics(field):
    gateway = ExecutionGateway(Config())
    row = {"id": "order", "asset_id": "token", "side": "BUY", "price": "0.5",
           "original_size": "10", "size_matched": "0", field: "nan"}
    gateway._client = SimpleNamespace(get_open_orders=lambda: [row])
    try:
        with pytest.raises(GatewayReadError):
            await gateway.open_orders()
    finally:
        gateway.close()
