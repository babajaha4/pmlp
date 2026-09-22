"""ExecutionGateway: the only component that sends actions to the CLOB.

Wraps the synchronous py-clob-client-v2 (which owns the hard V2 EIP-712 signing,
pUSD balance adjustment, and tick/fee caching) and offloads its blocking network
calls to a thread pool so the asyncio hot path never stalls. Every quote goes out
**post-only** (the maker-only mandate, enforced at the exchange).

A `paper=True` gateway shares the same path but fabricates order ids instead of
posting — so paper mode exercises the full pipeline.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import math
import random
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import date
from typing import Any, TypeVar

import httpx
from py_clob_client_v2.clob_types import RequestArgs
from py_clob_client_v2.headers.headers import create_level_2_headers

from polymaker.config import Config
from polymaker.domain import MarketMeta, OpenOrder, OrderState, Quote, Side
from polymaker.execution.ratelimit import TokenBucket
from polymaker.journal import Journal
from polymaker.logging import get_logger

log = get_logger("execution.gateway")

_T = TypeVar("_T")
_DATA_READ_ATTEMPTS = 3
_DATA_RETRYABLE_STATUS = frozenset({429, 502, 503, 504})
_DATA_RETRY_BASE_DELAY_S = 1.0
_DATA_RETRY_MAX_DELAY_S = 15.0
_LIQUIDITY_REWARD_MIN_PAYOUT = 1.0


class GatewayReadError(RuntimeError):
    """A required exchange snapshot could not be read authoritatively."""


def _tick_str(tick: float) -> str:
    return f"{tick:g}"


class ExecutionGateway:
    def __init__(
        self,
        cfg: Config,
        journal: Journal | None = None,
        *,
        paper: bool = False,
    ) -> None:
        self._cfg = cfg
        self._paper = paper
        self._journal = journal
        self._client: Any = None  # py_clob_client_v2.ClobClient
        self._creds: Any = None
        self._address: str = ""  # signer EOA
        self._funder: str = ""  # funds/positions live here (proxy/deposit wallet)
        self._data_host = cfg.wallet.data_api_host
        self._data_client = httpx.Client(timeout=15.0)
        # rate budgets: fraction of documented POST/DELETE ceilings (per second)
        f = cfg.execution.rate_budget_fraction
        self._order_bucket = TokenBucket(rate_per_s=200.0 * f, burst=500.0 * f)
        self._cancel_bucket = TokenBucket(rate_per_s=200.0 * f, burst=500.0 * f)
        self._paper_ids = itertools.count(1)
        self._hb_id: str = ""  # heartbeat chain
        self._hb_failures: int = 0
        # dedicated, bounded pool for blocking order/HTTP calls so a burst of
        # requotes across many markets can't starve the default executor
        self._pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="clob-io")

    @property
    def paper(self) -> bool:
        return self._paper

    @property
    def order_pressure(self) -> float:
        """0 = plenty of order-post budget, 1 = about to queue (shed load)."""
        return self._order_bucket.pressure

    def close(self) -> None:
        self._data_client.close()
        self._pool.shutdown(wait=False, cancel_futures=True)

    async def _io(self, fn: Callable[..., _T], *args: Any) -> _T:
        """Run a blocking client call on the dedicated pool."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._pool, fn, *args)

    @property
    def creds(self) -> Any:
        return self._creds

    @property
    def address(self) -> str:
        """The signing EOA address."""
        return self._address

    @property
    def funder(self) -> str:
        """The address holding funds/positions (proxy/deposit wallet, or the EOA)."""
        return self._funder or self._address

    # ── lifecycle ───────────────────────────────────────────────────────
    async def connect(self) -> None:
        """Build the client and derive L2 API creds (network). No-op fields in paper."""
        sec = self._cfg.secrets
        if self._paper and not sec.has_wallet:
            # paper mode runs the full pipeline without a wallet (no orders posted)
            self._address = sec.browser_address or "0xPAPER"
            self._funder = sec.browser_address or self._address
            log.info("gateway_connected", address=self._address[:10], paper=True)
            return
        if not sec.has_wallet:
            raise RuntimeError("no wallet configured (set PK and BROWSER_ADDRESS in .env)")

        def _build() -> tuple[Any, Any, str]:
            from py_clob_client_v2.client import ClobClient

            client = ClobClient(
                host=self._cfg.wallet.clob_host,
                chain_id=self._cfg.wallet.chain_id,
                key=sec.pk,
                # use_server_time=False: fetching /time before EVERY signed order
                # adds a full round-trip per op (latency killer through a proxy).
                # We check clock drift once below and rely on the local (NTP) clock.
                signature_type=self._cfg.wallet.signature_type,
                funder=sec.browser_address,
            )
            # Existing wallets already have deterministic L2 credentials. Derive
            # first to avoid a noisy/expected 400 from create_api_key; retain a
            # create fallback for a brand-new wallet.
            try:
                creds = client.derive_api_key()
            except Exception:
                creds = client.create_api_key()
            client.set_api_creds(creds)
            return client, creds, client.get_address()

        self._client, self._creds, self._address = await self._io(_build)
        await self._check_clock_drift()
        # funds/positions live on the funder (proxy/deposit wallet); fall back to EOA
        self._funder = sec.browser_address or self._address
        log.info("gateway_connected", signer=self._address[:10], funder=self._funder[:10],
                 paper=self._paper)

    async def _check_clock_drift(self) -> None:
        """Warn once if the local clock is skewed vs the exchange (affects L2 auth)."""
        try:
            async with httpx.AsyncClient(timeout=10.0) as c:
                r = await c.get(f"{self._cfg.wallet.clob_host}/time")
                server = float(r.text.strip().strip('"'))
            drift = abs(time.time() - server)
            if drift > 5.0:
                log.warning("clock_drift", drift_s=round(drift, 1),
                            note="sync system clock (NTP) — large skew can fail order auth")
            else:
                log.info("clock_ok", drift_s=round(drift, 1))
        except (httpx.HTTPError, ValueError) as exc:
            log.warning("clock_check_failed", err=str(exc))

    # ── placement ───────────────────────────────────────────────────────
    async def place(
        self, quotes: list[Quote], meta: MarketMeta,
        *, can_place: Callable[[], bool] | None = None,
    ) -> list[OpenOrder]:
        if not quotes:
            return []
        await self._order_bucket.acquire(len(quotes))
        if can_place is not None and not can_place():
            return []
        ts = time.time()
        self._journal_write("orders_out", [asdict(q) for q in quotes], ts)

        if self._paper:
            return [self._paper_order(q) for q in quotes]

        def _place() -> list[OpenOrder]:
            from py_clob_client_v2.clob_types import (
                OrderArgsV2,
                OrderType,
                PartialCreateOrderOptions,
                PostOrdersV2Args,
            )

            opts = PartialCreateOrderOptions(tick_size=_tick_str(meta.tick_size), neg_risk=meta.neg_risk)
            args = []
            for q in quotes:
                signed = self._client.create_order(
                    OrderArgsV2(token_id=q.token_id, price=q.price, size=q.size, side=q.side.value),
                    options=opts,
                )
                args.append(PostOrdersV2Args(order=signed, orderType=OrderType.GTC))
            resp = self._client.post_orders(args, post_only=self._cfg.execution.post_only)
            return self._parse_place_response(resp, quotes)

        try:
            return await self._io(_place)
        except Exception as exc:  # noqa: BLE001 - surface + continue; engine handles error rate
            log.error("place_failed", err=str(exc), n=len(quotes))
            return []

    def _paper_order(self, q: Quote) -> OpenOrder:
        oid = f"paper-{next(self._paper_ids)}"
        return OpenOrder(oid, q.token_id, q.side, q.price, q.size, OrderState.LIVE)

    def _parse_place_response(self, resp: Any, quotes: list[Quote]) -> list[OpenOrder]:
        """Map a batch post response to OpenOrders. Tolerant of shape variants;
        the user-WS order events + REST snapshot reconcile anything we miss."""
        items = resp if isinstance(resp, list) else resp.get("orders", resp.get("data", []))
        out: list[OpenOrder] = []
        for q, item in zip(quotes, items if isinstance(items, list) else [], strict=False):
            oid = _first(item, "orderID", "orderId", "order_id", "id", "hash")
            if not oid:
                log.warning("place_response_missing_id", item=str(item)[:120])
                continue
            out.append(OpenOrder(str(oid), q.token_id, q.side, q.price, q.size, OrderState.LIVE))
        return out

    # ── cancellation ────────────────────────────────────────────────────
    async def cancel(self, order_ids: list[str]) -> bool:
        """Cancel by id. Returns True on success — callers must NOT drop the
        orders from local state on failure (they may still be live)."""
        if not order_ids or self._paper:
            return True
        await self._cancel_bucket.acquire(1)

        def _cancel() -> None:
            self._client.cancel_orders(order_ids)

        try:
            await self._io(_cancel)
            return True
        except Exception as exc:  # noqa: BLE001
            log.error("cancel_failed", err=str(exc), n=len(order_ids))
            return False

    async def cancel_asset(self, asset_id: str) -> bool:
        """Cancel every order on one token (idempotent quarantine primitive)."""
        if self._paper:
            return True

        def _cancel() -> None:
            from py_clob_client_v2.clob_types import OrderMarketCancelParams

            self._client.cancel_market_orders(OrderMarketCancelParams(asset_id=asset_id))

        try:
            await self._io(_cancel)
            return True
        except Exception as exc:  # noqa: BLE001
            log.error("cancel_asset_failed", err=str(exc), token=asset_id[:12])
            return False

    async def cancel_all(self) -> None:
        if self._paper or self._client is None:
            return
        await self._io(self._client.cancel_all)
        log.info("cancel_all_sent")

    # ── market (taker) orders — used by moneydoctor, NOT the maker strategy ──
    async def market_order(
        self, token_id: str, side: Side, amount: float, meta: MarketMeta,
        *, fak: bool = True,
    ) -> dict[str, Any]:
        """Place a marketable order. amount = USD for BUY, shares for SELL.

        This is a TAKER order (crosses the spread) — only the moneydoctor live
        self-test uses it; the maker strategy never does.
        """
        if self._paper or self._client is None:
            return {"paper": True}

        def _do() -> dict[str, Any]:
            from py_clob_client_v2.clob_types import (
                MarketOrderArgsV2,
                OrderType,
                PartialCreateOrderOptions,
            )

            ot = OrderType.FAK if fak else OrderType.FOK
            args = MarketOrderArgsV2(token_id=token_id, amount=amount,
                                     side=side.value, order_type=ot)
            opts = PartialCreateOrderOptions(tick_size=_tick_str(meta.tick_size),
                                             neg_risk=meta.neg_risk)
            try:
                resp = self._client.create_and_post_market_order(args, opts, order_type=ot)
                return resp if isinstance(resp, dict) else {"resp": resp}
            except Exception as exc:  # noqa: BLE001 - surface as data, never crash the caller
                return {"status": "failed", "error": str(exc)}

        return await self._io(_do)

    async def get_book(self, token_id: str) -> dict[str, float]:
        """Live best bid/ask + touch depth for one token (public REST)."""
        try:
            async with httpx.AsyncClient(timeout=15.0) as c:
                r = await c.get(f"{self._cfg.wallet.clob_host}/book",
                                params={"token_id": token_id})
                r.raise_for_status()
                b = r.json()
                bids = [(float(x["price"]), float(x["size"])) for x in b.get("bids", [])]
                asks = [(float(x["price"]), float(x["size"])) for x in b.get("asks", [])]
                best_bid = max(bids)[0] if bids else 0.0
                best_ask = min(asks)[0] if asks else 1.0
                ask_depth = sum(s for p, s in asks if p <= best_ask + 1e-9)
                bid_depth = sum(s for p, s in bids if p >= best_bid - 1e-9)
                return {"best_bid": best_bid, "best_ask": best_ask,
                        "ask_depth": ask_depth, "bid_depth": bid_depth}
        except (httpx.HTTPError, KeyError, ValueError) as exc:
            log.warning("get_book_failed", err=str(exc))
            return {}

    async def get_full_book(
        self, token_id: str
    ) -> tuple[list[tuple[float, float]], list[tuple[float, float]], str | None] | None:
        """Full L2 book (bids, asks, hash) via public REST — for periodic
        integrity refresh against the WS book."""
        try:
            async with httpx.AsyncClient(timeout=15.0) as c:
                r = await c.get(f"{self._cfg.wallet.clob_host}/book",
                                params={"token_id": token_id})
                r.raise_for_status()
                b = r.json()
                bids = [(float(x["price"]), float(x["size"])) for x in b.get("bids", [])]
                asks = [(float(x["price"]), float(x["size"])) for x in b.get("asks", [])]
                return bids, asks, b.get("hash")
        except (httpx.HTTPError, KeyError, ValueError) as exc:
            log.warning("get_full_book_failed", err=str(exc))
            return None

    async def token_balance(self, token_id: str) -> float:
        """Exact on-chain conditional-token balance (shares) held by the funder.

        Returns None on total RPC failure so callers can distinguish "0 shares"
        from "couldn't read".
        """
        bal = await self._token_balance_opt(token_id)
        return bal if bal is not None else 0.0

    async def _token_balance_opt(self, token_id: str) -> float | None:
        def _read() -> float | None:
            from web3 import Web3
            from web3.middleware import ExtraDataToPOAMiddleware

            configured = self._cfg.secrets.polygon_rpc or self._cfg.wallet.polygon_rpc
            rpcs = [configured, "https://polygon-bor-rpc.publicnode.com",
                    "https://polygon.llamarpc.com", "https://rpc.ankr.com/polygon"]
            abi = [{"name": "balanceOf", "type": "function", "stateMutability": "view",
                    "inputs": [{"name": "a", "type": "address"}, {"name": "id", "type": "uint256"}],
                    "outputs": [{"name": "", "type": "uint256"}]}]
            for rpc in dict.fromkeys(rpcs):  # dedupe, keep order
                try:
                    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 15}))
                    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
                    ctf = w3.eth.contract(
                        address=Web3.to_checksum_address(self._cfg.merge.conditional_tokens),
                        abi=abi,
                    )
                    raw = ctf.functions.balanceOf(
                        Web3.to_checksum_address(self.funder), int(token_id)
                    ).call()
                    return float(raw) / 1e6
                except Exception:  # noqa: BLE001, PERF203 - try next RPC
                    continue
            return None

        try:
            return await self._io(_read)
        except Exception as exc:  # noqa: BLE001
            log.warning("token_balance_failed", err=str(exc))
            return None

    async def token_balances(self, token_ids: list[str]) -> dict[str, float] | None:
        """Batch on-chain balances for several tokens in one RPC session.

        Used by the position-divergence monitor. Returns None on RPC failure.
        """
        if not token_ids:
            return {}

        def _read() -> dict[str, float] | None:
            from web3 import Web3
            from web3.middleware import ExtraDataToPOAMiddleware

            configured = self._cfg.secrets.polygon_rpc or self._cfg.wallet.polygon_rpc
            rpcs = [configured, "https://polygon-bor-rpc.publicnode.com",
                    "https://polygon.llamarpc.com", "https://rpc.ankr.com/polygon"]
            abi = [{"name": "balanceOf", "type": "function", "stateMutability": "view",
                    "inputs": [{"name": "a", "type": "address"}, {"name": "id", "type": "uint256"}],
                    "outputs": [{"name": "", "type": "uint256"}]}]
            funder = None
            for rpc in dict.fromkeys(rpcs):
                try:
                    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 20}))
                    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
                    ctf = w3.eth.contract(
                        address=Web3.to_checksum_address(self._cfg.merge.conditional_tokens),
                        abi=abi,
                    )
                    funder = Web3.to_checksum_address(self.funder)
                    out: dict[str, float] = {}
                    for tid in token_ids:
                        raw = ctf.functions.balanceOf(funder, int(tid)).call()
                        out[tid] = float(raw) / 1e6
                    return out
                except Exception:  # noqa: BLE001, PERF203
                    continue
            return None

        try:
            return await self._io(_read)
        except Exception as exc:  # noqa: BLE001
            log.warning("token_balances_failed", err=str(exc))
            return None

    async def collateral_balance(self) -> float:
        """pUSD balance (float) on the funder."""
        ba = await self.balance_allowance()
        for k in ("balance", "collateral", "amount"):
            if isinstance(ba, dict) and k in ba:
                try:
                    v = float(ba[k])
                    return v / 1e6 if v > 1e6 else v
                except (ValueError, TypeError):
                    return 0.0
        return 0.0

    # ── heartbeat (dead-man switch) ─────────────────────────────────────
    async def heartbeat(self) -> bool:
        """Send one chained heartbeat. Returns True on success.

        The exchange expects each heartbeat to carry the previous heartbeat_id.
        Consecutive failures are tracked in `heartbeat_failures`: after enough
        misses the exchange auto-cancels ALL our orders, so the engine must
        stop quoting and resync once the heartbeat recovers.
        """
        if self._paper or self._client is None:
            return True

        def _beat() -> Any:
            return self._client.post_heartbeat(self._hb_id)

        try:
            resp = await self._io(_beat)
            new_id = _first(resp, "heartbeat_id", "heartbeatId", "id")
            self._hb_id = str(new_id) if new_id else ""
            if self._hb_failures:
                log.info("heartbeat_recovered", after_failures=self._hb_failures)
            self._hb_failures = 0
            return True
        except Exception as exc:  # noqa: BLE001
            self._hb_failures += 1
            self._hb_id = ""  # broken chain — restart it
            log.warning("heartbeat_failed", err=str(exc), consecutive=self._hb_failures)
            return False

    @property
    def heartbeat_failures(self) -> int:
        return self._hb_failures

    # ── reads ───────────────────────────────────────────────────────────
    async def get_order(self, order_id: str) -> OpenOrder | None:
        """Read one order, including terminal states; ``None`` means not found."""
        if self._paper or self._client is None:
            return None

        def _get() -> OpenOrder | None:
            raw = self._client.get_order(order_id)
            if raw is None:
                return None
            if isinstance(raw, dict) and "data" in raw:
                raw = raw["data"]
            if not isinstance(raw, dict):
                raise ValueError("unexpected order response shape")

            status = str(raw["status"]).upper()
            original = float(raw["original_size"])
            matched = float(raw.get("size_matched", 0))
            remaining = max(0.0, original - matched)
            if status == "LIVE":
                state = OrderState.PARTIALLY_FILLED if matched > 0 else OrderState.LIVE
            elif status == "MATCHED":
                state = OrderState.DONE
            elif status in ("CANCELED", "CANCELLED", "CANCELED_MARKET_RESOLVED"):
                state = OrderState.CANCELED
            elif status == "INVALID":
                state = OrderState.REJECTED
            else:
                raise ValueError(f"unknown order status: {status}")

            return OpenOrder(
                str(_first(raw, "id", "orderID", "order_id")),
                str(raw["asset_id"]),
                Side(str(raw["side"]).upper()),
                float(raw["price"]),
                remaining,
                state,
                created_ts=float(raw.get("created_at", time.time())),
            )

        try:
            return await self._io(_get)
        except Exception as exc:  # noqa: BLE001 - a snapshot failure is fail-closed
            log.warning("order_read_failed", order_id=order_id, err=str(exc))
            raise GatewayReadError(f"order {order_id} snapshot unavailable") from exc

    async def open_orders(self) -> list[OpenOrder]:
        if self._paper or self._client is None:
            return []

        def _get() -> list[OpenOrder]:
            raw = self._client.get_open_orders()
            rows: Any
            if isinstance(raw, list):
                rows = raw
            elif isinstance(raw, dict) and ("data" in raw or "orders" in raw):
                rows = raw.get("data", raw.get("orders"))
            else:
                raise ValueError("unexpected open-orders response shape")
            if not isinstance(rows, list):
                raise ValueError("open-orders payload is not a list")
            out = []
            for r in rows:
                try:
                    side = Side(str(r["side"]).upper())
                    original = float(r.get("original_size", r.get("size", 0)))
                    matched = float(r.get("size_matched", 0))
                    remaining = original - matched
                    price = float(r["price"])
                    if not all(math.isfinite(v) for v in (original, matched, remaining, price)):
                        raise ValueError("nonfinite open-order economics")
                    out.append(
                        OpenOrder(
                            str(_first(r, "id", "orderID", "order_id")),
                            str(r["asset_id"]),
                            side,
                            price,
                            remaining,
                            OrderState.LIVE,
                        )
                    )
                except (KeyError, ValueError, TypeError) as exc:
                    raise ValueError("malformed open order in snapshot") from exc
            return out

        try:
            return await self._io(_get)
        except Exception as exc:  # noqa: BLE001
            log.warning("open_orders_failed", err=str(exc))
            raise GatewayReadError("open-orders snapshot unavailable") from exc

    async def trades(self, *, after: int | None = None) -> list[dict[str, Any]]:
        """Read the complete authenticated trade snapshot for reconciliation."""
        if self._paper:
            return []
        if self._client is None:
            raise GatewayReadError("trades snapshot unavailable: gateway not connected")

        def _get() -> list[dict[str, Any]]:
            from py_clob_client_v2.clob_types import TradeParams

            raw = self._client.get_trades(TradeParams(after=after), only_first_page=False)
            if not isinstance(raw, list):
                raise ValueError("trades payload is not a list")

            known_statuses = {"MATCHED", "MINED", "CONFIRMED", "RETRYING", "FAILED"}
            for row in raw:
                if not isinstance(row, dict):
                    raise ValueError("trade row is not an object")
                trade_id = row.get("id")
                status = row.get("status")
                if not isinstance(trade_id, str) or not trade_id.strip():
                    raise ValueError("trade row has invalid identity")
                if not isinstance(status, str) or status.upper() not in known_statuses:
                    raise ValueError("trade row has invalid status")
                if not isinstance(row.get("maker_orders"), list):
                    raise ValueError("trade maker_orders is not a list")
            return raw

        try:
            return await self._io(_get)
        except Exception as exc:  # noqa: BLE001 - trade history is reconciliation-critical
            log.warning("trades_snapshot_failed", err=str(exc))
            raise GatewayReadError("trades snapshot unavailable") from exc

    async def positions(self) -> dict[str, tuple[float, float]]:
        """{token_id: (size, avg_price)} from the data API (reconcile use).

        Queries the FUNDER (where positions live), not the signer EOA.
        """
        user = self.funder
        if user == "0xPAPER":
            return {}
        if not user or not user.startswith("0x"):
            raise GatewayReadError("positions snapshot unavailable: invalid funder")

        def _get() -> dict[str, tuple[float, float]]:
            response: httpx.Response | None = None
            for attempt in range(_DATA_READ_ATTEMPTS):
                try:
                    response = self._data_client.get(
                        f"{self._data_host}/positions", params={"user": user}
                    )
                    if response.status_code not in _DATA_RETRYABLE_STATUS:
                        response.raise_for_status()
                        break
                    if attempt == _DATA_READ_ATTEMPTS - 1:
                        response.raise_for_status()
                    delay = _data_retry_delay(response, attempt)
                    log.warning(
                        "positions_retry",
                        status=response.status_code,
                        attempt=attempt + 1,
                        delay_s=round(delay, 2),
                    )
                    time.sleep(delay)
                except httpx.TransportError as exc:
                    if attempt == _DATA_READ_ATTEMPTS - 1:
                        raise
                    delay = _data_retry_delay(None, attempt)
                    log.warning(
                        "positions_retry",
                        err=str(exc),
                        attempt=attempt + 1,
                        delay_s=round(delay, 2),
                    )
                    time.sleep(delay)
            if response is None:
                raise RuntimeError("positions request produced no response")

            payload = response.json()
            if not isinstance(payload, list):
                raise ValueError("positions payload is not a list")
            positions: dict[str, tuple[float, float]] = {}
            for p in payload:
                if not isinstance(p, dict):
                    raise ValueError("position row is not an object")
                asset = p.get("asset")
                if not isinstance(asset, str) or not asset:
                    raise ValueError("position row has invalid asset")
                size = float(p.get("size", 0))
                avg = float(p.get("avgPrice", 0))
                if not math.isfinite(size) or not math.isfinite(avg):
                    raise ValueError("nonfinite position economics")
                if size > 0:
                    positions[asset] = (size, avg)
            return positions

        try:
            return await self._io(_get)
        except Exception as exc:  # noqa: BLE001 - a snapshot failure is fail-closed
            log.warning("positions_failed", err=str(exc))
            raise GatewayReadError("positions snapshot unavailable") from exc

    async def official_liquidity_rewards(
        self, day: str, order_ids: list[str],
    ) -> dict[str, Any]:
        """Read official CLOB reward earnings and live scoring state.

        L2 credentials authenticate the request. The maker query is explicitly
        scoped to the funder/Deposit Wallet because that address owns signature
        type 3 orders and receives the daily reward distribution.
        """
        if self._client is None or self._creds is None:
            raise GatewayReadError("official liquidity rewards unavailable: gateway not connected")
        try:
            parsed_day = date.fromisoformat(day)
        except ValueError as exc:
            raise GatewayReadError("official liquidity rewards unavailable: invalid date") from exc
        if parsed_day.isoformat() != day:
            raise GatewayReadError("official liquidity rewards unavailable: invalid date")
        unique_order_ids = list(dict.fromkeys(order_ids))
        if any(not isinstance(order_id, str) or not order_id for order_id in unique_order_ids):
            raise GatewayReadError("official liquidity rewards unavailable: invalid order identity")

        def _read() -> dict[str, Any]:
            maker_params: dict[str, str | int | float | bool | None] = {
                "maker_address": self.funder,
                "signature_type": int(self._cfg.wallet.signature_type),
            }
            total_response = self._authenticated_reward_get(
                "/rewards/user/total", {"date": day, **maker_params}
            )
            percentages_response = self._authenticated_reward_get(
                "/rewards/user/percentages", maker_params
            )
            scoring_response: object = {}
            if unique_order_ids:
                scoring_path = "/orders-scoring"
                serialized = json.dumps(unique_order_ids, separators=(",", ":"))
                request = RequestArgs(
                    method="POST", request_path=scoring_path,
                    body=unique_order_ids, serialized_body=serialized,
                )
                headers = create_level_2_headers(
                    self._client.signer, self._creds, request,
                )
                headers["Content-Type"] = "application/json"
                response = self._data_client.post(
                    f"{self._cfg.wallet.clob_host.rstrip('/')}{scoring_path}",
                    headers=headers,
                    content=serialized,
                )
                response.raise_for_status()
                scoring_response = response.json()
            return _parse_official_rewards(
                day, self.funder, unique_order_ids,
                total_response, percentages_response, scoring_response,
            )

        try:
            return await self._io(_read)
        except Exception as exc:  # noqa: BLE001 - optional snapshot remains explicitly unknown
            log.warning("official_liquidity_rewards_failed", err=str(exc))
            raise GatewayReadError("official liquidity rewards unavailable") from exc

    def _authenticated_reward_get(
        self, path: str, params: dict[str, str | int | float | bool | None],
    ) -> object:
        request = RequestArgs(method="GET", request_path=path)
        headers = create_level_2_headers(self._client.signer, self._creds, request)
        response = self._data_client.get(
            f"{self._cfg.wallet.clob_host.rstrip('/')}{path}",
            headers=headers,
            params=params,
        )
        response.raise_for_status()
        return response.json()

    async def balance_allowance(self) -> dict[str, Any]:
        """Collateral balance/allowance snapshot (for `doctor`)."""
        if self._client is None:
            if self._paper:
                return {}
            raise GatewayReadError("balance/allowance snapshot unavailable: gateway not connected")

        def _get() -> dict[str, Any]:
            from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

            result: dict[str, Any] = self._client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )
            return result

        try:
            return await self._io(_get)
        except Exception as exc:  # noqa: BLE001
            log.warning("balance_allowance_failed", err=str(exc))
            raise GatewayReadError("balance/allowance snapshot unavailable") from exc

    def _journal_write(self, kind: str, payload: Any, ts: float) -> None:
        if self._journal is not None:
            self._journal.write(kind, payload, ts)


def _first(d: Any, *keys: str) -> Any:
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d and d[k]:
            return d[k]
    return None


def _data_retry_delay(response: httpx.Response | None, attempt: int) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after is not None:
            try:
                parsed = float(retry_after)
            except ValueError:
                pass
            else:
                if math.isfinite(parsed) and parsed >= 0:
                    return min(parsed, _DATA_RETRY_MAX_DELAY_S)
    exponential = float(_DATA_RETRY_BASE_DELAY_S * (2**attempt))
    jitter = float(random.uniform(0.0, 0.25))
    return float(min(exponential + jitter, _DATA_RETRY_MAX_DELAY_S))


def _parse_official_rewards(
    day: str,
    funder: str,
    order_ids: list[str],
    total_payload: object,
    percentages_payload: object,
    scoring_payload: object,
) -> dict[str, Any]:
    if not isinstance(total_payload, list):
        raise ValueError("official earnings payload is not a list")
    earnings: list[dict[str, Any]] = []
    earnings_value = 0.0
    for row in total_payload:
        if not isinstance(row, dict):
            raise ValueError("official earnings row is not an object")
        asset = row.get("asset_address")
        maker = row.get("maker_address")
        observed_day = row.get("date")
        if not isinstance(asset, str) or not asset:
            raise ValueError("official earnings asset is invalid")
        if not isinstance(maker, str) or maker.lower() != funder.lower():
            raise ValueError("official earnings maker does not match funder")
        if not isinstance(observed_day, str) or not observed_day.startswith(day):
            raise ValueError("official earnings date does not match query")
        amount = _nonnegative_finite_float(row.get("earnings"))
        asset_rate = _nonnegative_finite_float(row.get("asset_rate"))
        earnings.append({
            "asset_address": asset,
            "earnings": amount,
            "asset_rate": asset_rate,
        })
        earnings_value += amount * asset_rate

    if not isinstance(percentages_payload, dict):
        raise ValueError("official reward percentages payload is not an object")
    percentages: dict[str, float] = {}
    for condition_id, raw_percentage in percentages_payload.items():
        percentage = _nonnegative_finite_float(raw_percentage)
        if (
            not isinstance(condition_id, str) or not condition_id
            or percentage > 100
        ):
            raise ValueError("official reward percentage is invalid")
        percentages[condition_id] = percentage

    if not isinstance(scoring_payload, dict) or set(scoring_payload) != set(order_ids):
        raise ValueError("official order scoring snapshot is incomplete")
    if any(type(value) is not bool for value in scoring_payload.values()):
        raise ValueError("official order scoring value is invalid")
    order_scoring = {order_id: bool(scoring_payload[order_id]) for order_id in order_ids}
    return {
        "date": day,
        "earnings": earnings,
        "earnings_value": earnings_value,
        "payout_eligible": earnings_value >= _LIQUIDITY_REWARD_MIN_PAYOUT,
        "percentages": percentages,
        "order_scoring": order_scoring,
        "scoring_order_count": sum(order_scoring.values()),
        "checked_order_count": len(order_scoring),
    }


def _nonnegative_finite_float(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError("official reward number is invalid")
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise ValueError("official reward number is invalid")
    return number
