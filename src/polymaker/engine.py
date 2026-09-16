"""Engine: wires every component into a single async event loop.

Data flow per market:
  market WS -> OrderBook -> (wake) -> Quoter task -> strategy (pure) -> reconcile
  -> ExecutionGateway ; user WS -> StateStore ; periodic REST reconcile + heartbeat.

One lightweight quoter task per market, woken by book/fill events and debounced.
The strategy layer is pure; the engine owns all the state and I/O around it.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from polymaker.alerts import Alerter
from polymaker.catalog.gamma import GammaClient, fetch_reward_rates, parse_market
from polymaker.catalog.store import CatalogStore
from polymaker.config import Config, StrategyProfile
from polymaker.domain import Fill, MarketMeta, Regime, Side, TradeState
from polymaker.execution.gateway import ExecutionGateway, GatewayReadError
from polymaker.execution.reconciler import reconcile
from polymaker.journal import Journal
from polymaker.logging import get_logger
from polymaker.marketdata.parse import TradePrint
from polymaker.marketdata.service import MarketDataService
from polymaker.merge import Merger
from polymaker.position_tolerance import authoritative_position_matches
from polymaker.risk.manager import RiskManager
from polymaker.state.store import StateStore
from polymaker.state.tracker import UserEventProcessor
from polymaker.strategy.estimators import (
    FlowEstimator,
    MarketEstimators,
    MarkoutTracker,
    VolEstimator,
)
from polymaker.strategy.quoting import QuoteInputs, compute_fair_value, construct_quotes
from polymaker.strategy.regime import RegimeInputs, RegimeMachine
from polymaker.userstream.client import UserStream
from polymaker.userstream.parse import normalize_trade

log = get_logger("engine")

_TRADE_SYNC_INITIALIZED = "confirmed_trade_sync_initialized"
_TRADE_SYNC_TS = "confirmed_trade_sync_ts"
_TRADE_SYNC_OVERLAP_S = 300
_TRADE_SYNC_PENDING = "confirmed_trade_pending"
_TRADE_SYNC_REQUIRE_PROOF = "confirmed_trade_requires_proof"
_TRADE_SYNC_PENDING_MAX_AGE_S = 7 * 86400


def _utc_day_start_ts(now: float | None = None) -> int:
    dt = datetime.fromtimestamp(time.time() if now is None else now, tz=UTC)
    return int(dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

class Engine:
    def __init__(self, cfg: Config, *, paper: bool = False) -> None:
        self.cfg = cfg
        self.paper = paper
        self._running = False

        self.journal = Journal(cfg.paths.journal_dir, enabled=cfg.engine.journal,
                               day="paper" if paper else "live")
        state_path = _paper_state_path(cfg.paths.db) if paper else cfg.paths.db
        self.state = StateStore(state_path)
        self.catalog = CatalogStore(cfg.paths.db)
        self.gateway = ExecutionGateway(cfg, self.journal, paper=paper)
        self.risk = RiskManager(cfg.risk, self.state)
        self.merger = Merger(cfg)
        self.alerter = Alerter(cfg.secrets.alert_webhook_url, proxy=cfg.proxy)

        self.md = MarketDataService(on_dirty=self._on_dirty, on_trade=self._on_trade,
                                    journal=self.journal, proxy=cfg.proxy)
        self.user_proc = UserEventProcessor(self.state, on_change=self._wake_cid,
                                            on_fill=self._on_fill, before_fill=self._prepare_fill)
        self.user: UserStream | None = None

        # per-market state
        self.metas: dict[str, MarketMeta] = {}
        self.profiles: dict[str, StrategyProfile] = {}
        self.est: dict[str, MarketEstimators] = {}
        self.regime_m: dict[str, RegimeMachine] = {}
        self._dirty: dict[str, asyncio.Event] = {}
        self._sweep: dict[str, bool] = {}
        self._merging: set[str] = set()
        self._token_cid: dict[str, str] = {}
        self._locks: dict[str, asyncio.Lock] = {}  # per-market: serialize recompute vs reconcile
        self._reservation_lock = asyncio.Lock()  # atomic cross-market exposure reservation
        self._placement_lock = asyncio.Lock()  # placement completion precedes cancellation
        self._placement_epoch = 0
        self._halted: set[str] = set()  # markets closed/resolved/not-accepting
        self._last_quote_fv: dict[str, float] = {}  # requote suppression
        # supervised tasks: name -> (factory, task) so a dead task restarts
        self._task_specs: dict[str, Any] = {}
        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._aux_tasks: list[asyncio.Task[Any]] = []  # fire-and-forget (merges)
        # health / recovery signals
        self._reconcile_now = asyncio.Event()
        self._user_started = False  # user WS task launched (live mode)
        self._hb_was_down = False
        self._state_unknown = False  # authoritative REST snapshot is unavailable
        self._chain_lock = asyncio.Lock()  # serialize on-chain txs (nonce safety)

    # ── lifecycle ───────────────────────────────────────────────────────
    async def start(self) -> None:
        self._running = True
        await self.gateway.connect()
        await self._resolve_markets()
        if not self.metas:
            log.warning("no_markets_selected", hint="add markets to config/markets.toml, run `polymaker scan`")
        # freshen reward/fee/end-date params from live Gamma BEFORE quoting so a
        # stale catalog (e.g. old reward min-size) can't mis-size our orders
        await self.refresh_market_metadata()
        await self._startup_reconcile()

        # subscribe feeds
        self.md.set_markets([(cid, [m.yes.token_id, m.no.token_id]) for cid, m in self.metas.items()])
        self.user = UserStream(
            self.gateway.creds, self.gateway.funder, self.user_proc,
            other_token=self._other_token, condition_of_token=self._cid_of_token,
            journal=self.journal, proxy=self.cfg.proxy,
            on_reconnect=self._on_user_reconnect,
        )
        self.user.set_markets(list(self.metas))

        # launch supervised tasks (a dead task is restarted, never silently gone)
        self._spawn("market_ws", self.md.run)
        if not self.paper:
            assert self.user is not None
            self._spawn("user_ws", self.user.run)
            # register the dead-man switch BEFORE any quoter can place an order,
            # so a crash between placing and the first heartbeat still auto-cancels
            with contextlib.suppress(Exception):
                await self.gateway.heartbeat()
            self._spawn("heartbeat", self._heartbeat_loop)
            self._user_started = True
        self._spawn("reconcile", self._reconcile_loop)
        self._spawn("metadata", self._metadata_refresh_loop)
        self._spawn("maintenance", self._maintenance_loop)
        for cid in self.metas:
            self._spawn(f"quote:{cid[:8]}", lambda c=cid: self._quoter(c))
        self._spawn("supervisor", self._supervise)
        log.info("engine_started", markets=len(self.metas), paper=self.paper)

    def _spawn(self, name: str, factory: Any) -> None:
        self._task_specs[name] = factory
        self._tasks[name] = asyncio.create_task(factory(), name=name)

    _supervise_interval_s: float = 5.0

    async def _supervise(self) -> None:
        """Restart any engine task that exits while we're running. Never down."""
        while self._running:
            await asyncio.sleep(self._supervise_interval_s)
            for name, task in list(self._tasks.items()):
                if name == "supervisor" or not task.done():
                    continue
                if not self._running:
                    return
                exc = None
                with contextlib.suppress(asyncio.CancelledError, asyncio.InvalidStateError):
                    exc = task.exception()
                log.critical("task_died_restarting", task=name, err=str(exc) if exc else "exited")
                self.alerter.alert("task_died", f"{name} died: {exc}", critical=True)
                self._tasks[name] = asyncio.create_task(self._task_specs[name](), name=name)

    async def run_forever(self) -> None:
        await self.start()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(*self._tasks.values(), *self._aux_tasks)

    async def shutdown(self) -> None:
        self._running = False
        log.info("engine_shutdown")
        self.md.stop()
        if self.user:
            self.user.stop()
        for t in [*self._tasks.values(), *self._aux_tasks]:
            t.cancel()
        await self._cancel_managed_assets()
        self.gateway.close()
        self.journal.close()
        self.state.close()
        self.catalog.close()

    # ── market resolution ───────────────────────────────────────────────
    async def _resolve_markets(self) -> None:
        reward_rates: dict[str, float] | None = None
        async with GammaClient(self.cfg.wallet.gamma_host) as gamma:
            for entry in self.cfg.enabled_markets:
                meta = self.catalog.get_by_slug(entry.slug) if entry.slug else None
                if meta is None and entry.condition_id:
                    meta = self.catalog.get(entry.condition_id)
                if meta is None:  # fall back to a live Gamma fetch
                    if reward_rates is None:
                        reward_rates = await fetch_reward_rates(self.cfg.wallet.clob_host)
                    meta = await self._fetch_meta(gamma, entry.slug, entry.condition_id, reward_rates)
                if meta is None:
                    log.warning("market_unresolved", ref=entry.ref)
                    continue
                self.metas[meta.condition_id] = meta
                self.profiles[meta.condition_id] = self.cfg.profile_for(entry)
                self.est[meta.condition_id] = self._make_estimators(self.profiles[meta.condition_id])
                self.regime_m[meta.condition_id] = RegimeMachine()
                self._dirty[meta.condition_id] = asyncio.Event()
                self._locks[meta.condition_id] = asyncio.Lock()
                for tok in (meta.yes.token_id, meta.no.token_id):
                    self._token_cid[tok] = meta.condition_id

    async def _fetch_meta(
        self, gamma: GammaClient, slug: str | None, condition_id: str | None,
        reward_rates: dict[str, float],
    ) -> MarketMeta | None:
        tag_id = self.catalog.cached_tag("politics")
        if tag_id is None:  # cold start: resolve + cache so the sweep is scoped
            tag_id = await gamma.resolve_tag_id("politics")
            if tag_id:
                self.catalog.cache_tag("politics", tag_id)
        async for raw in gamma.iter_markets(tag_id=tag_id, max_pages=25):
            if (slug and raw.get("slug") == slug) or (condition_id and raw.get("conditionId") == condition_id):
                m = parse_market(raw, reward_rates)
                if m:
                    self.catalog.upsert_market(m)
                return m
        return None

    @staticmethod
    def _make_estimators(p: StrategyProfile) -> MarketEstimators:
        return MarketEstimators(
            vol=VolEstimator(p.vol_short_halflife_s, p.vol_long_halflife_s),
            flow=FlowEstimator(p.flow_ewma_halflife_s),
            markout=MarkoutTracker(),
        )

    async def _startup_reconcile(self) -> None:
        # Never touch orders outside the configured markets. A single wallet may
        # also contain manual orders or another strategy's orders.
        try:
            if not await self._cancel_managed_assets():
                raise GatewayReadError("managed-token cancellation could not be confirmed")
            self.state.drop_untracked_positions(set(self._token_cid))
            positions_n, _ = await self._reconcile_authoritative_state(startup=True)
            managed_leftover = [o for o in self.state.orders.values() if o.token_id in self._token_cid]
            if managed_leftover:
                self.alerter.alert("startup_orders_stuck",
                                   f"{len(managed_leftover)} managed orders survived cancellation",
                                   critical=True)
                raise GatewayReadError("managed orders remain after startup cancellation")
            for tok in self._token_cid:
                self.state.replace_open_orders(tok, [], grace_s=0.0)
            self.risk.establish_daily_baseline()
            log.info("startup_positions", n=positions_n)
            self._state_unknown = False
        except Exception:
            self._state_unknown = True
            await self._cancel_managed_assets()
            raise

    async def _sync_confirmed_trades(self, full_day: bool = False) -> int:
        now = time.time()
        day_start = _utc_day_start_ts(now)
        pending = self._load_pending_trades()
        self._check_pending_age(pending, now)
        checkpoint = self.state.get_sync_value(_TRADE_SYNC_TS)
        first = self.state.get_sync_value(_TRADE_SYNC_INITIALIZED) != "1"
        after = day_start if first or full_day or not checkpoint else max(
            day_start, int(float(checkpoint)) - _TRADE_SYNC_OVERLAP_S
        )
        if pending:
            after = min(after, math.floor(min(pending.values())) - _TRADE_SYNC_OVERLAP_S)
            log.warning("pending_trade_recovery_window", pending=len(pending), after=after)
        trades = await self.gateway.trades(after=after)
        applied = 0
        events = []
        for payload in trades:
            try:
                if not isinstance(payload, dict):
                    raise ValueError("invalid payload")
                if str(payload.get("status", "")).upper() not in (
                    "MATCHED", "MINED", "RETRYING", "CONFIRMED", "FAILED"
                ):
                    raise ValueError("invalid status")
                owned = [mo for mo in payload.get("maker_orders", [])
                         if str(mo.get("maker_address", "")).lower() == self.gateway.funder.lower()]
                for mo in owned:
                    if not str(payload.get("id") or "").strip():
                        raise ValueError("missing identity")
                    price, size = float(mo["price"]), float(mo["matched_amount"])
                    if not math.isfinite(price) or not 0 < price < 1 or not math.isfinite(size) or size <= 0:
                        raise ValueError("invalid economics")
                    side = mo.get("side") if mo.get("asset_id") and mo.get("side") is not None else payload.get("side")
                    if str(side).upper() not in ("BUY", "SELL"):
                        raise ValueError("invalid side")
                normalized = normalize_trade(payload, self.gateway.funder, self._other_token)
                if len(normalized) != len(owned):
                    raise ValueError("lost owned maker leg")
                for ev in normalized:
                    if not ev.token_id or not math.isfinite(ev.ts) or ev.ts <= 0:
                        raise ValueError("invalid normalized trade")
                    if self._cid_of_token(ev.token_id):
                        events.append(ev)
            except (AttributeError, KeyError, OverflowError, TypeError, ValueError) as exc:
                raise GatewayReadError("invalid confirmed trade") from exc
        # Keep every observed identity pinned until the complete snapshot settles
        # successfully. Crashes or processor conflicts cannot forget older legs.
        for ev in events:
            pending[ev.trade_id] = min(pending.get(ev.trade_id, ev.ts), ev.ts)
        self.state.set_sync_value(_TRADE_SYNC_PENDING, json.dumps(pending, sort_keys=True))
        settled: set[str] = set()
        for ev in events:
            if ev.status not in (TradeState.CONFIRMED, TradeState.FAILED):
                continue
            fill = Fill(ev.token_id, ev.our_side, ev.price, ev.size, ev.trade_id, ev.ts)
            aliases = (ev.legacy_trade_id,) if ev.legacy_trade_id else ()
            accepted = self.user_proc.on_trade(ev, self._token_cid[ev.token_id])
            if ev.status is TradeState.FAILED:
                if not self.state.fill_failure_settled(fill, aliases=aliases):
                    raise GatewayReadError("FAILED trade lacks durable reversal")
            elif not accepted and not self.state.fill_identity_matches(fill, aliases=aliases):
                raise GatewayReadError("confirmed trade identity conflict")
            applied += int(accepted)
            settled.update((ev.trade_id, *aliases))
        remaining = {identity: ts for identity, ts in pending.items() if identity not in settled}
        self.state.set_sync_value(_TRADE_SYNC_PENDING, json.dumps(remaining, sort_keys=True))
        self._check_pending_age(remaining, now)
        self.state.set_sync_value(_TRADE_SYNC_TS, str(int(now)))
        return applied

    def _load_pending_trades(self) -> dict[str, float]:
        try:
            raw = self.state.get_sync_value(_TRADE_SYNC_PENDING)
            pending = json.loads("{}" if raw is None else raw)
            if not isinstance(pending, dict) or any(
                not identity.strip() or type(ts) not in (int, float)
                or not math.isfinite(ts) or ts <= 0 for identity, ts in pending.items()
            ):
                raise ValueError("invalid pending metadata")
            return pending
        except (OverflowError, TypeError, ValueError) as exc:
            raise GatewayReadError("invalid pending trade metadata") from exc

    @staticmethod
    def _check_pending_age(pending: dict[str, float], now: float) -> None:
        if pending and now - min(pending.values()) > _TRADE_SYNC_PENDING_MAX_AGE_S:
            raise GatewayReadError("unresolved trades exceed 7-day recovery window; operator review required")

    async def _reconcile_authoritative_state(self, *, startup: bool = False) -> tuple[int, int]:
        try:
            epoch = self._placement_epoch
            first = self.state.get_sync_value(_TRADE_SYNC_INITIALIZED) != "1"
            onchain_proof = self._load_onchain_proof()
            require_proof = not first or onchain_proof is not None
            if onchain_proof is not None:
                self._state_unknown = True
                self._restore_onchain_exposure(onchain_proof)
            await self._sync_confirmed_trades(full_day=startup or self._state_unknown)
            revision = self.state.fill_count()
            positions = self._only_traded(await self.gateway.positions())
            if epoch != self._placement_epoch:
                raise GatewayReadError("state changed during authoritative snapshot")
            self._assert_snapshot_revision(revision)
            if onchain_proof is not None:
                self._require_onchain_snapshot(onchain_proof, positions)
            if require_proof and not self._positions_explained(positions):
                replayed = await self._sync_confirmed_trades(full_day=True)
                revision += replayed
                if epoch != self._placement_epoch:
                    raise GatewayReadError("state changed during authoritative snapshot")
                self._assert_snapshot_revision(revision)
                if not self._positions_explained(positions):
                    for tok in self._token_cid:
                        self.state.set_position(tok, *positions.get(tok, (0.0, 0.0)))
                    raise GatewayReadError("confirmed trades do not explain positions")
            live = await self.gateway.open_orders()
            try:
                for order in live:
                    if order.token_id in self._token_cid:
                        _finite_float(order.price)
                        _finite_float(order.size)
            except (OverflowError, TypeError, ValueError) as exc:
                raise GatewayReadError("invalid open-orders snapshot") from exc
            async with contextlib.AsyncExitStack() as locks:
                for cid in self.metas:
                    await locks.enter_async_context(self._locks[cid])
                await locks.enter_async_context(self._placement_lock)
                # No await between this final proof and publishing the snapshot.
                if epoch != self._placement_epoch:
                    raise GatewayReadError("state changed during authoritative snapshot")
                if self._load_onchain_proof() != onchain_proof:
                    raise GatewayReadError("retained on-chain proof changed during authoritative snapshot")
                self._assert_snapshot_revision(revision)
                if first and (self._load_pending_trades() or any(
                    self.state.inflight(tok) for tok in self._token_cid
                )):
                    raise GatewayReadError("pending trades prevent ledger initialization")
                if require_proof and not self._positions_explained(positions):
                    raise GatewayReadError("confirmed trades do not explain positions")
                ledger = self.state.fill_position_sizes()
                for meta in self.metas.values():
                    for tok in (meta.yes.token_id, meta.no.token_id):
                        if first or self.state.inflight(tok) == 0:
                            self.state.set_position(tok, *positions.get(tok, (0.0, 0.0)))
                            self.state.replace_open_orders(tok, [o for o in live if o.token_id == tok])
                        if first:
                            self.state.set_sync_value(
                                f"confirmed_trade_baseline:{tok}",
                                str(positions.get(tok, (0.0, 0.0))[0] - ledger.get(tok, 0.0)),
                            )
                self.risk.reconcile_cash_ledger()
                self.state.set_sync_value(_TRADE_SYNC_INITIALIZED, "1")
                self.state.set_sync_value(_TRADE_SYNC_REQUIRE_PROOF, "0")
                self._state_unknown = False
            return len(positions), len(live)
        except Exception:
            self._state_unknown = True
            await self._cancel_managed_assets()
            raise

    def _load_onchain_proof(self) -> dict[str, float] | None:
        def unique_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            fields = dict(pairs)
            if len(fields) != len(pairs):
                raise ValueError("duplicate on-chain proof token")
            return fields

        raw = self.state.get_sync_value(_TRADE_SYNC_REQUIRE_PROOF)
        if raw is None or raw == "0":
            return None
        try:
            values = json.loads(raw, object_pairs_hook=unique_fields)
            if not isinstance(values, dict) or not values or any(
                tok not in self._token_cid for tok in values
            ):
                raise ValueError("missing or unconfigured on-chain proof")
            return {tok: _onchain_size(size) for tok, size in values.items()}
        except (OverflowError, TypeError, ValueError) as exc:
            raise GatewayReadError("invalid or missing retained on-chain proof") from exc

    def _restore_onchain_exposure(self, proof: dict[str, float]) -> None:
        for tok, size in proof.items():
            self.state.set_position(tok, size, self.state.position(tok).avg_price)

    def _require_onchain_snapshot(
        self, proof: dict[str, float], positions: dict[str, tuple[float, float]],
    ) -> None:
        if not all(math.isclose(
            positions.get(tok, (0.0, 0.0))[0], size, rel_tol=0.0, abs_tol=1e-6,
        ) for tok, size in proof.items()):
            self._restore_onchain_exposure(proof)
            raise GatewayReadError("positions disagree with retained on-chain proof")

    def _assert_snapshot_revision(self, revision: int) -> None:
        if self.state.fill_count() != revision:
            raise GatewayReadError("fill ledger changed during authoritative snapshot")

    def _positions_explained(self, positions: dict[str, tuple[float, float]]) -> bool:
        ledger = self.state.fill_position_sizes()
        return all(
            authoritative_position_matches(
                ledger.get(tok, 0.0) + float(self.state.get_sync_value(
                    f"confirmed_trade_baseline:{tok}"
                ) or "0"),
                positions.get(tok, (0.0, 0.0))[0],
            ) for tok in self._token_cid
        )

    async def _cancel_managed_assets(self) -> bool:
        """Cancel only orders on tokens owned by this engine instance."""
        return await self._cancel_assets([
            token.token_id for meta in self.metas.values() for token in (meta.yes, meta.no)
        ])

    async def _cancel_assets(self, tokens: list[str]) -> bool:
        # Invalidate waiters before waiting for an already-submitted placement.
        # Cancellation never acquires market or reservation locks.
        self._placement_epoch += 1
        ok = True
        async with self._placement_lock:
            for tok in tokens:
                try:
                    cancelled = await self.gateway.cancel_asset(tok)
                    ok = cancelled and ok
                    if cancelled:
                        for order in self.state.orders_for(tok):
                            self.state.remove_order(order.order_id)
                except Exception as exc:  # noqa: BLE001
                    ok = False
                    log.critical("managed_cancel_failed", token=tok[:12], err=str(exc))
        if not ok:
            self._state_unknown = True
            self.alerter.alert("managed_cancel_failed",
                               "could not confirm cancellation for a managed token",
                               critical=True)
        return ok

    def _apply_authoritative_positions(self, positions: dict[str, tuple[float, float]]) -> None:
        """Apply a successful funder snapshot, including explicit zeroes."""
        self.state.reconcile_positions(positions)
        for tok in self._token_cid:
            if tok not in positions and self.state.inflight(tok) == 0:
                self.state.set_position(tok, 0.0, 0.0)

    def _only_traded(self, positions: dict[str, tuple[float, float]]) -> dict[str, tuple[float, float]]:
        """Scope account positions to tokens WE trade. Manual/UI positions in
        other markets are the operator's business — they must not enter our
        state, exposure caps, or PnL."""
        try:
            return {
                t: (_finite_float(v[0]), _finite_float(v[1]))
                for t, v in positions.items() if t in self._token_cid
            }
        except (OverflowError, TypeError, ValueError) as exc:
            raise GatewayReadError("invalid positions snapshot") from exc

    # ── callbacks ───────────────────────────────────────────────────────
    def _on_dirty(self, condition_id: str, token_id: str) -> None:
        ev = self._dirty.get(condition_id)
        if ev is not None:
            ev.set()

    def _wake_cid(self, condition_id: str) -> None:
        ev = self._dirty.get(condition_id)
        if ev is not None:
            ev.set()

    def _wake_all(self) -> None:
        for ev in self._dirty.values():
            ev.set()

    def _on_user_reconnect(self) -> None:
        """User WS reconnected: events during the gap were lost — force an
        immediate REST reconcile before trusting our state again."""
        log.warning("user_ws_reconnected_forcing_reconcile")
        self._reconcile_now.set()

    def _on_trade(self, tp: TradePrint) -> None:
        cid = self._token_cid.get(tp.asset_id)
        if cid is None:
            return
        p = self.profiles[cid]
        self.est[cid].flow.update(tp.aggressor, tp.size, tp.ts)
        # A trade only flags a SWEEP (-> pull quotes) if it's genuinely toxic:
        # large in absolute terms AND large relative to the resting depth it
        # consumed (i.e. it actually ate through the book). A big trade absorbed
        # by a deep book doesn't move the price and isn't toxic — for a liquid
        # market the FV-jump detector is the real event signal. event_sweep_mult
        # sets how many order-sizes big the print must be to even be considered.
        base = p.base_size_usdc / max(tp.price, 0.01)
        if tp.size < p.event_sweep_mult * base:
            return
        book = self.md.book(tp.asset_id)
        if book is None:
            return
        bb, ba = book.best_bid(), book.best_ask()
        if bb is None or ba is None:
            return
        # aggressor BUY lifts asks; SELL hits bids — measure the side it consumed
        if tp.aggressor is Side.BUY:
            consumed = book.depth_within(Side.SELL, ba.price, ba.price + 3 * book.tick_size)
        else:
            consumed = book.depth_within(Side.BUY, bb.price - 3 * book.tick_size, bb.price)
        if consumed > 0 and tp.size >= p.event_sweep_frac * consumed:
            self._sweep[cid] = True

    def _prepare_fill(self) -> None:
        self.risk.prepare_fill()
        if self.state.get_sync_value(_TRADE_SYNC_INITIALIZED) == "1":
            # A new-day restart has known pre-fill holdings. Initial migration
            # still establishes its baseline after repairing the old snapshot.
            self.risk.establish_daily_baseline()

    def _on_fill(self, fill: Fill) -> None:
        self.risk.note_fill(fill)
        if fill.trade_id.endswith(":reverse"):
            return
        cid = self._token_cid.get(fill.token_id)
        if cid is None:
            return
        est = self.est[cid]
        fv = est.last_fv if est.last_fv is not None else fill.price
        token_fv = fv if fill.token_id == self.metas[cid].yes.token_id else (1.0 - fv)
        est.markout.record_fill(fill.side, token_fv, fill.ts)

    # ── quoter ──────────────────────────────────────────────────────────
    async def _quoter(self, cid: str) -> None:
        debounce = self.cfg.engine.debounce_ms / 1000.0
        base_tick = self.cfg.engine.quoter_tick_s
        ev = self._dirty[cid]
        while self._running:
            try:
                # Book/fill events wake us instantly. Otherwise we refresh on a
                # slow baseline tick, EXCEPT: if an EVENT cool-off is active,
                # wake precisely when it ends (re-enter promptly, not up to a
                # minute late); if we're holding inventory, tick faster to walk
                # exit urgency.
                timeout = self._next_wake_s(cid, base_tick)
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(ev.wait(), timeout=timeout)
                if ev.is_set():
                    await asyncio.sleep(debounce)  # coalesce a burst of updates
                ev.clear()
                await self._recompute(cid)
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001
                log.error("quoter_error", cid=cid[:8], err=str(exc))
                await asyncio.sleep(0.5)

    def _next_wake_s(self, cid: str, base_tick: float) -> float:
        now = time.time()
        wake = base_tick
        rm = self.regime_m.get(cid)
        if rm is not None:
            cd = rm.cooloff_remaining(now)
            if cd > 0:
                wake = min(wake, cd + 0.5)  # re-enter right when cool-off ends
        meta = self.metas.get(cid)
        if meta is not None:  # holding inventory -> tick faster to manage exits
            held = self.state.position(meta.yes.token_id).size + self.state.position(meta.no.token_id).size
            if held >= meta.min_order_size:
                wake = min(wake, 10.0)
        return max(1.0, wake)

    async def _recompute(self, cid: str) -> None:
        lock = self._locks.get(cid)
        if lock is None:
            return
        async with lock:  # serialize vs the reconcile loop mutating this market
            await self._recompute_locked(cid)

    async def _recompute_locked(self, cid: str) -> None:
        epoch = self._placement_epoch
        meta = self.metas[cid]
        p = self.profiles[cid]
        yes_book = self.md.book(meta.yes.token_id)
        no_book = self.md.book(meta.no.token_id)
        if yes_book is None or yes_book.is_empty:
            return

        # crossed/locked or one-sided book -> FV is unreliable; skip this tick
        bb, ba = yes_book.best_bid(), yes_book.best_ask()
        if bb is None or ba is None or bb.price >= ba.price:
            return

        now = time.time()
        micro = yes_book.microprice(p.micro_levels)
        if micro is None:
            return
        est = self.est[cid]
        est.flow.decay_to(now)
        fv = compute_fair_value(micro, est.flow.z, meta.tick_size)
        prev_fv = est.last_fv
        est.on_fair_value(fv, now)

        self.risk.update_mark(meta.yes.token_id, fv)
        self.risk.update_mark(meta.no.token_id, 1.0 - fv)

        pos_yes = self.state.position(meta.yes.token_id)
        pos_no = self.state.position(meta.no.token_id)
        q_max = p.q_max_usdc
        inv_util = abs(pos_yes.size - pos_no.size) * fv / q_max if q_max > 0 else 0.0
        hours_to_end = _hours_to_end(meta.end_date_iso, now)

        # ── blind/stale conditions ──────────────────────────────────────────
        # A QUIET market with a live WS link is NOT stale — the CLOB WS pings
        # every 5s (pong-timeout 10s), so a dead link flips `connected` within
        # ~15s. Gating on the connection (not book-mutation recency) stops a
        # legitimately-quiet thin market from false-halting into zero rewards.
        market_stale = (
            not self.md.connected
            and self.md.disconnected_since > 0.0
            and (now - self.md.disconnected_since) > self.cfg.risk.ws_stale_halt_s
        )
        user_blind = (
            self._user_started
            and self.user is not None
            and not self.user.connected
            and (now - self.user.disconnected_since) > self.cfg.risk.user_ws_blind_halt_s
        )
        hb_blind = (
            not self.paper
            and self.cfg.engine.heartbeat
            and self.gateway.heartbeat_failures >= self.cfg.risk.heartbeat_halt_failures
        )
        halted = cid in self._halted
        blind = market_stale or user_blind or hb_blind or halted
        blind = blind or self._state_unknown
        if blind:
            log.warning("market_blind", cid=cid[:8], market_stale=market_stale,
                        user_blind=user_blind, hb_blind=hb_blind, halted=halted)
            self.alerter.alert(
                f"blind:{cid[:8]}",
                f"{meta.question[:40]} blind (stale={market_stale} user={user_blind} "
                f"hb={hb_blind} halted={halted})",
                critical=hb_blind,
            )

        rd = self.risk.evaluate(meta, ws_stale=blind,
                                event_group_cost=self._event_group_cost(meta))
        if rd.halt and rd.reason not in ("ws_stale",):
            self.alerter.alert(
                f"risk_halt:{rd.reason}", f"risk halt: {rd.reason}",
                critical=any(k in rd.reason for k in ("daily_loss", "kill", "error_rate")),
            )
        ws_stale = blind
        regime = self.regime_m[cid].decide(
            RegimeInputs(
                now=now, tick=meta.tick_size, fv=fv, prev_fv=prev_fv,
                vol_ratio=est.vol.ratio, flow_z=est.flow.z, inventory_util=inv_util,
                hours_to_end=hours_to_end, sweep_flagged=self._sweep.pop(cid, False),
                ws_stale=ws_stale, risk_halt=rd.halt, risk_reduce_only=rd.reduce_only,
            ),
            p,
        )

        tq = construct_quotes(QuoteInputs(
            meta=meta, regime=regime, fv=fv, vol_short=est.vol.short,
            toxicity=est.markout.toxicity, yes_view=yes_book.view(),
            no_view=(no_book.view() if no_book else _empty_view()),
            pos_yes=pos_yes, pos_no=pos_no, profile=p, now=now,
            risk_size_scale=rd.size_scale,
        ))

        live = self.state.orders_for(meta.yes.token_id) + self.state.orders_for(meta.no.token_id)
        plan = reconcile(tq, live, tick=meta.tick_size,
                         reprice_ticks=p.reprice_ticks, resize_frac=p.resize_frac)
        if plan.is_noop:
            self._maybe_merge(cid, meta, p, pos_yes.size, pos_no.size)
            return

        if plan.to_cancel:
            ok = await self.gateway.cancel(plan.to_cancel)
            if ok:
                for oid in plan.to_cancel:
                    self.state.remove_order(oid)
            else:
                # cancel MAY have partially applied server-side — keep our view,
                # resync from REST, and skip placing this cycle (avoid doubles)
                try:
                    await self._refresh_token_orders(meta, grace_s=10.0)
                except GatewayReadError as exc:
                    self._state_unknown = True
                    self.alerter.alert("state_unknown", str(exc), critical=True)
                    log.critical("state_unknown_after_cancel_failure", err=str(exc))
                self._dirty[cid].set()
                return
        placed_n = 0
        if plan.to_place:
            async with self._reservation_lock:
                if not self._placement_allowed(cid, epoch):
                    return
                fitted = self.risk.fit_reservation(
                    meta, plan.to_place, event_group_cost=self._event_group_cost(meta)
                )
                if not fitted:
                    log.warning("risk_reservation_rejected", cid=cid[:8], n=len(plan.to_place))
                    plan = type(plan)(to_cancel=plan.to_cancel, to_place=[])
                else:
                    if len(fitted) != len(plan.to_place) or any(
                        a.size != b.size for a, b in zip(fitted, plan.to_place, strict=False)
                    ):
                        log.info("risk_reservation_scaled", cid=cid[:8],
                                 requested=len(plan.to_place), fitted=len(fitted))
                    plan = type(plan)(to_cancel=plan.to_cancel, to_place=fitted)
                    # LOAD SHED: under rate-budget pressure, skip *new* quotes in calm
                    # regimes (cancels/exits above already ran) so we don't inject latency
                    # right when the book is busy. Risk regimes always place.
                    shed = (
                        not self.paper
                        and self.gateway.order_pressure > 0.85
                        and regime in (Regime.QUIET, Regime.TRENDING)
                    )
                    if shed:
                        log.warning("shed_load", cid=cid[:8], pressure=round(self.gateway.order_pressure, 2))
                        self._dirty[cid].set()  # retry soon
                    else:
                        async with self._placement_lock:
                            if not self._placement_allowed(cid, epoch):
                                return
                            placed = await self.gateway.place(
                                plan.to_place, meta,
                                can_place=lambda: self._placement_allowed(cid, epoch),
                            )
                            placed_n = len(placed)
                            for o in placed:
                                self.state.upsert_order(o)
                            valid = self._placement_allowed(cid, epoch)
                            if valid:
                                self.risk.note_order_result(len(placed) == len(plan.to_place))
                        if not valid:
                            await self._quarantine(meta, reason="placement_invalidated")
                            return
                        if len(placed) < len(plan.to_place):
                            # QUARANTINE: a failed/partial batch may still have posted
                            # orders we don't have ids for. Cancel everything on these
                            # tokens (idempotent) and resync — never risk an untracked order.
                            await self._quarantine(meta, reason="place_incomplete")
        self._last_quote_fv[cid] = fv
        log.info("requote", cid=cid[:8], regime=regime.value, fv=round(fv, 4),
                 place=placed_n, cancel=len(plan.to_cancel),
                 pos_yes=round(pos_yes.size, 1), pos_no=round(pos_no.size, 1),
                 tox=round(est.markout.toxicity, 3), flowz=round(est.flow.z, 2))
        self._maybe_merge(cid, meta, p, pos_yes.size, pos_no.size)

    def _placement_allowed(self, cid: str, epoch: int) -> bool:
        return (
            epoch == self._placement_epoch and self._running and not self._state_unknown
            and cid not in self._halted and not self.risk.global_halt()[0]
        )

    async def _quarantine(self, meta: MarketMeta, reason: str) -> None:
        """Cancel all orders on a market's tokens and resync state from REST."""
        log.warning("quarantine", cid=meta.condition_id[:8], reason=reason)
        self._state_unknown = True
        self._reconcile_now.set()
        if not await self._cancel_assets([meta.yes.token_id, meta.no.token_id]):
            return
        try:
            await self._refresh_token_orders(meta)
        except GatewayReadError as exc:
            self._state_unknown = True
            self.alerter.alert("state_unknown", str(exc), critical=True)
            log.critical("state_unknown_after_quarantine", err=str(exc))

    async def _refresh_token_orders(self, meta: MarketMeta, grace_s: float = 0.0) -> None:
        """Open-orders resync for one market's tokens (grace_s=0 = authoritative)."""
        live = await self.gateway.open_orders()
        for tok in (meta.yes.token_id, meta.no.token_id):
            self.state.replace_open_orders(
                tok, [o for o in live if o.token_id == tok], grace_s=grace_s
            )

    def _maybe_merge(self, cid: str, meta: MarketMeta, p: StrategyProfile,
                     yes_size: float, no_size: float) -> None:
        amount = min(yes_size, no_size)
        if amount < p.merge_min_size or cid in self._merging or self.paper or self._state_unknown:
            return
        self._merging.add(cid)
        self._aux_tasks.append(asyncio.create_task(self._merge_task(cid, meta, amount)))

    async def _merge_task(self, cid: str, meta: MarketMeta, amount: float) -> None:
        try:
            # serialize all on-chain txs so concurrent merges can't reuse a nonce;
            # read on-chain balances as source of truth for the mergeable amount
            async with self._chain_lock:
                bals = await self.gateway.token_balances([meta.yes.token_id, meta.no.token_id])
                if bals:
                    amount = min(amount, bals.get(meta.yes.token_id, 0.0),
                                 bals.get(meta.no.token_id, 0.0))
                raw = int(amount * 1e6)
                if raw <= 0:
                    return
                await asyncio.to_thread(self.merger.merge, meta.condition_id, raw, meta.neg_risk)
        finally:
            self._merging.discard(cid)

    # ── background loops ────────────────────────────────────────────────
    async def _heartbeat_loop(self) -> None:
        if not self.cfg.engine.heartbeat:
            return
        halt_after = self.cfg.risk.heartbeat_halt_failures
        while self._running:
            ok = await self.gateway.heartbeat()
            if not ok and self.gateway.heartbeat_failures >= halt_after and not self._hb_was_down:
                # exchange is (or soon will be) auto-cancelling everything we
                # have live; recompute will see hb_blind and pull quotes
                self._hb_was_down = True
                log.critical("heartbeat_down_halting", failures=self.gateway.heartbeat_failures)
                self._wake_all()
            elif ok and self._hb_was_down:
                # recovered: our server-side orders were wiped — drop local
                # order state, resync authoritatively, then resume quoting
                self._hb_was_down = False
                log.warning("heartbeat_recovered_resyncing")
                self.state.clear_orders()
                for meta in self.metas.values():
                    try:
                        await self._refresh_token_orders(meta, grace_s=0.0)
                    except GatewayReadError as exc:
                        self._state_unknown = True
                        log.critical("heartbeat_resync_failed", err=str(exc))
                self._wake_all()
            await asyncio.sleep(self.cfg.engine.heartbeat_interval_s)

    async def _reconcile_loop(self) -> None:
        rounds = 0
        read_failure_streak = 0
        while self._running:
            # periodic cadence, but wake immediately when a reconnect/recovery
            # demands an urgent resync
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    self._reconcile_now.wait(),
                    timeout=self.cfg.engine.reconcile_interval_s,
                )
            forced = self._reconcile_now.is_set()
            self._reconcile_now.clear()
            rounds += 1
            try:
                # a MATCHED whose settlement event was lost would block a token's
                # reconciliation forever — expire stale in-flight guards first
                expired = self.state.expire_inflight(self.cfg.engine.reconcile_interval_s * 2)
                if expired:
                    self.alerter.alert("inflight_expired",
                                       f"{len(expired)} stuck in-flight guards cleared")

                positions_n, orders_n = await self._reconcile_authoritative_state()
                # The entire cycle fails closed, including local durable writes.
                if rounds % 4 == 0:
                    await self._check_position_divergence()
                self.state.record_pnl(self.risk.equity, self.risk.net_cash,
                                      self.risk.inventory_value, self.risk.daily_pnl)
                if rounds % 20 == 0:
                    self.state.checkpoint_wal()
                if forced and not self._state_unknown:
                    log.info("forced_reconcile_done", positions=positions_n,
                             open_orders=orders_n)
                    self._wake_all()
                read_failure_streak = 0
            except Exception as exc:
                self._state_unknown = True
                await self._cancel_managed_assets()
                read_failure_streak += 1
                self.alerter.alert("state_unknown", str(exc), critical=True)
                log.critical("state_unknown", err=str(exc))
                base = max(2.0, self.cfg.engine.reconcile_interval_s)
                delay = min(60.0, base * (2 ** min(read_failure_streak - 1, 5)))
                log.warning("reconcile_backoff", failures=read_failure_streak,
                            delay_s=round(delay, 1))
                await asyncio.sleep(delay)

    async def _check_position_divergence(self) -> None:
        """Compare internal positions to on-chain truth; alert + correct on drift.

        Catches subtle fill-attribution bugs before they compound. On-chain is
        authoritative (it's what the exchange settles), so we correct to it —
        but only for tokens with no in-flight trades (optimistic state is newer).
        """
        diverged = False
        try:
            tokens = [t for t in self._token_cid if self.state.inflight(t) == 0]
            onchain = await self.gateway.token_balances(tokens)
            # Merge against the latest durable row after the network wait.
            proof = self._load_onchain_proof()
            if proof is not None:
                self._state_unknown = True
            if not onchain:
                return
            try:
                observed = {tok: _onchain_size(size) for tok, size in onchain.items()
                            if tok in tokens and not self.state.inflight(tok)}
            except (OverflowError, TypeError, ValueError) as exc:
                raise GatewayReadError("invalid on-chain exposure snapshot") from exc
            corrections = {}
            for tok, chain_size in observed.items():
                internal = self.state.position(tok).size
                changed_proof = proof is not None and tok in proof and not math.isclose(
                    proof[tok], chain_size, rel_tol=0.0, abs_tol=1e-6,
                )
                if changed_proof or abs(internal - chain_size) > max(1.0, 0.02 * chain_size):
                    corrections[tok] = chain_size
            if not corrections:
                return
            diverged = True
            self._state_unknown = True
            retained = dict(proof) if proof is not None else {}
            retained.update(corrections)
            # One committed row holds every unresolved correction before any
            # position write. Partial later reads never drop earlier proof.
            self.state.set_sync_value(
                _TRADE_SYNC_REQUIRE_PROOF, json.dumps(retained, sort_keys=True, allow_nan=False),
            )
            for tok, chain_size in corrections.items():
                internal = self.state.position(tok).size
                log.error("position_divergence", token=tok[:12],
                          internal=round(internal, 2), onchain=round(chain_size, 2))
                self.alerter.alert(
                    f"divergence:{tok[:8]}",
                    f"position drift: internal {internal:.1f} vs on-chain {chain_size:.1f}",
                    critical=True,
                )
                self.state.force_set_position(tok, chain_size, self.state.position(tok).avg_price,
                                              source="onchain")
        except Exception:
            diverged = True
            self._state_unknown = True
            raise
        finally:
            if diverged:
                self._reconcile_now.set()
                await self._cancel_managed_assets()

    async def refresh_market_metadata(self) -> None:
        """Pull fresh metadata from Gamma for all traded markets: halt on
        closed/not-accepting, and freshen reward/fee/end-date params so we quote
        at the CURRENT reward minimum, band, and fees (these change over time —
        e.g. the reward min-size jumping 50->100 shares). Called at startup and
        periodically. Safe to await."""
        if not self.metas:
            return
        try:
            async with GammaClient(self.cfg.wallet.gamma_host) as gamma:
                raws = await gamma.markets_by_condition(list(self.metas))
        except Exception as exc:  # noqa: BLE001
            log.warning("metadata_refresh_error", err=str(exc))
            return
        for cid, raw in raws.items():
            if cid not in self.metas:
                continue
            accepting = bool(raw.get("acceptingOrders", True))
            closed = bool(raw.get("closed", False))
            if closed or not accepting:
                if cid not in self._halted:
                    self._halted.add(cid)
                    log.critical("market_halted_by_meta", cid=cid[:8], closed=closed,
                                 accepting=accepting)
                    self.alerter.alert(f"halted:{cid[:8]}",
                                       f"{self.metas[cid].question[:40]} closed/not-accepting",
                                       critical=True)
                    meta = self.metas[cid]
                    for tok in (meta.yes.token_id, meta.no.token_id):
                        with contextlib.suppress(Exception):
                            await self.gateway.cancel_asset(tok)
                    self._wake_cid(cid)
                continue
            self._halted.discard(cid)
            self._apply_meta_refresh(cid, raw)

    def _apply_meta_refresh(self, cid: str, raw: dict[str, Any]) -> None:
        import dataclasses

        old = self.metas[cid]
        fee = raw.get("feeSchedule") or {}
        rate = _fnum(fee.get("rate"))
        candidates: dict[str, Any] = {
            "rewards_min_size": _fnum(raw.get("rewardsMinSize")),
            "rewards_max_spread": _fnum(raw.get("rewardsMaxSpread")),
            "taker_fee_bps": int(round(rate * 10000)) if rate is not None else None,
            "rebate_rate": _fnum(fee.get("rebateRate")),
            "end_date_iso": raw.get("endDate"),
            "min_order_size": _fnum(raw.get("orderMinSize")),
        }
        updates = {k: v for k, v in candidates.items()
                   if v is not None and getattr(old, k) != v}
        if updates:
            self.metas[cid] = dataclasses.replace(old, **updates)
            log.info("meta_refreshed", cid=cid[:8], **updates)
            self._wake_cid(cid)

    async def _metadata_refresh_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.cfg.engine.catalog_refresh_s)
            await self.refresh_market_metadata()

    async def _maintenance_loop(self) -> None:
        """Periodic REST book refresh to catch any silently-missed WS deltas."""
        while self._running:
            await asyncio.sleep(120.0)
            for meta in list(self.metas.values()):
                for tok in (meta.yes.token_id, meta.no.token_id):
                    with contextlib.suppress(Exception):
                        await self._refresh_book(tok)

    async def _refresh_book(self, token_id: str) -> None:
        levels = await self.gateway.get_full_book(token_id)
        if levels is None:
            return
        bids, asks, book_hash = levels
        book = self.md.book(token_id)
        if book is None:
            return
        # drift check: only overwrite if the REST top-of-book disagrees with ours
        cur_bb = book.best_bid()
        cur_ba = book.best_ask()
        rest_bb = max((p for p, _ in bids), default=None)
        rest_ba = min((p for p, _ in asks), default=None)
        drift = (
            (cur_bb is None) != (rest_bb is None)
            or (cur_ba is None) != (rest_ba is None)
            or (cur_bb and rest_bb and abs(cur_bb.price - rest_bb) > book.tick_size)
            or (cur_ba and rest_ba and abs(cur_ba.price - rest_ba) > book.tick_size)
        )
        if drift:
            log.warning("book_drift_corrected", token=token_id[:12])
            book.apply_snapshot(bids, asks, time.time(), book_hash)
            cid = self._token_cid.get(token_id)
            if cid:
                self._wake_cid(cid)

    # ── helpers ─────────────────────────────────────────────────────────
    def _other_token(self, token_id: str) -> str | None:
        cid = self._token_cid.get(token_id)
        return self.metas[cid].other_token(token_id) if cid else None

    def _cid_of_token(self, token_id: str) -> str | None:
        return self._token_cid.get(token_id)

    def _event_group_cost(self, meta: MarketMeta) -> float:
        if not meta.event_id:
            return 0.0
        cost = 0.0
        for m in self.metas.values():
            if m.event_id == meta.event_id:
                for tok in (m.yes.token_id, m.no.token_id):
                    cost += self.risk.marked_position_notional(tok)
                    cost += sum(
                        o.notional for o in self.state.orders_for(tok) if o.side is Side.BUY
                    )
        return cost


def _onchain_size(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("invalid on-chain size type")
    size = _finite_float(value)
    if size < 0:
        raise ValueError("negative on-chain size")
    return size


def _finite_float(value: float) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("nonfinite authoritative number")
    return number


def _fnum(v: object) -> float | None:
    if v is None:
        return None
    try:
        return float(v)  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return None


def _hours_to_end(end_date_iso: str | None, now: float) -> float | None:
    if not end_date_iso:
        return None
    try:
        dt = datetime.fromisoformat(end_date_iso.replace("Z", "+00:00"))
        hrs = (dt.timestamp() - now) / 3600.0
        # A past end date on a still-trading market is a stale/placeholder date
        # (common for "next X" appointment markets) — treat as unknown so we
        # don't wrongly HALT. The true end is signalled by acceptingOrders=False,
        # which the metadata refresh already halts on.
        return hrs if hrs > 0.0 else None
    except (ValueError, TypeError):
        return None


def _empty_view() -> Any:
    from polymaker.marketdata.orderbook import BookView

    return BookView(None, 0.0, None, 0.0, None, None, 0.0, 0.0)


def _paper_state_path(db_path: str) -> str:
    if db_path == ":memory:":
        return db_path
    path = Path(db_path)
    suffix = path.suffix or ".db"
    return str(path.with_name(f"{path.stem}.paper{suffix}"))
