"""Deterministic offline replay for captured market-data journals.

The simulator deliberately uses conservative maker-fill assumptions: an order
must be traded through, or observed volume at its price must first consume the
configured queue ahead. Reward income is an L2-based estimate and is always
reported separately from trading PnL.
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from polymaker.config import Config, StrategyProfile
from polymaker.domain import Fill, MarketMeta, OpenOrder, OrderState, Quote, Side, TargetQuotes
from polymaker.execution.reconciler import reconcile
from polymaker.marketdata.orderbook import OrderBook
from polymaker.marketdata.parse import parse_book, parse_last_trade, parse_price_changes
from polymaker.risk.manager import RiskManager
from polymaker.state.store import StateStore
from polymaker.strategy.estimators import (
    FlowEstimator,
    MarketEstimators,
    MarkoutTracker,
    VolEstimator,
)
from polymaker.strategy.quoting import (
    QuoteInputs,
    compute_exit_urgency,
    compute_fair_value,
    construct_quotes,
)
from polymaker.strategy.regime import RegimeInputs, RegimeMachine


class BacktestError(RuntimeError):
    """The journal or market metadata is insufficient for a valid replay."""


@dataclass(frozen=True, slots=True)
class ReplayOptions:
    queue_ahead_fraction: float = 1.0
    quote_latency_ms: float = 250.0
    markout_seconds: float = 300.0
    max_reward_gap_seconds: float = 60.0

    def __post_init__(self) -> None:
        if self.queue_ahead_fraction < 0:
            raise ValueError("queue_ahead_fraction must be non-negative")
        if self.quote_latency_ms < 0 or self.markout_seconds <= 0:
            raise ValueError("latency must be non-negative and markout horizon positive")
        if self.max_reward_gap_seconds <= 0:
            raise ValueError("max_reward_gap_seconds must be positive")


@dataclass(frozen=True, slots=True)
class JournalEvent:
    ts: float
    kind: str
    data: Any
    sequence: int


@dataclass(frozen=True, slots=True)
class BacktestResult:
    start_ts: float
    end_ts: float
    duration_seconds: float
    events: int
    malformed_lines: int
    markets: int
    placed_orders: int
    filled_orders: int
    fills: int
    fill_probability: float
    filled_notional: float
    cash_flow: float
    inventory_value: float
    trading_mtm_pnl: float
    maker_rebate_estimate: float
    liquidity_reward_estimate: float
    total_pnl_estimate: float
    max_capital_at_risk: float
    max_drawdown: float
    mean_markout_bps: float | None
    unresolved_markouts: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class _QueuedOrder:
    order: OpenOrder
    queue_ahead: float


@dataclass(slots=True)
class _PendingMarkout:
    token_id: str
    side: Side
    fv_at_fill: float
    size: float
    due_ts: float


def load_journal(path: str | Path) -> tuple[list[JournalEvent], int]:
    events: list[JournalEvent] = []
    malformed = 0
    with Path(path).open(encoding="utf-8") as handle:
        for sequence, line in enumerate(handle):
            if not line.strip():
                continue
            try:
                raw = json.loads(line)
                ts = float(raw["ts"])
                kind = str(raw["kind"])
                data = raw["data"]
                if not math.isfinite(ts):
                    raise ValueError("invalid event")
                events.append(JournalEvent(ts, kind, data, sequence))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                malformed += 1
    events.sort(key=lambda event: (event.ts, event.sequence))
    return events, malformed


def configured_markets(cfg: Config) -> tuple[dict[str, MarketMeta], dict[str, StrategyProfile]]:
    from polymaker.catalog.store import CatalogStore

    catalog = CatalogStore(cfg.paths.db)
    metas: dict[str, MarketMeta] = {}
    profiles: dict[str, StrategyProfile] = {}
    missing: list[str] = []
    try:
        for entry in cfg.enabled_markets:
            meta = catalog.get_by_slug(entry.slug) if entry.slug else None
            if meta is None and entry.condition_id:
                meta = catalog.get(entry.condition_id)
            if meta is None:
                missing.append(entry.ref)
                continue
            metas[meta.condition_id] = meta
            profiles[meta.condition_id] = cfg.profile_for(entry)
    finally:
        catalog.close()
    if missing:
        raise BacktestError("market metadata missing; run `polymaker scan`: " + ", ".join(missing))
    if not metas:
        raise BacktestError("no enabled markets configured")
    return metas, profiles


class JournalBacktester:
    def __init__(
        self,
        cfg: Config,
        metas: dict[str, MarketMeta],
        profiles: dict[str, StrategyProfile],
        options: ReplayOptions | None = None,
    ) -> None:
        self.cfg = cfg
        self.metas = metas
        self.profiles = profiles
        self.options = options or ReplayOptions()
        self.token_market = {
            token: cid
            for cid, meta in metas.items()
            for token in (meta.yes.token_id, meta.no.token_id)
        }
        self.books = {
            token: OrderBook(meta.tick_size)
            for meta in metas.values()
            for token in (meta.yes.token_id, meta.no.token_id)
        }
        self.state = StateStore(":memory:")
        self.risk = RiskManager(cfg.risk, self.state)
        self.risk.establish_daily_baseline()
        self.estimators = {
            cid: MarketEstimators(
                VolEstimator(profile.vol_short_halflife_s, profile.vol_long_halflife_s),
                FlowEstimator(profile.flow_ewma_halflife_s),
                MarkoutTracker(self.options.markout_seconds),
            )
            for cid, profile in profiles.items()
        }
        self.regimes = {cid: RegimeMachine() for cid in metas}
        self.sweep_flagged: set[str] = set()
        self.queued: dict[str, _QueuedOrder] = {}
        self.pending_targets: dict[str, tuple[float, TargetQuotes]] = {}
        self.pending_markouts: list[_PendingMarkout] = []
        self.marks: dict[str, float] = {}
        self.cash = 0.0
        self.rewards = 0.0
        self.rebates = 0.0
        self.placed_orders = 0
        self.filled_order_ids: set[str] = set()
        self.fill_count = 0
        self.filled_notional = 0.0
        self._order_sequence = 0
        self._fill_sequence = 0
        self._equity_peak = 0.0
        self.max_drawdown = 0.0
        self.max_capital = 0.0
        self.markout_weighted = 0.0
        self.markout_size = 0.0

    def run(self, events: list[JournalEvent], *, malformed_lines: int = 0) -> BacktestResult:
        relevant = [
            event for event in events
            if event.kind in {"book", "price_change", "last_trade_price"}
            and isinstance(event.data, dict)
        ]
        if not relevant:
            raise BacktestError("journal contains no replayable L2 events")
        start = relevant[0].ts
        end = relevant[-1].ts
        clock = start
        for event in relevant:
            self._advance(clock, event.ts)
            clock = event.ts
            touched = self._apply_event(event)
            for cid in touched:
                self._schedule_quotes(cid, event.ts)
            self._resolve_markouts(event.ts)
            self._record_risk()
        self._advance(clock, end)
        self._resolve_markouts(end)
        self._record_risk()
        inventory = self._inventory_value()
        trading_pnl = self.cash + inventory
        total = trading_pnl + self.rebates + self.rewards
        return BacktestResult(
            start_ts=start,
            end_ts=end,
            duration_seconds=max(0.0, end - start),
            events=len(relevant),
            malformed_lines=malformed_lines,
            markets=len(metas_in_events(relevant, self.metas)),
            placed_orders=self.placed_orders,
            filled_orders=len(self.filled_order_ids),
            fills=self.fill_count,
            fill_probability=(len(self.filled_order_ids) / self.placed_orders if self.placed_orders else 0.0),
            filled_notional=self.filled_notional,
            cash_flow=self.cash,
            inventory_value=inventory,
            trading_mtm_pnl=trading_pnl,
            maker_rebate_estimate=self.rebates,
            liquidity_reward_estimate=self.rewards,
            total_pnl_estimate=total,
            max_capital_at_risk=self.max_capital,
            max_drawdown=self.max_drawdown,
            mean_markout_bps=(
                10_000.0 * self.markout_weighted / self.markout_size
                if self.markout_size > 0 else None
            ),
            unresolved_markouts=len(self.pending_markouts),
        )

    def close(self) -> None:
        self.state.close()

    def _advance(self, start: float, end: float) -> None:
        cursor = start
        while True:
            due = min((item[0] for item in self.pending_targets.values()), default=math.inf)
            if due > end:
                break
            self._accrue_rewards(cursor, due)
            cursor = due
            for cid, (target_due, target) in list(self.pending_targets.items()):
                if target_due <= due + 1e-9:
                    self._activate_target(cid, target, due)
                    del self.pending_targets[cid]
        self._accrue_rewards(cursor, end)

    def _apply_event(self, event: JournalEvent) -> set[str]:
        touched: set[str] = set()
        if event.kind == "book":
            update = parse_book(event.data)
            if update and update.asset_id in self.books:
                book = self.books[update.asset_id]
                if update.tick_size:
                    book.set_tick_size(update.tick_size)
                book.apply_snapshot(update.bids, update.asks, event.ts, update.book_hash)
                touched.add(self.token_market[update.asset_id])
        elif event.kind == "price_change":
            for change in parse_price_changes(event.data):
                changed_book = self.books.get(change.asset_id)
                if changed_book is not None:
                    changed_book.apply_delta(change.side, change.price, change.size, event.ts)
                    touched.add(self.token_market[change.asset_id])
        else:
            trade = parse_last_trade(event.data)
            if trade and trade.asset_id in self.books:
                cid = self.token_market[trade.asset_id]
                self._fill_from_trade(trade.asset_id, trade.aggressor, trade.price, trade.size, event.ts)
                self.estimators[cid].flow.update(trade.aggressor, trade.size, event.ts)
                self._detect_sweep(cid, trade.asset_id, trade.aggressor, trade.price, trade.size)
                touched.add(cid)
        return touched

    def _schedule_quotes(self, cid: str, ts: float) -> None:
        target = self._target_quotes(cid, ts)
        if target is not None:
            due = ts + self.options.quote_latency_ms / 1000.0
            self.pending_targets[cid] = (due, target)

    def _target_quotes(self, cid: str, ts: float) -> TargetQuotes | None:
        meta = self.metas[cid]
        profile = self.profiles[cid]
        yes_book = self.books[meta.yes.token_id]
        if yes_book.is_empty:
            return None
        bid, ask = yes_book.best_bid(), yes_book.best_ask()
        if bid is None or ask is None or bid.price >= ask.price:
            return None
        micro = yes_book.microprice(profile.micro_levels)
        if micro is None:
            return None
        estimates = self.estimators[cid]
        estimates.flow.decay_to(ts)
        fv = compute_fair_value(micro, estimates.flow.z, meta.tick_size)
        previous = estimates.last_fv
        estimates.on_fair_value(fv, ts)
        self.marks[meta.yes.token_id] = fv
        self.marks[meta.no.token_id] = 1.0 - fv
        self.risk.update_mark(meta.yes.token_id, fv)
        self.risk.update_mark(meta.no.token_id, 1.0 - fv)
        yes_pos = self.state.position(meta.yes.token_id)
        no_pos = self.state.position(meta.no.token_id)
        inventory_util = (
            abs(yes_pos.size - no_pos.size) * fv / profile.q_max_usdc
            if profile.q_max_usdc > 0 else 0.0
        )
        decision = self.risk.evaluate(
            meta, ws_stale=False, event_group_cost=self._event_group_cost(meta)
        )
        regime = self.regimes[cid].decide(
            RegimeInputs(
                now=ts,
                tick=meta.tick_size,
                fv=fv,
                prev_fv=previous,
                vol_ratio=estimates.vol.ratio,
                flow_z=estimates.flow.z,
                inventory_util=inventory_util,
                hours_to_end=_hours_to_end(meta.end_date_iso, ts),
                sweep_flagged=cid in self.sweep_flagged,
                risk_halt=decision.halt,
                risk_reduce_only=decision.reduce_only,
            ),
            profile,
        )
        self.sweep_flagged.discard(cid)
        return construct_quotes(
            QuoteInputs(
                meta=meta,
                regime=regime,
                fv=fv,
                vol_short=estimates.vol.short,
                toxicity=estimates.markout.toxicity,
                yes_view=yes_book.view(),
                no_view=self.books[meta.no.token_id].view(),
                pos_yes=yes_pos,
                pos_no=no_pos,
                profile=profile,
                now=ts,
                risk_size_scale=decision.size_scale,
                yes_exit_urgency=compute_exit_urgency(
                    self.state.last_fill_ts(meta.yes.token_id), ts, profile.exit_urgency_s,
                ),
                no_exit_urgency=compute_exit_urgency(
                    self.state.last_fill_ts(meta.no.token_id), ts, profile.exit_urgency_s,
                ),
            )
        )

    def _activate_target(self, cid: str, target: TargetQuotes, ts: float) -> None:
        meta = self.metas[cid]
        profile = self.profiles[cid]
        live = [order for order in self.state.orders.values() if order.token_id in {
            meta.yes.token_id, meta.no.token_id
        }]
        plan = reconcile(
            target,
            live,
            tick=meta.tick_size,
            reprice_ticks=profile.reprice_ticks,
            resize_frac=profile.resize_frac,
        )
        for order_id in plan.to_cancel:
            self.queued.pop(order_id, None)
            self.state.remove_order(order_id)
        quotes = self.risk.fit_reservation(
            meta, plan.to_place, event_group_cost=self._event_group_cost(meta)
        )
        for quote in quotes:
            self._order_sequence += 1
            order = OpenOrder(
                f"replay-{self._order_sequence}", quote.token_id, quote.side,
                quote.price, quote.size, OrderState.LIVE, ts,
            )
            queue = self._displayed_queue(quote) * self.options.queue_ahead_fraction
            self.queued[order.order_id] = _QueuedOrder(order, queue)
            self.state.upsert_order(order)
            self.placed_orders += 1

    def _displayed_queue(self, quote: Quote) -> float:
        book = self.books[quote.token_id]
        levels = book.bids if quote.side is Side.BUY else book.asks
        return float(levels.get(quote.price, 0.0))

    def _detect_sweep(
        self, cid: str, token_id: str, aggressor: Side, price: float, size: float
    ) -> None:
        profile = self.profiles[cid]
        if size < profile.event_sweep_mult * profile.base_size_usdc / max(price, 0.01):
            return
        book = self.books[token_id]
        bid, ask = book.best_bid(), book.best_ask()
        if bid is None or ask is None:
            return
        if aggressor is Side.BUY:
            consumed = book.depth_within(
                Side.SELL, ask.price, ask.price + 3 * book.tick_size
            )
        else:
            consumed = book.depth_within(
                Side.BUY, bid.price - 3 * book.tick_size, bid.price
            )
        if consumed > 0 and size >= profile.event_sweep_frac * consumed:
            self.sweep_flagged.add(cid)

    def _fill_from_trade(
        self, token_id: str, aggressor: Side, price: float, size: float, ts: float
    ) -> None:
        if size <= 0:
            return
        candidates = [queued for queued in self.queued.values() if queued.order.token_id == token_id]
        if aggressor is Side.SELL:
            candidates = [q for q in candidates if q.order.side is Side.BUY and q.order.price >= price]
            candidates.sort(key=lambda q: -q.order.price)
        else:
            candidates = [q for q in candidates if q.order.side is Side.SELL and q.order.price <= price]
            candidates.sort(key=lambda q: q.order.price)
        remaining_trade = size
        for queued in candidates:
            if remaining_trade <= 0:
                break
            order = queued.order
            traded_through = (
                order.price > price if order.side is Side.BUY else order.price < price
            )
            if traded_through:
                queued.queue_ahead = 0.0
            consumed = min(queued.queue_ahead, remaining_trade)
            queued.queue_ahead -= consumed
            remaining_trade -= consumed
            if remaining_trade <= 0 or queued.queue_ahead > 0:
                continue
            fill_size = min(order.size, remaining_trade)
            if order.side is Side.SELL:
                fill_size = min(fill_size, self.state.position(token_id).size)
            if fill_size <= 0:
                continue
            self._record_fill(order, fill_size, ts)
            remaining_trade -= fill_size

    def _record_fill(self, order: OpenOrder, size: float, ts: float) -> None:
        self._fill_sequence += 1
        fill = Fill(order.token_id, order.side, order.price, size, f"replay-fill-{self._fill_sequence}", ts)
        if not self.state.apply_fill(fill):
            return
        self.risk.note_fill(fill)
        signed = 1.0 if order.side is Side.SELL else -1.0
        notional = order.price * size
        self.cash += signed * notional
        self.filled_notional += notional
        self.fill_count += 1
        self.filled_order_ids.add(order.order_id)
        cid = self.token_market[order.token_id]
        meta = self.metas[cid]
        self.rebates += notional * meta.taker_fee_bps / 10_000.0 * meta.rebate_rate
        fv = self.marks.get(order.token_id, order.price)
        self.pending_markouts.append(
            _PendingMarkout(order.token_id, order.side, fv, size, ts + self.options.markout_seconds)
        )
        self.estimators[cid].markout.record_fill(order.side, fv, ts)
        order.size -= size
        if order.size <= 1e-9:
            self.queued.pop(order.order_id, None)
            self.state.remove_order(order.order_id)
        else:
            self.state.upsert_order(order)

    def _resolve_markouts(self, ts: float) -> None:
        pending: list[_PendingMarkout] = []
        for markout in self.pending_markouts:
            if markout.due_ts > ts or markout.token_id not in self.marks:
                pending.append(markout)
                continue
            move = self.marks[markout.token_id] - markout.fv_at_fill
            signed = move if markout.side is Side.BUY else -move
            self.markout_weighted += signed * markout.size
            self.markout_size += markout.size
        self.pending_markouts = pending

    def _accrue_rewards(self, start: float, end: float) -> None:
        dt = min(max(0.0, end - start), self.options.max_reward_gap_seconds)
        if dt <= 0:
            return
        for cid, meta in self.metas.items():
            profile = self.profiles[cid]
            fractions: list[float] = []
            for token in (meta.yes.token_id, meta.no.token_id):
                mark = self.marks.get(token)
                if mark is None or meta.rewards_max_spread <= 0:
                    continue
                band = meta.rewards_max_spread / 100.0
                own = sum(
                    _reward_score(order.price, order.size, mark, band)
                    for order in self.state.orders_for(token)
                    if order.side is Side.BUY and order.size >= meta.rewards_min_size * profile.reward_size_mult
                )
                visible = sum(
                    _reward_score(price, size, mark, band)
                    for price, size in self.books[token].bids.items()
                )
                fractions.append(own / (own + visible) if own > 0 else 0.0)
            if len(fractions) == 2:
                self.rewards += meta.rewards_daily_rate * dt / 86_400.0 * min(fractions)

    def _event_group_cost(self, meta: MarketMeta) -> float:
        if not meta.event_id:
            return 0.0
        return sum(
            self.state.position(token).size * self.marks.get(
                token, self.state.position(token).avg_price or 0.5
            ) + sum(
                order.notional for order in self.state.orders_for(token) if order.side is Side.BUY
            )
            for sibling in self.metas.values()
            if sibling.event_id == meta.event_id
            for token in (sibling.yes.token_id, sibling.no.token_id)
        )

    def _inventory_value(self) -> float:
        return sum(
            position.size * self.marks.get(token, position.avg_price)
            for token, position in self.state.positions.items()
        )

    def _record_risk(self) -> None:
        equity = self.cash + self._inventory_value()
        self._equity_peak = max(self._equity_peak, equity)
        self.max_drawdown = max(self.max_drawdown, self._equity_peak - equity)
        capital = self._inventory_value() + sum(
            queued.order.notional
            for queued in self.queued.values()
            if queued.order.side is Side.BUY
        )
        self.max_capital = max(self.max_capital, capital)


def metas_in_events(events: list[JournalEvent], metas: dict[str, MarketMeta]) -> set[str]:
    configured = set(metas)
    return {
        str(event.data.get("market"))
        for event in events
        if str(event.data.get("market")) in configured
    }


def _reward_score(price: float, size: float, fair_value: float, band: float) -> float:
    distance = abs(price - fair_value)
    if band <= 0 or distance > band:
        return 0.0
    quality = 1.0 - distance / band
    return size * quality * quality


def _hours_to_end(end_date_iso: str | None, now: float) -> float | None:
    if not end_date_iso:
        return None
    from datetime import datetime

    try:
        hours = (datetime.fromisoformat(end_date_iso.replace("Z", "+00:00")).timestamp() - now) / 3600
        return hours if hours > 0 else None
    except (TypeError, ValueError):
        return None
