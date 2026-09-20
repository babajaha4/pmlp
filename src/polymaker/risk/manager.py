"""RiskManager: pre-trade gates and circuit breakers (see the README).

Consulted by the engine before every quote set. Returns a per-market decision
(size scale / reduce-only / halt) and owns the global kill switches. Position
and order data come from the StateStore; fair-value marks are pushed in by the
engine so PnL is always current.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from polymaker.config import RiskConfig
from polymaker.domain import Fill, MarketMeta, Quote, Side
from polymaker.logging import get_logger
from polymaker.state.store import StateStore

log = get_logger("risk.manager")


@dataclass(frozen=True, slots=True)
class RiskDecision:
    halt: bool  # HALTED regime for this market
    reduce_only: bool  # REDUCE_ONLY regime for this market
    size_scale: float  # multiply quote sizes by this [0,1]
    reason: str = ""


class RiskManager:
    def __init__(self, cfg: RiskConfig, store: StateStore) -> None:
        self._cfg = cfg
        self._store = store
        self._marks: dict[str, float] = {}  # token_id -> fair value
        self._day_key = _day_key()
        saved = store.load_risk_state(self._day_key)
        previous = store.latest_risk_state() if saved is None else None
        self._baseline_pending = saved is None
        self._restored_daily_pnl = float(saved["daily_pnl"]) if saved else None
        cached_net_cash = (
            float(saved["net_cash"])
            if saved
            else float(previous["net_cash"]) if previous else 0.0
        )
        self._net_cash = store.fill_cash_flow()
        if abs(self._net_cash - cached_net_cash) > 1e-6:
            self._restored_daily_pnl = None
            log.warning(
                "cash_ledger_reconciled",
                cached_net_cash=cached_net_cash,
                ledger_net_cash=self._net_cash,
            )
        self._manual_killed = (
            bool(saved["manual_killed"])
            if saved
            else bool(previous and previous.get("manual_killed", previous["killed"]))
        )
        self._killed = bool(saved["killed"]) if saved else self._manual_killed
        self._order_attempts = int(saved["order_attempts"]) if saved else 0
        self._order_errors = int(saved["order_errors"]) if saved else 0
        self._day_start_equity = (
            float(saved["day_start_equity"]) if saved else self._net_cash + self._inventory_value()
        )
        if saved is not None:
            self._persist()

    def _ensure_day(self) -> None:
        key = _day_key()
        if key == self._day_key:
            return
        self._day_key = key
        self._day_start_equity = self.equity
        self._baseline_pending = False
        self._restored_daily_pnl = None
        self._order_attempts = 0
        self._order_errors = 0
        self._killed = self._manual_killed
        self._persist()

    def establish_daily_baseline(self) -> None:
        """Set a new UTC-day baseline after the first authoritative position read."""
        if not self._baseline_pending:
            return
        self._day_start_equity = self.equity
        self._restored_daily_pnl = None
        self._baseline_pending = False
        self._killed = self._manual_killed
        self._persist()

    def _persist(self) -> None:
        self._store.save_risk_state(
            self._day_key,
            day_start_equity=self._day_start_equity,
            net_cash=self._net_cash,
            daily_pnl=self._daily_pnl_value(),
            killed=self._killed,
            manual_killed=self._manual_killed,
            order_attempts=self._order_attempts,
            order_errors=self._order_errors,
        )

    # ── PnL bookkeeping ─────────────────────────────────────────────────
    def prepare_fill(self) -> None:
        """Persist UTC rollover equity before a fill mutates durable inventory."""
        self._ensure_day()

    def note_fill(self, fill: Fill) -> None:
        self._ensure_day()
        self._restored_daily_pnl = None
        self._net_cash += (fill.price * fill.size) * (1 if fill.side is Side.SELL else -1)
        self._persist()

    def reconcile_cash_ledger(self) -> None:
        """Repair cached cash from the durable fill ledger."""
        self._ensure_day()
        self._net_cash = self._store.fill_cash_flow()
        self._restored_daily_pnl = None
        self._persist()

    def update_mark(self, token_id: str, fv: float) -> None:
        self._marks[token_id] = fv
        self._restored_daily_pnl = None

    def _inventory_value(self) -> float:
        total = 0.0
        for tok, pos in self._store.positions.items():
            if pos.size > 0:
                total += pos.size * self._marks.get(tok, pos.avg_price)
        return total

    @property
    def net_cash(self) -> float:
        return self._net_cash

    @property
    def inventory_value(self) -> float:
        return self._inventory_value()

    @property
    def equity(self) -> float:
        return self._net_cash + self._inventory_value()

    def marked_position_notional(self, token_id: str) -> float:
        """Current inventory notional using the latest mark when available."""
        pos = self._store.position(token_id)
        if pos.size <= 0:
            return 0.0
        return pos.size * self._marks.get(token_id, pos.avg_price or 0.5)

    @property
    def daily_pnl(self) -> float:
        """Mark-to-market daily equity change, including unrealized inventory."""
        self._ensure_day()
        return self._daily_pnl_value()

    def _daily_pnl_value(self) -> float:
        if self._restored_daily_pnl is not None:
            return self._restored_daily_pnl
        return self.equity - self._day_start_equity

    def reset_day(self) -> None:
        self._ensure_day()
        self._day_start_equity = self.equity
        self._restored_daily_pnl = None
        self._baseline_pending = False
        self._persist()

    # ── error-rate breaker ──────────────────────────────────────────────
    def note_order_result(self, ok: bool) -> None:
        self._ensure_day()
        self._order_attempts += 1
        if not ok:
            self._order_errors += 1
        self._persist()

    @property
    def error_rate(self) -> float:
        return self._order_errors / self._order_attempts if self._order_attempts >= 20 else 0.0

    # ── global kill switch ──────────────────────────────────────────────
    def global_halt(self) -> tuple[bool, str]:
        self._ensure_day()
        if self._killed:
            return True, "manual_kill"
        if self.daily_pnl <= -self._cfg.daily_loss_kill_usdc:
            self._killed = True
            self._manual_killed = False
            self._persist()
            return True, f"daily_loss {self.daily_pnl:.0f}"
        if self.error_rate >= self._cfg.max_order_error_rate:
            return True, f"error_rate {self.error_rate:.2f}"
        return False, ""

    def kill(self) -> None:
        self._killed = True
        self._manual_killed = True
        self._persist()
        log.critical("kill_switch_engaged")

    def resume(self) -> None:
        self._ensure_day()
        self._killed = False
        self._manual_killed = False
        self._persist()
        log.warning("kill_switch_cleared")

    # ── per-market evaluation ───────────────────────────────────────────
    def evaluate(
        self, meta: MarketMeta, *, ws_stale: bool, event_group_cost: float
    ) -> RiskDecision:
        halted, why = self.global_halt()
        if halted:
            return RiskDecision(True, False, 0.0, why)
        if ws_stale:
            return RiskDecision(True, False, 0.0, "ws_stale")

        position_market_notional = self._position_market_notional(meta)
        position_total_exposure = self._position_total_exposure()

        # Filled inventory at a hard cap is reduce-only. Resting BUYs remain hard
        # reservations, but `fit_target_reservation` can resize or remove them;
        # treating them as filled inventory would cancel a correctly fitted quote
        # set on every subsequent recompute.
        if position_market_notional >= self._cfg.max_market_notional_usdc:
            return RiskDecision(False, True, 1.0, "market_cap")
        if event_group_cost >= self._cfg.max_event_group_loss_usdc:
            return RiskDecision(False, True, 1.0, "event_group_cap")
        if position_total_exposure >= self._cfg.max_total_exposure_usdc:
            return RiskDecision(False, True, 1.0, "total_exposure_cap")

        # soft scaling: taper size as any cap is approached (worst-binding wins)
        # Resting orders are hard reservations, but do not taper an already
        # resting quote set. Tapering on reservations creates cancel/replace
        # churn; `can_reserve` below blocks any additional capital instead.
        scale = min(
            _headroom(self._position_market_notional(meta), self._cfg.max_market_notional_usdc),
            _headroom(self._position_total_exposure(), self._cfg.max_total_exposure_usdc),
            _headroom(event_group_cost, self._cfg.max_event_group_loss_usdc),
        )
        return RiskDecision(False, False, scale, "")

    def _market_notional(self, meta: MarketMeta) -> float:
        """Worst-case deployed notional for this market.

        Resting BUY orders reserve capital because they can all fill before the
        next reconcile. SELL orders reduce inventory and do not add capital risk.
        """
        total = 0.0
        for tok in (meta.yes.token_id, meta.no.token_id):
            pos = self._store.position(tok)
            total += pos.size * self._marks.get(tok, pos.avg_price or 0.5)
            total += sum(o.notional for o in self._store.orders_for(tok) if o.side is Side.BUY)
        return total

    def _total_exposure(self) -> float:
        total = sum(
            pos.size * self._marks.get(tok, pos.avg_price or 0.5)
            for tok, pos in self._store.positions.items()
            if pos.size > 0
        )
        total += sum(
            order.notional for order in self._store.orders.values()
            if order.side is Side.BUY
        )
        return total

    def _position_market_notional(self, meta: MarketMeta) -> float:
        return sum(
            self._store.position(tok).size * self._marks.get(
                tok, self._store.position(tok).avg_price or 0.5
            )
            for tok in (meta.yes.token_id, meta.no.token_id)
        )

    def _position_total_exposure(self) -> float:
        return sum(
            pos.size * self._marks.get(tok, pos.avg_price or 0.5)
            for tok, pos in self._store.positions.items()
            if pos.size > 0
        )

    def can_reserve(
        self, meta: MarketMeta, quotes: list[Quote], *, event_group_cost: float = 0.0
    ) -> bool:
        """Return whether a new quote batch fits all applicable hard caps."""
        additional = sum(q.price * q.size for q in quotes if q.side is Side.BUY)
        if additional <= 0:
            return True
        return (
            self._market_notional(meta) + additional <= self._cfg.max_market_notional_usdc
            and self._total_exposure() + additional <= self._cfg.max_total_exposure_usdc
            and event_group_cost + additional <= self._cfg.max_event_group_loss_usdc
        )

    def fit_reservation(
        self, meta: MarketMeta, quotes: list[Quote], *, event_group_cost: float = 0.0,
        reward_min_size: float | None = None,
    ) -> list[Quote]:
        """Shrink pending BUY quotes to fit every cap; SELL quotes are unchanged."""
        headroom = max(0.0, min(
            self._cfg.max_market_notional_usdc - self._market_notional(meta),
            self._cfg.max_total_exposure_usdc - self._total_exposure(),
            self._cfg.max_event_group_loss_usdc - event_group_cost,
        ))
        return self._fit_to_headroom(meta, quotes, headroom, reward_min_size=reward_min_size)

    def fit_target_reservation(
        self, meta: MarketMeta, quotes: list[Quote], *, event_group_cost: float = 0.0,
        reward_min_size: float | None = None,
    ) -> list[Quote]:
        """Fit a complete desired quote set as a replacement for this market.

        Existing BUYs in the same market are part of the state being replaced,
        not additional exposure. Reservations in every other market remain in
        each applicable cap. Computing this final target before reconciliation
        makes a headroom-scaled order stable on the next identical recompute.
        """
        current_market_buys = sum(
            order.notional
            for token_id in (meta.yes.token_id, meta.no.token_id)
            for order in self._store.orders_for(token_id)
            if order.side is Side.BUY
        )
        headroom = max(0.0, min(
            self._cfg.max_market_notional_usdc
            - max(0.0, self._market_notional(meta) - current_market_buys),
            self._cfg.max_total_exposure_usdc
            - max(0.0, self._total_exposure() - current_market_buys),
            self._cfg.max_event_group_loss_usdc
            - max(0.0, event_group_cost - current_market_buys),
        ))
        return self._fit_to_headroom(meta, quotes, headroom, reward_min_size=reward_min_size)

    @staticmethod
    def _fit_to_headroom(
        meta: MarketMeta, quotes: list[Quote], headroom: float,
        *, reward_min_size: float | None = None,
    ) -> list[Quote]:
        buy_notional = sum(q.price * q.size for q in quotes if q.side is Side.BUY)
        if buy_notional <= 0:
            return quotes
        scale = min(1.0, headroom / buy_notional)
        fitted: list[Quote] = []
        for quote in quotes:
            if quote.side is Side.SELL:
                fitted.append(quote)
                continue
            size = quote.size * scale
            floor = max(meta.min_order_size, reward_min_size or 0.0)
            if size + 1e-9 >= floor:
                fitted.append(Quote(quote.token_id, quote.side, quote.price, size))
        return fitted


def _headroom(current: float, cap: float) -> float:
    """1.0 well below the cap, tapering to 0 as we approach it (from 70%)."""
    if cap <= 0:
        return 1.0
    frac = current / cap
    if frac <= 0.7:
        return 1.0
    return max(0.0, (1.0 - frac) / 0.3)


def _day_key() -> str:
    return datetime.now(UTC).date().isoformat()
