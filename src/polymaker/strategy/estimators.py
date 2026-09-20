"""Online estimators driven by the live stream: EWMAs of vol, flow, toxicity.

All are time-decayed (half-life in seconds) so they behave correctly under
irregular event arrival — a burst of ticks and a quiet minute are weighted by
elapsed wall-clock, not by sample count. Pure state machines: feed them
observations with timestamps, read scalar summaries. No I/O.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from polymaker.domain import Side


@dataclass(frozen=True, slots=True)
class MidpointObservation:
    """Filtered midpoint plus the anti-sniping state for one update."""

    value: float
    jumped: bool
    paused: bool
    stable: bool


class MidpointGuard:
    """EMA/median midpoint filter with a bounded post-jump pause.

    The guard is intentionally pure state: it never decides whether an order
    is safe to submit. The regime machine consumes ``paused``/``stable`` and
    remains responsible for pulling quotes. This keeps the risk and execution
    gates unchanged.
    """

    __slots__ = ("_ema", "_recent", "_last_raw", "_pause_until", "_stable_since")

    def __init__(self) -> None:
        self._ema: float | None = None
        self._recent: list[float] = []
        self._last_raw: float | None = None
        self._pause_until = 0.0
        self._stable_since: float | None = None

    def update(
        self,
        raw_mid: float,
        now: float,
        *,
        tick: float,
        jump_ticks: int,
        pause_s: float,
        stable_confirm_s: float,
        ema_alpha: float,
        median_window: int,
    ) -> MidpointObservation:
        if not math.isfinite(raw_mid) or not math.isfinite(now):
            raise ValueError("midpoint and timestamp must be finite")
        alpha = min(max(float(ema_alpha), 0.01), 1.0)
        window = min(max(int(median_window), 1), 31)
        previous_ema = self._ema if self._ema is not None else raw_mid
        # Compare a new observation with the previous raw observation. Using
        # the slowly moving EMA here would retrigger the pause on every tick
        # while a large jump remains at its new level.
        jumped = (
            self._last_raw is not None
            and abs(raw_mid - self._last_raw) >= max(1, jump_ticks) * max(tick, 1e-12)
        )
        self._ema = alpha * raw_mid + (1.0 - alpha) * previous_ema
        self._recent.append(raw_mid)
        if len(self._recent) > window:
            del self._recent[: len(self._recent) - window]
        ordered = sorted(self._recent)
        median = ordered[len(ordered) // 2]
        filtered = 0.5 * self._ema + 0.5 * median

        if jumped:
            self._pause_until = max(self._pause_until, now + max(0.0, pause_s))
            self._stable_since = None
        elif (
            self._stable_since is None
            or (
                self._last_raw is not None
                and abs(raw_mid - self._last_raw) > max(tick, 1e-12)
            )
        ):
            self._stable_since = now
        self._last_raw = raw_mid

        paused = now < self._pause_until
        stable = (
            not paused
            and self._stable_since is not None
            and now - self._stable_since >= max(0.0, stable_confirm_s)
        )
        return MidpointObservation(filtered, jumped, paused, stable)


class Ewma:
    """Time-decayed exponentially weighted mean.

    On each update the prior weight decays by 0.5 ** (dt / halflife); a fresh
    observation gets the remaining weight. The first observation seeds the mean.
    """

    __slots__ = ("halflife", "_value", "_last_ts", "_initialized")

    def __init__(self, halflife_s: float) -> None:
        if halflife_s <= 0:
            raise ValueError("halflife must be positive")
        self.halflife = halflife_s
        self._value = 0.0
        self._last_ts = 0.0
        self._initialized = False

    def update(self, value: float, ts: float) -> float:
        if not self._initialized:
            self._value = value
            self._last_ts = ts
            self._initialized = True
            return self._value
        dt = max(0.0, ts - self._last_ts)
        decay = 0.5 ** (dt / self.halflife)
        self._value = decay * self._value + (1.0 - decay) * value
        self._last_ts = ts
        return self._value

    def decay_to(self, ts: float) -> float:
        """Decay the stored value toward 0 as if observing 0 at `ts`.

        Used to age out flow/vol during silence without a new observation.
        """
        if self._initialized:
            dt = max(0.0, ts - self._last_ts)
            self._value *= 0.5 ** (dt / self.halflife)
            self._last_ts = ts
        return self._value

    @property
    def value(self) -> float:
        return self._value

    @property
    def ready(self) -> bool:
        return self._initialized


class VolEstimator:
    """Realized volatility at two horizons from fair-value changes."""

    __slots__ = ("_short", "_long", "_last_fv", "_last_ts")

    def __init__(self, short_halflife_s: float, long_halflife_s: float) -> None:
        self._short = Ewma(short_halflife_s)
        self._long = Ewma(long_halflife_s)
        self._last_fv: float | None = None
        self._last_ts = 0.0

    def update(self, fv: float, ts: float) -> None:
        if self._last_fv is not None:
            r = fv - self._last_fv
            sq = r * r
            self._short.update(sq, ts)
            self._long.update(sq, ts)
        self._last_fv = fv
        self._last_ts = ts

    @property
    def short(self) -> float:
        return math.sqrt(max(0.0, self._short.value))

    @property
    def long(self) -> float:
        return math.sqrt(max(0.0, self._long.value))

    @property
    def ratio(self) -> float:
        """short/long vol ratio; >1 means recent activity above baseline."""
        lo = self.long
        return self.short / lo if lo > 1e-9 else 1.0


class FlowEstimator:
    """Signed aggressor flow and its normalized strength (a crude z-score)."""

    __slots__ = ("_signed", "_abs")

    def __init__(self, halflife_s: float) -> None:
        self._signed = Ewma(halflife_s)
        self._abs = Ewma(halflife_s)

    def update(self, aggressor: Side, size: float, ts: float) -> None:
        signed = size if aggressor is Side.BUY else -size
        self._signed.update(signed, ts)
        self._abs.update(abs(size), ts)

    def decay_to(self, ts: float) -> None:
        self._signed.decay_to(ts)
        self._abs.decay_to(ts)

    @property
    def signed(self) -> float:
        return self._signed.value

    @property
    def z(self) -> float:
        """Signed flow normalized by average trade magnitude, in ~[-1, 1]+."""
        denom = self._abs.value
        return self._signed.value / denom if denom > 1e-9 else 0.0


@dataclass(slots=True)
class _PendingMarkout:
    fv_at_fill: float
    side: Side  # our side of the fill (BUY => we bought => adverse if price falls)
    due_ts: float


class MarkoutTracker:
    """Measures adverse selection: how fair value moves against us after fills.

    For each fill we remember FV-at-fill and, after a horizon, compare to the
    then-current FV. Signed so that a *positive* markout means the trade was
    good (price moved in our favor) and negative means we got picked off. The
    toxicity summary is the magnitude of recent adverse (negative) markout,
    which the quoter turns into extra spread / less size.
    """

    __slots__ = ("_horizon_s", "_pending", "_markout")

    def __init__(self, horizon_s: float = 300.0, ewma_halflife_s: float = 1800.0) -> None:
        self._horizon_s = horizon_s
        self._pending: list[_PendingMarkout] = []
        self._markout = Ewma(ewma_halflife_s)

    def record_fill(self, side: Side, fv_at_fill: float, ts: float) -> None:
        self._pending.append(_PendingMarkout(fv_at_fill, side, ts + self._horizon_s))

    def evaluate(self, fv_now: float, ts: float) -> None:
        """Resolve any markouts whose horizon has elapsed."""
        still: list[_PendingMarkout] = []
        for p in self._pending:
            if ts >= p.due_ts:
                move = fv_now - p.fv_at_fill
                # if we BOUGHT, a rise is good (+); if we SOLD, a fall is good (+)
                signed = move if p.side is Side.BUY else -move
                self._markout.update(signed, ts)
            else:
                still.append(p)
        self._pending = still

    @property
    def markout(self) -> float:
        return self._markout.value

    @property
    def toxicity(self) -> float:
        """Non-negative adverse-selection score (0 when fills are benign)."""
        return max(0.0, -self._markout.value)


@dataclass(slots=True)
class MarketEstimators:
    """Bundle of the per-market online estimators the engine keeps."""

    vol: VolEstimator
    flow: FlowEstimator
    markout: MarkoutTracker
    last_fv: float | None = None
    last_fv_ts: float = 0.0

    def on_fair_value(self, fv: float, ts: float) -> None:
        self.vol.update(fv, ts)
        self.markout.evaluate(fv, ts)
        self.last_fv = fv
        self.last_fv_ts = ts
