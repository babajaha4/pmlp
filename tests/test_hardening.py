"""Hardening tests: the nasty failure modes that cause double-buys/sells,
phantom orders, and flying-blind quoting. Every scenario here maps to a real
Polymarket API failure mode (WS replay, snapshot lag, heartbeat gaps, timeouts).
"""

from __future__ import annotations

import asyncio
import time

from polymaker.domain import (
    Fill,
    OpenOrder,
    OrderState,
    Side,
    TradeState,
)
from polymaker.state.store import StateStore
from polymaker.state.tracker import TradeEvent, UserEventProcessor

# ══════════════════════ double-fill protection ══════════════════════════


def test_replayed_matched_after_confirmed_not_double_applied(tmp_path):
    """WS reconnects can replay events. MATCHED -> CONFIRMED -> MATCHED(replay)
    must not double the position, even though the in-memory dedupe was cleared
    at CONFIRMED."""
    s = StateStore(tmp_path / "s.db")
    p = UserEventProcessor(s)
    ev = TradeEvent("tok", Side.BUY, 0.5, 100, "t1", TradeState.MATCHED, 1.0)
    p.on_trade(ev, "cid")
    p.on_trade(TradeEvent("tok", Side.BUY, 0.5, 100, "t1", TradeState.CONFIRMED, 2.0), "cid")
    assert s.position("tok").size == 100
    p.on_trade(ev, "cid")  # replayed MATCHED after confirm
    assert s.position("tok").size == 100  # NOT 200
    assert s.inflight("tok") == 0  # replay must not re-mark inflight
    s.close()


def test_duplicate_fill_across_restart(tmp_path):
    """Process restarts + WS replays the same trade: the SQLite fills table is
    the dedupe gate, so the position is not double-applied."""
    db = tmp_path / "s.db"
    s1 = StateStore(db)
    assert s1.apply_fill(Fill("tok", Side.BUY, 0.5, 100, "t1")) is True
    s1.close()
    s2 = StateStore(db)
    assert s2.apply_fill(Fill("tok", Side.BUY, 0.5, 100, "t1")) is False  # duplicate
    assert s2.position("tok").size == 100
    s2.close()


def test_duplicate_fill_side_effects_skipped(tmp_path):
    """A duplicate MATCHED must not fire on_fill/on_change callbacks."""
    s = StateStore(tmp_path / "s.db")
    fills, changes = [], []
    p = UserEventProcessor(s, on_change=changes.append, on_fill=fills.append)
    ev = TradeEvent("tok", Side.BUY, 0.5, 50, "t1", TradeState.MATCHED, 1.0)
    p.on_trade(ev, "cid")
    p.on_trade(TradeEvent("tok", Side.BUY, 0.5, 50, "t1", TradeState.CONFIRMED, 2.0), "cid")
    n_fills, n_changes = len(fills), len(changes)
    p.on_trade(ev, "cid")  # replay
    assert len(fills) == n_fills  # no new fill callback
    assert len(changes) == n_changes
    s.close()


def test_retrying_keeps_fill_failed_reverses_once(tmp_path):
    """RETRYING is not terminal (tx may still land) -> keep the fill.
    FAILED reverses exactly once, even if FAILED is replayed."""
    s = StateStore(tmp_path / "s.db")
    p = UserEventProcessor(s)
    assert p.on_trade(TradeEvent("tok", Side.BUY, 0.5, 100, "t1", TradeState.MATCHED, 1.0), "cid") is True
    assert p.on_trade(
        TradeEvent("tok", Side.BUY, 0.5, 100, "t1", TradeState.RETRYING, 2.0), "cid"
    ) is False
    assert s.position("tok").size == 100  # retrying: unchanged
    assert s.inflight("tok") == 1
    assert p.on_trade(
        TradeEvent("tok", Side.BUY, 0.5, 100, "t1", TradeState.FAILED, 3.0), "cid"
    ) is True
    assert s.position("tok").size == 0  # reversed
    assert p.on_trade(
        TradeEvent("tok", Side.BUY, 0.5, 100, "t1", TradeState.FAILED, 4.0), "cid"
    ) is False
    assert s.position("tok").size == 0  # replayed FAILED: no double reverse
    s.close()


# ══════════════════════ double-order protection ═════════════════════════


def _order(oid: str, tok: str = "tok", age_s: float = 60.0) -> OpenOrder:
    o = OpenOrder(oid, tok, Side.BUY, 0.49, 100, OrderState.LIVE)
    o.created_ts = time.time() - age_s
    return o


def test_grace_window_protects_fresh_orders(tmp_path):
    """A REST snapshot that lags a just-placed order must NOT evict it from
    state (that eviction is what caused re-placement -> double orders)."""
    s = StateStore(tmp_path / "s.db")
    fresh = _order("young", age_s=2.0)
    stale = _order("old", age_s=60.0)
    s.upsert_order(fresh)
    s.upsert_order(stale)
    # snapshot doesn't include either (lag for young; old was really cancelled)
    s.replace_open_orders("tok", [], grace_s=10.0)
    ids = {o.order_id for o in s.orders_for("tok")}
    assert "young" in ids  # protected by grace
    assert "old" not in ids  # correctly dropped
    s.close()


def test_grace_zero_is_authoritative_wipe(tmp_path):
    """grace_s=0 (post-quarantine / heartbeat recovery) drops everything the
    snapshot doesn't confirm — even fresh orders."""
    s = StateStore(tmp_path / "s.db")
    s.upsert_order(_order("young", age_s=1.0))
    s.replace_open_orders("tok", [], grace_s=0.0)
    assert s.orders_for("tok") == []
    s.close()


def test_replace_adopts_unknown_live_orders(tmp_path):
    """Orders live on the exchange but missing from state (e.g. a timed-out
    place that actually posted) are adopted so the reconciler can manage them."""
    s = StateStore(tmp_path / "s.db")
    ghost = _order("ghost", age_s=30.0)
    s.replace_open_orders("tok", [ghost])
    assert s.orders_for("tok")[0].order_id == "ghost"
    s.close()


# ══════════════════════ engine failure handling ═════════════════════════


def _mk_engine(tmp_path, meta):
    from tests.test_engine import _engine_with_market, _feed_book

    eng = _engine_with_market(tmp_path, meta)
    _feed_book(eng, meta)
    return eng


async def test_place_failure_triggers_quarantine(tmp_path, meta):
    """If a placement batch fails/returns incomplete, the engine must cancel
    the tokens' orders (idempotent) and resync — never leave the possibility
    of an untracked live order."""
    eng = _mk_engine(tmp_path, meta)
    cancelled_assets: list[str] = []

    async def failing_place(quotes, m, **_kwargs):  # posts may or may not have landed
        return []

    async def spy_cancel_asset(asset_id):
        cancelled_assets.append(asset_id)
        return True

    eng.gateway.place = failing_place  # type: ignore[method-assign]
    eng.gateway.cancel_asset = spy_cancel_asset  # type: ignore[method-assign]
    await eng._recompute(meta.condition_id)

    assert set(cancelled_assets) == {meta.yes.token_id, meta.no.token_id}
    assert eng.state.orders == {}  # nothing phantom left in state
    eng.state.close()
    eng.catalog.close()


async def test_cancel_failure_keeps_orders_and_skips_placement(tmp_path, meta):
    """A failed cancel must NOT drop orders from state (they may be live), and
    the engine must not place on top of them that cycle."""
    eng = _mk_engine(tmp_path, meta)
    # seed a live order the strategy will want to reprice away (far off + stale)
    stale = OpenOrder("stuck", meta.yes.token_id, Side.BUY, 0.10, 100, OrderState.LIVE)
    stale.created_ts = time.time() - 120
    eng.state.upsert_order(stale)

    placed_calls: list[int] = []

    async def failing_cancel(order_ids):
        return False

    async def spy_place(quotes, m):
        placed_calls.append(len(quotes))
        return []

    async def rest_still_live():  # REST confirms the order is still on the book
        return [stale]

    eng.gateway.cancel = failing_cancel  # type: ignore[method-assign]
    eng.gateway.place = spy_place  # type: ignore[method-assign]
    eng.gateway.open_orders = rest_still_live  # type: ignore[method-assign]
    await eng._recompute(meta.condition_id)

    assert "stuck" in {o.order_id for o in eng.state.orders_for(meta.yes.token_id)}
    assert placed_calls == []  # skipped placement entirely this cycle
    eng.state.close()
    eng.catalog.close()


async def test_user_ws_blind_halts_market(tmp_path, meta):
    """User WS down > threshold = we can't see fills -> pull all quotes."""
    from polymaker.userstream.client import UserStream

    eng = _mk_engine(tmp_path, meta)
    # simulate a live-mode engine whose user stream has been down for a while
    eng._user_started = True
    eng.user = UserStream.__new__(UserStream)  # bare instance, no connection
    eng.user.connected = False
    eng.user.disconnected_since = time.time() - 60.0

    await eng._recompute(meta.condition_id)
    assert eng.state.orders == {}  # nothing quoted while blind
    eng.state.close()
    eng.catalog.close()


async def test_heartbeat_failures_halt_market(tmp_path, meta):
    """Heartbeats failing = exchange is auto-cancelling us -> stop quoting."""
    eng = _mk_engine(tmp_path, meta)
    eng.paper = False  # hb_blind only applies in live mode
    eng.gateway._hb_failures = 5
    await eng._recompute(meta.condition_id)
    assert eng.state.orders == {}
    eng.state.close()
    eng.catalog.close()


async def test_healthy_engine_still_quotes(tmp_path, meta):
    """Sanity: none of the blind checks fire on a healthy paper engine."""
    eng = _mk_engine(tmp_path, meta)
    await eng._recompute(meta.condition_id)
    assert len(eng.state.orders) > 0
    eng.state.close()
    eng.catalog.close()


async def test_supervisor_restarts_dead_task(tmp_path, meta):
    """A task that dies unexpectedly is restarted by the supervisor."""
    from tests.test_engine import _engine_with_market

    eng = _engine_with_market(tmp_path, meta)
    runs: list[int] = []

    async def flaky() -> None:
        runs.append(1)
        if len(runs) == 1:
            raise RuntimeError("boom")  # first run dies
        await asyncio.sleep(30)  # second run stays alive

    eng._supervise_interval_s = 0.05  # fast polling for the test
    eng._spawn("flaky", flaky)
    sup = asyncio.create_task(eng._supervise())
    for _ in range(40):
        await asyncio.sleep(0.05)
        if len(runs) >= 2:
            break
    sup.cancel()
    eng._tasks["flaky"].cancel()
    assert len(runs) >= 2, "dead task was not restarted"
    eng.state.close()
    eng.catalog.close()


def test_reverse_fill_updates_cash_without_creating_markout(tmp_path, meta):
    eng = _mk_engine(tmp_path, meta)

    eng._on_fill(Fill(meta.yes.token_id, Side.SELL, 0.5, 10, "t:o:reverse", 0.0))
    eng.est[meta.condition_id].markout.evaluate(fv_now=0.1, ts=301.0)

    assert eng.risk.net_cash == 5.0
    assert eng.est[meta.condition_id].markout.markout == 0.0
    eng.state.close()
    eng.catalog.close()
