# Authoritative Trade Ledger Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover every confirmed maker fill into an idempotent SQLite cash ledger so signature-type-3 wallets retain correct positions, mark-to-market PnL, and daily-loss protection across WebSocket loss and process restarts.

**Architecture:** Normalize both user-WebSocket and authenticated CLOB trade-history payloads with the Deposit Wallet identity and a shared per-maker-leg ID. Persist fills as the cash-flow source of truth, backfill confirmed trades before every authoritative position snapshot, and halt/cancel managed orders whenever trades and positions cannot be reconciled.

**Tech Stack:** Python 3.12, asyncio, py-clob-client-v2 1.0.2, SQLite, pytest/pytest-asyncio, Ruff, Mypy.

**Spec:** `docs/superpowers/specs/2026-09-14-trade-ledger-reconciliation-design.md`

## Global Constraints

- Keep `polymaker-live.service` stopped until all local and VPS offline checks pass.
- Never use wallet-wide cancellation during startup, reconciliation, deployment, or recovery.
- Treat CLOB trades, Data API positions, and CLOB open orders as fail-closed reads.
- Scope fills and positions to tokens enabled by the current configuration.
- Do not change spread, size, inventory-skew, exposure-limit, or merge settings.
- Use UTC for the trade backfill day boundary and retain a 300-second overlap after the first successful sync.
- Only REST trades in `CONFIRMED` state become durable backfill fills.
- Do not derive strategy PnL from wallet-wide collateral balance.
- Do not log credentials, private keys, API secrets, or full `.env` content.

---

## File Structure

- Modify `src/polymaker/userstream/parse.py`: normalize maker legs from WebSocket and REST with a canonical ID plus a legacy ID alias.
- Modify `src/polymaker/userstream/client.py`: name the identity as the funder/maker address and forward normalized events unchanged.
- Modify `src/polymaker/state/tracker.py`: apply direct confirmed fills, preserve lifecycle idempotency, and reverse cash side effects on failure.
- Modify `src/polymaker/state/store.py`: support legacy fill aliases, derive cash from fills, and persist trade-sync metadata.
- Modify `src/polymaker/risk/manager.py`: repair cached net cash from the fill ledger and invalidate restored PnL on every cash change.
- Modify `src/polymaker/execution/gateway.py`: add a validated, fail-closed authenticated trade-history snapshot.
- Modify `src/polymaker/engine.py`: use the funder identity, backfill confirmed trades, verify positions, and quarantine on ambiguity.
- Modify `tests/test_userstream_parse.py`: real signature-type-3 maker-leg parsing and canonical-ID tests.
- Modify `tests/test_state.py`: direct-confirmed and failed-reversal lifecycle tests.
- Modify `tests/test_safety.py`: restart cash-ledger repair tests.
- Modify `tests/test_execution.py`: trade-history success and failure tests.
- Modify `tests/test_hardening2.py`: startup repair, periodic backfill, position mismatch, and read-failure tests.
- Modify `README.md` and `TIPS.md`: document authoritative trade reconciliation and PnL semantics.

---

### Task 1: Normalize Signature-Type-3 Maker Legs

**Files:**
- Modify: `src/polymaker/userstream/parse.py`
- Modify: `src/polymaker/userstream/client.py`
- Test: `tests/test_userstream_parse.py`

**Interfaces:**
- Consumes: raw CLOB trade dictionaries, `maker_address: str`, and `other_token(token_id) -> str | None`.
- Produces: `TradeEvent.legacy_trade_id: str | None` and canonical `TradeEvent.trade_id` values of `<trade-id>:<maker-order-id>`.

- [ ] **Step 1: Write failing parser tests using the observed production payload**

Add a fixture whose top-level taker side is `SELL`, whose three maker legs include
one leg with `maker_address == FUNDER`, and whose signer differs from the funder:

```python
SIGNER = "0x2f7636673e12c577c681B6fE112a93216d9627eB"
FUNDER = "0x5A5eD20745ce1c9bBd7E6595Fb6af3ac06C1D6Bc"

def test_signature_type_3_matches_funder_maker_leg():
    msg = _production_trade_payload(status="CONFIRMED")
    events = normalize_trade(msg, FUNDER, _other)
    assert len(events) == 1
    assert events[0].token_id == "yes-tok"
    assert events[0].our_side is Side.BUY
    assert events[0].price == 0.123
    assert events[0].size == 50
    assert events[0].trade_id == "trade-prod:order-ours"
    assert events[0].legacy_trade_id == "trade-prod:1"

def test_signature_type_3_does_not_match_signer():
    assert normalize_trade(_production_trade_payload(), SIGNER, _other) == []

def test_ws_and_rest_payloads_generate_same_fill_id():
    ws = _production_trade_payload(status="MATCHED")
    rest = _production_trade_payload(status="CONFIRMED")
    assert normalize_trade(ws, FUNDER, _other)[0].trade_id == (
        normalize_trade(rest, FUNDER, _other)[0].trade_id
    )
```

- [ ] **Step 2: Run the parser tests and verify the expected red state**

Run:

```powershell
.venv\Scripts\pytest.exe tests/test_userstream_parse.py -q
```

Expected: failures because `TradeEvent` has no `legacy_trade_id`, the canonical
ID still uses the maker-list index, and explicit maker-leg fields are not
preferred.

- [ ] **Step 3: Add the canonical identity and explicit maker-leg parsing**

Extend `TradeEvent` in `src/polymaker/state/tracker.py`:

```python
@dataclass(frozen=True, slots=True)
class TradeEvent:
    token_id: str
    our_side: Side
    price: float
    size: float
    trade_id: str
    status: TradeState
    ts: float
    legacy_trade_id: str | None = None
```

In `normalize_trade()`, select explicit maker-leg fields when present and retain
the old outcome fallback only for old payloads:

```python
maker_order_id = str(mo.get("order_id", ""))
canonical_id = f"{trade_id}:{maker_order_id}" if maker_order_id else legacy_id
maker_asset = str(mo.get("asset_id", ""))
maker_side = mo.get("side")
if maker_asset and maker_side is not None:
    token = maker_asset
    our_side = _side(maker_side)
elif mo.get("outcome") == taker_outcome:
    token = taker_asset
    our_side = taker_side.opposite
else:
    token = other_token(taker_asset) or taker_asset
    our_side = taker_side
```

Rename `UserStream`'s identity parameter and field from `our_address` to
`maker_address` without changing wire authentication.

- [ ] **Step 4: Run parser tests and verify green**

Run:

```powershell
.venv\Scripts\pytest.exe tests/test_userstream_parse.py -q
```

Expected: all parser tests pass.

- [ ] **Step 5: Commit the parser unit**

```powershell
git add src/polymaker/userstream/parse.py src/polymaker/userstream/client.py src/polymaker/state/tracker.py tests/test_userstream_parse.py
git commit -m "Fix funder maker fill attribution"
```

---

### Task 2: Make Confirmed and Failed Trade Lifecycles Cash-Complete

**Files:**
- Modify: `src/polymaker/state/tracker.py`
- Modify: `src/polymaker/engine.py`
- Test: `tests/test_state.py`
- Test: `tests/test_hardening.py`

**Interfaces:**
- Consumes: `TradeEvent` from Task 1 and the existing `StateStore.apply_fill(fill)` interface.
- Produces: `UserEventProcessor.on_trade(event, condition_id) -> bool`, returning `True` only when a new fill or reversal was applied.

- [ ] **Step 1: Write failing direct-confirmed and reversal tests**

```python
def test_confirmed_without_matched_applies_fill_once(tmp_path):
    store = StateStore(tmp_path / "s.db")
    cash_events: list[Fill] = []
    processor = UserEventProcessor(store, on_fill=cash_events.append)
    event = TradeEvent("tok", Side.BUY, 0.5, 10, "t:o", TradeState.CONFIRMED, 1.0)
    assert processor.on_trade(event, "cid") is True
    assert processor.on_trade(event, "cid") is False
    assert store.position("tok").size == 10
    assert cash_events == [Fill("tok", Side.BUY, 0.5, 10, "t:o", 1.0, True)]

def test_failed_trade_reverses_position_and_cash_callback(tmp_path):
    store = StateStore(tmp_path / "s.db")
    cash_events: list[Fill] = []
    processor = UserEventProcessor(store, on_fill=cash_events.append)
    matched = TradeEvent("tok", Side.BUY, 0.5, 10, "t:o", TradeState.MATCHED, 1.0)
    failed = TradeEvent("tok", Side.BUY, 0.5, 10, "t:o", TradeState.FAILED, 2.0)
    processor.on_trade(matched, "cid")
    assert processor.on_trade(failed, "cid") is True
    assert store.position("tok").size == 0
    assert [(f.side, f.size) for f in cash_events] == [(Side.BUY, 10), (Side.SELL, 10)]
```

- [ ] **Step 2: Run lifecycle tests and verify red**

Run:

```powershell
.venv\Scripts\pytest.exe tests/test_state.py tests/test_hardening.py -q
```

Expected: direct `CONFIRMED` does not apply, `on_trade()` returns `None`, and the
reverse fill does not reach `on_fill`.

- [ ] **Step 3: Implement direct confirmation and complete reversal callbacks**

Refactor the repeated fill construction into `_fill(ev)` and make every branch
return whether durable state changed. For `CONFIRMED`:

```python
if ev.trade_id in self._applied:
    self._store.clear_inflight(ev.token_id)
    self._applied.pop(ev.trade_id, None)
    self._on_change(condition_id)
    return False
fill = self._fill(ev)
if not self._store.apply_fill(fill):
    return False
self._on_fill(fill)
self._on_change(condition_id)
return True
```

For `FAILED`, apply the reverse fill and call both `_on_fill(reverse)` and
`_on_change(condition_id)` only if the reverse insert succeeds. Update
`Engine._on_fill()` to skip markout creation for IDs ending in `:reverse`, while
still calling `risk.note_fill(fill)`.

- [ ] **Step 4: Run lifecycle tests and verify green**

Run:

```powershell
.venv\Scripts\pytest.exe tests/test_state.py tests/test_hardening.py -q
```

Expected: all lifecycle and replay tests pass.

- [ ] **Step 5: Commit the lifecycle unit**

```powershell
git add src/polymaker/state/tracker.py src/polymaker/engine.py tests/test_state.py tests/test_hardening.py
git commit -m "Make trade lifecycle cash complete"
```

---

### Task 3: Make Persisted Fills the Cash Source of Truth

**Files:**
- Modify: `src/polymaker/state/store.py`
- Modify: `src/polymaker/risk/manager.py`
- Test: `tests/test_state.py`
- Test: `tests/test_safety.py`

**Interfaces:**
- Produces: `StateStore.apply_fill(fill: Fill, *, aliases: Sequence[str] = ()) -> bool`.
- Produces: `StateStore.fill_count() -> int`.
- Produces: `StateStore.fill_cash_flow() -> float`.
- Produces: `StateStore.get_sync_value(key: str) -> str | None`.
- Produces: `StateStore.set_sync_value(key: str, value: str) -> None`.
- Produces: `RiskManager.reconcile_cash_ledger() -> None`.

- [ ] **Step 1: Write failing alias, cash derivation, and restart-repair tests**

```python
def test_apply_fill_rejects_legacy_alias(tmp_path):
    store = StateStore(tmp_path / "s.db")
    old = Fill("tok", Side.BUY, 0.5, 10, "trade:1")
    new = Fill("tok", Side.BUY, 0.5, 10, "trade:order-id")
    assert store.apply_fill(old)
    assert not store.apply_fill(new, aliases=("trade:1",))
    assert store.position("tok").size == 10

def test_confirmed_trade_rejects_precanonical_legacy_fill(tmp_path):
    store = StateStore(tmp_path / "s.db")
    store.apply_fill(Fill("tok", Side.BUY, 0.5, 10, "trade:1"))
    processor = UserEventProcessor(store)
    event = TradeEvent(
        "tok", Side.BUY, 0.5, 10, "trade:order-id",
        TradeState.CONFIRMED, 1.0, legacy_trade_id="trade:1",
    )
    assert processor.on_trade(event, "cid") is False
    assert store.position("tok").size == 10

def test_fill_cash_flow_includes_reversals(tmp_path):
    store = StateStore(tmp_path / "s.db")
    store.apply_fill(Fill("tok", Side.BUY, 0.5, 10, "buy"))
    assert store.fill_cash_flow() == pytest.approx(-5.0)
    store.apply_fill(Fill("tok", Side.SELL, 0.5, 10, "buy:reverse"))
    assert store.fill_cash_flow() == pytest.approx(0.0)

def test_risk_restart_repairs_stale_cached_cash_from_fills(tmp_path):
    path = tmp_path / "state.db"
    store = StateStore(path)
    store.apply_fill(Fill("tok", Side.BUY, 0.5, 10, "fill"))
    store.save_risk_state(
        _day_key(), day_start_equity=0, net_cash=0, daily_pnl=5,
        killed=False, manual_killed=False, order_attempts=0, order_errors=0,
    )
    risk = RiskManager(RiskConfig(), store)
    assert risk.net_cash == pytest.approx(-5.0)
```

Also test that `get_sync_value()` returns `None`, `set_sync_value()` survives a
restart, and a new fill clears restored daily PnL before persistence.

- [ ] **Step 2: Run state/risk tests and verify red**

Run:

```powershell
.venv\Scripts\pytest.exe tests/test_state.py tests/test_safety.py -q
```

Expected: desired methods/signatures do not exist and cached risk cash remains
incorrect.

- [ ] **Step 3: Add additive SQLite schema and ledger operations**

Add to `_SCHEMA`:

```sql
CREATE TABLE IF NOT EXISTS sync_state (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_ts REAL NOT NULL
);
```

Before inserting a fill, query the exact ID and every supplied alias in one
parameterized `IN` expression. Update `UserEventProcessor` so each insertion
passes `aliases=(ev.legacy_trade_id,)` when the legacy ID is non-empty. Implement
cash derivation with a single aggregate:

```sql
SELECT COALESCE(SUM(
    CASE WHEN side='BUY' THEN -(price * size) ELSE price * size END
), 0) AS net_cash
FROM fills
```

Implement `fill_count()` as `SELECT COUNT(*) FROM fills` so engine recovery and
operator checks do not reach into `StateStore`'s private SQLite connection.

Persist sync values with `INSERT OR REPLACE`; never advance them from a caller
that has not completed a valid snapshot.

- [ ] **Step 4: Repair RiskManager from the durable ledger**

In `RiskManager.__init__`, derive `_net_cash` from `store.fill_cash_flow()` and
log a warning when it differs materially from cached `risk_state.net_cash`.
Implement:

```python
def reconcile_cash_ledger(self) -> None:
    self._net_cash = self._store.fill_cash_flow()
    self._restored_daily_pnl = None
    self._persist()
```

Call `_restored_daily_pnl = None` at the start of `note_fill()` so new cash
movement cannot preserve an old PnL snapshot.

- [ ] **Step 5: Run state/risk tests and verify green**

Run:

```powershell
.venv\Scripts\pytest.exe tests/test_state.py tests/test_safety.py -q
```

Expected: all state, persistence, risk, kill, and reservation tests pass.

- [ ] **Step 6: Commit the durable-ledger unit**

```powershell
git add src/polymaker/state/store.py src/polymaker/risk/manager.py tests/test_state.py tests/test_safety.py
git commit -m "Derive risk cash from persisted fills"
```

---

### Task 4: Add Fail-Closed Authenticated Trade History

**Files:**
- Modify: `src/polymaker/execution/gateway.py`
- Test: `tests/test_execution.py`
- Test: `tests/test_safety.py`

**Interfaces:**
- Produces: `ExecutionGateway.trades(*, after: int | None = None) -> list[dict[str, Any]]`.

- [ ] **Step 1: Write failing success and failure tests**

```python
@pytest.mark.asyncio
async def test_gateway_trades_reads_valid_snapshot():
    row = {"id": "t", "status": "CONFIRMED", "maker_orders": []}
    gateway = ExecutionGateway(Config())
    gateway._client = SimpleNamespace(get_trades=lambda *_args, **_kwargs: [row])
    assert await gateway.trades(after=123) == [row]

@pytest.mark.asyncio
async def test_gateway_trades_fails_closed_on_network_error():
    gateway = ExecutionGateway(Config())
    gateway._client = SimpleNamespace(
        get_trades=lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("down"))
    )
    with pytest.raises(GatewayReadError, match="trades snapshot unavailable"):
        await gateway.trades(after=123)

@pytest.mark.asyncio
async def test_gateway_trades_fails_closed_on_malformed_row():
    gateway = ExecutionGateway(Config())
    gateway._client = SimpleNamespace(get_trades=lambda *_args, **_kwargs: [{"id": "t"}])
    with pytest.raises(GatewayReadError):
        await gateway.trades()
```

- [ ] **Step 2: Run gateway tests and verify red**

Run:

```powershell
.venv\Scripts\pytest.exe tests/test_execution.py tests/test_safety.py -q
```

Expected: `ExecutionGateway.trades` does not exist.

- [ ] **Step 3: Implement the validated snapshot**

Use `TradeParams(after=after)` and SDK pagination. Accept only a list of dicts;
require non-empty `id`, a known lifecycle status, and a list-valued
`maker_orders`. Wrap every failure in `GatewayReadError`:

```python
async def trades(self, *, after: int | None = None) -> list[dict[str, Any]]:
    if self._paper:
        return []
    if self._client is None:
        raise GatewayReadError("trades snapshot unavailable: gateway not connected")

    def _get() -> list[dict[str, Any]]:
        from py_clob_client_v2.clob_types import TradeParams
        raw = self._client.get_trades(TradeParams(after=after), only_first_page=False)
        if not isinstance(raw, list):
            raise ValueError("trades payload is not a list")
        known = {"MATCHED", "MINED", "CONFIRMED", "RETRYING", "FAILED"}
        for row in raw:
            if not isinstance(row, dict):
                raise ValueError("trade row is not an object")
            if not row.get("id") or str(row.get("status", "")).upper() not in known:
                raise ValueError("trade row has invalid identity or status")
            if not isinstance(row.get("maker_orders"), list):
                raise ValueError("trade maker_orders is not a list")
        return raw

    try:
        return await self._io(_get)
    except Exception as exc:
        raise GatewayReadError("trades snapshot unavailable") from exc
```

Replace the ellipsis above with explicit list, dictionary, ID, status, and
maker-order-list checks. Known statuses are `MATCHED`, `MINED`, `CONFIRMED`,
`RETRYING`, and `FAILED`.

- [ ] **Step 4: Run gateway tests and verify green**

Run:

```powershell
.venv\Scripts\pytest.exe tests/test_execution.py tests/test_safety.py -q
```

Expected: gateway read tests pass, including all prior fail-closed snapshots.

- [ ] **Step 5: Commit the gateway unit**

```powershell
git add src/polymaker/execution/gateway.py tests/test_execution.py tests/test_safety.py
git commit -m "Add fail-closed trade history reads"
```

---

### Task 5: Reconcile Confirmed Trades Before Positions

**Files:**
- Modify: `src/polymaker/engine.py`
- Test: `tests/test_hardening2.py`
- Test: `tests/test_safety.py`

**Interfaces:**
- Consumes: `gateway.trades(after=timestamp)`, `normalize_trade(payload, maker_address, other_token)`, sync-state methods, `user_proc.on_trade(event, condition_id) -> bool`, and `risk.reconcile_cash_ledger()`.
- Produces: `Engine._sync_confirmed_trades(full_day: bool = False) -> int`.
- Produces: `Engine._reconcile_authoritative_state(*, startup: bool = False) -> tuple[int, int]`.

- [ ] **Step 1: Write a failing production-repair test**

Create an engine with the local position already set to 50 but no fills, return
the real confirmed maker payload from `gateway.trades`, and return `(50, 0.123)`
from `gateway.positions`:

```python
@pytest.mark.asyncio
async def test_first_trade_sync_repairs_rest_position_without_cash(tmp_path, meta):
    engine = _engine_with_market(tmp_path, meta)
    engine.state.set_position(meta.yes.token_id, 50, 0.123)
    engine.gateway._funder = FUNDER
    engine.gateway.trades = AsyncMock(return_value=[_confirmed_trade(meta)])
    engine.gateway.positions = AsyncMock(
        return_value={meta.yes.token_id: (50.0, 0.123)}
    )
    engine.gateway.open_orders = AsyncMock(return_value=[])
    await engine._reconcile_authoritative_state(startup=True)
    assert engine.state.position(meta.yes.token_id).size == 50
    assert engine.state.fill_count() == 1
    assert engine.risk.net_cash == pytest.approx(-6.15)
    assert engine.state.get_sync_value("confirmed_trade_sync_initialized") == "1"
```

- [ ] **Step 2: Run the repair test and verify red**

Run:

```powershell
.venv\Scripts\pytest.exe tests/test_hardening2.py::test_first_trade_sync_repairs_rest_position_without_cash -q
```

Expected: reconciliation methods and trade checkpoint do not exist.

- [ ] **Step 3: Implement UTC window selection and confirmed-fill replay**

Add constants and helper:

```python
_TRADE_SYNC_INITIALIZED = "confirmed_trade_sync_initialized"
_TRADE_SYNC_TS = "confirmed_trade_sync_ts"
_TRADE_SYNC_OVERLAP_S = 300

def _utc_day_start_ts(now: float | None = None) -> int:
    dt = datetime.fromtimestamp(now or time.time(), tz=UTC)
    return int(dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
```

`_sync_confirmed_trades()` selects the UTC boundary for first/full sync and
otherwise uses `max(day_start, checkpoint - 300)`. It validates every normalized
event, ignores non-configured tokens, sends only `CONFIRMED` events to
`user_proc.on_trade()`, and advances the timestamp only after the complete
snapshot succeeds.

Change `Engine.start()` to construct `UserStream` with `self.gateway.funder`.

- [ ] **Step 4: Implement ordered state reconciliation and migration mode**

`_reconcile_authoritative_state(startup: bool)` must execute:

```text
trade history -> positions -> open orders
```

On the first migration sync, allow authoritative positions to correct the
transient position produced by inserting historical fills, call
`risk.reconcile_cash_ledger()`, then persist the initialized marker.

After initialization, snapshot internal sizes after confirmed-fill replay and
compare them with all configured-token sizes from REST. On mismatch, replay from
the UTC boundary once. If still mismatched, apply authoritative positions for
exposure safety and raise `GatewayReadError("confirmed trades do not explain positions")`.

- [ ] **Step 5: Write and run periodic/restart/failure tests**

Add tests proving:

```python
async def test_periodic_sync_applies_confirmed_fill_before_positions(tmp_path, meta):
    engine = _engine_with_market(tmp_path, meta)
    engine.state.set_sync_value("confirmed_trade_sync_initialized", "1")
    engine.gateway.trades = AsyncMock(return_value=[_confirmed_trade(meta)])
    engine.gateway.positions = AsyncMock(
        return_value={meta.yes.token_id: (50.0, 0.123)}
    )
    engine.gateway.open_orders = AsyncMock(return_value=[])
    await engine._reconcile_authoritative_state()
    assert engine.risk.net_cash == pytest.approx(-6.15)
    assert engine.state.position(meta.yes.token_id).size == 50

async def test_trade_read_failure_sets_state_unknown_and_places_nothing(tmp_path, meta):
    engine = _engine_with_market(tmp_path, meta)
    engine.gateway.trades = AsyncMock(side_effect=GatewayReadError("trade read failed"))
    engine.gateway.place = AsyncMock()
    with pytest.raises(GatewayReadError, match="trade read failed"):
        await engine._reconcile_authoritative_state()
    engine._state_unknown = True
    await engine._recompute(meta.condition_id)
    engine.gateway.place.assert_not_awaited()

async def test_initialized_position_mismatch_remains_state_unknown(tmp_path, meta):
    engine = _engine_with_market(tmp_path, meta)
    engine.state.set_sync_value("confirmed_trade_sync_initialized", "1")
    engine.gateway.trades = AsyncMock(return_value=[])
    engine.gateway.positions = AsyncMock(
        return_value={meta.yes.token_id: (50.0, 0.123)}
    )
    engine.gateway.open_orders = AsyncMock(return_value=[])
    with pytest.raises(GatewayReadError, match="do not explain positions"):
        await engine._reconcile_authoritative_state()
    assert engine.state.position(meta.yes.token_id).size == 50

async def test_ws_and_rest_same_leg_is_counted_once(tmp_path, meta):
    engine = _engine_with_market(tmp_path, meta)
    matched = normalize_trade(_matched_trade(meta), FUNDER, engine._other_token)[0]
    engine.user_proc.on_trade(matched, meta.condition_id)
    engine.gateway.trades = AsyncMock(return_value=[_confirmed_trade(meta)])
    await engine._sync_confirmed_trades(full_day=True)
    assert engine.state.fill_count() == 1
    assert engine.risk.net_cash == pytest.approx(-6.15)

async def test_startup_does_not_quote_before_trade_sync_succeeds(tmp_path, meta):
    engine = _engine_with_market(tmp_path, meta)
    engine.gateway.trades = AsyncMock(side_effect=GatewayReadError("trade read failed"))
    engine.gateway.place = AsyncMock()
    with pytest.raises(GatewayReadError):
        await engine._startup_reconcile()
    engine.gateway.place.assert_not_awaited()
```

Run each new test individually first and verify it fails for the intended missing
behavior, then implement the smallest engine change and rerun until green.

- [ ] **Step 6: Quarantine managed orders immediately on authoritative failure**

In the reconcile loop's `GatewayReadError` branch, set `_state_unknown` before
calling `_cancel_managed_assets()`. Preserve exponential backoff and do not clear
`_state_unknown` until one complete trade/position/order cycle succeeds.

Add a test that seeds managed open orders, makes `gateway.trades()` fail, and
asserts configured assets were cancelled while `gateway.place()` was never
called.

- [ ] **Step 7: Run engine safety suites and verify green**

Run:

```powershell
.venv\Scripts\pytest.exe tests/test_hardening.py tests/test_hardening2.py tests/test_safety.py tests/test_engine.py -q
```

Expected: all startup, fail-closed, reservation, lifecycle, and reconcile tests
pass.

- [ ] **Step 8: Commit the engine reconciliation unit**

```powershell
git add src/polymaker/engine.py tests/test_hardening2.py tests/test_safety.py tests/test_engine.py
git commit -m "Backfill confirmed trades before position reconcile"
```

---

### Task 6: Documentation and Complete Local Verification

**Files:**
- Modify: `README.md`
- Modify: `TIPS.md`

**Interfaces:**
- Documents: fill-ledger authority, trade-before-position reconcile order, daily
  PnL semantics, `STATE_UNKNOWN`, and production recovery evidence.

- [ ] **Step 1: Update operator documentation**

Document that signature type 3 uses the Deposit Wallet as maker identity, that
confirmed CLOB trades repair missed WebSocket events, and that positions never
invent cash. State that a trade/position disagreement halts and cancels managed
orders until an authoritative full-day replay agrees.

- [ ] **Step 2: Run complete verification**

```powershell
.venv\Scripts\pytest.exe -q
.venv\Scripts\ruff.exe check .
.venv\Scripts\mypy.exe src
git diff --check
uv run --no-sync --with pip-audit pip-audit --local
```

Expected baseline: at least `147 passed, 2 skipped`, with every newly added test
also passing; Ruff and Mypy clean; no whitespace errors; no known dependency
vulnerabilities.

- [ ] **Step 3: Review branch scope and secrets**

```powershell
git status --short
git diff $(git merge-base main HEAD) HEAD --stat
git diff $(git merge-base main HEAD) HEAD
git ls-files .env state.db state.paper.db livecfg/state.db livecfg/journal .venv
```

Expected: only source, tests, README/TIPS, design, and plan files are included;
no environment, database, journal, log, or virtual-environment files are tracked.

- [ ] **Step 4: Commit documentation**

```powershell
git add README.md TIPS.md docs/superpowers/specs/2026-09-14-trade-ledger-reconciliation-design.md docs/superpowers/plans/2026-09-14-trade-ledger-reconciliation.md
git commit -m "Document trade ledger recovery"
```

---

### Task 7: VPS Deployment, Ledger Repair, and Controlled Restart

**Files:**
- Deploy: committed source and tests to `/home/ubuntu/pmlp`
- Preserve: `/home/ubuntu/pmlp/.env`
- Preserve and back up: `/home/ubuntu/pmlp/livecfg/state.db*`

**Interfaces:**
- Consumes: verified branch commit and existing Bitvise profile.
- Produces: repaired production SQLite ledger and a supervised LIVE maker only
  after every recovery invariant passes.

- [ ] **Step 1: Confirm the live service remains stopped and orders remain clear**

```bash
systemctl is-active polymaker-live.service
cd /home/ubuntu/pmlp
.venv/bin/polymaker doctor --config-dir livecfg
```

Expected: service inactive, positions show exactly 50 Newsom YES shares, and
open orders equal zero.

- [ ] **Step 2: Back up production state outside the repository**

Create a timestamped directory under `/home/ubuntu/backups/pmlp-ledger/`, copy
`state.db`, `state.db-wal`, and `state.db-shm` when present, and record SHA-256
hashes. Do not copy `.env` or print any secret.

- [ ] **Step 3: Deploy a traceable commit**

Push the reviewed branch only after explicit push approval, fast-forward local
`main` without force, then use `git pull --ff-only origin main` on the VPS. If
the VPS worktree is dirty, stop and inspect instead of overwriting it.

- [ ] **Step 4: Run VPS offline regression tests**

```bash
cd /home/ubuntu/pmlp
/home/ubuntu/.local/bin/uv run --offline --no-sync --with pytest-asyncio \
  pytest tests/test_userstream_parse.py tests/test_state.py tests/test_safety.py \
  tests/test_execution.py tests/test_hardening.py tests/test_hardening2.py -q
```

Expected: all selected tests pass and the production `.venv` remains unchanged.

- [ ] **Step 5: Run a no-quote ledger repair probe**

Use the implemented engine reconciliation entry point from a one-shot Python
process: connect the gateway, resolve configured markets, run startup confirmed
trade/position/order reconciliation, print only aggregate fill count, net cash,
position size, and PnL, then close resources. Do not launch quoter, heartbeat,
merge, or market-order tasks.

Expected database state:

```text
fills:             1
net_cash:          -6.1500 pUSD
Newsom YES size:   50
Newsom YES average: 0.123
open orders:       0
killed:            0
manual_killed:     0
```

Before market marks arrive, daily PnL may be zero at cost. With the current
reference mark near 0.1225, it should be approximately -0.025 pUSD. It must not
be approximately +6.17 pUSD.

- [ ] **Step 6: Start one supervised LIVE service and observe two reconciles**

Start the same `polymaker-live.service` command used previously, with
`Restart=on-failure`, `KillSignal=SIGINT`, `User=ubuntu`, and explicit
`--live --confirm-live`. Verify one PID, `NRestarts=0`, successful heartbeat,
successful confirmed-trade sync, successful positions/open-orders snapshots,
and no `STATE_UNKNOWN`, traceback, or order errors.

- [ ] **Step 7: Perform final authoritative verification**

Run `doctor`, inspect trade/fill/risk aggregates, and list authoritative open
orders without exposing credentials. Confirm the 50-share position is still
accounted for, any exit SELL is bounded by inventory, BUY reservations remain
within 15 pUSD per market and 40 pUSD total, and daily PnL remains plausible.

If any invariant fails, stop the service normally, verify zero managed open
orders, and leave the production database backup intact for rollback.
