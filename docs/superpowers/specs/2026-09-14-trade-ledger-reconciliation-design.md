# Authoritative Trade Ledger Reconciliation

## Problem

The live engine received a real maker fill for 50 Newsom YES shares at 0.123,
but recorded no fill and no cash movement. The authenticated user WebSocket
journal proves that `MATCHED`, `MINED`, and `CONFIRMED` frames all arrived.

The parser compared each maker leg against the signing EOA. For signature type
3, Polymarket reports the Deposit Wallet (the configured funder) as
`maker_orders[].maker_address`, so the parser discarded the valid leg. Periodic
position reconciliation later restored the 50-share inventory but could not
restore the -6.15 pUSD cash flow. This made mark-to-market daily PnL appear as a
profit of roughly the entire inventory value and made the daily-loss breaker
unreliable.

Position deltas cannot safely reconstruct cash. They do not contain sell prices,
cannot describe several fills between snapshots, and lose complete round trips
that return a position to zero. Wallet collateral balance is also unsuitable as
the strategy ledger because deposits, withdrawals, manual activity, and other
strategies can share the funder.

## Goals

- Attribute signature-type-3 maker fills to the Deposit Wallet correctly.
- Recover confirmed fills missed by the WebSocket from authenticated CLOB trade
  history.
- Give WebSocket and REST observations the same persistent fill identity so
  either source can arrive first without double-counting position or cash.
- Make persisted fills the source of truth for strategy cash flow across crashes
  and restarts.
- Prevent quoting whenever authoritative trades, positions, or orders cannot be
  reconciled.
- Repair the current VPS ledger before live quoting resumes.

## Non-Goals

- Using wallet-wide collateral balance as strategy PnL.
- Attributing manual activity outside configured tokens to this strategy.
- Changing quote spreads, sizes, inventory skew, or exposure limits.
- Automatically merging positions.
- Reconstructing fees or incentive rewards that are not present in the trade
  payload.

## Design

### Maker identity and canonical fill IDs

`UserStream` will match maker legs against `ExecutionGateway.funder`. The funder
already falls back to the signing EOA for an EOA wallet, while signature type 3
uses `BROWSER_ADDRESS`, which matches Polymarket's real maker payload.

The shared trade normalizer will prefer the maker leg's explicit `asset_id`,
`side`, `price`, and `matched_amount`. Older payloads without explicit maker-leg
asset or side fields retain the current taker-side/outcome fallback.

Every maker leg will use a canonical persistent identifier derived from the
top-level trade ID and maker order ID. If an old payload has no maker order ID,
the normalizer falls back to the existing deterministic maker-leg index. The
same normalizer is used for WebSocket and REST payloads, so a REST replay of a
WebSocket fill is rejected by the existing SQLite primary-key dedupe gate.

### Confirmed trade backfill

`ExecutionGateway` will expose a fail-closed authenticated trade-history read.
It will validate that the SDK result is a list of dictionaries and raise
`GatewayReadError` for transport, pagination, or schema failure. The engine will
scope normalized maker legs to configured tokens and the configured funder.

The engine will reconcile confirmed trades:

1. At startup, before the authoritative position snapshot and before any quote
   task starts.
2. At the start of each periodic reconcile, before positions and open orders.
3. Immediately after a user WebSocket reconnect through the existing forced
   reconcile signal.

The first sync queries from the start of the current UTC day. Subsequent syncs
use a persisted last-success timestamp with an overlap window. Overlap makes
late confirmations visible; SQLite fill IDs make replay harmless. If a regular
incremental sync cannot explain a position change, the engine retries once from
the UTC-day boundary before declaring state unknown.

Only `CONFIRMED` REST legs become durable fills. `MATCHED`, `MINED`, and
`RETRYING` are not treated as settled backfill because they may still fail.
WebSocket processing remains optimistic at `MATCHED` and reverses a `FAILED`
event. A directly observed `CONFIRMED` event with no prior `MATCHED` must apply
the fill, covering connection gaps and process restarts.

### Position agreement and fail-closed behavior

For an initialized ledger, the engine compares the position produced by the
existing state plus newly discovered confirmed fills with the authoritative
position snapshot. A mismatch means there is an unsettled, missing, malformed,
or otherwise unexplained trade. The engine updates inventory to the
authoritative size for exposure safety, sets `STATE_UNKNOWN`, cancels managed
orders, and does not resume quoting until a full-day trade replay and a later
position snapshot agree.

The first sync after this migration is a repair mode because the existing
position table may already contain REST-restored inventory without its fill.
It imports missing confirmed fills, then allows the position snapshot to correct
the transient double application. Once the first trade checkpoint is persisted,
all later reconciles enforce strict agreement.

Trade read failure is equivalent to a positions or open-orders read failure:
the engine enters `STATE_UNKNOWN`, stops new orders, and retries with the existing
exponential backoff.

### Cash ledger and crash consistency

The `fills` table is the durable source of strategy cash movement:

```text
BUY  cash flow = -(price * size)
SELL cash flow = +(price * size)
```

`RiskManager` will derive cumulative net cash from persisted fills on startup
instead of trusting the duplicated `risk_state.net_cash` value. It retains
`net_cash` in `risk_state` for operational visibility and compatibility, but
repairs that cached value from the fill ledger whenever the process starts.

This closes the crash window where `StateStore.apply_fill()` commits but the
subsequent risk callback does not. A new fill or reversal clears any restored
daily-PnL cache before persistence. When `FAILED` reverses an optimistic fill,
the reverse fill also reaches the risk callback, so position and cash reverse
together.

Daily PnL remains mark-to-market strategy equity relative to the UTC baseline:

```text
strategy equity = cumulative fill cash flow + marked configured inventory
daily PnL       = strategy equity - UTC day-start strategy equity
```

### Persistence

SQLite will gain a small synchronization-state table holding the successful
trade reconcile timestamp and whether initial ledger repair has completed. The
schema migration is additive and idempotent. Existing fill, position, PnL, and
risk rows remain intact.

Checkpoint advancement occurs only after a valid trade snapshot has been fully
parsed and applied. A partial or malformed response cannot move the checkpoint.

## Recovery of the Current VPS State

The production service remains stopped during implementation and deployment.
Before deploying, the current database is copied to a timestamped backup outside
the repository.

On the first startup with the fix:

1. The engine reads the confirmed 50-share maker leg from CLOB trade history.
2. The canonical fill is inserted once and contributes -6.15 pUSD cash.
3. The authoritative positions snapshot settles inventory at 50 shares with an
   average price of 0.123.
4. The current mark values inventory near 6.125 pUSD.
5. Daily mark-to-market PnL becomes approximately -0.025 pUSD rather than
   approximately +6.17 pUSD.
6. Only after trade, position, order, and market checks succeed may quote tasks
   start.

## Testing

Offline tests will cover:

- Signature type 3 matches a maker leg by funder rather than signer.
- EOA wallets still match because funder falls back to signer.
- WebSocket and REST observations generate the same canonical fill ID.
- A `CONFIRMED` event without an earlier `MATCHED` applies exactly once.
- A `MATCHED` then `CONFIRMED` sequence remains exactly once.
- A `FAILED` event reverses both position and cash.
- Restart derives cash from persisted fills and repairs stale risk cache.
- Startup backfill repairs a position that REST previously restored without a
  fill.
- Periodic backfill imports a missed confirmed fill before position reconcile.
- Trade-history transport or schema failure enters `STATE_UNKNOWN` and prevents
  placement.
- An unexplained post-migration position mismatch prevents quote recovery.
- Existing fill replay, reservation, kill persistence, startup cancellation,
  and live-test safety tests continue to pass.

Deployment verification will require the full local test, lint, type-check, and
diff checks, followed by VPS offline tests. The service may then start once. Its
startup log and SQLite state must show one recovered fill, approximately -6.15
pUSD net cash, 50 shares, no kill state, and plausible mark-to-market PnL before
new orders are accepted. A final authoritative doctor check must report READY.

## Operational Safety

- No live maker runs while the old ledger logic is deployed.
- No wallet-wide cancellation is used; shutdown and recovery remain scoped to
  configured tokens.
- No merge or market-order command is used.
- If repair results differ materially from the known 50 shares at 0.123, the
  service remains stopped for investigation.
