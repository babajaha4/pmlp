# poly-maker

A maker-only market-making bot for **Polymarket CLOB V2**, focused on political
markets. Single async process, local-file config, typed and
tested.

> [!WARNING]
> Market making on Polymarket is competitive and can lose money. This is a
> reference implementation and a research harness, not a guaranteed-profitable
> product. Test in `--paper` mode first; go live with small size.

## What it does

- Discovers political markets via the **Gamma API** (seconds) and ranks them by
  reward + rebate income vs. volatility/spread risk.
- Maintains a live order book per token from the **market WebSocket**.
- Quotes **maker-only** — every order is post-only. Fair-value + inventory-skew
  strategy that posts BUY-YES and BUY-NO as a two-sided quote, with live
  volatility/toxicity estimation and a regime machine that pulls quotes during
  news events (see [Strategy](#strategy)).
- Reconciles a target quote set against live orders with churn tolerances; runs
  the exchange **heartbeat** dead-man switch; enforces risk caps and a daily-loss
  kill switch.
- Config, market selection, and state are **local files + SQLite**. An operator
  with the repo, a `.env`, and a funded wallet is a complete deployment.

## Install

Uses [uv](https://docs.astral.sh/uv/) and Python 3.12+.

```bash
uv sync                      # install runtime + default dev/test tools
uv run polymaker --help
```

The default `dev` dependency group includes pytest, async and HTTP test support,
coverage, Ruff, and Mypy, so a normal deployment `uv sync` restores the complete
test environment. Use `uv sync --no-dev` only for an intentionally runtime-only
environment.

## Configure

```bash
cp .env.example .env         # then edit two values:
```

- `PK` — the private key of your signer wallet
- `BROWSER_ADDRESS` — your Polymarket address (shown on the profile / developer page)

Everything else is TOML under [`config/`](config/):

- `config.toml` — wallet/engine/risk/execution settings
- `strategy.toml` — named parameter profiles (`political-longdated`, `political-hot`)
- `markets.toml` — the trade list (populated via the CLI below)

## Use

```bash
# 1. discover + rank political markets (writes to state.db)
uv run polymaker scan
uv run polymaker markets

# 2. add markets to the trade list
uv run polymaker markets-add <slug> --profile political-longdated

# 3. dry run: full pipeline against the live feed, no orders posted
uv run polymaker run              # paper is the safe default
uv run polymaker run --paper      # explicit paper mode

# 4. preflight the wallet before going live
uv run polymaker doctor

# 5. self-tests: both commands are LIVE and require an explicit acknowledgement
uv run polymaker livetest --market <enabled-market-slug> --notional 5 --confirm-live
                                               # deep post-only order, then confirmed cleanup
uv run polymaker moneydoctor --confirm-live   # limit rest + market buy + market sell

# 6. go live (requires both flags; otherwise the command refuses to start)
uv run polymaker run --live --confirm-live

# ops
uv run polymaker status        # positions / open orders
uv run polymaker cancel-all    # panic button
uv run polymaker halt           # persist kill switch + cancel only configured tokens
uv run polymaker resume --confirm  # preflight, then clear kill switch
uv run polymaker control-panel    # loopback VPS dashboard + start/stop/restart

# offline research: replay captured L2 without wallet or network access
uv run polymaker backtest journal/paper.jsonl
uv run polymaker backtest journal/paper.jsonl --queue-ahead 1 --latency-ms 250 --json
```

### Local control panel

On the Windows operator machine, `control-panel` uses the existing Bitvise
profile to read authoritative VPS state and control only
`polymaker-live.service`:

```powershell
uv run polymaker control-panel
# http://127.0.0.1:8765
```

The defaults match the production setup used by this repository:

```text
sexec:      C:\Program Files (x86)\Bitvise SSH Client\sexec.exe
profile:    %USERPROFILE%\Desktop\malai.tlp
VPS repo:   /home/ubuntu/pmlp
config:     livecfg
unit:       polymaker-live.service
```

Override any of them with `--sexec`, `--profile`, `--remote-dir`,
`--remote-config-dir`, or `--unit`. The HTTP server refuses non-loopback bind
addresses. Service actions require a same-origin request, a per-process random
control token, and an operator confirmation dialog.

The dashboard shows exchange-authoritative collateral, positions and open
orders; read-only SQLite fills and risk state; current midpoint MTM; reservation
headroom; current market regime; and every effective strategy profile. It
refreshes every 20 seconds by default. Snapshot collection sends a compressed
read-only Python probe through SSH and does not install files or write the VPS
database.

`Stop` sends `SIGINT`, so normal shutdown cancels only configured-token orders
before exiting. `Restart` performs that same graceful shutdown and then starts
one fresh LIVE process. `Start` recreates the transient unit with
`--live --confirm-live` when it has been unloaded. None of these buttons calls
the wallet-wide `cancel-all`, clears a kill state, changes strategy parameters,
or bypasses startup reconciliation.

### Reward-aware and anti-sniping overlays

The production Python engine keeps its existing fail-closed execution and
ledger path, while live profiles enable two pure-strategy overlays imported
from the reference LP tool:

- `reward_aware_placement` uses positive-depth levels inside the current
  liquidity-reward band. Fine ticks target the middle of the band; coarse ticks
  choose a depth-backed level rather than blindly joining the touch. If the
  snapshot has insufficient depth, quoting falls back to the existing target.
- `anti_sniping_enabled` filters fair value with an EMA plus rolling median,
  pauses after a material midpoint jump, and requires a stable confirmation
  period before returning from `EVENT`.
- `fill_cooldown_s` suppresses new BUY quotes for the just-filled token for a
  short period, while maker-only exits remain available.
- `max_reprice_ticks_per_update` limits how far a resting order can chase a
  single update. Risk reservations and post-only checks still run afterwards.
- `reward_only_entries` makes reward metadata an entry gate: a market with zero
  reward rate, zero reward band, or a reward minimum that cannot fit the active
  reservation caps receives no new BUY orders. Existing inventory can still
  leave through maker-only SELL orders. A risk-scaled order is dropped rather
  than left below `rewardsMinSize`, because an order that is posted but below
  the reward floor does not earn the intended incentive.

These settings are per-profile in `config/strategy.toml` and are surfaced in
the local control panel. They do not change wallet identity, order signing,
managed-token cancellation, or the persistent risk/ledger safeguards.

### Journal replay assumptions

`backtest` reruns the configured strategy over timestamp-ordered `book`,
`price_change`, and `last_trade_price` events. Maker fills are conservative: a
trade must cross the quote, or observed volume at the quote must first consume
the configured visible queue ahead. The default assumes 100% of displayed size
is ahead and applies 250 ms quote latency. `--queue-ahead 0` is an optimistic
upper-bound scenario, not the default.

Trading mark-to-market PnL is reported separately from incentive estimates.
Maker rebates use simulated fill notional and configured fee metadata. Liquidity
rewards use a time-weighted L2 score-share approximation against visible in-band
depth; they are not settled rewards and must not be treated as realized income.
Adverse-selection markout defaults to 300 seconds and is omitted when the
journal ends before the horizon.

## Architecture

```
market WS ─▶ OrderBook ─▶ (wake) ─▶ Quoter ─▶ strategy (pure) ─▶ reconcile ─▶ ExecutionGateway
user WS   ─▶ StateStore                                         RiskManager ┘   (post-only, heartbeat)
Gamma     ─▶ Catalog/scanner ─▶ SQLite            periodic REST reconcile ┘
```

One async event loop. The strategy layer is a pure function `(book, inventory,
params, clock) → TargetQuotes` — deterministic and unit-tested. The engine owns
all I/O and state around it; the `ExecutionGateway` wraps `py-clob-client-v2`
(which handles the V2 EIP-712 signing) and offloads its blocking calls to a
thread pool so the hot path never stalls. State (positions, orders, PnL, catalog)
lives in one SQLite file; raw WS/order events are journaled to `journal/` for
replay.

### Authoritative fill ledger and recovery

For signature type 3, the maker identity in a trade is the configured funder
(the Polymarket Deposit Wallet), not the signing EOA. Authenticated CLOB trade
history repairs confirmed fills missed by the user WebSocket. WebSocket and REST
observations share a persistent fill identity, so replaying the same maker leg
does not apply its position or cash movement twice. Live wallet configuration
requires both `PK` and `BROWSER_ADDRESS`; for an EOA wallet, set
`BROWSER_ADDRESS` explicitly to the signer address.

Durable fills are the authority for strategy cash: a BUY contributes
`-(price * size)` and a SELL contributes `+(price * size)`. Daily PnL is the
mark-to-market change in strategy equity from the UTC day-start baseline, where
strategy equity is cumulative fill cash flow plus marked configured inventory.
Position snapshots are authoritative for exposure, but they never invent cash.

Startup and periodic authoritative reconciliation always read in this order:

```text
authenticated confirmed trades -> positions -> managed open orders
```

Ordinary incremental trade reads start at the later of the current UTC-day
boundary and the last successful checkpoint minus 300 seconds, so the overlap
can be shorter near UTC rollover. Separately, every observed but unresolved
trade identity is persisted. Its recovery query reaches back to the identity's
earliest observed timestamp minus 300 seconds across UTC rollover and process
restart.

If any trade, position, or order read fails; pending metadata is invalid; or
trades cannot explain the position snapshot, the engine enters `STATE_UNKNOWN`,
stops placing orders, and cancels every wallet order on configured tokens. Only
orders on unconfigured tokens are preserved; a manual or other-strategy order
sharing a configured token can also be cancelled. The engine resumes only after
a complete authoritative trade/position/order cycle succeeds; a position
mismatch also requires a full UTC-day trade replay to agree. Pending trades
unresolved for more than seven days remain fail-closed and require operator
review.

## Strategy

Maker-only, quoting both sides of each market as USDC-collateralized bids:

- **Fair value** — depth-weighted microprice off the live book, nudged by an
  EWMA of signed trade flow.
- **Quote construction** — reservation price `r = FV − skew(inventory)`;
  half-spread `δ = base + c_vol·σ + c_tox·toxicity`. Post **BUY-YES at `r − δ`**
  and **BUY-NO at `(1 − r) − δ`**. Because both legs are bids that sum below 1,
  a filled pair merges back to USDC at locked edge `1 − p − q` — a maker-only
  exit that never crosses the spread.
- **Inventory skew** — net position leans both quotes: long YES → bid YES lower,
  bid NO higher (acquire the offsetting leg). Size tapers as inventory approaches
  a soft cap, then the adding side is pulled entirely.
- **Aged inventory exits** — held inventory is offered maker-only, walking from
  fair value toward the touch over `exit_urgency_s`. The default is one hour;
  shorter windows should be justified by replay because they can crystallize
  adverse-selection losses.
- **Volatility / toxicity** — realized-vol and per-fill markout (adverse
  selection) EWMAs widen the spread and shrink size in markets that pick us off.
- **Regime machine** — per market: `QUIET` (farm rewards in-band), `TRENDING`
  (lean + widen + half size), `EVENT` (sweep/jump detected → pull quotes, cool
  off), `REDUCE_ONLY` (inventory cap / near end date → exits only), `HALTED`
  (stale data / resolved / kill switch → cancel all).
- **Rewards + rebates** — quotes stay inside the liquidity-rewards band in QUIET;
  reward-only live profiles allocate scarce reservation headroom by reward per
  dollar and skip markets whose minimum qualifying size exceeds the configured
  market/event caps. The market selector also scores the new maker-rebate
  program (a share of taker fees rebated to makers).
- **Risk** — per-market notional cap, neg-risk event-group worst-case cap, total
  exposure cap, daily-loss kill switch, WS-staleness halt.

Tune it all via profiles in `config/strategy.toml`.

## Develop

```bash
uv run pytest                 # unit suite (offline)
POLYMAKER_LIVE=1 uv run pytest tests/test_live_marketdata.py   # live WS integration
uv run ruff check src tests   # lint
uv run mypy src               # types (strict)
```

## Status

Implemented and live-verified end to end (auth → book → strategy → sign → post →
cancel): config, catalog/scanner, order book + analytics, strategy (FV,
vol/toxicity, regime, quoting), state store + lifecycle, execution gateway +
reconciler + heartbeat, market/user websockets, risk manager, merger, engine,
CLI, paper mode, journal capture/replay, conservative maker-fill simulation,
and fail-closed live safety gates. The offline suite is the source of truth for
test counts; run `uv run pytest` locally.

Not yet built: a continuously stateful paper fill engine and external data feeds
(polls / news / cross-venue). Automatic YES+NO merging is disabled by default;
enable it only after verifying the configured chain contracts and builder
relayer credentials. Inventory exits remain maker-only limit sells by default.

## License

MIT
