# Operator's field guide

Practical notes for running this bot live — where to look, what breaks, and what
to build next. Written from a real supervised session (Newsom 2028 + Romania PM),
so the failure modes below are ones that actually bit us, not hypotheticals.

## Running & watching

- **Run exactly ONE engine.** `uv run polymaker run` is paper mode; LIVE requires
  `uv run polymaker run --live --confirm-live`. If you background it, verify
  with `pgrep -f "polymaker run"` and `grep -c engine_started <logfile>` — two
  engines on the same wallet race each other and double-order. (The process tree
  is zsh→uv→python, so ~3 procs but only **one** `engine_started` line.)
- **Capture the log** to a file (`… > live.log 2>&1`). Everything meaningful is a
  structured line: `requote … regime=… fv=… place=… cancel=… tox=… flowz=…`,
  `fill …`, `meta_refreshed`, `market_ws_dropped`, `market_halted_by_meta`.
- **Watch the stream, don't poll snapshots.** Follow the log and react the instant
  something fires — a fill, `regime=EVENT/HALTED/REDUCE_ONLY`, `tox=0.1+`, or any
  `Traceback/quoter_error/divergence`. Polling every N minutes misses the fill +
  quick move that happens between checks. (See `session/watch.py` / `monitor.py`
  from the session for a working pattern.)
- **Liveness ≠ quiet.** A silent log can mean "healthy and resting" OR "hung." Do a
  real health probe periodically: open orders on the exchange, positions on-chain,
  and that each order is inside the reward band.

## Where it goes wrong (ranked by how much it cost us)

1. **Adverse selection on thin/gapped books — the big one.** On a market with a
   sparse book (e.g. a 14¢ air-pocket below the touch), someone can shove the
   price, fill your resting bid, and leave you holding a directional bag. We rested
   $75 / 159-share orders on Romania; a seller gapped the market 0.478→0.442 and
   filled our whole bid → an oversized −$5 long we never wanted.
   **→ On thin/manipulable markets, rest the *minimum* reward-qualifying size**
   (the market's `rewardsMinSize`), not larger. A fill should be small and
   disposable. Big size only belongs on deep books you can offload into.
2. **Orders that don't actually score.** To earn rewards an order must be **≥
   `rewardsMinSize`** shares **AND within `rewardsMaxSpread` of the midpoint**.
   Below-min orders earn **zero** — easy to miss. `rewardsMinSize` also *changes*
   (we saw 50→100 live); a stale catalog value silently mis-sizes you. Let the
   engine refresh metadata from Gamma at startup, and rescan periodically.
3. **False regime signals on quiet markets.** Two we hit and fixed:
   - *False HALT*: staleness measured "time since last book update," so a quiet
     market halted itself into zero rewards. Gate on the **WS connection** liveness
     instead (it pings every 5s).
   - *False TRENDING*: on a market trading ~1×/hour, microprice jitter spikes the
     short/long vol ratio → TRENDING → size halved → half the reward, for a trend
     that doesn't exist. Raise `trend_vol_ratio` on thin markets.
4. **Churn.** Reprice/resize thresholds too tight → cancel/replace every few
   seconds → you lose queue position and get sampled out of rewards. Make it
   **sticky**: raise `reprice_ticks`, `resize_frac`, and the trend thresholds.
   Resting > reacting for a reward farmer.
5. **Fine-tick illusion.** On 0.001-tick markets (prices like 19.3¢) per-share
   spread is fractions of a cent — profit is **rewards + rebates**, not spread
   capture. A "+$4 exit" is noise; don't let it set your strategy.
6. **Stale reads.** The positions API (`data-api`) lags; during a fast move it
   showed +$1 while the real book was −$5. Trust the live book / on-chain, not the
   position endpoint mid-move.

## Getting out (exits & closing) — learned the hard way

- **You can't cleanly exit a *large* position on a thin market.** This is the flip
  side of the min-size rule: small fills unwind easily, big ones don't. To close
  159 Romania YES we either market-dumped through the gap (VWAP craters ~0.42, then
  0.33) or rested a limit near mid that **didn't fill as the market drifted away
  from us**. We ended up eating ~2 ticks of slippage to get flat. If you can't exit
  a size without moving the book, you never should have been that size.
- **Taker fees hit on market-order exits.** Maker fills (resting) pay zero, but
  closing with a market/marketable order is a *taker* — the Newsom close reported
  a gross 126.97 but only 122.86 landed (~$4.1 fee); Romania ~$1.5. Budget for it.
- **Don't dump into the gap.** On a gapped book, a plain market/FAK sell fills
  straight through the air-pocket. Floor it: sell only into the near bids and stop
  before the gap, even if it leaves a small tail to work off.
- **Separate the bot's trades from your own.** Our cash showed −$70, which looked
  alarming — but the bot only lost ~$15.50; the rest was *manual World Cup sports
  bets* on the same wallet. When tallying bot PnL, filter to the exact tokens the
  bot traded (it already scopes untracked positions out of its own state/exposure —
  do the same in your accounting). **Session result: Newsom −$5.27, Romania −$10.24
  (one bad adverse fill), total ≈ −$15.51** — over-sizing a thin market cost ~$10
  of that, which min-size would have made ~−$3.

## Economics (set expectations)

- **Liquidity rewards** = a *fixed* daily pool split by your Qmin share. Diminishing
  returns — past ~a third of the pool you're fighting yourself. Sweet spot is a
  small-to-mid size on a market with a real pool and light competition.
- **Maker rebates** = 25% (most markets) or 20% (high-fee) of taker fees,
  **uncapped** and volume-driven. Modest on quiet markets, dominant on busy ones —
  but you only earn them on orders that **fill**, so they come coupled with
  inventory/adverse-selection risk.
- **Taker fee** = `rate × p(1-p)` per share. `rate` is 0.04 in the fee schedule and
  the client library treats it as **4%** (≈2–3% of notional). **Verify against the
  UI** — if it actually shows 0.4%, every rebate estimate is 10× too high.

## Recommended next directions

- **Backtest against historical L2 order-book data — the highest-leverage next
  step.** Every parameter we tuned this session (churn thresholds, `trend_vol_ratio`,
  event sensitivity, min-size, exit urgency) was fit by *intuition on live money*.
  Instead: record the market WS feed (book snapshots + deltas + trade prints) to a
  dataset, then replay it through the pure quoting/regime core (`strategy/quoting.py`
  and `strategy/regime.py` are already I/O-free and deterministic — designed for
  exactly this) to simulate fills, markouts, rewards, and PnL. Then sweep/optimize
  params per market archetype (deep-liquid vs thin-gappy) offline. Model the two
  things that actually decide profit: **fill probability** (are we at the touch when
  a taker crosses?) and **adverse selection** (where's the price 30–60s after a
  fill?). This turns the live losses above into a one-time data-collection cost.
- **Confirm the fee rate** (4% vs 0.4%) from a real taker fill / the UI — it 10×'s
  all rebate math.
- **Per-fill markout logging** + a **reward-band watchdog** alert (fires if a
  resting order drifts outside the band, i.e. stops scoring). We were half-blind to
  toxicity until we added `tox`/`flowz` to the requote line — go further.
- **Refine rebate estimates** with each shortlisted market's *actual* `/trades`
  volume (the scanner uses Gamma's 24h figure, which overstates CLOB flow).
- **Wire the alerts webhook** (`Alerter`) before any unattended run.
- **Market selection**: rank by reward pool + rebate pool, prefer *deep* books and
  *light* competition; treat thin gapped books as min-size-only.
- **Exit tuning per market**; consider re-enabling the merge path for hedged
  YES+NO pairs (currently gated off for deposit wallets).

## Quick reference

```bash
uv run polymaker scan          # discover + rank markets -> markets.csv, state.db
uv run polymaker doctor        # preflight: wallet, clock, WS, balances
uv run polymaker moneydoctor --confirm-live  # live self-test (spends a little)
uv run polymaker run           # paper maker (ONE instance)
uv run polymaker run --live --confirm-live  # LIVE maker (ONE instance)
uv run polymaker cancel-all    # pull every resting order
uv run polymaker halt          # persist kill switch; scoped to configured tokens
uv run polymaker resume --confirm
uv run polymaker backtest journal/paper.jsonl  # offline L2 replay; no wallet calls
```

Config lives in `config/*.toml`: `config.toml` (wallet/engine/risk), `strategy.toml`
(named profiles), `markets.toml` (trade list). A heartbeat dead-man switch cancels
all orders within ~10s if the engine dies — but that is a safety net, not a reason
to leave it unwatched on a thin market.
