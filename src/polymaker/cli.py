"""polymaker command-line interface.

  polymaker scan                 sweep Gamma for political markets -> SQLite
  polymaker markets              rank/browse the catalog
  polymaker markets-add <slug>   append a market to config/markets.toml
  polymaker status               positions / open orders / PnL (reads SQLite)
  polymaker doctor               preflight: wallet auth, balances, WS reachability
  polymaker backtest <journal>   replay captured L2 data without network access
  polymaker run [--paper|--live --confirm-live]  start the market maker
  polymaker cancel-all           panic button
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.table import Table

from polymaker import __version__
from polymaker.config import Config

app = typer.Typer(
    name="polymaker",
    help="Maker-only market maker for Polymarket CLOB V2.",
    no_args_is_help=True,
    add_completion=False,
)
console = Console()


@app.command()
def version() -> None:
    """Print the polymaker version."""
    console.print(f"polymaker {__version__}")


@app.command()
def scan(
    config_dir: str = typer.Option("config", help="config directory"),
    min_liquidity: float = typer.Option(1000.0, help="minimum market liquidity (USDC)"),
    all_markets: bool = typer.Option(False, "--all", help="include non-rewards markets"),
) -> None:
    """Sweep Gamma for political markets, score, and persist to SQLite."""
    from polymaker.catalog.scanner import ScanConfig, run_scan
    from polymaker.catalog.store import CatalogStore

    cfg = Config.load(config_dir)
    store = CatalogStore(cfg.paths.db)

    async def _go() -> int:
        metas = await run_scan(store, ScanConfig(min_liquidity=min_liquidity, rewards_only=not all_markets))
        return len(metas)

    n = asyncio.run(_go())
    csv_path = Path(config_dir).parent / "markets.csv"
    written = store.export_csv(csv_path)
    console.print(f"[green]Scanned and stored {n} markets.[/green] "
                  f"Wrote [bold]{csv_path}[/bold] ({written} rows) — open it, pick markets, "
                  f"then `polymaker markets-add <slug>`.")
    store.close()


@app.command()
def markets(
    config_dir: str = typer.Option("config", help="config directory"),
    limit: int = typer.Option(25, help="rows to show"),
) -> None:
    """Show the top scored markets from the catalog."""
    from polymaker.catalog.store import CatalogStore

    cfg = Config.load(config_dir)
    store = CatalogStore(cfg.paths.db)
    rows = store.top(limit)
    if not rows:
        console.print("[yellow]Catalog empty. Run `polymaker scan` first.[/yellow]")
        raise typer.Exit()

    table = Table(title="Political markets by score")
    for col in ("score", "reward/day", "rebate/day", "spread", "tick", "neg", "question"):
        table.add_column(col, justify="right" if col != "question" else "left")
    for meta, sc in rows:
        table.add_row(
            f"{sc.score:.2f}", f"{meta.rewards_daily_rate:.0f}", f"{sc.rebate_potential:.0f}",
            f"{sc.spread:.3f}", f"{meta.tick_size:g}", "Y" if meta.neg_risk else "-",
            meta.question[:60],
        )
    console.print(table)
    console.print("\nAdd one with: [bold]polymaker markets-add <slug>[/bold]  (slugs are in the catalog)")


@app.command(name="markets-add")
def markets_add(
    slug: str,
    profile: str = typer.Option("political-longdated", help="strategy profile"),
    config_dir: str = typer.Option("config", help="config directory"),
) -> None:
    """Append a market (by slug) to config/markets.toml."""
    from polymaker.catalog.store import CatalogStore

    cfg = Config.load(config_dir)
    store = CatalogStore(cfg.paths.db)
    meta = store.get_by_slug(slug)
    store.close()
    if meta is None:
        console.print(f"[red]No market with slug {slug!r} in the catalog. Run `polymaker scan`.[/red]")
        raise typer.Exit(1)

    path = Path(config_dir) / "markets.toml"
    block = f'\n[[markets]]\nslug    = "{slug}"\nprofile = "{profile}"\nenabled = true\n'
    with path.open("a") as fh:
        fh.write(block)
    console.print(f"[green]Added[/green] {meta.question[:60]!r} to {path}")


@app.command()
def status(config_dir: str = typer.Option("config", help="config directory")) -> None:
    """Show positions, open orders, and marks from the local state DB."""
    from polymaker.state.store import StateStore

    cfg = Config.load(config_dir)
    store = StateStore(cfg.paths.db)
    snap = store.snapshot()
    console.print(f"[bold]Open orders:[/bold] {snap['open_orders']}")
    positions: dict[str, Any] = snap["positions"]  # type: ignore[assignment]
    if not positions:
        console.print("[dim]No open positions.[/dim]")
    else:
        table = Table(title="Positions")
        table.add_column("token")
        table.add_column("size", justify="right")
        table.add_column("avg", justify="right")
        for tok, p in positions.items():
            table.add_row(tok[:16] + "…", f"{p['size']:.2f}", f"{p['avg_price']:.3f}")
        console.print(table)
    store.close()


@app.command()
def pnl(config_dir: str = typer.Option("config", help="config directory")) -> None:
    """Show PnL from the recorded snapshots (equity, daily PnL, fills)."""
    import sqlite3

    cfg = Config.load(config_dir)
    conn = sqlite3.connect(cfg.paths.db)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT ts, equity, net_cash, inventory_value, daily_pnl FROM pnl_snapshots "
        "ORDER BY ts DESC LIMIT 1"
    ).fetchall()
    if not rows:
        console.print("[yellow]No PnL snapshots yet (run the engine first).[/yellow]")
    else:
        r = rows[0]
        color = "green" if r["daily_pnl"] >= 0 else "red"
        console.print(f"[bold]equity:[/bold] {r['equity']:.4f}  "
                      f"[bold]inventory:[/bold] {r['inventory_value']:.4f}  "
                      f"[bold]net cash:[/bold] {r['net_cash']:.4f}")
        console.print(f"[bold]daily PnL (mark-to-market):[/bold] [{color}]"
                      f"{r['daily_pnl']:+.4f}[/{color}] pUSD")
    nfills = conn.execute("SELECT COUNT(*) n FROM fills").fetchone()["n"]
    console.print(f"[dim]total fills recorded: {nfills}[/dim]")
    conn.close()


@app.command(name="export-csv")
def export_csv(
    config_dir: str = typer.Option("config", help="config directory"),
    out: str = typer.Option("markets.csv", help="output CSV path"),
    limit: int = typer.Option(500, help="max rows"),
) -> None:
    """Export the scored market catalog to a CSV for easy picking."""
    from polymaker.catalog.store import CatalogStore

    cfg = Config.load(config_dir)
    store = CatalogStore(cfg.paths.db)
    n = store.export_csv(out, limit)
    store.close()
    console.print(f"[green]Wrote {n} markets to {out}.[/green]")


@app.command()
def doctor(config_dir: str = typer.Option("config", help="config directory")) -> None:
    """Preflight checks: config, wallet auth, balance/allowance, WS reachability."""
    from polymaker.doctor import run_doctor

    cfg = Config.load(config_dir)
    ok = asyncio.run(run_doctor(cfg, console))
    raise typer.Exit(0 if ok else 1)


@app.command()
def run(
    config_dir: str = typer.Option("config", help="config directory"),
    paper: bool = typer.Option(False, "--paper", help="paper mode: full pipeline, no orders posted"),
    live: bool = typer.Option(False, "--live", help="enable real order placement"),
    confirm_live: bool = typer.Option(
        False, "--confirm-live", help="required acknowledgement for real order placement"
    ),
) -> None:
    """Start the market maker (paper by default)."""
    if paper and live:
        raise typer.BadParameter("--paper and --live are mutually exclusive")
    if confirm_live and not live:
        raise typer.BadParameter("--confirm-live requires --live")
    if live and not confirm_live:
        console.print("[red]LIVE mode requires both --live and --confirm-live.[/red]")
        raise typer.Exit(2)

    from polymaker.engine import Engine
    from polymaker.logging import configure

    paper_mode = not live
    cfg = Config.load(config_dir)
    configure(json_file=Path(cfg.paths.log_dir) / ("paper.jsonl" if paper_mode else "live.jsonl"))
    if cfg.engine.loop == "uvloop":
        try:
            import uvloop  # type: ignore[import-not-found]

            uvloop.install()
        except Exception:  # noqa: BLE001
            pass

    engine = Engine(cfg, paper=paper_mode)

    async def _go() -> None:
        try:
            await engine.run_forever()
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            await engine.shutdown()

    mode = "PAPER" if paper_mode else "LIVE"
    console.print(f"[bold green]Starting polymaker[/bold green] ({mode})…")
    if not paper_mode:
        wallet = cfg.secrets.browser_address or cfg.secrets.pk[:10] or "<unset>"
        console.print(
            f"[yellow]risk limits: total={cfg.risk.max_total_exposure_usdc:g} "
            f"market={cfg.risk.max_market_notional_usdc:g} "
            f"daily-kill={cfg.risk.daily_loss_kill_usdc:g}; "
            f"markets={len(cfg.enabled_markets)}; wallet={wallet[:10]}…[/yellow]"
        )
    try:
        asyncio.run(_go())
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped.[/yellow]")


@app.command()
def livetest(
    config_dir: str = typer.Option("config", help="config directory"),
    notional: float = typer.Option(5.0, help="order notional in USDC"),
    confirm_live: bool = typer.Option(False, "--confirm-live"),
) -> None:
    """Live wallet round-trip: place a deep post-only order and cancel it (~$5)."""
    from polymaker.livetest import run_livetest

    if not confirm_live:
        console.print("[red]livetest requires --confirm-live.[/red]")
        raise typer.Exit(2)
    cfg = Config.load(config_dir)
    ok = asyncio.run(run_livetest(cfg, console, notional))
    raise typer.Exit(0 if ok else 1)


@app.command()
def moneydoctor(
    config_dir: str = typer.Option("config", help="config directory"),
    confirm_live: bool = typer.Option(False, "--confirm-live"),
) -> None:
    """LIVE trading self-test: rest a limit, then market buy + sell (spends a little)."""
    from polymaker.moneydoctor import run_moneydoctor

    if not confirm_live:
        console.print("[red]moneydoctor requires --confirm-live.[/red]")
        raise typer.Exit(2)
    cfg = Config.load(config_dir)
    ok = asyncio.run(run_moneydoctor(cfg, console))
    raise typer.Exit(0 if ok else 1)


@app.command(name="cancel-all")
def cancel_all(config_dir: str = typer.Option("config", help="config directory")) -> None:
    """Cancel all open orders for the wallet (panic button)."""
    from polymaker.execution.gateway import ExecutionGateway

    cfg = Config.load(config_dir)
    console.print("[bold red]WARNING: this cancels every open order in the wallet.[/bold red]")
    gw = ExecutionGateway(cfg)

    async def _go() -> None:
        try:
            await gw.connect()
            await gw.cancel_all()
        finally:
            gw.close()

    asyncio.run(_go())
    console.print("[green]Sent cancel-all.[/green]")


@app.command()
def halt(config_dir: str = typer.Option("config", help="config directory")) -> None:
    """Persist the kill switch and cancel only this bot's managed tokens."""
    from polymaker.execution.gateway import ExecutionGateway
    from polymaker.risk.manager import RiskManager
    from polymaker.state.store import StateStore

    cfg = Config.load(config_dir)
    store = StateStore(cfg.paths.db)
    RiskManager(cfg.risk, store).kill()
    ok = True

    async def _go() -> None:
        nonlocal ok
        from polymaker.engine import Engine

        resolver = Engine(cfg, paper=True)
        try:
            await resolver._resolve_markets()
            if len(resolver.metas) != len(cfg.enabled_markets):
                ok = False
                console.print(
                    "[red]Kill switch persisted, but not all configured markets "
                    "could be resolved for scoped cancellation.[/red]"
                )
                return
            tokens = {
                token
                for meta in resolver.metas.values()
                for token in (meta.yes.token_id, meta.no.token_id)
            }
        finally:
            resolver.gateway.close()
            resolver.journal.close()
            resolver.state.close()
            resolver.catalog.close()

        gw = ExecutionGateway(cfg)
        try:
            await gw.connect()
            for token in tokens:
                ok = await gw.cancel_asset(token) and ok
        finally:
            gw.close()

    try:
        asyncio.run(_go())
    finally:
        store.close()
    if not ok:
        console.print("[red]Kill switch persisted, but one or more cancellations failed.[/red]")
        raise typer.Exit(1)
    console.print("[green]Kill switch persisted; managed orders cancelled.[/green]")


@app.command()
def resume(
    config_dir: str = typer.Option("config", help="config directory"),
    confirm: bool = typer.Option(False, "--confirm"),
) -> None:
    """Run preflight checks, then clear the persistent kill switch."""
    from polymaker.doctor import run_doctor
    from polymaker.risk.manager import RiskManager
    from polymaker.state.store import StateStore

    if not confirm:
        console.print("[red]resume requires --confirm.[/red]")
        raise typer.Exit(2)
    cfg = Config.load(config_dir)
    if not asyncio.run(run_doctor(cfg, console)):
        console.print("[red]Preflight failed; kill switch remains active.[/red]")
        raise typer.Exit(1)
    store = StateStore(cfg.paths.db)
    try:
        RiskManager(cfg.risk, store).resume()
    finally:
        store.close()
    console.print("[green]Kill switch cleared after successful preflight.[/green]")


@app.command()
def backtest(
    journal_path: Annotated[Path, typer.Argument(exists=True, dir_okay=False, readable=True)],
    config_dir: str = typer.Option("config", help="config directory"),
    queue_ahead: float = typer.Option(
        1.0, min=0.0, help="fraction of visible size assumed ahead in queue"
    ),
    latency_ms: float = typer.Option(250.0, min=0.0, help="quote activation latency"),
    markout_seconds: float = typer.Option(300.0, min=0.001, help="adverse-selection horizon"),
    json_output: bool = typer.Option(False, "--json", help="emit machine-readable JSON"),
) -> None:
    """Replay a captured L2 journal with conservative maker-fill assumptions."""
    from polymaker.backtest import (
        BacktestError,
        JournalBacktester,
        ReplayOptions,
        configured_markets,
        load_journal,
    )

    try:
        cfg = Config.load(config_dir)
        metas, profiles = configured_markets(cfg)
        events, malformed = load_journal(journal_path)
        simulator = JournalBacktester(
            cfg,
            metas,
            profiles,
            ReplayOptions(
                queue_ahead_fraction=queue_ahead,
                quote_latency_ms=latency_ms,
                markout_seconds=markout_seconds,
            ),
        )
        try:
            result = simulator.run(events, malformed_lines=malformed)
        finally:
            simulator.close()
    except (BacktestError, OSError, ValueError) as exc:
        console.print(f"[red]Backtest failed: {exc}[/red]")
        raise typer.Exit(2) from exc

    if json_output:
        console.print_json(json.dumps(result.to_dict()))
        return

    table = Table(title=f"Journal replay · {journal_path.name}")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    rows = (
        ("Duration", f"{result.duration_seconds:.1f}s"),
        ("L2 events", str(result.events)),
        ("Markets", str(result.markets)),
        ("Orders / fills", f"{result.placed_orders} / {result.fills}"),
        ("Order fill probability", f"{result.fill_probability:.2%}"),
        ("Filled notional", f"{result.filled_notional:.4f}"),
        ("Trading MTM PnL", f"{result.trading_mtm_pnl:+.4f}"),
        ("Maker rebate estimate", f"{result.maker_rebate_estimate:+.4f}"),
        ("Liquidity reward estimate", f"{result.liquidity_reward_estimate:+.4f}"),
        ("Total PnL estimate", f"{result.total_pnl_estimate:+.4f}"),
        ("Max capital at risk", f"{result.max_capital_at_risk:.4f}"),
        ("Max drawdown", f"{result.max_drawdown:.4f}"),
        (
            f"Mean {markout_seconds:g}s markout",
            "n/a" if result.mean_markout_bps is None else f"{result.mean_markout_bps:+.2f} bps",
        ),
    )
    for label, value in rows:
        table.add_row(label, value)
    console.print(table)
    if result.malformed_lines:
        console.print(f"[yellow]Ignored malformed journal lines: {result.malformed_lines}[/yellow]")
    console.print(
        "[dim]Reward/rebate figures are model estimates, not realized income. "
        "Fills require observed trade-through or queue depletion.[/dim]"
    )


if __name__ == "__main__":
    app()
