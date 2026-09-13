"""One-order live wallet test with strict market and cleanup boundaries."""

from __future__ import annotations

import asyncio
import math

from rich.console import Console

from polymaker.catalog.store import CatalogStore
from polymaker.config import Config
from polymaker.domain import MarketMeta, Quote, Side
from polymaker.execution.gateway import ExecutionGateway

_MAX_TEST_NOTIONAL_USDC = 5.0
_DEEP_OFFSET = 0.10


async def run_livetest(
    cfg: Config,
    console: Console,
    notional_usdc: float = 5.0,
    *,
    market_slug: str,
) -> bool:
    """Place one deep post-only order on an enabled market and confirm cleanup."""
    if not cfg.secrets.has_wallet:
        console.print("[red]No wallet in .env. Set PK and BROWSER_ADDRESS first.[/red]")
        return False
    if not 0 < notional_usdc <= _MAX_TEST_NOTIONAL_USDC:
        console.print(
            f"[red]Live test notional must be between 0 and "
            f"{_MAX_TEST_NOTIONAL_USDC:g} pUSD.[/red]"
        )
        return False
    if not cfg.execution.post_only:
        console.print("[red]Live test requires execution.post_only=true.[/red]")
        return False

    meta = _configured_market(cfg, market_slug)
    if meta is None:
        console.print(
            f"[red]Market {market_slug!r} is not enabled in this configuration "
            "or is missing from its catalog.[/red]"
        )
        return False

    token = meta.yes.token_id
    gw = ExecutionGateway(cfg)
    placement_attempted = False
    order_id: str | None = None
    observed_resting = False
    cleanup_ok = True
    cancel_requested = False
    try:
        await gw.connect()
        address = getattr(gw, "address", "<unknown>")
        console.print(f"[bold]Live round-trip test[/bold] on: {meta.question[:60]}")
        console.print(f"  [green]OK[/green] wallet auth - address {address[:12]}...")
        await gw.balance_allowance()

        before = await gw.open_orders()
        existing = [order for order in before if order.token_id == token]
        if existing:
            console.print(
                f"  [red]Refusing to test: {len(existing)} order(s) already open "
                "on the selected token.[/red]"
            )
            return False

        book = await gw.get_book(token)
        best_bid = book.get("best_bid", 0.0)
        if best_bid <= 0:
            console.print("  [red]No authoritative live best bid for the selected token.[/red]")
            return False

        deep_price = _deep_price(meta, best_bid)
        if deep_price is None:
            console.print(
                f"  [red]No legal price is at least {_DEEP_OFFSET:.2f} below "
                "the live best bid; select another market.[/red]"
            )
            return False
        quote = _test_quote(meta, best_bid, notional_usdc)
        if quote is None:
            minimum = meta.min_order_size * deep_price
            console.print(
                f"  [red]Exchange minimum would require about {minimum:.4f} pUSD, "
                f"above the {notional_usdc:.4f} pUSD test limit.[/red]"
            )
            return False

        console.print(
            f"  placing post-only BUY {quote.size:g} @ {quote.price:g} on YES token "
            f"({quote.price * quote.size:.4f} pUSD maximum)"
        )
        placement_attempted = True
        placement_task = asyncio.create_task(gw.place([quote], meta))
        while True:
            try:
                placed = await asyncio.shield(placement_task)
                break
            except asyncio.CancelledError:
                cancel_requested = True
        if placed:
            order_id = placed[0].order_id
            console.print(f"  [green]OK[/green] placed - order id {order_id[:16]}...")
            if not cancel_requested:
                await asyncio.sleep(2.0)
                live = await gw.open_orders()
                observed_resting = any(order.order_id == order_id for order in live)
                console.print(
                    f"  [{'green' if observed_resting else 'red'}]"
                    f"{'OK' if observed_resting else 'FAIL'}[/] order readback "
                    f"{'confirmed' if observed_resting else 'not confirmed'}"
                )
        else:
            console.print(
                "  [red]Order placement was not confirmed; quarantining the selected token.[/red]"
            )
    except asyncio.CancelledError:
        cancel_requested = True
    except Exception as exc:  # noqa: BLE001 - cleanup must run for every failure mode
        console.print(f"  [red]Live test failed:[/red] {exc}")
    finally:
        try:
            if placement_attempted:
                cleanup_task = asyncio.create_task(
                    _cancel_and_confirm(gw, token, order_id, console)
                )
                while True:
                    try:
                        cleanup_ok = await asyncio.shield(cleanup_task)
                        break
                    except asyncio.CancelledError:
                        cancel_requested = True
        finally:
            gw.close()
        if cancel_requested:
            raise asyncio.CancelledError

    ok = observed_resting and cleanup_ok
    console.print(f"\n[bold]{'ROUND-TRIP OK' if ok else 'LIVE TEST FAILED'}[/bold]")
    return ok


def _configured_market(cfg: Config, market_slug: str) -> MarketMeta | None:
    enabled = {entry.slug for entry in cfg.enabled_markets if entry.slug}
    if market_slug not in enabled:
        return None
    store = CatalogStore(cfg.paths.db)
    try:
        return store.get_by_slug(market_slug)
    finally:
        store.close()


def _deep_price(meta: MarketMeta, best_bid: float) -> float | None:
    tick = meta.tick_size
    raw_price = best_bid - _DEEP_OFFSET
    minimum_price = 2 * tick
    if raw_price < minimum_price:
        return None
    ticks = math.floor((raw_price + 1e-12) / tick)
    price = round(ticks * tick, meta.price_decimals)
    if price < minimum_price or price >= best_bid:
        return None
    return price


def _test_quote(meta: MarketMeta, best_bid: float, max_notional: float) -> Quote | None:
    price = _deep_price(meta, best_bid)
    if price is None:
        return None
    target_size = math.floor((max_notional / price) * 100) / 100
    size = max(meta.min_order_size, target_size)
    if price * size > max_notional + 1e-9:
        return None
    return Quote(meta.yes.token_id, Side.BUY, price, size)


async def _cancel_and_confirm(
    gw: ExecutionGateway,
    token: str,
    order_id: str | None,
    console: Console,
) -> bool:
    if order_id is not None:
        try:
            await gw.cancel([order_id])
        except Exception as exc:  # noqa: BLE001
            console.print(f"  [yellow]Order-id cancellation raised: {exc}[/yellow]")
        if await _confirm_token_clear(gw, token, attempts=2):
            console.print("  [green]OK[/green] order cancelled and absence confirmed")
            return True

    console.print("  [yellow]Using configured-token cancellation fallback.[/yellow]")
    for attempt in range(3):
        try:
            cancelled = await gw.cancel_asset(token)
        except Exception as exc:  # noqa: BLE001
            cancelled = False
            console.print(f"  [yellow]Token cancellation raised: {exc}[/yellow]")
        if cancelled and await _confirm_token_clear(gw, token, attempts=5):
            console.print("  [green]OK[/green] selected token has no open orders")
            return True
        if attempt < 2:
            await asyncio.sleep(2**attempt)

    console.print(
        "  [bold red]CRITICAL: unable to confirm cleanup for the selected token. "
        "Do not start the maker.[/bold red]"
    )
    return False


async def _confirm_token_clear(
    gw: ExecutionGateway,
    token: str,
    *,
    attempts: int,
) -> bool:
    for attempt in range(attempts):
        try:
            live = await gw.open_orders()
            if not any(order.token_id == token for order in live):
                return True
        except Exception:  # noqa: BLE001 - any ambiguity must remain fail-closed
            pass
        if attempt < attempts - 1:
            await asyncio.sleep(min(2**attempt, 4))
    return False
