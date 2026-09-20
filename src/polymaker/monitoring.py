"""Read-only operational snapshots for the local control panel."""

from __future__ import annotations

import json
import math
import sqlite3
import subprocess
from collections import defaultdict, deque
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote

from polymaker.catalog.store import _load_meta
from polymaker.config import Config, MarketEntry, StrategyProfile
from polymaker.domain import MarketMeta, OpenOrder, Side
from polymaker.execution.gateway import ExecutionGateway
from polymaker.position_tolerance import authoritative_position_matches


class MonitoringGateway(Protocol):
    async def connect(self) -> None: ...

    async def open_orders(self) -> list[OpenOrder]: ...

    async def positions(self) -> dict[str, tuple[float, float]]: ...

    async def collateral_balance(self) -> float: ...

    async def get_book(self, token_id: str) -> dict[str, float]: ...

    def close(self) -> None: ...


def _read_only_connection(db_path: str | Path) -> sqlite3.Connection:
    resolved = Path(db_path).resolve().as_posix()
    conn = sqlite3.connect(f"file:{quote(resolved, safe='/:')}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _configured_markets(
    cfg: Config, conn: sqlite3.Connection
) -> list[tuple[MarketEntry, MarketMeta, StrategyProfile]]:
    configured: list[tuple[MarketEntry, MarketMeta, StrategyProfile]] = []
    for entry in cfg.enabled_markets:
        if entry.slug:
            row = conn.execute(
                "SELECT meta_json FROM markets WHERE slug=?", (entry.slug,)
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT meta_json FROM markets WHERE condition_id=?", (entry.condition_id,)
            ).fetchone()
        if row is None:
            raise RuntimeError(f"configured market is absent from catalog: {entry.ref}")
        configured.append((entry, _load_meta(str(row["meta_json"])), cfg.profile_for(entry)))
    return configured


def _systemd_status(unit: str) -> dict[str, Any]:
    properties = (
        "LoadState",
        "ActiveState",
        "SubState",
        "MainPID",
        "NRestarts",
        "ActiveEnterTimestamp",
    )
    result = subprocess.run(
        ["systemctl", "show", unit, *(f"--property={item}" for item in properties)],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    values: dict[str, str] = {}
    for line in result.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            values[key] = value
    return {
        "load_state": values.get("LoadState", "not-found"),
        "active_state": values.get("ActiveState", "inactive"),
        "sub_state": values.get("SubState", "dead"),
        "main_pid": int(values.get("MainPID", "0") or 0),
        "restarts": int(values.get("NRestarts", "0") or 0),
        "active_since": values.get("ActiveEnterTimestamp", ""),
    }


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _recent_runtime(log_path: Path, active_since: str) -> dict[str, Any]:
    latest_regime: dict[str, dict[str, Any]] = {}
    health = {
        "state_unknown": 0,
        "tracebacks": 0,
        "order_errors": 0,
        "reconcile_errors": 0,
        "heartbeat_ok": 0,
        "positions_ok": 0,
        "trades_ok": 0,
        "orders_ok": 0,
    }
    if not log_path.exists():
        return {"latest_regime": latest_regime, "health": health}

    active_dt: datetime | None = None
    if active_since:
        parsed = subprocess.run(
            ["date", "-d", active_since, "--iso-8601=seconds"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        active_dt = _parse_timestamp(parsed.stdout.strip())

    with log_path.open(encoding="utf-8", errors="replace") as handle:
        lines: Sequence[str] = deque(handle, maxlen=25_000)
    for line in lines:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            if "Traceback" in line:
                health["tracebacks"] += 1
            continue
        if not isinstance(event, dict):
            continue
        timestamp = _parse_timestamp(event.get("timestamp"))
        if active_dt is not None and timestamp is not None and timestamp < active_dt:
            continue
        name = str(event.get("event", ""))
        lowered = name.lower()
        if name == "requote" and event.get("cid"):
            latest_regime[str(event["cid"])] = {
                "regime": str(event.get("regime", "UNKNOWN")),
                "fair_value": _finite_or_none(event.get("fv")),
                "toxicity": _finite_or_none(event.get("tox")),
                "flow_z": _finite_or_none(event.get("flowz")),
                "timestamp": event.get("timestamp"),
            }
        health["state_unknown"] += int("state_unknown" in lowered)
        health["tracebacks"] += int("traceback" in lowered)
        health["order_errors"] += int(any(
            marker in lowered
            for marker in ("place_failed", "cancel_failed", "cancel_asset_failed", "order_error")
        ))
        health["reconcile_errors"] += int(
            "reconcile_error" in lowered or "reconcile" in lowered and "failed" in lowered
        )
        health["heartbeat_ok"] += int("/v1/heartbeats" in name and "200 OK" in name)
        health["positions_ok"] += int("data-api.polymarket.com/positions" in name and "200 OK" in name)
        health["trades_ok"] += int("clob.polymarket.com/data/trades" in name and "200 OK" in name)
        health["orders_ok"] += int("clob.polymarket.com/data/orders" in name and "200 OK" in name)
    return {"latest_regime": latest_regime, "health": health}


def _finite_or_none(value: object) -> float | None:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _fill_aggregate(
    conn: sqlite3.Connection, where: str = "", params: tuple[object, ...] = ()
) -> dict[str, float | int]:
    row = conn.execute(
        "SELECT COUNT(*) AS n, COALESCE(SUM(size),0) AS shares, "
        "COALESCE(SUM(price*size),0) AS notional, "
        "COALESCE(SUM(CASE WHEN side='SELL' THEN price*size ELSE -price*size END),0) "
        "AS cashflow, COALESCE(SUM(CASE WHEN is_maker=1 THEN 1 ELSE 0 END),0) AS maker_n "
        f"FROM fills {where}",
        params,
    ).fetchone()
    assert row is not None
    return {
        "count": int(row["n"]),
        "maker_count": int(row["maker_n"]),
        "shares": float(row["shares"]),
        "notional": float(row["notional"]),
        "net_cash": float(row["cashflow"]),
    }


def _latest_fills(
    conn: sqlite3.Connection, token_info: dict[str, dict[str, str]]
) -> list[dict[str, Any]]:
    fills: list[dict[str, Any]] = []
    rows = conn.execute(
        "SELECT token_id,side,price,size,is_maker,ts FROM fills ORDER BY ts DESC LIMIT 12"
    )
    for row in rows:
        info = token_info.get(str(row["token_id"]), {})
        fills.append({
            "market": info.get("question", "Unconfigured market"),
            "outcome": info.get("outcome", "Unknown"),
            "side": str(row["side"]),
            "price": float(row["price"]),
            "size": float(row["size"]),
            "notional": float(row["price"]) * float(row["size"]),
            "maker": bool(row["is_maker"]),
            "timestamp": datetime.fromtimestamp(float(row["ts"]), UTC).isoformat(),
        })
    return fills


def _risk_state(conn: sqlite3.Connection) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT day_key,day_start_equity,net_cash,daily_pnl,killed,manual_killed,"
        "order_attempts,order_errors,updated_ts FROM risk_state "
        "ORDER BY day_key DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row is not None else None


def _latest_pnl(conn: sqlite3.Connection) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT ts,equity,net_cash,inventory_value,daily_pnl FROM pnl_snapshots "
        "ORDER BY ts DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row is not None else None


def _strategy_payload(
    cfg: Config,
    configured: list[tuple[MarketEntry, MarketMeta, StrategyProfile]],
) -> dict[str, Any]:
    profiles = []
    for entry, meta, profile in configured:
        profiles.append({
            "market": meta.question,
            "condition_id": meta.condition_id,
            "profile": entry.profile,
            "overrides": entry.overrides,
            "parameters": {
                "base_size_usdc": profile.base_size_usdc,
                "inventory_cap_usdc": profile.q_max_usdc,
                "inventory_soft_fraction": profile.q_soft_frac,
                "layers": profile.layers,
                "minimum_edge_ticks": profile.min_edge_ticks,
                "minimum_half_spread_ticks": profile.delta_min_ticks,
                "inventory_skew_gamma": profile.gamma,
                "volatility_spread_weight": profile.c_vol,
                "toxicity_spread_weight": profile.c_tox,
                "reward_aware_placement": profile.reward_aware_placement,
                "reward_target_ratio": profile.reward_target_ratio,
                "anti_sniping_enabled": profile.anti_sniping_enabled,
                "anti_sniping_pause_seconds": profile.anti_sniping_pause_s,
                "anti_sniping_stable_confirm_seconds": profile.anti_sniping_stable_confirm_s,
                "fill_cooldown_seconds": profile.fill_cooldown_s,
                "max_reprice_ticks_per_update": profile.max_reprice_ticks_per_update,
                "trend_size_multiplier": 0.5,
                "event_cooloff_seconds": profile.event_cooloff_s,
                "exit_urgency_seconds": profile.exit_urgency_s,
            },
        })
    return {
        "maker_only": cfg.execution.post_only,
        "automatic_merge": cfg.merge.enabled,
        "heartbeat": cfg.engine.heartbeat,
        "heartbeat_interval_seconds": cfg.engine.heartbeat_interval_s,
        "reconcile_interval_seconds": cfg.engine.reconcile_interval_s,
        "profiles": profiles,
    }


async def collect_live_snapshot(
    cfg: Config,
    *,
    unit: str = "polymaker-live.service",
    gateway: MonitoringGateway | None = None,
    service_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Collect exchange truth and the local ledger without mutating SQLite."""
    conn = _read_only_connection(cfg.paths.db)
    owned_gateway = gateway is None
    live_gateway: MonitoringGateway = gateway or ExecutionGateway(cfg, paper=False)
    try:
        configured = _configured_markets(cfg, conn)
        metas = {meta.condition_id: meta for _, meta, _ in configured}
        token_info: dict[str, dict[str, str]] = {}
        for _, meta, _ in configured:
            for token in meta.tokens:
                token_info[token.token_id] = {
                    "condition_id": meta.condition_id,
                    "question": meta.question,
                    "outcome": token.outcome,
                }

        status = service_status or _systemd_status(unit)
        runtime = _recent_runtime(Path(cfg.paths.log_dir) / "live.jsonl", status["active_since"])
        await live_gateway.connect()
        orders = await live_gateway.open_orders()
        positions = await live_gateway.positions()
        collateral = await live_gateway.collateral_balance()

        books: dict[str, dict[str, float]] = {}
        warnings: list[str] = []
        for token_id in token_info:
            book = await live_gateway.get_book(token_id)
            if book:
                books[token_id] = book
            else:
                warnings.append(f"public order book unavailable for {token_id[:12]}")

        market_rows: dict[str, dict[str, Any]] = {}
        for entry, meta, _ in configured:
            regime = next(
                (
                    value
                    for short_cid, value in runtime["latest_regime"].items()
                    if meta.condition_id.startswith(short_cid)
                ),
                None,
            )
            market_rows[meta.condition_id] = {
                "condition_id": meta.condition_id,
                "market": meta.question,
                "slug": meta.slug,
                "profile": entry.profile,
                "regime": "STOPPED" if status["active_state"] != "active" else (
                    str(regime["regime"]) if regime else "WAITING"
                ),
                "fair_value": regime.get("fair_value") if regime else None,
                "toxicity": regime.get("toxicity") if regime else None,
                "flow_z": regime.get("flow_z") if regime else None,
                "regime_timestamp": regime.get("timestamp") if regime else None,
                "position_value": 0.0,
                "buy_reservation": 0.0,
                "sell_notional": 0.0,
                "exposure": 0.0,
                "exposure_limit": cfg.risk.max_market_notional_usdc,
                "positions": [],
                "order_count": 0,
            }

        position_rows: list[dict[str, Any]] = []
        inventory_value = 0.0
        for token_id, info in token_info.items():
            size, avg_price = positions.get(token_id, (0.0, 0.0))
            if size <= 0:
                continue
            position_book = books.get(token_id)
            mark = (
                (position_book["best_bid"] + position_book["best_ask"]) / 2
                if position_book
                else avg_price
            )
            value = size * mark
            inventory_value += value
            row = {
                "token": token_id[:12],
                "market": info["question"],
                "outcome": info["outcome"],
                "size": size,
                "average_price": avg_price,
                "best_bid": position_book["best_bid"] if position_book else None,
                "best_ask": position_book["best_ask"] if position_book else None,
                "mark": mark,
                "mark_source": "midpoint" if position_book else "average_price",
                "cost": size * avg_price,
                "value": value,
                "unrealized_pnl": value - size * avg_price,
            }
            position_rows.append(row)
            market_rows[info["condition_id"]]["positions"].append(row)
            market_rows[info["condition_id"]]["position_value"] += value

        order_rows: list[dict[str, Any]] = []
        total_buy = 0.0
        total_sell = 0.0
        unconfigured_orders = 0
        event_exposure: defaultdict[str, float] = defaultdict(float)
        for order in orders:
            order_info = token_info.get(order.token_id)
            notional = order.notional
            if order_info is None:
                unconfigured_orders += 1
            elif order.side is Side.BUY:
                total_buy += notional
                market_rows[order_info["condition_id"]]["buy_reservation"] += notional
            else:
                total_sell += notional
                market_rows[order_info["condition_id"]]["sell_notional"] += notional
            if order_info is not None:
                market_rows[order_info["condition_id"]]["order_count"] += 1
            order_rows.append({
                "order_id": order.order_id[:12],
                "token": order.token_id[:12],
                "market": order_info["question"] if order_info else "Unconfigured market",
                "outcome": order_info["outcome"] if order_info else "Unknown",
                "side": order.side.value,
                "price": order.price,
                "size": order.size,
                "notional": notional,
                "managed": order_info is not None,
            })

        for condition_id, market in market_rows.items():
            market["exposure"] = market["position_value"] + market["buy_reservation"]
            event_id = metas[condition_id].event_id
            if event_id:
                event_exposure[event_id] += float(market["exposure"])

        ledger_cash = float(_fill_aggregate(conn)["net_cash"])
        risk = _risk_state(conn)
        daily_baseline = float(risk["day_start_equity"]) if risk else 0.0
        equity = ledger_cash + inventory_value
        today_start = datetime.now(UTC).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).timestamp()
        ledger_positions = {
            str(row["token_id"]): float(row["signed_size"])
            for row in conn.execute(
                "SELECT token_id,SUM(CASE WHEN side='BUY' THEN size ELSE -size END) "
                "AS signed_size FROM fills GROUP BY token_id"
            )
        }
        ledger_matches = all(
            authoritative_position_matches(
                ledger_positions.get(token_id, 0.0),
                positions.get(token_id, (0.0, 0.0))[0],
            )
            for token_id in token_info
        )
        total_exposure = inventory_value + total_buy
        return {
            "queried_at": datetime.now(UTC).isoformat(),
            "service": status,
            "health": runtime["health"],
            "wallet": {
                "funder_prefix": live_gateway.funder[:10] if isinstance(live_gateway, ExecutionGateway) else "",
                "collateral_pusd": collateral,
            },
            "summary": {
                "market_count": len(configured),
                "token_count": len(token_info),
                "open_order_count": len(orders),
                "managed_order_count": len(orders) - unconfigured_orders,
                "unconfigured_order_count": unconfigured_orders,
                "buy_reservation": total_buy,
                "sell_notional": total_sell,
                "inventory_value": inventory_value,
                "net_cash": ledger_cash,
                "equity_mtm": equity,
                "daily_pnl_live": equity - daily_baseline,
                "total_exposure": total_exposure,
                "total_exposure_limit": cfg.risk.max_total_exposure_usdc,
                "ledger_matches_positions": ledger_matches,
            },
            "risk": {
                "state": risk,
                "limits": {
                    "total": cfg.risk.max_total_exposure_usdc,
                    "event_group": cfg.risk.max_event_group_loss_usdc,
                    "market": cfg.risk.max_market_notional_usdc,
                    "daily_loss": cfg.risk.daily_loss_kill_usdc,
                    "max_order_error_rate": cfg.risk.max_order_error_rate,
                },
                "event_exposure": dict(event_exposure),
            },
            "markets": list(market_rows.values()),
            "positions": position_rows,
            "orders": order_rows,
            "fills": {
                "all": _fill_aggregate(conn),
                "today_utc": _fill_aggregate(conn, "WHERE ts>=?", (today_start,)),
                "latest": _latest_fills(conn, token_info),
            },
            "latest_pnl_snapshot": _latest_pnl(conn),
            "strategy": _strategy_payload(cfg, configured),
            "warnings": warnings,
        }
    finally:
        if owned_gateway:
            live_gateway.close()
        conn.close()
