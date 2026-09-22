"""Authoritative public CLOB liquidity-reward market snapshots."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import Any

import httpx

from polymaker.domain import MarketMeta

PUSD_REWARD_ASSETS = frozenset({
    # Current pUSD reward asset and legacy bridged-USDC reward asset.
    "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb",
    "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
})
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class RewardMarketsReadError(RuntimeError):
    """The official reward-market snapshot could not be read completely."""


@dataclass(frozen=True, slots=True)
class RewardMarketSnapshot:
    condition_id: str
    daily_rate: float
    min_size: float
    max_spread: float
    competitiveness: float


@dataclass(frozen=True, slots=True)
class _ActiveReward:
    condition_id: str
    daily_rate: float
    min_size: float
    max_spread: float


def apply_reward_snapshot(meta: MarketMeta, snapshot: RewardMarketSnapshot) -> MarketMeta:
    """Return metadata with all reward fields from one authoritative snapshot."""
    if meta.condition_id != snapshot.condition_id:
        raise ValueError("reward snapshot condition_id mismatch")
    return replace(
        meta,
        rewards_daily_rate=snapshot.daily_rate,
        rewards_min_size=snapshot.min_size,
        rewards_max_spread=snapshot.max_spread,
        reward_competitiveness=snapshot.competitiveness,
    )


def clear_reward_snapshot(meta: MarketMeta) -> MarketMeta:
    """Mark a market absent from a successfully completed official snapshot."""
    return replace(meta, rewards_daily_rate=0.0, reward_competitiveness=None)


async def fetch_reward_markets(
    clob_host: str = "https://clob.polymarket.com",
    timeout: float = 20.0,
    *,
    max_pages: int = 50,
    max_retries: int = 2,
    condition_ids: Iterable[str] | None = None,
    event_ids: Iterable[str] | None = None,
    tag_slug: str | None = None,
) -> dict[str, RewardMarketSnapshot]:
    """Fetch every current pUSD reward market or fail without a partial result.

    The official endpoint uses cursor pagination and ``LTE=`` as its terminal
    cursor. Returning a partial catalog would make missing markets look like
    reward removals, so pagination, schema, and numeric failures reject the
    entire snapshot.
    """
    if max_pages <= 0 or max_retries < 0:
        raise ValueError("max_pages must be positive and max_retries non-negative")

    conditions = None if condition_ids is None else frozenset(condition_ids)
    events = frozenset(event_ids or ())
    if conditions == frozenset():
        return {}
    async with httpx.AsyncClient(base_url=clob_host.rstrip("/"), timeout=timeout) as client:
        active = await _fetch_active_rewards(
            client, max_pages, max_retries, conditions
        )
        if not active:
            return {}
        competition = await _fetch_competition(
            client,
            None if conditions is None else frozenset(active),
            max_pages,
            max_retries,
            event_ids=events,
            tag_slug=tag_slug,
        )
    relevant = active.keys() & competition.keys()
    return {
        condition_id: RewardMarketSnapshot(
            condition_id=condition_id,
            daily_rate=reward.daily_rate,
            min_size=reward.min_size,
            max_spread=reward.max_spread,
            competitiveness=competition[condition_id],
        )
        for condition_id in relevant
        for reward in (active[condition_id],)
    }


async def _fetch_active_rewards(
    client: httpx.AsyncClient,
    max_pages: int,
    max_retries: int,
    conditions: frozenset[str] | None,
) -> dict[str, _ActiveReward]:
    active: dict[str, _ActiveReward] = {}
    cursor = ""
    seen_cursors: set[str] = set()
    for _ in range(max_pages):
        payload = await _get_page(
            client,
            "/rewards/markets/current",
            httpx.QueryParams({"next_cursor": cursor}),
            max_retries,
        )
        rows, next_cursor = _page_rows(payload)
        for raw in rows:
            reward = _parse_active_reward(raw)
            if reward is None:
                continue
            if conditions is not None and reward.condition_id not in conditions:
                continue
            previous = active.get(reward.condition_id)
            if previous is not None and previous != reward:
                raise RewardMarketsReadError("conflicting duplicate active reward")
            active[reward.condition_id] = reward
        if conditions is not None and active.keys() >= conditions:
            return active
        if next_cursor == "LTE=":
            return active
        cursor = _advance_cursor(next_cursor, seen_cursors)
    raise RewardMarketsReadError("active rewards pagination exceeded page limit")


async def _fetch_competition(
    client: httpx.AsyncClient,
    targets: frozenset[str] | None,
    max_pages: int,
    max_retries: int,
    *,
    event_ids: frozenset[str],
    tag_slug: str | None,
) -> dict[str, float]:
    competition: dict[str, float] = {}
    cursor = ""
    seen_cursors: set[str] = set()
    for _ in range(max_pages):
        params: list[tuple[str, str | int | float | bool | None]] = [
            ("next_cursor", cursor),
            ("page_size", 500),
            ("order_by", "rate_per_day"),
            ("position", "DESC"),
        ]
        params.extend(("event_id", event_id) for event_id in sorted(event_ids))
        if tag_slug:
            params.append(("tag_slug", tag_slug))
        payload = await _get_page(
            client,
            "/rewards/markets/multi",
            httpx.QueryParams(params),
            max_retries,
        )
        rows, next_cursor = _page_rows(payload)
        for raw in rows:
            if not isinstance(raw, dict):
                raise RewardMarketsReadError("invalid reward-market row")
            condition_id = raw.get("condition_id")
            if not isinstance(condition_id, str) or not condition_id:
                raise RewardMarketsReadError("invalid reward-market identity")
            if targets is None or condition_id in targets:
                value = _finite_number(
                    raw.get("market_competitiveness"),
                    "market_competitiveness",
                    minimum=0.0,
                )
                previous = competition.get(condition_id)
                if previous is not None and previous != value:
                    raise RewardMarketsReadError("conflicting market competitiveness")
                competition[condition_id] = value
        if targets is not None and competition.keys() >= targets:
            return competition
        if next_cursor == "LTE=":
            if targets is not None:
                missing = len(targets - competition.keys())
                raise RewardMarketsReadError(
                    f"official competition missing for {missing} active reward markets"
                )
            return competition
        cursor = _advance_cursor(next_cursor, seen_cursors)
    raise RewardMarketsReadError("competition pagination exceeded page limit")


async def _get_page(
    client: httpx.AsyncClient,
    path: str,
    params: httpx.QueryParams,
    max_retries: int,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            response = await client.get(path, params=params)
            if response.status_code in _RETRYABLE_STATUS:
                last_error = httpx.HTTPStatusError(
                    f"retryable reward-market status {response.status_code}",
                    request=response.request,
                    response=response,
                )
                if attempt < max_retries:
                    await asyncio.sleep(_retry_delay(response, attempt))
                    continue
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise RewardMarketsReadError("invalid reward-market response")
            return payload
        except RewardMarketsReadError:
            raise
        except (httpx.HTTPError, ValueError) as exc:
            last_error = exc
            if attempt < max_retries and (
                isinstance(exc, httpx.TransportError)
                or (
                    isinstance(exc, httpx.HTTPStatusError)
                    and exc.response.status_code in _RETRYABLE_STATUS
                )
            ):
                await asyncio.sleep(0.25 * (2**attempt))
                continue
            break
    raise RewardMarketsReadError("official reward-market request failed") from last_error


def _retry_delay(response: httpx.Response, attempt: int) -> float:
    raw = response.headers.get("Retry-After", "")
    try:
        delay = float(raw)
    except ValueError:
        delay = 0.25 * (2**attempt)
    return min(max(delay, 0.0), 5.0)


def _page_rows(payload: dict[str, Any]) -> tuple[list[object], str]:
    rows = payload.get("data")
    next_cursor = payload.get("next_cursor")
    if not isinstance(rows, list) or not isinstance(next_cursor, str):
        raise RewardMarketsReadError("invalid reward-market pagination schema")
    return rows, next_cursor


def _advance_cursor(next_cursor: str, seen_cursors: set[str]) -> str:
    if not next_cursor or next_cursor in seen_cursors:
        raise RewardMarketsReadError("reward-market cursor did not advance")
    seen_cursors.add(next_cursor)
    return next_cursor


def _parse_active_reward(raw: object) -> _ActiveReward | None:
    if not isinstance(raw, dict):
        raise RewardMarketsReadError("invalid reward-market row")
    condition_id = raw.get("condition_id")
    configs = raw.get("rewards_config", [])
    if not isinstance(condition_id, str) or not condition_id or not isinstance(configs, list):
        raise RewardMarketsReadError("invalid reward-market identity or rewards_config")

    total_daily_rate = raw.get("total_daily_rate")
    if total_daily_rate is not None:
        daily_rate = _finite_number(total_daily_rate, "total_daily_rate", minimum=0.0)
    else:
        daily_rate = 0.0
        for config in configs:
            if not isinstance(config, dict):
                raise RewardMarketsReadError("invalid reward config")
            if str(config.get("asset_address", "")).lower() not in PUSD_REWARD_ASSETS:
                continue
            daily_rate += _finite_number(
                config.get("rate_per_day"), "rate_per_day", minimum=0.0
            )
    if daily_rate <= 0:
        return None

    return _ActiveReward(
        condition_id=condition_id,
        daily_rate=daily_rate,
        min_size=_finite_number(raw.get("rewards_min_size"), "rewards_min_size", minimum=0.0),
        max_spread=_finite_number(
            raw.get("rewards_max_spread"), "rewards_max_spread", minimum=0.0
        ),
    )


def _finite_number(value: object, field: str, *, minimum: float) -> float:
    if isinstance(value, bool):
        raise RewardMarketsReadError(f"invalid {field}")
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise RewardMarketsReadError(f"invalid {field}") from exc
    if not math.isfinite(number) or number < minimum:
        raise RewardMarketsReadError(f"invalid {field}")
    return number
