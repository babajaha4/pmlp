"""The scanner: sweep Gamma for political markets, score, persist to SQLite.

Replaces the v1 data_updater (hour-long crawl of every order book, written to
Google Sheets). A politics-filtered sweep here is seconds and one process.
"""

from __future__ import annotations

from dataclasses import dataclass

from polymaker.catalog.gamma import (
    POLITICS_TAG_SLUG,
    GammaClient,
    parse_market,
)
from polymaker.catalog.rewards import apply_reward_snapshot, fetch_reward_markets
from polymaker.catalog.scoring import score_market
from polymaker.catalog.store import CatalogStore
from polymaker.domain import MarketMeta
from polymaker.logging import get_logger

log = get_logger("catalog.scanner")


@dataclass(frozen=True, slots=True)
class ScanConfig:
    tag_slug: str = POLITICS_TAG_SLUG
    min_liquidity: float = 1000.0
    min_volume_24hr: float = 0.0
    rewards_only: bool = True  # keep only markets in the liquidity-rewards program
    gamma_host: str = "https://gamma-api.polymarket.com"
    clob_host: str = "https://clob.polymarket.com"


async def run_scan(store: CatalogStore, cfg: ScanConfig) -> list[MarketMeta]:
    """Fetch, parse, filter, score, and persist. Returns the kept markets."""
    reward_markets = await fetch_reward_markets(cfg.clob_host, tag_slug=cfg.tag_slug)
    reward_rates = {cid: snapshot.daily_rate for cid, snapshot in reward_markets.items()}
    log.info("reward_markets_loaded", n=len(reward_markets))

    kept: list[MarketMeta] = []
    async with GammaClient(cfg.gamma_host) as gamma:
        tag_id = store.cached_tag(cfg.tag_slug) or await gamma.resolve_tag_id(cfg.tag_slug)
        if tag_id:
            store.cache_tag(cfg.tag_slug, tag_id)

        seen = 0
        async for raw in gamma.iter_markets(
            tag_id=tag_id,
            min_liquidity=cfg.min_liquidity,
            min_volume_24hr=cfg.min_volume_24hr,
        ):
            seen += 1
            meta = parse_market(raw, reward_rates)
            if meta is None:
                continue
            snapshot = reward_markets.get(meta.condition_id)
            if snapshot is not None:
                meta = apply_reward_snapshot(meta, snapshot)
            if cfg.rewards_only and meta.rewards_daily_rate <= 0:
                continue
            kept.append(meta)

    for m in kept:
        store.upsert_market(m, score_market(m))
    log.info("scan_complete", seen=seen, kept=len(kept), tag=cfg.tag_slug)
    return kept
