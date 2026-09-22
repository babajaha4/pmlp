"""Official reward-market snapshot parsing and pagination tests."""

from __future__ import annotations

import httpx
import pytest
import respx

from polymaker.catalog.rewards import RewardMarketsReadError, fetch_reward_markets


def _market(
    condition_id: str,
    *,
    rate: float = 25.0,
    competitiveness: float = 50.0,
) -> dict[str, object]:
    return {
        "condition_id": condition_id,
        "market_competitiveness": competitiveness,
        "rewards_min_size": 10.0,
        "rewards_max_spread": 3.0,
        "rewards_config": [{
            "asset_address": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            "rate_per_day": rate,
        }],
    }


@pytest.mark.asyncio
@respx.mock
async def test_fetch_reward_markets_paginates_and_parses_official_fields() -> None:
    current = respx.get("https://clob.test/rewards/markets/current")
    current.side_effect = [
        httpx.Response(200, json={"data": [_market("one")], "next_cursor": "next"}),
        httpx.Response(200, json={"data": [_market("two", rate=30, competitiveness=5)],
                                  "next_cursor": "LTE="}),
    ]
    multi = respx.get("https://clob.test/rewards/markets/multi")
    multi.side_effect = [
        httpx.Response(200, json={"data": [_market("one")], "next_cursor": "more"}),
        httpx.Response(200, json={"data": [_market("two", competitiveness=5)],
                                  "next_cursor": "LTE="}),
    ]

    snapshots = await fetch_reward_markets("https://clob.test", max_retries=0)

    assert set(snapshots) == {"one", "two"}
    assert snapshots["two"].daily_rate == 30.0
    assert snapshots["two"].competitiveness == 5.0
    assert snapshots["two"].min_size == 10.0
    assert snapshots["two"].max_spread == 3.0
    assert current.call_count == 2
    assert current.calls[1].request.url.params["next_cursor"] == "next"
    assert multi.call_count == 2
    assert multi.calls[0].request.url.params["page_size"] == "500"
    assert multi.calls[0].request.url.params["order_by"] == "rate_per_day"
    assert multi.calls[1].request.url.params["next_cursor"] == "more"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_reward_markets_skips_markets_without_active_pusd_pool() -> None:
    no_pool = _market("none")
    no_pool["rewards_config"] = []
    respx.get("https://clob.test/rewards/markets/current").mock(
        return_value=httpx.Response(200, json={"data": [no_pool], "next_cursor": "LTE="})
    )

    assert await fetch_reward_markets("https://clob.test", max_retries=0) == {}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_reward_markets_accepts_current_pusd_asset_and_total_rate() -> None:
    current = _market("current", rate=1.0)
    current["total_daily_rate"] = 44.0
    current["rewards_config"] = [{
        "asset_address": "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB",
        "rate_per_day": 40.0,
    }]
    respx.get("https://clob.test/rewards/markets/current").mock(
        return_value=httpx.Response(200, json={"data": [current], "next_cursor": "LTE="})
    )
    respx.get("https://clob.test/rewards/markets/multi").mock(
        return_value=httpx.Response(
            200,
            json={"data": [_market("current", competitiveness=8.0)],
                  "next_cursor": "LTE="},
        )
    )

    snapshots = await fetch_reward_markets("https://clob.test", max_retries=0)

    assert snapshots["current"].daily_rate == 44.0
    assert snapshots["current"].competitiveness == 8.0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_reward_markets_scopes_competition_to_configured_events() -> None:
    current = respx.get("https://clob.test/rewards/markets/current")
    current.mock(return_value=httpx.Response(
        200,
        json={"data": [_market("wanted"), _market("unconfigured")],
              "next_cursor": "LTE="},
    ))
    multi = respx.get("https://clob.test/rewards/markets/multi")
    multi.mock(return_value=httpx.Response(
        200,
        json={"data": [_market("wanted")], "next_cursor": "LTE="},
    ))

    snapshots = await fetch_reward_markets(
        "https://clob.test",
        max_retries=0,
        condition_ids=["wanted"],
        event_ids=["event-b", "event-a"],
    )

    assert set(snapshots) == {"wanted"}
    assert multi.calls[0].request.url.params.get_list("event_id") == ["event-a", "event-b"]


@pytest.mark.asyncio
@respx.mock
async def test_tag_scan_intersects_active_rewards_with_filtered_markets() -> None:
    respx.get("https://clob.test/rewards/markets/current").mock(
        return_value=httpx.Response(
            200,
            json={"data": [_market("politics"), _market("sports")],
                  "next_cursor": "LTE="},
        )
    )
    multi = respx.get("https://clob.test/rewards/markets/multi")
    multi.mock(return_value=httpx.Response(
        200,
        json={"data": [_market("politics")], "next_cursor": "LTE="},
    ))

    snapshots = await fetch_reward_markets(
        "https://clob.test", max_retries=0, tag_slug="politics"
    )

    assert set(snapshots) == {"politics"}
    assert multi.calls[0].request.url.params["tag_slug"] == "politics"


@pytest.mark.asyncio
@respx.mock
@pytest.mark.parametrize("competition", [-1, "bad", None])
async def test_fetch_reward_markets_rejects_invalid_competition(competition) -> None:
    raw = _market("bad")
    raw["market_competitiveness"] = competition
    respx.get("https://clob.test/rewards/markets/current").mock(
        return_value=httpx.Response(200, json={"data": [_market("bad")],
                                               "next_cursor": "LTE="})
    )
    respx.get("https://clob.test/rewards/markets/multi").mock(
        return_value=httpx.Response(200, json={"data": [raw], "next_cursor": "LTE="})
    )

    with pytest.raises(RewardMarketsReadError):
        await fetch_reward_markets("https://clob.test", max_retries=0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_reward_markets_rejects_non_finite_competition() -> None:
    respx.get("https://clob.test/rewards/markets/current").mock(
        return_value=httpx.Response(200, json={"data": [_market("bad")],
                                               "next_cursor": "LTE="})
    )
    respx.get("https://clob.test/rewards/markets/multi").mock(
        return_value=httpx.Response(
            200,
            content=(
                b'{"data":[{"condition_id":"bad","market_competitiveness":1e999,'
                b'"rewards_min_size":10,"rewards_max_spread":3,'
                b'"rewards_config":[{"asset_address":'
                b'"0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174","rate_per_day":25}]}],'
                b'"next_cursor":"LTE="}'
            ),
            headers={"content-type": "application/json"},
        )
    )

    with pytest.raises(RewardMarketsReadError):
        await fetch_reward_markets("https://clob.test", max_retries=0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_reward_markets_rejects_incomplete_or_repeating_pagination() -> None:
    respx.get("https://clob.test/rewards/markets/current").mock(
        return_value=httpx.Response(200, json={"data": [_market("one")],
                                               "next_cursor": "LTE="})
    )
    respx.get("https://clob.test/rewards/markets/multi").mock(
        return_value=httpx.Response(200, json={"data": [_market("other")],
                                               "next_cursor": "repeat"})
    )

    with pytest.raises(RewardMarketsReadError):
        await fetch_reward_markets("https://clob.test", max_pages=2, max_retries=0)
