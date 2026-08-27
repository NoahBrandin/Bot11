"""Thin async client for the piece of Polymarket's Gamma REST API we need:
resolving a BTC 5-min window slug to its condition ID and CLOB token IDs.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"

# Length of a Polymarket BTC UP/DOWN window in seconds (5 minutes). Lives
# here rather than in window_tracker.py because it's needed to derive a
# fetched market's window_start from its endDate -- window_tracker.py
# imports this constant instead of redefining it, and DatastreamLayer passes
# the same window_seconds value to both GammaClient and WindowTracker, so
# the two can never drift apart.
DEFAULT_WINDOW_SECONDS = 300


@dataclass(frozen=True, slots=True)
class WindowMarket:
    slug: str
    condition_id: str
    window_start: float
    window_end: float
    up_token_id: str
    down_token_id: str
    # Only meaningful for historical/backtest lookups -- a live window is
    # never closed at fetch time. up/down are the resolved $0/$1 payouts.
    closed: bool = False
    up_payout: Optional[float] = None
    down_payout: Optional[float] = None


class GammaClient:
    def __init__(self, session: aiohttp.ClientSession, window_seconds: float = DEFAULT_WINDOW_SECONDS) -> None:
        self._session = session
        self._window_seconds = window_seconds

    async def fetch_window_market(self, slug: str) -> WindowMarket | None:
        async with self._session.get(GAMMA_EVENTS_URL, params={"slug": slug}) as resp:
            resp.raise_for_status()
            events = await resp.json()

        if not events:
            return None

        market = events[0]["markets"][0]
        token_ids = _maybe_json(market["clobTokenIds"])
        outcomes = _maybe_json(market["outcomes"])

        up_index = outcomes.index("Up")
        down_index = outcomes.index("Down")

        window_end = _parse_iso(market["endDate"])
        window_start = window_end - self._window_seconds

        up_payout = down_payout = None
        outcome_prices_raw = market.get("outcomePrices")
        if outcome_prices_raw:
            outcome_prices = _maybe_json(outcome_prices_raw)
            up_payout = float(outcome_prices[up_index])
            down_payout = float(outcome_prices[down_index])

        return WindowMarket(
            slug=slug,
            condition_id=market["conditionId"],
            window_start=window_start,
            window_end=window_end,
            up_token_id=token_ids[up_index],
            down_token_id=token_ids[down_index],
            closed=bool(market.get("closed", False)),
            up_payout=up_payout,
            down_payout=down_payout,
        )


def _maybe_json(value):
    return json.loads(value) if isinstance(value, str) else value


def _parse_iso(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()
