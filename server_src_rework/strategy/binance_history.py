"""Fetches BTC prices from Binance's REST endpoints: recent 1-minute closes
to bootstrap the Strategy-Layer's rolling price history on startup, and a
single current price as a general-purpose fallback for callers that need one
independent of the live kline websocket feed (e.g. if that feed is stale or
briefly disconnected). Not currently wired into StrategyLayer -- window
reference-price capture is the Datastream-Layer's responsibility now, see
datastream/window_tracker.py::_open and strategy/manager.py's module
docstring.
"""
from __future__ import annotations

import time

import aiohttp

KLINES_URL = "https://api.binance.com/api/v3/klines"
TICKER_PRICE_URL = "https://api.binance.com/api/v3/ticker/price"


async def fetch_recent_closes(symbol: str = "BTCUSDT", interval: str = "1m", count: int = 100) -> list[float]:
    async with aiohttp.ClientSession() as session:
        params = {"symbol": symbol, "interval": interval, "limit": count + 1}
        async with session.get(KLINES_URL, params=params) as resp:
            resp.raise_for_status()
            raw = await resp.json()

    now_ms = time.time() * 1000
    closed = [candle for candle in raw if candle[6] <= now_ms]
    return [float(candle[4]) for candle in closed[-count:]]


async def fetch_current_price(symbol: str = "BTCUSDT") -> float:
    """A single fresh spot price, fetched independently of the kline
    websocket stream -- used as a fallback when that stream has gone quiet
    (dropped connection, reconnect in progress) right when a price is
    actually needed, rather than trusting a value that may be minutes
    stale."""
    async with aiohttp.ClientSession() as session:
        async with session.get(TICKER_PRICE_URL, params={"symbol": symbol}) as resp:
            resp.raise_for_status()
            payload = await resp.json()
    return float(payload["price"])
