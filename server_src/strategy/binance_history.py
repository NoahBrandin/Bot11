"""Fetches BTC prices from Binance's REST endpoints: recent 1-minute closes
to bootstrap the Strategy-Layer's rolling price history on startup, and a
single current price as a fallback for when the live kline websocket feed
is stale or briefly disconnected (see manager.py::_resolve_reference_price).
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
