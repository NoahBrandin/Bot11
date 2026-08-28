"""Fetches BTC prices from Binance's REST endpoints: recent closes (any
interval, any count) to bootstrap the Strategy-Layer's GBM price history on
startup (see strategy/manager.py::StrategyLayer.run), and a single current
price as a general-purpose fallback for callers that need one independent of
the live kline websocket feed (e.g. if that feed is stale or briefly
disconnected).
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Awaitable, Callable, TypeVar

import aiohttp

logger = logging.getLogger(__name__)

KLINES_URL = "https://api.binance.com/api/v3/klines"
TICKER_PRICE_URL = "https://api.binance.com/api/v3/ticker/price"

# Binance's klines endpoint hard-caps `limit` at 1000 regardless of what's
# requested -- fetch_recent_closes pages past that with startTime/endTime
# cursoring (same approach as other_src/backtest/download.py's
# _fetch_klines_range) instead of silently truncating, so a large `count`
# (e.g. gbm_tick_interval="1s" wanting thousands of ticks for a deep
# calibration window) actually returns that many candles instead of being
# capped at 1000.
MAX_KLINES_PER_REQUEST = 1000
RETRY_ATTEMPTS = 5
RETRY_BASE_DELAY = 1.0

# Matches strategy/manager.py's _INTERVAL_SECONDS -- duplicated rather than
# imported to avoid a circular import (manager.py imports this module).
_INTERVAL_SECONDS = {"1s": 1.0, "1m": 60.0, "3m": 180.0, "5m": 300.0, "15m": 900.0, "30m": 1800.0, "1h": 3600.0}

T = TypeVar("T")


async def _with_retry(request: Callable[[], Awaitable[T]], description: str) -> T:
    """Retries on 429/5xx and connection-level timeouts/disconnects with
    exponential backoff + jitter. Anything else (4xx client errors, other
    non-HTTP exceptions) raises immediately. Mirrors other_src/backtest/
    download.py's helper of the same name -- duplicated rather than shared,
    since other_src is separate offline tooling, not part of the live
    service's import path."""
    for attempt in range(RETRY_ATTEMPTS):
        try:
            return await request()
        except aiohttp.ClientResponseError as exc:
            if exc.status != 429 and exc.status < 500:
                raise
            status, last_exc = exc.status, exc
        except (asyncio.TimeoutError, aiohttp.ClientConnectionError) as exc:
            status, last_exc = type(exc).__name__, exc
        if attempt == RETRY_ATTEMPTS - 1:
            raise last_exc
        delay = RETRY_BASE_DELAY * (2**attempt) + random.uniform(0, 0.5)
        logger.warning(
            "%s failed (status=%s), retrying in %.1fs (attempt %d/%d)",
            description, status, delay, attempt + 1, RETRY_ATTEMPTS,
        )
        await asyncio.sleep(delay)
    raise AssertionError("unreachable")


async def fetch_recent_closes(symbol: str = "BTCUSDT", interval: str = "1m", count: int = 100) -> list[float]:
    """The most recent `count` *closed* candles' close prices, oldest first.
    Pages past Binance's 1000-per-request limit via startTime/endTime
    cursoring, so `count` can be arbitrarily large -- bounded only by how far
    back `count * interval` actually reaches, not by the API's per-call cap.
    """
    interval_seconds = _INTERVAL_SECONDS.get(interval)
    if interval_seconds is None:
        raise ValueError(f"Unknown Binance interval {interval!r}")

    now = time.time()
    # +5 candles of slack: candle boundaries don't align perfectly with
    # "now", so a plain count*interval_seconds lookback can land one candle
    # short right at the edge.
    start_ms = int((now - (count + 5) * interval_seconds) * 1000)
    end_ms = int(now * 1000)

    candles: list[list] = []
    cursor = start_ms
    async with aiohttp.ClientSession() as session:
        while cursor < end_ms and len(candles) < count + 5:
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": cursor,
                "endTime": end_ms,
                "limit": MAX_KLINES_PER_REQUEST,
            }

            async def _do() -> list:
                async with session.get(KLINES_URL, params=params) as resp:
                    resp.raise_for_status()
                    return await resp.json()

            batch = await _with_retry(_do, f"fetch {symbol} {interval} klines")
            if not batch:
                break
            candles.extend(batch)
            next_cursor = batch[-1][6] + 1  # past this batch's last close_time
            if next_cursor <= cursor:
                break
            cursor = next_cursor
            if len(batch) < MAX_KLINES_PER_REQUEST:
                break

    now_ms = time.time() * 1000
    closed = [candle for candle in candles if candle[6] <= now_ms]
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
