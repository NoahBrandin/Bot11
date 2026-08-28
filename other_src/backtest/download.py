"""Downloads historical BTC price data (Binance) and historical Polymarket
BTC 5-minute UP/DOWN window data (Gamma + CLOB prices-history) into a single
JSON file, for backtest/engine.py to replay.

Both data sources were confirmed live during design: Polymarket's CLOB
exposes a public, unauthenticated `/prices-history` endpoint returning real
historical trade prices per token, and closed-window metadata (including the
resolved $0/$1 payout) stays queryable indefinitely via the same
`?slug=btc-updown-5m-<window_start>` lookup the live datastream layer uses.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from typing import Awaitable, Callable, Optional, TypeVar

import aiohttp

from datastream.utils.gamma_client import DEFAULT_WINDOW_SECONDS as WINDOW_SECONDS
from datastream.utils.gamma_client import GammaClient
from datastream import slug_for

logger = logging.getLogger(__name__)

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
CLOB_PRICES_HISTORY_URL = "https://clob.polymarket.com/prices-history"

DEFAULT_OUTPUT_PATH = "backtest_data.json"
DEFAULT_HOURS = 24
WARMUP_MINUTES = 100  # matches StrategyLayer's default GBM history_size
POST_CLOSE_GRACE_SECONDS = 90  # observed liquidation-trading window post-close
# Sub-minute price ticks, fetched only across the actual backtest range (not
# the warmup period -- warmup only feeds the GBM's per-minute mu/sigma
# history, which is 1m-close data regardless). These replay as intra-minute
# BinanceKlineEvents with is_closed=False, matching how the live kline
# websocket already updates current_price continuously and settles into
# price_history only once a minute -- see manager.py::_on_binance_kline.
TICK_INTERVAL = "1s"
# Lowered from 10 after a 7-day download at concurrency=10 hit 429 Too Many
# Requests on ~32% of window lookups -- retry-with-backoff (below) is the
# real fix, this just reduces how often it's needed.
CONCURRENCY = 5
RETRY_ATTEMPTS = 6
RETRY_BASE_DELAY = 1.0

T = TypeVar("T")


async def _with_retry(request: Callable[[], Awaitable[T]], description: str) -> T:
    """Retries on 429/5xx and connection-level timeouts/disconnects with
    exponential backoff + jitter. Anything else (4xx client errors, other
    non-HTTP exceptions) raises immediately."""
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
            description,
            status,
            delay,
            attempt + 1,
            RETRY_ATTEMPTS,
        )
        await asyncio.sleep(delay)
    raise AssertionError("unreachable")


async def download(hours: int = DEFAULT_HOURS, output_path: str = DEFAULT_OUTPUT_PATH) -> None:
    now = time.time()
    range_end = now - (now % WINDOW_SECONDS)
    range_start = range_end - hours * 3600

    async with aiohttp.ClientSession() as session:
        logger.info("Fetching Binance klines for %s to %s (+%d min warmup)...", range_start, range_end, WARMUP_MINUTES)
        klines = await _fetch_klines_range(
            session,
            symbol="BTCUSDT",
            interval="1m",
            start_ms=int((range_start - WARMUP_MINUTES * 60) * 1000),
            end_ms=int(range_end * 1000),
        )
        logger.info("Fetched %d klines", len(klines))

        logger.info("Fetching %s Binance ticks for %s to %s...", TICK_INTERVAL, range_start, range_end)
        ticks = await _fetch_klines_range(
            session,
            symbol="BTCUSDT",
            interval=TICK_INTERVAL,
            start_ms=int(range_start * 1000),
            end_ms=int(range_end * 1000),
        )
        logger.info("Fetched %d ticks", len(ticks))

        gamma = GammaClient(session)
        semaphore = asyncio.Semaphore(CONCURRENCY)
        window_count = int((range_end - range_start) // WINDOW_SECONDS)
        window_starts = [range_start + i * WINDOW_SECONDS for i in range(window_count)]

        logger.info("Fetching %d windows...", window_count)
        results = await asyncio.gather(
            *(_fetch_window(session, gamma, semaphore, ws) for ws in window_starts)
        )
        windows = [w for w in results if w is not None]
        logger.info("Fetched %d resolved windows (%d skipped)", len(windows), window_count - len(windows))

    data = {
        "generated_at": now,
        "range_start": range_start,
        "range_end": range_end,
        "binance_klines": klines,
        "binance_ticks": ticks,
        "windows": windows,
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    logger.info(
        "Wrote %s (%d klines, %d ticks, %d windows)", output_path, len(klines), len(ticks), len(windows)
    )


async def _fetch_klines_range(
    session: aiohttp.ClientSession, symbol: str, interval: str, start_ms: int, end_ms: int
) -> list[dict]:
    klines: list[dict] = []
    cursor = start_ms
    while cursor < end_ms:
        params = {"symbol": symbol, "interval": interval, "startTime": cursor, "endTime": end_ms, "limit": 1000}

        async def _do() -> list:
            async with session.get(BINANCE_KLINES_URL, params=params) as resp:
                resp.raise_for_status()
                return await resp.json()

        batch = await _with_retry(_do, "fetch Binance klines")
        if not batch:
            break
        for candle in batch:
            # candle[0] is this kline's OPEN time, candle[4] its close price --
            # pairing them (as an earlier version of this function did) dates
            # each close price ~1 interval before it was actually knowable,
            # a systematic look-ahead leak once replayed by engine.py against
            # real-time-stamped Polymarket ticks. candle[6] (close time) is
            # when that price was actually realized.
            klines.append({"t": candle[6] / 1000.0, "c": float(candle[4])})
        next_cursor = batch[-1][6] + 1  # past this batch's last close_time
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if len(batch) < 1000:
            break
    return klines


async def _fetch_window(
    session: aiohttp.ClientSession, gamma: GammaClient, semaphore: asyncio.Semaphore, window_start: float
) -> Optional[dict]:
    slug = slug_for(window_start)
    async with semaphore:
        try:
            market = await _with_retry(lambda: gamma.fetch_window_market(slug), f"fetch window market {slug}")
        except Exception:
            logger.exception("Failed fetching window market %s", slug)
            return None

    if market is None:
        logger.warning("Window market %s not found, skipping", slug)
        return None
    if not market.closed or market.up_payout is None or market.down_payout is None:
        logger.warning("Window market %s not resolved yet, skipping", slug)
        return None

    up_prices, down_prices = await asyncio.gather(
        _fetch_price_history(session, semaphore, market.up_token_id, market.window_start, market.window_end),
        _fetch_price_history(session, semaphore, market.down_token_id, market.window_start, market.window_end),
    )

    return {
        "slug": market.slug,
        "condition_id": market.condition_id,
        "window_start": market.window_start,
        "window_end": market.window_end,
        "up_token_id": market.up_token_id,
        "down_token_id": market.down_token_id,
        "resolved_outcome": "UP" if market.up_payout >= market.down_payout else "DOWN",
        "up_prices": up_prices,
        "down_prices": down_prices,
    }


async def _fetch_price_history(
    session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, token_id: str, window_start: float, window_end: float
) -> list[dict]:
    params = {
        "market": token_id,
        "startTs": int(window_start),
        "endTs": int(window_end + POST_CLOSE_GRACE_SECONDS),
        "fidelity": 1,
    }

    async def _do() -> dict:
        async with session.get(CLOB_PRICES_HISTORY_URL, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()

    async with semaphore:
        data = await _with_retry(_do, f"fetch price history {token_id}")
    return data.get("history", [])
