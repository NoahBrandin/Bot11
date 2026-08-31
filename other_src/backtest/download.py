"""Downloads historical BTC price data (Binance only) and synthesizes it
into the same recorder JSON-Lines format server_src/utils/record_datastream.py
produces, so other_src/backtest/recording.py + engine.py can replay it
completely unmodified -- no Polymarket market/CLOB data, no Gamma
dependency. We only need BTC price data to test/calibrate the model's own
mu/sigma (Brier score against the real resolved outcome), not to simulate
fills/P&L against a real order book.

Chainlink's TWAP settlement stream has no historical REST API (see
chainlink_feed.py's module docstring -- it's a live SDK subscription only),
so real historical Chainlink prices can't be downloaded after the fact.
Instead, each 5-min window boundary's reference/settlement price is a
60-second TWAP computed directly from the downloaded 1s Binance ticks (mean
close price over the trailing 60s) -- matching the *shape* of Polymarket's
actual 60s-Chainlink-TWAP resolution rule (same module) as closely as
BTC-only data allows, rather than a single point price. These are emitted
as synthetic chainlink_price-shaped records, so recording.py's existing
resolution logic (tick at window_end >= tick at window_start) and the
model's own current_price/target_price capture (manager.py::
_try_capture_target_price) both work completely unmodified against this
data -- only the *source* of that number differs (a Binance-derived TWAP,
not real Chainlink), a known, documented approximation, not real settlement
truth. Expect it to disagree with true Chainlink on the razor-thin-margin
windows (measured ~19% of real windows move <1bp on one real recording) --
there's no way around that without real historical Chainlink data.

Note this data has no PolymarketPriceChangeEvents (no real bid/ask), so
run_backtest.py's --trade-log-output produces nothing against it --
execution.converge() only ever runs once a quote exists. Use
model_brier_report.py instead, which captures every settlement_probability_up()
call directly and doesn't need a quote at all:

    python download.py --hours 120 --output-dir ../data/downloaded_120h
    python model_brier_report.py --log-dir ../data/downloaded_120h --gbm-tick-interval 2s
"""
from __future__ import annotations

import argparse
import asyncio
import bisect
import json
import logging
import random
import time
from pathlib import Path
from statistics import fmean
from typing import Awaitable, Callable, TypeVar

import aiohttp

from datastream import slug_for

logger = logging.getLogger(__name__)

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

DEFAULT_OUTPUT_DIR = "data/downloaded"
DEFAULT_HOURS = 24
WINDOW_SECONDS = 300  # Polymarket's 5-min BTC up/down window length
TWAP_WINDOW_SECONDS = 60  # matches Polymarket's actual settlement TWAP window
# Extra 1s-tick history fetched before range_start, purely so the model
# warms up "live-restart style" while replaying (see other_src/backtest/
# recording.py's module docstring) before the first window we actually
# score: GBMEstimator needs history_size(6000) samples, reversion_mu() needs
# its own full 6000s trailing window -- both satisfied by 6000s of 1s ticks,
# +200s slack for buffer/timestamp-rounding margin.
WARMUP_SECONDS = 6200
TICK_INTERVAL = "1s"
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
            description, status, delay, attempt + 1, RETRY_ATTEMPTS,
        )
        await asyncio.sleep(delay)
    raise AssertionError("unreachable")


async def _fetch_klines_range(
    session: aiohttp.ClientSession, symbol: str, interval: str, start_ms: int, end_ms: int
) -> list[dict]:
    """Full OHLCV per candle (unlike the old download.py's t/c-only
    trimming) -- BinanceKlineEvent needs open/high/low/volume too, not just
    close, to round-trip through recording.py's _parse_event."""
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
            # candle[6] (close time) is when this price was actually
            # realized/knowable -- pairing close price with open time
            # instead would be a look-ahead leak once replayed.
            klines.append(
                {
                    "kline_open_time": candle[0] / 1000.0,
                    "kline_close_time": candle[6] / 1000.0,
                    "open": float(candle[1]),
                    "high": float(candle[2]),
                    "low": float(candle[3]),
                    "close": float(candle[4]),
                    "volume": float(candle[5]),
                }
            )
        next_cursor = batch[-1][6] + 1  # past this batch's last close_time
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if len(batch) < 1000:
            break
    return klines


def _twap_at(
    klines: list[dict], close_times: list[float], boundary: float, window_seconds: float
) -> float | None:
    """Mean close price of ticks with boundary - window_seconds < t <= boundary
    -- None if no ticks fall in that trailing window (only possible right at
    the very start of the fetched range, before WARMUP_SECONDS has elapsed).

    close_times is klines' own kline_close_time values, pre-extracted and
    sorted once by the caller -- bisecting into it is O(log n) per boundary
    instead of the O(n) full-list scan an earlier version did, which took
    ~4 minutes to resolve 2881 boundaries against 870k ticks (2881 * 870201
    linear scans) on a 10-day download; bisecting the same is sub-second."""
    lo = bisect.bisect_right(close_times, boundary - window_seconds)
    hi = bisect.bisect_right(close_times, boundary)
    if hi <= lo:
        return None
    return fmean(klines[i]["close"] for i in range(lo, hi))


def _write_event(f, event: str, timestamp: float, **fields) -> None:
    f.write(json.dumps({"event": event, "timestamp": timestamp, **fields}) + "\n")


async def download(hours: float, output_dir: str) -> None:
    now = time.time()
    range_end = now - (now % WINDOW_SECONDS)
    range_start = range_end - hours * 3600
    range_start = range_start - (range_start % WINDOW_SECONDS)

    async with aiohttp.ClientSession() as session:
        logger.info(
            "Fetching %s BTCUSDT ticks for %.1fh (%s to %s, +%ds warmup)...",
            TICK_INTERVAL, hours, range_start, range_end, WARMUP_SECONDS,
        )
        klines = await _fetch_klines_range(
            session, symbol="BTCUSDT", interval=TICK_INTERVAL,
            start_ms=int((range_start - WARMUP_SECONDS) * 1000), end_ms=int(range_end * 1000),
        )
        logger.info("Fetched %d ticks", len(klines))

    if not klines:
        raise SystemExit("No klines returned -- check the requested range and network connectivity")

    boundary_count = int(round((range_end - range_start) / WINDOW_SECONDS)) + 1
    boundaries = [range_start + i * WINDOW_SECONDS for i in range(boundary_count)]
    # _fetch_klines_range appends in increasing cursor order, so this is
    # already sorted -- bisect below relies on that.
    close_times = [k["kline_close_time"] for k in klines]
    twap_by_boundary = {b: _twap_at(klines, close_times, b, TWAP_WINDOW_SECONDS) for b in boundaries}
    missing = [b for b, v in twap_by_boundary.items() if v is None]
    if missing:
        logger.warning("%d/%d boundaries have no ticks in their trailing 60s window, left unresolved", len(missing), len(boundaries))

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "datastream.jsonl"

    windows_written = 0
    with out_path.open("w", encoding="utf-8") as f:
        for k in klines:
            _write_event(
                f, "binance_kline", k["kline_close_time"],
                symbol="BTCUSDT", interval=TICK_INTERVAL,
                kline_open_time=k["kline_open_time"], kline_close_time=k["kline_close_time"],
                open=k["open"], high=k["high"], low=k["low"], close=k["close"], volume=k["volume"],
                is_closed=True,
            )

        for b in boundaries:
            twap = twap_by_boundary[b]
            if twap is not None:
                _write_event(
                    f, "chainlink_price", b,
                    symbol="btc/usd", price=twap, window_seconds=TWAP_WINDOW_SECONDS, source_timestamp=b,
                )

        for i in range(len(boundaries) - 1):
            window_start, window_end = boundaries[i], boundaries[i + 1]
            slug = slug_for(window_start)
            condition_id = f"synthetic-{slug}"
            up_token_id, down_token_id = f"{slug}-up", f"{slug}-down"
            _write_event(
                f, "window_open", window_start,
                slug=slug, condition_id=condition_id, window_start=window_start, window_end=window_end,
                up_token_id=up_token_id, down_token_id=down_token_id,
            )
            _write_event(
                f, "window_close", window_end,
                slug=slug, condition_id=condition_id, window_start=window_start, window_end=window_end,
            )
            windows_written += 1

    logger.info(
        "Wrote %s (%d ticks, %d windows, %d/%d boundaries with a TWAP)",
        out_path, len(klines), windows_written, boundary_count - len(missing), boundary_count,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--hours", type=float, default=DEFAULT_HOURS, help="How many hours back from now to download")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Directory to write datastream.jsonl into")
    args = parser.parse_args()
    asyncio.run(download(args.hours, args.output_dir))


if __name__ == "__main__":
    main()
