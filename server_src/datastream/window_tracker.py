"""Tracks Polymarket BTC 5-minute window lifecycle from wall-clock time and
emits WindowOpenEvent / WindowCloseEvent onto the shared queue.

Windows are derived purely from time (aligned to the 5-minute UTC grid) --
no polling is needed to detect boundaries. Gamma REST is only used to
resolve a window's condition ID / CLOB token IDs, fetched ahead of time so
they're ready the instant a window opens.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Awaitable, Callable, Optional

from monitoring import Monitor

from .binance_feed import BinanceFeed
from .events import WindowCloseEvent, WindowOpenEvent
from .gamma_client import DEFAULT_WINDOW_SECONDS, GammaClient, WindowMarket

logger = logging.getLogger(__name__)

SLUG_PREFIX = "btc-updown-5m-"
DEFAULT_PREFETCH_LEAD_SECONDS = 60
DEFAULT_FETCH_RETRY_DELAY = 2.0

WindowCallback = Callable[[WindowMarket], Awaitable[None]]


def slug_for(window_start: float) -> str:
    return f"{SLUG_PREFIX}{int(window_start)}"


def current_window_start(now: Optional[float] = None, window_seconds: float = DEFAULT_WINDOW_SECONDS) -> float:
    now = time.time() if now is None else now
    return now - (now % window_seconds)


class WindowTracker:
    def __init__(
        self,
        queue: asyncio.Queue,
        gamma: GammaClient,
        on_window_open: Optional[WindowCallback] = None,
        on_window_close: Optional[WindowCallback] = None,
        monitor: Optional[Monitor] = None,
        window_seconds: float = DEFAULT_WINDOW_SECONDS,
        prefetch_lead_seconds: float = DEFAULT_PREFETCH_LEAD_SECONDS,
        fetch_retry_delay: float = DEFAULT_FETCH_RETRY_DELAY,
        binance_feed: Optional[BinanceFeed] = None,
    ) -> None:
        self._queue = queue
        self._gamma = gamma
        self._on_window_open = on_window_open
        self._on_window_close = on_window_close
        self._monitor = monitor or Monitor()
        self._window_seconds = window_seconds
        self._prefetch_lead_seconds = prefetch_lead_seconds
        self._fetch_retry_delay = fetch_retry_delay
        self._binance_feed = binance_feed

    async def run(self) -> None:
        market = await self._fetch_with_retry(current_window_start(window_seconds=self._window_seconds))
        await self._open(market)

        while True:
            window_end = market.window_end

            await self._sleep_until(window_end - self._prefetch_lead_seconds)
            next_market = await self._fetch_with_retry(window_end)

            await self._sleep_until(window_end)

            # Subscribe/emit the new window before tearing down the old one
            # so there's no gap in Polymarket coverage across the boundary.
            await self._open(next_market)
            await self._close(market)

            market = next_market

    async def _fetch_with_retry(self, window_start: float) -> WindowMarket:
        slug = slug_for(window_start)
        while True:
            try:
                market = await self._gamma.fetch_window_market(slug)
                if market is not None:
                    return market
                logger.warning("Window market %s not yet available, retrying", slug)
            except Exception:
                logger.exception("Failed fetching window market %s", slug)
                self._monitor.error(f"Failed fetching window market {slug}")
            await asyncio.sleep(self._fetch_retry_delay)

    async def _open(self, market: WindowMarket) -> None:
        # Always emits WindowOpenEvent immediately -- on_window_open (the
        # Polymarket subscribe) and the event itself must never be delayed
        # waiting on target_price. last_closed is a plain in-memory read, so
        # this never blocks; if the window's own Binance candle-close hasn't
        # arrived over the network yet (or never arrives), target_price just
        # comes through as None rather than holding anything up.
        if self._on_window_open is not None:
            await self._on_window_open(market)
        self._monitor.event(f"Window opened: {market.slug}")

        target_price = None
        target_price_timestamp = None
        last_closed = self._binance_feed.last_closed if self._binance_feed is not None else None
        if last_closed is not None and last_closed.kline_close_time >= market.window_start:
            # < window_start means this window's own candle-close hasn't
            # arrived yet (a race, not a crash) -- last_closed is still the
            # *previous* window's price, so using it would silently mislabel
            # a stale value as this window's target_price. Leave it None
            # instead; the event still goes out on time either way.
            target_price = last_closed.close
            target_price_timestamp = last_closed.kline_close_time

        await self._queue.put(
            WindowOpenEvent(
                timestamp=time.time(),
                slug=market.slug,
                condition_id=market.condition_id,
                window_start=market.window_start,
                window_end=market.window_end,
                up_token_id=market.up_token_id,
                down_token_id=market.down_token_id,
                target_price=target_price,
                target_price_timestamp=target_price_timestamp,
            )
        )

    async def _close(self, market: WindowMarket) -> None:
        if self._on_window_close is not None:
            await self._on_window_close(market)
        self._monitor.event(f"Window closed: {market.slug}")
        await self._queue.put(
            WindowCloseEvent(
                timestamp=time.time(),
                slug=market.slug,
                condition_id=market.condition_id,
                window_start=market.window_start,
                window_end=market.window_end,
            )
        )

    @staticmethod
    async def _sleep_until(deadline: float) -> None:
        delay = deadline - time.time()
        if delay > 0:
            await asyncio.sleep(delay)
