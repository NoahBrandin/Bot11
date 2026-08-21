"""Wires the Datastream-Layer's feeds together behind a single asyncio.Queue.

Downstream layers just do:

    layer = DatastreamLayer()
    async with layer:
        while True:
            event = await layer.queue.get()
            ...
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import aiohttp

from monitoring import Monitor

from .binance_feed import BinanceFeed
from .binance_feed import DEFAULT_RECONNECT_DELAY as DEFAULT_BINANCE_RECONNECT_DELAY
from .gamma_client import DEFAULT_WINDOW_SECONDS, GammaClient, WindowMarket
from .polymarket_feed import (
    DEFAULT_APP_PING_INTERVAL,
    DEFAULT_POLYMARKET_POLL_INTERVAL,
)
from .polymarket_feed import DEFAULT_RECONNECT_DELAY as DEFAULT_POLYMARKET_RECONNECT_DELAY
from .polymarket_feed import PolymarketFeed
from .window_tracker import DEFAULT_FETCH_RETRY_DELAY, DEFAULT_PREFETCH_LEAD_SECONDS, WindowTracker

logger = logging.getLogger(__name__)


class DatastreamLayer:
    def __init__(
        self,
        binance_symbol: str = "btcusdt",
        binance_interval: str = "1m",
        monitor: Optional[Monitor] = None,
        polymarket_poll_interval: float = DEFAULT_POLYMARKET_POLL_INTERVAL,
        binance_reconnect_delay: float = DEFAULT_BINANCE_RECONNECT_DELAY,
        polymarket_reconnect_delay: float = DEFAULT_POLYMARKET_RECONNECT_DELAY,
        polymarket_app_ping_interval: float = DEFAULT_APP_PING_INTERVAL,
        window_seconds: float = DEFAULT_WINDOW_SECONDS,
        prefetch_lead_seconds: float = DEFAULT_PREFETCH_LEAD_SECONDS,
        window_fetch_retry_delay: float = DEFAULT_FETCH_RETRY_DELAY,
    ) -> None:
        self.queue: asyncio.Queue = asyncio.Queue()
        self._monitor = monitor or Monitor()
        self._polymarket_feed = PolymarketFeed(
            self.queue,
            monitor=self._monitor,
            poll_interval=polymarket_poll_interval,
            reconnect_delay=polymarket_reconnect_delay,
            app_ping_interval=polymarket_app_ping_interval,
        )
        self._binance_feed = BinanceFeed(
            self.queue,
            symbol=binance_symbol,
            interval=binance_interval,
            monitor=self._monitor,
            reconnect_delay=binance_reconnect_delay,
        )
        self._window_seconds = window_seconds
        self._prefetch_lead_seconds = prefetch_lead_seconds
        self._window_fetch_retry_delay = window_fetch_retry_delay

    async def _on_window_open(self, market: WindowMarket) -> None:
        await self._polymarket_feed.subscribe(market.up_token_id, market.down_token_id, market.slug)

    async def _on_window_close(self, market: WindowMarket) -> None:
        await self._polymarket_feed.unsubscribe(market.up_token_id, market.down_token_id)

    async def run(self) -> None:
        async with aiohttp.ClientSession() as session:
            gamma = GammaClient(session, window_seconds=self._window_seconds)
            window_tracker = WindowTracker(
                self.queue,
                gamma,
                on_window_open=self._on_window_open,
                on_window_close=self._on_window_close,
                monitor=self._monitor,
                window_seconds=self._window_seconds,
                prefetch_lead_seconds=self._prefetch_lead_seconds,
                fetch_retry_delay=self._window_fetch_retry_delay,
            )

            await asyncio.gather(
                self._binance_feed.run(),
                self._polymarket_feed.run(),
                window_tracker.run(),
            )
