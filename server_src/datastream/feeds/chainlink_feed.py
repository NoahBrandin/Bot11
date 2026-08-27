"""Chainlink TWAP price feed for Polymarket's crypto up/down markets, via the
official `polymarket-client` SDK's streams API.

This is the literal price Polymarket's 5-min BTC market resolves against (a
30-second time-weighted average of Chainlink's price -- 15-min/4h markets use
a 60s window instead; see
https://docs.polymarket.com/market-data/chainlink-twap), unlike
binance_feed.py's klines, which are only ever a proxy the strategy trades
against, not the settlement price.

An earlier version of this hand-rolled the RTDS websocket protocol directly
(see git history) before the SDK's `CryptoPricesChainlinkTwapSpec` was found
-- that raw approach also only exposed the plain Chainlink tick, not the TWAP
Polymarket actually settles against, and required manually polling/resending
subscribe messages since the raw RTDS topic doesn't push incremental ticks to
an open subscription. The SDK's SubscriptionHandle restores itself across
disconnects internally, so the outer reconnect loop here (modeled after
binance_feed.py's) is only a backstop for the whole client connection dying
outright.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from polymarket import AsyncPublicClient
from polymarket.models.rtds_events import CryptoPricesChainlinkTwapEvent
from polymarket.streams import CryptoPricesChainlinkTwapSpec

from monitoring import Monitor

from datastream.utils.events import ChainlinkPriceEvent

logger = logging.getLogger(__name__)

DEFAULT_SYMBOL = "btc/usd"
DEFAULT_WINDOW_SECONDS = 30
DEFAULT_RECONNECT_DELAY = 2.0


class ChainlinkFeed:
    def __init__(
        self,
        queue: asyncio.Queue,
        symbol: str = DEFAULT_SYMBOL,
        window_seconds: int = DEFAULT_WINDOW_SECONDS,
        monitor: Optional[Monitor] = None,
        reconnect_delay: float = DEFAULT_RECONNECT_DELAY,
    ) -> None:
        self._queue = queue
        self._symbol = symbol
        self._window_seconds = window_seconds
        self._monitor = monitor or Monitor()
        self._reconnect_delay = reconnect_delay

    async def run(self) -> None:
        while True:
            try:
                await self._run_once()
            except Exception:
                logger.exception("Chainlink feed connection error, reconnecting")
                self._monitor.error("Chainlink feed connection error, reconnecting")
            await asyncio.sleep(self._reconnect_delay)

    async def _run_once(self) -> None:
        async with AsyncPublicClient() as client:
            spec = CryptoPricesChainlinkTwapSpec(window_seconds=self._window_seconds, symbols=[self._symbol])
            async with await client.subscribe(spec) as stream:
                async for event in stream:
                    self._handle_event(event)

    def _handle_event(self, event: CryptoPricesChainlinkTwapEvent) -> None:
        payload = event.payload
        chainlink_event = ChainlinkPriceEvent(
            timestamp=time.time(),
            symbol=payload.symbol,
            price=float(payload.value),
            window_seconds=payload.window_seconds,
            source_timestamp=payload.timestamp / 1000,
        )
        self._queue.put_nowait(chainlink_event)
