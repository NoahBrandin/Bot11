"""Chainlink TWAP price feed for Polymarket's crypto up/down markets, via the
official `polymarket-client` SDK's streams API.

This is the literal price Polymarket's 5-min BTC market resolves against. Its
own resolution rules name the 60-second Chainlink TWAP stream explicitly
(https://data.chain.link/streams/btc-usd-twap-60s-streams) -- confirmed by
reading an actual market's resolution text, not Polymarket's docs page
(https://docs.polymarket.com/market-data/chainlink-twap), which claims a 30s
window for 5-min markets and turned out to be wrong/stale. Unlike
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

That internal auto-restore is also why it's not a complete backstop:
recorder logs have shown multi-minute stretches with zero ChainlinkPriceEvents
and no logged reconnect at all -- the SDK evidently retries the underlying
connection silently in some failure modes without the `async for` ever
raising, so `run()`'s `except Exception` never fires. `_run_once` below adds
its own staleness watchdog (independent of the SDK) so a quiet stream still
gets torn down and resubscribed, and -- unlike the SDK's silent retries --
logged.
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
DEFAULT_WINDOW_SECONDS = 60
DEFAULT_RECONNECT_DELAY = 2.0
# Observed live tick cadence is roughly one every 1-2s -- 10s of silence is
# already well outside normal jitter, and far short of the multi-minute
# silent stalls this is meant to catch (see module docstring).
DEFAULT_STALE_TIMEOUT = 10.0


class ChainlinkFeed:
    def __init__(
        self,
        queue: asyncio.Queue,
        symbol: str = DEFAULT_SYMBOL,
        window_seconds: int = DEFAULT_WINDOW_SECONDS,
        monitor: Optional[Monitor] = None,
        reconnect_delay: float = DEFAULT_RECONNECT_DELAY,
        stale_timeout: float = DEFAULT_STALE_TIMEOUT,
    ) -> None:
        self._queue = queue
        self._symbol = symbol
        self._window_seconds = window_seconds
        self._monitor = monitor or Monitor()
        self._reconnect_delay = reconnect_delay
        self._stale_timeout = stale_timeout

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
                stream_iter = stream.__aiter__()
                while True:
                    try:
                        event = await asyncio.wait_for(stream_iter.__anext__(), timeout=self._stale_timeout)
                    except asyncio.TimeoutError:
                        # Not an exception the SDK raised -- our own watchdog,
                        # since its internal auto-restore can otherwise leave
                        # the subscription silently dead (see module
                        # docstring). Returning drops this stream/client and
                        # lets run() resubscribe from scratch after
                        # reconnect_delay, same as a real connection error.
                        logger.warning(
                            "Chainlink feed stale (no tick in %.0fs), reconnecting", self._stale_timeout
                        )
                        self._monitor.error(
                            f"Chainlink feed stale (no tick in {self._stale_timeout:.0f}s), reconnecting"
                        )
                        return
                    except StopAsyncIteration:
                        return
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
