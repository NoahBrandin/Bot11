"""Raw (un-windowed) Chainlink price feed, via the same `polymarket-client`
SDK streams API chainlink_feed.py uses -- but the plain `prices.crypto.
chainlink` topic (CryptoPricesSpec) instead of the 60-second TWAP spec.

Why this exists as a second feed instead of just widening ChainlinkFeed:
every ChainlinkPriceEvent (the TWAP stream) is itself a 60s sliding average,
so consecutive ticks 1-2s apart share ~59/60 of their underlying window --
almost perfectly autocorrelated. probability_model.py's module docstring
already documents that an earlier sigma estimator built on exactly this kind
of already-smoothed series was abandoned as "a poor estimator" for that
reason. Feeding momentum_mu()/reversion_mu()/GBMEstimator a genuinely raw
tick series -- while still sourced from Chainlink, not Binance -- avoids
that pitfall AND the small systematic lag/bias a live recording found
between Chainlink's TWAP and a Binance-tick reconstruction of it (see
md/backtest_recording_mu_calibration_2026-08-30.md, section 8).

Not yet wired into StrategyLayer's mu/sigma inputs (see ChainlinkRawPriceEvent's
docstring): raw Chainlink's real update cadence is unmeasured -- Chainlink
oracle networks classically push on a deviation-threshold-or-heartbeat basis,
which could be far sparser than Binance's ~1-2s kline push cadence and too
thin for TwoScaleRealizedVariance's subsample grids. This feed exists to
record that cadence for a while and answer that question before any further
integration.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from polymarket import AsyncPublicClient
from polymarket.models.rtds_events import CryptoPricesChainlinkEvent
from polymarket.streams import CryptoPricesSpec

from monitoring import Monitor

from datastream.utils.events import ChainlinkRawPriceEvent

logger = logging.getLogger(__name__)

DEFAULT_SYMBOL = "btc/usd"
DEFAULT_RECONNECT_DELAY = 2.0
# Deliberately more forgiving than ChainlinkFeed's 10s DEFAULT_STALE_TIMEOUT:
# this feed's real cadence is exactly what's being measured, and a
# heartbeat-style oracle update could legitimately be tens of seconds apart
# -- flagging that as a connection "stall" before any real data exists to
# judge normal jitter against would be premature. Revisit once cadence is
# actually known.
DEFAULT_STALE_TIMEOUT = 60.0


class ChainlinkRawFeed:
    def __init__(
        self,
        queue: asyncio.Queue,
        symbol: str = DEFAULT_SYMBOL,
        monitor: Optional[Monitor] = None,
        reconnect_delay: float = DEFAULT_RECONNECT_DELAY,
        stale_timeout: float = DEFAULT_STALE_TIMEOUT,
    ) -> None:
        self._queue = queue
        self._symbol = symbol
        self._monitor = monitor or Monitor()
        self._reconnect_delay = reconnect_delay
        self._stale_timeout = stale_timeout

    async def run(self) -> None:
        while True:
            try:
                await self._run_once()
            except Exception:
                logger.exception("Raw Chainlink feed connection error, reconnecting")
                self._monitor.error("Raw Chainlink feed connection error, reconnecting")
            await asyncio.sleep(self._reconnect_delay)

    async def _run_once(self) -> None:
        async with AsyncPublicClient() as client:
            spec = CryptoPricesSpec(topic="prices.crypto.chainlink", symbols=[self._symbol])
            async with await client.subscribe(spec) as stream:
                stream_iter = stream.__aiter__()
                while True:
                    try:
                        event = await asyncio.wait_for(stream_iter.__anext__(), timeout=self._stale_timeout)
                    except asyncio.TimeoutError:
                        logger.warning(
                            "Raw Chainlink feed stale (no tick in %.0fs), reconnecting", self._stale_timeout
                        )
                        self._monitor.error(
                            f"Raw Chainlink feed stale (no tick in {self._stale_timeout:.0f}s), reconnecting"
                        )
                        return
                    except StopAsyncIteration:
                        return
                    self._handle_event(event)

    def _handle_event(self, event: CryptoPricesChainlinkEvent) -> None:
        payload = event.payload
        raw_event = ChainlinkRawPriceEvent(
            timestamp=time.time(),
            symbol=payload.symbol,
            price=float(payload.value),
            source_timestamp=payload.timestamp / 1000,
        )
        self._queue.put_nowait(raw_event)
