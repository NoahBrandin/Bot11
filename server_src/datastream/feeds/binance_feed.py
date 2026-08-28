"""WebSocket client for Binance's BTCUSDT kline (candlestick) stream."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

import websockets

from monitoring import Monitor

from datastream.utils.events import BinanceKlineEvent

logger = logging.getLogger(__name__)

BASE_WS_URL = "wss://stream.binance.com:9443/ws"
DEFAULT_RECONNECT_DELAY = 2.0


class BinanceFeed:
    def __init__(
        self,
        queue: asyncio.Queue,
        symbol: str = "btcusdt",
        interval: str = "1m",
        monitor: Optional[Monitor] = None,
        reconnect_delay: float = DEFAULT_RECONNECT_DELAY,
    ) -> None:
        self._queue = queue
        # .lower(): Binance's WS stream path requires lowercase, but the
        # symbol may arrive uppercase (e.g. shared with strategy/manager.py's
        # REST bootstrap call, which is case-insensitive) -- normalizing here
        # keeps a single BINANCE_SYMBOL env value valid for both call sites.
        self._url = f"{BASE_WS_URL}/{symbol.lower()}@kline_{interval}"
        self._monitor = monitor or Monitor()
        self._reconnect_delay = reconnect_delay

    async def run(self) -> None:
        while True:
            try:
                await self._run_once()
            except Exception:
                logger.exception("Binance feed connection error, reconnecting")
                self._monitor.error("Binance feed connection error, reconnecting")
            await asyncio.sleep(self._reconnect_delay)

    async def _run_once(self) -> None:
        async with websockets.connect(self._url, ping_interval=20, ping_timeout=20) as ws:
            async for raw in ws:
                self._handle_message(raw)

    def _handle_message(self, raw: str) -> None:
        payload = json.loads(raw)
        k = payload.get("k")
        if k is None:
            return

        # Emits on every kline update, not just candle close -- is_closed
        # tells consumers which is which, so anything wanting the old
        # once-per-candle cadence (e.g. matching engine.py's backtest
        # replay, which only ever produces is_closed=True) can filter on it.
        event = BinanceKlineEvent(
            timestamp=time.time(),
            symbol=k["s"],
            interval=k["i"],
            kline_open_time=k["t"] / 1000,
            kline_close_time=k["T"] / 1000,
            open=float(k["o"]),
            high=float(k["h"]),
            low=float(k["l"]),
            close=float(k["c"]),
            volume=float(k["v"]),
            is_closed=bool(k["x"]),
        )
        self._queue.put_nowait(event)
