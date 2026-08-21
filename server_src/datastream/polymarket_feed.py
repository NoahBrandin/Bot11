"""WebSocket client for Polymarket's CLOB market channel.

Subscriptions are driven externally (by the WindowTracker, via subscribe()/
unsubscribe()) rather than fixed at construction time, since the two token
IDs we care about change every 5 minutes.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

import websockets

from monitoring import Monitor

from .events import (
    Outcome,
    PolymarketBookEvent,
    PolymarketLastTradePriceEvent,
    PolymarketPriceChangeEvent,
    PriceLevel,
    Side,
)

logger = logging.getLogger(__name__)

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
DEFAULT_APP_PING_INTERVAL = 10.0
DEFAULT_RECONNECT_DELAY = 2.0
# Owning-module default for orchestrator.py's POLYMARKET_POLL_INTERVAL env
# var and datastream/manager.py's DatastreamLayer default -- both import this
# instead of retyping 0.0, so there's exactly one literal for "no throttling"
# in the codebase.
DEFAULT_POLYMARKET_POLL_INTERVAL = 0.0


class PolymarketFeed:
    def __init__(
        self,
        queue: asyncio.Queue,
        monitor: Optional[Monitor] = None,
        poll_interval: float = DEFAULT_POLYMARKET_POLL_INTERVAL,
        reconnect_delay: float = DEFAULT_RECONNECT_DELAY,
        app_ping_interval: float = DEFAULT_APP_PING_INTERVAL,
    ) -> None:
        self._queue = queue
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_lock = asyncio.Lock()
        self._monitor = monitor or Monitor()
        # asset_id -> (slug, outcome), used to enrich/dispatch raw WS messages
        self._asset_outcome: dict[str, tuple[str, Outcome]] = {}
        # Minimum spacing between queued book/price_change events for a given
        # (asset_id, event_type) pair. The WS can push several updates a
        # second per token, each one driving a full strategy re-evaluation
        # (bankroll/position fetches against the real exchange) -- <=0
        # disables throttling and forwards every message, same as before.
        self._poll_interval = poll_interval
        self._last_emit: dict[tuple[str, str], float] = {}
        self._reconnect_delay = reconnect_delay
        self._app_ping_interval = app_ping_interval

    def _should_emit(self, asset_id: str, event_type: str, now: float) -> bool:
        if self._poll_interval <= 0:
            return True
        key = (asset_id, event_type)
        last = self._last_emit.get(key)
        if last is not None and now - last < self._poll_interval:
            return False
        self._last_emit[key] = now
        return True

    async def subscribe(self, up_token_id: str, down_token_id: str, slug: str) -> None:
        self._asset_outcome[up_token_id] = (slug, Outcome.UP)
        self._asset_outcome[down_token_id] = (slug, Outcome.DOWN)
        await self._send({"assets_ids": [up_token_id, down_token_id], "type": "market", "operation": "subscribe"})

    async def unsubscribe(self, up_token_id: str, down_token_id: str) -> None:
        await self._send({"assets_ids": [up_token_id, down_token_id], "type": "market", "operation": "unsubscribe"})
        self._asset_outcome.pop(up_token_id, None)
        self._asset_outcome.pop(down_token_id, None)

    async def _send(self, payload: dict) -> None:
        async with self._ws_lock:
            if self._ws is None:
                logger.warning("Polymarket WS not connected yet, dropping: %s", payload)
                return
            await self._ws.send(json.dumps(payload))

    async def run(self) -> None:
        while True:
            try:
                await self._run_once()
            except Exception:
                logger.exception("Polymarket feed connection error, reconnecting")
                self._monitor.error("Polymarket feed connection error, reconnecting")
            self._ws = None
            await asyncio.sleep(self._reconnect_delay)

    async def _run_once(self) -> None:
        async with websockets.connect(WS_URL, ping_interval=5, ping_timeout=10) as ws:
            async with self._ws_lock:
                self._ws = ws
                # Re-subscribe to whatever's currently tracked (covers reconnects).
                asset_ids = list(self._asset_outcome.keys())
                if asset_ids:
                    await ws.send(json.dumps({"assets_ids": asset_ids, "type": "market"}))

            ping_task = asyncio.create_task(self._ping_loop(ws))
            try:
                async for raw in ws:
                    self._handle_message(raw)
            finally:
                ping_task.cancel()

    async def _ping_loop(self, ws: websockets.WebSocketClientProtocol) -> None:
        while True:
            await asyncio.sleep(self._app_ping_interval)
            try:
                await ws.send("PING")
            except Exception:
                return

    def _handle_message(self, raw: str) -> None:
        if raw in ("PONG", "PING"):
            return
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            logger.debug("Non-JSON message from Polymarket WS: %s", raw)
            return

        for msg in payload if isinstance(payload, list) else [payload]:
            self._dispatch(msg)

    def _dispatch(self, msg: dict) -> None:
        event_type = msg.get("event_type")
        now = time.time()

        if event_type == "price_change":
            for change in msg.get("price_changes", []):
                self._handle_price_change(change, now)
            return

        asset_id = msg.get("asset_id")
        info = self._asset_outcome.get(asset_id)
        if info is None:
            return
        slug, outcome = info

        if event_type == "book":
            self._handle_book(msg, slug, outcome, now)
        elif event_type == "last_trade_price":
            self._handle_last_trade_price(msg, slug, outcome, now)

    def _handle_book(self, msg: dict, slug: str, outcome: Outcome, now: float) -> None:
        if not self._should_emit(msg["asset_id"], "book", now):
            return
        bids = tuple(PriceLevel(float(b["price"]), float(b["size"])) for b in msg.get("bids", []))
        asks = tuple(PriceLevel(float(a["price"]), float(a["size"])) for a in msg.get("asks", []))
        self._queue.put_nowait(
            PolymarketBookEvent(
                timestamp=now, slug=slug, asset_id=msg["asset_id"], outcome=outcome, bids=bids, asks=asks
            )
        )

    def _handle_price_change(self, change: dict, now: float) -> None:
        asset_id = change.get("asset_id")
        info = self._asset_outcome.get(asset_id)
        if info is None:
            return
        if not self._should_emit(asset_id, "price_change", now):
            return
        slug, outcome = info
        self._queue.put_nowait(
            PolymarketPriceChangeEvent(
                timestamp=now,
                slug=slug,
                asset_id=asset_id,
                outcome=outcome,
                price=float(change["price"]),
                size=float(change.get("size", 0.0)),
                side=Side(change["side"]) if change.get("side") else Side.BUY,
                best_bid=float(change["best_bid"]) if change.get("best_bid") is not None else None,
                best_ask=float(change["best_ask"]) if change.get("best_ask") is not None else None,
            )
        )

    def _handle_last_trade_price(self, msg: dict, slug: str, outcome: Outcome, now: float) -> None:
        self._queue.put_nowait(
            PolymarketLastTradePriceEvent(
                timestamp=now,
                slug=slug,
                asset_id=msg["asset_id"],
                outcome=outcome,
                price=float(msg["price"]),
                size=float(msg.get("size", 0.0)),
                side=Side(msg["side"]) if msg.get("side") else Side.BUY,
            )
        )
