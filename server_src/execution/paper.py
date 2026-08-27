"""Simulated execution: instant fills at the quoted price, Polymarket's real
documented crypto-category taker fee applied on every fill, no slippage
modeled, in-memory bankroll and positions.

Unlike live trading, a paper position still held when its window closes
never settles on-chain -- nothing credits its $0/$1 payout unless something
here does it. on_window_close() polls Gamma for the window's resolved payout
and settles any residual position, so paper PnL run live against real market
data stays trustworthy even when the strategy fails to exit before expiry.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Optional

import aiohttp

from datastream.utils.events import Side
from datastream.utils.gamma_client import DEFAULT_WINDOW_SECONDS, GammaClient
from monitoring import Monitor

from .base import (
    DEFAULT_MAX_POSITION_NOTIONAL,
    DEFAULT_REJECTION_ALERT_THRESHOLD,
    DEFAULT_RETRY_COOLDOWN_SECONDS,
    ExecutionLayer,
    ExecutionResult,
    Order,
    OrderStatus,
)

logger = logging.getLogger(__name__)

DEFAULT_STARTING_BANKROLL = 1000.0

# Polymarket's documented taker fee schedule: fee = rate * size * price *
# (1 - price), i.e. proportional to share count and maximized at p=0.5
# (highest uncertainty). 0.07 is the crypto-category rate -- BTC 5-min
# markets fall under it. Settlement/redemption isn't a taker fill and isn't
# fee'd.
DEFAULT_TAKER_FEE_RATE = 0.07

# A window's real resolution isn't necessarily available the instant it
# closes -- Gamma needs to observe/settle it first. Poll rather than assume
# it's immediate, same retry-with-delay shape as WindowTracker's own market
# fetch, but capped so a Gamma hiccup can't hang a settlement task forever.
DEFAULT_SETTLEMENT_POLL_DELAY_SECONDS = 2.0
DEFAULT_SETTLEMENT_MAX_WAIT_SECONDS = 60.0


class PaperExecutionLayer(ExecutionLayer):
    def __init__(
        self,
        monitor: Optional[Monitor] = None,
        retry_cooldown_seconds: float = DEFAULT_RETRY_COOLDOWN_SECONDS,
        rejection_alert_threshold: int = DEFAULT_REJECTION_ALERT_THRESHOLD,
        max_position_notional: Optional[float] = DEFAULT_MAX_POSITION_NOTIONAL,
        starting_bankroll: float = DEFAULT_STARTING_BANKROLL,
        taker_fee_rate: float = DEFAULT_TAKER_FEE_RATE,
        clock: Callable[[], float] = time.time,
        session: Optional[aiohttp.ClientSession] = None,
        window_seconds: float = DEFAULT_WINDOW_SECONDS,
        settlement_poll_delay_seconds: float = DEFAULT_SETTLEMENT_POLL_DELAY_SECONDS,
        settlement_max_wait_seconds: float = DEFAULT_SETTLEMENT_MAX_WAIT_SECONDS,
    ) -> None:
        super().__init__(
            monitor=monitor,
            retry_cooldown_seconds=retry_cooldown_seconds,
            rejection_alert_threshold=rejection_alert_threshold,
            max_position_notional=max_position_notional,
            clock=clock,
        )
        self._starting_bankroll = starting_bankroll
        self._bankroll = starting_bankroll
        self._positions: dict = {}
        self._lock = asyncio.Lock()
        self._taker_fee_rate = taker_fee_rate
        self._fill_count = 0
        self._buy_count = 0
        self._sell_count = 0
        self._reject_count = 0
        self._settle_count = 0
        self._settled_value = 0.0
        self._total_fees = 0.0

        self._session = session
        self._gamma: Optional[GammaClient] = None
        self._window_seconds = window_seconds
        self._settlement_poll_delay_seconds = settlement_poll_delay_seconds
        self._settlement_max_wait_seconds = settlement_max_wait_seconds
        # Held so settlement tasks aren't garbage-collected mid-flight and so
        # a slow/never-resolving one is at least visible, not silently lost.
        self._settlement_tasks: set = set()

    async def _get_bankroll(self) -> float:
        async with self._lock:
            return self._bankroll

    async def _get_held(self, asset_id: str) -> float:
        async with self._lock:
            return self._positions.get(asset_id, 0.0)

    async def _place_order(self, order: Order) -> ExecutionResult:
        async with self._lock:
            held = self._positions.get(order.asset_id, 0.0)
            fee = self._taker_fee_rate * order.size * order.price * (1.0 - order.price)
            cost = order.price * order.size

            if order.action is Side.BUY:
                if cost + fee > self._bankroll:
                    self._reject_count += 1
                    return self._result(
                        order, OrderStatus.REJECTED,
                        reason=f"cost {cost + fee:.4f} exceeds bankroll {self._bankroll:.4f}",
                    )
                self._bankroll -= cost + fee
                self._positions[order.asset_id] = held + order.size
                self._buy_count += 1
            else:
                if order.size > held:
                    self._reject_count += 1
                    return self._result(
                        order, OrderStatus.REJECTED,
                        reason=f"size {order.size:.4f} exceeds held {held:.4f}",
                    )
                self._bankroll += cost - fee
                self._positions[order.asset_id] = held - order.size
                self._sell_count += 1

            self._fill_count += 1
            self._total_fees += fee
            return self._result(order, OrderStatus.FILLED, filled_price=order.price, filled_size=order.size)

    async def settle(self, asset_id: str, payout_price: float) -> float:
        """Credits a position still held when its market resolved, at its
        $0/$1 payout, and zeroes it out. No-op (returns 0.0) if nothing was
        held for asset_id."""
        async with self._lock:
            size = self._positions.pop(asset_id, 0.0)
            payout = size * payout_price
            self._bankroll += payout
            if size:
                self._settle_count += 1
                self._settled_value += payout
            return payout

    async def on_window_close(self, slug: str) -> None:
        task = asyncio.create_task(self._settle_window(slug))
        self._settlement_tasks.add(task)
        task.add_done_callback(self._settlement_tasks.discard)

    async def _settle_window(self, slug: str) -> None:
        gamma = await self._ensure_gamma()
        deadline = self._clock() + self._settlement_max_wait_seconds
        while True:
            market = None
            try:
                market = await gamma.fetch_window_market(slug)
            except Exception:
                logger.exception("Failed fetching resolution for %s", slug)
                self._monitor.error(f"Failed fetching resolution for {slug}")

            if market is not None and market.up_payout is not None and market.down_payout is not None:
                up_paid = await self.settle(market.up_token_id, market.up_payout)
                down_paid = await self.settle(market.down_token_id, market.down_payout)
                if up_paid or down_paid:
                    logger.info(
                        "Settled %s: up=%.4f down=%.4f", slug, up_paid, down_paid,
                        extra={
                            "event": "settlement",
                            "slug": slug,
                            "up_token_id": market.up_token_id,
                            "down_token_id": market.down_token_id,
                            "up_payout": market.up_payout,
                            "down_payout": market.down_payout,
                            "up_paid": up_paid,
                            "down_paid": down_paid,
                        },
                    )
                    self._monitor.execution(f"Settled {slug}: up={up_paid:.4f} down={down_paid:.4f}")
                return

            if self._clock() >= deadline:
                logger.error("Giving up waiting for %s to resolve after %.0fs", slug, self._settlement_max_wait_seconds)
                self._monitor.error(
                    f"Giving up waiting for {slug} to resolve -- any residual position is unsettled in the paper book"
                )
                return

            await asyncio.sleep(self._settlement_poll_delay_seconds)

    async def _ensure_gamma(self) -> GammaClient:
        if self._gamma is None:
            if self._session is None:
                self._session = aiohttp.ClientSession()
            self._gamma = GammaClient(self._session, window_seconds=self._window_seconds)
        return self._gamma

    def log_stats(self) -> None:
        """Logs a summary of paper-trading activity; call once the run is
        finished (e.g. on shutdown) to see the net result."""
        logger.info("Paper execution summary: %s", self._summary())

    async def status_text(self) -> str:
        async with self._lock:
            return f"Paper execution:\n{self._summary()}"

    def _summary(self) -> str:
        pnl = self._bankroll - self._starting_bankroll
        pnl_pct = (pnl / self._starting_bankroll * 100.0) if self._starting_bankroll else 0.0
        held = {asset_id: size for asset_id, size in self._positions.items() if abs(size) > 1e-9}
        held_summary = ", ".join(f"{asset_id}={size:.2f}" for asset_id, size in held.items()) or "none"
        return (
            f"starting_bankroll={self._starting_bankroll:.2f} ending_bankroll={self._bankroll:.2f} "
            f"pnl={pnl:.2f} ({pnl_pct:.2f}%) fills={self._fill_count} "
            f"(buys={self._buy_count}, sells={self._sell_count}) rejected={self._reject_count} "
            f"settled={self._settle_count} (value={self._settled_value:.2f}) "
            f"held={held_summary} fees_paid={self._total_fees:.2f}"
        )
