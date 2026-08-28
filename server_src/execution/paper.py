"""Simulated execution: instant fills at the quoted price, Polymarket's real
documented crypto-category taker fee applied on every fill, no slippage
modeled, in-memory bankroll and positions.

Unlike live trading, a paper position still held when its window closes
never settles on-chain -- nothing credits its $0/$1 payout here. This layer
does not poll Gamma for resolution itself (on_window_close is base's no-op);
other_src/paper_run_review.py re-derives every position's true settlement
straight from Gamma after the fact instead, so there's no need to block a
live run waiting on-chain resolution to show up.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Optional

from datastream.utils.events import Side
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
