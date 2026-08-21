"""Backtest-only PaperExecutionLayer variant that simulates real order-
placement latency: instead of filling instantly at the price observed at
decision time (signal.price, frozen when the entry/exit condition first
fired), the fill price is re-looked-up from the same historical tick series
at decision_time + latency, subject to the same price-protection tolerance
LiveExecutionLayer actually enforces -- if the market has moved beyond that
band by the time the (simulated) order lands, it's REJECTED rather than
filled at an arbitrarily worse price, matching a real capped FAK order
(see execution/live.py's DEFAULT_PRICE_PROTECTION_TOLERANCE).

This exists to answer one question: how much of the backtest's apparent
edge survives a realistic decision-to-order-landing delay, given the timing
analysis showing ~92% of entries and ~99% of exits fire within a couple of
seconds of a fresh 1-minute BTC kline landing -- exactly when other
participants are also reacting, and when a slow order is most likely to
land against a price that's already moved.

At decision_to_fill_latency_seconds=0.0 this is equivalent to the plain
PaperExecutionLayer: the fill-time lookup resolves to the same tick used to
build signal.price in the first place, so it's a strict superset of the
un-delayed behavior, not a separate model.
"""
from __future__ import annotations

import bisect
from typing import Optional

from datastream.events import Side
from execution.base import Order, OrderStatus
from execution.paper import PaperExecutionLayer

MIN_PRICE = 0.01
MAX_PRICE = 0.99


class LatencyModelingPaperExecutionLayer(PaperExecutionLayer):
    def __init__(
        self,
        *args,
        decision_to_fill_latency_seconds: float = 0.0,
        price_protection_tolerance: float = 0.02,
        spread_half_width: float = 0.02,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._latency = decision_to_fill_latency_seconds
        self._protection = price_protection_tolerance
        self._spread_half_width = spread_half_width
        # asset_id -> that window's raw {"t", "p"} traded-price ticks, the
        # same series _make_price_change_event() draws the synthetic
        # bid/ask from -- registered per window by the replay loop before
        # any order against that window's tokens can be placed.
        self._price_series: dict[str, list[dict]] = {}

    def register_window(self, window: dict) -> None:
        self._price_series[window["up_token_id"]] = window["up_prices"]
        self._price_series[window["down_token_id"]] = window["down_prices"]

    def _price_at(self, asset_id: str, t: float) -> Optional[float]:
        points = self._price_series.get(asset_id)
        if not points:
            return None
        times = [p["t"] for p in points]
        idx = bisect.bisect_right(times, t) - 1
        if idx < 0:
            idx = 0
        return float(points[idx]["p"])

    async def _place_order(self, order: Order):
        decision_time = self._clock()
        fill_time = decision_time + self._latency
        true_price = self._price_at(order.asset_id, fill_time)

        if true_price is None:
            # No tick data registered for this asset (shouldn't normally
            # happen) -- fall back to filling at the decision price rather
            # than failing the whole backtest run.
            return await super()._place_order(order)

        if order.action is Side.BUY:
            fill_price = min(MAX_PRICE, round(true_price + self._spread_half_width, 2))
            if fill_price > order.price + self._protection:
                self._reject_count += 1
                return self._result(
                    order, OrderStatus.REJECTED,
                    reason=(
                        f"ask moved to {fill_price} beyond protection tolerance from "
                        f"decision price {order.price} over {self._latency:.3f}s latency"
                    ),
                )
        else:
            fill_price = max(MIN_PRICE, round(true_price - self._spread_half_width, 2))
            if fill_price < order.price - self._protection:
                self._reject_count += 1
                return self._result(
                    order, OrderStatus.REJECTED,
                    reason=(
                        f"bid moved to {fill_price} beyond protection tolerance from "
                        f"decision price {order.price} over {self._latency:.3f}s latency"
                    ),
                )

        repriced = Order(asset_id=order.asset_id, action=order.action, price=fill_price, size=order.size)
        return await super()._place_order(repriced)
