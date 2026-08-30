"""Backtest-only PaperExecutionLayer variant that simulates real order-
placement latency: instead of filling instantly at the price observed at
decision time (signal.price, frozen when the entry/exit condition first
fired), the fill price is re-looked-up from the real recorded order book at
decision_time + latency, subject to the same price-protection tolerance
LiveExecutionLayer actually enforces -- if the market has moved beyond that
band by the time the (simulated) order lands, it's REJECTED rather than
filled at an arbitrarily worse price, matching a real capped FAK order
(see execution/live.py's DEFAULT_PRICE_PROTECTION_TOLERANCE).

Unlike the old download.py-based engine (which only had CLOB
prices-history's last-traded price to replay from, and so faked a bid/ask by
padding a guessed half-spread around it), every recorded
PolymarketPriceChangeEvent already carries the real best_bid/best_ask
observed at that tick -- so fills here look up the real book instead of an
approximation. FALLBACK_SPREAD_HALF_WIDTH only kicks in for the rare tick
that recorded a trade without book data (best_bid/best_ask is None).

At decision_to_fill_latency_seconds=0.0 this is equivalent to the plain
PaperExecutionLayer: the fill-time lookup resolves to the same tick used to
build signal.price in the first place, so it's a strict superset of the
un-delayed behavior, not a separate model.
"""
from __future__ import annotations

import bisect
from typing import Optional

from datastream.utils.events import Side
from execution.base import Order, OrderStatus
from execution.paper import PaperExecutionLayer

from .recording import Recording

MIN_PRICE = 0.01
MAX_PRICE = 0.99

# Only used when a recorded tick's best_bid/best_ask is None (trade reported
# without book data) -- half-width padded around that tick's traded price.
FALLBACK_SPREAD_HALF_WIDTH = 0.005


class LatencyModelingPaperExecutionLayer(PaperExecutionLayer):
    def __init__(
        self,
        *args,
        decision_to_fill_latency_seconds: float = 0.0,
        price_protection_tolerance: float = 0.02,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._latency = decision_to_fill_latency_seconds
        self._protection = price_protection_tolerance
        # asset_id -> sorted (timestamp, best_bid, best_ask, price) ticks.
        self._book: dict[str, list[tuple[float, Optional[float], Optional[float], float]]] = {}
        # asset_id -> that same list's timestamps, cached separately so
        # _book_at()'s bisect has a plain list to search without rebuilding
        # it every call.
        self._times: dict[str, list[float]] = {}

    def index_recording(self, recording: Recording) -> None:
        """Builds the per-asset book index once, from every
        PolymarketPriceChangeEvent in the recording -- call before replay
        starts so any order (including ones filling with latency into a
        later tick) can look up the real book at fill time."""
        from datastream.utils.events import PolymarketPriceChangeEvent

        raw: dict[str, list[tuple[float, Optional[float], Optional[float], float]]] = {}
        for event in recording.events:
            if isinstance(event, PolymarketPriceChangeEvent):
                raw.setdefault(event.asset_id, []).append(
                    (event.timestamp, event.best_bid, event.best_ask, event.price)
                )
        # Sorted timestamps cached alongside each asset's ticks so
        # _book_at()'s bisect doesn't rebuild this list on every single
        # order placement (this is a hot path: one call per simulated
        # entry/exit/track order across the whole backtest).
        self._book = raw
        self._times = {asset_id: [tick[0] for tick in ticks] for asset_id, ticks in raw.items()}
        for ticks, times in zip(raw.values(), self._times.values()):
            paired = sorted(zip(times, ticks))
            times[:] = [t for t, _ in paired]
            ticks[:] = [tick for _, tick in paired]

    def _book_at(self, asset_id: str, t: float) -> Optional[tuple[Optional[float], Optional[float], float]]:
        ticks = self._book.get(asset_id)
        if not ticks:
            return None
        times = self._times[asset_id]
        idx = bisect.bisect_right(times, t) - 1
        if idx < 0:
            idx = 0
        _, bid, ask, price = ticks[idx]
        return bid, ask, price

    async def _place_order(self, order: Order):
        decision_time = self._clock()
        fill_time = decision_time + self._latency
        looked_up = self._book_at(order.asset_id, fill_time)

        if looked_up is None:
            # No book data registered for this asset (shouldn't normally
            # happen) -- fall back to filling at the decision price rather
            # than failing the whole backtest run.
            return await super()._place_order(order)

        bid, ask, price = looked_up
        if order.action is Side.BUY:
            fill_price = ask if ask is not None else min(MAX_PRICE, round(price + FALLBACK_SPREAD_HALF_WIDTH, 2))
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
            fill_price = bid if bid is not None else max(MIN_PRICE, round(price - FALLBACK_SPREAD_HALF_WIDTH, 2))
            if fill_price < order.price - self._protection:
                self._reject_count += 1
                return self._result(
                    order, OrderStatus.REJECTED,
                    reason=(
                        f"bid moved to {fill_price} beyond protection tolerance from "
                        f"decision price {order.price} over {self._latency:.3f}s latency"
                    ),
                )

        repriced = Order(
            asset_id=order.asset_id, action=order.action, price=fill_price, size=order.size,
            probability=order.probability, sigma=order.sigma, minutes_remaining=order.minutes_remaining,
        )
        return await super()._place_order(repriced)
