"""Shared Execution-Layer contract: paper and live implementations expose
identical async hooks so the Strategy-Layer never needs to know which one
it's talking to, and never needs to track position/bankroll itself.

Execution is the sole owner of bankroll and held-position truth. Strategy
emits a Signal (desired outcome + target fraction of bankroll); converge()
-- implemented once, here, as a template method -- diffs that target against
the real held position (via the subclass-specific _get_bankroll/_get_held
hooks) and submits at most one FAK order to close the gap, via the
subclass-specific _place_order hook. Because the target is always recomputed
from real state, converge() is idempotent: calling it again with an
unchanged Signal after it already converged is a cheap no-op, not a
duplicate order -- this is what lets Strategy safely re-emit its current
target on every qualifying tick instead of trying to track "did this already
get sent" itself.
"""
from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Callable, Optional

from datastream.utils.events import Side
from monitoring import Monitor

if TYPE_CHECKING:
    from strategy.signal import Signal

logger = logging.getLogger(__name__)

MIN_ORDER_SIZE = 5.0  # Polymarket's real minimum share size

# FAK fills report filled_size rounded to ~2 decimals (e.g. a requested
# 6.526189 legitimately settles as 6.52) -- that's a full fill, not a
# partial one. An absolute epsilon doesn't survive that rounding, which was
# misclassifying full fills as partial and forcing an immediate re-check:
# converge() would then re-fetch held from Polymarket's balance-allowance
# API before its post-fill balance update had propagated, compute a bogus
# non-zero delta against the just-closed position, and fire a duplicate
# order the exchange correctly rejected as "not enough balance". A relative
# tolerance survives normal FAK rounding (observed gaps are <0.1% of
# order.size) while still catching genuine partial fills, which are
# typically an order of magnitude larger.
FULL_FILL_RELATIVE_TOLERANCE = 0.01

DEFAULT_RETRY_COOLDOWN_SECONDS = 1.0
DEFAULT_REJECTION_ALERT_THRESHOLD = 5
DEFAULT_MAX_POSITION_NOTIONAL = 1.0


class OrderStatus(str, Enum):
    FILLED = "FILLED"
    # No state change: the exchange (or the paper book) declined/killed it
    # (e.g. an unmatched FAK order).
    REJECTED = "REJECTED"
    # Attempted but errored (network/exchange-side exception) -- outcome
    # genuinely uncertain, treat with more caution than REJECTED.
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class Order:
    """Execution-internal: the concrete BUY/SELL converge() decided is
    needed to close the gap between a Signal's target and the real held
    position. Never constructed by Strategy."""

    asset_id: str
    action: Side
    price: float
    size: float
    # Decision-time context carried through purely so LiveExecutionLayer can
    # size its FAK price-protection band dynamically (see execution/live.py's
    # _price_protection_tolerance) -- meaningless to PaperExecutionLayer,
    # which ignores them and fills at exactly `price`.
    probability: float
    sigma: float
    minutes_remaining: float


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    order: Order
    status: OrderStatus
    filled_price: Optional[float]
    filled_size: Optional[float]
    reason: Optional[str]
    timestamp: float


class ExecutionLayer(ABC):
    def __init__(
        self,
        monitor: Optional[Monitor] = None,
        retry_cooldown_seconds: float = DEFAULT_RETRY_COOLDOWN_SECONDS,
        rejection_alert_threshold: int = DEFAULT_REJECTION_ALERT_THRESHOLD,
        max_position_notional: Optional[float] = DEFAULT_MAX_POSITION_NOTIONAL,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._monitor = monitor or Monitor()
        self._retry_cooldown_seconds = retry_cooldown_seconds
        self._rejection_alert_threshold = rejection_alert_threshold
        self._max_position_notional = max_position_notional
        self._clock = clock
        self.results: asyncio.Queue = asyncio.Queue()
        self._last_attempt: dict[tuple, float] = {}
        self._rejection_streak: dict[tuple, int] = {}
        # (slug, outcome) -> target_pct last successfully converged to (only
        # ever set on a FILLED result, see converge()). Left unset/stale on
        # REJECTED/FAILED or a too-small no-op so the next identical Signal
        # still retries against fresh bankroll instead of being suppressed.
        self._converged_target_pct: dict[tuple, float] = {}

    # -- subclass hooks -----------------------------------------------------

    @abstractmethod
    async def _get_bankroll(self) -> float: ...

    @abstractmethod
    async def _get_held(self, asset_id: str) -> float: ...

    @abstractmethod
    async def _place_order(self, order: Order) -> ExecutionResult: ...

    @abstractmethod
    async def status_text(self) -> str:
        """Human-readable snapshot for on-demand reporting (e.g. Telegram
        /status), including real held positions -- the single source of
        truth for what's actually held now lives here, not in Strategy."""
        ...

    async def on_window_close(self, slug: str) -> None:
        """Hook for anything Execution needs to do once a window's tokens
        stop trading (e.g. paper-mode settlement). No-op by default -- live
        trading needs nothing here, real positions settle on-chain."""
        return None

    # -- shared convergence logic --------------------------------------------

    async def converge(self, signal: "Signal") -> Optional[ExecutionResult]:
        if not (0.0 < signal.price < 1.0):
            logger.warning("Signal price %s outside (0, 1), skipping: %r", signal.price, signal)
            return None

        key = (signal.slug, signal.outcome)
        now = self._clock()
        last_attempt = self._last_attempt.get(key)
        if last_attempt is not None and now - last_attempt < self._retry_cooldown_seconds:
            return None

        if self._converged_target_pct.get(key) == signal.target_pct:
            # Already converged to this exact target on a prior fill.
            # Bankroll has since moved -- because we spent/received money
            # *getting* here -- but re-deriving target_shares from the new
            # bankroll would treat that self-inflicted movement as a reason
            # to resize, immediately selling off part of a position just
            # bought (or buying more right after an exit) purely because the
            # denominator changed. Nothing to do until the target itself
            # actually changes.
            return None

        try:
            bankroll = await self._get_bankroll()
            held = await self._get_held(signal.asset_id)
        except Exception as exc:
            # Same rationale as a transient market-data hiccup: skip this
            # tick rather than crash the caller, there's another tick soon.
            logger.warning(
                "Bankroll/position fetch failed converging %s (%s), skipping this tick: %s",
                signal.slug, signal.outcome.value, exc,
            )
            return None

        target_notional = signal.target_pct * bankroll
        if self._max_position_notional is not None:
            target_notional = min(target_notional, self._max_position_notional)
        target_shares = target_notional / signal.price

        delta = target_shares - held
        if abs(delta) < MIN_ORDER_SIZE:
            # Either the desired entry is too small to clear the exchange's
            # minimum order size yet, or what's left after an exit is dust
            # that couldn't be sold as its own order anyway -- either way,
            # nothing to do this tick.
            return None

        action = Side.BUY if delta > 0 else Side.SELL
        order = Order(
            asset_id=signal.asset_id, action=action, price=signal.price, size=abs(delta),
            probability=signal.probability, sigma=signal.sigma, minutes_remaining=signal.minutes_remaining,
        )
        self._last_attempt[key] = now

        self._monitor.order(
            f"{order.action.value} {signal.outcome.value} {order.size:.2f} @ {order.price:.2f} "
            f"(slug={signal.slug}, p={signal.probability:.3f})"
        )

        # Latency instrumentation only -- self._clock() is real time.time()
        # outside the backtest's SimulatedClock, so these three deltas are a
        # genuine measurement of the live round trip: how long between
        # Strategy deciding (signal.timestamp) and Execution starting to
        # place the order (queueing/cooldown/event-loop delay), and how long
        # the order placement call itself took (the real analogue of the
        # backtest's decision_to_fill_latency_seconds sweep).
        t_submit = self._clock()
        try:
            result = await self._place_order(order)
        except Exception as exc:
            logger.exception("Order placement failed: %s", order)
            self._monitor.error(f"Order placement failed: {exc}")
            result = self._result(order, OrderStatus.FAILED, reason=str(exc))
        t_result = self._clock()
        logger.info(
            "Order latency: queue=%.3fs place=%.3fs total=%.3fs (slug=%s outcome=%s action=%s)",
            t_submit - signal.timestamp, t_result - t_submit, t_result - signal.timestamp,
            signal.slug, signal.outcome.value, order.action.value,
        )

        self.results.put_nowait(result)
        logger.info(
            "Execution result: %s",
            result,
            extra={
                "event": "order_result",
                "slug": signal.slug,
                "condition_id": signal.condition_id,
                "asset_id": signal.asset_id,
                "outcome": signal.outcome.value,
                "probability": signal.probability,
                "target_price": signal.target_price,
                "current_price": signal.current_price,
                "twap_window_minutes": signal.twap_window_minutes,
                "target_pct": signal.target_pct,
                "signal_price": signal.price,
                "action": order.action.value,
                "order_size": order.size,
                "order_price": order.price,
                "status": result.status.value,
                "filled_price": result.filled_price,
                "filled_size": result.filled_size,
                "reason": result.reason,
            },
        )

        summary = (
            f"{result.status.value}: {order.action.value} {signal.outcome.value} "
            f"{result.filled_size if result.filled_size is not None else order.size:.2f} @ "
            f"{result.filled_price if result.filled_price is not None else order.price:.2f}"
        )
        if result.reason:
            summary += f" ({result.reason})"

        if result.status is OrderStatus.FILLED:
            self._rejection_streak[key] = 0
            filled_size = result.filled_size if result.filled_size is not None else order.size
            if filled_size >= order.size * (1 - FULL_FILL_RELATIVE_TOLERANCE):
                # Only mark this target as reached on a *full* fill. FAK can
                # fill partially -- if it did, held only partly moved toward
                # target_shares, so leaving this unset makes the next tick
                # recompute fresh (not frozen) and retry the remainder,
                # rather than treating a partial fill as "done".
                self._converged_target_pct[key] = signal.target_pct
            self._monitor.execution(summary)
        else:
            self._monitor.error(summary)
            streak = self._rejection_streak.get(key, 0) + 1
            self._rejection_streak[key] = streak
            if streak >= self._rejection_alert_threshold:
                self._monitor.error(
                    f"Execution stuck: {streak} consecutive failed attempts for "
                    f"{signal.slug} ({signal.outcome.value}) -- last reason: {result.reason}"
                )

        return result

    @staticmethod
    def _result(
        order: Order,
        status: OrderStatus,
        filled_price: Optional[float] = None,
        filled_size: Optional[float] = None,
        reason: Optional[str] = None,
    ) -> ExecutionResult:
        return ExecutionResult(
            order=order,
            status=status,
            filled_price=filled_price,
            filled_size=filled_size,
            reason=reason,
            timestamp=time.time(),
        )
