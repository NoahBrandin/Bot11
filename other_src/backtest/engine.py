"""Replays a downloaded backtest data file through the real StrategyLayer
decision code (not a reimplementation of it), as fast as the CPU allows.

A SimulatedClock stands in for time.time() so window-countdown and
exit-hysteresis logic still see correct historical time while the replay
loop itself runs unthrottled. Windows are processed one at a time in
chronological order (they're contiguous and non-overlapping by
construction, so this is equivalent to a single global merge but gives a
natural checkpoint for settlement): after a window's events are fully
replayed, PaperExecutionLayer.settle() is called for both of its tokens
using the window's real resolved outcome. It's a no-op if nothing was held,
so this handles the common case (position already closed by the strategy)
and the "held to expiry" case identically without needing to track which
one happened.
"""
from __future__ import annotations

import json
import logging
import math

from datastream.events import (
    BinanceKlineEvent,
    Outcome,
    PolymarketPriceChangeEvent,
    Side,
    WindowCloseEvent,
    WindowOpenEvent,
)
from execution.base import OrderStatus
from execution.live import DEFAULT_PRICE_PROTECTION_TOLERANCE
from execution.paper import DEFAULT_TAKER_FEE_RATE
from strategy import StrategyLayer
from strategy.orders import Signal

from . import analysis
from .clock import SimulatedClock
from .latency_execution import LatencyModelingPaperExecutionLayer

logger = logging.getLogger(__name__)

# Polymarket's prices-history endpoint gives one traded price per point, not
# separate bid/ask -- approximate the spread around each traded price. The
# original 0.005 (1-cent total spread) was measured against a shallow,
# actively-traded book; it understates the real spread away from that
# condition -- e.g. thinner windows, or the extremes near window close where
# the GBM model's own probability is most confident and real liquidity is
# often worse, not better. Widened 4x by default as a more conservative
# assumption; still a flat approximation, not a modeled function of
# liquidity or time-to-expiry.
DEFAULT_SPREAD_HALF_WIDTH = 0.02
MIN_PRICE = 0.01
MAX_PRICE = 0.99

_KLINE_PRIORITY = 0
_WINDOW_OPEN_PRIORITY = 1
_PRICE_CHANGE_PRIORITY = 2
_WINDOW_CLOSE_PRIORITY = 3

# Per-trade log (see run_backtest's trade_log_path): buckets a continuous
# price/time value onto a coarser grid so the resulting distribution can be
# histogrammed and compared apples-to-apples against a similarly-bucketed
# live trade log, without either side's raw noise (float price jitter,
# sub-second timestamps) hiding the comparison.
DEFAULT_PRICE_BUCKET_SIZE = 0.02
DEFAULT_TIME_BUCKET_SECONDS = 10.0


async def run_backtest(
    data_path: str,
    starting_bankroll: float = 1000.0,
    max_position_notional: float = 5.0,
    spread_half_width: float = DEFAULT_SPREAD_HALF_WIDTH,
    decision_to_fill_latency_seconds: float = 0.0,
    price_protection_tolerance: float = DEFAULT_PRICE_PROTECTION_TOLERANCE,
    trade_log_path: str | None = None,
    analysis_output: str | None = None,
    price_bucket_size: float = DEFAULT_PRICE_BUCKET_SIZE,
    time_bucket_seconds: float = DEFAULT_TIME_BUCKET_SECONDS,
) -> None:
    with open(data_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    windows = data["windows"]
    if not windows:
        logger.warning("No windows in %s, nothing to backtest", data_path)
        return

    klines = sorted(data["binance_klines"], key=lambda k: k["t"])

    clock = SimulatedClock()
    execution = LatencyModelingPaperExecutionLayer(
        starting_bankroll=starting_bankroll,
        clock=clock,
        max_position_notional=max_position_notional,
        decision_to_fill_latency_seconds=decision_to_fill_latency_seconds,
        price_protection_tolerance=price_protection_tolerance,
        spread_half_width=spread_half_width,
    )
    # StrategyLayer.handle_event() calls execution.on_window_close() on every
    # WindowCloseEvent, which for the real PaperExecutionLayer polls the live
    # Gamma API to resolve the window before settling -- needed for a live
    # paper-mode dry run, but wrong here: this loop already knows every
    # window's real resolved outcome from the downloaded data and settles it
    # directly below with execution.settle(...). Left wired up, replaying
    # ~hundreds of windows would fire that many unthrottled live network
    # calls into an otherwise fully offline, deterministic replay. No-op it
    # for the backtest only; production paper-mode is unaffected.
    async def _no_network_settlement(slug: str) -> None:
        return None

    execution.on_window_close = _no_network_settlement

    # Mirrors _no_network_settlement above: manager.py's reference-price
    # capture falls back to a live Binance REST call when the kline feed
    # looks stale or missing at window open (see
    # StrategyLayer._resolve_live_reference_price). In this offline replay
    # there's no real connection to be stale -- _current_price is simply
    # unset until the first kline event lands (e.g. every backtest's very
    # first window, before any kline has been replayed yet) -- so stub the
    # fallback to return None, which reproduces the pre-fallback behavior of
    # deferring capture to the next kline tick, instead of making a live
    # network call mid-replay.
    async def _no_network_price_fallback(symbol: str) -> None:
        return None

    strategy = StrategyLayer(execution=execution, clock=clock, price_fallback=_no_network_price_fallback)

    # Per-trade log: wraps converge() (the one chokepoint every order attempt
    # passes through, live or backtest) to record each attempt's decision
    # price/time alongside its window context -- FILLED as well as
    # REJECTED/FAILED, since rejection patterns (e.g. clustering right after
    # a kline tick) are analysis input too. `_current_window` is mutable
    # state closed over by _recording_converge, updated at the top of each
    # iteration of the loop below -- converge() is never called concurrently
    # across windows in this single-threaded replay, so there's no ambiguity
    # about which window a given attempt belongs to.
    taker_fee_rate = getattr(execution, "_taker_fee_rate", DEFAULT_TAKER_FEE_RATE)
    trade_records: list[dict] = []
    _current_window: dict = {}
    original_converge = execution.converge

    async def _recording_converge(signal: Signal):
        result = await original_converge(signal)
        if result is not None and _current_window:
            window_start = _current_window["window_start"]
            is_filled = result.status == OrderStatus.FILLED
            filled_price = result.filled_price if result.filled_price is not None else (result.order.price if is_filled else None)
            filled_size = result.filled_size if result.filled_size is not None else (result.order.size if is_filled else None)
            fee = _fee(filled_price, filled_size, taker_fee_rate) if is_filled else 0.0
            time_into_window = signal.timestamp - window_start
            trade_records.append(
                {
                    "window_slug": _current_window["slug"],
                    "window_start": window_start,
                    "outcome": signal.outcome.value,
                    "action": result.order.action.value,
                    "status": result.status.value,
                    "reason": result.reason,
                    "probability": signal.probability,
                    "target_pct": signal.target_pct,
                    "decision_price": signal.price,
                    "decision_price_bucket": _bucket_round(signal.price, price_bucket_size),
                    "order_price": result.order.price,
                    "order_size": result.order.size,
                    "filled_price": filled_price,
                    "filled_price_bucket": _bucket_round(filled_price, price_bucket_size) if filled_price is not None else None,
                    "filled_size": filled_size,
                    "fee": fee,
                    "time_into_window_seconds": time_into_window,
                    "time_into_window_bucket": _bucket_floor(time_into_window, time_bucket_seconds),
                    # Filled in once the window resolves, below.
                    "resolved_outcome": None,
                }
            )
        return result

    if trade_log_path is not None or analysis_output is not None:
        execution.converge = _recording_converge

    first_window_start = windows[0]["window_start"]
    kline_idx = 0
    warmup_closes = []
    while kline_idx < len(klines) and klines[kline_idx]["t"] < first_window_start:
        warmup_closes.append(klines[kline_idx]["c"])
        kline_idx += 1
    strategy.seed_history(warmup_closes)
    logger.info("Seeded %d minutes of warmup history", len(warmup_closes))

    for i, window in enumerate(windows):
        window_start = window["window_start"]
        window_end = window["window_end"]

        execution.register_window(window)
        _current_window.clear()
        _current_window.update(slug=window["slug"], window_start=window_start)
        trades_before = len(trade_records)

        events: list[tuple[float, int, object]] = []

        while kline_idx < len(klines) and klines[kline_idx]["t"] <= window_end:
            events.append((klines[kline_idx]["t"], _KLINE_PRIORITY, _make_kline_event(klines[kline_idx])))
            kline_idx += 1

        events.append(
            (
                window_start,
                _WINDOW_OPEN_PRIORITY,
                WindowOpenEvent(
                    timestamp=window_start,
                    slug=window["slug"],
                    condition_id=window["condition_id"],
                    window_start=window_start,
                    window_end=window_end,
                    up_token_id=window["up_token_id"],
                    down_token_id=window["down_token_id"],
                ),
            )
        )
        for point in window["up_prices"]:
            events.append(
                (point["t"], _PRICE_CHANGE_PRIORITY, _make_price_change_event(point, window, Outcome.UP, spread_half_width))
            )
        for point in window["down_prices"]:
            events.append(
                (point["t"], _PRICE_CHANGE_PRIORITY, _make_price_change_event(point, window, Outcome.DOWN, spread_half_width))
            )
        events.append(
            (
                window_end,
                _WINDOW_CLOSE_PRIORITY,
                WindowCloseEvent(
                    timestamp=window_end,
                    slug=window["slug"],
                    condition_id=window["condition_id"],
                    window_start=window_start,
                    window_end=window_end,
                ),
            )
        )

        events.sort(key=lambda e: (e[0], e[1]))
        for t, _, event in events:
            clock.set(t)
            await strategy.handle_event(event)

        up_payout = 1.0 if window["resolved_outcome"] == "UP" else 0.0
        down_payout = 1.0 if window["resolved_outcome"] == "DOWN" else 0.0
        await execution.settle(window["up_token_id"], up_payout)
        await execution.settle(window["down_token_id"], down_payout)

        for record in trade_records[trades_before:]:
            record["resolved_outcome"] = window["resolved_outcome"]

        if (i + 1) % 50 == 0:
            logger.info("Replayed %d/%d windows", i + 1, len(windows))

    logger.info("Replayed %d windows", len(windows))
    execution.log_stats()

    if trade_log_path is not None:
        with open(trade_log_path, "w", encoding="utf-8") as f:
            for record in trade_records:
                f.write(json.dumps(record))
                f.write("\n")
        logger.info("Wrote %d trade records to %s", len(trade_records), trade_log_path)

    if analysis_output is not None:
        execution_summary = {
            "starting_bankroll": execution._starting_bankroll,
            "ending_bankroll": execution._bankroll,
            "pnl": execution._bankroll - execution._starting_bankroll,
            "pnl_pct": (
                (execution._bankroll - execution._starting_bankroll) / execution._starting_bankroll * 100.0
                if execution._starting_bankroll
                else 0.0
            ),
            "fill_count": execution._fill_count,
            "buy_count": execution._buy_count,
            "sell_count": execution._sell_count,
            "reject_count": execution._reject_count,
            "settle_count": execution._settle_count,
            "settled_value": execution._settled_value,
            "total_fees": execution._total_fees,
        }
        meta = {
            "data_path": data_path,
            "num_windows": len(windows),
            "config": {
                "starting_bankroll": starting_bankroll,
                "max_position_notional": max_position_notional,
                "spread_half_width": spread_half_width,
                "decision_to_fill_latency_seconds": decision_to_fill_latency_seconds,
                "price_protection_tolerance": price_protection_tolerance,
                "price_bucket_size": price_bucket_size,
                "time_bucket_seconds": time_bucket_seconds,
                "taker_fee_rate": taker_fee_rate,
            },
            "execution_summary": execution_summary,
        }
        report = analysis.build_report(trade_records, windows, time_bucket_seconds, meta)
        with open(analysis_output, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        logger.info("Wrote analysis report to %s", analysis_output)


def _fee(price: float | None, size: float | None, taker_fee_rate: float) -> float:
    # Mirrors execution/paper.py's PaperExecutionLayer._place_order fee
    # calc exactly -- kept here rather than imported since it's a one-line
    # formula and pulling it out into a shared helper would be more ceremony
    # than the duplication it removes.
    if price is None or size is None:
        return 0.0
    return taker_fee_rate * size * price * (1.0 - price)


def _bucket_round(value: float, size: float) -> float:
    """Snaps `value` to the nearest multiple of `size` -- used for price,
    where a bucket represents "the value is approximately this"."""
    if size <= 0:
        return value
    return round(round(value / size) * size, 6)


def _bucket_floor(value: float, size: float) -> float:
    """Assigns `value` to the bucket it falls in, e.g. size=10 groups
    [0, 10) -> 0, [10, 20) -> 10 -- used for time-into-window, where a bucket
    represents "which interval this happened in", not a nearest-value snap."""
    if size <= 0:
        return value
    return math.floor(value / size) * size


def _make_kline_event(k: dict) -> BinanceKlineEvent:
    # k["t"] is this candle's close time (see download.py) -- the moment its
    # close price actually became known, and what drives clock.set(t) in the
    # replay loop above, so the strategy sees this price no earlier than a
    # real-time listener would have.
    close = k["c"]
    return BinanceKlineEvent(
        timestamp=k["t"],
        symbol="BTCUSDT",
        interval="1m",
        kline_open_time=k["t"] - 60.0,
        kline_close_time=k["t"],
        open=close,
        high=close,
        low=close,
        close=close,
        volume=0.0,
        is_closed=True,
    )


def _make_price_change_event(
    point: dict, window: dict, outcome: Outcome, spread_half_width: float
) -> PolymarketPriceChangeEvent:
    price = float(point["p"])
    ask = min(MAX_PRICE, round(price + spread_half_width, 2))
    bid = max(MIN_PRICE, round(price - spread_half_width, 2))
    return PolymarketPriceChangeEvent(
        timestamp=point["t"],
        slug=window["slug"],
        asset_id=window["up_token_id"] if outcome is Outcome.UP else window["down_token_id"],
        outcome=outcome,
        price=price,
        size=0.0,
        side=Side.BUY,
        best_bid=bid,
        best_ask=ask,
    )
