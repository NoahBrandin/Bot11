"""Replays a raw datastream recording (see server_src/utils/record_datastream.py,
loaded here via recording.py) through the real StrategyLayer decision code
(not a reimplementation of it), as fast as the CPU allows.

Unlike the old download.py-based engine, there's no separate "warmup range"
to seed: the recording is one continuous real event stream, so the strategy
naturally starts un-ready and self-warms from the replay exactly like a
fresh live restart would (see commit "Fix GBM readiness gap on restart").

A SimulatedClock stands in for time.time() so window-countdown and
exit-hysteresis logic still see correct historical time while the replay
loop itself runs unthrottled. Every event -- kline, tick, chainlink price,
window open/close, order-book tick -- is replayed in the exact order it was
recorded; window settlement (crediting a still-held position its real $0/$1
payout) happens right after each WindowCloseEvent, using the outcome
recording.py already resolved from the recorded Chainlink stream.
"""
from __future__ import annotations

import json
import logging
import math
from pathlib import Path

from datastream.utils.events import WindowCloseEvent, WindowOpenEvent
from execution.base import OrderStatus
from execution.live import DEFAULT_PRICE_PROTECTION_TOLERANCE
from execution.paper import DEFAULT_TAKER_FEE_RATE
from strategy import StrategyLayer
from strategy.manager import (
    DEFAULT_MOMENTUM_SHRINKAGE,
    DEFAULT_MOMENTUM_WINDOW_SECONDS,
    DEFAULT_MOMENTUM_Z_CAP,
    DEFAULT_PROBABILITY_MARGIN,
    DEFAULT_REVERSION_SHRINKAGE,
    DEFAULT_REVERSION_WINDOW_SECONDS,
    DEFAULT_REVERSION_Z_CAP,
)
from strategy.signal import Signal

from . import analysis
from .clock import SimulatedClock
from .latency_execution import LatencyModelingPaperExecutionLayer
from .recording import load_recording

logger = logging.getLogger(__name__)

# Matches orchestrator.py's live DEFAULT_BINANCE_TICK_INTERVAL -- tells
# probability_model.py's two-scale realized-variance estimator what spacing
# to assume between the price samples it's fed. Not a real measurement of
# the recorded feed's actual push cadence (see orchestrator.py's comment on
# DEFAULT_BINANCE_TICK_INTERVAL); kept identical to live so a backtest run
# calibrates the same way a live run does.
DEFAULT_GBM_TICK_INTERVAL = "1s"

# Per-trade log (see run_backtest's trade_log_path): buckets a continuous
# price/time value onto a coarser grid so the resulting distribution can be
# histogrammed and compared apples-to-apples against a similarly-bucketed
# live trade log, without either side's raw noise (float price jitter,
# sub-second timestamps) hiding the comparison.
DEFAULT_PRICE_BUCKET_SIZE = 0.02
DEFAULT_TIME_BUCKET_SECONDS = 10.0


async def run_backtest(
    log_dir: str,
    starting_bankroll: float = 1000.0,
    max_position_notional: float = 5.0,
    decision_to_fill_latency_seconds: float = 0.0,
    price_protection_tolerance: float = DEFAULT_PRICE_PROTECTION_TOLERANCE,
    trade_log_path: str | None = None,
    analysis_output: str | None = None,
    price_bucket_size: float = DEFAULT_PRICE_BUCKET_SIZE,
    time_bucket_seconds: float = DEFAULT_TIME_BUCKET_SECONDS,
    ewma_halflife_seconds: float | None = None,
    gbm_tick_interval: str = DEFAULT_GBM_TICK_INTERVAL,
    momentum_window_seconds: float = DEFAULT_MOMENTUM_WINDOW_SECONDS,
    momentum_z_cap: float = DEFAULT_MOMENTUM_Z_CAP,
    momentum_shrinkage: float = DEFAULT_MOMENTUM_SHRINKAGE,
    reversion_window_seconds: float = DEFAULT_REVERSION_WINDOW_SECONDS,
    reversion_z_cap: float = DEFAULT_REVERSION_Z_CAP,
    reversion_shrinkage: float = DEFAULT_REVERSION_SHRINKAGE,
    probability_margin: float = DEFAULT_PROBABILITY_MARGIN,
) -> None:
    recording = load_recording(Path(log_dir))
    if not recording.events:
        logger.warning("No events found under %s, nothing to backtest", log_dir)
        return

    clock = SimulatedClock()
    execution = LatencyModelingPaperExecutionLayer(
        starting_bankroll=starting_bankroll,
        clock=clock,
        max_position_notional=max_position_notional,
        decision_to_fill_latency_seconds=decision_to_fill_latency_seconds,
        price_protection_tolerance=price_protection_tolerance,
    )
    execution.index_recording(recording)

    strategy = StrategyLayer(
        execution=execution,
        clock=clock,
        ewma_halflife_seconds=ewma_halflife_seconds,
        gbm_tick_interval=gbm_tick_interval,
        momentum_window_seconds=momentum_window_seconds,
        momentum_z_cap=momentum_z_cap,
        momentum_shrinkage=momentum_shrinkage,
        reversion_window_seconds=reversion_window_seconds,
        reversion_z_cap=reversion_z_cap,
        reversion_shrinkage=reversion_shrinkage,
        probability_margin=probability_margin,
    )

    # Per-trade log: wraps converge() (the one chokepoint every order attempt
    # passes through, live or backtest) to record each attempt's decision
    # price/time alongside its window context -- FILLED as well as
    # REJECTED/FAILED, since rejection patterns (e.g. clustering right after
    # a kline tick) are analysis input too. `_current_window` tracks the
    # most recently opened window (updated on every WindowOpenEvent below);
    # converge() is never called concurrently across windows in this
    # single-threaded replay, so there's no ambiguity about which window a
    # given attempt belongs to even across the brief prefetch overlap where
    # one window's WindowOpenEvent arrives before the previous one's
    # WindowCloseEvent.
    taker_fee_rate = getattr(execution, "_taker_fee_rate", DEFAULT_TAKER_FEE_RATE)
    trade_records: list[dict] = []
    # window_slug -> its own trade_records, so resolving a window's outcome
    # below only has to touch that window's records instead of re-scanning
    # the whole (ever-growing) trade_records list on every window close.
    records_by_window: dict[str, list[dict]] = {}
    _current_window: dict = {}
    original_converge = execution.converge

    async def _recording_converge(signal: Signal):
        result = await original_converge(signal)
        if result is not None and _current_window:
            window_start = _current_window["window_start"]
            slug = _current_window["slug"]
            is_filled = result.status == OrderStatus.FILLED
            filled_price = result.filled_price if result.filled_price is not None else (result.order.price if is_filled else None)
            filled_size = result.filled_size if result.filled_size is not None else (result.order.size if is_filled else None)
            fee = _fee(filled_price, filled_size, taker_fee_rate) if is_filled else 0.0
            time_into_window = signal.timestamp - window_start
            record = {
                "window_slug": slug,
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
            trade_records.append(record)
            records_by_window.setdefault(slug, []).append(record)
        return result

    if trade_log_path is not None or analysis_output is not None:
        execution.converge = _recording_converge

    windows_closed = 0
    windows_unsettled = 0
    for event in recording.events:
        clock.set(event.timestamp)

        if isinstance(event, WindowOpenEvent):
            _current_window = {"slug": event.slug, "window_start": event.window_start}

        await strategy.handle_event(event)

        if isinstance(event, WindowCloseEvent):
            window = recording.windows.get(event.slug)
            if window is not None and window.resolved_outcome is not None:
                up_payout = 1.0 if window.resolved_outcome == "UP" else 0.0
                down_payout = 1.0 if window.resolved_outcome == "DOWN" else 0.0
                await execution.settle(window.up_token_id, up_payout)
                await execution.settle(window.down_token_id, down_payout)
                for record in records_by_window.get(event.slug, ()):
                    if record["resolved_outcome"] is None:
                        record["resolved_outcome"] = window.resolved_outcome
            else:
                # No Chainlink boundary tick to resolve this window's real
                # payout (see recording.py) -- any position still held here
                # never gets settled: its entry cost stays debited from the
                # paper bankroll with nothing credited back, and its
                # trade_records keep resolved_outcome=None (analysis.py
                # excludes those from pnl/win-rate rather than guessing).
                windows_unsettled += 1
                logger.debug("Window %s closed without a resolvable outcome, skipping settlement", event.slug)

            windows_closed += 1
            if windows_closed % 50 == 0:
                logger.info("Replayed %d windows", windows_closed)

    logger.info(
        "Replayed %d events across %d windows (%d closed without a resolvable outcome, skipped)",
        len(recording.events), windows_closed, windows_unsettled,
    )
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
            "log_dir": log_dir,
            "num_windows": len(recording.windows),
            "num_windows_resolved": sum(1 for w in recording.windows.values() if w.resolved_outcome is not None),
            "num_windows_closed_unresolved": windows_unsettled,
            "config": {
                "starting_bankroll": starting_bankroll,
                "max_position_notional": max_position_notional,
                "decision_to_fill_latency_seconds": decision_to_fill_latency_seconds,
                "price_protection_tolerance": price_protection_tolerance,
                "price_bucket_size": price_bucket_size,
                "time_bucket_seconds": time_bucket_seconds,
                "taker_fee_rate": taker_fee_rate,
                "ewma_halflife_seconds": ewma_halflife_seconds,
            },
            "execution_summary": execution_summary,
        }
        report = analysis.build_report(trade_records, list(recording.windows.values()), time_bucket_seconds, meta)
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
