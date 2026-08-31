"""Scores the *actual* live model's Brier score over time-into-window,
against any recording.py-loadable log directory -- real recorder logs
(server_src/utils/record_datastream.py) or backtest/download.py's
downloaded, Binance-only synthetic ones alike, since both produce the exact
same datastream.jsonl schema (see backtest/recording.py).

Runs the real StrategyLayer decision code via a thin replay loop copied from
backtest/engine.py::run_backtest() (SimulatedClock + PaperExecutionLayer +
StrategyLayer, same wiring, same defaults) rather than reimplementing
GBMEstimator/momentum_mu/reversion_mu/settlement_probability_up by hand a
second time -- three different from-scratch reimplementations drifted from
the real code during this session's earlier ad-hoc testing (each subtly
wrong about tick_interval, mu source, or reversion_mu's existence). The only
thing patched in here is a thin recorder around strategy.manager's
settlement_probability_up call site: captures every real call's (probability,
window, time-into-window) with zero reimplemented model logic, then lets it
run exactly like a live restart replay (no fills needed -- a resolved window
does not require any PolymarketPriceChangeEvents, entries only happen if a
quote is present, and capturing here happens independently of that).

    python model_brier_report.py --log-dir ../data/recorder_logs_2026-08-30
    python model_brier_report.py --log-dir ../data/downloaded_120h --gbm-tick-interval 2s
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from calibration_report import _print_table, brier_score, log_loss, time_stratified  # noqa: E402

import strategy.manager as manager_module  # noqa: E402
from backtest.clock import SimulatedClock  # noqa: E402
from backtest.latency_execution import LatencyModelingPaperExecutionLayer  # noqa: E402
from backtest.recording import load_recording  # noqa: E402
from strategy import StrategyLayer  # noqa: E402
from strategy.manager import (  # noqa: E402
    DEFAULT_MOMENTUM_SHRINKAGE,
    DEFAULT_MOMENTUM_WINDOW_SECONDS,
    DEFAULT_MOMENTUM_Z_CAP,
    DEFAULT_PROBABILITY_MARGIN,
    DEFAULT_REVERSION_SHRINKAGE,
    DEFAULT_REVERSION_WINDOW_SECONDS,
    DEFAULT_REVERSION_Z_CAP,
)

# Matches orchestrator.py's live BINANCE_TICK_INTERVAL default -- override
# with --gbm-tick-interval to match whatever's actually set in .env (e.g.
# "2s", see manager.py's _INTERVAL_SECONDS comment for why the "1s" default
# undercounts the real kline WS push cadence).
DEFAULT_GBM_TICK_INTERVAL = "1s"
DEFAULT_TIME_BUCKET_SECONDS = 60.0


async def score(
    log_dir: str,
    gbm_tick_interval: str = DEFAULT_GBM_TICK_INTERVAL,
    momentum_window_seconds: float = DEFAULT_MOMENTUM_WINDOW_SECONDS,
    momentum_z_cap: float = DEFAULT_MOMENTUM_Z_CAP,
    momentum_shrinkage: float = DEFAULT_MOMENTUM_SHRINKAGE,
    reversion_window_seconds: float = DEFAULT_REVERSION_WINDOW_SECONDS,
    reversion_z_cap: float = DEFAULT_REVERSION_Z_CAP,
    reversion_shrinkage: float = DEFAULT_REVERSION_SHRINKAGE,
    probability_margin: float = DEFAULT_PROBABILITY_MARGIN,
) -> list[tuple[float, float, float]]:
    """Returns (probability, realized 0/1, time_into_window_seconds) for
    every settlement_probability_up() call the real StrategyLayer made
    against a window that ended up resolved -- one row per evaluated tick,
    not just at fixed minute marks (unlike this session's earlier ad-hoc
    sampling), so callers bucket with calibration_report.time_stratified()."""
    recording = load_recording(Path(log_dir))
    if not recording.events:
        raise SystemExit(f"No events found under {log_dir}")

    clock = SimulatedClock()
    execution = LatencyModelingPaperExecutionLayer(starting_bankroll=1000.0, clock=clock)
    execution.index_recording(recording)

    strategy = StrategyLayer(
        execution=execution,
        clock=clock,
        gbm_tick_interval=gbm_tick_interval,
        momentum_window_seconds=momentum_window_seconds,
        momentum_z_cap=momentum_z_cap,
        momentum_shrinkage=momentum_shrinkage,
        reversion_window_seconds=reversion_window_seconds,
        reversion_z_cap=reversion_z_cap,
        reversion_shrinkage=reversion_shrinkage,
        probability_margin=probability_margin,
    )

    obs: list[tuple[float, float, float]] = []
    original = manager_module.settlement_probability_up

    def _capturing_settlement_probability_up(*args, **kwargs):
        p_up = original(*args, **kwargs)
        window = strategy._window
        if window is not None:
            resolved = recording.windows.get(window.slug)
            if resolved is not None and resolved.resolved_outcome is not None:
                realized = 1.0 if resolved.resolved_outcome == "UP" else 0.0
                time_into_window = clock() - window.window_start
                obs.append((p_up, realized, time_into_window))
        return p_up

    manager_module.settlement_probability_up = _capturing_settlement_probability_up
    try:
        for event in recording.events:
            clock.set(event.timestamp)
            await strategy.handle_event(event)
    finally:
        manager_module.settlement_probability_up = original

    return obs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--log-dir", required=True, help="Directory containing datastream.jsonl(.N) files (recorded or downloaded)")
    parser.add_argument("--gbm-tick-interval", default=DEFAULT_GBM_TICK_INTERVAL, help='e.g. "1s", "2s" -- must match the actual feed cadence, see manager.py')
    parser.add_argument("--momentum-shrinkage", type=float, default=DEFAULT_MOMENTUM_SHRINKAGE)
    parser.add_argument("--momentum-window-seconds", type=float, default=DEFAULT_MOMENTUM_WINDOW_SECONDS)
    parser.add_argument("--reversion-shrinkage", type=float, default=DEFAULT_REVERSION_SHRINKAGE)
    parser.add_argument("--reversion-window-seconds", type=float, default=DEFAULT_REVERSION_WINDOW_SECONDS)
    parser.add_argument("--time-bucket-seconds", type=float, default=DEFAULT_TIME_BUCKET_SECONDS)
    args = parser.parse_args()

    obs = asyncio.run(
        score(
            args.log_dir,
            gbm_tick_interval=args.gbm_tick_interval,
            momentum_window_seconds=args.momentum_window_seconds,
            momentum_shrinkage=args.momentum_shrinkage,
            reversion_window_seconds=args.reversion_window_seconds,
            reversion_shrinkage=args.reversion_shrinkage,
        )
    )
    if not obs:
        print("No scoreable (probability, resolved-outcome) observations found.")
        return

    baseline = [(0.5, y, t) for _, y, t in obs]
    print(f"gbm_tick_interval={args.gbm_tick_interval}  momentum_shrinkage={args.momentum_shrinkage}  reversion_shrinkage={args.reversion_shrinkage}")
    print(f"Observations: {len(obs)}")
    print(f"{'Brier score (model)':<28} {brier_score(obs):.4f}")
    print(f"{'Brier score (always p=0.5)':<28} {brier_score(baseline):.4f}")
    print(f"{'Log loss (model)':<28} {log_loss(obs):.4f}")
    print(f"{'Log loss (always p=0.5)':<28} {log_loss(baseline):.4f}")
    print()
    print("=== Model Brier score by time into the window ===")
    _print_table(
        time_stratified(obs, args.time_bucket_seconds),
        [
            ("time_into_window_bucket_seconds", "t_bucket_s", ".0f"),
            ("n", "n", "d"),
            ("mean_modeled_probability_dev_from_0.5", "mean|p-.5|", ".4f"),
            ("brier", "brier", ".4f"),
            ("log_loss", "log_loss", ".4f"),
        ],
    )


if __name__ == "__main__":
    main()
