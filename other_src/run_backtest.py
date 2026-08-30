"""Entry point: replays a raw recorder log directory (see
server_src/utils/record_datastream.py) through the real Strategy-Layer
decision code as fast as the CPU allows, and logs a final Paper execution
summary.

    python run_backtest.py --log-dir ../data/recorder_logs_2026-08-30
"""
from __future__ import annotations

import argparse
import asyncio
import logging

from backtest.engine import (
    DEFAULT_GBM_TICK_INTERVAL,
    DEFAULT_PRICE_BUCKET_SIZE,
    DEFAULT_TIME_BUCKET_SECONDS,
    run_backtest,
)
from execution.live import DEFAULT_PRICE_PROTECTION_TOLERANCE
from strategy.manager import (
    DEFAULT_EWMA_HALFLIFE_SECONDS,
    DEFAULT_MOMENTUM_SHRINKAGE,
    DEFAULT_MOMENTUM_WINDOW_SECONDS,
    DEFAULT_MOMENTUM_Z_CAP,
    DEFAULT_PROBABILITY_MARGIN,
    DEFAULT_REVERSION_SHRINKAGE,
    DEFAULT_REVERSION_WINDOW_SECONDS,
    DEFAULT_REVERSION_Z_CAP,
)

DEFAULT_LOG_DIR = "data/recorder_logs_2026-08-30"


def _optional_float(value: str) -> float | None:
    return None if value.strip().lower() == "none" else float(value)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--log-dir", default=DEFAULT_LOG_DIR,
        help="Directory containing the recorder's datastream.jsonl(.N) files",
    )
    parser.add_argument("--bankroll", type=float, default=1000.0, help="Starting paper bankroll")
    parser.add_argument(
        "--max-position-notional", type=float, default=5.0,
        help="Hard cap on a single position's dollar notional (must clear MIN_ORDER_SIZE * max share price)",
    )
    parser.add_argument(
        "--latency-ms", type=float, default=0.0,
        help="Simulated decision-to-order-landing delay in milliseconds; the fill price is re-looked-up "
             "from the recorded order book at (decision_time + latency) instead of the decision-time price",
    )
    parser.add_argument(
        "--price-protection-tolerance", type=float, default=DEFAULT_PRICE_PROTECTION_TOLERANCE,
        help="Max price move (same units as price, e.g. 0.02) tolerated between decision and fill before "
             "the simulated order is REJECTED instead of filled at the moved price -- matches live's cap",
    )
    parser.add_argument(
        "--trade-log-output", default=None,
        help="Path to write a JSON Lines per-order log (one record per order attempt, FILLED or "
             "REJECTED/FAILED: bucketed decision/fill price, bucketed time-into-window, probability, fee, "
             "etc.) -- omit to skip writing one",
    )
    parser.add_argument(
        "--analysis-output", default=None,
        help="Path to write a single aggregated JSON report for reviewing the run: order timing/price "
             "distributions, paired-position win rate/PnL, probability calibration, rejection patterns -- "
             "omit to skip writing one",
    )
    parser.add_argument(
        "--price-bucket-size", type=float, default=DEFAULT_PRICE_BUCKET_SIZE,
        help="Trade log/analysis only: snaps decision/filled price to the nearest multiple of this",
    )
    parser.add_argument(
        "--time-bucket-seconds", type=float, default=DEFAULT_TIME_BUCKET_SECONDS,
        help="Trade log/analysis only: groups time-into-window into buckets of this many seconds",
    )
    parser.add_argument(
        "--ewma-halflife-seconds", type=_optional_float, default=DEFAULT_EWMA_HALFLIFE_SECONDS,
        help="Switch the GBM estimator from a flat rolling-window mean/stdev to an exponentially "
             "weighted one with this halflife (in seconds, converted to samples internally) -- "
             "pass 'none' to use the flat-window estimator instead",
    )
    parser.add_argument(
        "--gbm-tick-interval", default=DEFAULT_GBM_TICK_INTERVAL,
        help="Assumed spacing between price samples fed to the two-scale realized-variance estimator -- "
             "matches orchestrator.py's live default, change only to study sensitivity to this assumption",
    )
    parser.add_argument(
        "--momentum-window-seconds", type=float, default=DEFAULT_MOMENTUM_WINDOW_SECONDS,
        help="momentum_mu()'s disjoint TWAP leg width in seconds",
    )
    parser.add_argument(
        "--momentum-z-cap", type=float, default=DEFAULT_MOMENTUM_Z_CAP,
        help="momentum_mu()'s cap on the momentum z-score before it's rescaled into mu",
    )
    parser.add_argument(
        "--momentum-shrinkage", type=float, default=DEFAULT_MOMENTUM_SHRINKAGE,
        help="momentum_mu()'s shrinkage on the capped z-score -- 0 reproduces the old fixed mu=0",
    )
    parser.add_argument(
        "--reversion-window-seconds", type=float, default=DEFAULT_REVERSION_WINDOW_SECONDS,
        help="reversion_mu()'s trailing-average window in seconds -- how far back price is "
             "compared against for the long-horizon mean-reversion term",
    )
    parser.add_argument(
        "--reversion-z-cap", type=float, default=DEFAULT_REVERSION_Z_CAP,
        help="reversion_mu()'s cap on the deviation z-score before it's rescaled into mu",
    )
    parser.add_argument(
        "--reversion-shrinkage", type=float, default=DEFAULT_REVERSION_SHRINKAGE,
        help="reversion_mu()'s shrinkage on the capped z-score -- 0 (the default) disables the "
             "reversion term entirely, an unvalidated addition until swept on more data",
    )
    parser.add_argument(
        "--probability-margin", type=float, default=DEFAULT_PROBABILITY_MARGIN,
        help="Minimum required edge over the live quote before entering/exiting a position "
             "(probability must clear ask+margin to buy, or drop below bid-margin to sell)",
    )
    args = parser.parse_args()

    asyncio.run(
        run_backtest(
            args.log_dir,
            starting_bankroll=args.bankroll,
            max_position_notional=args.max_position_notional,
            decision_to_fill_latency_seconds=args.latency_ms / 1000.0,
            price_protection_tolerance=args.price_protection_tolerance,
            trade_log_path=args.trade_log_output,
            analysis_output=args.analysis_output,
            price_bucket_size=args.price_bucket_size,
            time_bucket_seconds=args.time_bucket_seconds,
            ewma_halflife_seconds=args.ewma_halflife_seconds,
            gbm_tick_interval=args.gbm_tick_interval,
            momentum_window_seconds=args.momentum_window_seconds,
            momentum_z_cap=args.momentum_z_cap,
            momentum_shrinkage=args.momentum_shrinkage,
            reversion_window_seconds=args.reversion_window_seconds,
            reversion_z_cap=args.reversion_z_cap,
            reversion_shrinkage=args.reversion_shrinkage,
            probability_margin=args.probability_margin,
        )
    )


if __name__ == "__main__":
    main()
