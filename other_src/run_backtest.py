"""Entry point: replays a downloaded backtest data file (see
run_backtest_download.py) through the real Strategy-Layer decision code as
fast as the CPU allows, and logs a final Paper execution summary.

    python run_backtest.py [--data PATH] [--bankroll N]
"""
from __future__ import annotations

import argparse
import asyncio
import logging

#from backtest.download import DEFAULT_OUTPUT_PATH
from backtest.engine import (
    DEFAULT_PRICE_BUCKET_SIZE,
    DEFAULT_SPREAD_HALF_WIDTH,
    DEFAULT_TIME_BUCKET_SECONDS,
    run_backtest,
)
from execution.live import DEFAULT_PRICE_PROTECTION_TOLERANCE

DEFAULT_OUTPUT_PATH = "backtest_data_1d.json"

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", default=DEFAULT_OUTPUT_PATH, help="Path to the downloaded backtest data JSON file")
    parser.add_argument("--bankroll", type=float, default=1000.0, help="Starting paper bankroll")
    parser.add_argument(
        "--max-position-notional", type=float, default=5.0,
        help="Hard cap on a single position's dollar notional (must clear MIN_ORDER_SIZE * max share price)",
    )
    parser.add_argument(
        "--spread-half-width", type=float, default=DEFAULT_SPREAD_HALF_WIDTH,
        help="Synthetic half-spread added/subtracted around each historical traded price to fake a bid/ask",
    )
    parser.add_argument(
        "--latency-ms", type=float, default=0.0,
        help="Simulated decision-to-order-landing delay in milliseconds; the fill price is re-looked-up "
             "from the historical tick series at (decision_time + latency) instead of the decision-time price",
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
    args = parser.parse_args()

    asyncio.run(
        run_backtest(
            args.data,
            starting_bankroll=args.bankroll,
            max_position_notional=args.max_position_notional,
            spread_half_width=args.spread_half_width,
            decision_to_fill_latency_seconds=args.latency_ms / 1000.0,
            price_protection_tolerance=args.price_protection_tolerance,
            trade_log_path=args.trade_log_output,
            analysis_output=args.analysis_output,
            price_bucket_size=args.price_bucket_size,
            time_bucket_seconds=args.time_bucket_seconds,
        )
    )


if __name__ == "__main__":
    main()
