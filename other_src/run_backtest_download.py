"""Entry point: downloads historical BTC + Polymarket data into a single
JSON file (default: last 24 hours) for run_backtest.py to replay.

    python run_backtest_download.py [--hours N] [--output PATH]
"""
from __future__ import annotations

import argparse
import asyncio
import logging

from backtest.download import DEFAULT_HOURS, DEFAULT_OUTPUT_PATH, download


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=int, default=DEFAULT_HOURS, help="How many hours of history to download")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="Output JSON file path")
    args = parser.parse_args()

    asyncio.run(download(hours=args.hours, output_path=args.output))


if __name__ == "__main__":
    main()
