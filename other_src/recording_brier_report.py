"""Scores the Polymarket market's own implied probability (its UP best_bid/
best_ask midpoint) at every full-minute mark inside each 5-min window,
straight from a raw datastream recording (see
server_src/utils/record_datastream.py) -- no backtest run, strategy replay,
or network access needed.

Unlike calibration_report.py (which scores a run_backtest.py trade log and
needs a separately downloaded --data file to also pull in the market's
price), this reads window_open/window_close/polymarket_price_change events
directly off the recorder's JSON-Lines log(s).

The market's "price" is read as the best_bid/best_ask midpoint off each
outcome="Up" polymarket_price_change event, not that event's own `price`
field -- `price` is an individual order-book price-LEVEL update (Polymarket's
raw CLOB `price_change` WS message: whatever level just changed size, often a
thin/spoof level far from the top of book), not a trade or a quote. Sanity-
checked directly against this recording: at a given instant, the same
window's consecutive `price` values bounce between e.g. 0.01 and 0.99 while
best_bid/best_ask sit still around a stable 0.60/0.61 -- the bid/ask midpoint
is the actual market-implied probability, `price` alone is noise (and scores
a Brier *worse* than a coin flip if used directly).

The one thing the recording doesn't contain explicitly is how each window
resolved -- but it doesn't need to be fetched from Gamma either, since the
recording already has everything needed to derive it: chainlink_feed.py's
module docstring confirms Polymarket's 5-min BTC market resolves against the
60-second Chainlink TWAP stream itself (https://data.chain.link/streams/
btc-usd-twap-60s-streams), which is exactly what each recorded
chainlink_price event already is. manager.py::_try_capture_target_price
independently confirms the live strategy uses the same mechanism for its own
reference_price: the Chainlink tick whose source_timestamp lands on
window_start's own second. This script mirrors that exactly -- resolved
outcome = UP iff the Chainlink tick at window_end's second >= the Chainlink
tick at window_start's second. Windows missing either exact tick (feed gaps,
reconnects) are skipped rather than guessed at.

Rotated recorder logs (datastream.jsonl, .1, .2, ...) are read oldest-first
automatically -- point --log-dir at the directory containing them:

    python recording_brier_report.py --log-dir ../data/recorder_logs_2026-08-30
"""
from __future__ import annotations

import argparse
import bisect
import json
import re
import sys
from pathlib import Path
from statistics import fmean

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from calibration_report import _print_table, brier_score, log_loss, time_stratified  # noqa: E402

DEFAULT_LOG_DIR = _HERE.parent / "data" / "recorder_logs_2026-08-30"
DEFAULT_MINUTE_STEP = 60.0
DEFAULT_START_OFFSET = 60.0

_ROTATION_SUFFIX = re.compile(r"\.jsonl\.(\d+)$")


def _rotation_sort_key(path: Path) -> int:
    """Oldest-first ordering for RotatingFileHandler output: datastream.jsonl.N
    is older than .N-1, ..., older than the un-suffixed (current) file."""
    m = _ROTATION_SUFFIX.search(path.name)
    return int(m.group(1)) if m else -1


def _discover_log_files(log_dir: Path) -> list[Path]:
    files = sorted(log_dir.glob("datastream.jsonl*"), key=_rotation_sort_key, reverse=True)
    if not files:
        raise SystemExit(f"No datastream.jsonl* files found in {log_dir}")
    return files


class _WindowState:
    __slots__ = ("slug", "window_start", "window_end", "closed", "up_prices")

    def __init__(self, slug: str, window_start: float, window_end: float) -> None:
        self.slug = slug
        self.window_start = window_start
        self.window_end = window_end
        self.closed = False
        self.up_prices: list[tuple[float, float]] = []  # (timestamp, best_bid/best_ask midpoint), append-order


def _load_recording(
    log_files: list[Path],
) -> tuple[dict[str, _WindowState], dict[int, float]]:
    windows: dict[str, _WindowState] = {}
    chainlink_by_second: dict[int, float] = {}
    bad_lines = 0
    for path in log_files:
        with path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    bad_lines += 1
                    continue
                event = record.get("event")
                if event == "window_open":
                    slug = record["slug"]
                    windows[slug] = _WindowState(slug, record["window_start"], record["window_end"])
                elif event == "window_close":
                    w = windows.get(record["slug"])
                    if w is not None:
                        w.closed = True
                elif event == "polymarket_price_change" and record.get("outcome") == "Up":
                    w = windows.get(record["slug"])
                    best_bid, best_ask = record.get("best_bid"), record.get("best_ask")
                    if w is not None and best_bid is not None and best_ask is not None:
                        w.up_prices.append((record["timestamp"], (best_bid + best_ask) / 2.0))
                elif event == "chainlink_price":
                    # Later ticks for the same second (feed jitter) overwrite
                    # earlier ones -- last-observed value for that second,
                    # same as what a live consumer would end up holding.
                    chainlink_by_second[round(record["source_timestamp"])] = record["price"]
    if bad_lines:
        print(f"WARNING: {bad_lines} unparseable log line(s) skipped\n")
    for w in windows.values():
        w.up_prices.sort(key=lambda pt: pt[0])
    return windows, chainlink_by_second


def _price_at_or_before(up_prices: list[tuple[float, float]], ts: float) -> float | None:
    times = [t for t, _ in up_prices]
    i = bisect.bisect_right(times, ts) - 1
    if i < 0:
        return None
    return up_prices[i][1]


def _resolve_outcome(w: _WindowState, chainlink_by_second: dict[int, float]) -> str | None:
    """UP/DOWN via the same Chainlink-tick-at-the-boundary-second rule
    manager.py::_try_capture_target_price uses for its own reference_price
    -- None if either boundary tick is missing from the recording."""
    target_price = chainlink_by_second.get(round(w.window_start))
    settlement_price = chainlink_by_second.get(round(w.window_end))
    if target_price is None or settlement_price is None:
        return None
    return "UP" if settlement_price >= target_price else "DOWN"


def _minute_offsets(window_seconds: float, start: float, step: float, end: float | None) -> list[float]:
    end = window_seconds if end is None else end
    offsets = []
    t = start
    while t < end:
        offsets.append(t)
        t += step
    return offsets


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--log-dir", default=str(DEFAULT_LOG_DIR), help="Directory containing datastream.jsonl(.N) recorder logs")
    parser.add_argument("--minute-step", type=float, default=DEFAULT_MINUTE_STEP, help="Seconds between full-minute marks (default: 60)")
    parser.add_argument("--start-offset", type=float, default=DEFAULT_START_OFFSET, help="First offset into the window to score, in seconds (default: 60, i.e. skip t=0)")
    parser.add_argument("--end-offset", type=float, default=None, help="Last offset (exclusive) to score, in seconds (default: the window length, i.e. skip the close itself)")
    parser.add_argument("--max-windows", type=int, default=None, help="Only score the first N resolved windows found (for a quick smoke test)")
    args = parser.parse_args()

    log_dir = Path(args.log_dir)
    log_files = _discover_log_files(log_dir)
    print(f"Reading {len(log_files)} recorder log file(s) from {log_dir} (oldest first):")
    for p in log_files:
        print(f"  {p.name}")
    print()

    windows, chainlink_by_second = _load_recording(log_files)
    print(f"Windows recorded: {len(windows)}, Chainlink ticks indexed: {len(chainlink_by_second)}")

    resolved_slugs: list[tuple[str, str]] = []  # (slug, resolved_outcome)
    unresolved_no_chainlink = 0
    for slug in sorted(windows, key=lambda s: windows[s].window_start):
        w = windows[slug]
        if not (w.closed and w.up_prices):
            continue
        outcome = _resolve_outcome(w, chainlink_by_second)
        if outcome is None:
            unresolved_no_chainlink += 1
            continue
        resolved_slugs.append((slug, outcome))

    if args.max_windows is not None:
        resolved_slugs = resolved_slugs[: args.max_windows]

    print(f"Closed windows with a traded UP price and both Chainlink boundary ticks: {len(resolved_slugs)}")
    if unresolved_no_chainlink:
        print(f"  ({unresolved_no_chainlink} closed window(s) skipped -- missing a boundary Chainlink tick)")
    print()
    if not resolved_slugs:
        print("Nothing to score.")
        return

    obs: list[tuple[float, int, float]] = []
    for slug, outcome in resolved_slugs:
        w = windows[slug]
        realized = 1.0 if outcome == "UP" else 0.0
        window_seconds = w.window_end - w.window_start
        for offset in _minute_offsets(window_seconds, args.start_offset, args.minute_step, args.end_offset):
            p = _price_at_or_before(w.up_prices, w.window_start + offset)
            if p is None:
                continue
            obs.append((p, realized, offset))

    if not obs:
        print("No scoreable (price, resolved-outcome) observations found.")
        return

    baseline = [(0.5, y, t) for _, y, t in obs]

    print(f"Observations (market UP price at each full-minute mark, resolved windows only): {len(obs)}")
    print(f"Overall realized UP rate: {fmean(y for _, y, _ in obs):.4f}")
    print()
    print(f"{'Brier score (market)':<28} {brier_score(obs):.4f}")
    print(f"{'Brier score (always p=0.5)':<28} {brier_score(baseline):.4f}   (baseline -- lower score = beats coin flip)")
    print(f"{'Log loss (market)':<28} {log_loss(obs):.4f}")
    print(f"{'Log loss (always p=0.5)':<28} {log_loss(baseline):.4f}")
    print()

    print("=== Market Brier score by full-minute mark into the window ===")
    _print_table(
        time_stratified(obs, args.minute_step),
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
