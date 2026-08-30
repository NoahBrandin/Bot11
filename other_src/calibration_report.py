"""Rates the strategy's probability estimates against what actually happened,
independent of any trading decision -- every _evaluate_outcome() tick emits a
Signal through converge() regardless of whether a position is wanted (see
manager.py), so run_backtest.py's --trade-log-output already captures one
row per (window, outcome, tick) with the modeled `probability` and the
window's real `resolved_outcome` filled in once it settles. This is a
broader, decision-independent view than backtest.analysis's (currently
unused) _calibration(), which only looks at probabilities at the moment of
entry.

Only Outcome.UP rows are used: Outcome.DOWN rows are exact mirrors
(down_probability = 1 - up_probability, down_outcome = 1 - up_outcome), so
every scoring term is identical between the two -- including both would just
double-count each observation, not add information.

    PYTHONPATH=../server_src python calibration_report.py --trade-log trades.jsonl
"""
from __future__ import annotations

import argparse
import bisect
import json
import math
from collections import defaultdict
from statistics import fmean

DEFAULT_PROB_BUCKET_SIZE = 0.1
DEFAULT_TIME_BUCKET_SECONDS = 60.0
# Clamp before log() so a single p=0/p=1 miss can't send log loss to
# infinity -- standard practice, matches sklearn's log_loss default eps.
LOG_LOSS_EPS = 1e-15


def _load_trade_log(path: str) -> list[dict]:
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _up_observations(records: list[dict]) -> list[tuple[float, int, float]]:
    """(probability, realized 0/1, time_into_window_seconds) for every
    Outcome.UP row with a resolved window."""
    out = []
    for r in records:
        if r["outcome"] != "Up" or r["resolved_outcome"] is None:
            continue
        p = r["probability"]
        realized = 1.0 if r["resolved_outcome"] == "UP" else 0.0
        out.append((p, realized, r["time_into_window_seconds"]))
    return out


def _load_market_index(data_path: str) -> dict[str, dict]:
    """slug -> {"up": sorted (times, prices), "down": sorted (times, prices)}
    -- the same up_prices/down_prices series backtest.engine replays as
    PolymarketPriceChangeEvents, here read directly instead of through the
    strategy so the market's own price (a $0/$1-payoff token's price IS the
    market's probability estimate) can be scored independently of anything
    the strategy decided."""
    with open(data_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    index: dict[str, dict] = {}
    for w in data["windows"]:
        up_sorted = sorted(w["up_prices"], key=lambda pt: pt["t"])
        index[w["slug"]] = {
            "up": ([pt["t"] for pt in up_sorted], [pt["p"] for pt in up_sorted]),
        }
    return index


def _market_probability(market_index: dict, window_slug: str, ts: float) -> float | None:
    """Most recently traded UP price at or before `ts` -- the same "last
    quote known at decision time" a live trader would see. None if the
    window isn't in the index or no trade had happened yet at `ts`."""
    series = market_index.get(window_slug)
    if series is None:
        return None
    times, prices = series["up"]
    i = bisect.bisect_right(times, ts) - 1
    if i < 0:
        return None
    return prices[i]


def _market_observations(
    records: list[dict], market_index: dict
) -> list[tuple[float, int, float]]:
    """Same (probability, realized, time_into_window_seconds) shape as
    _up_observations, but probability is the market's own last-traded price
    at the exact same decision instant the model was scored at -- so the two
    are directly comparable over an identical observation set."""
    out = []
    for r in records:
        if r["outcome"] != "Up" or r["resolved_outcome"] is None:
            continue
        abs_ts = r["window_start"] + r["time_into_window_seconds"]
        p = _market_probability(market_index, r["window_slug"], abs_ts)
        if p is None:
            continue
        realized = 1.0 if r["resolved_outcome"] == "UP" else 0.0
        out.append((p, realized, r["time_into_window_seconds"]))
    return out


def brier_score(obs: list[tuple[float, int, float]]) -> float:
    return fmean((p - y) ** 2 for p, y, _ in obs)


def log_loss(obs: list[tuple[float, int, float]]) -> float:
    total = 0.0
    for p, y, _ in obs:
        p = min(max(p, LOG_LOSS_EPS), 1.0 - LOG_LOSS_EPS)
        total += -(y * math.log(p) + (1.0 - y) * math.log(1.0 - p))
    return total / len(obs)


def reliability_table(obs: list[tuple[float, int, float]], bucket_size: float) -> list[dict]:
    buckets: dict[float, list[tuple[float, int]]] = defaultdict(list)
    for p, y, _ in obs:
        b = min(round(p / bucket_size) * bucket_size, 1.0 - bucket_size / 2 if bucket_size < 1 else 1.0)
        b = round(b, 6)
        buckets[b].append((p, y))
    rows = []
    for b in sorted(buckets):
        group = buckets[b]
        mean_p = fmean(p for p, _ in group)
        realized_rate = fmean(y for _, y in group)
        rows.append(
            {
                "probability_bucket": b,
                "n": len(group),
                "mean_modeled_probability": mean_p,
                "realized_up_rate": realized_rate,
                "gap": realized_rate - mean_p,
            }
        )
    return rows


def time_stratified(obs: list[tuple[float, int, float]], bucket_seconds: float) -> list[dict]:
    buckets: dict[float, list[tuple[float, int]]] = defaultdict(list)
    for p, y, t in obs:
        b = math.floor(t / bucket_seconds) * bucket_seconds
        buckets[b].append((p, y))
    rows = []
    for b in sorted(buckets):
        group = buckets[b]
        p_list = [p for p, _ in group]
        rows.append(
            {
                "time_into_window_bucket_seconds": b,
                "n": len(group),
                "mean_modeled_probability_dev_from_0.5": fmean(abs(p - 0.5) for p in p_list),
                "brier": fmean((p - y) ** 2 for p, y in group),
                "log_loss": fmean(
                    -(y * math.log(min(max(p, LOG_LOSS_EPS), 1 - LOG_LOSS_EPS))
                      + (1 - y) * math.log(1 - min(max(p, LOG_LOSS_EPS), 1 - LOG_LOSS_EPS)))
                    for p, y in group
                ),
            }
        )
    return rows


def _print_table(rows: list[dict], columns: list[tuple[str, str, str]]) -> None:
    """columns: list of (key, header, format_spec)."""
    header = "  ".join(f"{h:>14}" for _, h, _ in columns)
    print(header)
    print("-" * len(header))
    for row in rows:
        cells = []
        for key, _, fmt in columns:
            v = row[key]
            cells.append(f"{v:{fmt}}" if v is not None else f"{'--':>14}")
        print("  ".join(f"{c:>14}" for c in cells))


def _print_side_by_side(
    label: str,
    model_rows: list[dict],
    market_rows: list[dict] | None,
    columns: list[tuple[str, str, str]],
) -> None:
    print(f"=== {label} ===")
    if market_rows is None:
        _print_table(model_rows, columns)
        return
    by_key = columns[0][0]
    market_by_bucket = {r[by_key]: r for r in market_rows}
    header_cols = [("source", "source", "")] + columns
    header = "  ".join(f"{h:>14}" for _, h, _ in header_cols)
    print(header)
    print("-" * len(header))
    for row in model_rows:
        cells = [f"{'model':>14}"]
        for key, _, fmt in columns:
            v = row[key]
            cells.append(f"{v:{fmt}}" if v is not None else f"{'--':>14}")
        print("  ".join(f"{c:>14}" for c in cells))
        mrow = market_by_bucket.get(row[by_key])
        if mrow is not None:
            cells = [f"{'market':>14}"]
            for key, _, fmt in columns:
                v = mrow[key]
                cells.append(f"{v:{fmt}}" if v is not None else f"{'--':>14}")
            print("  ".join(f"{c:>14}" for c in cells))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-log", required=True, help="Path to a trade log written by run_backtest.py --trade-log-output")
    parser.add_argument(
        "--data", default=None,
        help="Path to the original backtest data JSON (run_backtest.py's --data) -- when given, also scores "
             "the market's own last-traded price at each decision instant, as a head-to-head comparison",
    )
    parser.add_argument("--prob-bucket-size", type=float, default=DEFAULT_PROB_BUCKET_SIZE)
    parser.add_argument("--time-bucket-seconds", type=float, default=DEFAULT_TIME_BUCKET_SECONDS)
    args = parser.parse_args()

    records = _load_trade_log(args.trade_log)
    obs = _up_observations(records)
    if not obs:
        print("No resolved Outcome.UP observations found in trade log.")
        return

    baseline = [(0.5, y, t) for _, y, t in obs]

    market_obs = None
    if args.data is not None:
        market_index = _load_market_index(args.data)
        market_obs = _market_observations(records, market_index)

    print(f"Observations (Outcome.UP, resolved windows only): {len(obs)}")
    if market_obs is not None:
        print(f"Observations with a market price available: {len(market_obs)} ({100 * len(market_obs) / len(obs):.1f}% coverage)")
    print(f"Overall realized UP rate: {fmean(y for _, y, _ in obs):.4f}")
    print()
    print(f"{'Brier score (model)':<28} {brier_score(obs):.4f}")
    if market_obs is not None:
        print(f"{'Brier score (market)':<28} {brier_score(market_obs):.4f}   (same market_obs subset -- see coverage above)")
    print(f"{'Brier score (always p=0.5)':<28} {brier_score(baseline):.4f}   (baseline -- lower score = beats coin flip)")
    print(f"{'Log loss (model)':<28} {log_loss(obs):.4f}")
    if market_obs is not None:
        print(f"{'Log loss (market)':<28} {log_loss(market_obs):.4f}")
    print(f"{'Log loss (always p=0.5)':<28} {log_loss(baseline):.4f}   ({math.log(2):.4f} exactly)")
    print()

    print("=== Reliability table -- MODEL (bucketed by modeled probability) ===")
    print("A well-calibrated model has realized_up_rate == mean_modeled_probability in every row (gap ~ 0).")
    print("gap > 0 in the upper buckets and gap < 0 in the lower buckets == UNDERconfident (realized outcomes")
    print("are more extreme than claimed). Opposite pattern == OVERconfident.")
    _print_table(
        reliability_table(obs, args.prob_bucket_size),
        [
            ("probability_bucket", "p_bucket", ".2f"),
            ("n", "n", "d"),
            ("mean_modeled_probability", "mean_p", ".4f"),
            ("realized_up_rate", "realized", ".4f"),
            ("gap", "gap", "+.4f"),
        ],
    )
    print()

    if market_obs is not None:
        print("=== Reliability table -- MARKET (bucketed by market-implied probability) ===")
        _print_table(
            reliability_table(market_obs, args.prob_bucket_size),
            [
                ("probability_bucket", "p_bucket", ".2f"),
                ("n", "n", "d"),
                ("mean_modeled_probability", "mean_p", ".4f"),
                ("realized_up_rate", "realized", ".4f"),
                ("gap", "gap", "+.4f"),
            ],
        )
        print()

    print("=== Calibration by time remaining into the window (0s = window just opened) ===")
    print("mean |p-0.5| is the source's own average conviction in that time bucket; compare brier/log_loss")
    print("between model and market rows at the same t_bucket_s for a same-instant head-to-head.")
    _print_side_by_side(
        "model vs market, by time-into-window",
        time_stratified(obs, args.time_bucket_seconds),
        time_stratified(market_obs, args.time_bucket_seconds) if market_obs is not None else None,
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
