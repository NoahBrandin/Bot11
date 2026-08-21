"""Aggregates a backtest run's per-order trade log into review-ready summary
statistics: order timing/price distributions, paired-position economics,
probability calibration, and rejection patterns.

Pure aggregation over the trade_records engine.py's per-trade log already
builds (see engine.py::_recording_converge) plus the run's window list --
no I/O, no replay, so the report is always consistent with whatever
trade_records the engine actually produced.
"""
from __future__ import annotations

from collections import defaultdict
from statistics import fmean, median
from typing import Optional

CALIBRATION_BUCKET_SIZE = 0.05


def build_report(
    trade_records: list[dict],
    windows: list[dict],
    time_bucket_seconds: float,
    meta: dict,
) -> dict:
    filled = [r for r in trade_records if r["status"] == "FILLED"]
    not_filled = [r for r in trade_records if r["status"] != "FILLED"]
    time_buckets = _time_bucket_range(windows, time_bucket_seconds)
    positions = _build_positions(filled)

    return {
        "meta": meta,
        "order_time_distribution": _order_time_distribution(filled, time_buckets),
        "order_price_distribution": _order_price_distribution(filled),
        "rejection_summary": _rejection_summary(trade_records, not_filled, time_buckets),
        "positions_summary": _positions_summary(positions),
        "calibration": _calibration(positions),
        "entry_time_pnl": _entry_time_pnl(positions, time_buckets),
        "window_participation": _window_participation(trade_records, windows),
        "positions": positions,
    }


def _time_bucket_range(windows: list[dict], size: float) -> list[float]:
    if not windows:
        return [0.0]
    duration = max(w["window_end"] - w["window_start"] for w in windows)
    if size <= 0:
        return [0.0]
    n = int(duration // size) + 1
    return [i * size for i in range(n)]


def _order_time_distribution(filled: list[dict], time_buckets: list[float]) -> dict:
    result = {}
    for action in ("BUY", "SELL"):
        subset = [r for r in filled if r["action"] == action]
        counts: dict[float, int] = defaultdict(int)
        for r in subset:
            counts[r["time_into_window_bucket"]] += 1
        total = len(subset)
        buckets = [
            {
                "time_into_window_seconds": b,
                "count": counts.get(b, 0),
                "pct": (counts.get(b, 0) / total * 100.0) if total else 0.0,
            }
            for b in time_buckets
        ]
        result[action.lower()] = {"total": total, "buckets": buckets}
    return result


def _order_price_distribution(filled: list[dict]) -> dict:
    result = {}
    for action in ("BUY", "SELL"):
        subset = [r for r in filled if r["action"] == action and r["filled_price_bucket"] is not None]
        counts: dict[float, int] = defaultdict(int)
        for r in subset:
            counts[r["filled_price_bucket"]] += 1
        total = len(subset)
        buckets = [
            {"price_bucket": b, "count": c, "pct": (c / total * 100.0) if total else 0.0}
            for b, c in sorted(counts.items())
        ]
        result[action.lower()] = {"total": total, "buckets": buckets}
    return result


def _reason_category(reason: Optional[str]) -> str:
    if not reason:
        return "unknown"
    if "bankroll" in reason:
        return "insufficient_bankroll"
    if "protection tolerance" in reason:
        return "price_moved_beyond_tolerance"
    if "exceeds held" in reason:
        return "insufficient_held_size"
    return "other"


def _rejection_summary(all_records: list[dict], not_filled: list[dict], time_buckets: list[float]) -> dict:
    total_attempts = len(all_records)
    total_not_filled = len(not_filled)
    reasons: dict[str, int] = defaultdict(int)
    by_time: dict[float, int] = defaultdict(int)
    for r in not_filled:
        reasons[_reason_category(r["reason"])] += 1
        by_time[r["time_into_window_bucket"]] += 1
    return {
        "total_order_attempts": total_attempts,
        "not_filled": total_not_filled,
        "not_filled_rate_pct": (total_not_filled / total_attempts * 100.0) if total_attempts else 0.0,
        "by_reason": dict(sorted(reasons.items(), key=lambda kv: -kv[1])),
        "by_time_into_window": [
            {"time_into_window_seconds": b, "count": by_time[b]} for b in time_buckets if by_time.get(b)
        ],
    }


def _build_positions(filled: list[dict]) -> list[dict]:
    """Pairs each (window, outcome)'s entry (BUY) with its exit (SELL), if
    any -- strategy/manager.py's entry/exit hysteresis normally produces at
    most one of each per window/outcome. Defensive against the unexpected
    (a stray SELL with no open entry, or back-to-back BUYs) rather than
    assuming it can't happen, but doesn't try to model partial-position
    averaging -- that's not something the current strategy does."""
    positions = []
    open_by_key: dict[tuple, dict] = {}
    for r in filled:
        key = (r["window_slug"], r["outcome"])
        if r["action"] == "BUY":
            if key in open_by_key:
                positions.append(_close_position(open_by_key.pop(key), None))
            open_by_key[key] = r
        else:
            entry = open_by_key.pop(key, None)
            if entry is None:
                continue
            positions.append(_close_position(entry, r))
    for entry in open_by_key.values():
        positions.append(_close_position(entry, None))
    return positions


def _close_position(entry: dict, exit_: Optional[dict]) -> dict:
    entry_price = entry["filled_price"]
    entry_size = entry["filled_size"]
    entry_fee = entry["fee"]
    cost = entry_price * entry_size + entry_fee
    resolved_outcome = entry["resolved_outcome"]
    held_to_expiry = exit_ is None
    # entry["outcome"] is Outcome.value ("Up"/"Down"); resolved_outcome comes
    # from the downloaded window data ("UP"/"DOWN", see download.py) --
    # different casing for the same two values, normalize before comparing.
    entered_outcome_won = entry["outcome"].upper() == resolved_outcome

    if exit_ is not None:
        exit_price = exit_["filled_price"]
        exit_fee = exit_["fee"]
        proceeds = exit_price * exit_["filled_size"] - exit_fee
        hold_duration = exit_["time_into_window_seconds"] - entry["time_into_window_seconds"]
        exit_time = exit_["time_into_window_seconds"]
    else:
        # Held to expiry: settles at the resolved $1/$0 payout, not a taker
        # fill, so no exit fee (see execution/paper.py).
        payout_price = 1.0 if entered_outcome_won else 0.0
        proceeds = entry_size * payout_price
        exit_price = payout_price
        exit_fee = 0.0
        hold_duration = None
        exit_time = None

    pnl = proceeds - cost
    return {
        "window_slug": entry["window_slug"],
        "outcome": entry["outcome"],
        "entry_time_into_window_seconds": entry["time_into_window_seconds"],
        "entry_time_into_window_bucket": entry["time_into_window_bucket"],
        "entry_price": entry_price,
        "entry_price_bucket": entry["filled_price_bucket"],
        "entry_probability": entry["probability"],
        "entry_edge": entry["probability"] - entry_price,
        "size": entry_size,
        "held_to_expiry": held_to_expiry,
        "exit_time_into_window_seconds": exit_time,
        "exit_price": exit_price,
        "hold_duration_seconds": hold_duration,
        "resolved_outcome": resolved_outcome,
        "entered_outcome_won": entered_outcome_won,
        "pnl": pnl,
        "win": pnl > 0,
        "fees_paid": entry_fee + exit_fee,
    }


def _positions_summary(positions: list[dict]) -> dict:
    if not positions:
        return {"total_positions": 0}
    pnls = [p["pnl"] for p in positions]
    wins = [p["pnl"] for p in positions if p["pnl"] > 0]
    losses = [p["pnl"] for p in positions if p["pnl"] < 0]
    held = [p for p in positions if p["held_to_expiry"]]
    closed = [p for p in positions if not p["held_to_expiry"]]
    gross_win = sum(wins)
    gross_loss = -sum(losses)
    return {
        "total_positions": len(positions),
        "held_to_expiry": len(held),
        "held_to_expiry_pct": len(held) / len(positions) * 100.0,
        "closed_before_expiry": len(closed),
        "closed_before_expiry_pct": len(closed) / len(positions) * 100.0,
        "win_rate_pct": len(wins) / len(positions) * 100.0,
        "total_pnl": sum(pnls),
        "avg_pnl": fmean(pnls),
        "median_pnl": median(pnls),
        "avg_win": fmean(wins) if wins else 0.0,
        "avg_loss": fmean(losses) if losses else 0.0,
        "profit_factor": (gross_win / gross_loss) if gross_loss else None,
        "avg_entry_edge": fmean(p["entry_edge"] for p in positions),
        "avg_entry_probability": fmean(p["entry_probability"] for p in positions),
        "avg_hold_duration_seconds": fmean(p["hold_duration_seconds"] for p in closed) if closed else None,
        "total_fees_paid": sum(p["fees_paid"] for p in positions),
    }


def _calibration(positions: list[dict], bucket_size: float = CALIBRATION_BUCKET_SIZE) -> list[dict]:
    """Buckets positions by modeled entry probability and compares against
    the actual rate the entered outcome resolved true, and against realized
    win rate -- a well-calibrated model should see actual_outcome_rate track
    mean_modeled_probability closely; systematic drift is a strategy problem,
    not an execution/latency one."""
    groups: dict[float, list[dict]] = defaultdict(list)
    for p in positions:
        b = round(p["entry_probability"] / bucket_size) * bucket_size
        groups[b].append(p)
    rows = []
    for b, group in sorted(groups.items()):
        outcome_hits = sum(1 for p in group if p["entered_outcome_won"])
        win_hits = sum(1 for p in group if p["pnl"] > 0)
        rows.append(
            {
                "probability_bucket": round(b, 4),
                "count": len(group),
                "mean_modeled_probability": fmean(p["entry_probability"] for p in group),
                "actual_outcome_rate_pct": outcome_hits / len(group) * 100.0,
                "win_rate_pct": win_hits / len(group) * 100.0,
                "mean_pnl": fmean(p["pnl"] for p in group),
            }
        )
    return rows


def _entry_time_pnl(positions: list[dict], time_buckets: list[float]) -> list[dict]:
    groups: dict[float, list[dict]] = defaultdict(list)
    for p in positions:
        groups[p["entry_time_into_window_bucket"]].append(p)
    rows = []
    for b in time_buckets:
        group = groups.get(b)
        if not group:
            continue
        wins = sum(1 for p in group if p["pnl"] > 0)
        rows.append(
            {
                "time_into_window_seconds": b,
                "count": len(group),
                "win_rate_pct": wins / len(group) * 100.0,
                "avg_pnl": fmean(p["pnl"] for p in group),
                "avg_entry_edge": fmean(p["entry_edge"] for p in group),
            }
        )
    return rows


def _window_participation(all_records: list[dict], windows: list[dict]) -> dict:
    traded_slugs = {r["window_slug"] for r in all_records}
    total = len(windows)
    return {
        "total_windows": total,
        "windows_with_order_attempt": len(traded_slugs),
        "windows_with_order_attempt_pct": (len(traded_slugs) / total * 100.0) if total else 0.0,
    }
