"""Same metrics as performance_review.py (win rate, right-direction rate,
capital-normalized ROI, avg entry price, profit factor, ...), computed over
a backtest trade log instead of the live wallet -- so the two reports read
side by side and any drift between backtested and live performance is
obvious without eyeballing differently-shaped output.

Feed it the JSON Lines trade log run_backtest.py writes via
--trade-log-output (paired into positions via backtest.analysis's existing
_build_positions, not reimplemented here). backtest/__init__.py imports
datastream (server_src), same as run_backtest.py, so server_src needs to be
on PYTHONPATH for both:

    PYTHONPATH=../server_src python run_backtest.py --data DATA.json --trade-log-output trades.jsonl
    PYTHONPATH=../server_src python backtest_performance_review.py --trade-log trades.jsonl

Two differences from the live report, both because a backtest replay
settles every position at its window's real resolved outcome before moving
on to the next window:
  - There's no "open"/"stuck" split -- every position is decided, so
    everything counts as closed.
  - "Right direction" isn't a cur_price > 0.5 proxy like it is live -- the
    backtest already knows the window's real resolved_outcome, so it's read
    directly off entered_outcome_won (exact, not approximated).
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from statistics import fmean, median

from backtest.analysis import _build_positions

BY_MARKET_HEAD = 15
BY_MARKET_TAIL = 15
EDGE_BUCKET_SIZE = 0.05


def _load_trade_log(path: str) -> list[dict]:
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _positions_summary(positions: list[dict]) -> dict:
    """Same fields/semantics as performance_review.py's _positions_summary,
    computed over backtest position dicts instead of ClosedPosition
    objects."""
    if not positions:
        return {"total_positions": 0}
    pnls = [p["pnl"] for p in positions]
    wins = [v for v in pnls if v > 0]
    losses = [v for v in pnls if v < 0]
    gross_win = sum(wins)
    gross_loss = -sum(losses)

    right_direction = [p for p in positions if p["entered_outcome_won"]]

    capital_deployed = sum(p["entry_price"] * p["size"] for p in positions)
    entry_prices = [p["entry_price"] for p in positions]

    return {
        "total_positions": len(positions),
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": len(positions) - len(wins) - len(losses),
        "win_rate_pct": len(wins) / len(positions) * 100.0,
        "total_pnl": sum(pnls),
        "avg_pnl": fmean(pnls),
        "median_pnl": median(pnls),
        "avg_win": fmean(wins) if wins else 0.0,
        "avg_loss": fmean(losses) if losses else 0.0,
        "profit_factor": (gross_win / gross_loss) if gross_loss else None,
        "direction_total": len(positions),
        "direction_correct": len(right_direction),
        "direction_correct_pct": len(right_direction) / len(positions) * 100.0,
        "capital_deployed": capital_deployed,
        "roi_pct": (sum(pnls) / capital_deployed * 100.0) if capital_deployed else None,
        "avg_entry_price": fmean(entry_prices) if entry_prices else None,
    }


def _print_trade_status(positions: list) -> None:
    print("\n-- Trade Status --")
    total = len(positions)
    if not total:
        print("  No positions taken.")
        return
    print(f"  Positions decided: {total}  Still active: 0")
    print("  Not yet sold (still active): 0.0%  (a backtest settles every position before the run ends)")


def _print_realized_summary(positions: list, stats: dict) -> None:
    print("\n-- Realized P&L (Closed Positions) --")
    if not positions:
        print("  No positions.")
        return

    print(
        f"  Closed positions: {stats['total_positions']}  wins={stats['wins']}  "
        f"losses={stats['losses']}  breakeven={stats['breakeven']}"
    )
    print(f"  Win rate: {stats['win_rate_pct']:.1f}%")
    print(
        f"  Directional accuracy: {stats['direction_correct_pct']:.1f}% "
        f"({stats['direction_correct']}/{stats['direction_total']})"
    )
    print(f"  Total realized P&L: ${stats['total_pnl']:,.2f}")
    print(f"  Avg P&L: ${stats['avg_pnl']:,.2f}   Median P&L: ${stats['median_pnl']:,.2f}")
    print(f"  Avg win: ${stats['avg_win']:,.2f}   Avg loss: ${stats['avg_loss']:,.2f}")
    profit_factor = stats["profit_factor"]
    print(f"  Profit factor: {profit_factor:.2f}" if profit_factor is not None else "  Profit factor: n/a (no losses)")
    print(f"  Capital deployed: ${stats['capital_deployed']:,.2f}")
    roi_pct = stats["roi_pct"]
    print(f"  Return on capital: {roi_pct:+.1f}%" if roi_pct is not None else "  Return on capital: n/a (no capital deployed)")
    avg_entry = stats["avg_entry_price"]
    if avg_entry is not None:
        print(f"  Avg entry price: {avg_entry:.3f}  (closer to 1.0 = favorites, closer to 0.0 = longshots)")

    by_market: dict = defaultdict(lambda: {"pnl": 0.0, "count": 0})
    for p in positions:
        entry = by_market[p["window_slug"]]
        entry["pnl"] += p["pnl"]
        entry["count"] += 1
    ranked = sorted(by_market.items(), key=lambda kv: kv[1]["pnl"], reverse=True)

    print(f"\n  By market (top {BY_MARKET_HEAD} / bottom {BY_MARKET_TAIL} of {len(ranked)} unique windows):")
    for slug, entry in ranked[:BY_MARKET_HEAD]:
        print(f"    {slug:<40} trades={entry['count']:>3}  pnl=${entry['pnl']:>10,.2f}")
    print("    ...")
    for slug, entry in ranked[-BY_MARKET_TAIL:]:
        print(f"    {slug:<40} trades={entry['count']:>3}  pnl=${entry['pnl']:>10,.2f}")


def _close_method_summary(positions: list[dict]) -> dict:
    """Mirrors performance_review.py's _close_method_summary. Backtest
    positions already carry an exact held_to_expiry bool (see
    backtest/analysis.py::_close_position) rather than the live report's
    proxy (inferring it from whether a SELL trade exists for that token) --
    so this split is exact here, not approximated."""
    groups: dict = {"sold_early": [], "held_to_expiry": []}
    for p in positions:
        key = "held_to_expiry" if p["held_to_expiry"] else "sold_early"
        groups[key].append(p)

    result = {}
    for key, group in groups.items():
        correct = [p for p in group if p["entered_outcome_won"]]
        result[key] = {
            "total": len(group),
            "direction_correct": len(correct),
            "direction_correct_pct": (len(correct) / len(group) * 100.0) if group else None,
        }
    return result


def _print_close_method_summary(summary: dict) -> None:
    print("\n-- Closed Positions by Exit Method --")
    labels = [("sold_early", "Sold early"), ("held_to_expiry", "Held to expiry")]
    if all(summary[key]["total"] == 0 for key, _ in labels):
        print("  No positions.")
        return
    print("  (each row is accuracy within that group alone -- two independent rates, not a 100% split)")
    for key, label in labels:
        row = summary[key]
        if row["total"] == 0:
            print(f"  {label}: 0")
            continue
        pct = row["direction_correct_pct"]
        pct_str = f"{pct:.1f}% ({row['direction_correct']}/{row['total']})" if pct is not None else "n/a"
        print(f"  {label}: {row['total']}  directionally right: {pct_str}")


def _time_bucket_summary(positions: list[dict]) -> list[dict]:
    """Mirrors performance_review.py's _time_bucket_summary, bucketed by
    entry_time_into_window_seconds instead of re-deriving minute-into-window
    from raw trade timestamps -- the backtest trade log already has that
    field per position. pnl_per_sell is simpler here than live's version:
    _build_positions pairs at most one exit per position (no partial-fill
    tracking), so it's just that bucket's sold-early positions' avg pnl,
    not a distinct per-execution figure."""
    buckets: dict = defaultdict(
        lambda: {
            "total": 0,
            "correct": 0,
            "pnls": [],
            "volume": 0.0,
            "entry_prices": [],
            "sold_total": 0,
            "sold_correct": 0,
            "sold_pnls": [],
            "held_total": 0,
            "held_correct": 0,
        }
    )
    for p in positions:
        minute = int(p["entry_time_into_window_seconds"] // 60)
        entry = buckets[minute]
        entry["total"] += 1
        is_correct = p["entered_outcome_won"]
        if is_correct:
            entry["correct"] += 1
        entry["pnls"].append(p["pnl"])
        entry["volume"] += p["entry_price"] * p["size"]
        entry["entry_prices"].append(p["entry_price"])
        if p["held_to_expiry"]:
            entry["held_total"] += 1
            if is_correct:
                entry["held_correct"] += 1
        else:
            entry["sold_total"] += 1
            entry["sold_pnls"].append(p["pnl"])
            if is_correct:
                entry["sold_correct"] += 1

    rows = []
    for m, v in sorted(buckets.items()):
        wins = [p for p in v["pnls"] if p > 0]
        losses = [p for p in v["pnls"] if p < 0]
        rows.append(
            {
                "minute": m,
                "total": v["total"],
                "correct": v["correct"],
                "correct_pct": v["correct"] / v["total"] * 100.0,
                "avg_win": fmean(wins) if wins else None,
                "avg_loss": fmean(losses) if losses else None,
                "avg_pnl": fmean(v["pnls"]) if v["pnls"] else None,
                "volume": v["volume"],
                "avg_entry_price": fmean(v["entry_prices"]) if v["entry_prices"] else None,
                "sold_total": v["sold_total"],
                "sold_correct": v["sold_correct"],
                "sold_correct_pct": (v["sold_correct"] / v["sold_total"] * 100.0) if v["sold_total"] else None,
                "held_total": v["held_total"],
                "held_correct": v["held_correct"],
                "held_correct_pct": (v["held_correct"] / v["held_total"] * 100.0) if v["held_total"] else None,
                "pnl_per_sell": fmean(v["sold_pnls"]) if v["sold_pnls"] else None,
            }
        )
    return rows


def _print_time_bucket_summary(rows: list[dict]) -> None:
    print("\n-- Direction Accuracy by Time-Into-Window (1min buckets) --")
    if not rows:
        print("  No positions.")
        return
    print("  (sold early/held to expiry are each accuracy within that group alone -- not a 100% split)")
    for row in rows:
        m = row["minute"]
        avg_win_str = f"${row['avg_win']:,.2f}" if row["avg_win"] is not None else "n/a"
        avg_loss_str = f"${row['avg_loss']:,.2f}" if row["avg_loss"] is not None else "n/a"
        avg_pnl_str = f"${row['avg_pnl']:,.2f}" if row["avg_pnl"] is not None else "n/a"
        avg_entry_str = f"{row['avg_entry_price']:.3f}" if row["avg_entry_price"] is not None else "n/a"
        sold_pct = row["sold_correct_pct"]
        sold_str = f"{sold_pct:.1f}% ({row['sold_correct']}/{row['sold_total']})" if sold_pct is not None else "n/a"
        held_pct = row["held_correct_pct"]
        held_str = f"{held_pct:.1f}% ({row['held_correct']}/{row['held_total']})" if held_pct is not None else "n/a"
        pnl_per_sell = row["pnl_per_sell"]
        pnl_per_sell_str = f"${pnl_per_sell:,.2f}" if pnl_per_sell is not None else "n/a"
        print(f"  minute {m}-{m + 1}:")
        print(f"    direction accuracy: {row['correct_pct']:.1f}% ({row['correct']}/{row['total']})")
        print(f"    avg win: {avg_win_str}   avg loss: {avg_loss_str}   avg pnl: {avg_pnl_str}")
        print(f"    volume: ${row['volume']:,.2f}   avg entry: {avg_entry_str}")
        print(f"    sold early: {sold_str}   held to expiry: {held_str}")
        print(f"    pnl per sell: {pnl_per_sell_str}")


def _edge_bucket_summary(positions: list[dict], bucket_size: float = EDGE_BUCKET_SIZE) -> list[dict]:
    """Buckets closed positions by entry_edge (modeled probability minus
    entry price -- the strategy's buy-in conviction, and what kelly.py sizes
    positions off of) and reports each bucket's realized avg return. The
    strategy's whole premise is that more edge at buy-in should pay off more
    on average; this checks that against the actual trade log instead of
    assuming it."""
    groups: dict[float, list[dict]] = defaultdict(list)
    for p in positions:
        b = round(p["entry_edge"] / bucket_size) * bucket_size
        groups[b].append(p)
    rows = []
    for b, group in sorted(groups.items()):
        capital = sum(p["entry_price"] * p["size"] for p in group)
        pnl = sum(p["pnl"] for p in group)
        wins = sum(1 for p in group if p["pnl"] > 0)
        rows.append(
            {
                "edge_bucket": round(b, 4),
                "count": len(group),
                "mean_entry_edge": fmean(p["entry_edge"] for p in group),
                "win_rate_pct": wins / len(group) * 100.0,
                "avg_pnl": fmean(p["pnl"] for p in group),
                "roi_pct": (pnl / capital * 100.0) if capital else None,
            }
        )
    return rows


def _print_edge_bucket_summary(rows: list[dict], bucket_size: float = EDGE_BUCKET_SIZE) -> None:
    print("\n-- Return by Buy-In Edge (modeled probability minus entry price) --")
    if not rows:
        print("  No positions.")
        return
    print(f"  (bucketed in {bucket_size:.2f}-wide entry_edge bands -- tests whether more edge at buy-in actually returns more)")
    best = max(rows, key=lambda r: r["avg_pnl"])
    for row in rows:
        roi_str = f"{row['roi_pct']:+.1f}%" if row["roi_pct"] is not None else "n/a"
        lo, hi = row["edge_bucket"], row["edge_bucket"] + bucket_size
        marker = "  <== highest avg pnl" if row is best else ""
        print(
            f"  edge {lo:.2f}-{hi:.2f}: n={row['count']:>5}  win rate={row['win_rate_pct']:>5.1f}%  "
            f"avg pnl=${row['avg_pnl']:>6.2f}  roi={roi_str:>7}{marker}"
        )


def _print_open_positions() -> None:
    print("\n-- Open Positions (Active, Unresolved) --")
    print("  N/A -- a backtest replay settles every position at its window's real resolved outcome before moving on.")


def _print_pending_redemption() -> None:
    print("\n-- Pending On-Chain Redemption --")
    print("  N/A -- backtest positions settle in-memory; there's no on-chain redemption step to be behind on.")


def _print_recent_trades(records: list[dict], limit: int) -> None:
    print(f"\n-- Recent Trades (last {limit} filled orders) --")
    filled = [r for r in records if r["status"] == "FILLED"]
    if not filled:
        print("  No filled orders.")
        return
    ordered = sorted(filled, key=lambda r: (r["window_start"], r["time_into_window_seconds"]))
    for r in ordered[-limit:]:
        size = r["filled_size"] if r["filled_size"] is not None else 0.0
        price = r["filled_price"] if r["filled_price"] is not None else 0.0
        print(
            f"    {r['window_slug']:<30} +{r['time_into_window_seconds']:>5.0f}s  {r['action']:<4} "
            f"outcome={r['outcome']:<5} size={size:>8.2f} @ ${price:.3f}"
        )


def run_review(trade_log_path: str, *, recent_trades: int = 20) -> None:
    records = _load_trade_log(trade_log_path)
    filled = [r for r in records if r["status"] == "FILLED"]
    positions = _build_positions(filled)
    stats = _positions_summary(positions)

    print(f"Backtest Performance Review\nTrade log: {trade_log_path}\n" + "=" * 60)
    _print_trade_status(positions)
    _print_realized_summary(positions, stats)
    _print_open_positions()
    _print_pending_redemption()
    _print_close_method_summary(_close_method_summary(positions))
    _print_time_bucket_summary(_time_bucket_summary(positions))
    _print_edge_bucket_summary(_edge_bucket_summary(positions))
    _print_recent_trades(records, recent_trades)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--trade-log", required=True,
        help="Path to a JSON Lines trade log written by run_backtest.py --trade-log-output",
    )
    parser.add_argument("--recent-trades", type=int, default=20, help="Number of recent filled orders to show")
    args = parser.parse_args()
    run_review(args.trade_log, recent_trades=args.recent_trades)


if __name__ == "__main__":
    main()
