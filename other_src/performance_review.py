"""Read-only performance review for the bot's Polymarket wallet: current
portfolio value, realized P&L / win rate from closed positions (plus the
backtest-analysis-style stats below), open positions with unrealized P&L,
recent trade activity, and an equity-curve/drawdown/PnL-distribution chart.
Reads the Deposit/Funder wallet address from POLYMARKET_FUNDER_ADRESS
(typo'd, this repo's active var) or POLYMARKET_FUNDER_ADDRESS (correct
spelling) in .env, or --address. No private key needed -- uses
AsyncPublicClient only.

    python performance_review.py [--address 0x...] [--recent-trades N]
        [--chart-output FILE] [--no-chart]

Mirrors other_src/backtest/analysis.py's positions_summary as closely as
the public Data API allows: win rate, avg/median PnL, avg win/loss, profit
factor. What analysis.py has that this can't reproduce is calibration and
entry_edge (probability vs. entry price) -- those need the GBM's modeled
probability at the moment of entry, which the live strategy never persists
anywhere queryable (see manager.py's per-tick logger.info call, which only
lands in free-text log lines, not a durable per-trade record). Wiring that
up is a separate change to the execution layer, not something this
read-only wallet report can backfill from Polymarket's API alone.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
from collections import defaultdict
from decimal import Decimal
from statistics import fmean, median
from typing import Optional

from dotenv import load_dotenv
from polymarket import AsyncPublicClient

logger = logging.getLogger(__name__)

DEFAULT_CHART_OUTPUT = "performance_review_chart.png"


def resolve_address(cli_address: Optional[str]) -> str:
    if cli_address:
        return cli_address
    address = os.environ.get("POLYMARKET_FUNDER_ADRESS") or os.environ.get("POLYMARKET_FUNDER_ADDRESS")
    if not address:
        raise SystemExit(
            "No wallet address found. Set POLYMARKET_FUNDER_ADRESS or "
            "POLYMARKET_FUNDER_ADDRESS in .env, or pass --address 0x..."
        )
    return address


def _header(address: str) -> str:
    return f"Polymarket Performance Review\nWallet: {address}\n" + "=" * 60


async def _print_portfolio_value(client: AsyncPublicClient, address: str) -> None:
    print("\n-- Portfolio Value --")
    try:
        values = await client.get_portfolio_values(user=address)
    except Exception as exc:
        print(f"  (failed to fetch: {exc})")
        return
    if not values:
        print("  No portfolio value data available.")
        return
    for v in values:
        amount = float(v.value) if v.value is not None else 0.0
        print(f"  Current value: ${amount:,.2f}")


async def _collect_closed_positions(client: AsyncPublicClient, address: str) -> list:
    try:
        # The closed-positions endpoint caps page_size at 50 (unlike open
        # positions/trades, which accept larger pages) -- iter_items() still
        # walks every page regardless of page size.
        return [p async for p in client.list_closed_positions(user=address, page_size=50).iter_items()]
    except Exception as exc:
        print(f"\n-- Realized P&L (Closed Positions) --\n  (failed to fetch closed positions: {exc})")
        return []


def _positions_summary(closed: list) -> dict:
    """Mirrors backtest/analysis.py's _positions_summary, computed over
    realized_pnl from closed positions instead of paired trade_records --
    same shape, so the two reports read the same way side by side."""
    if not closed:
        return {"total_positions": 0}
    pnls = [float(p.realized_pnl or Decimal(0)) for p in closed]
    wins = [v for v in pnls if v > 0]
    losses = [v for v in pnls if v < 0]
    gross_win = sum(wins)
    gross_loss = -sum(losses)
    return {
        "total_positions": len(closed),
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": len(closed) - len(wins) - len(losses),
        "win_rate_pct": len(wins) / len(closed) * 100.0,
        "total_pnl": sum(pnls),
        "avg_pnl": fmean(pnls),
        "median_pnl": median(pnls),
        "avg_win": fmean(wins) if wins else 0.0,
        "avg_loss": fmean(losses) if losses else 0.0,
        "profit_factor": (gross_win / gross_loss) if gross_loss else None,
    }


def _print_realized_summary(closed: list, stats: dict) -> None:
    print("\n-- Realized P&L (Closed Positions) --")
    if not closed:
        print("  No closed positions yet.")
        return

    print(
        f"  Closed positions: {stats['total_positions']}  wins={stats['wins']}  "
        f"losses={stats['losses']}  breakeven={stats['breakeven']}"
    )
    print(f"  Win rate: {stats['win_rate_pct']:.1f}%")
    print(f"  Total realized P&L: ${stats['total_pnl']:,.2f}")
    print(f"  Avg P&L: ${stats['avg_pnl']:,.2f}   Median P&L: ${stats['median_pnl']:,.2f}")
    print(f"  Avg win: ${stats['avg_win']:,.2f}   Avg loss: ${stats['avg_loss']:,.2f}")
    profit_factor = stats["profit_factor"]
    print(f"  Profit factor: {profit_factor:.2f}" if profit_factor is not None else "  Profit factor: n/a (no losses)")

    print("\n  By market:")
    by_market: dict = defaultdict(lambda: {"pnl": Decimal(0), "count": 0, "title": ""})
    for p in closed:
        key = p.condition_id or p.slug or "unknown"
        entry = by_market[key]
        entry["pnl"] += p.realized_pnl or Decimal(0)
        entry["count"] += 1
        entry["title"] = p.title or entry["title"] or key
    for key, entry in sorted(by_market.items(), key=lambda kv: kv[1]["pnl"], reverse=True):
        title = (entry["title"] or key)[:40]
        print(f"    {title:<40} trades={entry['count']:>3}  pnl=${float(entry['pnl']):>10,.2f}")


def _render_chart(closed: list, output_path: str) -> Optional[str]:
    """Equity curve / drawdown / daily PnL / PnL distribution, built from
    closed positions' realized_pnl ordered by ClosedPosition.timestamp (the
    only per-position time signal the Data API exposes). Returns the written
    path, or None if there's nothing to plot or matplotlib isn't installed."""
    dated = sorted(
        ((p.timestamp, float(p.realized_pnl or Decimal(0))) for p in closed if p.timestamp is not None),
        key=lambda pair: pair[0],
    )
    if not dated:
        print("\n-- Chart --\n  No timestamped closed positions to plot.")
        return None

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("\n-- Chart --\n  matplotlib not installed (pip install matplotlib) -- skipping chart.")
        return None

    times = [t for t, _ in dated]
    pnls = [v for _, v in dated]
    cumulative = []
    running = 0.0
    for v in pnls:
        running += v
        cumulative.append(running)
    peak = []
    running_peak = float("-inf")
    for v in cumulative:
        running_peak = max(running_peak, v)
        peak.append(running_peak)
    drawdown = [c - p for c, p in zip(cumulative, peak)]

    daily: dict = defaultdict(float)
    for t, v in dated:
        daily[t.date()] += v
    days = sorted(daily)
    day_pnls = [daily[d] for d in days]

    fig, axes = plt.subplots(2, 2, figsize=(12, 8))

    ax = axes[0][0]
    ax.plot(times, cumulative, color="tab:blue")
    ax.set_title("Cumulative Realized P&L")
    ax.set_ylabel("$")
    ax.axhline(0, color="gray", linewidth=0.8)
    ax.tick_params(axis="x", rotation=30)

    ax = axes[0][1]
    ax.fill_between(times, drawdown, 0, color="tab:red", alpha=0.5)
    ax.set_title("Drawdown from Peak")
    ax.set_ylabel("$")
    ax.tick_params(axis="x", rotation=30)

    ax = axes[1][0]
    colors = ["tab:green" if v >= 0 else "tab:red" for v in day_pnls]
    ax.bar(days, day_pnls, color=colors)
    ax.set_title("Daily Realized P&L")
    ax.set_ylabel("$")
    ax.tick_params(axis="x", rotation=30)

    ax = axes[1][1]
    ax.hist(pnls, bins=min(30, max(5, len(pnls) // 2)), color="tab:purple", alpha=0.8)
    ax.set_title("Per-Position P&L Distribution")
    ax.set_xlabel("$")
    ax.set_ylabel("count")

    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    return output_path


async def _collect_open_positions(client: AsyncPublicClient, address: str) -> list:
    try:
        return [p async for p in client.list_positions(user=address, page_size=100).iter_items()]
    except Exception as exc:
        print(f"\n-- Open Positions (Unrealized) --\n  (failed to fetch open positions: {exc})")
        return []


def _print_open_positions(positions: list) -> None:
    print("\n-- Open Positions (Unrealized) --")
    if not positions:
        print("  No open positions.")
        return

    total_unrealized = sum((p.cash_pnl or Decimal(0)) for p in positions)
    total_value = sum((p.current_value or Decimal(0)) for p in positions)
    print(
        f"  Open positions: {len(positions)}  current value=${float(total_value):,.2f}  "
        f"unrealized pnl=${float(total_unrealized):,.2f}"
    )
    for p in sorted(positions, key=lambda p: p.cash_pnl or Decimal(0), reverse=True):
        title = (p.title or p.condition_id or "unknown")[:40]
        pnl = float(p.cash_pnl) if p.cash_pnl is not None else 0.0
        pct = p.percent_pnl if p.percent_pnl is not None else 0.0
        value = float(p.current_value) if p.current_value is not None else 0.0
        print(
            f"    {title:<40} outcome={p.outcome or '?':<5} value=${value:>10,.2f} "
            f"pnl=${pnl:>10,.2f} ({pct:>+6.1f}%)"
        )


async def _print_recent_trades(client: AsyncPublicClient, address: str, limit: int) -> None:
    print(f"\n-- Recent Trades (up to {limit}) --")
    try:
        page = await client.list_trades(user=address, page_size=limit).first_page()
    except Exception as exc:
        print(f"  (failed to fetch trades: {exc})")
        return
    if not page.items:
        print("  No trades yet.")
        return
    for t in page.items:
        ts = t.timestamp.strftime("%Y-%m-%d %H:%M") if t.timestamp else "?"
        title = (t.title or t.condition_id or "unknown")[:35]
        side = t.side or "?"
        size = float(t.size) if t.size is not None else 0.0
        price = float(t.price) if t.price is not None else 0.0
        print(f"    {ts}  {side:<4} {title:<35} outcome={t.outcome or '?':<5} size={size:>8.2f} @ ${price:.3f}")


async def run_review(
    address: str, *, recent_trades: int = 20, chart_output: Optional[str] = DEFAULT_CHART_OUTPUT
) -> None:
    async with AsyncPublicClient() as client:
        print(_header(address))
        await _print_portfolio_value(client, address)
        closed = await _collect_closed_positions(client, address)
        stats = _positions_summary(closed)
        _print_realized_summary(closed, stats)
        positions = await _collect_open_positions(client, address)
        _print_open_positions(positions)
        await _print_recent_trades(client, address, recent_trades)

        if chart_output is not None:
            written = _render_chart(closed, chart_output)
            if written is not None:
                print(f"\n-- Chart --\n  Wrote {written}")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    load_dotenv()

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address", default=None, help="Wallet address to review (overrides .env)")
    parser.add_argument("--recent-trades", type=int, default=20, help="Number of recent trades to show")
    parser.add_argument(
        "--chart-output", default=DEFAULT_CHART_OUTPUT, help="Path to write the performance chart PNG"
    )
    parser.add_argument("--no-chart", action="store_true", help="Skip generating the chart")
    args = parser.parse_args()

    address = resolve_address(args.address)
    chart_output = None if args.no_chart else args.chart_output
    asyncio.run(run_review(address, recent_trades=args.recent_trades, chart_output=chart_output))


if __name__ == "__main__":
    main()
