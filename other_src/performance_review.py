"""Read-only performance review for the bot's Polymarket wallet: current
portfolio value, realized P&L / win rate from closed positions (plus the
backtest-analysis-style stats below), open positions with unrealized P&L,
recent trade activity, and an equity-curve/cash-vs-positions/PnL-distribution
chart.
Reads the Deposit/Funder wallet address from POLYMARKET_FUNDER_ADRESS
(typo'd, this repo's active var) or POLYMARKET_FUNDER_ADDRESS (correct
spelling) in .env, or --address. No private key needed -- uses
AsyncPublicClient only.

    python performance_review.py [--address 0x...] [--recent-trades N]
        [--chart-output FILE] [--no-chart]
        [--since DATE] [--until DATE] [--days N]

--since/--until accept 'YYYY-MM-DD' or a full ISO datetime (naive values are
treated as UTC); --days N is shorthand for --since being N days ago. They
scope realized P&L, trades, and the chart to positions/trades that closed or
executed in that window -- open positions and current portfolio value are
always "as of now" regardless of the window, since there's no past state to
show for still-open risk.

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
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from statistics import fmean, median
from types import SimpleNamespace
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


def _parse_time_arg(value: str) -> datetime:
    """Accepts 'YYYY-MM-DD' or a full ISO datetime (e.g.
    '2026-08-01T12:00:00'); naive values are treated as UTC since that's
    what every timestamp elsewhere in this report is compared against."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise SystemExit(f"Could not parse time '{value}' -- use YYYY-MM-DD or a full ISO datetime.")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _header(address: str, since: Optional[datetime] = None, until: Optional[datetime] = None) -> str:
    header = f"Polymarket Performance Review\nWallet: {address}"
    if since is not None or until is not None:
        since_str = since.strftime("%Y-%m-%d %H:%M UTC") if since is not None else "the beginning"
        until_str = until.strftime("%Y-%m-%d %H:%M UTC") if until is not None else "now"
        header += f"\nWindow: {since_str} -> {until_str}"
    return header + "\n" + "=" * 60


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


async def _collect_closed_positions(
    client: AsyncPublicClient,
    address: str,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> list:
    try:
        # The closed-positions endpoint caps page_size at 50 (unlike open
        # positions/trades, which accept larger pages) -- iter_items() still
        # walks every page regardless of page size. The API has no
        # start/end filter for this endpoint (unlike list_trades), so when a
        # window is given, page newest-first and stop as soon as we're past
        # `since` -- avoids walking a long-running wallet's entire history
        # just to answer "what closed in this window" (mirrors
        # server_src/execution/live_report.py's _collect_closed_positions_since).
        if since is None and until is None:
            return [p async for p in client.list_closed_positions(user=address, page_size=50).iter_items()]
        out = []
        paginator = client.list_closed_positions(
            user=address, page_size=50, sort_by="TIMESTAMP", sort_direction="DESC"
        )
        async for p in paginator.iter_items():
            if p.timestamp is None:
                continue
            if until is not None and p.timestamp > until:
                continue
            if since is not None and p.timestamp < since:
                break
            out.append(p)
        return out
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

    # "Right direction" is independent of realized_pnl: cur_price on a
    # ClosedPosition keeps tracking that outcome token's price after you
    # exit, so it tells you whether the side you bought would have won had
    # you held to resolution -- even if you sold early for a loss (right
    # direction, bad trade) or took early profit before the market moved
    # against that side (wrong direction, good trade).
    directional = [p for p in closed if p.cur_price is not None]
    right_direction = [p for p in directional if p.cur_price > Decimal("0.5")]

    # Capital-normalized: raw $ P&L says nothing about whether the strategy
    # is actually good without knowing how much was staked to get it.
    # ClosedPosition.total_bought is a SHARE count, not a dollar amount (the
    # API's naming is misleading -- confirmed against raw data: a position
    # bought at avg_price=0.0011 had total_bought=4304.4444, exactly equal to
    # its share size, while the real $ cost was initial_value=$4.9965) --
    # multiply by avg_price to recover actual $ deployed.
    capital_deployed = sum(
        float(p.avg_price or Decimal(0)) * float(p.total_bought or Decimal(0)) for p in closed
    )
    entry_prices = [float(p.avg_price) for p in closed if p.avg_price is not None]

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
        "direction_total": len(directional),
        "direction_correct": len(right_direction),
        "direction_correct_pct": (len(right_direction) / len(directional) * 100.0) if directional else None,
        "capital_deployed": capital_deployed,
        "roi_pct": (sum(pnls) / capital_deployed * 100.0) if capital_deployed else None,
        "avg_entry_price": fmean(entry_prices) if entry_prices else None,
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
    direction_pct = stats["direction_correct_pct"]
    if direction_pct is not None:
        print(
            f"  Directional accuracy: {direction_pct:.1f}% "
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


def _print_trade_status(closed: list, active: list) -> None:
    """'closed' here already includes resolved-but-unredeemed positions
    (see _as_closed) -- they're decided, so they count as closed for every
    stat in this report. 'active' is only what's genuinely still undecided,
    so 'not yet sold' means actual live risk, not settled dust."""
    total = len(closed) + len(active)
    print("\n-- Trade Status --")
    if not total:
        print("  No positions taken yet.")
        return
    not_sold_pct = len(active) / total * 100.0
    print(f"  Positions decided: {len(closed)}  Still active: {len(active)}")
    print(f"  Not yet sold (still active): {not_sold_pct:.1f}%")


def _render_chart(closed: list, active: list, bucket_rows: list[dict], output_path: str) -> Optional[str]:
    """Equity curve / cash-vs-positions / daily PnL / PnL distribution, built
    from closed positions' realized_pnl ordered by ClosedPosition.timestamp
    (the only per-position time signal the Data API exposes), plus two panels
    from _time_bucket_summary's per-minute-into-window rows: direction
    accuracy and net avg P&L. Kept as separate panels rather than one
    dual-axis chart -- accuracy (%) and avg P&L ($) are different scales, and
    overlaying them on two y-axes invites reading a spurious correlation into
    where the lines happen to cross. Returns the written path, or None if
    there's nothing to plot or matplotlib isn't installed."""
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

    # Cash vs. capital-in-positions over time: a position "opens" at its
    # window's start (stake leaves cash, enters positions) and "closes" at
    # its window's resolution (stake leaves positions; cash gets back
    # stake + realized_pnl). A loss removes the stake from positions exactly
    # like a win does -- it just returns less cash than went in -- so
    # positions never just accumulates. Baselined at 0, not an actual bank
    # balance, since the API has no historical cash-balance endpoint; only
    # the split's shape over time is meaningful here. Still-open positions
    # (from 'active') only contribute an entry event, so real live stakes
    # stay reflected in "in positions" through to the right edge of the chart.
    # Stake in $ terms: ClosedPosition has no direct dollar-cost field, so
    # it's avg_price * total_bought (total_bought is a SHARE count, not
    # dollars, despite the name -- see _positions_summary's capital_deployed
    # comment). Position (still-open) does carry a real $ field, initial_value,
    # so that's used directly there instead.
    events: list[tuple[datetime, float, float]] = []  # (time, d_cash, d_positions)
    for p in closed:
        window_start = _parse_window_start(p.slug)
        if window_start is None or p.timestamp is None:
            continue
        stake = float(p.avg_price or Decimal(0)) * float(p.total_bought or Decimal(0))
        pnl = float(p.realized_pnl or Decimal(0))
        entry_time = datetime.fromtimestamp(window_start, tz=timezone.utc)
        events.append((entry_time, -stake, stake))
        events.append((p.timestamp, stake + pnl, -stake))
    for p in active:
        window_start = _parse_window_start(p.slug)
        if window_start is None:
            continue
        stake = float(p.initial_value or Decimal(0))
        entry_time = datetime.fromtimestamp(window_start, tz=timezone.utc)
        events.append((entry_time, -stake, stake))
    events.sort(key=lambda e: e[0])
    event_times = [e[0] for e in events]
    cash_series = []
    positions_series = []
    cash = 0.0
    in_positions = 0.0
    for _, d_cash, d_positions in events:
        cash += d_cash
        in_positions += d_positions
        cash_series.append(cash)
        positions_series.append(in_positions)

    daily: dict = defaultdict(float)
    for t, v in dated:
        daily[t.date()] += v
    days = sorted(daily)
    day_pnls = [daily[d] for d in days]

    fig, axes = plt.subplots(2, 3, figsize=(16, 8))

    ax = axes[0][0]
    ax.plot(times, cumulative, color="tab:blue")
    ax.set_title("Cumulative Realized P&L")
    ax.set_ylabel("$")
    ax.axhline(0, color="gray", linewidth=0.8)
    ax.tick_params(axis="x", rotation=30)

    ax = axes[0][1]
    if events:
        ax.plot(event_times, cash_series, color="tab:orange", label="Cash")
        ax.plot(event_times, positions_series, color="tab:blue", label="In positions")
        ax.axhline(0, color="gray", linewidth=0.8)
        ax.legend(fontsize=8)
    else:
        ax.text(0.5, 0.5, "No parsable\nwindow slugs", ha="center", va="center", transform=ax.transAxes)
    ax.set_title("Cash vs. Capital In Positions")
    ax.set_ylabel("$ (relative to start)")
    ax.tick_params(axis="x", rotation=30)

    ax = axes[0][2]
    colors = ["tab:green" if v >= 0 else "tab:red" for v in day_pnls]
    ax.bar(days, day_pnls, color=colors)
    ax.set_title("Daily Realized P&L")
    ax.set_ylabel("$")
    ax.tick_params(axis="x", rotation=30)

    ax = axes[1][0]
    ax.hist(pnls, bins=min(30, max(5, len(pnls) // 2)), color="tab:purple", alpha=0.8)
    ax.set_title("Per-Position P&L Distribution")
    ax.set_xlabel("$")
    ax.set_ylabel("count")

    ax = axes[1][1]
    if bucket_rows:
        labels = [f"{r['minute']}-{r['minute'] + 1}" for r in bucket_rows]
        accuracy = [r["correct_pct"] for r in bucket_rows]
        colors = ["tab:green" if v >= 50.0 else "tab:red" for v in accuracy]
        ax.bar(labels, accuracy, color=colors)
        ax.axhline(50.0, color="gray", linewidth=0.8, linestyle="--")
        ax.set_ylim(0, 100)
    else:
        ax.text(0.5, 0.5, "No resolved 5-min\nwindow trades", ha="center", va="center", transform=ax.transAxes)
    ax.set_title("Direction Accuracy by Minute-Into-Window")
    ax.set_xlabel("minute")
    ax.set_ylabel("%")

    ax = axes[1][2]
    if bucket_rows:
        labels = [f"{r['minute']}-{r['minute'] + 1}" for r in bucket_rows]
        avg_pnls = [r["avg_pnl"] if r["avg_pnl"] is not None else 0.0 for r in bucket_rows]
        colors = ["tab:green" if v >= 0 else "tab:red" for v in avg_pnls]
        ax.bar(labels, avg_pnls, color=colors)
        ax.axhline(0, color="gray", linewidth=0.8)
    else:
        ax.text(0.5, 0.5, "No resolved 5-min\nwindow trades", ha="center", va="center", transform=ax.transAxes)
    ax.set_title("Avg Realized P&L by Minute-Into-Window")
    ax.set_xlabel("minute")
    ax.set_ylabel("$")

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
    """positions here should already exclude resolved-but-unredeemed
    positions (see _stuck_positions/_as_closed) -- everything printed here
    is genuinely undecided, still-live risk."""
    print("\n-- Open Positions (Active, Unresolved) --")
    if not positions:
        print("  No active positions.")
        return

    total_unrealized = sum((p.cash_pnl or Decimal(0)) for p in positions)
    total_value = sum((p.current_value or Decimal(0)) for p in positions)
    capital_deployed = sum(float(p.initial_value or Decimal(0)) for p in positions)
    print(
        f"  Open positions: {len(positions)}  current value=${float(total_value):,.2f}  "
        f"unrealized pnl=${float(total_unrealized):,.2f}"
    )
    print(f"  Capital deployed: ${capital_deployed:,.2f}")
    if capital_deployed:
        print(f"  Unrealized return on capital: {float(total_unrealized) / capital_deployed * 100.0:+.1f}%")

    marked = [p for p in positions if p.cur_price is not None and p.avg_price is not None]
    currently_winning = [p for p in marked if p.cur_price > p.avg_price]
    if marked:
        print(
            f"  Currently winning (mark-to-market): {len(currently_winning) / len(marked) * 100.0:.1f}% "
            f"({len(currently_winning)}/{len(marked)})"
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


def _stuck_positions(positions: list) -> list:
    """Open positions whose market has actually resolved on-chain
    (redeemable=True) or whose end_date has passed, but which still show up
    as 'open' because they were never sold or redeemed."""
    today = date.today()
    return [p for p in positions if p.redeemable or (p.end_date is not None and p.end_date < today)]


def _as_closed(p) -> SimpleNamespace:
    """Adapts a stuck (resolved-but-unredeemed) Position into the shape
    _positions_summary/_print_realized_summary expect from ClosedPosition,
    so resolved positions count as closed everywhere in this report instead
    of hiding in a separate 'open' bucket. realized_pnl is taken from
    cash_pnl: nothing was actually sold, but the position is fully resolved,
    so cash_pnl already reflects what redeeming it would realize. There's no
    on-chain close event (never sold or redeemed), so timestamp is derived
    from the slug's window start + length instead -- the window resolved at
    that instant regardless of whether redeemPositions() has been called
    since, so this is a real resolution time, not a placeholder."""
    window_start = _parse_window_start(p.slug)
    resolved_at = (
        datetime.fromtimestamp(window_start + _parse_window_length_seconds(p.slug), tz=timezone.utc)
        if window_start is not None
        else None
    )
    return SimpleNamespace(
        condition_id=p.condition_id,
        token_id=p.token_id,
        slug=p.slug,
        title=p.title,
        avg_price=p.avg_price,
        cur_price=p.cur_price,
        total_bought=p.total_bought,
        realized_pnl=p.cash_pnl,
        timestamp=resolved_at,
    )


def _print_pending_redemption(stuck: list) -> None:
    """Stuck positions are already folded into the closed-position stats
    above -- this is just an operational flag that they still need an
    on-chain redeemPositions() call to actually collect any winnings."""
    print("\n-- Pending On-Chain Redemption --")
    if not stuck:
        print("  None -- nothing resolved is sitting unredeemed.")
        return
    won = [p for p in stuck if p.cur_price is not None and p.cur_price > Decimal("0.5")]
    redeemable_value = sum((p.current_value or Decimal(0)) for p in won)
    print(
        f"  {len(stuck)} resolved position(s) need redeemPositions() -- "
        f"{len(won)} won and worth ${float(redeemable_value):,.2f} unclaimed, "
        f"{len(stuck) - len(won)} lost (nothing to claim, just dust)."
    )


async def _collect_all_trades(
    client: AsyncPublicClient,
    address: str,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> list:
    try:
        start = int(since.timestamp()) if since is not None else None
        end = int(until.timestamp()) if until is not None else None
        return [
            t async for t in client.list_trades(user=address, page_size=500, start=start, end=end).iter_items()
        ]
    except Exception as exc:
        print(f"\n-- First-Trade Direction --\n  (failed to fetch trade history: {exc})")
        return []


def _first_trade_direction(trades: list, closed: list) -> Optional[dict]:
    """For each market (window) that has resolved, finds the earliest BUY
    made in that window -- across the wallet's whole trade history, not just
    the --recent-trades slice -- and checks whether that first trade landed
    on the side the window actually resolved to. Joined against 'closed' by
    token_id, since ClosedPosition.cur_price snaps to 0/1 once a window
    resolves, same signal _positions_summary uses for its own direction
    stat -- this just scopes it to the first trade instead of the position
    as a whole."""
    if not trades or not closed:
        return None
    buys = [t for t in trades if t.side == "BUY" and t.timestamp is not None and t.condition_id is not None]
    if not buys:
        return None

    first_by_window: dict = {}
    for t in sorted(buys, key=lambda t: t.timestamp):
        first_by_window.setdefault(t.condition_id, t)

    resolution_by_token = {p.token_id: p.cur_price for p in closed if p.token_id is not None and p.cur_price is not None}

    correct = 0
    total = 0
    for trade in first_by_window.values():
        cur_price = resolution_by_token.get(trade.token_id)
        if cur_price is None:
            continue  # window hasn't resolved yet, or token_id unmatched
        total += 1
        if cur_price > Decimal("0.5"):
            correct += 1

    if not total:
        return None
    return {"total_windows": total, "correct": correct, "pct": correct / total * 100.0}


def _print_first_trade_direction(stats: Optional[dict]) -> None:
    print("\n-- First-Trade Direction (by window) --")
    if stats is None:
        print("  Not enough data (need trade history and at least one resolved window).")
        return
    print(
        f"  First trade called it right: {stats['pct']:.1f}% "
        f"({stats['correct']}/{stats['total_windows']} resolved windows)"
    )


def _close_method_summary(closed: list, trades: list) -> dict:
    """Splits closed positions into 'sold early' (there's a SELL trade for
    that token, so exiting was a deliberate decision before the window
    resolved) vs 'held to expiry' (no SELL -- the position just rode the
    window to settlement), then reports directional accuracy for each.
    Mirrors backtest/analysis.py's held_to_expiry split, and the same
    distinction _positions_summary's docstring already calls out: selling
    early on the right side can still be a loss, and holding the wrong side
    can still turn a profit if you got out in time -- so it's worth seeing
    whether one exit style is systematically better than the other."""
    sold_token_ids = {t.token_id for t in trades if t.side == "SELL" and t.token_id is not None}

    groups: dict = {"sold_early": [], "held_to_expiry": []}
    for p in closed:
        key = "sold_early" if p.token_id in sold_token_ids else "held_to_expiry"
        groups[key].append(p)

    result = {}
    for key, group in groups.items():
        directional = [p for p in group if p.cur_price is not None]
        correct = [p for p in directional if p.cur_price > Decimal("0.5")]
        result[key] = {
            "total": len(group),
            "direction_total": len(directional),
            "direction_correct": len(correct),
            "direction_correct_pct": (len(correct) / len(directional) * 100.0) if directional else None,
        }
    return result


def _print_close_method_summary(summary: dict) -> None:
    print("\n-- Closed Positions by Exit Method --")
    labels = [("sold_early", "Sold early"), ("held_to_expiry", "Held to expiry")]
    if all(summary[key]["total"] == 0 for key, _ in labels):
        print("  No closed positions yet.")
        return
    print("  (each row is accuracy within that group alone -- two independent rates, not a 100% split)")
    for key, label in labels:
        row = summary[key]
        if row["total"] == 0:
            print(f"  {label}: 0")
            continue
        pct = row["direction_correct_pct"]
        pct_str = f"{pct:.1f}% ({row['direction_correct']}/{row['direction_total']})" if pct is not None else "n/a"
        print(f"  {label}: {row['total']}  directionally right: {pct_str}")


def _parse_window_start(slug: Optional[str]) -> Optional[int]:
    """Extracts a window's start unix-timestamp from Polymarket's 5-minute
    crypto up/down slug convention, e.g. 'btc-updown-5m-1787354400' -> window
    starts at unix time 1787354400. Returns None for slugs that don't follow
    that '...-<digits>' pattern (other market types), so callers skip them --
    there's no window-start signal to bucket on otherwise."""
    if not slug:
        return None
    tail = slug.rsplit("-", 1)[-1]
    return int(tail) if tail.isdigit() else None


def _parse_window_length_seconds(slug: Optional[str], default_seconds: float = 300.0) -> float:
    """Best-effort window length from the slug's '...-<N>m-<unix_start>'
    segment (e.g. 'btc-updown-5m-...' -> 300s). Falls back to 5 minutes --
    today's only live market type -- if that segment doesn't parse, since a
    resolution time still needs *some* length to be placed on."""
    if slug:
        parts = slug.rsplit("-", 2)
        if len(parts) == 3 and parts[1].endswith("m") and parts[1][:-1].isdigit():
            return int(parts[1][:-1]) * 60.0
    return default_seconds


def _time_bucket_summary(trades: list, closed: list) -> list[dict]:
    """Buckets BUY trades by how many whole minutes into their window they
    were placed (0 = first minute, 1 = second minute, ...) and reports each
    bucket's direction-correct rate -- the same cur_price > 0.5 'right
    direction' signal _first_trade_direction uses, just split across the
    window's timeline instead of collapsed to one trade per window. Lets you
    see whether entering earlier or later in the window correlates with
    calling the right side. avg_win/avg_loss/avg_pnl pair with that: mean
    realized_pnl of that bucket's profitable/losing/all trades (mirrors
    _positions_summary's avg_win/avg_loss) -- a bucket can call direction
    well but still win small (bought late/expensive), so accuracy and P&L
    need to be read side by side. volume is total $ deployed (size * price)
    in the bucket, and avg_entry_price is the mean price paid -- together
    they show whether the strategy is already sizing down or paying up into
    the low-accuracy late-window buckets. sold_early/held_to_expiry mirror
    _close_method_summary's split (was there a SELL trade for that token, or
    did it just ride the window to settlement) but broken out per bucket, to
    see whether one exit style is systematically better at particular
    minutes rather than just overall. pnl_per_sell divides a bucket's
    sold-early positions' total realized_pnl by their actual count of SELL
    executions (not their count of BUY entries) -- a position can be sold in
    several partial fills, so this is dollars per sell execution, distinct
    from avg_pnl/avg_win which are per position."""
    cur_price_by_token = {p.token_id: p.cur_price for p in closed if p.token_id is not None and p.cur_price is not None}
    pnl_by_token = {p.token_id: float(p.realized_pnl) for p in closed if p.token_id is not None and p.realized_pnl is not None}
    sell_count_by_token: dict = defaultdict(int)
    for t in trades:
        if t.side == "SELL" and t.token_id is not None:
            sell_count_by_token[t.token_id] += 1
    sold_token_ids = set(sell_count_by_token)

    buckets: dict = defaultdict(
        lambda: {
            "total": 0,
            "correct": 0,
            "pnls": [],
            "volume": 0.0,
            "entry_prices": [],
            "sold_total": 0,
            "sold_correct": 0,
            "sold_tokens": set(),
            "held_total": 0,
            "held_correct": 0,
        }
    )
    for t in trades:
        if t.side != "BUY" or t.timestamp is None:
            continue
        window_start = _parse_window_start(t.slug)
        if window_start is None:
            continue
        cur_price = cur_price_by_token.get(t.token_id)
        if cur_price is None:
            continue  # window unresolved, or token_id unmatched
        seconds_in = t.timestamp.timestamp() - window_start
        if seconds_in < 0:
            continue  # clock skew / slug didn't parse to a real window start
        minute = int(seconds_in // 60)
        entry = buckets[minute]
        entry["total"] += 1
        is_correct = cur_price > Decimal("0.5")
        if is_correct:
            entry["correct"] += 1
        pnl = pnl_by_token.get(t.token_id)
        if pnl is not None:
            entry["pnls"].append(pnl)
        if t.size is not None and t.price is not None:
            entry["volume"] += float(t.size) * float(t.price)
        if t.price is not None:
            entry["entry_prices"].append(float(t.price))
        if t.token_id in sold_token_ids:
            entry["sold_total"] += 1
            entry["sold_tokens"].add(t.token_id)
            if is_correct:
                entry["sold_correct"] += 1
        else:
            entry["held_total"] += 1
            if is_correct:
                entry["held_correct"] += 1

    rows = []
    for m, v in sorted(buckets.items()):
        wins = [p for p in v["pnls"] if p > 0]
        losses = [p for p in v["pnls"] if p < 0]
        sold_pnl_total = sum(pnl_by_token[tok] for tok in v["sold_tokens"] if tok in pnl_by_token)
        sold_sell_count = sum(sell_count_by_token[tok] for tok in v["sold_tokens"])
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
                "pnl_per_sell": (sold_pnl_total / sold_sell_count) if sold_sell_count else None,
            }
        )
    return rows


def _print_time_bucket_summary(rows: list[dict]) -> None:
    print("\n-- Direction Accuracy by Time-Into-Window (1min buckets) --")
    if not rows:
        print("  Not enough data (need resolved trades on 5-minute crypto up/down markets).")
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


def _print_recent_trades(all_trades: list, limit: int) -> None:
    """Takes its slice from the already-fetched, already-window-filtered
    `all_trades` (see _collect_all_trades) rather than a separate
    unfiltered API call, so a --since/--until window scopes this list too
    instead of always showing whatever's most recent overall. list_trades
    returns newest-first by default (unaffected by the start/end filters),
    so the first `limit` items are the most recent within the window."""
    print(f"\n-- Recent Trades (up to {limit}) --")
    if not all_trades:
        print("  No trades yet.")
        return
    for t in all_trades[:limit]:
        ts = t.timestamp.strftime("%Y-%m-%d %H:%M") if t.timestamp else "?"
        title = (t.title or t.condition_id or "unknown")[:35]
        side = t.side or "?"
        size = float(t.size) if t.size is not None else 0.0
        price = float(t.price) if t.price is not None else 0.0
        print(f"    {ts}  {side:<4} {title:<35} outcome={t.outcome or '?':<5} size={size:>8.2f} @ ${price:.3f}")


async def run_review(
    address: str,
    *,
    recent_trades: int = 20,
    chart_output: Optional[str] = DEFAULT_CHART_OUTPUT,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> None:
    async with AsyncPublicClient() as client:
        print(_header(address, since, until))
        await _print_portfolio_value(client, address)
        closed_raw = await _collect_closed_positions(client, address, since, until)
        positions = await _collect_open_positions(client, address)

        stuck = _stuck_positions(positions)
        stuck_ids = {id(p) for p in stuck}
        active = [p for p in positions if id(p) not in stuck_ids]
        stuck_closed = [_as_closed(p) for p in stuck]
        # Stuck (resolved-but-unredeemed) positions have no server-side
        # timestamp filter to apply, since they're derived from currently
        # -open positions -- filter their derived resolution time locally
        # against the same window.
        if since is not None or until is not None:
            stuck_closed = [
                p for p in stuck_closed
                if p.timestamp is not None
                and (since is None or p.timestamp >= since)
                and (until is None or p.timestamp <= until)
            ]
        closed = closed_raw + stuck_closed

        stats = _positions_summary(closed)
        _print_trade_status(closed, active)
        _print_realized_summary(closed, stats)
        _print_open_positions(active)
        _print_pending_redemption(stuck)

        all_trades = await _collect_all_trades(client, address, since, until)
        _print_close_method_summary(_close_method_summary(closed, all_trades))
        _print_first_trade_direction(_first_trade_direction(all_trades, closed))
        bucket_rows = _time_bucket_summary(all_trades, closed)
        _print_time_bucket_summary(bucket_rows)

        _print_recent_trades(all_trades, recent_trades)

        if chart_output is not None:
            written = _render_chart(closed, active, bucket_rows, chart_output)
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
    parser.add_argument(
        "--since", default=None, type=_parse_time_arg,
        help="Only include positions/trades from this time on (YYYY-MM-DD or ISO datetime, UTC if no tz given)",
    )
    parser.add_argument(
        "--until", default=None, type=_parse_time_arg,
        help="Only include positions/trades up to this time (YYYY-MM-DD or ISO datetime, UTC if no tz given)",
    )
    parser.add_argument(
        "--days", default=None, type=float,
        help="Shorthand for --since being N days ago (overridden by an explicit --since)",
    )
    args = parser.parse_args()

    address = resolve_address(args.address)
    chart_output = None if args.no_chart else args.chart_output
    since = args.since
    if since is None and args.days is not None:
        since = datetime.now(timezone.utc) - timedelta(days=args.days)
    asyncio.run(
        run_review(
            address,
            recent_trades=args.recent_trades,
            chart_output=chart_output,
            since=since,
            until=args.until,
        )
    )


if __name__ == "__main__":
    main()
