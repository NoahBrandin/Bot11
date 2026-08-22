"""Current-run performance report for live execution's /status command.

Trimmed, run-scoped adaptation of other_src/performance_review.py (which
isn't deployed to the server -- see md/ssh_help.md's deploy step, only
server_src/* is copied). Same stats (portfolio value, realized P&L, open
positions, exit-method breakdown, per-minute-into-window direction accuracy)
computed the same way, minus the recent-trades list and chart (Telegram
message, not a terminal/file), and scoped to `since` (the orchestrator's
start time for this run) instead of full wallet history -- see each
fetch/filter helper below for how that cutoff is applied.
"""
from __future__ import annotations

import os
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from statistics import fmean, median
from types import SimpleNamespace
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from polymarket import AsyncPublicClient


def _format_duration(total_seconds: float) -> str:
    total_minutes = int(total_seconds // 60)
    hours, minutes = divmod(total_minutes, 60)
    days, hours = divmod(hours, 24)
    if days:
        return f"{days}d {hours}h {minutes}m"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def resolve_wallet_address() -> Optional[str]:
    """POLYMARKET_FUNDER_ADRESS is this repo's active (typo'd) var name --
    see execution/live.py's _build_client, which reads the correctly spelled
    POLYMARKET_FUNDER_ADDRESS instead for the signing wallet override. Both
    are accepted here since either may be set depending on deploy vintage."""
    return os.environ.get("POLYMARKET_FUNDER_ADRESS") or os.environ.get("POLYMARKET_FUNDER_ADDRESS")


async def _collect_closed_positions_since(client: "AsyncPublicClient", address: str, since: datetime) -> list:
    """Paginates newest-first (sort_by=TIMESTAMP, DESC) and stops as soon as
    a page's items fall before `since` -- avoids walking a long-running
    wallet's entire history just to answer "what closed this run", unlike
    other_src/performance_review.py's unbounded iter_items()."""
    out = []
    paginator = client.list_closed_positions(
        user=address, page_size=50, sort_by="TIMESTAMP", sort_direction="DESC"
    )
    async for p in paginator.iter_items():
        if p.timestamp is not None and p.timestamp < since:
            break
        out.append(p)
    return out


async def _collect_open_positions(client: "AsyncPublicClient", address: str) -> list:
    return [p async for p in client.list_positions(user=address, page_size=100).iter_items()]


async def _collect_trades_since(client: "AsyncPublicClient", address: str, since: datetime) -> list:
    start = int(since.timestamp())
    return [t async for t in client.list_trades(user=address, page_size=500, start=start).iter_items()]


def _stuck_positions(positions: list) -> list:
    today = date.today()
    return [p for p in positions if p.redeemable or (p.end_date is not None and p.end_date < today)]


def _parse_window_start(slug: Optional[str]) -> Optional[int]:
    if not slug:
        return None
    tail = slug.rsplit("-", 1)[-1]
    return int(tail) if tail.isdigit() else None


def _parse_window_length_seconds(slug: Optional[str], default_seconds: float = 300.0) -> float:
    if slug:
        parts = slug.rsplit("-", 2)
        if len(parts) == 3 and parts[1].endswith("m") and parts[1][:-1].isdigit():
            return int(parts[1][:-1]) * 60.0
    return default_seconds


def _as_closed(p) -> SimpleNamespace:
    """Adapts a resolved-but-unredeemed open Position into ClosedPosition
    shape, same as other_src/performance_review.py's helper of the same
    name -- see there for the realized_pnl/timestamp derivation rationale."""
    window_start = _parse_window_start(p.slug)
    resolved_at = (
        datetime.fromtimestamp(window_start + _parse_window_length_seconds(p.slug), tz=timezone.utc)
        if window_start is not None
        else None
    )
    return SimpleNamespace(
        token_id=p.token_id,
        slug=p.slug,
        title=p.title,
        avg_price=p.avg_price,
        cur_price=p.cur_price,
        total_bought=p.total_bought,
        realized_pnl=p.cash_pnl,
        timestamp=resolved_at,
    )


def _positions_summary(closed: list) -> dict:
    if not closed:
        return {"total_positions": 0}
    pnls = [float(p.realized_pnl or Decimal(0)) for p in closed]
    wins = [v for v in pnls if v > 0]
    losses = [v for v in pnls if v < 0]
    gross_win = sum(wins)
    gross_loss = -sum(losses)

    directional = [p for p in closed if p.cur_price is not None]
    right_direction = [p for p in directional if p.cur_price > Decimal("0.5")]

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


def _close_method_summary(closed: list, trades: list) -> dict:
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


def _first_trade_direction(trades: list, closed: list) -> Optional[dict]:
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
            continue
        total += 1
        if cur_price > Decimal("0.5"):
            correct += 1

    if not total:
        return None
    return {"total_windows": total, "correct": correct, "pct": correct / total * 100.0}


def _time_bucket_summary(trades: list, closed: list) -> list[dict]:
    cur_price_by_token = {p.token_id: p.cur_price for p in closed if p.token_id is not None and p.cur_price is not None}
    pnl_by_token = {p.token_id: float(p.realized_pnl) for p in closed if p.token_id is not None and p.realized_pnl is not None}
    sell_count_by_token: dict = defaultdict(int)
    for t in trades:
        if t.side == "SELL" and t.token_id is not None:
            sell_count_by_token[t.token_id] += 1
    sold_token_ids = set(sell_count_by_token)

    buckets: dict = defaultdict(
        lambda: {
            "total": 0, "correct": 0, "pnls": [], "volume": 0.0, "entry_prices": [],
            "sold_total": 0, "sold_correct": 0, "sold_tokens": set(),
            "held_total": 0, "held_correct": 0,
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
            continue
        seconds_in = t.timestamp.timestamp() - window_start
        if seconds_in < 0:
            continue
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
                "sold_correct_pct": (v["sold_correct"] / v["sold_total"] * 100.0) if v["sold_total"] else None,
                "sold_correct": v["sold_correct"],
                "sold_total": v["sold_total"],
                "held_correct_pct": (v["held_correct"] / v["held_total"] * 100.0) if v["held_total"] else None,
                "held_correct": v["held_correct"],
                "held_total": v["held_total"],
                "pnl_per_sell": (sold_pnl_total / sold_sell_count) if sold_sell_count else None,
            }
        )
    return rows


def _render_portfolio_value(values: list) -> list[str]:
    lines = ["-- Portfolio Value --"]
    if not values:
        lines.append("  No portfolio value data available.")
        return lines
    for v in values:
        amount = float(v.value) if v.value is not None else 0.0
        lines.append(f"  Current value: ${amount:,.2f}")
    return lines


def _render_trade_status(closed: list, active: list) -> list[str]:
    lines = ["-- Trade Status (this run) --"]
    total = len(closed) + len(active)
    if not total:
        lines.append("  No positions decided or open yet this run.")
        return lines
    not_sold_pct = len(active) / total * 100.0
    lines.append(f"  Positions decided: {len(closed)}  Still active: {len(active)}")
    lines.append(f"  Not yet sold (still active): {not_sold_pct:.1f}%")
    return lines


def _render_realized_summary(closed: list, stats: dict) -> list[str]:
    lines = ["-- Realized P&L (Closed Positions, this run) --"]
    if not closed:
        lines.append("  No closed positions yet this run.")
        return lines
    lines.append(
        f"  Closed positions: {stats['total_positions']}  wins={stats['wins']}  "
        f"losses={stats['losses']}  breakeven={stats['breakeven']}"
    )
    lines.append(f"  Win rate: {stats['win_rate_pct']:.1f}%")
    direction_pct = stats["direction_correct_pct"]
    if direction_pct is not None:
        lines.append(
            f"  Directional accuracy: {direction_pct:.1f}% "
            f"({stats['direction_correct']}/{stats['direction_total']})"
        )
    lines.append(f"  Total realized P&L: ${stats['total_pnl']:,.2f}")
    lines.append(f"  Avg P&L: ${stats['avg_pnl']:,.2f}   Median P&L: ${stats['median_pnl']:,.2f}")
    lines.append(f"  Avg win: ${stats['avg_win']:,.2f}   Avg loss: ${stats['avg_loss']:,.2f}")
    profit_factor = stats["profit_factor"]
    lines.append(f"  Profit factor: {profit_factor:.2f}" if profit_factor is not None else "  Profit factor: n/a (no losses)")
    lines.append(f"  Capital deployed: ${stats['capital_deployed']:,.2f}")
    roi_pct = stats["roi_pct"]
    lines.append(f"  Return on capital: {roi_pct:+.1f}%" if roi_pct is not None else "  Return on capital: n/a (no capital deployed)")
    avg_entry = stats["avg_entry_price"]
    if avg_entry is not None:
        lines.append(f"  Avg entry price: {avg_entry:.3f}  (closer to 1.0 = favorites, closer to 0.0 = longshots)")
    return lines


def _render_open_positions(positions: list) -> list[str]:
    lines = ["-- Open Positions (Active, Unresolved) --"]
    if not positions:
        lines.append("  No active positions.")
        return lines

    total_unrealized = sum((p.cash_pnl or Decimal(0)) for p in positions)
    total_value = sum((p.current_value or Decimal(0)) for p in positions)
    capital_deployed = sum(float(p.initial_value or Decimal(0)) for p in positions)
    lines.append(
        f"  Open positions: {len(positions)}  current value=${float(total_value):,.2f}  "
        f"unrealized pnl=${float(total_unrealized):,.2f}"
    )
    lines.append(f"  Capital deployed: ${capital_deployed:,.2f}")
    if capital_deployed:
        lines.append(f"  Unrealized return on capital: {float(total_unrealized) / capital_deployed * 100.0:+.1f}%")

    marked = [p for p in positions if p.cur_price is not None and p.avg_price is not None]
    currently_winning = [p for p in marked if p.cur_price > p.avg_price]
    if marked:
        lines.append(
            f"  Currently winning (mark-to-market): {len(currently_winning) / len(marked) * 100.0:.1f}% "
            f"({len(currently_winning)}/{len(marked)})"
        )
    return lines


def _render_pending_redemption(stuck: list) -> list[str]:
    lines = ["-- Pending On-Chain Redemption --"]
    if not stuck:
        lines.append("  None -- nothing resolved is sitting unredeemed.")
        return lines
    won = [p for p in stuck if p.cur_price is not None and p.cur_price > Decimal("0.5")]
    redeemable_value = sum((p.current_value or Decimal(0)) for p in won)
    lines.append(
        f"  {len(stuck)} resolved position(s) need redeemPositions() -- "
        f"{len(won)} won and worth ${float(redeemable_value):,.2f} unclaimed, "
        f"{len(stuck) - len(won)} lost (nothing to claim, just dust)."
    )
    return lines


def _render_close_method_summary(summary: dict) -> list[str]:
    lines = ["-- Closed Positions by Exit Method (this run) --"]
    labels = [("sold_early", "Sold early"), ("held_to_expiry", "Held to expiry")]
    if all(summary[key]["total"] == 0 for key, _ in labels):
        lines.append("  No closed positions yet this run.")
        return lines
    lines.append("  (each row is accuracy within that group alone -- not a 100% split)")
    for key, label in labels:
        row = summary[key]
        if row["total"] == 0:
            lines.append(f"  {label}: 0")
            continue
        pct = row["direction_correct_pct"]
        pct_str = f"{pct:.1f}% ({row['direction_correct']}/{row['direction_total']})" if pct is not None else "n/a"
        lines.append(f"  {label}: {row['total']}  directionally right: {pct_str}")
    return lines


def _render_first_trade_direction(stats: Optional[dict]) -> list[str]:
    lines = ["-- First-Trade Direction (this run, by window) --"]
    if stats is None:
        lines.append("  Not enough data yet this run.")
        return lines
    lines.append(
        f"  First trade called it right: {stats['pct']:.1f}% "
        f"({stats['correct']}/{stats['total_windows']} resolved windows)"
    )
    return lines


def _render_time_bucket_summary(rows: list[dict]) -> list[str]:
    lines = ["-- Direction Accuracy by Time-Into-Window (1min buckets, this run) --"]
    if not rows:
        lines.append("  Not enough resolved trades yet this run.")
        return lines
    for row in rows:
        m = row["minute"]
        avg_pnl_str = f"${row['avg_pnl']:,.2f}" if row["avg_pnl"] is not None else "n/a"
        lines.append(
            f"  minute {m}-{m + 1}: accuracy {row['correct_pct']:.1f}% ({row['correct']}/{row['total']})  "
            f"avg pnl {avg_pnl_str}  volume ${row['volume']:,.2f}"
        )
    return lines


async def build_status_report(client: "AsyncPublicClient", address: str, since: datetime) -> str:
    """Assembles the current-run performance report shown by Telegram
    /status -- see this module's docstring for how it relates to
    other_src/performance_review.py."""
    now = datetime.now(timezone.utc)
    elapsed = _format_duration((now - since).total_seconds())
    lines = [
        f"Polymarket Performance Review (current run)\nWallet: {address}",
        f"Run started: {since.strftime('%Y-%m-%d %H:%M UTC')} (running {elapsed})",
        "=" * 40,
    ]

    try:
        values = await client.get_portfolio_values(user=address)
    except Exception as exc:
        values = None
        lines.append(f"-- Portfolio Value --\n  (failed to fetch: {exc})")
    if values is not None:
        lines.extend(_render_portfolio_value(values))

    try:
        closed_raw = await _collect_closed_positions_since(client, address, since)
    except Exception as exc:
        closed_raw = []
        lines.append(f"-- Realized P&L (Closed Positions, this run) --\n  (failed to fetch: {exc})")

    try:
        positions = await _collect_open_positions(client, address)
    except Exception as exc:
        positions = []
        lines.append(f"-- Open Positions (Active, Unresolved) --\n  (failed to fetch: {exc})")

    stuck = _stuck_positions(positions)
    stuck_ids = {id(p) for p in stuck}
    active = [p for p in positions if id(p) not in stuck_ids]
    stuck_closed = [_as_closed(p) for p in stuck]
    stuck_closed_since = [p for p in stuck_closed if p.timestamp is not None and p.timestamp >= since]
    closed = closed_raw + stuck_closed_since

    stats = _positions_summary(closed)
    lines.extend(_render_trade_status(closed, active))
    lines.extend(_render_realized_summary(closed, stats))
    lines.extend(_render_open_positions(active))
    lines.extend(_render_pending_redemption(stuck))

    try:
        trades = await _collect_trades_since(client, address, since)
    except Exception as exc:
        trades = []
        lines.append(f"-- Closed Positions by Exit Method (this run) --\n  (failed to fetch trades: {exc})")
    else:
        lines.extend(_render_close_method_summary(_close_method_summary(closed, trades)))
        lines.extend(_render_first_trade_direction(_first_trade_direction(trades, closed)))
        lines.extend(_render_time_bucket_summary(_time_bucket_summary(trades, closed)))

    return "\n".join(lines)
