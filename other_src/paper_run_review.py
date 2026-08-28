"""Structured paper/live trade review: parses the JSON-lines app log (see
server_src/utils/json_logging.py) for the `event: "order_result"` /
`event: "settlement"` records emitted by execution/base.py::converge() and
execution/paper.py::_settle_window(), and turns them into a per-position
table with realized P&L and the model's probability at entry.

This is exactly the calibration/entry-edge data other_src/performance_review.py's
docstring says isn't otherwise obtainable -- paper positions never touch
chain, and even live's own wallet API doesn't expose what the model believed
at decision time. Works for either mode's log, since converge() is shared.

    python paper_run_review.py [LOG_FILE] [--chart-output FILE] [--no-chart]

LOG_FILE defaults to bot11.jsonl (orchestrator.py's default log path when run
from the working directory it was started in). P&L includes Polymarket's
documented crypto-category taker fee (see execution/paper.py's
DEFAULT_TAKER_FEE_RATE) applied the same way paper fills are -- exact for
paper-mode data, an approximation for live (whose real fee/fills the wallet
API would need to confirm exactly).
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from statistics import fmean
from typing import Optional

DEFAULT_LOG_FILE = "bot11.jsonl"
DEFAULT_CHART_OUTPUT = "paper_run_chart.png"
TAKER_FEE_RATE = 0.07  # mirrors execution/paper.py's DEFAULT_TAKER_FEE_RATE
CALIBRATION_BUCKETS = [(0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.01)]


@dataclass
class Position:
    slug: str
    outcome: str
    entry_probability: Optional[float] = None
    entry_timestamp: Optional[str] = None
    target_price: Optional[float] = None
    current_price: Optional[float] = None
    twap_window_minutes: Optional[float] = None
    net_cash_flow: float = 0.0  # cumulative buy/sell cash flow so far, fee-inclusive
    net_shares: float = 0.0
    settlement_payout: Optional[float] = None

    def apply_fill(self, action: str, price: float, size: float) -> None:
        fee = TAKER_FEE_RATE * size * price * (1.0 - price)
        if action == "BUY":
            self.net_cash_flow -= price * size + fee
            self.net_shares += size
        else:
            self.net_cash_flow += price * size - fee
            self.net_shares -= size

    @property
    def realized_pnl(self) -> Optional[float]:
        # PaperExecutionLayer only logs a settlement line when a payout is
        # nonzero (see _settle_window's `if up_paid or down_paid:`), so a
        # position fully sold back to flat before window close never gets
        # one -- but there's nothing left to settle either, so net_cash_flow
        # alone is already the final number, not a missing one.
        if abs(self.net_shares) < 1e-6:
            return self.net_cash_flow
        if self.settlement_payout is None:
            return None
        return self.net_cash_flow + self.settlement_payout

    @property
    def is_win(self) -> Optional[bool]:
        pnl = self.realized_pnl
        return None if pnl is None else pnl > 0


def load_positions(log_file: Path) -> dict[tuple[str, str], Position]:
    positions: dict[tuple[str, str], Position] = {}
    bad_lines = 0
    with log_file.open(encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                # Seen in practice: a single log record split across two
                # physical lines, likely two writers (e.g. LOG_FILE_PATH's
                # file handler and a shell `>` redirect of the same stdout)
                # both appending to the same file without coordination. Warn
                # rather than silently drop it -- a dropped fill can quietly
                # misclassify an otherwise-closed position as still open.
                bad_lines += 1
                continue
            event = record.get("event")
            if event == "order_result":
                _apply_order(positions, record)
            elif event == "settlement":
                _apply_settlement(positions, record)
    if bad_lines:
        print(
            f"WARNING: {bad_lines} unparseable log line(s) skipped -- if you're piping the same "
            "process's stdout to a file *and* have LOG_FILE_PATH set, that's likely two writers "
            "racing on one file; use only one.\n"
        )
    return positions


def _apply_order(positions: dict[tuple[str, str], Position], record: dict) -> None:
    if record.get("status") != "FILLED":
        return
    key = (record["slug"], record["outcome"])
    pos = positions.setdefault(key, Position(slug=record["slug"], outcome=record["outcome"]))
    if pos.entry_probability is None and record.get("action") == "BUY":
        pos.entry_probability = record.get("probability")
        pos.entry_timestamp = record.get("ts")
        pos.target_price = record.get("target_price")
        pos.current_price = record.get("current_price")
        pos.twap_window_minutes = record.get("twap_window_minutes")
    price, size = record.get("filled_price"), record.get("filled_size")
    if price is not None and size is not None:
        pos.apply_fill(record["action"], price, size)


def _apply_settlement(positions: dict[tuple[str, str], Position], record: dict) -> None:
    slug = record["slug"]
    for outcome, payout in (("Up", record.get("up_paid")), ("Down", record.get("down_paid"))):
        key = (slug, outcome)
        if key in positions and payout is not None:
            positions[key].settlement_payout = payout


def print_summary(positions: dict[tuple[str, str], Position]) -> list[Position]:
    entered = [p for p in positions.values() if p.entry_probability is not None]
    settled = [p for p in entered if p.realized_pnl is not None]

    print(f"Positions entered: {len(entered)}")
    print(f"Positions settled: {len(settled)} ({len(entered) - len(settled)} still open/unsettled)")

    if not settled:
        return settled

    wins = [p for p in settled if p.is_win]
    pnls = [p.realized_pnl for p in settled]
    print(f"Win rate: {len(wins)}/{len(settled)} ({len(wins) / len(settled) * 100:.1f}%)")
    print(f"Total realized P&L: {sum(pnls):.2f}")
    print(f"Avg P&L per position: {fmean(pnls):.2f}")

    print("\nCalibration (modeled probability at entry vs actual win rate):")
    for lo, hi in CALIBRATION_BUCKETS:
        in_bucket = [p for p in settled if lo <= p.entry_probability < hi]
        if not in_bucket:
            continue
        bucket_wins = sum(1 for p in in_bucket if p.is_win)
        avg_p = fmean(p.entry_probability for p in in_bucket)
        print(
            f"  p in [{lo:.1f},{hi:.1f}): n={len(in_bucket):4d} "
            f"modeled_avg={avg_p:.3f} actual_win_rate={bucket_wins / len(in_bucket):.3f}"
        )
    return settled


def plot_equity_curve(settled: list[Position], output: str) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ordered = sorted(settled, key=lambda p: p.entry_timestamp or "")
    cumulative = []
    total = 0.0
    for p in ordered:
        total += p.realized_pnl
        cumulative.append(total)

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(range(1, len(cumulative) + 1), cumulative, marker="o", markersize=2)
    ax.axhline(0, color="gray", linewidth=0.8)
    ax.set_xlabel("Settled position #")
    ax.set_ylabel("Cumulative realized P&L")
    ax.set_title("Paper run equity curve")
    fig.tight_layout()
    fig.savefig(output)
    print(f"\nEquity curve saved to {output}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("log_file", nargs="?", default=DEFAULT_LOG_FILE)
    parser.add_argument("--chart-output", default=DEFAULT_CHART_OUTPUT)
    parser.add_argument("--no-chart", action="store_true")
    args = parser.parse_args()

    log_path = Path(args.log_file)
    if not log_path.exists():
        raise SystemExit(f"Log file not found: {log_path}")

    positions = load_positions(log_path)
    settled = print_summary(positions)

    if settled and not args.no_chart:
        plot_equity_curve(settled, args.chart_output)


if __name__ == "__main__":
    main()
