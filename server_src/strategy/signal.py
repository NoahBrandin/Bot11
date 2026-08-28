"""Signal dataclass: the Strategy-Layer's sole output, handed to the
Execution-Layer's converge().

A Signal states a *desired* target, never a claim about what's actually
held -- Execution alone owns that truth (see execution/base.py). Strategy is
free to re-emit the same Signal on every qualifying tick; converge() is
idempotent, so a Signal that's already been converged to is a cheap no-op.
"""
from __future__ import annotations

from dataclasses import dataclass

from datastream.utils.events import Outcome


@dataclass(frozen=True, slots=True)
class Signal:
    timestamp: float
    slug: str
    condition_id: str
    asset_id: str
    outcome: Outcome
    target_pct: float  # fraction of bankroll wanted in this outcome; 0.0 == flat
    price: float  # ask when entering, bid-or-ask-fallback when exiting
    probability: float  # modeled P(outcome) at decision time -- for logging only
    # Below: the GBM inputs behind `probability`, carried along purely so
    # ExecutionLayer.converge() can log them structured alongside the fill --
    # a durable, queryable per-trade record of what the model believed at
    # decision time, which performance_review.py's docstring notes doesn't
    # otherwise exist anywhere (see execution/base.py's converge()).
    target_price: float
    current_price: float
    twap_window_minutes: float
    # GBM sigma (per-minute log-return stdev) and time-to-expiry at decision
    # time -- not used for probability at all, carried purely so
    # LiveExecutionLayer can size its FAK price-protection band off actual
    # market-movement risk instead of a fixed constant (see execution/live.py's
    # _price_protection_tolerance). minutes_remaining in particular matters
    # because probability_up's sensitivity to a given BTC move grows sharply
    # as time-to-expiry shrinks (vol_term = sigma*sqrt(minutes_remaining)
    # shrinks), so the book can gap further, faster, right before a window
    # closes than mid-window.
    sigma: float
    minutes_remaining: float
