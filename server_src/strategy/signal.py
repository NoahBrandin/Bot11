"""Signal dataclass: the Strategy-Layer's sole output, handed to the
Execution-Layer's converge().

A Signal states a *desired* target, never a claim about what's actually
held -- Execution alone owns that truth (see execution/base.py). Strategy is
free to re-emit the same Signal on every qualifying tick; converge() is
idempotent, so a Signal that's already been converged to is a cheap no-op.
"""
from __future__ import annotations

from dataclasses import dataclass

from datastream.events import Outcome


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
