"""Half-Kelly position sizing for a binary (pays $1 or $0) share."""
from __future__ import annotations

# The "half" in half-Kelly: fraction of full-Kelly staked, trading expected
# growth for lower variance. Configurable via STRATEGY_KELLY_MULTIPLIER.
DEFAULT_KELLY_MULTIPLIER = 0.5


def kelly_fraction(probability: float, ask: float) -> float:
    """Full-Kelly fraction of bankroll to stake on a share priced at `ask`
    that pays $1 with probability `probability`, $0 otherwise. 0 when there's
    no edge (probability <= ask) or ask is degenerate."""
    if ask <= 0 or ask >= 1:
        return 0.0
    edge = (probability - ask) / (1.0 - ask)
    return max(0.0, edge)
