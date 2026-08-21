"""Pure GBM math: parameter estimation from a price history, and the
probability that BTC finishes at/above a reference price by window close.
"""
from __future__ import annotations

import math
from statistics import fmean, stdev
from typing import Sequence


def log_returns(closes: Sequence[float]) -> list[float]:
    return [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]


def estimate_params(closes: Sequence[float]) -> tuple[float, float]:
    """Returns (mu, sigma), the per-minute drift and volatility estimated
    from the log-returns of a series of 1-minute closes."""
    returns = log_returns(closes)
    mu = fmean(returns)
    sigma = stdev(returns) if len(returns) > 1 else 0.0
    return mu, sigma


def probability_up(
    current_price: float,
    reference_price: float,
    minutes_remaining: float,
    mu: float,
    sigma: float,
) -> float:
    """P(S_T >= reference_price) under GBM, T = minutes_remaining, starting
    from current_price with drift/vol estimated per-minute."""
    if minutes_remaining <= 0:
        return 1.0 if current_price >= reference_price else 0.0

    drift_term = math.log(current_price / reference_price) + (mu - 0.5 * sigma**2) * minutes_remaining
    vol_term = sigma * math.sqrt(minutes_remaining)

    if vol_term == 0:
        return 1.0 if drift_term >= 0 else 0.0

    return _standard_normal_cdf(drift_term / vol_term)


def _standard_normal_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2)))
