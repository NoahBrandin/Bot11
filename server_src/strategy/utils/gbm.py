"""Pure GBM math: parameter estimation from a price history, and the
probability that BTC finishes at/above a reference price by window close.
"""
from __future__ import annotations

import math
from collections import deque
from statistics import fmean, stdev
from typing import Optional, Sequence


def log_returns(closes: Sequence[float]) -> list[float]:
    return [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]


class GBMEstimator:
    """Incrementally maintains (mu, sigma) from a stream of price samples,
    equivalent to calling estimate_params/estimate_params_ewma on the whole
    window every time a new sample arrives but without redoing the O(window)
    log-returns/summation work on each call -- see StrategyLayer._evaluate,
    the only caller.

    `halflife_samples=None` selects flat mode: a plain rolling-window
    mean/sample-stdev over the last `history_size - 1` log-returns, kept
    exactly in sync with estimate_params via running sum/sum-of-squares
    (periodically resynced from the retained window to bound floating-point
    drift over long runs).

    `halflife_samples` set selects EWMA mode: a standard streaming
    exponential recursion (mu += decay*(r-mu), matching pandas'
    .ewm(adjust=False)). Unlike estimate_params_ewma, this has no hard
    window cutoff -- old samples decay smoothly forever rather than
    disappearing at history_size. Renormalizing exact windowed EWMA weights
    after evicting the oldest sample would require revisiting every
    remaining sample, which isn't O(1), so this is an intentional, accepted
    difference from estimate_params_ewma (negligible whenever
    halflife_samples << history_size, the intended use)."""

    def __init__(self, history_size: int, halflife_samples: Optional[float] = None) -> None:
        self._history_size = history_size
        self._halflife_samples = halflife_samples
        self._last_close: Optional[float] = None
        self._n_closes = 0

        if halflife_samples is None:
            self._returns: deque = deque(maxlen=max(history_size - 1, 1))
            self._sum = 0.0
            self._sum_sq = 0.0
            self._n_returns_seen = 0
        else:
            self._decay = 1.0 - math.exp(-math.log(2.0) / halflife_samples)
            self._mu = 0.0
            self._var = 0.0
            self._n_returns_seen = 0

    def seed(self, closes: Sequence[float]) -> None:
        for close in closes:
            self.add_close(close)

    def add_close(self, close: float) -> None:
        self._n_closes += 1
        if self._last_close is not None:
            self._add_return(math.log(close / self._last_close))
        self._last_close = close

    def _add_return(self, r: float) -> None:
        if self._halflife_samples is None:
            if len(self._returns) == self._returns.maxlen:
                outgoing = self._returns[0]
                self._sum -= outgoing
                self._sum_sq -= outgoing * outgoing
            self._returns.append(r)
            self._sum += r
            self._sum_sq += r * r
            self._n_returns_seen += 1
            if self._n_returns_seen % self._history_size == 0:
                self._resync()
        elif self._n_returns_seen == 0:
            self._mu = r
            self._n_returns_seen += 1
        else:
            delta = r - self._mu
            self._mu += self._decay * delta
            self._var = (1.0 - self._decay) * (self._var + self._decay * delta * delta)
            self._n_returns_seen += 1

    def _resync(self) -> None:
        self._sum = sum(self._returns)
        self._sum_sq = sum(x * x for x in self._returns)

    @property
    def ready(self) -> bool:
        return self._n_closes >= self._history_size

    @property
    def mu(self) -> float:
        if self._halflife_samples is None:
            n = len(self._returns)
            return self._sum / n if n > 0 else 0.0
        return self._mu

    @property
    def sigma(self) -> float:
        if self._halflife_samples is None:
            n = len(self._returns)
            if n <= 1:
                return 0.0
            mean = self._sum / n
            variance = (self._sum_sq - n * mean * mean) / (n - 1)
            return math.sqrt(max(variance, 0.0))
        return math.sqrt(max(self._var, 0.0))


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
