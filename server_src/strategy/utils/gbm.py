"""Pure GBM math: parameter estimation from a price history, and the
probability that BTC finishes at/above a reference price by window close.
"""
from __future__ import annotations

import math
from collections import deque
from statistics import fmean, stdev
from typing import Optional, Sequence

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
    twap_window_minutes: float = 0.0,
) -> float:
    """P(A_T >= reference_price) under GBM, where A_T is the price at window
    close -- a point value if twap_window_minutes is 0 (the default, and the
    only behavior before Chainlink TWAP resolution), or the trailing
    time-weighted average over the last twap_window_minutes if set.

    mu/sigma are per-minute GBM parameters estimated from Binance's *raw*
    spot returns (see GBMEstimator) -- but Polymarket's actual settlement
    price is a Chainlink TWAP, not a spot price, and averaging mechanically
    reduces variance relative to a terminal point value. Plugging spot sigma
    straight into the point-value formula overstates how much the eventual
    TWAP can move, skewing probabilities toward the extremes. See
    _twap_adjusted_horizon for the correction."""
    if minutes_remaining <= 0:
        return 1.0 if current_price >= reference_price else 0.0

    drift_time, var_time = _twap_adjusted_horizon(minutes_remaining, twap_window_minutes)

    drift_term = math.log(current_price / reference_price) + (mu - 0.5 * sigma**2) * drift_time
    vol_term = sigma * math.sqrt(var_time)

    if vol_term == 0:
        return 1.0 if drift_term >= 0 else 0.0

    return _standard_normal_cdf(drift_term / vol_term)


def _twap_adjusted_horizon(minutes_remaining: float, twap_window_minutes: float) -> tuple[float, float]:
    """Effective (drift_time, variance_time) to plug into the point-value GBM
    formula so it instead prices A_T = (1/w) integral[T-w, T] S_u du -- the
    trailing w-minute time-weighted average ending at T = minutes_remaining
    from now -- instead of the terminal point S_T.

    Writing log S_u = log S_0 + (mu - sigma^2/2)*u + sigma*W_u for standard
    Brownian motion W, the deterministic drift term averages to (mu -
    sigma^2/2) * (1/w) integral[T-w,T] u du = (mu - sigma^2/2) * (T - w/2) --
    exact for T >= w, since that integral doesn't care whether T-w is
    positive.

    The random part is sigma * A where A = (1/w) integral[T-w,T] W_u du.
    For T >= w, decompose W_u = W_{T-w} + (W_u - W_{T-w}); the first term
    is a single Gaussian with Var = T-w, and by independent increments the
    second averages to an independent term with the classic continuous-
    average-of-BM variance w/3 (Var[(1/L) integral[0,L] W_s ds] = L/3). So
    Var[A] = (T-w) + w/3 = T - (2/3)*w exactly.

    For T < w, the window [T-w, T] starts before "now" -- part of it is
    already realized, information this function doesn't have access to
    (that would need a running buffer of the actual historical TWAP path,
    which correlates with current_price's own trailing window too -- not
    implemented). Below is an extrapolation, not a derivation: it matches
    both the T>=w formulas and their limit at exactly T=w (T/2 = T-w/2 and
    T/3 = T-2w/3 when T=w), and correctly goes to zero as T->0 (settlement
    is fully determined the instant the window closes), but doesn't account
    for the correlation with what current_price already partially knows
    about that overlapping historical segment."""
    if twap_window_minutes <= 0:
        return minutes_remaining, minutes_remaining

    t, w = minutes_remaining, twap_window_minutes
    if t >= w:
        return t - w / 2.0, t - (2.0 / 3.0) * w
    return t / 2.0, t / 3.0


def _standard_normal_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2)))
