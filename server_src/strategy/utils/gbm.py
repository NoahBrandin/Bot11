"""GBM volatility estimation from raw, high-frequency BTC price ticks.

other_src/backtest/download.py already pulls genuine 1-second-resolution
Binance klines (`TICK_INTERVAL = "1s"`) -- an actual raw tick series, not a
smoothed average like Chainlink's 60s TWAP. That makes the classic
Zhang/Mykland/Ait-Sahalia two-scale realized variance estimator directly
applicable: compute RV on the full 1s grid ("fast" -- more samples, but
inflated by any microstructure noise) and again on several coarser,
non-overlapping subsamples of the *same* series ("slow" -- fewer samples,
less inflated), then combine to cancel the noise term.

This supersedes an earlier draft of this file that tried to derive sigma by
comparing 1-min Binance closes against the Chainlink TWAP and correcting for
the TWAP's smoothing bias -- unnecessary now that a real sub-minute raw tick
series is available directly, and empirically found to be a poor estimator
anyway (the short/fast window needed for quick reaction had almost no
effective independent samples once TWAP-smoothed data was the only fast
source, due to the 59/60 sample overlap in a 60s moving average).

The Chainlink TWAP remains relevant elsewhere (current_price/target_price,
trailing-window Monte Carlo payoff modeling) -- just not for sigma estimation
anymore.

Also home to probability_up(), the actual P(S_T >= reference_price) GBM
formula -- unrelated to the estimator above other than consuming its mu/
sigma; kept in this module so StrategyLayer only needs one import.
"""
from __future__ import annotations

import math
from collections import deque
from typing import Deque, List, Optional, Sequence


class RealizedVarianceEstimator:
    """Rolling realized variance: sum of squared log-returns over the last
    `history_size` returns, O(1) per update. No demeaning -- appropriate at
    short/intraday horizons where mu ~= 0 dominates any sample-mean estimate
    anyway.
    """

    def __init__(self, history_size: int, data: Optional[Sequence[float]] = None) -> None:
        self._history_size = history_size
        self._prices: Deque[float] = deque(maxlen=history_size + 1)
        self._sq_returns: Deque[float] = deque(maxlen=history_size)
        self.rv = 0.0
        for v in data or ():
            self.add_value(v)

    def add_value(self, value: float) -> None:
        if self._prices:
            r = math.log(value / self._prices[-1])
            r_sq = r * r
            if len(self._sq_returns) == self._sq_returns.maxlen:
                self.rv -= self._sq_returns[0]
            self._sq_returns.append(r_sq)
            self.rv += r_sq
        self._prices.append(value)

    @property
    def ready(self) -> bool:
        return len(self._sq_returns) >= self._history_size

    @property
    def n_returns(self) -> int:
        return len(self._sq_returns)

    @property
    def variance_per_return(self) -> float:
        """Mean squared log-return -- realized variance per sample, not yet
        normalized to any particular unit of time."""
        n = len(self._sq_returns)
        return self.rv / n if n else 0.0


class TwoScaleRealizedVariance:
    """Zhang/Mykland/Ait-Sahalia (2005) subsampled two-scale RV, computed
    entirely from one raw tick series.

    Splits the incoming ticks into `subsample_k` interleaved subgrids (tick
    0, k, 2k, ... goes to subgrid 0; tick 1, k+1, 2k+1, ... to subgrid 1;
    etc). Each subgrid's own realized variance is a valid, less-noise-biased
    RV estimate at a coarser sampling interval; averaging across all K of
    them uses every tick exactly once and cuts the slow estimator's own
    sampling error roughly by sqrt(K). The full-resolution ("fast") RV is
    biased upward by roughly `n * noise_variance`; each subgrid's RV is
    biased upward by roughly `(n/K) * noise_variance` -- the same noise
    variance, scaled by how many returns went into it. Subtracting a
    (n_bar/n)-weighted share of the fast RV from the averaged slow RV cancels
    that shared noise term, leaving (an estimate of) the true variance.

    tick_interval_seconds is only used by GBMEstimator to convert the
    resulting raw sum-of-squares into a per-minute variance -- this class
    itself is unit-agnostic.
    """

    def __init__(self, history_size: int, subsample_k: int = 5) -> None:
        if subsample_k < 2:
            raise ValueError("subsample_k must be >= 2 -- need at least one slow subgrid to compare against")
        self.fast = RealizedVarianceEstimator(history_size)
        self._subsample_k = subsample_k
        # ceil, not floor: with e.g. history_size=100, subsample_k=2, each
        # subgrid only ever receives 50 prices (49 returns) once `fast` first
        # hits history_size, so a floor'd sub-window of 50 returns would
        # never actually become ready -- readiness would silently lag behind
        # what history_size implies by another subsample_k-ish samples.
        slow_history_size = max(math.ceil(history_size / subsample_k), 1)
        self._slow: List[RealizedVarianceEstimator] = [
            RealizedVarianceEstimator(slow_history_size) for _ in range(subsample_k)
        ]
        self._n_seen = 0

    def add_value(self, price: float) -> None:
        self.fast.add_value(price)
        self._slow[self._n_seen % self._subsample_k].add_value(price)
        self._n_seen += 1

    @property
    def ready(self) -> bool:
        return self.fast.ready and all(e.ready for e in self._slow)

    @property
    def noise_corrected_rv(self) -> float:
        """Raw sum-of-squares TSRV, in the same units as .rv on the
        component estimators -- i.e. not yet normalized per unit time."""
        if not self.ready:
            return 0.0
        n = self.fast.n_returns
        n_bar = sum(e.n_returns for e in self._slow) / self._subsample_k
        avg_slow_rv = sum(e.rv for e in self._slow) / self._subsample_k
        tsrv = avg_slow_rv - (n_bar / n) * self.fast.rv
        small_sample_adjustment = 1.0 / (1.0 - n_bar / n)
        return max(tsrv * small_sample_adjustment, 0.0)


class GBMEstimator:
    """Wraps TwoScaleRealizedVariance with the time-unit bookkeeping needed
    to expose a per-minute sigma, matching gbm.py's GBMEstimator.sigma
    convention.

    `history_size` is in ticks (at `tick_interval_seconds` spacing), not
    minutes -- e.g. history_size=1200, tick_interval_seconds=1.0 covers a
    20-minute rolling window of 1s Binance ticks.

    `target_slow_spacing_seconds` replaces a raw `subsample_k` -- picking K
    directly is unintuitive because the right K depends on how fast the
    specific bias mechanism relaxes, not on any property of this class.
    Validated empirically against 5 days of real Binance 1s klines
    (other_src/data/backtest_data_5d_ticks.json): naive RV is ~21% low at 1s
    resolution and only converges to the true (1-min-kline-matching) value
    once the comparison spacing reaches ~60-180s -- the old subsample_k=5
    default (5s slow spacing) barely corrected anything. Default here is
    120s, the middle of that plateau.
    """

    def __init__(
        self,
        history_size: int,
        tick_interval_seconds: float = 1.0,
        target_slow_spacing_seconds: float = 120.0,
    ) -> None:
        subsample_k = max(round(target_slow_spacing_seconds / tick_interval_seconds), 2)
        self._tsrv = TwoScaleRealizedVariance(history_size, subsample_k)
        self._tick_interval_seconds = tick_interval_seconds
        self.subsample_k = subsample_k

    def add_price(self, price: float) -> None:
        """Feed one price sample, spaced `tick_interval_seconds` apart --
        StrategyLayer feeds 1-min Binance kline closes (tick_interval_seconds
        =60.0); other_src/backtest/download.py's raw 1s ticks also work
        (tick_interval_seconds=1.0), see the module docstring for why that
        finer grain needs the two-scale correction and 1-min closes don't."""
        self._tsrv.add_value(price)

    def seed(self, prices: Sequence[float]) -> None:
        for price in prices:
            self.add_price(price)

    @property
    def ready(self) -> bool:
        return self._tsrv.ready

    @property
    def sigma(self) -> float:
        """Per-minute sigma, bias-corrected for microstructure noise."""
        if not self.ready:
            return 0.0
        n = self._tsrv.fast.n_returns
        total_minutes = n * self._tick_interval_seconds / 60.0
        variance_per_minute = self._tsrv.noise_corrected_rv / total_minutes
        return math.sqrt(max(variance_per_minute, 0.0))

    @property
    def mu(self) -> float:
        """Fixed at 0 (martingale assumption) -- not estimated from price
        history at all, deliberately.

        Var(mu_hat) = sigma^2 / T depends only on the calibration window's
        calendar length T, not on tick count -- unlike sigma, more/faster
        ticks buy nothing here, so there's no version of this estimator that
        high-frequency data would fix. Checked against today's live paper-run
        log (server_src/log.jsonl, 194 readings of gbm.py's actual live mu/
        sigma): median |mu|/sigma was 1.14, with |mu| > sigma in 53.6% of
        readings -- a real drift that large relative to sigma at this
        calibration window would be extraordinary; estimation noise dominating
        the signal is the mundane explanation. Fed into probability_up's
        drift_term vs vol_term, that noise ends up dominating the vol term by
        roughly sigma's own ratio times sqrt(minutes_remaining) -- ~2.5x at a
        5-minute horizon and the observed median ratio."""
        return 0.0


def probability_up(
    current_price: float,
    reference_price: float,
    minutes_remaining: float,
    mu: float,
    sigma: float,
) -> float:
    """P(S_T >= reference_price) under GBM, T = minutes_remaining, starting
    from current_price with drift/vol estimated per-minute.

    Note: this treats both current_price and reference_price as point
    values, even though Polymarket actually settles against a trailing
    Chainlink TWAP -- an earlier version corrected for that (see git
    history, _twap_adjusted_horizon), removed after live paper-run
    calibration data showed it made the model more overconfident, not less
    (see git log for the run details)."""
    if minutes_remaining <= 0:
        return 1.0 if current_price >= reference_price else 0.0

    drift_term = math.log(current_price / reference_price) + (mu - 0.5 * sigma**2) * minutes_remaining
    vol_term = sigma * math.sqrt(minutes_remaining)

    if vol_term == 0:
        return 1.0 if drift_term >= 0 else 0.0

    return _standard_normal_cdf(drift_term / vol_term)


def _standard_normal_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2)))
