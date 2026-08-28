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
from typing import Deque, List, Optional, Sequence, Tuple


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
    to expose a per-minute sigma, matching probability_model.py's GBMEstimator.sigma
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
        log (server_src/log.jsonl, 194 readings of probability_model.py's actual live mu/
        sigma): median |mu|/sigma was 1.14, with |mu| > sigma in 53.6% of
        readings -- a real drift that large relative to sigma at this
        calibration window would be extraordinary; estimation noise dominating
        the signal is the mundane explanation. Fed into probability_up's
        drift_term vs vol_term, that noise ends up dominating the vol term by
        roughly sigma's own ratio times sqrt(minutes_remaining) -- ~2.5x at a
        5-minute horizon and the observed median ratio."""
        return 0.0


class RecentTickBuffer:
    """Rolling (timestamp, price) buffer of raw ticks, retained for
    max_age_seconds -- feeds settlement_probability_up()'s known/unknown
    window split below. Distinct from probability_model.py's GBMEstimator, which only
    needs the returns between consecutive ticks, not their absolute
    timestamps or a bounded-recency window of the raw values themselves."""

    def __init__(self, max_age_seconds: float) -> None:
        self._max_age_seconds = max_age_seconds
        self._ticks: Deque[Tuple[float, float]] = deque()

    def add(self, timestamp: float, price: float) -> None:
        self._ticks.append((timestamp, price))
        cutoff = timestamp - self._max_age_seconds
        while self._ticks and self._ticks[0][0] < cutoff:
            self._ticks.popleft()

    def average_since(self, since_timestamp: float) -> Optional[float]:
        """Mean price of retained ticks with timestamp >= since_timestamp --
        None if there aren't any (buffer not warmed up, or since_timestamp
        is more recent than every tick seen so far)."""
        values = [p for t, p in self._ticks if t >= since_timestamp]
        return (sum(values) / len(values)) if values else None

    @property
    def latest_price(self) -> Optional[float]:
        return self._ticks[-1][1] if self._ticks else None


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
    (see git log for the run details). settlement_probability_up() below is
    a different, narrower correction for the same underlying fact -- see its
    docstring for why this one wasn't judged to have the same problem."""
    return _probability_above(current_price, reference_price, minutes_remaining, minutes_remaining, mu, sigma)


def _probability_above(
    current_price: float,
    reference_price: float,
    drift_time: float,
    var_time: float,
    mu: float,
    sigma: float,
) -> float:
    """Shared core: P(a lognormal step from current_price, applying drift
    over drift_time and vol over var_time, lands >= reference_price).
    probability_up() is the drift_time == var_time == minutes_remaining
    case (a point value at T); settlement_probability_up() below calls this
    with the two split apart (a path average over T has a smaller effective
    var_time than drift_time, or than a point value's T == T)."""
    if var_time <= 0:
        return 1.0 if current_price >= reference_price else 0.0

    drift_term = math.log(current_price / reference_price) + (mu - 0.5 * sigma**2) * drift_time
    vol_term = sigma * math.sqrt(var_time)

    if vol_term == 0:
        return 1.0 if drift_term >= 0 else 0.0

    return _standard_normal_cdf(drift_term / vol_term)


def _standard_normal_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2)))

def settlement_probability_up(
    live_price: float,
    reference_price: float,
    minutes_remaining: float,
    mu: float,
    sigma: float,
    twap_window_seconds: float,
    known_average: Optional[float] = None,
    known_seconds: float = 0.0,
) -> float:
    """P(settlement TWAP >= reference_price) -- unlike probability_up(),
    accounts for Polymarket settling against a trailing twap_window_seconds
    average, not a point value, by splitting that average into its already-
    realized and still-stochastic parts once minutes_remaining is inside the
    window:

        TWAP = (known_seconds/W)*known_average + (unknown_seconds/W)*A

    where W = twap_window_seconds, unknown_seconds = minutes_remaining*60,
    known_average is the mean of the raw ticks already observed over the
    portion of the trailing window that's already elapsed (from
    RecentTickBuffer -- the raw feed, NOT the Chainlink TWAP stream, which
    is already smoothed and isn't the underlying process being modeled
    here), and A is the still-unknown average of the GBM path over the
    remaining unknown_seconds. Solving TWAP >= reference_price for A reduces
    to probability_up() against an adjusted reference price -- except A is a
    path *average*, not a terminal point value, so its effective drift/vol
    time isn't just unknown_seconds: drift accrues over roughly half that
    remaining time on average and its variance is a third of a point
    value's over the same span (Var[(1/T)int W_s ds] = T/3 for Brownian
    motion) -- the same style of approximation probability_model.py's earlier
    _twap_adjusted_horizon used for its own t<w case (see git history),
    reused here rather than the exact (much messier) Kemna/Vorst
    arithmetic-Asian moment formula.

    Falls back to plain probability_up() (against live_price, the raw tick
    price, not a TWAP snapshot) whenever the whole trailing window is still
    in the future (minutes_remaining*60 >= twap_window_seconds) or no
    known_average is available yet (RecentTickBuffer not warmed up) -- in
    both cases there's no known/unknown split to make yet.

    Unlike the old _twap_adjusted_horizon (a probability_up() docstring note
    says removed for being empirically overconfident), this only ever
    activates in the last twap_window_seconds of a window, and pins its
    known component to observed ticks instead of an analytic approximation
    over the whole remaining time -- untested against live/backtest
    calibration data itself, so treat that history as a reason to check
    calibration again before trusting this live, not as a reason it's
    already fine."""
    unknown_seconds = minutes_remaining * 60.0
    if unknown_seconds >= twap_window_seconds or known_average is None or known_seconds <= 0 or twap_window_seconds <= 0:
        return probability_up(live_price, reference_price, minutes_remaining, mu, sigma)

    known_weight = known_seconds / twap_window_seconds
    unknown_weight = unknown_seconds / twap_window_seconds

    if unknown_weight <= 0:
        settlement = known_weight * known_average
        return 1.0 if settlement >= reference_price else 0.0

    effective_reference = (reference_price - known_weight * known_average) / unknown_weight
    if effective_reference <= 0:
        # known_average alone already clears reference_price -- A (a price
        # average) can't be negative, so settlement is guaranteed >= it.
        return 1.0

    unknown_minutes = unknown_seconds / 60.0
    drift_time = unknown_minutes / 2.0
    var_time = unknown_minutes / 3.0
    return _probability_above(live_price, effective_reference, drift_time, var_time, mu, sigma)
