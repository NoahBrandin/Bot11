"""Strategy-Layer: decides a target position (as a fraction of bankroll) for
Polymarket's BTC 5-minute UP/DOWN tokens by modeling BTC as a Geometric
Brownian Motion.

Consumes the typed Events produced by the Datastream-Layer and emits Signals
to an injected Execution-Layer via converge() -- Strategy never tracks what's
actually held or how much bankroll is available; that's Execution's sole
responsibility (see execution/base.py). Strategy's only per-outcome memory is
which side of its entry/exit probability band it currently regards itself as
on (`wants_position`) and, while wanting a position, the target fraction and
execution price frozen at the moment it decided to enter. The *target*
fraction (the Kelly-derived conviction) stays frozen for as long as the
position is wanted, so a fluctuating probability estimate can't turn into
continuous resizing -- but the *price* tracks the live ask on every
_evaluate() (as long as it still clears entry-line edge) instead of staying
frozen forever, since Strategy can't tell whether an earlier attempt at the
old price actually filled (that's Execution's truth, not Strategy's -- see
above), and resubmitting a stale price once the market has moved on has no
chance of matching. Converge() is idempotent, so re-emitting the same
Signal on every _evaluate() (rather than only on a state change) is cheap
and safe, and is what lets a too-small or rejected attempt keep retrying
without Strategy needing to know whether it actually filled. _evaluate()
itself only runs once per Binance candle close, not on every Polymarket
price_change tick -- see _on_price_change's comment for why.

Both _current_price (the live price _evaluate() compares against target_price)
and target_price itself (the price a window's outcome is judged against) now
come from Chainlink (_on_chainlink_price), not Binance -- Chainlink's TWAP is
the actual price Polymarket resolves against. Its ticks land on exact whole
seconds, and window_start is always a whole-second (5-minute-aligned)
boundary too, so target_price capture (_try_capture_target_price) waits
specifically for the tick whose second matches window_start rather than
settling for "closest available" -- an exact reference price, not an
approximation. Chainlink streams continuously regardless of window state, so
that exact tick may already have arrived (and be sitting in _current_price)
by the time _on_window_open is processed -- it checks there first, then
_on_chainlink_price checks again on every subsequent tick until the match
shows up. If the whole process restarts mid-window, a freshly (re)connected
ChainlinkFeed has no backlog of the window's true start-of-window tick
either, so skip_trading blocks capture entirely in that case rather than
trading off a mismatched reference. Binance klines are now only used for the
GBM mu/sigma estimator (_on_binance_kline), gated on candle close same as
before.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from datastream.utils.events import (
    BinanceKlineEvent,
    ChainlinkPriceEvent,
    Event,
    Outcome,
    PolymarketPriceChangeEvent,
    WindowCloseEvent,
    WindowOpenEvent,
)
from execution.base import ExecutionLayer
from monitoring import Monitor

from strategy.utils.binance_history import fetch_recent_closes
from strategy.utils.gbm import GBMEstimator, probability_up
from strategy.utils.kelly import DEFAULT_KELLY_MULTIPLIER, kelly_fraction
from .signal import Signal

logger = logging.getLogger(__name__)

DEFAULT_HISTORY_SIZE = 100
# How far past event.window_start this process's _on_window_open can fire
# before treating it as a late/mid-window join rather than a boundary-
# aligned one. In steady state, WindowTracker.run() sleeps precisely until
# window_end and opens the next window within a fraction of a second of its
# real start; anything past a few seconds means this window was already
# running when the process (re)started. A restart also means the freshly
# (re)connected ChainlinkFeed has no backlog of what the price was back at
# the window's real start -- so whatever gets captured (from persisted state
# or the next live tick) really is "price when we joined", not "price when
# the window opened". Trading the GBM model off that mismatched reference
# for the rest of the window risks systematically wrong probabilities. Skip
# trading that window entirely instead.
DEFAULT_LATE_JOIN_THRESHOLD_SECONDS = 5.0
# Half-width of the dead zone around p=0.5: enter an outcome once its
# modeled probability clears 0.5 + margin, exit once it drops to 0.5 -
# margin. This symmetric band -- not a single shared threshold -- is what
# absorbs tick-to-tick probability noise without needing a time-based
# debounce. The right width depends on the GBM estimator's actual
# tick-to-tick noise; this default needs empirical tuning (e.g. a paper-mode
# observation run) before being trusted live.
DEFAULT_PROBABILITY_MARGIN = 0.02
# Switches the GBMEstimator from a flat rolling-window mean/stdev to
# recency-weighted EWMA mode -- see gbm.py. 30s picked from a 5-day backtest
# comparison (2026-08-22, other_src/run_backtest.py --ewma-halflife-seconds):
# 30/60/120s all beat the flat-window baseline on 1-minute data (+12-22% more
# realized P&L, similar ROI%), with 30s the best of the three. None disables
# EWMA entirely, back to the original flat-window estimate_params behavior.
# Expressed in seconds (not samples) so it stays meaningful independent of
# binance_interval/history resolution; converted to samples via
# _interval_seconds(binance_interval) once at construction (see
# StrategyLayer.__init__).
DEFAULT_EWMA_HALFLIFE_SECONDS: Optional[float] = 30.0
# Disabled by default -- see StrategyLayer._state_file_path.
DEFAULT_STATE_FILE_PATH = ""

_INTERVAL_SECONDS = {"1s": 1.0, "1m": 60.0, "3m": 180.0, "5m": 300.0, "15m": 900.0, "30m": 1800.0, "1h": 3600.0}


def _interval_seconds(interval: str) -> float:
    return _INTERVAL_SECONDS[interval]


@dataclass(slots=True)
class _Quote:
    bid: Optional[float] = None
    ask: Optional[float] = None


@dataclass(slots=True)
class _ActiveWindow:
    slug: str
    condition_id: str
    window_start: float
    window_end: float
    up_token_id: str
    down_token_id: str
    target_price: Optional[float] = None
    target_price_timestamp: Optional[float] = None
    # True when this window was already running by the time
    # _on_window_open fired for it (see DEFAULT_LATE_JOIN_THRESHOLD_SECONDS)
    # -- _evaluate() refuses to emit any signal for the window while this
    # is set, so a mid-window join never trades off a mispriced reference.
    skip_trading: bool = False
    quotes: dict = field(default_factory=lambda: {Outcome.UP: _Quote(), Outcome.DOWN: _Quote()})
    # Decision-state only, not a truth claim about fills -- see module
    # docstring. Which side of the entry/exit band each outcome is on, and
    # (while wanted) the target/price frozen at the moment it was decided.
    wants_position: dict = field(default_factory=lambda: {Outcome.UP: False, Outcome.DOWN: False})
    frozen_target_pct: dict = field(default_factory=lambda: {Outcome.UP: 0.0, Outcome.DOWN: 0.0})
    frozen_price: dict = field(default_factory=lambda: {Outcome.UP: 0.0, Outcome.DOWN: 0.0})

    def token_id(self, outcome: Outcome) -> str:
        return self.up_token_id if outcome is Outcome.UP else self.down_token_id


class StrategyLayer:
    def __init__(
        self,
        execution: ExecutionLayer,
        history_size: int = DEFAULT_HISTORY_SIZE,
        binance_symbol: str = "BTCUSDT",
        binance_interval: str = "1m",
        monitor: Optional[Monitor] = None,
        clock: Callable[[], float] = time.time,
        probability_margin: float = DEFAULT_PROBABILITY_MARGIN,
        kelly_multiplier: float = DEFAULT_KELLY_MULTIPLIER,
        state_file_path: str = DEFAULT_STATE_FILE_PATH,
        late_join_threshold: float = DEFAULT_LATE_JOIN_THRESHOLD_SECONDS,
        ewma_halflife_seconds: Optional[float] = DEFAULT_EWMA_HALFLIFE_SECONDS,
    ) -> None:
        self._execution = execution
        self._monitor = monitor or Monitor()
        self._clock = clock
        self._entry_line = 0.5 + probability_margin
        self._exit_line = 0.5 - probability_margin
        self._kelly_multiplier = kelly_multiplier
        self._history_size = history_size
        self._binance_symbol = binance_symbol
        self._binance_interval = binance_interval
        self._ewma_halflife_seconds = ewma_halflife_seconds
        halflife_samples = (
            ewma_halflife_seconds / _interval_seconds(binance_interval)
            if ewma_halflife_seconds is not None
            else None
        )
        self._gbm = GBMEstimator(history_size, halflife_samples)
        # Both sourced from Chainlink now (_on_chainlink_price) -- Binance
        # klines only drive the GBM estimator these days, see module
        # docstring.
        self._current_price: Optional[float] = None
        self._current_price_timestamp: Optional[float] = None
        self._late_join_threshold = late_join_threshold
        self._window: Optional[_ActiveWindow] = None
        # Where the currently-open window's (slug, reference_price) is
        # persisted -- see _load_persisted_reference_price/_persist_
        # reference_price. Empty disables persistence entirely (the
        # backtest engine never passes a path, so it can never read/write
        # real live-run state). A restart mid-window otherwise has no
        # memory of the reference_price WindowOpenEvent carried before it
        # died, and a freshly (re)started datastream layer can't recover it
        # either (see DEFAULT_LATE_JOIN_THRESHOLD_SECONDS) -- so without
        # persistence a restart would silently re-capture a later (and
        # therefore wrong) price from whatever Binance kline arrives next.
        self._state_file_path = Path(state_file_path) if state_file_path else None
        # While paused, _evaluate_outcome still runs every tick (so exits,
        # settlement, and status reporting are all unaffected) -- it just
        # stops opening any *new* position. A position already open keeps
        # being evaluated for its normal exit line, rather than being force-
        # flattened, since an automatic forced exit is a bigger, riskier
        # action than a human on the other end of /pause is likely expecting.
        self._paused = False

    async def run(self, events: asyncio.Queue) -> None:
        closes = await fetch_recent_closes(
            symbol=self._binance_symbol, interval=self._binance_interval, count=self._history_size
        )
        self._gbm.seed(closes)
        logger.info("Bootstrapped %d minutes of BTC price history", len(closes))
        self._monitor.info(f"Bootstrapped {len(closes)} minutes of BTC price history")

        while True:
            event = await events.get()
            try:
                await self.handle_event(event)
            except Exception:
                # A single bad event/signal (e.g. a transient live-execution
                # client error) must not kill this task -- and with it, via
                # orchestrator.run()'s FIRST_COMPLETED wait, the entire
                # process. Log and keep processing subsequent events instead.
                logger.exception("Unhandled error processing event %r, continuing", event)
                self._monitor.error(f"Unhandled error processing event: {event!r}")

    async def handle_event(self, event: Event) -> None:
        match event:
            case WindowOpenEvent():
                await self._on_window_open(event)
            case WindowCloseEvent():
                await self._execution.on_window_close(event.slug)
            case BinanceKlineEvent():
                await self._on_binance_kline(event)
            case ChainlinkPriceEvent():
                await self._on_chainlink_price(event)
            case PolymarketPriceChangeEvent():
                await self._on_price_change(event)

#   --- Event-Handling ---
    async def _on_window_open(self, event: WindowOpenEvent) -> None:
        recovered = False
        persisted = self._load_persisted_target_price(event.slug)
        if persisted is not None:
            target_price, target_price_timestamp = persisted
            recovered = True
        else:
            target_price, target_price_timestamp = None, None

        late_by = self._clock() - event.window_start
        skip_trading = late_by > self._late_join_threshold

        self._window = _ActiveWindow(
            slug=event.slug,
            condition_id=event.condition_id,
            window_start=event.window_start,
            window_end=event.window_end,
            up_token_id=event.up_token_id,
            down_token_id=event.down_token_id,
            target_price=target_price,
            target_price_timestamp=target_price_timestamp,
            skip_trading=skip_trading,
        )
        if recovered:
            logger.info(
                "Window %s target_price recovered from persisted state: %s", event.slug, target_price
            )
            self._monitor.info(f"Window {event.slug} target_price recovered from persisted state: {target_price}")
        elif self._current_price is not None and self._current_price_timestamp is not None:
            # Chainlink has been streaming continuously regardless of window
            # state, so the tick for window_start's own second may already
            # have arrived and be sitting here -- check now rather than only
            # in _on_chainlink_price (_try_capture_target_price no-ops if
            # it's not actually that exact tick).
            self._try_capture_target_price(self._current_price, self._current_price_timestamp)

        if skip_trading:
            logger.warning(
                "Window %s opened %.1fs after its real start -- joined mid-window, skipping trading for it",
                event.slug, late_by,
            )
            self._monitor.info(
                f"Window {event.slug} opened {late_by:.1f}s after its real start -- joined mid-window, "
                "skipping trading for it"
            )
            self._monitor.error(
                f"Window {event.slug} joined mid-window ({late_by:.1f}s after real start) -- skipping trading"
            )
        logger.info(
            "Window opened: %s (target_price=%s, skip_trading=%s)",
            event.slug, self._window.target_price, skip_trading,
        )
        self._monitor.info(
            f"Window opened: {event.slug} (target_price={self._window.target_price}, skip_trading={skip_trading})"
        )

    async def _on_binance_kline(self, event: BinanceKlineEvent) -> None:
        if event.is_closed:
            self._gbm.add_close(event.close)
            await self._evaluate()

    async def _on_chainlink_price(self, event: ChainlinkPriceEvent) -> None:
        self._current_price = event.price
        self._current_price_timestamp = event.source_timestamp
        self._try_capture_target_price(event.price, event.source_timestamp)

    async def _on_price_change(self, event: PolymarketPriceChangeEvent) -> None:
        if self._window is None or event.slug != self._window.slug:
            return
        self._update_quote(event.outcome, event.best_bid, event.best_ask)

#   --- Evaluation ---
    async def _evaluate(self) -> None:
        window = self._window
        if window is None or window.target_price is None or self._current_price is None:
            return
        if window.skip_trading:
            return
        if not self._gbm.ready:
            return

        minutes_remaining = (window.window_end - self._clock()) / 60.0
        if minutes_remaining <= 0:
            return

        mu, sigma = self._gbm.mu, self._gbm.sigma
        p_up = probability_up(self._current_price, window.target_price, minutes_remaining, mu, sigma)
        probabilities = {Outcome.UP: p_up, Outcome.DOWN: 1.0 - p_up}

        for outcome, probability in probabilities.items():
            self._monitor.info(f"Outcome: {outcome}, probability: {probability}, quote: {window.quotes[outcome]}, "
                               f"btc_price:{self._current_price}, target_price:{window.target_price} mu: {mu}, sigma: {sigma}")
            logger.info(f"Outcome: {outcome}, probability: {probability}, quote: {window.quotes[outcome]}, "
                               f"btc_price:{self._current_price}, target_price:{window.target_price} mu: {mu}, sigma: {sigma}")
            await self._evaluate_outcome(window, outcome, probability)

    async def _evaluate_outcome(self, window: _ActiveWindow, outcome: Outcome, probability: float) -> None:
        quote = window.quotes[outcome]

        if not window.wants_position[outcome] and not self._paused and probability > self._entry_line:
            if quote.ask is None:
                return  # can't size an entry without an ask yet, retry next tick
            if probability <= quote.ask:
                return
            if quote.ask < 0.4:
                return

            window.wants_position[outcome] = True
            window.frozen_target_pct[outcome] = self._kelly_multiplier * kelly_fraction(probability, quote.ask)
            window.frozen_price[outcome] = quote.ask
        elif window.wants_position[outcome] and probability <= self._exit_line:
            window.wants_position[outcome] = False
            window.frozen_target_pct[outcome] = 0.0
        elif window.wants_position[outcome] and quote.ask is not None and quote.ask >= 0.4 and probability > quote.ask:
            # Still wanted, not exiting, and the live ask still leaves edge
            # on the table -- track it. Strategy has no visibility into
            # whether the entry attempt at the *old* frozen_price actually
            # filled (Execution alone owns fill truth, see module
            # docstring), so if it didn't, resubmitting that same stale
            # price once the market has moved on has no chance of matching
            # (observed live: a fast-moving window left the frozen price
            # far behind the current ask, and every FAK retry for the rest
            # of the window failed with "no orders found to match").
            # frozen_target_pct -- the Kelly-derived conviction -- stays
            # fixed at what was decided on entry, so tick-to-tick
            # probability noise still can't turn into continuous resizing;
            # only the mechanical execution price tracks the market.
            window.frozen_price[outcome] = quote.ask

        if window.wants_position[outcome]:
            price = window.frozen_price[outcome]
            target_pct = window.frozen_target_pct[outcome]
        else:
            # A reported best_bid of exactly 0 means the bid side is empty
            # (an empty book, not an actual willingness to buy at $0) --
            # Polymarket's feed sends 0 rather than omitting the field, so
            # treat it the same as "no bid" and fall back to the ask.
            price = quote.bid if quote.bid is not None and quote.bid > 0 else quote.ask
            target_pct = 0.0
            if price is None:
                return  # nothing quoted yet, and nothing held without a quote

        signal = Signal(
            timestamp=self._clock(),
            slug=window.slug,
            condition_id=window.condition_id,
            asset_id=window.token_id(outcome),
            outcome=outcome,
            target_pct=target_pct,
            price=price,
            probability=probability,
        )
        await self._execution.converge(signal)

#   --- Commands ---
    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        self._paused = False

    def status_text(self) -> str:
        if self._window is None:
            return f"Strategy: no active window yet (paused={self._paused})"
        w = self._window
        return f"Strategy:\nwindow={w.slug} current_price={self._current_price} paused={self._paused}"

#   --- Help ---
    def seed_history(self, closes: list[float]) -> None:
        """Pre-fills the rolling price history without the live Binance
        bootstrap fetch -- used by the backtest engine, which already has
        historical closes loaded from its downloaded data file."""
        self._gbm.seed(closes)

    def _load_persisted_target_price(self, slug: str) -> Optional[tuple[float, Optional[float]]]:
        """Recovers the reference_price captured for `slug` before a prior
        process restart, so a window that's still open picks up exactly
        where it left off instead of re-capturing a later (and therefore
        wrong) price from whatever Binance kline arrives next."""
        if self._state_file_path is None:
            return None
        try:
            data = json.loads(self._state_file_path.read_text())
        except (OSError, ValueError):
            return None
        if not isinstance(data, dict) or data.get("slug") != slug:
            return None
        price = data.get("reference_price")
        if price is None:
            return None
        return float(price), data.get("reference_price_timestamp")

    def _persist_target_price(
        self, slug: str, reference_price: float, reference_price_timestamp: Optional[float]
    ) -> None:
        if self._state_file_path is None:
            return
        try:
            self._state_file_path.write_text(
                json.dumps(
                    {
                        "slug": slug,
                        "reference_price": reference_price,
                        "reference_price_timestamp": reference_price_timestamp,
                    }
                )
            )
        except OSError:
            logger.warning("Failed to persist reference_price for %s", slug)

    def _try_capture_target_price(self, price: float, timestamp: float) -> None:
        """Captures `price` as the active window's target_price once the
        Chainlink tick for window_start's own second arrives. Chainlink ticks
        land on exact whole seconds and window_start is always a whole-second
        (5-minute-aligned) boundary, so waiting for that exact match gives a
        precise reference price instead of settling for "closest available"
        -- round() on both sides absorbs float noise from window_start's own
        now-%-window_seconds computation (see window_tracker.current_window_
        start), not any imprecision in the Chainlink timestamp itself.
        Called both right at _on_window_open (in case that tick already
        arrived just before window-open was processed) and from every
        _on_chainlink_price tick after that; once target_price is set, every
        later call is a no-op."""
        window = self._window
        if window is None or window.target_price is not None or window.skip_trading:
            return
        if round(timestamp) != round(window.window_start):
            return

        window.target_price = price
        window.target_price_timestamp = timestamp
        self._persist_target_price(window.slug, price, timestamp)
        logger.info("Window %s target_price captured from Chainlink: %s", window.slug, price)
        self._monitor.info(f"Window {window.slug} target_price captured from Chainlink: {price}")

    def _update_quote(self, outcome: Outcome, bid: Optional[float], ask: Optional[float]) -> None:
        quote = self._window.quotes[outcome]
        if bid is not None:
            quote.bid = bid
        if ask is not None:
            quote.ask = ask
