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
continuous resizing -- but the *price* tracks the live ask every tick (as
long as it still clears entry-line edge) instead of staying frozen forever,
since Strategy can't tell whether an earlier attempt at the old price
actually filled (that's Execution's truth, not Strategy's -- see above), and
resubmitting a stale price once the market has moved on has no chance of
matching. Converge() is idempotent, so re-emitting the same Signal on every
tick (rather than only on a state change) is cheap and safe, and is what
lets a too-small or rejected attempt keep retrying without Strategy needing
to know whether it actually filled.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable, Optional

from datastream.events import (
    BinanceKlineEvent,
    Event,
    Outcome,
    PolymarketBookEvent,
    PolymarketLastTradePriceEvent,
    PolymarketPriceChangeEvent,
    WindowCloseEvent,
    WindowOpenEvent,
)
from execution.base import ExecutionLayer
from monitoring import Monitor, monitor

from .binance_history import fetch_current_price, fetch_recent_closes
from .gbm import estimate_params, probability_up
from .kelly import DEFAULT_KELLY_MULTIPLIER, kelly_fraction
from .orders import Signal

logger = logging.getLogger(__name__)

DEFAULT_HISTORY_SIZE = 100
# How old self._current_price is allowed to be, at the moment a window opens
# and reference_price needs to be captured, before it's treated as stale
# rather than live. The kline websocket normally pushes at ~1/sec (see
# binance_feed.py); a gap past a few seconds means a connection hiccup
# (drop + reconnect, or a stalled event loop) rather than ordinary jitter.
DEFAULT_STALE_PRICE_THRESHOLD_SECONDS = 5.0
# How far past event.window_start this process's _on_window_open can fire
# before treating it as a late/mid-window join rather than a boundary-
# aligned one. In steady state, WindowTracker.run() sleeps precisely until
# window_end and opens the next window within a fraction of a second of its
# real start; anything past a few seconds means this window was already
# running when the process (re)started. There's no way to know what BTC did
# during the elapsed portion, so whatever reference_price gets captured now
# would really be "price when we joined", not "price when the window
# opened" -- trading the GBM model off that mismatched reference for the
# rest of the window risks systematically wrong probabilities. Skip trading
# that window entirely instead.
DEFAULT_LATE_JOIN_THRESHOLD_SECONDS = 5.0
# Half-width of the dead zone around p=0.5: enter an outcome once its
# modeled probability clears 0.5 + margin, exit once it drops to 0.5 -
# margin. This symmetric band -- not a single shared threshold -- is what
# absorbs tick-to-tick probability noise without needing a time-based
# debounce. The right width depends on the GBM estimator's actual
# tick-to-tick noise; this default needs empirical tuning (e.g. a paper-mode
# observation run) before being trusted live.
DEFAULT_PROBABILITY_MARGIN = 0.02


@dataclass(slots=True)
class _Quote:
    bid: Optional[float] = None
    ask: Optional[float] = None


@dataclass(slots=True)
class _ActiveWindow:
    slug: str
    condition_id: str
    window_end: float
    up_token_id: str
    down_token_id: str
    reference_price: Optional[float] = None
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
        state_file_path: str = "",
        stale_price_threshold: float = DEFAULT_STALE_PRICE_THRESHOLD_SECONDS,
        late_join_threshold: float = DEFAULT_LATE_JOIN_THRESHOLD_SECONDS,
        price_fallback: Callable[[str], Awaitable[Optional[float]]] = fetch_current_price,
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
        self._price_history: deque = deque(maxlen=history_size)
        self._current_price: Optional[float] = None
        # Wall-clock (well, self._clock()) time self._current_price was last
        # set -- lets _resolve_live_reference_price tell a live, just-ticked
        # price apart from one that's sitting stale because the kline
        # websocket dropped and hasn't reconnected yet. See
        # DEFAULT_STALE_PRICE_THRESHOLD_SECONDS.
        self._current_price_ts: Optional[float] = None
        self._stale_price_threshold = stale_price_threshold
        self._late_join_threshold = late_join_threshold
        # Independent REST read used only when the live feed can't be
        # trusted at the exact moment a reference_price is needed --
        # injectable so the backtest engine can stub it out (no network
        # calls in an offline, deterministic replay), same pattern as
        # execution.on_window_close there. See _resolve_live_reference_price.
        self._price_fallback = price_fallback
        self._window: Optional[_ActiveWindow] = None
        # Where the currently-open window's (slug, reference_price) is
        # persisted -- see _load_persisted_reference_price/_persist_
        # reference_price. Empty disables persistence entirely (the
        # backtest engine never passes a path, so it can never read/write
        # real live-run state). A restart mid-window otherwise has no
        # memory of the reference_price captured before it died, and would
        # silently re-capture a later (and therefore wrong) one from
        # whatever Binance kline arrives next -- see module history.
        self._state_file_path = Path(state_file_path) if state_file_path else None
        # While paused, _evaluate_outcome still runs every tick (so exits,
        # settlement, and status reporting are all unaffected) -- it just
        # stops opening any *new* position. A position already open keeps
        # being evaluated for its normal exit line, rather than being force-
        # flattened, since an automatic forced exit is a bigger, riskier
        # action than a human on the other end of /pause is likely expecting.
        self._paused = False

    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        self._paused = False

    def seed_history(self, closes: list[float]) -> None:
        """Pre-fills the rolling price history without the live Binance
        bootstrap fetch -- used by the backtest engine, which already has
        historical closes loaded from its downloaded data file."""
        self._price_history.extend(closes)

    def status_text(self) -> str:
        """Human-readable snapshot for on-demand reporting (e.g. Telegram
        /status). Position/bankroll reporting lives on the Execution side
        now -- see execution/base.py::status_text -- since that's the only
        place real holdings are known."""
        if self._window is None:
            return f"Strategy: no active window yet (paused={self._paused})"
        w = self._window
        return f"Strategy:\nwindow={w.slug} current_price={self._current_price} paused={self._paused}"

    async def run(self, events: asyncio.Queue) -> None:
        closes = await fetch_recent_closes(
            symbol=self._binance_symbol, interval=self._binance_interval, count=self._history_size
        )
        self._price_history.extend(closes)
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
                logger.info("Window closed: %s", event.slug)
                await self._execution.on_window_close(event.slug)
            case BinanceKlineEvent():
                await self._on_binance_kline(event)
            case PolymarketBookEvent():
                await self._on_book(event)
            case PolymarketPriceChangeEvent():
                await self._on_price_change(event)
            case PolymarketLastTradePriceEvent():
                pass

    async def _on_window_open(self, event: WindowOpenEvent) -> None:
        reference_price = self._load_persisted_reference_price(event.slug)
        recovered = reference_price is not None
        if not recovered:
            reference_price = await self._resolve_live_reference_price()

        late_by = self._clock() - event.window_start
        skip_trading = late_by > self._late_join_threshold

        self._window = _ActiveWindow(
            slug=event.slug,
            condition_id=event.condition_id,
            window_end=event.window_end,
            up_token_id=event.up_token_id,
            down_token_id=event.down_token_id,
            reference_price=reference_price,
            skip_trading=skip_trading,
        )
        if recovered:
            logger.info(
                "Window %s reference_price recovered from persisted state: %s", event.slug, reference_price
            )
        elif reference_price is not None:
            self._persist_reference_price(event.slug, reference_price)
        if skip_trading:
            logger.warning(
                "Window %s opened %.1fs after its real start -- joined mid-window, skipping trading for it",
                event.slug, late_by,
            )
            self._monitor.error(
                f"Window {event.slug} joined mid-window ({late_by:.1f}s after real start) -- skipping trading"
            )
        logger.info(
            "Window opened: %s (reference_price=%s, skip_trading=%s)",
            event.slug, self._window.reference_price, skip_trading,
        )

    async def _resolve_live_reference_price(self) -> Optional[float]:
        """The Binance kline websocket normally updates _current_price
        about once a second (see binance_feed.py), so a fresh value is
        exactly what a live listener would see at window open. If it's
        missing or older than _stale_price_threshold -- a dropped/
        reconnecting websocket, or a startup race -- that value can't be
        trusted, so fetch one independent REST price instead of silently
        locking in a reference_price that's actually minutes old (the
        original bug: a connection hiccup around window open produced a
        wrong reference_price for the rest of that window, since it's
        frozen once set)."""
        now = self._clock()
        if (
            self._current_price is not None
            and self._current_price_ts is not None
            and (now - self._current_price_ts) <= self._stale_price_threshold
        ):
            return self._current_price

        age = None if self._current_price_ts is None else now - self._current_price_ts
        logger.warning("Binance price stale or missing at window open (age=%s), using REST fallback", age)
        self._monitor.error(f"Binance feed stale at window open (age={age}) -- using REST fallback for reference_price")
        try:
            return await self._price_fallback(self._binance_symbol)
        except Exception:
            logger.exception("REST fallback for reference_price failed; will capture late from next kline tick")
            self._monitor.error("REST fallback for reference_price failed; capturing late from next kline tick")
            return None

    async def _on_binance_kline(self, event: BinanceKlineEvent) -> None:
        self._current_price = event.close
        self._current_price_ts = self._clock()
        if event.is_closed:
            self._price_history.append(event.close)
        if self._window is not None and self._window.reference_price is None:
            self._window.reference_price = event.close
            self._persist_reference_price(self._window.slug, event.close)
            logger.info("Window %s reference_price captured late: %s", self._window.slug, event.close)
        await self._evaluate()

    def _load_persisted_reference_price(self, slug: str) -> Optional[float]:
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
        return float(price) if price is not None else None

    def _persist_reference_price(self, slug: str, reference_price: float) -> None:
        if self._state_file_path is None:
            return
        try:
            self._state_file_path.write_text(json.dumps({"slug": slug, "reference_price": reference_price}))
        except OSError:
            logger.warning("Failed to persist reference_price for %s", slug)

    async def _on_book(self, event: PolymarketBookEvent) -> None:
        if self._window is None or event.slug != self._window.slug:
            return
        best_bid = max((level.price for level in event.bids), default=None)
        best_ask = min((level.price for level in event.asks), default=None)
        self._update_quote(event.outcome, best_bid, best_ask)
        await self._evaluate()

    async def _on_price_change(self, event: PolymarketPriceChangeEvent) -> None:
        if self._window is None or event.slug != self._window.slug:
            return
        self._update_quote(event.outcome, event.best_bid, event.best_ask)
        await self._evaluate()

    def _update_quote(self, outcome: Outcome, bid: Optional[float], ask: Optional[float]) -> None:
        quote = self._window.quotes[outcome]
        if bid is not None:
            quote.bid = bid
        if ask is not None:
            quote.ask = ask

    async def _evaluate(self) -> None:
        window = self._window
        if window is None or window.reference_price is None or self._current_price is None:
            return
        if window.skip_trading:
            return
        if len(self._price_history) < self._history_size:
            return

        minutes_remaining = (window.window_end - self._clock()) / 60.0
        if minutes_remaining <= 0:
            return

        mu, sigma = estimate_params(tuple(self._price_history))
        p_up = probability_up(self._current_price, window.reference_price, minutes_remaining, mu, sigma)
        probabilities = {Outcome.UP: p_up, Outcome.DOWN: 1.0 - p_up}

        # Independent per outcome, in either order: probabilities[UP] +
        # probabilities[DOWN] == 1, so at most one can ever clear
        # entry_line (> 0.5 + margin) at a time -- mutual exclusivity falls
        # out of the arithmetic, no explicit exit-before-entry ordering
        # needed.
        for outcome, probability in probabilities.items():
            logger.info(
                f"Outcome: {outcome}, probability: {probability}, quote: {window.quotes[outcome]}, mu: {mu}, sigma: {sigma}")
            await self._evaluate_outcome(window, outcome, probability)

    async def _evaluate_outcome(self, window: _ActiveWindow, outcome: Outcome, probability: float) -> None:
        quote = window.quotes[outcome]

        if not window.wants_position[outcome] and not self._paused and probability > self._entry_line:
            if quote.ask is None:
                return  # can't size an entry without an ask yet, retry next tick
            if probability <= quote.ask or quote.ask < 0.4:
                # Probability cleared the entry line, but the market's own
                # ask already prices in (or exceeds) that probability -- no
                # edge left to capture (kelly_fraction would size this at
                # 0 anyway). Don't freeze a zero-edge "position": stay flat
                # and keep re-checking every tick, so a later tick where the
                # ask drops back below probability can still enter. Freezing
                # here would otherwise lock in a permanent 0.0 target until
                # the exit line, blind to the edge reappearing.
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
