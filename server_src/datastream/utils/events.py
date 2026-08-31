"""Event dataclasses emitted by the Datastream-Layer.

Every event carries a `timestamp` (unix seconds, when it was received or
derived locally) so downstream layers can reason about latency/ordering
without parsing source-specific message formats. Each source/kind of event
gets its own frozen dataclass so consumers can dispatch on type, e.g.:

    match event:
        case BinanceKlineEvent():
            ...
        case WindowOpenEvent():
            ...
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class Outcome(str, Enum):
    UP = "Up"
    DOWN = "Down"


@dataclass(frozen=True, slots=True)
class Event:
    timestamp: float


# ---------------------------------------------------------------------------
# Binance
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class BinanceKlineEvent(Event):
    symbol: str
    interval: str
    kline_open_time: float
    kline_close_time: float
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_closed: bool


# ---------------------------------------------------------------------------
# Polymarket BTC 5-min window lifecycle
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class WindowOpenEvent(Event):
    slug: str
    condition_id: str
    window_start: float
    window_end: float
    up_token_id: str
    down_token_id: str

@dataclass(frozen=True, slots=True)
class WindowCloseEvent(Event):
    slug: str
    condition_id: str
    window_start: float
    window_end: float


# ---------------------------------------------------------------------------
# Polymarket CLOB market data
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class PolymarketPriceChangeEvent(Event):
    slug: str
    asset_id: str
    outcome: Outcome
    price: float
    size: float
    side: Side
    best_bid: float | None
    best_ask: float | None


# ---------------------------------------------------------------------------
# Chainlink price feed (relayed via Polymarket's RTDS) -- this is the price
# source Polymarket's crypto up/down markets actually resolve against, as
# opposed to BinanceKlineEvent above which is only ever a proxy the strategy
# trades against.
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ChainlinkPriceEvent(Event):
    symbol: str
    price: float
    # Time-weighted average window this price was computed over -- 60s for
    # our 5-min BTC up/down markets (confirmed from an actual market's
    # resolution text, see chainlink_feed.py's module docstring).
    window_seconds: int
    source_timestamp: float


@dataclass(frozen=True, slots=True)
class ChainlinkRawPriceEvent(Event):
    """The un-windowed Chainlink tick, not the 60s TWAP ChainlinkPriceEvent
    carries -- see chainlink_raw_feed.py's module docstring for why this
    exists as a separate feed/event rather than reusing ChainlinkPriceEvent
    with window_seconds=0. Not (yet) consumed by StrategyLayer; recorded
    alongside everything else purely to measure real tick density before
    deciding whether it's dense enough to replace BinanceKlineEvent as the
    momentum_mu()/reversion_mu()/GBMEstimator input."""

    symbol: str
    price: float
    source_timestamp: float
