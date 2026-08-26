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
    # The Chainlink BTC/USD price observed at (or nearest to) window_start --
    # the actual resolution anchor for the window's Up/Down outcome (distinct
    # from strategy/manager.py's own Binance-derived reference_price, which
    # feeds the GBM model rather than settlement). None when no Chainlink
    # tick had arrived yet at open time.
    oracle_price: Optional[float] = None
    oracle_price_timestamp: Optional[float] = None


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
# Polymarket RTDS Chainlink crypto prices
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ChainlinkPriceEvent(Event):
    symbol: str
    value: float
    oracle_timestamp: float
