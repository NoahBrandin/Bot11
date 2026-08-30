"""Loads raw recorder JSON-Lines (see server_src/utils/record_datastream.py)
back into the exact live Event dataclasses the strategy already knows how to
handle -- no reconstruction/approximation of klines, ticks, chainlink prices
or bid/ask needed, since these *are* the real events, verbatim.

Also resolves each window's outcome directly from the recorded Chainlink
stream, with no network/Gamma access required: chainlink_feed.py's module
docstring confirms Polymarket's 5-min BTC market resolves against the
60-second Chainlink TWAP stream itself, and manager.py's
_try_capture_target_price uses the Chainlink tick whose source_timestamp
lands exactly on a window boundary's second as that boundary's price. This
mirrors that rule: resolved outcome is UP iff the Chainlink tick at
window_end's second >= the tick at window_start's second. Windows missing
either exact boundary tick (feed gaps, reconnects) are left unresolved
rather than guessed at -- same stance other_src/recording_brier_report.py
takes for its (simpler, replay-free) market-price Brier score.
"""
from __future__ import annotations

import dataclasses
import json
import logging
import re
from pathlib import Path
from typing import Optional

from datastream.utils.events import (
    BinanceKlineEvent,
    ChainlinkPriceEvent,
    Event,
    Outcome,
    PolymarketPriceChangeEvent,
    Side,
    WindowCloseEvent,
    WindowOpenEvent,
)

logger = logging.getLogger(__name__)

_EVENT_TYPES: dict[str, type] = {
    "binance_kline": BinanceKlineEvent,
    "window_open": WindowOpenEvent,
    "window_close": WindowCloseEvent,
    "polymarket_price_change": PolymarketPriceChangeEvent,
    "chainlink_price": ChainlinkPriceEvent,
}
_ENUM_FIELDS = {"outcome": Outcome, "side": Side}
# Precomputed once per event class rather than calling dataclasses.fields()
# on every single parsed line -- with ~1M lines in a day-scale recording,
# that call alone (it rebuilds a tuple from each class's __dataclass_fields__
# every time) was ~15% of total load time.
_FIELD_NAMES = {cls: tuple(f.name for f in dataclasses.fields(cls)) for cls in _EVENT_TYPES.values()}

# Tie-break for events sharing an identical recorded timestamp -- mirrors
# the dispatch order a live consumer would see: a window opening logically
# precedes any price tick against its now-existing tokens, which in turn
# precedes the window closing.
_PRIORITY = {
    WindowOpenEvent: 0,
    BinanceKlineEvent: 0,
    ChainlinkPriceEvent: 0,
    PolymarketPriceChangeEvent: 1,
    WindowCloseEvent: 2,
}

_ROTATION_SUFFIX = re.compile(r"\.jsonl\.(\d+)$")


def _rotation_sort_key(path: Path) -> int:
    """Oldest-first ordering for RotatingFileHandler output: datastream.jsonl.N
    is older than .N-1, ..., older than the un-suffixed (current) file."""
    m = _ROTATION_SUFFIX.search(path.name)
    return int(m.group(1)) if m else -1


def discover_log_files(log_dir: Path) -> list[Path]:
    files = sorted(Path(log_dir).glob("datastream.jsonl*"), key=_rotation_sort_key, reverse=True)
    if not files:
        raise SystemExit(f"No datastream.jsonl* files found in {log_dir}")
    return files


def _parse_event(record: dict) -> Optional[Event]:
    cls = _EVENT_TYPES.get(record.get("event"))
    if cls is None:
        return None
    kwargs = {}
    for name in _FIELD_NAMES[cls]:
        if name not in record:
            return None  # partial/malformed line -- skip rather than guess
        value = record[name]
        enum_cls = _ENUM_FIELDS.get(name)
        kwargs[name] = enum_cls(value) if enum_cls and value is not None else value
    return cls(**kwargs)


@dataclasses.dataclass(slots=True)
class Window:
    slug: str
    condition_id: str
    window_start: float
    window_end: float
    up_token_id: str
    down_token_id: str
    closed: bool = False
    # "UP" / "DOWN", None if a boundary Chainlink tick is missing.
    resolved_outcome: Optional[str] = None


@dataclasses.dataclass(slots=True)
class Recording:
    # Every parsed event, chronologically ordered -- exactly what a live
    # DatastreamLayer.queue would have yielded, replayable through
    # StrategyLayer.handle_event() unmodified.
    events: list[Event]
    windows: dict[str, Window]


def load_recording(log_dir: Path) -> Recording:
    events: list[Event] = []
    windows: dict[str, Window] = {}
    chainlink_by_second: dict[int, float] = {}
    bad_lines = 0

    log_files = discover_log_files(log_dir)
    logger.info("Reading %d recorder log file(s) from %s", len(log_files), log_dir)
    for path in log_files:
        with path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    bad_lines += 1
                    continue
                event = _parse_event(record)
                if event is None:
                    continue
                events.append(event)
                if isinstance(event, WindowOpenEvent):
                    windows[event.slug] = Window(
                        slug=event.slug,
                        condition_id=event.condition_id,
                        window_start=event.window_start,
                        window_end=event.window_end,
                        up_token_id=event.up_token_id,
                        down_token_id=event.down_token_id,
                    )
                elif isinstance(event, WindowCloseEvent):
                    w = windows.get(event.slug)
                    if w is not None:
                        w.closed = True
                elif isinstance(event, ChainlinkPriceEvent):
                    # Later ticks for the same second (feed jitter) overwrite
                    # earlier ones -- last-observed value for that second,
                    # same as what a live consumer ends up holding.
                    chainlink_by_second[round(event.source_timestamp)] = event.price

    if bad_lines:
        logger.warning("%d unparseable log line(s) skipped", bad_lines)

    events.sort(key=lambda e: (e.timestamp, _PRIORITY.get(type(e), 0)))

    resolved = 0
    for w in windows.values():
        if not w.closed:
            continue
        target = chainlink_by_second.get(round(w.window_start))
        settlement = chainlink_by_second.get(round(w.window_end))
        if target is None or settlement is None:
            continue
        w.resolved_outcome = "UP" if settlement >= target else "DOWN"
        resolved += 1

    logger.info(
        "Parsed %d events, %d windows (%d resolved, %d unresolved/open)",
        len(events), len(windows), resolved, len(windows) - resolved,
    )
    return Recording(events=events, windows=windows)
