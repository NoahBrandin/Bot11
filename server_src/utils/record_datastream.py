"""Standalone Datastream-Layer recorder: connects every feed (same as
run_datastream.py) and persists each event as JSON Lines, instead of just
printing it and discarding it.

Runs as its own process/systemd unit (see deploy/bot11-recorder.service),
independent of orchestrator.py -- a crash or restart of one doesn't affect
the other, and this keeps raw tick data flowing even while the trading
process is paused/stopped via Telegram. Reads the same DatastreamLayer .env
vars as orchestrator.py::load_config() (BINANCE_SYMBOL, CHAINLINK_SYMBOL,
etc.) so it records exactly what the live strategy sees, without a second
config to keep in sync.

Reuses utils/json_logging.py::JsonLinesFormatter (the project's one JSON
Lines convention) and a RotatingFileHandler, the same pattern
orchestrator.py::main() uses for bot11.jsonl -- just a separate file, with
larger rotation limits, since tick volume is far higher than order/strategy
events. See deploy/bot11-log-upload.service/.timer for shipping finished
(rotated) segments off-box to Google Drive via rclone.

    python record_datastream.py
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
import os
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv

from datastream import (
    BinanceKlineEvent,
    ChainlinkPriceEvent,
    DatastreamLayer,
    PolymarketPriceChangeEvent,
    WindowCloseEvent,
    WindowOpenEvent,
)
from datastream.feeds.binance_feed import DEFAULT_RECONNECT_DELAY as DEFAULT_BINANCE_RECONNECT_DELAY
from datastream.feeds.chainlink_feed import DEFAULT_RECONNECT_DELAY as DEFAULT_CHAINLINK_RECONNECT_DELAY
from datastream.feeds.chainlink_feed import DEFAULT_SYMBOL as DEFAULT_CHAINLINK_SYMBOL
from datastream.feeds.chainlink_feed import DEFAULT_WINDOW_SECONDS as DEFAULT_CHAINLINK_WINDOW_SECONDS
from datastream.feeds.polymarket_feed import DEFAULT_APP_PING_INTERVAL, DEFAULT_POLYMARKET_POLL_INTERVAL
from datastream.feeds.polymarket_feed import DEFAULT_RECONNECT_DELAY as DEFAULT_POLYMARKET_RECONNECT_DELAY
from datastream.utils.gamma_client import DEFAULT_WINDOW_SECONDS
from datastream.utils.window_tracker import DEFAULT_FETCH_RETRY_DELAY, DEFAULT_PREFETCH_LEAD_SECONDS
from utils import env_config
from utils.json_logging import JsonLinesFormatter

# Same binance defaults orchestrator.py uses -- kept in sync manually since
# duplicating orchestrator.Config here for one recorder script would be more
# indirection than the two literals below.
DEFAULT_BINANCE_SYMBOL = "BTCUSDT"
DEFAULT_BINANCE_INTERVAL = "1m"

# Tick volume is far higher than the app log's order/strategy events, so this
# gets a much larger rotation budget: 50MB x 10 backups (~500MB worst case)
# instead of bot11.jsonl's 10MB x 5. deploy/bot11-log-upload.timer ships
# finished segments to Google Drive and deletes them locally, so this cap is
# a safety net rather than the only thing bounding disk usage.
LOG_FILE_MAX_BYTES = 50 * 1024 * 1024
LOG_FILE_BACKUP_COUNT = 10

logger = logging.getLogger("record_datastream")

# Event dataclass -> short snake_case tag, same convention as
# execution/base.py's extra={"event": "order_result", ...}.
_EVENT_TAGS = {
    BinanceKlineEvent: "binance_kline",
    WindowOpenEvent: "window_open",
    WindowCloseEvent: "window_close",
    PolymarketPriceChangeEvent: "polymarket_price_change",
    ChainlinkPriceEvent: "chainlink_price",
}


def _log_event(event) -> None:
    tag = _EVENT_TAGS.get(type(event), type(event).__name__)
    fields = dataclasses.asdict(event)
    # Side/Outcome are (str, Enum) subclasses -- json.dumps serializes them
    # as their plain string value directly, no extra conversion needed.
    logger.info(tag, extra={"event": tag, **fields})


def _configure_logging() -> None:
    formatter = JsonLinesFormatter()
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    handlers: list[logging.Handler] = [stream_handler]

    # Same LOGS_DIRECTORY/LOG_FILE_PATH convention as orchestrator.py::main()
    # -- LOGS_DIRECTORY is set by systemd (see deploy/bot11-recorder.service),
    # falls back to the working directory for local/dev runs.
    default_log_path = os.path.join(os.environ.get("LOGS_DIRECTORY", "."), "datastream.jsonl")
    log_file_path = env_config.env_str("DATASTREAM_LOG_FILE_PATH", default_log_path)
    if log_file_path:
        log_dir = os.path.dirname(log_file_path)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_file_path, maxBytes=LOG_FILE_MAX_BYTES, backupCount=LOG_FILE_BACKUP_COUNT
        )
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    logging.basicConfig(level=logging.INFO, handlers=handlers)


def _build_datastream_layer() -> DatastreamLayer:
    return DatastreamLayer(
        binance_symbol=env_config.env_str("BINANCE_SYMBOL", DEFAULT_BINANCE_SYMBOL),
        binance_interval=env_config.env_str("BINANCE_INTERVAL", DEFAULT_BINANCE_INTERVAL),
        polymarket_poll_interval=env_config.env_float(
            "POLYMARKET_POLL_INTERVAL", DEFAULT_POLYMARKET_POLL_INTERVAL
        ),
        binance_reconnect_delay=env_config.env_float(
            "DATASTREAM_BINANCE_RECONNECT_DELAY_SECONDS", DEFAULT_BINANCE_RECONNECT_DELAY
        ),
        polymarket_reconnect_delay=env_config.env_float(
            "DATASTREAM_POLYMARKET_RECONNECT_DELAY_SECONDS", DEFAULT_POLYMARKET_RECONNECT_DELAY
        ),
        polymarket_app_ping_interval=env_config.env_float(
            "DATASTREAM_POLYMARKET_APP_PING_INTERVAL_SECONDS", DEFAULT_APP_PING_INTERVAL
        ),
        chainlink_symbol=env_config.env_str("CHAINLINK_SYMBOL", DEFAULT_CHAINLINK_SYMBOL),
        chainlink_window_seconds=env_config.env_int(
            "CHAINLINK_WINDOW_SECONDS", DEFAULT_CHAINLINK_WINDOW_SECONDS
        ),
        chainlink_reconnect_delay=env_config.env_float(
            "DATASTREAM_CHAINLINK_RECONNECT_DELAY_SECONDS", DEFAULT_CHAINLINK_RECONNECT_DELAY
        ),
        window_seconds=env_config.env_float("DATASTREAM_WINDOW_SECONDS", DEFAULT_WINDOW_SECONDS),
        prefetch_lead_seconds=env_config.env_float(
            "DATASTREAM_PREFETCH_LEAD_SECONDS", DEFAULT_PREFETCH_LEAD_SECONDS
        ),
        window_fetch_retry_delay=env_config.env_float(
            "DATASTREAM_WINDOW_FETCH_RETRY_DELAY_SECONDS", DEFAULT_FETCH_RETRY_DELAY
        ),
    )


async def run() -> None:
    layer = _build_datastream_layer()
    asyncio.create_task(layer.run())

    logger.info("Recorder started", extra={"event": "recorder_started"})
    while True:
        event = await layer.queue.get()
        _log_event(event)


def main() -> None:
    load_dotenv()
    _configure_logging()
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Recorder stopped by user", extra={"event": "recorder_stopped"})


if __name__ == "__main__":
    main()
