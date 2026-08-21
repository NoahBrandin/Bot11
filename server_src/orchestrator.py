"""Orchestration-File: wires the Datastream-Layer into the Strategy-Layer,
the Execution-Layer, and the Monitor.

The Datastream-Layer's events are handed straight to the Strategy-Layer,
which converges Signals against Execution (paper or live, chosen here) on
every tick -- Execution alone owns bankroll/position truth and decides
whether a real order is needed to close the gap (see execution/base.py).
The same Monitor instance is handed to every layer at construction time, so
which categories of message (Event/Order/Execution/Error/Info) actually
reach Telegram is purely a .env setting -- see monitoring/monitor.py.

This file also registers the two Telegram commands (/status, /stop) --
that's wiring specific to here rather than Monitor itself, since only the
orchestrator has both execution/strategy (for /status) and the running task
list (for /stop). /stop sets an asyncio.Event that's raced against the other
tasks via asyncio.wait(..., FIRST_COMPLETED), so a stop request shuts things
down through the exact same cancel + log_stats() path a crash or Ctrl+C
would.

Every behavioral tunable across all layers is resolved to a single Config
here, once, from .env -- see load_config() -- rather than each layer reading
os.environ itself or using its own hardcoded default in production. Every
env var defaults to exactly the value that was previously hardcoded, so an
unmodified .env reproduces prior behavior exactly.
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from typing import Optional

from dotenv import load_dotenv

import env_config
from datastream import DatastreamLayer
from datastream.binance_feed import DEFAULT_RECONNECT_DELAY as DEFAULT_BINANCE_RECONNECT_DELAY
from datastream.gamma_client import DEFAULT_WINDOW_SECONDS
from datastream.polymarket_feed import DEFAULT_APP_PING_INTERVAL, DEFAULT_POLYMARKET_POLL_INTERVAL
from datastream.polymarket_feed import DEFAULT_RECONNECT_DELAY as DEFAULT_POLYMARKET_RECONNECT_DELAY
from datastream.window_tracker import DEFAULT_FETCH_RETRY_DELAY, DEFAULT_PREFETCH_LEAD_SECONDS
from execution import ExecutionLayer, ExecutionResult, LiveExecutionLayer, PaperExecutionLayer
from execution.base import (
    DEFAULT_MAX_POSITION_NOTIONAL,
    DEFAULT_REJECTION_ALERT_THRESHOLD,
    DEFAULT_RETRY_COOLDOWN_SECONDS,
)
from execution.live import DEFAULT_BANKROLL_CACHE_SECONDS, DEFAULT_PRICE_PROTECTION_TOLERANCE
from execution.paper import (
    DEFAULT_SETTLEMENT_MAX_WAIT_SECONDS,
    DEFAULT_SETTLEMENT_POLL_DELAY_SECONDS,
    DEFAULT_STARTING_BANKROLL,
    DEFAULT_TAKER_FEE_RATE,
)
from json_logging import JsonLinesFormatter
from monitoring import Monitor, MonitorCategory
from strategy import StrategyLayer
from strategy.kelly import DEFAULT_KELLY_MULTIPLIER
from strategy.manager import DEFAULT_HISTORY_SIZE, DEFAULT_PROBABILITY_MARGIN

logger = logging.getLogger("orchestrator")

# Canonical single source for the Binance symbol/interval -- both
# DatastreamLayer (WS) and StrategyLayer (REST bootstrap) receive the exact
# same values from here instead of each hardcoding its own (previously
# mismatched-case) default.
DEFAULT_BINANCE_SYMBOL = "BTCUSDT"
DEFAULT_BINANCE_INTERVAL = "1m"

# Rotating JSON-lines log file, independent of journald's own retention --
# see json_logging.py and main() below. 10MB x 5 backups is a fixed cap
# (~50MB worst case) rather than a time window, so disk usage can't grow
# unbounded on a long-running live deployment.
LOG_FILE_MAX_BYTES = 10 * 1024 * 1024
LOG_FILE_BACKUP_COUNT = 5


@dataclass(slots=True)
class Config:
    execution_mode: str
    binance_symbol: str
    binance_interval: str
    polymarket_poll_interval: float
    binance_reconnect_delay: float
    polymarket_reconnect_delay: float
    polymarket_app_ping_interval: float
    window_seconds: float
    prefetch_lead_seconds: float
    window_fetch_retry_delay: float
    strategy_history_size: int
    strategy_probability_margin: float
    strategy_kelly_multiplier: float
    strategy_state_file_path: str
    execution_retry_cooldown_seconds: float
    execution_rejection_alert_threshold: int
    execution_max_position_notional: Optional[float]
    paper_starting_bankroll: float
    paper_taker_fee_rate: float
    paper_settlement_poll_delay_seconds: float
    paper_settlement_max_wait_seconds: float
    live_bankroll_cache_seconds: float
    live_price_protection_tolerance: float


def load_config() -> Config:
    return Config(
        execution_mode=env_config.env_str("EXECUTION_MODE", "paper").strip().lower() or "paper",
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
        window_seconds=env_config.env_float("DATASTREAM_WINDOW_SECONDS", DEFAULT_WINDOW_SECONDS),
        prefetch_lead_seconds=env_config.env_float(
            "DATASTREAM_PREFETCH_LEAD_SECONDS", DEFAULT_PREFETCH_LEAD_SECONDS
        ),
        window_fetch_retry_delay=env_config.env_float(
            "DATASTREAM_WINDOW_FETCH_RETRY_DELAY_SECONDS", DEFAULT_FETCH_RETRY_DELAY
        ),
        strategy_history_size=env_config.env_int("STRATEGY_HISTORY_SIZE", DEFAULT_HISTORY_SIZE),
        strategy_probability_margin=env_config.env_float(
            "STRATEGY_PROBABILITY_MARGIN", DEFAULT_PROBABILITY_MARGIN
        ),
        strategy_kelly_multiplier=env_config.env_float("STRATEGY_KELLY_MULTIPLIER", DEFAULT_KELLY_MULTIPLIER),
        strategy_state_file_path=env_config.env_str(
            "STRATEGY_STATE_FILE_PATH",
            os.path.join(os.environ.get("LOGS_DIRECTORY", "."), "strategy_state.json"),
        ),
        execution_retry_cooldown_seconds=env_config.env_float(
            "EXECUTION_RETRY_COOLDOWN_SECONDS", DEFAULT_RETRY_COOLDOWN_SECONDS
        ),
        execution_rejection_alert_threshold=env_config.env_int(
            "EXECUTION_REJECTION_ALERT_THRESHOLD", DEFAULT_REJECTION_ALERT_THRESHOLD
        ),
        execution_max_position_notional=env_config.env_optional_float(
            "EXECUTION_MAX_POSITION_NOTIONAL", DEFAULT_MAX_POSITION_NOTIONAL
        ),
        paper_starting_bankroll=env_config.env_float("PAPER_STARTING_BANKROLL", DEFAULT_STARTING_BANKROLL),
        paper_taker_fee_rate=env_config.env_float("PAPER_TAKER_FEE_RATE", DEFAULT_TAKER_FEE_RATE),
        paper_settlement_poll_delay_seconds=env_config.env_float(
            "PAPER_SETTLEMENT_POLL_DELAY_SECONDS", DEFAULT_SETTLEMENT_POLL_DELAY_SECONDS
        ),
        paper_settlement_max_wait_seconds=env_config.env_float(
            "PAPER_SETTLEMENT_MAX_WAIT_SECONDS", DEFAULT_SETTLEMENT_MAX_WAIT_SECONDS
        ),
        live_bankroll_cache_seconds=env_config.env_float(
            "LIVE_BANKROLL_CACHE_SECONDS", DEFAULT_BANKROLL_CACHE_SECONDS
        ),
        live_price_protection_tolerance=env_config.env_float(
            "LIVE_PRICE_PROTECTION_TOLERANCE", DEFAULT_PRICE_PROTECTION_TOLERANCE
        ),
    )


def _build_datastream_layer(config: Config, monitor: Monitor) -> DatastreamLayer:
    return DatastreamLayer(
        binance_symbol=config.binance_symbol,
        binance_interval=config.binance_interval,
        monitor=monitor,
        polymarket_poll_interval=config.polymarket_poll_interval,
        binance_reconnect_delay=config.binance_reconnect_delay,
        polymarket_reconnect_delay=config.polymarket_reconnect_delay,
        polymarket_app_ping_interval=config.polymarket_app_ping_interval,
        window_seconds=config.window_seconds,
        prefetch_lead_seconds=config.prefetch_lead_seconds,
        window_fetch_retry_delay=config.window_fetch_retry_delay,
    )


def _build_strategy_layer(config: Config, execution: ExecutionLayer, monitor: Monitor) -> StrategyLayer:
    return StrategyLayer(
        execution=execution,
        history_size=config.strategy_history_size,
        binance_symbol=config.binance_symbol,
        binance_interval=config.binance_interval,
        monitor=monitor,
        probability_margin=config.strategy_probability_margin,
        kelly_multiplier=config.strategy_kelly_multiplier,
        state_file_path=config.strategy_state_file_path,
    )


def _build_execution_layer(config: Config, monitor: Monitor) -> ExecutionLayer:
    mode = config.execution_mode

    if mode == "paper":
        return PaperExecutionLayer(
            monitor=monitor,
            retry_cooldown_seconds=config.execution_retry_cooldown_seconds,
            rejection_alert_threshold=config.execution_rejection_alert_threshold,
            max_position_notional=config.execution_max_position_notional,
            starting_bankroll=config.paper_starting_bankroll,
            taker_fee_rate=config.paper_taker_fee_rate,
            window_seconds=config.window_seconds,
            settlement_poll_delay_seconds=config.paper_settlement_poll_delay_seconds,
            settlement_max_wait_seconds=config.paper_settlement_max_wait_seconds,
        )
    if mode == "live":
        logger.warning("LIVE TRADING ENABLED -- REAL MONEY AT RISK")
        monitor.info("LIVE TRADING ENABLED -- REAL MONEY AT RISK")
        return LiveExecutionLayer(
            monitor=monitor,
            retry_cooldown_seconds=config.execution_retry_cooldown_seconds,
            rejection_alert_threshold=config.execution_rejection_alert_threshold,
            max_position_notional=config.execution_max_position_notional,
            bankroll_cache_seconds=config.live_bankroll_cache_seconds,
            price_protection_tolerance=config.live_price_protection_tolerance,
        )

    raise ValueError(f"Invalid EXECUTION_MODE {mode!r}: must be 'paper' or 'live'")


def route_result(result: ExecutionResult) -> None:
    print(f"[execution] {result}", flush=True)


def _register_commands(
    monitor: Monitor, strategy: StrategyLayer, execution: ExecutionLayer, stop_event: asyncio.Event
) -> None:
    async def handle_status() -> str:
        return f"{strategy.status_text()}\n\n{await execution.status_text()}"

    async def handle_stop() -> str:
        # Send the confirmation *before* signaling stop: setting stop_event
        # causes run()'s finally block to cancel this very listener task,
        # which would otherwise race (and often lose to) the in-flight send
        # of a reply returned the normal way.
        await monitor.reply("Stopping...")
        stop_event.set()
        return ""

    async def handle_pause() -> str:
        strategy.pause()
        return "Paused: no new positions will be opened. Existing positions still exit/settle normally. /resume to continue."

    async def handle_resume() -> str:
        strategy.resume()
        return "Resumed: new positions may be opened again."

    # Description text lives alongside each handler so /help can't drift out
    # of sync with what's actually registered.
    commands = {
        "status": ("Show strategy & execution status", handle_status),
        "pause": ("Stop opening new positions (existing ones still exit/settle normally)", handle_pause),
        "resume": ("Resume opening new positions", handle_resume),
        "stop": ("Shut down the orchestrator", handle_stop),
    }

    async def handle_help() -> str:
        lines = [f"/{name} - {desc}" for name, (desc, _) in sorted(commands.items())]
        return "Available commands:\n/help - Show this message\n" + "\n".join(lines)

    for name, (_, handler) in commands.items():
        monitor.register_command(name, handler)
    monitor.register_command("help", handle_help)


async def run() -> None:
    config = load_config()
    monitor = Monitor.from_env()
    stop_event = asyncio.Event()

    datastream = _build_datastream_layer(config, monitor)
    execution = _build_execution_layer(config, monitor)
    strategy = _build_strategy_layer(config, execution, monitor)
    _register_commands(monitor, strategy, execution, stop_event)

    mode_label = "LIVE (real money)" if isinstance(execution, LiveExecutionLayer) else "paper"
    logger.info("Orchestrator started (execution_mode=%s)", mode_label)
    monitor.info(f"Orchestrator started (execution_mode={mode_label})")
    logger.info("Effective config: %s", config)

    async def drain_results() -> None:
        while True:
            result = await execution.results.get()
            route_result(result)

    tasks = [
        asyncio.create_task(datastream.run()),
        asyncio.create_task(strategy.run(datastream.queue)),
        asyncio.create_task(drain_results()),
        asyncio.create_task(monitor.run_command_listener()),
        asyncio.create_task(stop_event.wait(), name="stop_event"),
    ]
    try:
        # FIRST_COMPLETED, not gather: lets a /stop (stop_event resolving)
        # shut everything down the same way an unhandled exception in any
        # other task would, rather than requiring every task to finish.
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            exc = task.exception()
            if exc is not None:
                raise exc
        if stop_event.is_set():
            logger.info("Stop requested via Telegram")
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        # systemd restarts the process on-failure (see deploy/bot11.service),
        # so without this a crash goes completely dark on Telegram -- the
        # only trace is a local journal entry nobody's watching in real
        # time. Awaited, not fired via monitor.error(), since a
        # fire-and-forget task isn't guaranteed to run before this
        # exception finishes propagating and the event loop stops.
        logger.exception("Orchestrator crashed")
        await monitor.notify_and_wait(MonitorCategory.ERROR, f"Orchestrator crashed: {exc!r}")
        raise
    else:
        await monitor.notify_and_wait(MonitorCategory.INFO, "Orchestrator stopped")
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if isinstance(execution, PaperExecutionLayer):
            execution.log_stats()


def main() -> None:
    load_dotenv()

    formatter = JsonLinesFormatter()
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    handlers: list[logging.Handler] = [stream_handler]

    # LOGS_DIRECTORY is set by systemd when the unit declares LogsDirectory=
    # (see deploy/bot11.service) -- a writable path carved out without
    # loosening ProtectSystem=strict for anything else. Falls back to the
    # working directory for local/dev runs with no systemd involved.
    # LOG_FILE_PATH overrides either default outright; set it to an empty
    # string to disable the file handler and keep stdout/journal-only.
    default_log_path = os.path.join(os.environ.get("LOGS_DIRECTORY", "."), "bot11.jsonl")
    log_file_path = env_config.env_str("LOG_FILE_PATH", default_log_path)
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
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Orchestrator stopped by user")


if __name__ == "__main__":
    main()
