"""Monitor: optional Telegram forwarding layered on top of each layer's
existing local logging, plus an inbound command listener (/status, /stop).

Two independent Telegram bots back this, deliberately kept separate:
`telegram` (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID) sends outbound alerts only,
`commands_telegram` (TELEGRAM_COMMANDS_BOT_TOKEN/TELEGRAM_COMMANDS_CHAT_ID)
is the only one that listens for and responds to commands. Either can be
configured without the other.

Every layer gets a Monitor instance (defaulting to a no-op one if none is
given, so passing one is never required to keep existing behavior working).
Call sites keep their existing logger.info/exception(...) calls exactly as
they are, and additionally call the matching Monitor method (.event/.order/
.execution/.error/.info) with the same message -- those method names are
literally the coarse category names, so which categories reach Telegram is
purely a matter of .env configuration, never a code change.

Commands are separate from that category system: register_command() lets
the orchestrator (which owns execution/strategy/shutdown) wire up handlers,
and a response is always sent back regardless of TELEGRAM_ENABLED_CATEGORIES
-- it's a direct reply to something the user just asked for, not a passive
notification to be filtered.
"""
from __future__ import annotations

import asyncio
import logging
import os
from enum import Enum
from typing import Awaitable, Callable, Optional

from utils import env_config

from .telegram import DEFAULT_LONGPOLL_TIMEOUT, TelegramNotifier

logger = logging.getLogger(__name__)

CommandHandler = Callable[[str], Awaitable[str]]

# Delay before retrying a failed getUpdates long-poll (distinct from the
# poll's own timeout).
DEFAULT_GETUPDATES_RETRY_DELAY = 2.0


class MonitorCategory(str, Enum):
    EVENT = "EVENT"
    ORDER = "ORDER"
    EXECUTION = "EXECUTION"
    ERROR = "ERROR"
    INFO = "INFO"
    DEBUG = "DEBUG"


class Monitor:
    def __init__(
        self,
        telegram: Optional[TelegramNotifier] = None,
        enabled_categories: Optional[set] = None,
        longpoll_timeout: int = DEFAULT_LONGPOLL_TIMEOUT,
        getupdates_retry_delay: float = DEFAULT_GETUPDATES_RETRY_DELAY,
        commands_telegram: Optional[TelegramNotifier] = None,
    ) -> None:
        self._telegram = telegram
        self._enabled = enabled_categories or set()
        self._command_handlers: dict[str, CommandHandler] = {}
        self._longpoll_timeout = longpoll_timeout
        self._getupdates_retry_delay = getupdates_retry_delay
        self._commands_telegram = commands_telegram

    def event(self, message: str) -> None:
        self._notify(MonitorCategory.EVENT, message)

    def order(self, message: str) -> None:
        self._notify(MonitorCategory.ORDER, message)

    def execution(self, message: str) -> None:
        self._notify(MonitorCategory.EXECUTION, message)

    def error(self, message: str) -> None:
        self._notify(MonitorCategory.ERROR, message)

    def info(self, message: str) -> None:
        self._notify(MonitorCategory.INFO, message)

    def debug(self, message: str) -> None:
        self._notify(MonitorCategory.DEBUG, message)

    def _notify(self, category: MonitorCategory, message: str) -> None:
        if self._telegram is None or category not in self._enabled:
            return
        asyncio.create_task(self._send_safely(f"[{category.value}] {message}"))

    async def notify_and_wait(self, category: MonitorCategory, message: str) -> None:
        """Like the .event/.order/.execution/.error/.info methods, but awaits
        delivery instead of firing a background task. Use this right before
        the process is about to exit (crash, graceful shutdown) -- a
        fire-and-forget task raised via _notify() is not guaranteed to run
        before the event loop stops."""
        if self._telegram is None or category not in self._enabled:
            return
        await self._send_safely(f"[{category.value}] {message}")

    async def _send_safely(self, text: str) -> None:
        try:
            await self._telegram.send(text)
        except Exception:
            logger.exception("Failed to deliver Telegram notification")

    def register_command(self, name: str, handler: CommandHandler) -> None:
        """`handler` is an async callable taking the raw text after the
        command (e.g. "/notify ORDER,ERROR" -> "ORDER,ERROR"; "" if none was
        given) and returning the text to send back. `name` is the command
        without its leading slash (e.g. "status")."""
        self._command_handlers[name.lstrip("/").lower()] = handler

    def enabled_categories(self) -> set:
        return set(self._enabled)

    def set_enabled_categories(self, categories: set) -> None:
        """Lets a command handler (e.g. /notify) change which categories
        reach Telegram at runtime, instead of only via the
        TELEGRAM_ENABLED_CATEGORIES env var read once at startup."""
        self._enabled = set(categories)

    async def run_command_listener(self) -> None:
        """Long-polls the commands bot for inbound messages and dispatches
        registered commands. No-ops if the commands bot isn't configured.
        Only responds to messages from its configured chat_id -- anyone else
        who finds the bot must not be able to see status or send /stop."""
        if self._commands_telegram is None:
            return

        # Telegram redelivers any update we haven't acknowledged with a
        # higher offset yet -- since we don't persist offset across
        # restarts, a fresh process would otherwise immediately re-receive
        # (and act on) whatever command was last sent to a *previous* run,
        # including things like /stop from an old test. Drain and discard
        # any backlog once at startup before processing anything for real.
        offset: Optional[int] = None
        try:
            backlog = await self._commands_telegram.get_updates(offset, timeout=0)
        except Exception:
            # Same rationale as the main poll loop below: a transient
            # failure here (network timeout, DNS hiccup) must not propagate
            # -- unlike that loop, this call runs once before it, so an
            # unhandled exception here previously killed the whole
            # orchestrator via orchestrator.run()'s FIRST_COMPLETED wait.
            # Skipping the drain just means a stale backlog (if any) gets
            # processed once the main loop's first successful poll runs,
            # instead of crashing the bot before it even starts listening.
            logger.exception("Telegram backlog drain failed, continuing without it")
            backlog = []
        if backlog:
            offset = backlog[-1]["update_id"] + 1
            logger.info("Discarded %d stale Telegram update(s) from before startup", len(backlog))

        while True:
            try:
                updates = await self._commands_telegram.get_updates(offset, timeout=self._longpoll_timeout)
            except Exception:
                logger.exception("Telegram getUpdates failed, retrying shortly")
                await asyncio.sleep(self._getupdates_retry_delay)
                continue

            for update in updates:
                offset = update["update_id"] + 1
                await self._handle_update(update)

    async def _handle_update(self, update: dict) -> None:
        message = update.get("message") or {}
        text = (message.get("text") or "").strip()
        if not text.startswith("/"):
            return

        chat_id = str(message.get("chat", {}).get("id", ""))
        if chat_id != str(self._commands_telegram.chat_id):
            logger.warning("Ignoring Telegram command from unauthorized chat_id=%s", chat_id)
            return

        head, _, args = text[1:].partition(" ")
        command = head.split("@")[0].lower()
        handler = self._command_handlers.get(command)
        if handler is None:
            return

        try:
            response = await handler(args.strip())
        except Exception:
            logger.exception("Command handler for /%s failed", command)
            response = f"Error running /{command} -- see logs"

        # A handler that triggers a side effect racing this reply (e.g.
        # /stop tearing down the task this loop runs on) must send its own
        # confirmation *before* that side effect and return "" here, so we
        # don't try to send an empty/duplicate follow-up.
        if not response:
            return

        try:
            await self._commands_telegram.send(response)
        except Exception:
            logger.exception("Failed to send /%s response", command)

    async def reply(self, text: str) -> None:
        """For a handler that needs its reply guaranteed sent before
        triggering some side effect (e.g. /stop), rather than relying on
        _handle_update's post-handler send, which can lose a race against
        whatever the side effect tears down."""
        if self._commands_telegram is not None:
            await self._commands_telegram.send(text)

    @classmethod
    def from_env(cls) -> "Monitor":
        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID")

        telegram = None
        if bot_token and chat_id:
            telegram = TelegramNotifier(bot_token, chat_id)
        else:
            logger.warning("TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set; Telegram notifications disabled")

        commands_bot_token = os.environ.get("TELEGRAM_COMMANDS_BOT_TOKEN")
        commands_chat_id = os.environ.get("TELEGRAM_COMMANDS_CHAT_ID")

        commands_telegram = None
        if commands_bot_token and commands_chat_id:
            commands_telegram = TelegramNotifier(commands_bot_token, commands_chat_id)
        else:
            logger.warning(
                "TELEGRAM_COMMANDS_BOT_TOKEN/TELEGRAM_COMMANDS_CHAT_ID not set; Telegram commands disabled"
            )

        enabled = _parse_categories(os.environ.get("TELEGRAM_ENABLED_CATEGORIES", ""))
        longpoll_timeout = env_config.env_int("TELEGRAM_LONGPOLL_TIMEOUT_SECONDS", DEFAULT_LONGPOLL_TIMEOUT)
        getupdates_retry_delay = env_config.env_float(
            "MONITOR_GETUPDATES_RETRY_DELAY_SECONDS", DEFAULT_GETUPDATES_RETRY_DELAY
        )
        return cls(
            telegram=telegram,
            enabled_categories=enabled,
            longpoll_timeout=longpoll_timeout,
            getupdates_retry_delay=getupdates_retry_delay,
            commands_telegram=commands_telegram,
        )


def _parse_categories(raw: str) -> set:
    raw = raw.strip()
    if not raw:
        return set()
    if raw.upper() == "ALL":
        return set(MonitorCategory)

    enabled = set()
    for part in raw.split(","):
        part = part.strip().upper()
        if not part:
            continue
        try:
            enabled.add(MonitorCategory(part))
        except ValueError:
            logger.warning("Unknown TELEGRAM_ENABLED_CATEGORIES entry: %s", part)
    return enabled
