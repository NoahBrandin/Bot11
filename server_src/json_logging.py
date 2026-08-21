"""JSON Lines log formatter -- one JSON object per record, still written to
stdout (captured by journald under systemd, see deploy/bot11.service) so no
new files or hardening changes are needed. Wired in once, in
orchestrator.py::main() via logging.basicConfig(), and applies to every
logger.*() call across every module (all of which just do
logging.getLogger(__name__) and rely on root propagation -- see
orchestrator.py's module docstring) without touching any of those call
sites.

A reviewer handed a `journalctl -u bot11 --since ... -o cat` transcript (or
any other captured stdout) can load it line-by-line with json.loads/jq
instead of parsing free-text log prose.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

# Standard LogRecord attributes -- anything else on the record was passed in
# via logger.info(msg, extra={...}) at a call site and should ride along in
# the JSON payload as a structured field.
_RESERVED_RECORD_KEYS = frozenset(
    {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "taskName",
    }
)


class JsonLinesFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key in _RESERVED_RECORD_KEYS or key in payload:
                continue
            try:
                json.dumps(value)
            except TypeError:
                value = repr(value)
            payload[key] = value
        return json.dumps(payload, default=str)
