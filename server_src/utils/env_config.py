"""Shared .env parsing helpers. Leaf module (no intra-project imports) so
both orchestrator.py and monitoring/monitor.py's from_env() can use it
without a circular import between them.

Every numeric helper falls back to its default and logs a warning on a
malformed value rather than raising -- a bad .env line should degrade to
known-good behavior, not crash the whole orchestrator at startup.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


def env_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid int for %s=%r, using default %r", name, raw, default)
        return default


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid float for %s=%r, using default %r", name, raw, default)
        return default


def env_optional_float(name: str, default: Optional[float]) -> Optional[float]:
    """Same as env_float, but a *present-and-empty* or "none" value means
    None (used for tunables that can be explicitly disabled via .env) --
    the var being unset entirely still falls back to `default` like the
    other helpers."""
    if name not in os.environ:
        return default
    raw = os.environ[name]
    if raw.strip() == "" or raw.strip().lower() == "none":
        return None
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid float for %s=%r, using default %r", name, raw, default)
        return default
