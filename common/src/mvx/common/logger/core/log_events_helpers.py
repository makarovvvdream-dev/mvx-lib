# common/src/mvx/common/logger/core/log_events_helpers.py
from __future__ import annotations

import logging
from typing import Any, Mapping, MutableMapping


def log_event(
    logger: logging.Logger,
    evt: str,
    data: Mapping[str, Any] | None = None,
    *,
    level: int = logging.INFO,
) -> None:
    """
    Emit a structured log event with a stable `evt` and payload in `data`.

    Contract:
      - msg is always the event name (evt).
      - Variable payload is always placed into `extra={"evt": evt, "data": <dict>}`.
      - Caller is responsible for choosing the log level.

    This helper never raises; if logging itself fails, it is propagated by the logging module.
    """
    payload: MutableMapping[str, Any] = dict(data or {})
    logger.log(level, evt, extra={"evt": evt, "data": payload})


def log_debug_event(
    logger: logging.Logger,
    evt: str,
    data: Mapping[str, Any] | None = None,
) -> None:
    """
    Emit a structured debug log event with a stable `evt` and payload in `data`.
    """
    log_event(logger, evt, data=data, level=logging.DEBUG)


def log_info_event(
    logger: logging.Logger,
    evt: str,
    data: Mapping[str, Any] | None = None,
) -> None:
    """
    Emit a structured info log event with a stable `evt` and payload in `data`.
    """
    log_event(logger, evt, data=data, level=logging.INFO)


def log_warning_event(
    logger: logging.Logger,
    evt: str,
    data: Mapping[str, Any] | None = None,
) -> None:
    """
    Emit a structured warning log event with a stable `evt` and payload in `data`.
    """
    log_event(logger, evt, data=data, level=logging.WARNING)


def log_error_event(
    logger: logging.Logger,
    evt: str,
    data: Mapping[str, Any] | None = None,
) -> None:
    """
    Emit a structured error log event with a stable `evt` and payload in `data`.
    """
    log_event(logger, evt, data=data, level=logging.ERROR)


__all__ = [
    "log_event",
    "log_debug_event",
    "log_info_event",
    "log_warning_event",
    "log_error_event",
]
