# src/mvx/common/logger/log_payload_processor/types.py
from __future__ import annotations
from typing import Any, TypeAlias, Callable

from enum import StrEnum

__all__ = (
    "LogVerbosityLevel",
    "LogAdapter",
    "LogAdapterResolver",
    "DEFAULT_MAX_STR_LEN",
    "DEFAULT_MAX_ITEMS",
)

DEFAULT_MAX_STR_LEN = 200
DEFAULT_MAX_ITEMS = 10


class LogVerbosityLevel(StrEnum):
    MINIMAL = "MINIMAL"
    NORMAL = "NORMAL"
    MAXIMUM = "MAXIMUM"


LogAdapter: TypeAlias = Callable[[Any, LogVerbosityLevel], dict[str, Any]]
LogAdapterResolver: TypeAlias = Callable[[Any], LogAdapter | None]
