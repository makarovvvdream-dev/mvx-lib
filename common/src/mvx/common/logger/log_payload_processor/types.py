# src/mvx/common/logger/log_payload_processor/types.py
from __future__ import annotations
from mvx.common.helpers.document_enum import document_enum

from typing import Any, TypeAlias, Callable

from enum import StrEnum

__all__ = (
    "LogVerbosityLevel",
    "LogAdapter",
    "LogAdapterResolver",
    "DEFAULT_MAX_STR_LEN",
    "DEFAULT_MAX_ITEMS",
)

# Default maximum string length used by the default payload processor.
DEFAULT_MAX_STR_LEN = 200
# Default maximum number of collection items used by the default payload processor.
DEFAULT_MAX_ITEMS = 10


@document_enum
class LogVerbosityLevel(StrEnum):
    """
    Verbosity levels used by the default payload processor.

    The level controls how much detail the default processor and related helpers
    may include when converting values to log payloads.
    """

    #: Minimal payload detail.
    MINIMAL = "MINIMAL"

    #: Default payload detail.
    NORMAL = "NORMAL"

    #: Maximum payload detail.
    MAXIMUM = "MAXIMUM"


# Callable used by the default payload processor to convert custom objects.
#
# The callable receives the value being normalized and the current verbosity
# level, and returns a log-ready payload dictionary.
LogAdapter: TypeAlias = Callable[[Any, LogVerbosityLevel], dict[str, Any]]

# Callable used by the default payload processor to resolve a log adapter.
#
# The callable receives the value being normalized and returns an adapter for
# that value, or None if no adapter is available.
LogAdapterResolver: TypeAlias = Callable[[Any], LogAdapter | None]
