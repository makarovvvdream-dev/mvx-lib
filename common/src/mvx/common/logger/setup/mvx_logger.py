# common/src/mvx/common/logger/mvx_logger.py
from __future__ import annotations

import logging
from typing import Any, Mapping
from contextvars import Token

from .adapter_registry import (
    get_active_log_profile as _get_active_log_profile,
    resolve_log_adapter as _resolve_log_adapter,
    LogAdapter,
)
from .payload_helpers import (
    normalize_value_for_log as _normalize_value_for_log,
    normalize_primitive as _normalize_primitive,
    normalize_list_for_log as _normalize_list_for_log,
    normalize_dict_for_log as _normalize_dict_for_log,
)
from .log_errors_helpers import (
    build_error_payload as _build_error_payload,
    is_error_logged as _is_error_logged,
    mark_error_logged as _mark_error_logged,
)
from .log_events_helpers import (
    log_event as _log_event,
    log_debug_event as _log_debug_event,
    log_info_event as _log_info_event,
    log_warning_event as _log_warning_event,
    log_error_event as _log_error_event,
)
from .trace_context import (
    get_trace_id as _get_trace_id,
    set_trace_id as _set_trace_id,
    reset_trace_id as _reset_trace_id,
)


class MvxLogger(logging.Logger):
    """
    Extended Logger with helper methods for mvx logging facilities.

    Features
    --------
    - Exposes the current active log profile via `log_profile` property.
    - Exposes the current trace id via `trace_id` property and provides
      helpers to set/reset it in the current context.

    - Provides helpers for value normalization:
        * normalize_value(...)
        * normalize_primitive(...)
        * normalize_list(...)
        * normalize_dict(...)
      all delegating to mvx.logger.payload_helpers.

    - Provides structured event helpers as instance methods:
        * log_event(...)
        * log_debug_event(...)
        * log_info_event(...)
        * log_warning_event(...)
        * log_error_event(...)

      These mirror mvx.logger.helpers, but are bound to this logger.

    - Provides `build_error_payload(err)` to normalize exceptions into a payload.
    - Provides error-flag helpers:
        * is_error_logged(err)
        * mark_error_logged(err)

    - Provides `get_adapter(value, profile=...)` to inspect which type-based
      log adapter (if any) would be used for a given value.
    """

    # -------- Profiles / adapters --------

    @property
    def log_profile(self) -> str:
        """
        Return the currently active log profile for type-based log adapters.

        This is effectively a proxy to adapter_registry.get_active_log_profile().
        """
        return _get_active_log_profile()

    @staticmethod
    def get_adapter(
        value: Any,
        *,
        profile: str | None = None,
    ) -> LogAdapter | None:
        """
        Resolve a log adapter for `value` and the given profile.

        This is a thin convenience wrapper around adapter_registry.resolve_log_adapter.
        """
        return _resolve_log_adapter(value, profile=profile)

    # -------- Trace id helpers --------

    @property
    def trace_id(self) -> str:
        """
        Return the current trace_id for the execution context.

        This is a proxy to trace_context.get_trace_id().
        """
        return _get_trace_id()

    @staticmethod
    def set_trace_id(value: str | None = None) -> Token[str]:
        """
        Set trace_id for the current execution context.

        This is a thin wrapper around trace_context.set_trace_id(value).
        """
        return _set_trace_id(value)

    @staticmethod
    def reset_trace_id(token: Token[str]) -> None:
        """
        Restore the previous trace_id value using the token returned by set_trace_id().
        """
        _reset_trace_id(token)

    # -------- Normalization helpers --------

    @staticmethod
    def normalize_value(
        value: Any, *, max_items: int | None = None
    ) -> str | int | float | bytes | bool | dict[str, Any] | list[Any] | None:
        """
        Normalize a value for logging using the same rules as log_invovation.
        """
        return _normalize_value_for_log(value, max_items=max_items)

    @staticmethod
    def normalize_primitive(value: Any) -> str | int | float | bytes | bool | None:
        """
        Normalize primitive values for logging.

        This is a thin wrapper around payload_helpers.normalize_primitive.
        """
        return _normalize_primitive(value)

    @staticmethod
    def normalize_list(value: Any, *, max_items: int | None = None) -> str | list[Any]:
        """
        Normalize a list/tuple for logging, one level deep.

        This is a thin wrapper around payload_helpers.normalize_list_for_log.
        """
        return _normalize_list_for_log(value, max_items=max_items)

    @staticmethod
    def normalize_dict(value: Any, *, max_items: int | None = None) -> str | dict[str, Any]:
        """
        Normalize a dict for logging, one level deep.

        This is a thin wrapper around payload_helpers.normalize_dict_for_log.
        """
        return _normalize_dict_for_log(value, max_items=max_items)

    # -------- Structured event helpers --------

    def log_event(
        self,
        evt: str,
        data: Mapping[str, Any] | None = None,
        *,
        level: int = logging.INFO,
    ) -> None:
        """
        Emit a structured log event with a stable `evt` and payload in `data`.

        This is a bound version of mvx.logger.log_events_helpers.log_event, using
        this logger instance as the sink.
        """
        _log_event(self, evt, data=data, level=level)

    def log_debug_event(
        self,
        evt: str,
        data: Mapping[str, Any] | None = None,
    ) -> None:
        """
        Emit a structured debug log event with a stable `evt` and payload in `data`.
        """
        _log_debug_event(self, evt, data=data)

    def log_info_event(
        self,
        evt: str,
        data: Mapping[str, Any] | None = None,
    ) -> None:
        """
        Emit a structured info log event with a stable `evt` and payload in `data`.
        """
        _log_info_event(self, evt, data=data)

    def log_warning_event(
        self,
        evt: str,
        data: Mapping[str, Any] | None = None,
    ) -> None:
        """
        Emit a structured warning log event with a stable `evt` and payload in `data`.
        """
        _log_warning_event(self, evt, data=data)

    def log_error_event(
        self,
        evt: str,
        data: Mapping[str, Any] | None = None,
    ) -> None:
        """
        Emit a structured error log event with a stable `evt` and payload in `data`.
        """
        _log_error_event(self, evt, data=data)

    # -------- Error payload & flags --------

    @staticmethod
    def build_error_payload(err: BaseException) -> dict[str, Any]:
        """
        Normalize an exception into a serializable payload.

        This is a thin wrapper around mvx.logger.log_errors_helpers.build_error_payload.
        """
        return _build_error_payload(err)

    @staticmethod
    def is_error_logged(err: BaseException) -> bool:
        """
        Check whether the given exception instance has already been logged with
        a detailed error payload.
        """
        return _is_error_logged(err)

    @staticmethod
    def mark_error_logged(err: BaseException) -> None:
        """
        Mark the given exception instance as already logged.

        This is a thin wrapper around mvx.logger.log_errors_helpers.build_error_payload.
        """
        _mark_error_logged(err)
