# common/src/mvx/common/logger/core/log_context.py
from __future__ import annotations
from typing import Mapping, Any, Callable

from enum import StrEnum
import time

from ..errors import LogContextError, LogContextErrorReason

from .models import (
    LogLevel,
    LogAdapterResolver,
    LogSinkProto,
    LogEventPolicy,
    LogEvent,
)
from .log_payload_helpers import normalize_value_for_log

__all__ = ("LogVerbosityLevel", "LogContext")


class LogVerbosityLevel(StrEnum):
    MINIMAL = "MINIMAL"
    NORMAL = "NORMAL"
    MAXIMUM = "MAXIMUM"


ERR_LOGGED_FLAG = "_mvx_error_logged"

DEFAULT_MAX_STR_LEN = 200
DEFAULT_MAX_ITEMS = 10


class LogContext:

    def __init__(
        self,
        *,
        namespace: str,
        parent: LogContext | None,
        log_sink_name: str | None,
        log_sink_resolver: Callable[[str], LogSinkProto],
        event_policy: LogEventPolicy,
        verbosity_level: LogVerbosityLevel = LogVerbosityLevel.NORMAL,
        log_adapter_resolver: LogAdapterResolver | None = None,
        max_str_len: int = DEFAULT_MAX_STR_LEN,
        max_items: int = DEFAULT_MAX_ITEMS,
    ):
        self._namespace = namespace
        self._parent = parent
        self._log_sink_name = log_sink_name
        self._log_sink_resolver = log_sink_resolver
        self._event_policy = event_policy
        self._verbosity_level = verbosity_level
        self._log_adapter_resolver = log_adapter_resolver
        self._max_str_len = max_str_len
        self._max_items = max_items

    # ---- Properties ----------------------------------------------------------------------

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def log_sink_name(self) -> str | None:
        return self._log_sink_name

    @property
    def parent(self) -> LogContext | None:
        return self._parent

    @property
    def event_policy(self) -> LogEventPolicy:
        return self._event_policy

    @event_policy.setter
    def event_policy(self, event_policy: LogEventPolicy) -> None:
        self._event_policy = event_policy

    @property
    def verbosity_level(self) -> str:
        return self._verbosity_level.value

    @verbosity_level.setter
    def verbosity_level(self, verbosity_level: LogVerbosityLevel) -> None:
        self._verbosity_level = verbosity_level

    # ---- Log sink ------------------------------------------------------------------------

    def _resolve_sink(self) -> LogSinkProto:
        if self._log_sink_name is not None:
            return self._log_sink_resolver(self._log_sink_name)

        if self._parent is not None:
            return self._parent._resolve_sink()

        raise LogContextError(
            message=f"Log context {self._namespace!r} has no configured log sink",
            reason=LogContextErrorReason.LOG_CONTEXT_SINK_NOT_CONFIGURED.value,
        )

    # ---- Logging events ------------------------------------------------------------------

    def is_event_enabled(self, event: str) -> bool:
        return self._event_policy.is_event_enabled(event)

    def log_event(
        self,
        event: str,
        level: LogLevel,
        payload: Mapping[str, Any],
        *,
        event_type: str | None = None,
        entity_id: str | None = None,
    ) -> None:
        log_event = LogEvent(
            level=level,
            namespace=self._namespace,
            event_name=event,
            event_type=event_type if event_type is not None else "<not defined>",
            timestamp=time.time(),
            entity_id=entity_id if entity_id is not None else "<not defined>",
            payload=payload,
        )

        sink = self._resolve_sink()
        sink.log(log_event)

    # ---- Errors handlers -----------------------------------------------------------------

    def build_error_payload(self, err: BaseException) -> Mapping[str, Any]:
        """
        Normalize an exception into a serializable payload.

        Rules:
          - If the exception provides a callable `to_log_extra()` method, use it.
          - Otherwise:
              * try to surface `code` and `code_desc` attributes if present;
              * always include `kind` and `message`.

        This keeps RedisAdapterError and similar exceptions compatible without
        importing them directly.
        """
        # this realization does not use self, but others may
        _ = self

        # Duck-typing: Error should expose to_log_extra()
        to_extra = getattr(err, "to_log_payload", None)

        if callable(to_extra):
            # noinspection PyBroadException
            try:
                extra = to_extra()
                if isinstance(extra, dict):
                    return dict(extra)
            except Exception:
                # Fallback to generic representation below.
                pass

        payload: dict[str, Any] = {}

        code = getattr(err, "code", None)
        if code is not None:
            payload["code"] = code

        code_desc = getattr(err, "code_desc", None)
        if code_desc is not None:
            payload["code_desc"] = code_desc

        payload.setdefault("kind", type(err).__name__)
        payload.setdefault("message", str(err))

        return payload

    def is_error_logged(self, err: BaseException) -> bool:
        """
        Check whether the given exception instance has already been logged
        with a detailed error payload.
        """
        # this realization does not use self, but others may
        _ = self

        # noinspection PyBroadException
        try:
            return bool(getattr(err, ERR_LOGGED_FLAG, False))
        except Exception:
            return False

    def mark_error_logged(self, err: BaseException) -> None:
        """
        Best-effort marking of an exception instance as already logged.

        If the instance does not allow arbitrary attributes (e.g. __slots__ without
        __dict__), the error is silently ignored and the flag is not set.
        """
        # this realization does not use self, but others may
        _ = self

        # noinspection PyBroadException
        try:
            setattr(err, ERR_LOGGED_FLAG, True)
        except Exception:
            # Best effort: ignore if we cannot set the flag
            pass

    # ---- Values normalization ------------------------------------------------------------

    def normalize_value_for_log(
        self,
        value: Any,
        *,
        unbounded: bool = False,
    ) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None:

        return normalize_value_for_log(
            value,
            log_adapter_resolver=self._log_adapter_resolver,
            verbosity_level=self._verbosity_level.value,
            max_items=self._max_items if not unbounded else None,
            max_str_len=self._max_str_len,
        )
