# src/mvx/common/logger/log_context/log_context.py
from __future__ import annotations

from typing import Mapping, Any, overload

from enum import StrEnum
import time
import threading


from ..models import (
    LogLevel,
    LogAdapterResolver,
    LogSinkProto,
    LogEventPolicy,
    LogEvent,
)
from ..helpers import log_internal_error as _log_internal_error

from ..errors import LogContextResetError, LogContextUnableToLog


from .log_payload_helpers import (
    normalize_value_for_log,
    normalize_primitive,
    normalize_list_for_log,
    normalize_dict_for_log,
)

__all__ = ("LogVerbosityLevel", "LogErrorHandlingPolicy", "LogContext")


class LogVerbosityLevel(StrEnum):
    MINIMAL = "MINIMAL"
    NORMAL = "NORMAL"
    MAXIMUM = "MAXIMUM"


class LogErrorHandlingPolicy(StrEnum):
    IGNORE = "IGNORE"
    PRINT_STDERR = "PRINT_STDERR"
    RAISE = "RAISE"


ERR_LOGGED_FLAG = "_mvx_error_logged"

DEFAULT_MAX_STR_LEN = 200
DEFAULT_MAX_ITEMS = 10


class LogContext:

    _log_adapter_resolver: LogAdapterResolver | None

    @overload
    def __init__(
        self,
        *,
        namespace: str | None = None,
        parent: None = None,
        log_sink: LogSinkProto,
        event_policy: LogEventPolicy | None = None,
        verbosity_level: LogVerbosityLevel,
        max_str_len: int | None = None,
        max_items: int | None = None,
        log_error_handling_policy: LogErrorHandlingPolicy | None = None,
    ): ...
    @overload
    def __init__(
        self,
        *,
        namespace: str | None = None,
        parent: LogContext,
        log_sink: LogSinkProto | None = None,
        event_policy: LogEventPolicy | None = None,
        verbosity_level: LogVerbosityLevel | None = None,
        max_str_len: int | None = None,
        max_items: int | None = None,
        log_error_handling_policy: LogErrorHandlingPolicy | None = None,
    ): ...

    def __init__(
        self,
        *,
        namespace: str | None = None,
        parent: LogContext | None = None,
        log_sink: LogSinkProto | None = None,
        event_policy: LogEventPolicy | None = None,
        verbosity_level: LogVerbosityLevel | None = None,
        max_str_len: int | None = None,
        max_items: int | None = None,
        log_error_handling_policy: LogErrorHandlingPolicy | None = None,
    ):

        if namespace is not None:
            if not isinstance(namespace, str):
                raise TypeError("argument 'namespace' must be string")

            namespace = namespace.strip()

        if parent is None:
            if log_sink is None:
                raise ValueError(
                    "argument 'log_sink' is mandatory for the root log context, must not be None"
                )

            if verbosity_level is None:
                raise ValueError(
                    "argument 'verbosity_level' is mandatory for the root log context, must not be None"
                )

        else:
            if not isinstance(parent, LogContext):
                raise TypeError("argument 'parent' must be an instance of 'LogContext'")

        if log_sink is not None:
            if not isinstance(log_sink, LogSinkProto):
                raise TypeError("argument 'log_sink' must be an instance of 'LogSinkProto'")

        if event_policy is not None:
            if not isinstance(event_policy, LogEventPolicy):
                raise TypeError("argument 'event_policy' must be an instance of 'LogEventPolicy'")

        if verbosity_level is not None:
            if not isinstance(verbosity_level, LogVerbosityLevel):
                raise TypeError(
                    "argument 'verbosity_level' must be an instance of 'LogVerbosityLevel'"
                )

        if max_str_len is not None:
            if not isinstance(max_str_len, int):
                raise TypeError("argument 'max_str_len' must be integer when provided")

            if max_str_len < 1:
                raise ValueError("argument 'max_str_len' must be greater than 0")

        if max_items is not None:
            if not isinstance(max_items, int):
                raise TypeError("argument 'max_items' must be integer when provided")

            if max_items < 1:
                raise ValueError("argument 'max_items' must be greater than 0")

        if log_error_handling_policy is not None:
            if not isinstance(log_error_handling_policy, LogErrorHandlingPolicy):
                raise TypeError(
                    "argument 'log_error_handling_policy' must be an instance of 'LogErrorHandlingPolicy'"
                )

        self._config_lock = threading.RLock()
        self._namespace = namespace
        self._parent = parent
        self._log_sink = log_sink
        self._event_policy = event_policy
        self._verbosity_level = verbosity_level

        if parent is None:
            max_str_len = max_str_len if max_str_len is not None else DEFAULT_MAX_STR_LEN
            max_items = max_items if max_items is not None else DEFAULT_MAX_ITEMS
            log_error_handling_policy = (
                log_error_handling_policy
                if log_error_handling_policy is not None
                else LogErrorHandlingPolicy.RAISE
            )

        self._max_str_len = max_str_len
        self._max_items = max_items
        self._log_error_handling_policy = log_error_handling_policy
        self._log_adapter_resolver = None

        self._log_error_printed = False

    # ---- Properties ----------------------------------------------------------------------

    @property
    def namespace(self) -> str:
        return self._namespace if self._namespace is not None else "<not defined>"

    @property
    def is_root(self) -> bool:
        return bool(self._parent is None)

    @property
    def parent(self) -> LogContext | None:
        return self._parent

    @property
    def log_sink(self) -> LogSinkProto:
        with self._config_lock:
            if self._log_sink is not None:
                return self._log_sink

            assert self._parent is not None, "invariant: controlled by constructor"
            return self._parent.log_sink

    def set_log_sink(self, log_sink: LogSinkProto) -> None:
        if log_sink is None:
            raise ValueError("argument 'log_sink' must not be None")

        if not isinstance(log_sink, LogSinkProto):
            raise TypeError("argument 'log_sink' must be an instance of 'LogSinkProto'")

        with self._config_lock:
            self._log_sink = log_sink

    def reset_log_sink(self) -> None:
        if self.is_root:
            raise LogContextResetError(
                target="log_sink",
            )

        with self._config_lock:
            self._log_sink = None

    @property
    def event_policy(self) -> LogEventPolicy | None:
        with self._config_lock:
            return self._event_policy

    def set_event_policy(self, event_policy: LogEventPolicy) -> None:
        if event_policy is None:
            raise ValueError("argument 'event_policy' must not be None")

        if not isinstance(event_policy, LogEventPolicy):
            raise TypeError("argument 'event_policy' must be an instance of 'LogEventPolicy'")

        with self._config_lock:
            self._event_policy = event_policy

    def reset_event_policy(self) -> None:
        with self._config_lock:
            self._event_policy = None

    @property
    def verbosity_level(self) -> str:
        with self._config_lock:
            if self._verbosity_level is not None:
                return self._verbosity_level.value

            assert self._parent is not None, "invariant: controlled by constructor"
            return self._parent.verbosity_level

    def set_verbosity_level(self, verbosity_level: LogVerbosityLevel) -> None:
        if verbosity_level is None:
            raise ValueError("argument 'verbosity_level' must not be None")

        if not isinstance(verbosity_level, LogVerbosityLevel):
            raise TypeError("argument 'verbosity_level' must be an instance of 'LogVerbosityLevel'")

        with self._config_lock:
            self._verbosity_level = verbosity_level

    def reset_verbosity_level(self) -> None:
        if self.is_root:
            raise LogContextResetError(
                target="verbosity_level",
            )
        with self._config_lock:
            self._verbosity_level = None

    @property
    def max_str_len(self) -> int:
        with self._config_lock:
            if self._max_str_len is not None:
                return self._max_str_len

            assert self._parent is not None, "invariant: controlled by constructor"
            return self._parent.max_str_len

    def set_max_str_len(self, max_str_len: int) -> None:
        if max_str_len is None:
            raise ValueError("argument 'max_str_len' must not be None")

        if not isinstance(max_str_len, int):
            raise TypeError("argument 'max_str_len' must be integer")

        if max_str_len < 1:
            raise ValueError("argument 'max_str_len' must be greater than 0")

        with self._config_lock:
            self._max_str_len = max_str_len

    def reset_max_str_len(self) -> None:
        max_str_len = DEFAULT_MAX_STR_LEN if self.is_root else None

        with self._config_lock:
            self._max_str_len = max_str_len

    @property
    def max_items(self) -> int:
        with self._config_lock:
            if self._max_items is not None:
                return self._max_items

            assert self._parent is not None, "invariant: controlled by constructor"
            return self._parent.max_items

    def set_max_items(self, max_items: int) -> None:
        if max_items is None:
            raise ValueError("argument 'max_items' must not be None")

        if not isinstance(max_items, int):
            raise TypeError("argument 'max_items' must be integer")

        if max_items < 1:
            raise ValueError("argument 'max_items' must be greater than 0")

        with self._config_lock:
            self._max_items = max_items

    def reset_max_items(self) -> None:
        max_items = DEFAULT_MAX_ITEMS if self.is_root else None

        with self._config_lock:
            self._max_items = max_items

    @property
    def log_adapter_resolver(self) -> LogAdapterResolver | None:
        with self._config_lock:
            if self._log_adapter_resolver is not None:
                return self._log_adapter_resolver

            if self._parent is None:
                return None

            return self._parent.log_adapter_resolver

    def set_log_adapter_resolver(self, log_adapter_resolver: LogAdapterResolver) -> None:
        if log_adapter_resolver is None:
            raise ValueError("argument 'log_adapter_resolver' must not be None")

        if not callable(log_adapter_resolver):
            raise TypeError("argument 'log_adapter_resolver' must be a callable")

        with self._config_lock:
            self._log_adapter_resolver = log_adapter_resolver

    def reset_log_adapter_resolver(self) -> None:
        with self._config_lock:
            self._log_adapter_resolver = None

    @property
    def log_error_handling_policy(self) -> LogErrorHandlingPolicy:
        with self._config_lock:
            if self._log_error_handling_policy is not None:
                return self._log_error_handling_policy

            assert self._parent is not None, "invariant: controlled by constructor"
            return self._parent.log_error_handling_policy

    def set_log_error_handling_policy(
        self, log_error_handling_policy: LogErrorHandlingPolicy
    ) -> None:
        if log_error_handling_policy is None:
            raise ValueError("argument 'log_error_handling_policy' must not be None")

        if not isinstance(log_error_handling_policy, LogErrorHandlingPolicy):
            raise TypeError(
                "argument 'log_error_handling_policy' must be an instance of 'LogErrorHandlingPolicy'"
            )

        with self._config_lock:
            self._log_error_handling_policy = log_error_handling_policy

    def reset_log_error_handling_policy(self) -> None:
        if self.is_root:
            raise LogContextResetError(
                target="log_error_handling_policy",
            )
        with self._config_lock:
            self._log_error_handling_policy = None

    def get_local_log_sink(self) -> LogSinkProto | None:
        with self._config_lock:
            return self._log_sink

    # ---- Logging events ------------------------------------------------------------------

    def is_event_enabled(self, event: str) -> bool:
        with self._config_lock:
            event_policy = self._event_policy

        if event_policy is None:
            return True

        return event_policy.is_event_enabled(event)

    def log_event(
        self,
        event: str,
        level: LogLevel,
        payload: Mapping[str, Any],
        *,
        event_namespace: str | None = None,
        event_type: str | None = None,
        entity_id: str | None = None,
        source_path: str | None = None,
        source_line: int | None = None,
        source_func: str | None = None,
    ) -> None:
        if not self.is_event_enabled(event):
            return

        log_event = LogEvent(
            level=level,
            event_namespace=event_namespace if event_namespace is not None else self.namespace,
            event_name=event,
            event_type=event_type,
            timestamp=time.time(),
            entity_id=entity_id,
            payload=payload,
            source_path=source_path,
            source_line=source_line,
            source_func=source_func,
        )

        # noinspection PyBroadException
        try:
            self.log_sink.log(log_event)
            # not under lock intentionally
            self._log_error_printed = False

        except Exception as exc:
            handling_policy = self.log_error_handling_policy

            if handling_policy == LogErrorHandlingPolicy.RAISE:
                raise LogContextUnableToLog(exc) from exc

            elif handling_policy == LogErrorHandlingPolicy.PRINT_STDERR:
                if not self._log_error_printed:
                    # not under lock intentionally
                    self._log_error_printed = True
                    _log_internal_error("LogContext.log_event() failed", exc)
            else:
                pass

    def log_debug_event(
        self,
        event: str,
        payload: Mapping[str, Any],
        *,
        event_namespace: str | None = None,
        event_type: str | None = None,
        entity_id: str | None = None,
        source_path: str | None = None,
        source_line: int | None = None,
        source_func: str | None = None,
    ) -> None:
        """
        Emit a structured debug log event.
        """
        self.log_event(
            event=event,
            level=LogLevel.DEBUG,
            payload=payload,
            event_namespace=event_namespace,
            event_type=event_type,
            entity_id=entity_id,
            source_path=source_path,
            source_line=source_line,
            source_func=source_func,
        )

    def log_info_event(
        self,
        event: str,
        payload: Mapping[str, Any],
        *,
        event_namespace: str | None = None,
        event_type: str | None = None,
        entity_id: str | None = None,
        source_path: str | None = None,
        source_line: int | None = None,
        source_func: str | None = None,
    ) -> None:
        """
        Emit a structured info log event.
        """
        self.log_event(
            event=event,
            level=LogLevel.INFO,
            payload=payload,
            event_namespace=event_namespace,
            event_type=event_type,
            entity_id=entity_id,
            source_path=source_path,
            source_line=source_line,
            source_func=source_func,
        )

    def log_warning_event(
        self,
        event: str,
        payload: Mapping[str, Any],
        *,
        event_namespace: str | None = None,
        event_type: str | None = None,
        entity_id: str | None = None,
        source_path: str | None = None,
        source_line: int | None = None,
        source_func: str | None = None,
    ) -> None:
        """
        Emit a structured warning log event.
        """
        self.log_event(
            event=event,
            level=LogLevel.WARNING,
            payload=payload,
            event_namespace=event_namespace,
            event_type=event_type,
            entity_id=entity_id,
            source_path=source_path,
            source_line=source_line,
            source_func=source_func,
        )

    def log_error_event(
        self,
        event: str,
        payload: Mapping[str, Any],
        *,
        event_namespace: str | None = None,
        event_type: str | None = None,
        entity_id: str | None = None,
        source_path: str | None = None,
        source_line: int | None = None,
        source_func: str | None = None,
    ) -> None:
        """
        Emit a structured error log event.
        """
        self.log_event(
            event=event,
            level=LogLevel.ERROR,
            payload=payload,
            event_namespace=event_namespace,
            event_type=event_type,
            entity_id=entity_id,
            source_path=source_path,
            source_line=source_line,
            source_func=source_func,
        )

    # ---- Error handlers ------------------------------------------------------------------

    def build_error_payload(self, err: BaseException) -> Mapping[str, Any]:
        """
        Normalize an exception into a serializable payload.

        Rules:
          - If the exception provides a callable `to_log_payload()` method, use it.
          - Otherwise:
              * try to surface `code` and `code_desc` attributes if present;
              * always include `kind` and `message`.

        This keeps RedisAdapterError and similar exceptions compatible without
        importing them directly.
        """
        # This implementation does not use self, but subclasses may.
        _ = self

        # Duck-typing: Error should expose to_log_payload()
        to_log_payload = getattr(err, "to_log_payload", None)

        if callable(to_log_payload):
            # noinspection PyBroadException
            try:
                provided_payload = to_log_payload()
                if isinstance(provided_payload, dict):
                    return dict(provided_payload)
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
        # This implementation does not use self, but subclasses may.
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
        # This implementation does not use self, but subclasses may.
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
            log_adapter_resolver=self.log_adapter_resolver,
            verbosity_level=self.verbosity_level,
            max_items=self.max_items if not unbounded else None,
            max_str_len=self.max_str_len,
        )

    def normalize_primitive_for_log(self, value: Any) -> str | int | float | bytes | bool | None:
        """
        Normalize primitive values for logging.

        This is a thin wrapper around payload_helpers.normalize_primitive.
        """
        return normalize_primitive(
            value,
            max_str_len=self.max_str_len,
        )

    def normalize_list_for_log(
        self,
        value: Any,
        *,
        unbounded: bool = False,
    ) -> str | list[Any]:
        """
        Normalize a list/tuple for logging, one level deep.

        This is a thin wrapper around payload_helpers.normalize_list_for_log.
        """
        return normalize_list_for_log(
            value,
            log_adapter_resolver=self.log_adapter_resolver,
            verbosity_level=self.verbosity_level,
            max_items=self.max_items if not unbounded else None,
            max_str_len=self.max_str_len,
        )

    def normalize_dict_for_log(
        self,
        value: Any,
        *,
        unbounded: bool = False,
    ) -> str | dict[str, Any]:
        """
        Normalize a dict for logging, one level deep.

        This is a thin wrapper around payload_helpers.normalize_dict_for_log.
        """
        return normalize_dict_for_log(
            value,
            log_adapter_resolver=self.log_adapter_resolver,
            verbosity_level=self.verbosity_level,
            max_items=self.max_items if not unbounded else None,
            max_str_len=self.max_str_len,
        )
