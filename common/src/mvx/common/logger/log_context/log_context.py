# src/mvx/common/logger/log_context/log_context.py
from __future__ import annotations

from mvx.common.helpers.document_enum import document_enum

from typing import Mapping, Any, overload

from enum import StrEnum
import time
import threading


from ..models import (
    LogLevel,
    LogSinkProto,
    LogEventPolicyProto,
    LogEventMeta,
    LogEvent,
    LogPayloadProcessorProto,
)

from ..helpers import log_internal_error as _log_internal_error

from ..errors import LogContextResetError, LogContextUnableToLog

__all__ = ("LogContext", "LogErrorHandlingPolicy")


@document_enum
class LogErrorHandlingPolicy(StrEnum):
    """
    Policy for handling logging infrastructure failures inside `LogContext`.

    This policy is applied when a prepared `LogEvent` cannot be delivered
    through the resolved sink.
    """

    #: Suppress sink delivery errors.
    IGNORE = "IGNORE"

    #: Report sink delivery errors through the last-resort stderr path.
    PRINT_STDERR = "PRINT_STDERR"

    #: Raise `LogContextUnableToLog`.
    RAISE = "RAISE"


ERR_LOGGED_FLAG = "_mvx_error_logged"


class LogContext:
    """
    Structured logging context for a namespace.

    `LogContext` is the object-level coordinator of the logger pipeline. It
    combines event metadata construction, event policy checks, payload
    normalization, and sink delivery.

    A context may be used directly or through the package-level facade.
    """

    @overload
    def __init__(
        self,
        *,
        namespace: str | None = None,
        parent: None = None,
        log_sink: LogSinkProto,
        event_policy: LogEventPolicyProto | None = None,
        payload_processor: LogPayloadProcessorProto,
        log_error_handling_policy: LogErrorHandlingPolicy | None = None,
    ): ...
    @overload
    def __init__(
        self,
        *,
        namespace: str | None = None,
        parent: LogContext,
        log_sink: LogSinkProto | None = None,
        event_policy: LogEventPolicyProto | None = None,
        payload_processor: LogPayloadProcessorProto | None = None,
        log_error_handling_policy: LogErrorHandlingPolicy | None = None,
    ): ...

    def __init__(
        self,
        *,
        namespace: str | None = None,
        parent: LogContext | None = None,
        log_sink: LogSinkProto | None = None,
        event_policy: LogEventPolicyProto | None = None,
        payload_processor: LogPayloadProcessorProto | None = None,
        log_error_handling_policy: LogErrorHandlingPolicy | None = None,
    ):
        """
        Create a root or child log context.

        A root context has no parent and must receive both `log_sink` and
        `payload_processor`.

        A child context has a parent and may define local overrides. Missing inherited
        components are resolved from the parent.

        :param namespace: optional context name.
        :param parent: parent context, or None for a root context.
        :param log_sink: local sink. Required for a root context.
        :param event_policy: optional local event policy.
        :param payload_processor: local payload processor. Required for a root context.
        :param log_error_handling_policy: optional local policy for logging
            infrastructure failures. Root contexts default to `PRINT_STDERR`.
        :raises TypeError: if an argument has an invalid type.
        :raises ValueError: if a root context is missing required infrastructure.
        """
        if namespace is not None:
            if not isinstance(namespace, str):
                raise TypeError("argument 'namespace' must be string")

            namespace = namespace.strip()

        if parent is None:
            if log_sink is None:
                raise ValueError(
                    "argument 'log_sink' is mandatory for the root log context, must not be None"
                )

            if payload_processor is None:
                raise ValueError(
                    "argument 'payload_processor' is mandatory for the root log context, must not be None"
                )

        else:
            if not isinstance(parent, LogContext):
                raise TypeError("argument 'parent' must be an instance of 'LogContext'")

        if log_sink is not None:
            if not isinstance(log_sink, LogSinkProto):
                raise TypeError("argument 'log_sink' must be an instance of 'LogSinkProto'")

        if event_policy is not None:
            if not isinstance(event_policy, LogEventPolicyProto):
                raise TypeError("argument 'event_policy' must be an instance of 'LogEventPolicy'")

        if payload_processor is not None:
            if not isinstance(payload_processor, LogPayloadProcessorProto):
                raise TypeError(
                    "argument 'payload_processor' must be an instance of 'LogPayloadProcessorProto'"
                )

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

        self._payload_processor = payload_processor

        if parent is None:
            log_error_handling_policy = (
                log_error_handling_policy
                if log_error_handling_policy is not None
                else LogErrorHandlingPolicy.PRINT_STDERR
            )

        self._log_error_handling_policy = log_error_handling_policy
        self._log_error_printed = False

    # ---- Properties ----------------------------------------------------------------------

    @property
    def namespace(self) -> str:
        """
        Return the context namespace.

        If no namespace was configured, returns ``"<not defined>"``.

        :return: context namespace.
        """
        return self._namespace if self._namespace is not None else "<not defined>"

    @property
    def is_root(self) -> bool:
        """
        Return whether this context is the root context.

        :return: True if the context has no parent, False otherwise.
        """
        return bool(self._parent is None)

    @property
    def parent(self) -> LogContext | None:
        """
        Return the parent context.

        :return: parent context, or None for a root context.
        """
        return self._parent

    @property
    def log_sink(self) -> LogSinkProto:
        """
        Return the effective sink for this context.

        If a local sink is configured, it is returned. Otherwise, the sink is resolved
        from the parent context.

        :return: effective sink.
        """
        with self._config_lock:
            if self._log_sink is not None:
                return self._log_sink

            assert self._parent is not None, "invariant: controlled by constructor"
            return self._parent.log_sink

    def set_log_sink(self, log_sink: LogSinkProto) -> None:
        """
        Set the local sink for this context.

        :param log_sink: sink to assign locally.
        :return: None.
        :raises ValueError: if `log_sink` is None.
        :raises TypeError: if `log_sink` does not implement `LogSinkProto`.
        """
        if log_sink is None:
            raise ValueError("argument 'log_sink' must not be None")

        if not isinstance(log_sink, LogSinkProto):
            raise TypeError("argument 'log_sink' must be an instance of 'LogSinkProto'")

        with self._config_lock:
            self._log_sink = log_sink

    def reset_log_sink(self) -> None:
        """
        Remove the local sink override.

        After reset, a child context resolves its sink from the parent context.

        :return: None.
        :raises LogContextResetError: if called on a root context.
        """
        if self.is_root:
            raise LogContextResetError(
                target="log_sink",
            )

        with self._config_lock:
            self._log_sink = None

    @property
    def event_policy(self) -> LogEventPolicyProto | None:
        """
        Return the local event policy.

        Event policy is not inherited from the parent context. If no local policy is
        configured, returns None.

        :return: local event policy, or None.
        """
        with self._config_lock:
            return self._event_policy

    def set_event_policy(self, event_policy: LogEventPolicyProto) -> None:
        """
        Set the local event policy.

        :param event_policy: event policy to assign locally.
        :return: None.
        :raises ValueError: if `event_policy` is None.
        :raises TypeError: if `event_policy` does not implement
            `LogEventPolicyProto`.
        """
        if event_policy is None:
            raise ValueError("argument 'event_policy' must not be None")

        if not isinstance(event_policy, LogEventPolicyProto):
            raise TypeError("argument 'event_policy' must be an instance of 'LogEventPolicy'")

        with self._config_lock:
            self._event_policy = event_policy

    def reset_event_policy(self) -> None:
        """
        Remove the local event policy.

        After reset, events emitted through this context are enabled by default.

        :return: None.
        """
        with self._config_lock:
            self._event_policy = None

    @property
    def payload_processor(self) -> LogPayloadProcessorProto:
        """
        Return the effective payload processor.

        If a local processor is configured, it is returned. Otherwise, the processor is
        resolved from the parent context.

        :return: effective payload processor.
        """
        with self._config_lock:
            processor = self._payload_processor
            if processor is not None:
                return processor

            assert self._parent is not None, "invariant: controlled by constructor"
            return self._parent.payload_processor

    def set_payload_processor(self, payload_processor: LogPayloadProcessorProto) -> None:
        """
        Set the local payload processor.

        :param payload_processor: payload processor to assign locally.
        :return: None.
        :raises ValueError: if `payload_processor` is None.
        :raises TypeError: if `payload_processor` does not implement
            `LogPayloadProcessorProto`.
        """
        if payload_processor is None:
            raise ValueError("argument 'payload_processor' must not be None")

        if not isinstance(payload_processor, LogPayloadProcessorProto):
            raise TypeError(
                "argument 'payload_processor' must be an instance of 'LogPayloadProcessorProto'"
            )

        with self._config_lock:
            self._payload_processor = payload_processor

    def reset_payload_processor(self) -> None:
        """
        Remove the local payload processor override.

        After reset, a child context resolves its payload processor from the parent
        context.

        :return: None.
        :raises LogContextResetError: if called on a root context.
        """
        if self.is_root:
            raise LogContextResetError(
                target="payload_processor",
            )

        with self._config_lock:
            self._payload_processor = None

    @property
    def log_error_handling_policy(self) -> LogErrorHandlingPolicy:
        """
        Return the effective logging error handling policy.

        If a local policy is configured, it is returned. Otherwise, the policy is
        resolved from the parent context.

        :return: effective logging error handling policy.
        """
        with self._config_lock:
            if self._log_error_handling_policy is not None:
                return self._log_error_handling_policy

            assert self._parent is not None, "invariant: controlled by constructor"
            return self._parent.log_error_handling_policy

    def set_log_error_handling_policy(
        self, log_error_handling_policy: LogErrorHandlingPolicy
    ) -> None:
        """
        Set the local logging error handling policy.

        :param log_error_handling_policy: policy to assign locally.
        :return: None.
        :raises ValueError: if `log_error_handling_policy` is None.
        :raises TypeError: if `log_error_handling_policy` is not a
            `LogErrorHandlingPolicy`.
        """
        if log_error_handling_policy is None:
            raise ValueError("argument 'log_error_handling_policy' must not be None")

        if not isinstance(log_error_handling_policy, LogErrorHandlingPolicy):
            raise TypeError(
                "argument 'log_error_handling_policy' must be an instance of 'LogErrorHandlingPolicy'"
            )

        with self._config_lock:
            self._log_error_handling_policy = log_error_handling_policy

    def reset_log_error_handling_policy(self) -> None:
        """
        Remove the local logging error handling policy override.

        After reset, a child context resolves this policy from the parent context.

        :return: None.
        :raises LogContextResetError: if called on a root context.
        """
        if self.is_root:
            raise LogContextResetError(
                target="log_error_handling_policy",
            )
        with self._config_lock:
            self._log_error_handling_policy = None

    def get_local_log_sink(self) -> LogSinkProto | None:
        """
        Return the locally configured sink.

        This method does not resolve inherited sinks.

        :return: local sink, or None if no local sink is configured.
        """
        with self._config_lock:
            return self._log_sink

    # ---- Logging events ------------------------------------------------------------------

    def is_event_enabled(self, event: LogEventMeta) -> bool:
        """
        Return whether an event is enabled for this context.

        Only the local event policy is used. If no local event policy is configured,
        the event is enabled.

        :param event: event metadata to evaluate.
        :return: True if the event is enabled, False otherwise.
        """
        with self._config_lock:
            event_policy = self._event_policy

        if event_policy is None:
            return True

        return event_policy.is_event_enabled(event)

    def emit_log_event(
        self,
        event: LogEvent,
    ) -> None:
        """
        Emit a fully prepared log event through the effective sink.

        This method does not apply event policy and does not normalize payload data.
        The caller is responsible for providing a prepared `LogEvent`.

        Sink delivery failures are handled according to the effective
        `LogErrorHandlingPolicy`.

        :param event: prepared event to emit.
        :return: None.
        :raises LogContextUnableToLog: if sink delivery fails and the effective error
            handling policy is `RAISE`.
        """
        # noinspection PyBroadException
        try:
            self.log_sink.log(event)
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
                    _log_internal_error("LogContext log event failed", exc)
            else:
                pass

    def log_event(
        self,
        event: str,
        level: LogLevel,
        payload: Mapping[str, Any],
        *,
        event_namespace: str | None = None,
        event_outcome: str | None = None,
        entity_id: str | None = None,
        source_path: str | None = None,
        source_line: int | None = None,
        source_func: str | None = None,
        skip_payload_normalization: bool = False,
    ) -> None:
        """
        Build and emit a structured log event.

        The method builds `LogEventMeta`, checks the local event policy, normalizes the
        payload unless requested otherwise, creates `LogEvent`, and emits it through
        the effective sink.

        :param event: stable event name.
        :param level: event severity level.
        :param payload: structured event payload.
        :param event_namespace: optional namespace override. If omitted, the context
            namespace is used.
        :param event_outcome: optional event outcome or phase.
        :param entity_id: optional related entity identifier.
        :param source_path: optional source file path.
        :param source_line: optional source line.
        :param source_func: optional source function name.
        :param skip_payload_normalization: if True, use the payload as-is.
        :return: None.
        :raises LogContextUnableToLog: if sink delivery fails and the effective error
            handling policy is `RAISE`.
        """
        log_event_meta = LogEventMeta(
            event_namespace=event_namespace if event_namespace is not None else self.namespace,
            event_name=event,
            entity_id=entity_id,
            source_path=source_path,
            source_line=source_line,
            source_func=source_func,
        )

        if not self.is_event_enabled(log_event_meta):
            return

        payload_for_log = (
            self.payload_processor.normalize_payload(payload)
            if not skip_payload_normalization
            else payload
        )

        log_event = LogEvent(
            meta=log_event_meta,
            level=level,
            event_outcome=event_outcome,
            timestamp=time.time(),
            payload=payload_for_log,
        )

        self.emit_log_event(log_event)

    def log_debug_event(
        self,
        event: str,
        payload: Mapping[str, Any],
        *,
        event_namespace: str | None = None,
        event_outcome: str | None = None,
        entity_id: str | None = None,
        source_path: str | None = None,
        source_line: int | None = None,
        source_func: str | None = None,
        skip_payload_normalization: bool = False,
    ) -> None:
        """
        Emit a structured debug-level event.

        This is a convenience wrapper around `log_event()` with `LogLevel.DEBUG`.

        :return: None.
        """
        self.log_event(
            event=event,
            level=LogLevel.DEBUG,
            payload=payload,
            event_namespace=event_namespace,
            event_outcome=event_outcome,
            entity_id=entity_id,
            source_path=source_path,
            source_line=source_line,
            source_func=source_func,
            skip_payload_normalization=skip_payload_normalization,
        )

    def log_info_event(
        self,
        event: str,
        payload: Mapping[str, Any],
        *,
        event_namespace: str | None = None,
        event_outcome: str | None = None,
        entity_id: str | None = None,
        source_path: str | None = None,
        source_line: int | None = None,
        source_func: str | None = None,
        skip_payload_normalization: bool = False,
    ) -> None:
        """
        Emit a structured info-level event.

        This is a convenience wrapper around `log_event()` with `LogLevel.INFO`.

        :return: None.
        """
        self.log_event(
            event=event,
            level=LogLevel.INFO,
            payload=payload,
            event_namespace=event_namespace,
            event_outcome=event_outcome,
            entity_id=entity_id,
            source_path=source_path,
            source_line=source_line,
            source_func=source_func,
            skip_payload_normalization=skip_payload_normalization,
        )

    def log_warning_event(
        self,
        event: str,
        payload: Mapping[str, Any],
        *,
        event_namespace: str | None = None,
        event_outcome: str | None = None,
        entity_id: str | None = None,
        source_path: str | None = None,
        source_line: int | None = None,
        source_func: str | None = None,
        skip_payload_normalization: bool = False,
    ) -> None:
        """
        Emit a structured warning-level event.

        This is a convenience wrapper around `log_event()` with `LogLevel.WARNING`.

        :return: None.
        """
        self.log_event(
            event=event,
            level=LogLevel.WARNING,
            payload=payload,
            event_namespace=event_namespace,
            event_outcome=event_outcome,
            entity_id=entity_id,
            source_path=source_path,
            source_line=source_line,
            source_func=source_func,
            skip_payload_normalization=skip_payload_normalization,
        )

    def log_error_event(
        self,
        event: str,
        payload: Mapping[str, Any],
        *,
        event_namespace: str | None = None,
        event_outcome: str | None = None,
        entity_id: str | None = None,
        source_path: str | None = None,
        source_line: int | None = None,
        source_func: str | None = None,
        skip_payload_normalization: bool = False,
    ) -> None:
        """
        Emit a structured error-level event.

        This is a convenience wrapper around `log_event()` with `LogLevel.ERROR`.

        :return: None.
        """
        self.log_event(
            event=event,
            level=LogLevel.ERROR,
            payload=payload,
            event_namespace=event_namespace,
            event_outcome=event_outcome,
            entity_id=entity_id,
            source_path=source_path,
            source_line=source_line,
            source_func=source_func,
            skip_payload_normalization=skip_payload_normalization,
        )

    # ---- Error handlers ------------------------------------------------------------------

    def build_error_payload(self, err: BaseException) -> Mapping[str, Any]:
        """
        Build a structured logging payload for an exception.

        If the exception provides a callable `to_log_payload()` method returning a
        dictionary, that dictionary is used. Otherwise, the method builds a generic
        payload from available `code`, `code_desc`, exception kind, and message.

        :param err: exception instance to describe.
        :return: structured error payload.
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
        Return whether the exception instance is marked as already logged.

        This is a best-effort marker used to suppress repeated detailed error payloads
        for the same exception instance.

        :param err: exception instance to check.
        :return: True if the exception is marked as logged, False otherwise.
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
        Mark the exception instance as already logged.

        The marker is best-effort. If the exception object does not allow setting
        custom attributes, the method silently does nothing.

        :param err: exception instance to mark.
        :return: None.
        """
        # This implementation does not use self, but subclasses may.
        _ = self

        # noinspection PyBroadException
        try:
            setattr(err, ERR_LOGGED_FLAG, True)
        except Exception:
            # Best effort: ignore if we cannot set the flag
            pass

    # ---- Processing payload --------------------------------------------------------------

    def normalize_payload(
        self,
        payload: Mapping[str, Any],
        *,
        unbounded: bool = False,
    ) -> dict[str, Any]:
        """
        Normalize a structured payload through the effective payload processor.

        :param payload: payload mapping to normalize.
        :param unbounded: whether item-count limiting should be disabled while
            normalizing this payload.
        :return: normalized payload dictionary.
        """
        return self.payload_processor.normalize_payload(payload, unbounded=unbounded)

    def normalize_value_for_log(
        self,
        value: Any,
        *,
        unbounded: bool = False,
    ) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None:
        """
        Normalize a single value through the effective payload processor.

        :param value: value to normalize.
        :param unbounded: whether item-count limiting should be disabled for this
            value.
        :return: normalized log-ready value.
        """
        return self.payload_processor.normalize_value_for_log(value, unbounded=unbounded)

    def get_plain_verbosity_level(self) -> str | None:
        """
        Return the effective payload processor verbosity level as a plain string.

        :return: Plain verbosity level, or None if no verbosity level is available.
        """
        return self.payload_processor.get_plain_verbosity_level()
