# src/mvx/common/logger/__init__.py
from __future__ import annotations

from typing import Any
from dataclasses import dataclass

import threading
import re

from .models import (
    LogLevel,
    LogPayloadProvider,
    LogAdapter,
    LogAdapterResolver,
    LogEvent,
    LogSinkProto,
    LogSinkDescriptor,
    LogSinkTerminator,
    LogSinkClassProto,
    LogEventPolicy,
)
from .helpers import log_internal_error as _log_internal_error

from .errors import (
    LoggerError,
    LogContextError,
    LogContextResetError,
    LogContextUnableToLog,
    LogSinkConfigurationError,
    LogSinkConfigurationConflictError,
    LogSinkDescriptorBuildError,
    LogSinkCreateError,
    LogSinkCloseError,
    LogSinkIsInUseError,
)

from .log_context import (
    LogContext,
    LogVerbosityLevel,
    LogErrorHandlingPolicy,
)

from .log_components import (
    log_invocation,
    LogContextProto,
    LogContextProviderProto,
    LogEntityIdProviderProto,
)

from .asyncio_log_sink import (
    AsyncioLogSinkState,
    AsyncioLogSinkQueueOverflowPolicy,
    AsyncioLogSinkOp,
    AsyncioLogSinkOpResult,
    AsyncioLogSinkWaitHandle,
    AsyncioLogSink,
    AsyncioLogSinkError,
    AsyncioLogSinkErrorReason,
    AsyncioLogSinkEventLoopUnavailableError,
    AsyncioLogSinkInvalidStateError,
    AsyncioLogSinkOnStartingHookFailedError,
    AsyncioLogSinkOnStoppedHookFailedError,
    AsyncioLogSinkQueueOverflowError,
    AsyncioLogSinkDispatcherCancelledError,
    AsyncioLogSinkUnexpectedError,
)

from .adapter_logging import (
    LogStreamOutput,
    LoggingStreamConfig,
    LoggingFileConfig,
    StreamLogSink,
    FileLogSink,
)

__all__ = (
    # from .models
    "LogLevel",
    "LogPayloadProvider",
    "LogAdapter",
    "LogAdapterResolver",
    "LogEvent",
    "LogSinkProto",
    "LogSinkDescriptor",
    "LogSinkTerminator",
    "LogSinkClassProto",
    "LogEventPolicy",
    # from .errors
    "LoggerError",
    "LogContextError",
    "LogContextResetError",
    "LogContextUnableToLog",
    "LogSinkConfigurationError",
    "LogSinkConfigurationConflictError",
    "LogSinkDescriptorBuildError",
    "LogSinkCreateError",
    "LogSinkCloseError",
    "LogSinkIsInUseError",
    # from .log_context
    "LogContext",
    "LogVerbosityLevel",
    "LogErrorHandlingPolicy",
    # from .log_components
    "log_invocation",
    "LogContextProto",
    "LogContextProviderProto",
    "LogEntityIdProviderProto",
    # from .asyncio_log_sink
    "AsyncioLogSinkState",
    "AsyncioLogSinkQueueOverflowPolicy",
    "AsyncioLogSinkOp",
    "AsyncioLogSinkOpResult",
    "AsyncioLogSinkWaitHandle",
    "AsyncioLogSink",
    "AsyncioLogSinkError",
    "AsyncioLogSinkErrorReason",
    "AsyncioLogSinkEventLoopUnavailableError",
    "AsyncioLogSinkInvalidStateError",
    "AsyncioLogSinkOnStartingHookFailedError",
    "AsyncioLogSinkOnStoppedHookFailedError",
    "AsyncioLogSinkQueueOverflowError",
    "AsyncioLogSinkDispatcherCancelledError",
    "AsyncioLogSinkUnexpectedError",
    # from .adapter_logging
    "LogStreamOutput",
    "LoggingStreamConfig",
    "LoggingFileConfig",
    "StreamLogSink",
    "FileLogSink",
    # public API
    "configure_log_sink",
    "get_log_sink",
    "get_configured_log_sink_names",
    "has_configured_log_sinks",
    "close_log_sink",
    "get_root_log_context",
    "get_log_context",
    "configure_log_context",
    "get_log_context_namespaces",
    "has_log_context",
    "reset_log_contexts",
    "reset_logger",
)

ROOT_LOG_CONTEXT_NAMESPACE = ""

DEFAULT_STDERR_LOG_SINK_NAME = "stderr"
DEFAULT_ROOT_LOG_SINK_NAME = DEFAULT_STDERR_LOG_SINK_NAME


# ---- Validators --------------------------------------------------------------------------

_NAMESPACE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*(?:\.[A-Za-z][A-Za-z0-9_-]*)*$")
_LOG_SINK_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")


def _validate_log_sink_name(arg_name: str, log_sink_name: str) -> str:
    if not isinstance(log_sink_name, str):
        raise TypeError(f"argument '{arg_name}' must be a string")

    if _LOG_SINK_NAME_RE.fullmatch(log_sink_name) is None:
        raise ValueError(f"argument '{arg_name}' is malformed: '{log_sink_name}'")

    return log_sink_name


def _validate_namespace(arg_name: str, namespace: str) -> str:
    if not isinstance(namespace, str):
        raise TypeError(f"argument '{arg_name}' must be a string")

    if not namespace.strip():
        raise ValueError(f"argument '{arg_name}' must not be empty")

    if _NAMESPACE_RE.fullmatch(namespace) is None:
        raise ValueError(f"argument '{arg_name}' is malformed: '{namespace}'")

    return namespace


# ---- Internal sink registry --------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _RegisteredLogSink:
    sink: LogSinkProto
    terminator: LogSinkTerminator
    descriptor: LogSinkDescriptor


class _LogSinkRegistry:

    def __init__(self) -> None:
        self._lifecycle_lock = threading.RLock()
        self._registry_lock = threading.RLock()
        self._registered_sinks: dict[str, _RegisteredLogSink] = {}

    def register(
        self,
        *,
        name: str,
        sink_cls: LogSinkClassProto,
        **sink_kwargs: Any,
    ) -> LogSinkProto:

        with self._lifecycle_lock:
            try:
                descriptor = sink_cls.build_descriptor(**sink_kwargs)
            except Exception as exc:
                raise LogSinkDescriptorBuildError(
                    sink_name=name,
                    sink_class=sink_cls,
                    cause=exc,
                ) from exc

            with self._registry_lock:
                existing = self._registered_sinks.get(name)
                if existing is not None:
                    if existing.descriptor == descriptor:
                        return existing.sink

                    raise LogSinkConfigurationConflictError(
                        sink_name=name,
                        existing_descriptor=existing.descriptor,
                        requested_descriptor=descriptor,
                    )

            try:
                sink, terminator = sink_cls.create(**sink_kwargs)
            except Exception as exc:
                raise LogSinkCreateError(
                    sink_name=name,
                    sink_class=sink_cls,
                    cause=exc,
                ) from exc

            with self._registry_lock:
                self._registered_sinks[name] = _RegisteredLogSink(
                    sink=sink,
                    terminator=terminator,
                    descriptor=descriptor,
                )

            return sink

    def get(self, name: str) -> LogSinkProto | None:
        with self._registry_lock:
            registered = self._registered_sinks.get(name)
            if registered is not None:
                return registered.sink
        return None

    def get_sinks_names(self) -> tuple[str, ...]:
        with self._registry_lock:
            return tuple(self._registered_sinks.keys())

    def is_empty(self) -> bool:
        with self._registry_lock:
            return len(self._registered_sinks) == 0

    def reset(self) -> None:
        with self._lifecycle_lock:
            with self._registry_lock:
                registered_sinks = tuple(self._registered_sinks.items())
                self._registered_sinks.clear()

        errors_list: list[tuple[str, Exception]] = []

        for sink_name, registered_sink in reversed(registered_sinks):
            # noinspection PyBroadException
            try:
                registered_sink.terminator()
            except Exception as exc:
                errors_list.append((sink_name, exc))

        if errors_list:
            raise LogSinkCloseError(causes=tuple(errors_list))

    def unregister(self, name: str) -> bool:
        with self._lifecycle_lock:
            with self._registry_lock:
                registered = self._registered_sinks.pop(name, None)

        if registered is None:
            return False

        try:
            registered.terminator()
        except Exception as exc:
            raise LogSinkCloseError(causes=((name, exc),)) from exc

        return True


# ---- Internal log context registry -------------------------------------------------------


class _LogContextRegistry:
    def __init__(self, root_log_context: LogContext) -> None:
        self._lock = threading.RLock()
        self._root_log_context = root_log_context
        self._contexts: dict[str, LogContext] = {ROOT_LOG_CONTEXT_NAMESPACE: root_log_context}

    def get_root_log_context(self) -> LogContext:
        return self._root_log_context

    def get(self, namespace: str) -> LogContext | None:
        with self._lock:
            return self._contexts.get(namespace)

    def put(self, context: LogContext) -> LogContext:
        with self._lock:
            existing = self._contexts.get(context.namespace)
            if existing is not None:
                return existing

            self._contexts[context.namespace] = context
            return context

    def contains(self, namespace: str) -> bool:
        with self._lock:
            return namespace in self._contexts

    def list_namespaces(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(self._contexts.keys())

    def clear(self) -> None:
        with self._lock:
            self._contexts = {ROOT_LOG_CONTEXT_NAMESPACE: self._root_log_context}

    def get_contexts_by_log_sink(self, log_sink: LogSinkProto) -> tuple[LogContext, ...]:
        with self._lock:
            result: list[LogContext] = []
            for context in self._contexts.values():
                if context.get_local_log_sink() is log_sink:
                    result.append(context)

            return tuple(result)

    def create_log_context_chain(
        self,
        namespace: str,
        *,
        log_sink: LogSinkProto | None = None,
        event_policy: LogEventPolicy | None = None,
        verbosity_level: LogVerbosityLevel | None = None,
        max_str_len: int | None = None,
        max_items: int | None = None,
        log_error_handling_policy: LogErrorHandlingPolicy | None = None,
    ) -> LogContext:

        def _iter_context_chain(_namespace: str) -> tuple[str, ...]:
            """
            Build cumulative context namespaces from a dotted namespace.

            Example:
                "mvx.ldap.schema" ->
                ("mvx", "mvx.ldap", "mvx.ldap.schema")
            """
            parts = _namespace.split(".")
            result: list[str] = []

            for index in range(1, len(parts) + 1):
                result.append(".".join(parts[:index]))

            return tuple(result)

        parent = self.get_root_log_context()

        if namespace == ROOT_LOG_CONTEXT_NAMESPACE:
            return parent

        for current_namespace in _iter_context_chain(namespace):
            existing = self.get(current_namespace)
            if existing is not None:
                parent = existing
                continue

            is_leaf = current_namespace == namespace

            context = LogContext(
                namespace=current_namespace,
                parent=parent,
                log_sink=log_sink if is_leaf else None,
                event_policy=event_policy if is_leaf else None,
                verbosity_level=verbosity_level if is_leaf else None,
                max_str_len=max_str_len if is_leaf else None,
                max_items=max_items if is_leaf else None,
                log_error_handling_policy=log_error_handling_policy if is_leaf else None,
            )

            parent = self.put(context)

        return parent


_log_context_wiring_lock = threading.RLock()


# ---- Boot strap wiring -------------------------------------------------------------------


def _bootstrap() -> tuple[_LogSinkRegistry, _LogContextRegistry]:
    log_sink_registry = _LogSinkRegistry()

    try:
        log_sink = log_sink_registry.register(
            name=DEFAULT_ROOT_LOG_SINK_NAME,
            sink_cls=StreamLogSink,
        )
        root_ctx = LogContext(
            namespace=ROOT_LOG_CONTEXT_NAMESPACE,
            log_sink=log_sink,
            verbosity_level=LogVerbosityLevel.NORMAL,
        )

        log_context_registry = _LogContextRegistry(root_ctx)

        return log_sink_registry, log_context_registry

    except Exception as exc:
        _log_internal_error("logger bootstrap failed", exc)
        raise


_log_sink_registry, _log_context_registry = _bootstrap()


# ---- Public API --------------------------------------------------------------------------


def configure_log_sink(
    *,
    name: str,
    sink_cls: LogSinkClassProto,
    **sink_kwargs: Any,
) -> LogSinkProto:
    _validate_log_sink_name("name", name)
    with _log_context_wiring_lock:
        return _log_sink_registry.register(name=name, sink_cls=sink_cls, **sink_kwargs)


def get_log_sink(name: str) -> LogSinkProto | None:
    _validate_log_sink_name("name", name)
    with _log_context_wiring_lock:
        return _log_sink_registry.get(name=name)


def get_configured_log_sink_names() -> tuple[str, ...]:
    with _log_context_wiring_lock:
        return _log_sink_registry.get_sinks_names()


def has_configured_log_sinks() -> bool:
    with _log_context_wiring_lock:
        return not _log_sink_registry.is_empty()


def close_log_sink(name: str) -> bool:
    _validate_log_sink_name("name", name)

    with _log_context_wiring_lock:
        log_sink = _log_sink_registry.get(name)
        if log_sink is None:
            return False

        contexts = _log_context_registry.get_contexts_by_log_sink(log_sink)

        if contexts:
            namespaces: list[str] = []
            for context in contexts:
                namespaces.append("<root>" if context.is_root else context.namespace)

            raise LogSinkIsInUseError(
                sink_name=name,
                context_namespaces=tuple(namespaces),
            )

        return _log_sink_registry.unregister(name)


def get_root_log_context() -> LogContext:
    with _log_context_wiring_lock:
        return _log_context_registry.get_root_log_context()


def get_log_context(
    namespace: str,
) -> LogContext | None:

    _validate_namespace("namespace", namespace)

    with _log_context_wiring_lock:
        return _log_context_registry.get(namespace)


def configure_log_context(
    namespace: str,
    *,
    log_sink: LogSinkProto | None = None,
    event_policy: LogEventPolicy | None = None,
    verbosity_level: LogVerbosityLevel | None = None,
    max_str_len: int | None = None,
    max_items: int | None = None,
    log_error_handling_policy: LogErrorHandlingPolicy | None = None,
) -> LogContext:

    _validate_namespace("namespace", namespace)

    with _log_context_wiring_lock:
        existing = _log_context_registry.get(namespace)

        if existing is not None:
            if log_sink is not None:
                existing.set_log_sink(log_sink)
            if event_policy is not None:
                existing.set_event_policy(event_policy)
            if verbosity_level is not None:
                existing.set_verbosity_level(verbosity_level)
            if max_str_len is not None:
                existing.set_max_str_len(max_str_len)
            if max_items is not None:
                existing.set_max_items(max_items)
            if log_error_handling_policy is not None:
                existing.set_log_error_handling_policy(log_error_handling_policy)

            return existing

        return _log_context_registry.create_log_context_chain(
            namespace,
            log_sink=log_sink,
            event_policy=event_policy,
            verbosity_level=verbosity_level,
            max_str_len=max_str_len,
            max_items=max_items,
            log_error_handling_policy=log_error_handling_policy,
        )


def get_log_context_namespaces() -> tuple[str, ...]:
    with _log_context_wiring_lock:
        return _log_context_registry.list_namespaces()


def has_log_context(namespace: str) -> bool:
    _validate_namespace("namespace", namespace)

    with _log_context_wiring_lock:
        return _log_context_registry.contains(namespace)


def reset_log_contexts() -> None:
    with _log_context_wiring_lock:
        _log_context_registry.clear()


def reset_logger() -> None:
    global _log_sink_registry, _log_context_registry

    with _log_context_wiring_lock:
        _log_sink_registry.reset()
        _log_sink_registry, _log_context_registry = _bootstrap()
