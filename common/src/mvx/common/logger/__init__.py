# src/mvx/common/logger/__init__.py
from __future__ import annotations

from typing import Any
from dataclasses import dataclass

import threading
import re

from .models import (
    LogLevel,
    LogPayloadProvider,
    LogEvent,
    LogEventMeta,
    LogSinkProto,
    LogSinkDescriptor,
    LogSinkTerminator,
    LogSinkClassProto,
    LogEventPolicyProto,
    LogPayloadProcessorProto,
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
    LogErrorHandlingPolicy,
)

from .pattern_event_policy import (
    PatternLogEventPolicyAction,
    PatternLogEventPolicyRuleConfig,
    PatternLogEventPolicyConfig,
    PatternLogEventPolicy,
)


from .log_payload_processor import (
    LogPayloadProcessor,
    LogVerbosityLevel,
    LogAdapter,
    LogAdapterResolver,
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
    "LogEvent",
    "LogEventMeta",
    "LogSinkProto",
    "LogSinkDescriptor",
    "LogSinkTerminator",
    "LogSinkClassProto",
    "LogEventPolicyProto",
    "LogPayloadProcessorProto",
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
    "LogErrorHandlingPolicy",
    # from .pattern_event_policy
    "PatternLogEventPolicyAction",
    "PatternLogEventPolicyRuleConfig",
    "PatternLogEventPolicyConfig",
    "PatternLogEventPolicy",
    # from .log_payload_processor
    "LogPayloadProcessor",
    "LogVerbosityLevel",
    "LogAdapter",
    "LogAdapterResolver",
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
        event_policy: LogEventPolicyProto | None = None,
        payload_processor: LogPayloadProcessorProto | None = None,
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

            context: LogContext = LogContext(
                namespace=current_namespace,
                parent=parent,
                log_sink=log_sink if is_leaf else None,
                event_policy=event_policy if is_leaf else None,
                payload_processor=payload_processor if is_leaf else None,
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
            payload_processor=LogPayloadProcessor(),
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
    """
    Configure or retrieve a package-level named log sink.

    If no sink is registered under `name`, the function asks `sink_cls` to build a
    descriptor, creates the sink, stores it in the package-level sink registry, and
    returns it.

    If a sink with the same name and the same descriptor is already registered, the
    existing sink is returned.

    If a sink with the same name but a different descriptor is already registered,
    a configuration conflict is raised.

    :param name: package-level sink name.
    :param sink_cls: sink class implementing the package-managed sink factory
        contract.
    :param sink_kwargs: sink-specific configuration arguments passed to
        `sink_cls.build_descriptor()` and `sink_cls.create()`.
    :return: configured log sink.
    :raises TypeError: if `name` is not a string.
    :raises ValueError: if `name` is malformed.
    :raises LogSinkDescriptorBuildError: if descriptor construction fails.
    :raises LogSinkCreateError: if sink creation fails.
    :raises LogSinkConfigurationConflictError: if the name is already registered
        with a different descriptor.
    """
    _validate_log_sink_name("name", name)
    with _log_context_wiring_lock:
        return _log_sink_registry.register(name=name, sink_cls=sink_cls, **sink_kwargs)


def get_log_sink(name: str) -> LogSinkProto | None:
    """
    Return a package-level sink by name.

    :param name: package-level sink name.
    :return: registered sink, or None if no sink is registered under this name.
    :raises TypeError: if `name` is not a string.
    :raises ValueError: if `name` is malformed.
    """
    _validate_log_sink_name("name", name)
    with _log_context_wiring_lock:
        return _log_sink_registry.get(name=name)


def get_configured_log_sink_names() -> tuple[str, ...]:
    """
    Return names of package-level configured sinks.

    :return: tuple of registered sink names.
    """
    with _log_context_wiring_lock:
        return _log_sink_registry.get_sinks_names()


def has_configured_log_sinks() -> bool:
    """
    Return whether the package-level sink registry contains any sinks.

    :return: True if at least one sink is registered, False otherwise.
    """
    with _log_context_wiring_lock:
        return not _log_sink_registry.is_empty()


def close_log_sink(name: str) -> bool:
    """
    Close and unregister a package-level sink by name.

    If no sink is registered under `name`, the function returns False.

    A sink cannot be closed while it is locally assigned to any package-level
    registered context. In that case, `LogSinkIsInUseError` is raised.

    On successful close, the sink is removed from the registry and its terminator
    is called.

    :param name: package-level sink name.
    :return: True if a registered sink was closed, False if the name was not
        registered.
    :raises TypeError: if `name` is not a string.
    :raises ValueError: if `name` is malformed.
    :raises LogSinkIsInUseError: if the sink is locally assigned to one or more
        registered contexts.
    :raises LogSinkCloseError: if the sink terminator fails.
    """
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
    """
    Return the package-level root log context.

    The root context is created during package bootstrap and provides the default
    logging infrastructure for package-managed child contexts.

    :return: root log context.
    """
    with _log_context_wiring_lock:
        return _log_context_registry.get_root_log_context()


def get_log_context(
    namespace: str,
) -> LogContext | None:
    """
    Return a package-level log context by namespace.

    :param namespace: registered context namespace.
    :return: log context registered under `namespace`, or None if no such context
        exists.
    :raises TypeError: if `namespace` is not a string.
    :raises ValueError: if `namespace` is empty or malformed.
    """

    _validate_namespace("namespace", namespace)

    with _log_context_wiring_lock:
        return _log_context_registry.get(namespace)


def configure_log_context(
    namespace: str,
    *,
    log_sink: LogSinkProto | None = None,
    event_policy: LogEventPolicyProto | None = None,
    payload_processor: LogPayloadProcessorProto | None = None,
    log_error_handling_policy: LogErrorHandlingPolicy | None = None,
) -> LogContext:
    """
    Create or update a package-level log context.

    If the namespace already exists, only explicitly supplied local components are
    updated. Passing None does not reset an existing local component.

    If the namespace does not exist, missing parent contexts are created
    automatically and the supplied components are applied only to the requested
    leaf context.

    :param namespace: context namespace to configure.
    :param log_sink: optional local sink for the configured context.
    :param event_policy: optional local event policy for the configured context.
    :param payload_processor: optional local payload processor for the configured
        context.
    :param log_error_handling_policy: optional local logging infrastructure error
        handling policy for the configured context.
    :return: created or updated log context.
    :raises TypeError: if `namespace` is not a string.
    :raises ValueError: if `namespace` is empty or malformed.
    """
    _validate_namespace("namespace", namespace)

    with _log_context_wiring_lock:
        existing = _log_context_registry.get(namespace)

        if existing is not None:
            if log_sink is not None:
                existing.set_log_sink(log_sink)
            if event_policy is not None:
                existing.set_event_policy(event_policy)
            if payload_processor is not None:
                existing.set_payload_processor(payload_processor)
            if log_error_handling_policy is not None:
                existing.set_log_error_handling_policy(log_error_handling_policy)

            return existing

        return _log_context_registry.create_log_context_chain(
            namespace,
            log_sink=log_sink,
            event_policy=event_policy,
            payload_processor=payload_processor,
            log_error_handling_policy=log_error_handling_policy,
        )


def get_log_context_namespaces() -> tuple[str, ...]:
    """
    Return namespaces of package-level registered log contexts.

    The returned tuple includes the root context namespace.

    :return: tuple of registered context namespaces.
    """
    with _log_context_wiring_lock:
        return _log_context_registry.list_namespaces()


def has_log_context(namespace: str) -> bool:
    """
    Return whether a package-level log context exists for the namespace.

    :param namespace: context namespace to check.
    :return: True if a context is registered under `namespace`, False otherwise.
    :raises TypeError: if `namespace` is not a string.
    :raises ValueError: if `namespace` is empty or malformed.
    """
    _validate_namespace("namespace", namespace)

    with _log_context_wiring_lock:
        return _log_context_registry.contains(namespace)


def reset_log_contexts() -> None:
    """
    Remove all package-level non-root log contexts.

    The root context remains registered. Registered sinks are not closed or removed.

    :return: None.
    """
    with _log_context_wiring_lock:
        _log_context_registry.clear()


def reset_logger() -> None:
    """
    Reset package-level logger state.

    The function closes all package-level registered sinks, clears package-level
    registries, and recreates the default bootstrap state.

    :return: None.
    :raises LogSinkCloseError: if one or more registered sink terminators fail
        during reset.
    """
    global _log_sink_registry, _log_context_registry

    with _log_context_wiring_lock:
        _log_sink_registry.reset()
        _log_sink_registry, _log_context_registry = _bootstrap()
