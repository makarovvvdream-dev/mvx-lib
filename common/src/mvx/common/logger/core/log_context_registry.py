# common/src/mvx/common/logger/core/log_context_registry.py

from __future__ import annotations

from typing import Callable
import threading

from .log_context import LogContext, LogVerbosityLevel
from .models import (
    LogAdapterResolver,
    LogEventPolicy,
    LogSinkProto,
)

from ..errors import LogContextError, LogContextErrorReason

__all__ = ("LogContextRegistry",)


ROOT_NAMESPACE = ""


class LogContextRegistry:

    def __init__(
        self,
        *,
        log_sink_resolver: Callable[[str], LogSinkProto],
        event_policy: LogEventPolicy,
        root_log_sink_name: str | None = None,
        verbosity_level: LogVerbosityLevel = LogVerbosityLevel.NORMAL,
        log_adapter_resolver: LogAdapterResolver | None = None,
    ) -> None:
        self._lock = threading.RLock()

        self._log_sink_resolver = log_sink_resolver
        self._event_policy = event_policy
        self._verbosity_level = verbosity_level
        self._log_adapter_resolver = log_adapter_resolver

        self._contexts: dict[str, LogContext] = {}

        root = LogContext(
            namespace=ROOT_NAMESPACE,
            parent=None,
            log_sink_name=root_log_sink_name,
            log_sink_resolver=self._log_sink_resolver,
            event_policy=self._event_policy,
            verbosity_level=self._verbosity_level,
            log_adapter_resolver=self._log_adapter_resolver,
        )

        self._contexts[ROOT_NAMESPACE] = root

    def get(
        self,
        name: str | None = None,
        *,
        log_sink_name: str | None = None,
    ) -> LogContext:
        namespace = self._normalize_namespace(name)

        with self._lock:
            existing = self._contexts.get(namespace)
            if existing is not None:
                self._validate_existing_context_sink(
                    context=existing,
                    log_sink_name=log_sink_name,
                )
                return existing

            parent = self._get_or_create_parent(namespace)

            context = LogContext(
                namespace=namespace,
                parent=parent,
                log_sink_name=log_sink_name,
                log_sink_resolver=self._log_sink_resolver,
                event_policy=self._event_policy,
                verbosity_level=self._verbosity_level,
                log_adapter_resolver=self._log_adapter_resolver,
            )

            self._contexts[namespace] = context
            return context

    def _get_or_create_parent(self, namespace: str) -> LogContext:
        parent_namespace = self._get_parent_namespace(namespace)

        existing = self._contexts.get(parent_namespace)
        if existing is not None:
            return existing

        parent = self._get_or_create_parent(parent_namespace)

        context = LogContext(
            namespace=parent_namespace,
            parent=parent,
            log_sink_name=None,
            log_sink_resolver=self._log_sink_resolver,
            event_policy=self._event_policy,
            verbosity_level=self._verbosity_level,
            log_adapter_resolver=self._log_adapter_resolver,
        )

        self._contexts[parent_namespace] = context
        return context

    @staticmethod
    def _get_parent_namespace(namespace: str) -> str:
        if namespace == ROOT_NAMESPACE:
            return ROOT_NAMESPACE

        if "." not in namespace:
            return ROOT_NAMESPACE

        return namespace.rsplit(".", 1)[0]

    @staticmethod
    def _normalize_namespace(name: str | None) -> str:
        if name is None:
            return ROOT_NAMESPACE

        namespace = name.strip()

        if namespace == "":
            return ROOT_NAMESPACE

        if namespace.startswith(".") or namespace.endswith(".") or ".." in namespace:
            raise LogContextError(
                message=f"Invalid log context namespace: {name!r}",
                reason=LogContextErrorReason.LOG_CONTEXT_INVALID_NAMESPACE.value,
            )

        return namespace

    @staticmethod
    def _validate_existing_context_sink(
        *,
        context: LogContext,
        log_sink_name: str | None,
    ) -> None:
        if log_sink_name is None:
            return

        if context.log_sink_name == log_sink_name:
            return

        if context.log_sink_name is None:
            raise LogContextError(
                message=(
                    f"Log context {context.namespace!r} already exists without local "
                    f"log sink binding"
                ),
                reason=(
                    LogContextErrorReason.LOG_CONTEXT_ALREADY_REGISTERED_WITH_DIFFERENT_SINK.value
                ),
            )

        raise LogContextError(
            message=(
                f"Log context {context.namespace!r} is already bound to log sink "
                f"{context.log_sink_name!r}"
            ),
            reason=(LogContextErrorReason.LOG_CONTEXT_ALREADY_REGISTERED_WITH_DIFFERENT_SINK.value),
        )
