# common/src/mvx/common/logger/core/log_sink_registry.py
from __future__ import annotations

from enum import StrEnum
from typing import Any

import threading

from .models import (
    LogSinkProto,
    LogSinkClassProto,
    RegisteredLogSink,
)

from ..errors import LogSinkRegistryError, LogSinkRegistryErrorReason

__all__ = ("LogSinkRegistry", "LogSinkRegistryState")


class LogSinkRegistryState(StrEnum):
    ACTIVE = "ACTIVE"
    SHUTTING_DOWN = "SHUTTING_DOWN"
    SHUT_DOWN = "SHUT_DOWN"


class LogSinkRegistry:

    def __init__(self) -> None:
        self._state: LogSinkRegistryState = LogSinkRegistryState.ACTIVE

        self._lifecycle_lock = threading.RLock()
        self._registry_lock = threading.RLock()

        self._registered_sinks: dict[str, RegisteredLogSink] = {}

    def get_state(self) -> LogSinkRegistryState:
        with self._lifecycle_lock:
            return self._state

    def register(
        self,
        *,
        name: str,
        sink_cls: LogSinkClassProto,
        **sink_kwargs: Any,
    ) -> LogSinkProto:

        with self._lifecycle_lock:
            if self._state in (LogSinkRegistryState.SHUTTING_DOWN, LogSinkRegistryState.SHUT_DOWN):
                raise LogSinkRegistryError(
                    message="Log sink registry is not active",
                    reason=LogSinkRegistryErrorReason.LOG_SINK_REGISTRY_IS_NOT_ACTIVE.value,
                )

            descriptor = sink_cls.build_descriptor(**sink_kwargs)

            with self._registry_lock:
                existing = self._registered_sinks.get(name)
                if existing is not None:
                    if existing.descriptor == descriptor:
                        return existing.sink

                    raise LogSinkRegistryError(
                        message=f"Log sink {name!r} is already registered with different descriptor",
                        reason=(
                            LogSinkRegistryErrorReason.LOG_SINK_ALREADY_REGISTERED_WITH_DIFFERENT_DESCRIPTOR.value
                        ),
                    )

            try:
                sink, terminator = sink_cls.create(**sink_kwargs)
            except Exception as exc:
                raise LogSinkRegistryError(
                    message=f"Failed to create log sink {name!r}",
                    reason=LogSinkRegistryErrorReason.LOG_SINK_CREATE_FAILED.value,
                    cause=exc,
                ) from exc

            with self._registry_lock:
                self._registered_sinks[name] = RegisteredLogSink(
                    sink=sink,
                    terminator=terminator,
                    descriptor=descriptor,
                )

            return sink

    def get(self, name: str) -> LogSinkProto:
        with self._lifecycle_lock:
            if self._state in (LogSinkRegistryState.SHUTTING_DOWN, LogSinkRegistryState.SHUT_DOWN):
                raise LogSinkRegistryError(
                    message="Log sink registry is not active",
                    reason=LogSinkRegistryErrorReason.LOG_SINK_REGISTRY_IS_NOT_ACTIVE.value,
                )

            with self._registry_lock:
                registered = self._registered_sinks.get(name)
                if registered is not None:
                    return registered.sink

            raise LogSinkRegistryError(
                message=f"Log sink {name!r} is not registered",
                reason=LogSinkRegistryErrorReason.LOG_SINK_NOT_FOUND.value,
            )

    def shutdown(self) -> None:
        with self._lifecycle_lock:
            if self._state in (LogSinkRegistryState.SHUTTING_DOWN, LogSinkRegistryState.SHUT_DOWN):
                return

            self._state = LogSinkRegistryState.SHUTTING_DOWN

        with self._registry_lock:
            registered_sinks = list(self._registered_sinks.values())
            self._registered_sinks.clear()

        errors: list[Exception] = []

        for log_sink in reversed(registered_sinks):
            try:
                log_sink.terminator()
            except Exception as exc:
                errors.append(exc)

        with self._lifecycle_lock:
            self._state = LogSinkRegistryState.SHUT_DOWN

        if errors:
            raise LogSinkRegistryError(
                message=f"Failed to terminate {len(errors)} log sink(s)",
                reason=LogSinkRegistryErrorReason.LOG_SINK_TERMINATOR_FAILED.value,
                cause=errors[0],
            )
