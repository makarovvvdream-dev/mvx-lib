# common/src/mvx/common/logger/__init__.py
from __future__ import annotations

from typing import Any

from .core.models import LogSinkClassProto, LogSinkProto
from .core.log_sink_registry import LogSinkRegistry

from .errors import LogSinkRegistryError, LogSinkRegistryErrorReason

__all__ = (
    "register_log_sink",
    "get_log_sink",
    "shutdown_log_sinks",
    "LogSinkRegistryError",
    "LogSinkRegistryErrorReason",
)


_log_sink_registry = LogSinkRegistry()


def register_log_sink(
    *,
    name: str,
    sink_cls: LogSinkClassProto,
    **sink_kwargs: Any,
) -> LogSinkProto:

    return _log_sink_registry.register(name=name, sink_cls=sink_cls, **sink_kwargs)


def get_log_sink(name: str) -> LogSinkProto:
    return _log_sink_registry.get(name=name)


def shutdown_log_sinks() -> None:
    _log_sink_registry.shutdown()
