# src/mvx/common/logger/models.py
from __future__ import annotations

from typing import Protocol, runtime_checkable, Any, Mapping, TypeAlias, Callable
from dataclasses import dataclass
from enum import IntEnum

__all__ = (
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
)


class LogLevel(IntEnum):
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


# ---- LogPayloadProvider ------------------------------------------------------------------


@runtime_checkable
class LogPayloadProvider(Protocol):
    """
    Objects implementing this protocol can provide a fully controlled logging payload.

    Semantics
    ---------
    - to_log_payload() must return a dict[str, Any] that is already suitable
      for logging:
        * no item-count limits are applied;
        * no additional normalization is performed;
        * nested structures are preserved as-is.

    - It is the implementer's responsibility to ensure that the returned
      payload is reasonable in size and does not contain sensitive data.

    - When present, this protocol takes precedence over any type-based
      log adapter registered in the adapter registry.
    """

    def to_log_payload(self) -> dict[str, Any]: ...


# ---- LogAdapterResolver ------------------------------------------------------------------


LogAdapter: TypeAlias = Callable[[Any, str], dict[str, Any]]
LogAdapterResolver: TypeAlias = Callable[[Any], LogAdapter | None]


# ---- LogSink -----------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LogEvent:
    level: int
    event_namespace: str | None
    event_name: str
    event_type: str | None
    timestamp: float
    entity_id: str | None
    payload: Mapping[str, Any]
    source_path: str | None
    source_line: int | None
    source_func: str | None


@runtime_checkable
class LogSinkProto(Protocol):
    def log(self, event: LogEvent) -> None: ...


@dataclass(frozen=True, slots=True)
class LogSinkDescriptor:
    sink_type: str
    resource_key: tuple[Any, ...]
    config_key: tuple[Any, ...] = ()

    def to_log_payload(self) -> dict[str, Any]:
        return {
            "sink_type": self.sink_type,
            "resource_key": self.resource_key,
            "config_key": self.config_key,
        }


LogSinkTerminator = Callable[[], None]


@runtime_checkable
class LogSinkClassProto(Protocol):

    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor: ...

    @classmethod
    def create(cls, **kwargs: Any) -> tuple[LogSinkProto, LogSinkTerminator]: ...


# ---- LogEventPolicy ----------------------------------------------------------------------


@runtime_checkable
class LogEventPolicy(Protocol):
    def is_event_enabled(self, event: str) -> bool: ...
