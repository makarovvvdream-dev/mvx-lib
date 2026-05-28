# src/mvx/common/logger/models.py
from __future__ import annotations

from typing import Protocol, runtime_checkable, Any, Callable
from collections.abc import Mapping
from dataclasses import dataclass
from enum import IntEnum

__all__ = (
    "LogLevel",
    "LogEventMeta",
    "LogEvent",
    "LogSinkProto",
    "LogSinkDescriptor",
    "LogSinkTerminator",
    "LogSinkClassProto",
    "LogEventPolicyProto",
    "LogPayloadProcessorProto",
    "LogPayloadProvider",
)


class LogLevel(IntEnum):
    """
    `LogLevel` defines standard severity levels used by the logger.

    The numeric values follow the conventional Python logging level scale.
    """

    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


# ---- LogSink -----------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LogEventMeta:
    """
    Metadata used to identify and select a log event before delivery.

    `LogEventMeta` contains the stable identity of an event: where it belongs,
    what event it represents, which entity it is related to, and optionally where
    it originated in source code.

    Event policies receive this object before payload normalization and sink
    delivery.

    :param event_namespace: logical namespace of the event, or None when no
        namespace is associated with the event.
    :param event_name: stable event name.
    :param entity_id: optional identifier of the runtime or domain entity related
        to the event.
    :param source_path: optional source file path associated with the event.
    :param source_line: optional source line associated with the event.
    :param source_func: optional source function associated with the event.
    """

    event_namespace: str | None
    event_name: str
    entity_id: str | None
    source_path: str | None
    source_line: int | None
    source_func: str | None


@dataclass(frozen=True, slots=True)
class LogEvent:
    """
    Fully prepared structured log event.

    `LogEvent` is the object delivered to a log sink. At this point the event has
    already been selected for emission and its payload is expected to be log-ready.

    :param level: numeric severity level.
    :param meta: event metadata.
    :param event_outcome: optional outcome or phase of the event. For example,
        `log_invocation` uses values such as ``"invoke"``, ``"success"``,
        ``"failed"``, and ``"cancelled"``.
    :param timestamp: event timestamp.
    :param payload: structured event payload.
    """

    level: int
    meta: LogEventMeta
    event_outcome: str | None
    timestamp: float
    payload: Mapping[str, Any]


@runtime_checkable
class LogSinkProto(Protocol):
    def log(self, event: LogEvent) -> None:
        """
        Deliver a prepared log event.

        A sink receives only completed `LogEvent` objects. Event selection and payload
        preparation are performed before this method is called.

        :param event: prepared event to deliver.
        :return: None.
        """
        ...


@dataclass(frozen=True, slots=True)
class LogSinkDescriptor:
    """
    Stable descriptor of a configured log sink.

    The descriptor is used by the sink registry to identify whether repeated sink
    configuration requests describe the same sink or a conflicting sink.

    :param sink_type: logical sink type.
    :param resource_key: values identifying the target resource.
    :param config_key: values identifying relevant sink configuration that affects
        compatibility.
    """

    sink_type: str
    resource_key: tuple[Any, ...]
    config_key: tuple[Any, ...] = ()

    def to_log_payload(self) -> dict[str, Any]:
        """
        Return a structured logging representation of the descriptor.

        :return: descriptor payload suitable for inclusion in log data.
        """
        return {
            "sink_type": self.sink_type,
            "resource_key": self.resource_key,
            "config_key": self.config_key,
        }


# Cleanup hook returned by a package-managed sink factory.
#
# The terminator is called by package-level lifecycle management when a sink is
# closed or when the logger is reset. Implementations should make the callable
# idempotent, because cleanup paths may be invoked defensively.
LogSinkTerminator = Callable[[], None]


@runtime_checkable
class LogSinkClassProto(Protocol):

    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor:
        """
        Build a descriptor for a sink configuration request.

        The descriptor is used before sink creation to detect idempotent repeated
        configuration and configuration conflicts.

        :param kwargs: sink-specific configuration arguments.
        :return: descriptor of the requested sink configuration.
        """
        ...

    @classmethod
    def create(cls, **kwargs: Any) -> tuple[LogSinkProto, LogSinkTerminator]:
        """
        Create a sink instance and its terminator.

        The returned terminator is called by package-level lifecycle management when
        the sink is closed or the logger is reset.

        :param kwargs: sink-specific configuration arguments.
        :return: pair containing the sink instance and its terminator.
        """
        ...


# ---- LogEventPolicy ----------------------------------------------------------------------


@runtime_checkable
class LogEventPolicyProto(Protocol):
    """
    Protocol for event selection policies.

    An event policy decides whether an event described by `LogEventMeta` should be
    emitted.

    Policies receive metadata only. They do not receive event payloads and do not
    perform payload normalization.
    """

    def is_event_enabled(self, event: LogEventMeta) -> bool:
        """
        Return whether the event described by the given metadata is enabled.

        :param event: event metadata to evaluate.
        :return: True if the event is enabled, False otherwise.
        """
        ...


# ---- LogPayloadProcessorProto ------------------------------------------------------------


@runtime_checkable
class LogPayloadProcessorProto(Protocol):
    """
    Protocol for payload processors.

    A payload processor converts arbitrary payload values into log-ready
    structured data.
    """

    def normalize_payload(
        self,
        payload: Mapping[str, Any],
        *,
        unbounded: bool = False,
    ) -> dict[str, Any]:
        """
        Normalize a structured payload for logging.

        :param payload: payload mapping to normalize.
        :param unbounded: whether item-count limiting should be disabled while
            normalizing this payload.
        :return: normalized payload dictionary.
        """
        ...

    def normalize_value_for_log(
        self,
        value: Any,
        *,
        unbounded: bool = False,
    ) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None:
        """
        Normalize a single value for inclusion in a log payload.

        :param value: value to normalize.
        :param unbounded: whether item-count limiting should be disabled for this
            value.
        :return: normalized log-ready value.
        """
        ...

    def get_plain_verbosity_level(self) -> str | None:
        """
        Return the current verbosity level as a plain string.

        This method is used by components such as `log_invocation` to evaluate
        verbosity-gated field specifications.

        :return: plain verbosity level, or None if no verbosity level is available.
        """
        ...


@runtime_checkable
class LogPayloadProvider(Protocol):
    """
    Protocol for objects that provide their own logging payload.

    When an object implements this protocol, its `to_log_payload()` result is used
    as the object's logging representation.

    The returned payload is expected to be log-ready. Implementers are responsible
    for keeping it reasonably sized and free of sensitive data.

    This protocol takes precedence over type-based log adapters.
    """

    def to_log_payload(self) -> dict[str, Any]:
        """
        Return this object's structured logging payload.

        :return: log-ready payload dictionary.
        """
        ...
