# common/src/mvx/common/logger/core/models.py
from __future__ import annotations

from typing import Protocol, runtime_checkable, Any, Mapping, TypeAlias, Callable
from dataclasses import dataclass
from enum import IntEnum


import logging

__all__ = (
    "LogLevel",
    "LogContextProto",
    "LogContextProviderProto",
    "LogEntityIdProviderProto",
    "LogPayloadProvider",
    "LogAdapter",
    "LogAdapterResolver",
    "LogEvent",
    "LogSinkProto",
    "LogSinkDescriptor",
    "LogSinkTerminator",
    "LogSinkClassProto",
    "RegisteredLogSink",
    "LogEventPolicy",
)


class LogLevel(IntEnum):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


# ---- LogContextProto ---------------------------------------------------------------------


@runtime_checkable
class LogContextProto(Protocol):

    def is_event_enabled(self, event: str) -> bool:
        """
        Determines whether a specific event is enabled for logging.

        This method checks the applied log policy to determine if the
        specified event is enabled for logging or not and returns a boolean value accordingly.

        :param event: The name of the event to check.
        :type event: str
        :return: True if the event is enabled, False otherwise.
        :rtype: bool
        """
        ...

    @property
    def verbosity_level(self) -> str:
        """
        Retrieves the current verbosity level setting for the payload.

        This property fetches and returns the current verbosity level that
        determines the detail level or granularity of information included
        in the payload.

        :return: The verbosity level of the payload.
        :rtype: str
        """
        ...

    def normalize_value_for_log(
        self,
        value: Any,
        *,
        unbounded: bool = False,
    ) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None:
        """
        Normalize a value for logging purposes by converting it into a form suitable
        for inclusion in log messages. The method ensures that the transformed value
        is serializable and comprehensible in logs.

        :param value: The input value to normalize. It can be of any type.
        :param unbounded: A flag indicating whether the value can be transformed
            into an unbounded representation (e.g., raw data without truncation).
            Defaults to False.
        :return: The normalized value. It can be of one of the following types:
            str, int, float, bool, bytes, dictionary with string keys, list, or None.
        """
        ...

    def build_error_payload(self, err: BaseException) -> Mapping[str, Any]:
        """
        Builds a payload for logging an error

        :param err: The exception instance to build the payload for.
        :type err: BaseException
        :return: A dictionary containing the error payload.
        """
        ...

    def is_error_logged(self, err: BaseException) -> bool:
        """
        Determines whether an error should be logged based on its type and message.

        :param err: The exception instance to check.
        :type err: BaseException
        :return: True if the error should be logged, False otherwise.
        """
        ...

    def mark_error_logged(self, err: BaseException) -> None:
        """
        Marks an error as logged, preventing it from being logged again.

        :param err: The exception instance to mark as logged.
        :type err: BaseException
        """
        ...

    def log_event(
        self,
        event: str,
        level: LogLevel,
        payload: Mapping[str, Any],
        *,
        event_type: str | None = None,
        entity_id: str | None = None,
    ) -> None:
        """
        Logs an event with a specified level, payload, and optional metadata such as
        event type and entity ID.

        :param event: The name of the event being logged.
        :type event: str
        :param level: The log level indicating the severity or importance of the event.
        :type level: LogLevel
        :param payload: Additional data or context for the event.
        :type payload: Mapping[str, Any]
        :param event_type: Optional event category.
        :type event_type: str | None
        :param entity_id: Optional associated entity identifier.
        :type entity_id: str | None
        :return: None
        :rtype: None
        """
        ...


# ---- LogContextProviderProto -------------------------------------------------------------


@runtime_checkable
class LogContextProviderProto(Protocol):
    def get_log_context(self) -> LogContextProto:
        """
        Retrieve the logging context.

        This method provides the context information required for logging purposes.

        :return: The logging context for the current operation.
        :rtype: LogContextProto
        """
        ...


# ---- LogEntityIdProviderProto ------------------------------------------------------------


@runtime_checkable
class LogEntityIdProviderProto(Protocol):

    @property
    def identity(self) -> str:
        """
        Retrieves the identity of the object. The identity is a unique identifier that
        represents the current instance.

        :return: The unique identifier as a string.
        :rtype: str
        """
        ...


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


# ---- LogSinkProto ------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LogEvent:
    level: int
    namespace: str
    event_name: str
    event_type: str
    timestamp: float
    entity_id: str
    payload: Mapping[str, Any]


@runtime_checkable
class LogSinkProto(Protocol):
    def log(self, event: LogEvent) -> None: ...


# ---- Sink lifecycle ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LogSinkDescriptor:
    sink_type: str
    resource_key: tuple[Any, ...]
    config_key: tuple[Any, ...] = ()


LogSinkTerminator = Callable[[], None]


@runtime_checkable
class LogSinkClassProto(Protocol):
    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor: ...

    @classmethod
    def create(cls, **kwargs: Any) -> tuple[LogSinkProto, LogSinkTerminator]: ...


@dataclass(frozen=True, slots=True)
class RegisteredLogSink:
    sink: LogSinkProto
    terminator: LogSinkTerminator
    descriptor: LogSinkDescriptor


# ---- LogEventPolicy ----------------------------------------------------------------------


@runtime_checkable
class LogEventPolicy(Protocol):
    def is_event_enabled(self, event: str) -> bool: ...
