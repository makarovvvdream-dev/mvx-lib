# src/mvx/common/logger/log_components/protocols.py
from __future__ import annotations
from typing import Any, Mapping, Protocol, runtime_checkable

from ..models import LogLevel, LogEvent, LogEventMeta

# ---- LogContextProto ---------------------------------------------------------------------


@runtime_checkable
class LogContextProto(Protocol):

    def is_event_enabled(self, event: LogEventMeta) -> bool:
        """
        Determines whether a specific event is enabled for logging.

        This method checks the applied log policy to determine if the
        specified event is enabled for logging or not and returns a boolean value accordingly.

        :param event: The event to check.
        :type event: LogEventMeta
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

    @property
    def namespace(self) -> str:
        """
        Provides access to the namespace associated with the context.

        The namespace is a string identifier that can be used to distinguish the
        context or scope for certain operations or data within the object.

        :return: The namespace as a string.
        :rtype: str
        """

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

    def emit_log_event(
        self,
        event: LogEvent,
    ) -> None:
        """
        Emit a fully prepared log event to the configured sink.

        This method does not apply the event policy and does not perform payload
        normalization. The caller is responsible for deciding whether the event
        should be emitted and for providing a log-ready payload.

        :param event: The prepared log event to emit.
        :type event: LogEvent
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
