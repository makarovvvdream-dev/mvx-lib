# src/mvx/common/logger/log_components/protocols.py
from __future__ import annotations
from typing import Any, Mapping, Protocol, runtime_checkable

from ..models import LogLevel

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
        event_namespace: str | None = None,
        event_type: str | None = None,
        entity_id: str | None = None,
        source_path: str | None = None,
        source_line: int | None = None,
        source_func: str | None = None,
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
        :param event_namespace: Optional namespace for the event.
        :type event_namespace: str | None
        :param event_type: Optional event category.
        :type event_type: str | None
        :param entity_id: Optional associated entity identifier.
        :type entity_id: str | None
        :param source_path: Optional path to the source file where the event was logged.
        :type source_path: str | None
        :param source_line: Optional line number in the source file where the event was logged.
        :type source_line: int | None
        :param source_func: Optional name of the function where the event was logged.
        :type source_func: str | None
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
