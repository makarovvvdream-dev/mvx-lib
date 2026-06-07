# src/mvx/common/logger/log_components/protocols.py
from __future__ import annotations
from typing import Any, Mapping, Protocol, runtime_checkable

from ..models import LogEvent, LogEventMeta

# ---- LogContextProto ---------------------------------------------------------------------


@runtime_checkable
class LogContextProto(Protocol):
    def is_event_enabled(self, event: LogEventMeta) -> bool:
        """
        Return whether the event described by the given metadata is enabled.

        `log_invocation` calls this method before emitting normal operation
        tracing outcomes such as `invoke` and `success`.

        :param event: event metadata to check.
        :return: True if the event is enabled, False otherwise.
        """
        ...

    def get_plain_verbosity_level(self) -> str | None:
        """
        Return the current verbosity level as a plain string.

        `log_invocation` uses this value to evaluate verbosity-gated field
        specifications such as `MAXIMUM:payload` or
        `NORMAL,MAXIMUM:request_id`.

        :return: the current plain verbosity level, or None if no level is set.
        """
        ...

    @property
    def namespace(self) -> str:
        """
        Return the namespace used for events emitted through this context.

        `log_invocation` copies this value into `LogEventMeta.event_namespace`
        when building metadata for the decorated operation.

        :return: the context namespace.
        """

    def normalize_value_for_log(
        self,
        value: Any,
        *,
        unbounded: bool = False,
    ) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None:
        """
        Normalize a single value for inclusion in a log payload.

        `log_invocation` uses this method for selected argument values,
        context fields, closure values, and selected result values.

        :param value: value to normalize.
        :param unbounded: whether item-count limiting should be disabled for
            this value.
        :return: the normalized log-ready value.
        """
        ...

    def build_error_payload(self, err: BaseException) -> Mapping[str, Any]:
        """
        Build a structured payload for an exception.

        `log_invocation` uses this method for full `failed` outcomes and for
        emitted `cancelled` outcomes.

        :param err: exception instance to describe.
        :return: structured error payload.
        """
        ...

    def is_error_logged(self, err: BaseException) -> bool:
        """
        Return whether the exception instance is already marked as logged.

        `log_invocation` uses this marker to suppress repeated detailed error
        payloads for the same exception instance.

        :param err: exception instance to check.
        :return: True if the exception is already marked as logged.
        """
        ...

    def mark_error_logged(self, err: BaseException) -> None:
        """
        Mark the exception instance as already logged.

        `log_invocation` calls this after emitting a full error payload or
        after applying an explicit error policy rule.

        :param err: exception instance to mark.
        :return: None.
        """
        ...

    def emit_log_event(
        self,
        event: LogEvent,
    ) -> None:
        """
        Emit a fully prepared log event.

        This method is the final boundary used by `log_invocation`. The event
        metadata, outcome, timestamp, level, and payload have already been
        prepared by the caller.

        This method does not apply event selection for `log_invocation` and
        does not build the operation payload.

        :param event: prepared log event to emit.
        :return: None.
        """
        ...


# ---- LogContextProviderProto -------------------------------------------------------------


@runtime_checkable
class LogContextProviderProto(Protocol):
    def get_log_context(self) -> LogContextProto | None:
        """
        Return the logging context for this object.

        `log_invocation` uses this protocol for method-based context
        resolution. For instance methods, the first positional argument is
        usually `self`, and this method supplies the effective context.

        Returning ``None`` explicitly disables logging through `log_invocation`
        for the current call. In that case, the decorated callable is executed
        normally and no invocation lifecycle events are emitted.

        :return: logging context used by the decorated operation, or ``None`` to
            disable decorator-driven logging for the current call.
        """
        ...


# ---- LogEntityIdProviderProto ------------------------------------------------------------


@runtime_checkable
class LogEntityIdProviderProto(Protocol):
    @property
    def entity_id(self) -> str:
        """
        Return the stable entity identifier for this object.

        `log_invocation` uses this value as `LogEventMeta.entity_id` when no
        explicit `entity_id_getter` is supplied.

        :return: entity identifier.
        """
        ...
