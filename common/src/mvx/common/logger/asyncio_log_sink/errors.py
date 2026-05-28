# src/mvx/common/logger/asyncio_log_sink/errors.py
from __future__ import annotations
from enum_tools.documentation import document_enum

from enum import StrEnum

from mvx.common.errors import ReasonedError

from .common import AsyncioLogSinkState

__all__ = (
    "AsyncioLogSinkError",
    "AsyncioLogSinkErrorReason",
    "AsyncioLogSinkEventLoopUnavailableError",
    "AsyncioLogSinkInvalidStateError",
    "AsyncioLogSinkOnStartingHookFailedError",
    "AsyncioLogSinkOnStoppedHookFailedError",
    "AsyncioLogSinkQueueOverflowError",
    "AsyncioLogSinkDispatcherCancelledError",
    "AsyncioLogSinkUnexpectedError",
)


@document_enum
class AsyncioLogSinkErrorReason(StrEnum):
    """
    Reason codes used by `AsyncioLogSinkError` subclasses.
    """

    #: A sink was created without an available running event loop.
    EVENT_LOOP_UNAVAILABLE = "EVENT_LOOP_UNAVAILABLE"

    #: An operation was requested while the sink was in an invalid state.
    INVALID_LOG_SINK_STATE = "INVALID_LOG_SINK_STATE"

    #: The `_on_starting()` hook failed.
    ON_STARTING_HOOK_FAILED = "ON_STARTING_HOOK_FAILED"

    #: The `_on_stopped()` hook failed.
    ON_STOPPED_HOOK_FAILED = "ON_STOPPED_HOOK_FAILED"

    #: The sink queue limit was reached.
    QUEUE_OVERFLOW = "QUEUE_OVERFLOW"

    #: The dispatcher task was cancelled unexpectedly.
    DISPATCHER_UNEXPECTEDLY_CANCELLED = "DISPATCHER_UNEXPECTEDLY_CANCELLED"

    #: An unexpected error occurred inside the sink runtime.
    UNEXPECTED_ERROR = "UNEXPECTED_ERROR"


class AsyncioLogSinkError(ReasonedError):
    """
    Base class for `AsyncioLogSink` errors.

    All errors raised by the async sink runtime inherit from this class and
    carry a reason code from `AsyncioLogSinkErrorReason`.
    """

    pass


class AsyncioLogSinkEventLoopUnavailableError(AsyncioLogSinkError):
    """
    Raised when direct sink construction cannot find a running event loop.

    This applies to direct `AsyncioLogSink` construction. Package-managed
    creation through `create()` builds a dedicated event loop runtime.
    """

    def __init__(self) -> None:
        super().__init__(
            reason=AsyncioLogSinkErrorReason.EVENT_LOOP_UNAVAILABLE.value,
            message="unable to get a running event loop for asyncio log sink",
        )


class AsyncioLogSinkInvalidStateError(AsyncioLogSinkError):
    """
    Raised when a sink operation is not valid for the current lifecycle state.
    """

    def __init__(
        self,
        sink_state: AsyncioLogSinkState,
        expected_states: tuple[AsyncioLogSinkState, ...],
        cause: Exception | None = None,
    ) -> None:
        """
        Create an invalid-state error.

        :param sink_state: actual sink state.
        :param expected_states: states allowed for the requested operation.
        :param cause: optional underlying cause.
        """
        if len(expected_states) == 1:
            expected_states_str = expected_states[0].value
            msg = f"invalid log sink state '{sink_state.value}', expected '{expected_states_str}'"
        else:
            expected_states_str = ", ".join(f"'{state.value}'" for state in expected_states)
            msg = (
                f"invalid log sink state '{sink_state.value}', "
                f"expected one of: {expected_states_str}"
            )

        details = {
            "sink_state": sink_state.value,
            "expected_states": tuple(state.value for state in expected_states),
        }

        super().__init__(
            reason=AsyncioLogSinkErrorReason.INVALID_LOG_SINK_STATE.value,
            message=msg,
            details=details,
            cause=cause,
        )


class AsyncioLogSinkOnStartingHookFailedError(AsyncioLogSinkError):
    """
    Raised when the `_on_starting()` lifecycle hook fails.
    """

    def __init__(
        self,
        cause: Exception,
    ) -> None:
        """
        Create a startup-hook failure error.

        :param cause: exception raised by the startup hook.
        """
        super().__init__(
            reason=AsyncioLogSinkErrorReason.ON_STARTING_HOOK_FAILED.value,
            message=f"on starting hook failed -> {str(cause)}",
            cause=cause,
        )


class AsyncioLogSinkOnStoppedHookFailedError(AsyncioLogSinkError):
    """
    Raised when the `_on_stopped()` lifecycle hook fails.
    """

    def __init__(
        self,
        cause: Exception,
    ) -> None:
        """
        Create a stop-hook failure error.

        :param cause: exception raised by the stop hook.
        """
        super().__init__(
            reason=AsyncioLogSinkErrorReason.ON_STOPPED_HOOK_FAILED.value,
            message=f"on stopped hook failed -> {str(cause)}",
            cause=cause,
        )


class AsyncioLogSinkQueueOverflowError(AsyncioLogSinkError):
    """
    Raised when the accepted-event limit is reached.

    This error is raised when queue overflow policy is `RAISE_ERROR`.
    """

    def __init__(self) -> None:
        super().__init__(
            reason=AsyncioLogSinkErrorReason.QUEUE_OVERFLOW.value,
            message="queue overflow",
        )


class AsyncioLogSinkDispatcherCancelledError(AsyncioLogSinkError):
    """
    Raised when the dispatcher task is cancelled unexpectedly.

    Normal shutdown cancels the dispatcher as part of stopping. This error
    represents cancellation outside the normal stopping path.
    """

    def __init__(self) -> None:
        super().__init__(
            reason=AsyncioLogSinkErrorReason.DISPATCHER_UNEXPECTEDLY_CANCELLED.value,
            message="dispatcher unexpectedly cancelled",
        )


class AsyncioLogSinkUnexpectedError(AsyncioLogSinkError):
    """
    Raised when an unexpected runtime error is wrapped by the async sink.
    """

    def __init__(
        self,
        cause: Exception,
    ) -> None:
        """
        Create an unexpected-error wrapper.

        :param cause: original unexpected exception.
        """
        super().__init__(
            reason=AsyncioLogSinkErrorReason.UNEXPECTED_ERROR.value,
            message=f"unexpected error -> {str(cause)}",
            cause=cause,
        )
