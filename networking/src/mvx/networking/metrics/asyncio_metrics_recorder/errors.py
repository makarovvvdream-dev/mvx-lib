# src/mvx/networking/metrics/asyncio_metrics_recorder/errors.py
from __future__ import annotations
from mvx.common.helpers.document_enum import document_enum

from enum import StrEnum

from mvx.common.errors import ReasonedError

from .common import AsyncioMetricsRecorderState

__all__ = (
    "AsyncioMetricsRecorderError",
    "AsyncioMetricsRecorderErrorReason",
    "AsyncioMetricsRecorderLoopUnavailableError",
    "AsyncioMetricsRecorderInvalidStateError",
    "AsyncioMetricsRecorderOnStartingHookFailedError",
    "AsyncioMetricsRecorderStoppedHookFailedError",
    "AsyncioMetricsRecorderQueueOverflowError",
    "AsyncioMetricsRecorderDispatcherCancelledError",
    "AsyncioMetricsRecorderUnexpectedError",
)


@document_enum
class AsyncioMetricsRecorderErrorReason(StrEnum):
    """
    Reason codes used by `AsyncioMetricsRecorderError` subclasses.
    """

    #: A recorder was created without an available running event loop.
    EVENT_LOOP_UNAVAILABLE = "EVENT_LOOP_UNAVAILABLE"

    #: An operation was requested while the recorder was in an invalid state.
    INVALID_RECORDER_STATE = "INVALID_RECORDER_STATE"

    #: The `_on_starting()` hook failed.
    ON_STARTING_HOOK_FAILED = "ON_STARTING_HOOK_FAILED"

    #: The `_on_stopped()` hook failed.
    ON_STOPPED_HOOK_FAILED = "ON_STOPPED_HOOK_FAILED"

    #: The recorder queue limit was reached.
    QUEUE_OVERFLOW = "QUEUE_OVERFLOW"

    #: The dispatcher task was cancelled unexpectedly.
    DISPATCHER_UNEXPECTEDLY_CANCELLED = "DISPATCHER_UNEXPECTEDLY_CANCELLED"

    #: An unexpected error occurred inside the recorder runtime.
    UNEXPECTED_ERROR = "UNEXPECTED_ERROR"


class AsyncioMetricsRecorderError(ReasonedError):
    """
    Base class for `AsyncioMetricsRecorder` errors.

    All errors raised by the async recorder runtime inherit from this class and
    carry a reason code from `AsyncioMetricsRecorderErrorReason`.
    """

    pass


class AsyncioMetricsRecorderLoopUnavailableError(AsyncioMetricsRecorderError):
    """
    Raised when direct recorder construction cannot find a running event loop.

    This applies to direct `AsyncioMetricsRecorder` construction. Package-managed
    creation through `create()` builds a dedicated event loop runtime.
    """

    def __init__(self) -> None:
        super().__init__(
            reason=AsyncioMetricsRecorderErrorReason.EVENT_LOOP_UNAVAILABLE.value,
            message="unable to get a running event loop for asyncio recorder",
        )


class AsyncioMetricsRecorderInvalidStateError(AsyncioMetricsRecorderError):
    """
    Raised when a recorder operation is not valid for the current lifecycle state.
    """

    def __init__(
        self,
        recorder_state: AsyncioMetricsRecorderState,
        expected_states: tuple[AsyncioMetricsRecorderState, ...],
        cause: Exception | None = None,
    ) -> None:
        """
        Create an invalid-state error.

        :param recorder_state: actual recorder state.
        :param expected_states: states allowed for the requested operation.
        :param cause: optional underlying cause.
        """
        if len(expected_states) == 1:
            expected_states_str = expected_states[0].value
            msg = (
                f"invalid recorder state '{recorder_state.value}', expected '{expected_states_str}'"
            )
        else:
            expected_states_str = ", ".join(f"'{state.value}'" for state in expected_states)
            msg = (
                f"invalid recorder state '{recorder_state.value}', "
                f"expected one of: {expected_states_str}"
            )

        details = {
            "recorder_state": recorder_state.value,
            "expected_states": tuple(state.value for state in expected_states),
        }

        super().__init__(
            reason=AsyncioMetricsRecorderErrorReason.INVALID_RECORDER_STATE.value,
            message=msg,
            details=details,
            cause=cause,
        )


class AsyncioMetricsRecorderOnStartingHookFailedError(AsyncioMetricsRecorderError):
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
            reason=AsyncioMetricsRecorderErrorReason.ON_STARTING_HOOK_FAILED.value,
            message=f"on starting hook failed -> {str(cause)}",
            cause=cause,
        )


class AsyncioMetricsRecorderStoppedHookFailedError(AsyncioMetricsRecorderError):
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
            reason=AsyncioMetricsRecorderErrorReason.ON_STOPPED_HOOK_FAILED.value,
            message=f"on stopped hook failed -> {str(cause)}",
            cause=cause,
        )


class AsyncioMetricsRecorderQueueOverflowError(AsyncioMetricsRecorderError):
    """
    Raised when the accepted-event limit is reached.

    This error is raised when queue overflow policy is `RAISE_ERROR`.
    """

    def __init__(self) -> None:
        super().__init__(
            reason=AsyncioMetricsRecorderErrorReason.QUEUE_OVERFLOW.value,
            message="queue overflow",
        )


class AsyncioMetricsRecorderDispatcherCancelledError(AsyncioMetricsRecorderError):
    """
    Raised when the dispatcher task is cancelled unexpectedly.

    Normal shutdown cancels the dispatcher as part of stopping. This error
    represents cancellation outside the normal stopping path.
    """

    def __init__(self) -> None:
        super().__init__(
            reason=AsyncioMetricsRecorderErrorReason.DISPATCHER_UNEXPECTEDLY_CANCELLED.value,
            message="dispatcher unexpectedly cancelled",
        )


class AsyncioMetricsRecorderUnexpectedError(AsyncioMetricsRecorderError):
    """
    Raised when an unexpected runtime error is wrapped by the async recorder.
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
            reason=AsyncioMetricsRecorderErrorReason.UNEXPECTED_ERROR.value,
            message=f"unexpected error -> {str(cause)}",
            cause=cause,
        )
