# src/mvx/common/metrics/asyncio_metrics_recorder/errors.py
from __future__ import annotations

from enum import StrEnum

from mvx.common.errors import ReasonedError, RuntimeUnexpectedError

from .common import AsyncioMetricsRecorderState

__all__ = (
    "AsyncioMetricsRecorderError",
    "AsyncioMetricsRecorderLoopUnavailableError",
    "AsyncioMetricsRecorderInvalidStateError",
    "AsyncioMetricsRecorderOnStartingHookFailedError",
    "AsyncioMetricsRecorderOnStoppedHookFailedError",
    "AsyncioMetricsRecorderQueueOverflowError",
    "AsyncioMetricsRecorderDispatcherCancelledError",
    "AsyncioMetricsRecorderUnexpectedError",
)


class _AsyncioMetricsRecorderErrorReason(StrEnum):
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
    carry a reason code from `_AsyncioMetricsRecorderErrorReason`.
    """

    pass


class AsyncioMetricsRecorderLoopUnavailableError(AsyncioMetricsRecorderError):
    """
    Raised when direct recorder construction cannot find a running event loop.

    `AsyncioMetricsRecorder` is bound to an existing asyncio event loop.
    Dedicated-loop creation is handled by the metrics runtime layer.
    """

    def __init__(self) -> None:
        super().__init__(
            reason=_AsyncioMetricsRecorderErrorReason.EVENT_LOOP_UNAVAILABLE.value,
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
            reason=_AsyncioMetricsRecorderErrorReason.INVALID_RECORDER_STATE.value,
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
            reason=_AsyncioMetricsRecorderErrorReason.ON_STARTING_HOOK_FAILED.value,
            message=f"on starting hook failed -> {str(cause)}",
            cause=cause,
        )


class AsyncioMetricsRecorderOnStoppedHookFailedError(AsyncioMetricsRecorderError):
    """
    Raised when the `_on_stopped()` lifecycle hook fails.
    """

    def __init__(
        self,
        cause: Exception,
    ) -> None:
        """
        Create a stopped-hook failure error.

        :param cause: exception raised by the stopped hook.
        """
        super().__init__(
            reason=_AsyncioMetricsRecorderErrorReason.ON_STOPPED_HOOK_FAILED.value,
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
            reason=_AsyncioMetricsRecorderErrorReason.QUEUE_OVERFLOW.value,
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
            reason=_AsyncioMetricsRecorderErrorReason.DISPATCHER_UNEXPECTEDLY_CANCELLED.value,
            message="dispatcher unexpectedly cancelled",
        )


class AsyncioMetricsRecorderUnexpectedError(AsyncioMetricsRecorderError, RuntimeUnexpectedError):
    """
    Raised when the async metrics recorder catches an unexpected internal failure.

    This error wraps failures that are not part of the normal recorder control
    flow and marks them as unexpected runtime errors.
    """

    def __init__(
        self,
        cause: Exception,
    ) -> None:
        """
        Create an unexpected async recorder error.

        :param cause: original unexpected exception.
        """
        super().__init__(
            reason=_AsyncioMetricsRecorderErrorReason.UNEXPECTED_ERROR.value,
            message=f"unexpected async metrics recorder error -> {str(cause)}",
            cause=cause,
        )
