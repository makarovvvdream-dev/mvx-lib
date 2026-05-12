# src/mvx/common/logger/asyncio_log_sink/errors.py
from __future__ import annotations
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


class AsyncioLogSinkErrorReason(StrEnum):
    EVENT_LOOP_UNAVAILABLE = "EVENT_LOOP_UNAVAILABLE"
    INVALID_LOG_SINK_STATE = "INVALID_LOG_SINK_STATE"
    ON_STARTING_HOOK_FAILED = "ON_STARTING_HOOK_FAILED"
    ON_STOPPED_HOOK_FAILED = "ON_STOPPED_HOOK_FAILED"
    QUEUE_OVERFLOW = "QUEUE_OVERFLOW"
    DISPATCHER_UNEXPECTEDLY_CANCELLED = "DISPATCHER_UNEXPECTEDLY_CANCELLED"
    UNEXPECTED_ERROR = "UNEXPECTED_ERROR"


class AsyncioLogSinkError(ReasonedError):
    pass


class AsyncioLogSinkEventLoopUnavailableError(AsyncioLogSinkError):
    def __init__(self) -> None:
        super().__init__(
            reason=AsyncioLogSinkErrorReason.EVENT_LOOP_UNAVAILABLE.value,
            message="unable to get a running event loop for asyncio log sink",
        )


class AsyncioLogSinkInvalidStateError(AsyncioLogSinkError):
    def __init__(
        self,
        sink_state: AsyncioLogSinkState,
        expected_states: tuple[AsyncioLogSinkState, ...],
        cause: Exception | None = None,
    ) -> None:
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
    def __init__(
        self,
        cause: Exception,
    ) -> None:
        super().__init__(
            reason=AsyncioLogSinkErrorReason.ON_STARTING_HOOK_FAILED.value,
            message=f"on starting hook failed -> {str(cause)}",
            cause=cause,
        )


class AsyncioLogSinkOnStoppedHookFailedError(AsyncioLogSinkError):
    def __init__(
        self,
        cause: Exception,
    ) -> None:
        super().__init__(
            reason=AsyncioLogSinkErrorReason.ON_STOPPED_HOOK_FAILED.value,
            message=f"on stopped hook failed -> {str(cause)}",
            cause=cause,
        )


class AsyncioLogSinkQueueOverflowError(AsyncioLogSinkError):
    def __init__(self) -> None:
        super().__init__(
            reason=AsyncioLogSinkErrorReason.QUEUE_OVERFLOW.value,
            message="queue overflow",
        )


class AsyncioLogSinkDispatcherCancelledError(AsyncioLogSinkError):
    def __init__(self) -> None:
        super().__init__(
            reason=AsyncioLogSinkErrorReason.DISPATCHER_UNEXPECTEDLY_CANCELLED.value,
            message="dispatcher unexpectedly cancelled",
        )


class AsyncioLogSinkUnexpectedError(AsyncioLogSinkError):
    def __init__(
        self,
        cause: Exception,
    ) -> None:
        super().__init__(
            reason=AsyncioLogSinkErrorReason.UNEXPECTED_ERROR.value,
            message=f"unexpected error -> {str(cause)}",
            cause=cause,
        )
