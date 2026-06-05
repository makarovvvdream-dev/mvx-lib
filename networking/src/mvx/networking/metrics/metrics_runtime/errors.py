# src/mvx/networking/metrics/metrics_runtime/errors.py
from __future__ import annotations

from typing import Any

from mvx.common.helpers.document_enum import document_enum

from enum import StrEnum

from mvx.common.errors import ReasonedError


from .common import MetricsRuntimeState

__all__ = (
    "MetricsRuntimeError",
    "MetricsRuntimeInvalidStateError",
    "MetricsRuntimeStartupError",
    "MetricsRuntimeShutdownError",
    "MetricsRuntimeLoopUnavailableError",
    "MetricsRuntimeRecorderError",
    "MetricsRuntimeRecorderStartupError",
    "MetricsRuntimeRecorderAlreadyExistsError",
    "MetricsRuntimeRecorderNotFoundError",
    "MetricsRuntimeRecorderStopError",
)


@document_enum
class AsyncioMetricsRuntimeErrorReason(StrEnum):
    """
    Reason codes used by `AsyncioMetricsRuntimeError` subclasses.
    """

    #: An operation was requested while the metrics runtime was in an invalid state.
    INVALID_RUNTIME_STATE = "INVALID_RUNTIME_STATE"

    #: ---
    STARTUP_ERROR = "STARTUP_ERROR"

    #: ---
    SHUTDOWN_ERROR = "SHUTDOWN_ERROR"

    #: A recorder was created without an available running event loop.
    EVENT_LOOP_UNAVAILABLE = "EVENT_LOOP_UNAVAILABLE"

    #: ---
    RECORDER_STARTUP_ERROR = "RECORDER_STARTUP_ERROR"

    #: ---
    RECORDER_ALREADY_EXISTS = "RECORDER_ALREADY_EXISTS"

    #: ---
    RECORDER_NOT_FOUND = "RECORDER_NOT_FOUND"

    #: ---
    RECORDER_STOP_ERROR = "RECORDER_STOP_ERROR"

    #: An unexpected error occurred inside the recorder runtime.
    UNEXPECTED_ERROR = "UNEXPECTED_ERROR"


class MetricsRuntimeError(ReasonedError): ...


class MetricsRuntimeInvalidStateError(MetricsRuntimeError):
    """
    Raised when a metrics runtime operation is not valid for the current lifecycle state.
    """

    def __init__(
        self,
        runtime_state: MetricsRuntimeState,
        expected_states: tuple[MetricsRuntimeState, ...],
        cause: Exception | None = None,
    ) -> None:
        """
        Create an invalid-state error.

        :param runtime_state: actual metrics runtime state.
        :param expected_states: states allowed for the requested operation.
        :param cause: optional underlying cause.
        """
        if len(expected_states) == 1:
            expected_states_str = expected_states[0].value
            msg = (
                f"invalid recorder state '{runtime_state.value}', expected '{expected_states_str}'"
            )
        else:
            expected_states_str = ", ".join(f"'{state.value}'" for state in expected_states)
            msg = (
                f"invalid recorder state '{runtime_state.value}', "
                f"expected one of: {expected_states_str}"
            )

        details = {
            "recorder_state": runtime_state.value,
            "expected_states": tuple(state.value for state in expected_states),
        }

        super().__init__(
            reason=AsyncioMetricsRuntimeErrorReason.INVALID_RUNTIME_STATE.value,
            message=msg,
            details=details,
            cause=cause,
        )


class MetricsRuntimeStartupError(MetricsRuntimeError):
    def __init__(
        self,
        cause: Exception | None = None,
    ) -> None:
        msg = "unable to start the metrics runtime"
        if cause is not None:
            msg += " -> " + str(cause)

        super().__init__(
            reason=AsyncioMetricsRuntimeErrorReason.STARTUP_ERROR.value,
            message=msg,
            cause=cause,
        )


class MetricsRuntimeShutdownError(MetricsRuntimeError):
    def __init__(
        self,
        details: dict[str, Any] | None = None,
        cause: Exception | None = None,
    ) -> None:

        msg = "unable to shutdown the metrics runtime"
        if cause is not None:
            msg += " -> " + str(cause)

        super().__init__(
            reason=AsyncioMetricsRuntimeErrorReason.SHUTDOWN_ERROR.value,
            message=msg,
            details=details,
            cause=cause,
        )


class MetricsRuntimeLoopUnavailableError(MetricsRuntimeError):
    def __init__(self) -> None:
        super().__init__(
            reason=AsyncioMetricsRuntimeErrorReason.EVENT_LOOP_UNAVAILABLE.value,
            message="unable to get a running event loop for metrics runtime",
        )


class MetricsRuntimeRecorderError(MetricsRuntimeError):
    def __init__(
        self,
        *,
        message: str,
        details: dict[str, Any] | None = None,
        cause: Exception | None = None,
        reason: str,
        recorder_id: str,
    ) -> None:
        self.recorder_id = recorder_id

        message = f"metrics runtime recorder error -> {message}"

        base_details = {
            "recorder_id": recorder_id,
        }
        if details is not None:
            base_details.update(details)

        super().__init__(
            message=message,
            details=base_details,
            cause=cause,
            reason=reason,
        )


class MetricsRuntimeRecorderStartupError(MetricsRuntimeRecorderError):
    def __init__(
        self,
        recorder_id: str,
        cause: Exception | None = None,
    ) -> None:
        msg = "unable to start the metrics recorder"
        if cause is not None:
            msg += " -> " + str(cause)

        super().__init__(
            message=msg,
            cause=cause,
            reason=AsyncioMetricsRuntimeErrorReason.RECORDER_STARTUP_ERROR,
            recorder_id=recorder_id,
        )


class MetricsRuntimeRecorderAlreadyExistsError(MetricsRuntimeRecorderError):
    def __init__(
        self,
        recorder_id: str,
    ) -> None:
        super().__init__(
            message=f"metrics recorder with '{recorder_id}' alrerady exists",
            reason=AsyncioMetricsRuntimeErrorReason.RECORDER_ALREADY_EXISTS,
            recorder_id=recorder_id,
        )


class MetricsRuntimeRecorderNotFoundError(MetricsRuntimeRecorderError):
    def __init__(
        self,
        recorder_id: str,
    ) -> None:
        super().__init__(
            message=f"metrics recorder with '{recorder_id}' not found",
            reason=AsyncioMetricsRuntimeErrorReason.RECORDER_NOT_FOUND,
            recorder_id=recorder_id,
        )


class MetricsRuntimeRecorderStopError(MetricsRuntimeRecorderError):
    def __init__(
        self,
        recorder_id: str,
        cause: Exception | None = None,
    ) -> None:
        msg = "unable to stop the metrics recorder"
        if cause is not None:
            msg += " -> " + str(cause)

        super().__init__(
            message=msg,
            cause=cause,
            reason=AsyncioMetricsRuntimeErrorReason.RECORDER_STOP_ERROR,
            recorder_id=recorder_id,
        )
