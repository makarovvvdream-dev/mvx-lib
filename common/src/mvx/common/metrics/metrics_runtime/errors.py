# src/mvx/common/metrics/metrics_runtime/errors.py
from __future__ import annotations

from typing import Any
from enum import StrEnum

from mvx.common.errors import ReasonedError, RuntimeUnexpectedError

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
    "MetricsRuntimeUnexpectedError",
)


class _MetricsRuntimeErrorReason(StrEnum):
    """
    Reason codes used by `MetricsRuntimeError` subclasses.
    """

    #: An operation was requested while the metrics runtime was in an invalid state.
    INVALID_RUNTIME_STATE = "INVALID_RUNTIME_STATE"

    #: The metrics runtime failed to start.
    STARTUP_ERROR = "STARTUP_ERROR"

    #: The metrics runtime failed to shut down.
    SHUTDOWN_ERROR = "SHUTDOWN_ERROR"

    #: The runtime event loop is not available.
    EVENT_LOOP_UNAVAILABLE = "EVENT_LOOP_UNAVAILABLE"

    #: A metrics recorder failed to start.
    RECORDER_STARTUP_ERROR = "RECORDER_STARTUP_ERROR"

    #: A recorder with the requested id already exists.
    RECORDER_ALREADY_EXISTS = "RECORDER_ALREADY_EXISTS"

    #: A recorder with the requested id was not found.
    RECORDER_NOT_FOUND = "RECORDER_NOT_FOUND"

    #: A metrics recorder failed to stop.
    RECORDER_STOP_ERROR = "RECORDER_STOP_ERROR"

    #: An unexpected metrics runtime error occurred.
    UNEXPECTED_ERROR = "UNEXPECTED_ERROR"


class MetricsRuntimeError(ReasonedError):
    """
    Base class for metrics runtime errors.
    """

    ...


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
            msg = f"invalid metrics runtime state '{runtime_state.value}', expected '{expected_states_str}'"
        else:
            expected_states_str = ", ".join(f"'{state.value}'" for state in expected_states)
            msg = (
                f"invalid metrics runtime state '{runtime_state.value}', "
                f"expected one of: {expected_states_str}"
            )

        details = {
            "runtime_state": runtime_state.value,
            "expected_states": tuple(state.value for state in expected_states),
        }

        super().__init__(
            reason=_MetricsRuntimeErrorReason.INVALID_RUNTIME_STATE.value,
            message=msg,
            details=details,
            cause=cause,
        )


class MetricsRuntimeStartupError(MetricsRuntimeError):
    """
    Raised when the metrics runtime cannot be started.

    This error wraps startup failures that prevent the runtime thread,
    event loop, or runtime infrastructure from reaching the running state.
    """

    def __init__(
        self,
        cause: Exception | None = None,
    ) -> None:
        """
        Create a metrics runtime startup error.

        :param cause: optional original startup exception.
        """
        msg = "unable to start the metrics runtime"
        if cause is not None:
            msg += " -> " + str(cause)

        super().__init__(
            reason=_MetricsRuntimeErrorReason.STARTUP_ERROR.value,
            message=msg,
            cause=cause,
        )


class MetricsRuntimeShutdownError(MetricsRuntimeError):
    """
    Raised when the metrics runtime cannot be shut down cleanly.

    This error wraps failures that occur while stopping recorders,
    terminating the runtime loop, or closing the runtime infrastructure.
    """

    def __init__(
        self,
        details: dict[str, Any] | None = None,
        cause: Exception | None = None,
    ) -> None:
        """
        Create a metrics runtime shutdown error.

        :param details: optional structured shutdown details.
        :param cause: optional original shutdown exception.
        """
        msg = "unable to shutdown the metrics runtime"
        if cause is not None:
            msg += " -> " + str(cause)

        super().__init__(
            reason=_MetricsRuntimeErrorReason.SHUTDOWN_ERROR.value,
            message=msg,
            details=details,
            cause=cause,
        )


class MetricsRuntimeLoopUnavailableError(MetricsRuntimeError):
    """
    Raised when the metrics runtime event loop is not available.

    This error is used when runtime code needs access to the dedicated
    metrics event loop, but the loop has not been created or is no longer
    available.
    """

    def __init__(self) -> None:
        """
        Create a metrics runtime loop-unavailable error.
        """
        super().__init__(
            reason=_MetricsRuntimeErrorReason.EVENT_LOOP_UNAVAILABLE.value,
            message="unable to get a running event loop for metrics runtime",
        )


class MetricsRuntimeRecorderError(MetricsRuntimeError):
    """
    Base class for metrics-runtime recorder errors.

    These errors describe failures related to recorder lifecycle or recorder
    registry operations managed by the metrics runtime.
    """

    def __init__(
        self,
        *,
        message: str,
        details: dict[str, Any] | None = None,
        cause: Exception | None = None,
        reason: str,
        recorder_id: str,
    ) -> None:
        """
        Create a metrics runtime recorder error.

        :param message: error message describing the recorder failure.
        :param details: optional structured recorder error details.
        :param cause: optional original exception.
        :param reason: machine-readable reason code.
        :param recorder_id: runtime recorder identifier.
        """
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
    """
    Raised when a metrics recorder cannot be started by the runtime.

    This error wraps failures raised while creating or starting a recorder
    inside the metrics runtime infrastructure.
    """

    def __init__(
        self,
        recorder_id: str,
        cause: Exception | None = None,
    ) -> None:
        """
        Create a metrics recorder startup error.

        :param recorder_id: runtime recorder identifier.
        :param cause: optional original recorder startup exception.
        """
        msg = "unable to start the metrics recorder"
        if cause is not None:
            msg += " -> " + str(cause)

        super().__init__(
            message=msg,
            cause=cause,
            reason=_MetricsRuntimeErrorReason.RECORDER_STARTUP_ERROR.value,
            recorder_id=recorder_id,
        )


class MetricsRuntimeRecorderAlreadyExistsError(MetricsRuntimeRecorderError):
    """
    Raised when a recorder with the requested id is already registered.

    Recorder ids are unique within one metrics runtime instance.
    """

    def __init__(
        self,
        recorder_id: str,
    ) -> None:
        """
        Create a recorder-already-exists error.

        :param recorder_id: runtime recorder identifier.
        """
        super().__init__(
            message=f"metrics recorder with '{recorder_id}' already exists",
            reason=_MetricsRuntimeErrorReason.RECORDER_ALREADY_EXISTS.value,
            recorder_id=recorder_id,
        )


class MetricsRuntimeRecorderNotFoundError(MetricsRuntimeRecorderError):
    """
    Raised when a recorder with the requested id is not registered.
    """

    def __init__(
        self,
        recorder_id: str,
    ) -> None:
        """
        Create a recorder-not-found error.

        :param recorder_id: runtime recorder identifier.
        """
        super().__init__(
            message=f"metrics recorder with '{recorder_id}' not found",
            reason=_MetricsRuntimeErrorReason.RECORDER_NOT_FOUND.value,
            recorder_id=recorder_id,
        )


class MetricsRuntimeRecorderStopError(MetricsRuntimeRecorderError):
    """
    Raised when a metrics recorder cannot be stopped by the runtime.

    This error wraps failures raised while stopping a recorder managed by
    the metrics runtime infrastructure.
    """

    def __init__(
        self,
        recorder_id: str,
        cause: Exception | None = None,
    ) -> None:
        """
        Create a metrics recorder stop error.

        :param recorder_id: runtime recorder identifier.
        :param cause: optional original recorder stop exception.
        """
        msg = "unable to stop the metrics recorder"
        if cause is not None:
            msg += " -> " + str(cause)

        super().__init__(
            message=msg,
            cause=cause,
            reason=_MetricsRuntimeErrorReason.RECORDER_STOP_ERROR.value,
            recorder_id=recorder_id,
        )


class MetricsRuntimeUnexpectedError(
    MetricsRuntimeError,
    RuntimeUnexpectedError,
):
    """
    Raised when the metrics runtime catches an unexpected internal failure.

    This error wraps failures that are not part of the normal metrics runtime
    control flow and marks them as unexpected runtime errors.
    """

    def __init__(
        self,
        message: str = "unexpected metrics runtime error",
        details: dict[str, Any] | None = None,
        cause: Exception | None = None,
    ) -> None:
        """
        Create an unexpected metrics runtime error.

        :param message: error message describing the unexpected failure.
        :param details: optional structured error details.
        :param cause: optional original unexpected exception.
        """
        if cause is not None:
            message += " -> " + str(cause)

        super().__init__(
            reason=_MetricsRuntimeErrorReason.UNEXPECTED_ERROR.value,
            message=message,
            details=details,
            cause=cause,
        )
