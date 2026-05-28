# src/mvx/common/logger/errors.py
from ..errors import ReasonedError
from typing import Any
from enum import StrEnum


from .models import LogSinkDescriptor, LogSinkClassProto, LogPayloadProvider

__all__ = (
    "LoggerError",
    "LogContextError",
    "LogContextResetError",
    "LogContextUnableToLog",
    "LogSinkConfigurationError",
    "LogSinkConfigurationConflictError",
    "LogSinkDescriptorBuildError",
    "LogSinkCreateError",
    "LogSinkCloseError",
    "LogSinkIsInUseError",
)


# ==== Errors ==============================================================================


class LoggerError(ReasonedError):
    """
    Base class for logger-specific errors.
    """

    pass


# ---- LogContextError ---------------------------------------------------------------------


class LogContextError(LoggerError):
    """
    Base class for `LogContext` errors.
    """

    pass


class _LogContextErrorReason(StrEnum):
    LOG_CONTEXT_RESET_NOT_ALLOWED_FOR_ROOT = "LOG_CONTEXT_RESET_NOT_ALLOWED_FOR_ROOT"
    LOG_CONTEXT_UNABLE_TO_LOG = "LOG_CONTEXT_UNABLE_TO_LOG"


class LogContextResetError(LogContextError):
    """
    Raised when a root log context component cannot be reset.

    Root contexts have no parent fallback, so mandatory infrastructure such as
    sink, payload processor, and logging error handling policy cannot be reset.
    """

    def __init__(self, target: str) -> None:
        """
        Create a root-context reset error.

        :param target: component name that cannot be reset.
        """
        msg = f"resetting '{target}' is not allowed for the root log context"
        details = {
            "target": target,
        }

        super().__init__(
            message=msg,
            reason=_LogContextErrorReason.LOG_CONTEXT_RESET_NOT_ALLOWED_FOR_ROOT.value,
            details=details,
        )


class LogContextUnableToLog(LogContextError):
    """
    Raised when a log context cannot deliver a prepared event.

    This error is raised when sink delivery fails and the effective
    `LogErrorHandlingPolicy` is `RAISE`.
    """

    def __init__(self, cause: Exception) -> None:
        """
        Create an unable-to-log error.

        :param cause: original sink delivery exception.
        """
        msg = f"unable to log event -> {str(cause)}"

        super().__init__(
            message=msg,
            reason=_LogContextErrorReason.LOG_CONTEXT_UNABLE_TO_LOG.value,
            cause=cause,
        )


# ---- LogSinkConfigurationError -----------------------------------------------------------


def _describe_sink_class(sink_cls: LogSinkClassProto) -> str:
    module = getattr(sink_cls, "__module__", None)
    qualname = getattr(sink_cls, "__qualname__", None)

    if isinstance(module, str) and isinstance(qualname, str):
        return f"{module}.{qualname}"

    name = getattr(sink_cls, "__name__", None)

    if isinstance(module, str) and isinstance(name, str):
        return f"{module}.{name}"

    if isinstance(qualname, str):
        return qualname

    if isinstance(name, str):
        return name

    return "<unknown>"


class _LogSinkConfigurationErrorReason(StrEnum):
    LOG_SINK_CONFIGURATION_CONFLICT = "LOG_SINK_CONFIGURATION_CONFLICT"
    LOG_SINK_DESCRIPTOR_BUILD_FAILED = "LOG_SINK_DESCRIPTOR_BUILD_FAILED"
    LOG_SINK_CREATE_FAILED = "LOG_SINK_CREATE_FAILED"
    LOG_SINK_CLOSE_FAILED = "LOG_SINK_CLOSE_FAILED"
    LOG_SINK_IS_IN_USE = "LOG_SINK_IS_IN_USE"


class LogSinkConfigurationError(LoggerError):
    """
    Base class for package-level sink configuration errors.
    """

    def __init__(
        self,
        message: str,
        reason: str,
        details: dict[str, Any] | None = None,
        cause: Exception | None = None,
    ) -> None:
        """
        Create a sink configuration error.

        :param message: error message describing the configuration failure.
        :param reason: machine-readable reason code.
        :param details: optional structured error details.
        :param cause: optional original exception.
        """
        msg = f"log sink configuration error -> {message}"

        super().__init__(message=msg, reason=reason, details=details, cause=cause)


class LogSinkConfigurationConflictError(LogSinkConfigurationError):
    """
    Raised when a sink name is already registered with a different descriptor.

    The package-level sink registry treats same-name, same-descriptor registration
    as idempotent. Same-name, different-descriptor registration is a conflict.
    """

    def __init__(
        self,
        sink_name: str,
        existing_descriptor: LogSinkDescriptor,
        requested_descriptor: LogSinkDescriptor,
    ) -> None:
        """
        Create a sink configuration conflict error.

        :param sink_name: package-level sink name.
        :param existing_descriptor: descriptor already registered for this name.
        :param requested_descriptor: descriptor requested by the new configuration.
        """
        msg = f"log sink '{sink_name}' is already configured with different settings"
        details = {
            "sink_name": sink_name,
            "existing_descriptor": existing_descriptor.to_log_payload(),
            "requested_descriptor": requested_descriptor.to_log_payload(),
        }

        super().__init__(
            message=msg,
            reason=_LogSinkConfigurationErrorReason.LOG_SINK_CONFIGURATION_CONFLICT.value,
            details=details,
        )


class LogSinkDescriptorBuildError(LogSinkConfigurationError):
    """
    Raised when sink descriptor construction fails.

    This error wraps an exception raised by `sink_cls.build_descriptor(...)`.
    """

    def __init__(
        self,
        sink_name: str,
        sink_class: LogSinkClassProto,
        cause: Exception,
    ) -> None:
        """
        Create a descriptor-build failure error.

        :param sink_name: package-level sink name.
        :param sink_class: sink class used for configuration.
        :param cause: original descriptor construction exception.
        """
        msg = f"unable to build descriptor for log sink '{sink_name}' -> {str(cause)}"
        details = {
            "sink_name": sink_name,
            "sink_class": _describe_sink_class(sink_class),
        }

        super().__init__(
            message=msg,
            reason=_LogSinkConfigurationErrorReason.LOG_SINK_DESCRIPTOR_BUILD_FAILED.value,
            details=details,
            cause=cause,
        )


class LogSinkCreateError(LogSinkConfigurationError):
    """
    Raised when sink creation fails.

    This error wraps an exception raised by `sink_cls.create(...)`.
    """

    def __init__(
        self,
        sink_name: str,
        sink_class: LogSinkClassProto,
        cause: Exception,
    ) -> None:
        """
        Create a sink creation failure error.

        :param sink_name: package-level sink name.
        :param sink_class: sink class used for configuration.
        :param cause: original sink creation exception.
        """
        msg = f"unable to create log sink '{sink_name}' -> {str(cause)}"
        details = {
            "sink_name": sink_name,
            "sink_class": _describe_sink_class(sink_class),
        }

        super().__init__(
            message=msg,
            reason=_LogSinkConfigurationErrorReason.LOG_SINK_CREATE_FAILED.value,
            details=details,
            cause=cause,
        )


class LogSinkCloseError(LogSinkConfigurationError):
    """
    Raised when one or more package-level sinks cannot be closed.

    The error contains structured details for every sink terminator that failed.
    """

    def __init__(
        self,
        causes: tuple[tuple[str, Exception], ...],
    ) -> None:
        """
        Create a sink close failure error.

        :param causes: sink names and exceptions raised by their terminators.
        """
        msg = "unable to close one or more log sinks"

        payload_parts: list[dict[str, Any]] = []
        for sink_name, cause in causes:
            payload: dict[str, Any] = {"sink_name": sink_name}

            if isinstance(cause, LogPayloadProvider):
                payload.update(cause.to_log_payload())
            else:
                payload.update(
                    {
                        "kind": cause.__class__.__name__,
                        "message": str(cause),
                    }
                )

            payload_parts.append(payload)

        details = {"errors": payload_parts}

        super().__init__(
            message=msg,
            reason=_LogSinkConfigurationErrorReason.LOG_SINK_CLOSE_FAILED.value,
            details=details,
        )


class LogSinkIsInUseError(LogSinkConfigurationError):
    """
    Raised when a package-level sink cannot be closed because it is still in use.

    A sink is considered in use when it is locally assigned to one or more
    registered log contexts.
    """

    def __init__(
        self,
        sink_name: str,
        context_namespaces: tuple[str, ...],
    ) -> None:
        """
        Create a sink-in-use error.

        :param sink_name: package-level sink name.
        :param context_namespaces: namespaces of contexts that locally use the sink.
        """
        context_namespaces_str = ", ".join(context_namespaces)
        msg = f"log sink '{sink_name}' is in use by log contexts: {context_namespaces_str}"
        details = {
            "sink_name": sink_name,
            "context_namespaces": context_namespaces,
        }

        super().__init__(
            message=msg,
            reason=_LogSinkConfigurationErrorReason.LOG_SINK_IS_IN_USE.value,
            details=details,
        )
