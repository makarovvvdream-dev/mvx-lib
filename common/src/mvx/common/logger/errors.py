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
    pass


# ---- LogContextError ---------------------------------------------------------------------


class LogContextError(LoggerError):
    pass


class _LogContextErrorReason(StrEnum):
    LOG_CONTEXT_RESET_NOT_ALLOWED_FOR_ROOT = "LOG_CONTEXT_RESET_NOT_ALLOWED_FOR_ROOT"
    LOG_CONTEXT_UNABLE_TO_LOG = "LOG_CONTEXT_UNABLE_TO_LOG"


class LogContextResetError(LogContextError):
    def __init__(self, target: str):
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
    def __init__(self, cause: Exception):
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
    def __init__(
        self,
        message: str,
        reason: str,
        details: dict[str, Any] | None = None,
        cause: Exception | None = None,
    ):
        msg = f"log sink configuration error -> {message}"

        super().__init__(message=msg, reason=reason, details=details, cause=cause)


class LogSinkConfigurationConflictError(LogSinkConfigurationError):
    def __init__(
        self,
        sink_name: str,
        existing_descriptor: LogSinkDescriptor,
        requested_descriptor: LogSinkDescriptor,
    ):
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
    def __init__(
        self,
        sink_name: str,
        sink_class: LogSinkClassProto,
        cause: Exception,
    ):
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
    def __init__(
        self,
        sink_name: str,
        sink_class: LogSinkClassProto,
        cause: Exception,
    ):
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
    def __init__(
        self,
        causes: tuple[tuple[str, Exception], ...],
    ):
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
    def __init__(
        self,
        sink_name: str,
        context_namespaces: tuple[str, ...],
    ):
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
