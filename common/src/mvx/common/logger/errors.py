# common/src/mvx/common/logger/errors.py
from ..errors import ReasonedError
from enum import StrEnum

__all__ = (
    "LoggerError",
    "LogSinkRegistryError",
    "LogSinkRegistryErrorReason",
    "LogContextError",
    "LogContextErrorReason",
)


class LoggerError(ReasonedError):
    pass


class LogSinkRegistryErrorReason(StrEnum):
    LOG_SINK_NOT_FOUND = "LOG_SINK_NOT_FOUND"
    LOG_SINK_ALREADY_REGISTERED_WITH_DIFFERENT_DESCRIPTOR = (
        "LOG_SINK_ALREADY_REGISTERED_WITH_DIFFERENT_DESCRIPTOR"
    )
    LOG_SINK_REGISTRY_IS_NOT_ACTIVE = "LOG_SINK_REGISTRY_IS_NOT_ACTIVE"
    LOG_SINK_CREATE_FAILED = "LOG_SINK_CREATE_FAILED"
    LOG_SINK_TERMINATOR_FAILED = "LOG_SINK_TERMINATOR_FAILED"


class LogSinkRegistryError(LoggerError):
    pass


class LogContextErrorReason(StrEnum):
    LOG_CONTEXT_SINK_NOT_CONFIGURED = "LOG_CONTEXT_SINK_NOT_CONFIGURED"
    LOG_CONTEXT_ALREADY_REGISTERED_WITH_DIFFERENT_SINK = (
        "LOG_CONTEXT_ALREADY_REGISTERED_WITH_DIFFERENT_SINK"
    )
    LOG_CONTEXT_INVALID_NAMESPACE = "LOG_CONTEXT_INVALID_NAMESPACE"


class LogContextError(LoggerError):
    pass
