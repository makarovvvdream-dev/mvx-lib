# common/tests/test_logger/test_errors.py
from __future__ import annotations

from typing import Any, cast

from mvx.common.logger.errors import (
    LoggerError,
    LogContextError,
    LogContextResetError,
    LogContextUnableToLog,
    LogSinkCloseError,
    LogSinkConfigurationConflictError,
    LogSinkConfigurationError,
    LogSinkCreateError,
    LogSinkDescriptorBuildError,
)

# noinspection PyProtectedMember
from mvx.common.logger.errors import _describe_sink_class
from mvx.common.logger.models import (
    LogSinkClassProto,
    LogSinkDescriptor,
)


class FakeSink:
    pass


class SinkClassLike:
    pass


class FakeDescriptor:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def to_log_payload(self) -> dict[str, Any]:
        return self.payload


class PayloadProviderError(RuntimeError):
    def __init__(self, message: str, payload: dict[str, Any]) -> None:
        super().__init__(message)
        self.payload = payload

    def to_log_payload(self) -> dict[str, Any]:
        return self.payload


def as_sink_class(value: object) -> LogSinkClassProto:
    return cast(LogSinkClassProto, value)


def as_sink_descriptor(value: object) -> LogSinkDescriptor:
    return cast(LogSinkDescriptor, value)


def test_a01_logger_error_builds_expected_payload() -> None:
    err = LoggerError(
        message="logger failed",
        reason="LOGGER_FAILED",
        details={"step": "emit"},
    )

    payload = err.to_log_payload()

    assert isinstance(err, LoggerError)
    assert payload == {
        "kind": "LoggerError",
        "message": "logger failed",
        "reason": "LOGGER_FAILED",
        "details": {
            "step": "emit",
        },
    }


def test_b01_log_context_reset_error_builds_expected_payload() -> None:
    err = LogContextResetError(target="sink")

    payload = err.to_log_payload()

    assert isinstance(err, LogContextError)
    assert isinstance(err, LoggerError)
    assert payload == {
        "kind": "LogContextResetError",
        "message": "resetting 'sink' is not allowed for the root log context",
        "reason": "LOG_CONTEXT_RESET_NOT_ALLOWED_FOR_ROOT",
        "details": {
            "target": "sink",
        },
    }


def test_b02_log_context_unable_to_log_wraps_cause() -> None:
    cause = RuntimeError("write failed")

    err = LogContextUnableToLog(cause)

    payload = err.to_log_payload()

    assert isinstance(err, LogContextError)
    assert isinstance(err, LoggerError)
    assert payload == {
        "kind": "LogContextUnableToLog",
        "message": "unable to log event -> write failed",
        "reason": "LOG_CONTEXT_UNABLE_TO_LOG",
        "details": {},
        "cause": {
            "kind": "RuntimeError",
            "message": "write failed",
        },
    }


def test_c01_describe_sink_class_uses_module_and_qualname() -> None:
    result = _describe_sink_class(as_sink_class(FakeSink))

    assert result == f"{FakeSink.__module__}.{FakeSink.__qualname__}"


def test_c02_describe_sink_class_uses_module_and_name_when_qualname_is_missing() -> None:
    sink_cls = SinkClassLike()
    setattr(sink_cls, "__module__", "fake.module")
    setattr(sink_cls, "__qualname__", None)
    setattr(sink_cls, "__name__", "FakeSink")

    result = _describe_sink_class(as_sink_class(sink_cls))

    assert result == "fake.module.FakeSink"


def test_c03_describe_sink_class_uses_qualname_without_module() -> None:
    sink_cls = SinkClassLike()
    setattr(sink_cls, "__module__", None)
    setattr(sink_cls, "__qualname__", "FakeSink.Nested")
    setattr(sink_cls, "__name__", "FakeSink")

    result = _describe_sink_class(as_sink_class(sink_cls))

    assert result == "FakeSink.Nested"


def test_c04_describe_sink_class_uses_name_without_module_or_qualname() -> None:
    sink_cls = SinkClassLike()
    setattr(sink_cls, "__module__", None)
    setattr(sink_cls, "__qualname__", None)
    setattr(sink_cls, "__name__", "FakeSink")

    result = _describe_sink_class(as_sink_class(sink_cls))

    assert result == "FakeSink"


def test_c05_describe_sink_class_returns_unknown_when_name_parts_are_missing() -> None:
    sink_cls = SinkClassLike()
    setattr(sink_cls, "__module__", None)
    setattr(sink_cls, "__qualname__", None)
    setattr(sink_cls, "__name__", None)

    result = _describe_sink_class(as_sink_class(sink_cls))

    assert result == "<unknown>"


def test_d01_log_sink_configuration_error_builds_expected_payload() -> None:
    cause = RuntimeError("bad config")

    err = LogSinkConfigurationError(
        message="sink setup failed",
        reason="TEST_REASON",
        details={"sink_name": "file"},
        cause=cause,
    )

    payload = err.to_log_payload()

    assert isinstance(err, LogSinkConfigurationError)
    assert isinstance(err, LoggerError)
    assert payload == {
        "kind": "LogSinkConfigurationError",
        "message": "log sink configuration error -> sink setup failed",
        "reason": "TEST_REASON",
        "details": {
            "sink_name": "file",
        },
        "cause": {
            "kind": "RuntimeError",
            "message": "bad config",
        },
    }


def test_d02_log_sink_configuration_conflict_error_builds_expected_payload() -> None:
    existing_descriptor = FakeDescriptor(
        {
            "kind": "existing",
            "path": "/tmp/a.log",
        }
    )
    requested_descriptor = FakeDescriptor(
        {
            "kind": "requested",
            "path": "/tmp/b.log",
        }
    )

    err = LogSinkConfigurationConflictError(
        sink_name="file",
        existing_descriptor=as_sink_descriptor(existing_descriptor),
        requested_descriptor=as_sink_descriptor(requested_descriptor),
    )

    payload = err.to_log_payload()

    assert isinstance(err, LogSinkConfigurationError)
    assert isinstance(err, LoggerError)
    assert payload == {
        "kind": "LogSinkConfigurationConflictError",
        "message": (
            "log sink configuration error -> "
            "log sink 'file' is already configured with different settings"
        ),
        "reason": "LOG_SINK_CONFIGURATION_CONFLICT",
        "details": {
            "sink_name": "file",
            "existing_descriptor": {
                "kind": "existing",
                "path": "/tmp/a.log",
            },
            "requested_descriptor": {
                "kind": "requested",
                "path": "/tmp/b.log",
            },
        },
    }


def test_d03_log_sink_descriptor_build_error_builds_expected_payload() -> None:
    cause = RuntimeError("descriptor failed")

    err = LogSinkDescriptorBuildError(
        sink_name="file",
        sink_class=as_sink_class(FakeSink),
        cause=cause,
    )

    payload = err.to_log_payload()

    assert isinstance(err, LogSinkConfigurationError)
    assert isinstance(err, LoggerError)
    assert payload == {
        "kind": "LogSinkDescriptorBuildError",
        "message": (
            "log sink configuration error -> "
            "unable to build descriptor for log sink 'file' -> descriptor failed"
        ),
        "reason": "LOG_SINK_DESCRIPTOR_BUILD_FAILED",
        "details": {
            "sink_name": "file",
            "sink_class": f"{FakeSink.__module__}.{FakeSink.__qualname__}",
        },
        "cause": {
            "kind": "RuntimeError",
            "message": "descriptor failed",
        },
    }


def test_d04_log_sink_create_error_builds_expected_payload() -> None:
    cause = RuntimeError("create failed")

    err = LogSinkCreateError(
        sink_name="file",
        sink_class=as_sink_class(FakeSink),
        cause=cause,
    )

    payload = err.to_log_payload()

    assert isinstance(err, LogSinkConfigurationError)
    assert isinstance(err, LoggerError)
    assert payload == {
        "kind": "LogSinkCreateError",
        "message": (
            "log sink configuration error -> " "unable to create log sink 'file' -> create failed"
        ),
        "reason": "LOG_SINK_CREATE_FAILED",
        "details": {
            "sink_name": "file",
            "sink_class": f"{FakeSink.__module__}.{FakeSink.__qualname__}",
        },
        "cause": {
            "kind": "RuntimeError",
            "message": "create failed",
        },
    }


def test_d05_log_sink_close_error_builds_expected_payload_for_plain_exceptions() -> None:
    err = LogSinkCloseError(
        causes=(
            ("file", RuntimeError("file close failed")),
            ("stdout", ValueError("stdout close failed")),
        )
    )

    payload = err.to_log_payload()

    assert isinstance(err, LogSinkConfigurationError)
    assert isinstance(err, LoggerError)
    assert payload == {
        "kind": "LogSinkCloseError",
        "message": "log sink configuration error -> unable to close one or more log sinks",
        "reason": "LOG_SINK_CLOSE_FAILED",
        "details": {
            "errors": [
                {
                    "sink_name": "file",
                    "kind": "RuntimeError",
                    "message": "file close failed",
                },
                {
                    "sink_name": "stdout",
                    "kind": "ValueError",
                    "message": "stdout close failed",
                },
            ]
        },
    }


def test_d06_log_sink_close_error_uses_log_payload_provider_for_causes() -> None:
    cause = PayloadProviderError(
        "close failed",
        {
            "kind": "PayloadProviderError",
            "message": "payload message",
            "reason": "PAYLOAD_REASON",
            "details": {
                "step": "flush",
            },
        },
    )

    err = LogSinkCloseError(causes=(("file", cause),))

    payload = err.to_log_payload()

    assert payload == {
        "kind": "LogSinkCloseError",
        "message": "log sink configuration error -> unable to close one or more log sinks",
        "reason": "LOG_SINK_CLOSE_FAILED",
        "details": {
            "errors": [
                {
                    "sink_name": "file",
                    "kind": "PayloadProviderError",
                    "message": "payload message",
                    "reason": "PAYLOAD_REASON",
                    "details": {
                        "step": "flush",
                    },
                }
            ]
        },
    }


def test_d07_log_sink_close_error_supports_empty_causes() -> None:
    err = LogSinkCloseError(causes=())

    payload = err.to_log_payload()

    assert payload == {
        "kind": "LogSinkCloseError",
        "message": "log sink configuration error -> unable to close one or more log sinks",
        "reason": "LOG_SINK_CLOSE_FAILED",
        "details": {
            "errors": [],
        },
    }
