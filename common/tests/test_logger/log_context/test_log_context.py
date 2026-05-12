# tests/test_logger/log_context/test_log_context.py

from __future__ import annotations

import sys
import threading
import time
from collections.abc import Callable
from typing import Any, cast

import pytest

from mvx.common.logger.log_context.log_context import (
    DEFAULT_MAX_ITEMS,
    DEFAULT_MAX_STR_LEN,
    ERR_LOGGED_FLAG,
    LogContext,
    LogErrorHandlingPolicy,
    LogVerbosityLevel,
)
from mvx.common.logger.errors import LogContextResetError, LogContextUnableToLog
from mvx.common.logger.models import LogEvent, LogEventPolicy, LogLevel, LogSinkProto


class RecordingLogSink:
    def __init__(self) -> None:
        self.events: list[LogEvent] = []
        self.raise_exc: Exception | None = None

    def log(self, event: LogEvent) -> None:
        if self.raise_exc is not None:
            raise self.raise_exc

        self.events.append(event)


class RecordingEventPolicy:
    def __init__(self, *, enabled: bool = True) -> None:
        self.enabled = enabled
        self.checked_events: list[str] = []

    def is_event_enabled(self, event: str) -> bool:
        self.checked_events.append(event)
        return self.enabled


class User:
    def __init__(self, name: str) -> None:
        self.name = name


def make_sink() -> RecordingLogSink:
    return RecordingLogSink()


def make_root_context(
    *,
    namespace: str | None = "test.ns",
    log_sink: RecordingLogSink | None = None,
    event_policy: RecordingEventPolicy | None = None,
    verbosity_level: LogVerbosityLevel = LogVerbosityLevel.NORMAL,
    max_str_len: int | None = None,
    max_items: int | None = None,
    log_error_handling_policy: LogErrorHandlingPolicy | None = None,
) -> LogContext:
    sink = log_sink if log_sink is not None else make_sink()

    return LogContext(
        namespace=namespace,
        log_sink=cast(LogSinkProto, sink),
        event_policy=cast(LogEventPolicy | None, event_policy),
        verbosity_level=verbosity_level,
        max_str_len=max_str_len,
        max_items=max_items,
        log_error_handling_policy=log_error_handling_policy,
    )


def make_child_context(
    parent: LogContext,
    *,
    namespace: str | None = "child.ns",
    log_sink: RecordingLogSink | None = None,
    event_policy: RecordingEventPolicy | None = None,
    verbosity_level: LogVerbosityLevel | None = None,
    max_str_len: int | None = None,
    max_items: int | None = None,
    log_error_handling_policy: LogErrorHandlingPolicy | None = None,
) -> LogContext:
    return LogContext(
        namespace=namespace,
        parent=parent,
        log_sink=cast(LogSinkProto | None, log_sink),
        event_policy=cast(LogEventPolicy | None, event_policy),
        verbosity_level=verbosity_level,
        max_str_len=max_str_len,
        max_items=max_items,
        log_error_handling_policy=log_error_handling_policy,
    )


def make_user_resolver(value: Any) -> Callable[[Any, str], dict[str, Any]] | None:
    if isinstance(value, User):
        return lambda obj, verbosity_level: {
            "kind": "user",
            "name": obj.name,
            "verbosity_level": verbosity_level,
        }

    return None


# ---------- A: constructor / root context validation ----------


def test_a01_root_context_requires_log_sink() -> None:
    with pytest.raises(ValueError):
        # noinspection PyArgumentList
        LogContext(
            verbosity_level=LogVerbosityLevel.NORMAL,
        )


def test_a02_root_context_requires_verbosity_level() -> None:
    with pytest.raises(ValueError):
        # noinspection PyArgumentList
        LogContext(
            log_sink=cast(LogSinkProto, make_sink()),
        )


def test_a03_root_context_uses_default_limits_and_error_policy() -> None:
    ctx = make_root_context(
        max_str_len=None,
        max_items=None,
        log_error_handling_policy=None,
    )

    assert ctx.max_str_len == DEFAULT_MAX_STR_LEN
    assert ctx.max_items == DEFAULT_MAX_ITEMS
    assert ctx.log_error_handling_policy is LogErrorHandlingPolicy.RAISE


def test_a04_root_context_accepts_custom_limits_and_error_policy() -> None:
    ctx = make_root_context(
        max_str_len=50,
        max_items=5,
        log_error_handling_policy=LogErrorHandlingPolicy.IGNORE,
    )

    assert ctx.max_str_len == 50
    assert ctx.max_items == 5
    assert ctx.log_error_handling_policy is LogErrorHandlingPolicy.IGNORE


def test_a05_namespace_is_stripped() -> None:
    ctx = make_root_context(namespace="  test.ns  ")

    assert ctx.namespace == "test.ns"


def test_a06_missing_namespace_returns_not_defined() -> None:
    ctx = make_root_context(namespace=None)

    assert ctx.namespace == "<not defined>"


def test_a07_non_string_namespace_fails() -> None:
    with pytest.raises(TypeError):
        LogContext(
            namespace=cast(Any, 123),
            log_sink=cast(LogSinkProto, make_sink()),
            verbosity_level=LogVerbosityLevel.NORMAL,
        )


def test_a08_invalid_parent_fails() -> None:
    with pytest.raises(TypeError):
        LogContext(
            parent=cast(Any, object()),
        )


def test_a09_root_context_reports_is_root_and_parent() -> None:
    ctx = make_root_context()

    assert ctx.is_root is True
    assert ctx.parent is None


def test_a10_child_context_reports_not_root_and_parent() -> None:
    parent = make_root_context()
    child = make_child_context(parent)

    assert child.is_root is False
    assert child.parent is parent


# ---------- B: constructor argument validation ----------


def test_b01_invalid_log_sink_type_fails() -> None:
    with pytest.raises(TypeError):
        LogContext(
            log_sink=cast(Any, object()),
            verbosity_level=LogVerbosityLevel.NORMAL,
        )


def test_b02_invalid_event_policy_type_fails() -> None:
    with pytest.raises(TypeError):
        LogContext(
            log_sink=cast(LogSinkProto, make_sink()),
            event_policy=cast(Any, object()),
            verbosity_level=LogVerbosityLevel.NORMAL,
        )


def test_b03_invalid_verbosity_level_type_fails() -> None:
    with pytest.raises(TypeError):
        LogContext(
            log_sink=cast(LogSinkProto, make_sink()),
            verbosity_level=cast(Any, "NORMAL"),
        )


def test_b04_invalid_max_str_len_type_fails() -> None:
    with pytest.raises(TypeError):
        LogContext(
            log_sink=cast(LogSinkProto, make_sink()),
            verbosity_level=LogVerbosityLevel.NORMAL,
            max_str_len=cast(Any, "200"),
        )


@pytest.mark.parametrize("value", [0, -1])
def test_b05_invalid_max_str_len_value_fails(value: int) -> None:
    with pytest.raises(ValueError):
        LogContext(
            log_sink=cast(LogSinkProto, make_sink()),
            verbosity_level=LogVerbosityLevel.NORMAL,
            max_str_len=value,
        )


def test_b06_invalid_max_items_type_fails() -> None:
    with pytest.raises(TypeError):
        LogContext(
            log_sink=cast(LogSinkProto, make_sink()),
            verbosity_level=LogVerbosityLevel.NORMAL,
            max_items=cast(Any, "10"),
        )


@pytest.mark.parametrize("value", [0, -1])
def test_b07_invalid_max_items_value_fails(value: int) -> None:
    with pytest.raises(ValueError):
        LogContext(
            log_sink=cast(LogSinkProto, make_sink()),
            verbosity_level=LogVerbosityLevel.NORMAL,
            max_items=value,
        )


def test_b08_invalid_log_error_handling_policy_type_fails() -> None:
    with pytest.raises(TypeError):
        LogContext(
            log_sink=cast(LogSinkProto, make_sink()),
            verbosity_level=LogVerbosityLevel.NORMAL,
            log_error_handling_policy=cast(Any, "RAISE"),
        )


# ---------- C: parent / child inheritance semantics ----------


def test_c01_child_inherits_log_sink_from_parent() -> None:
    parent_sink = make_sink()
    parent = make_root_context(log_sink=parent_sink)
    child = make_child_context(parent)

    assert child.log_sink is parent.log_sink


def test_c02_child_can_override_log_sink() -> None:
    parent = make_root_context()
    child_sink = make_sink()
    child = make_child_context(parent, log_sink=child_sink)

    assert child.log_sink is child_sink


def test_c03_child_reset_log_sink_restores_parent_sink() -> None:
    parent_sink = make_sink()
    child_sink = make_sink()
    parent = make_root_context(log_sink=parent_sink)
    child = make_child_context(parent, log_sink=child_sink)

    child.reset_log_sink()

    assert child.log_sink is parent.log_sink


def test_c04_root_reset_log_sink_fails() -> None:
    parent = make_root_context()

    with pytest.raises(LogContextResetError):
        parent.reset_log_sink()


def test_c05_child_inherits_verbosity_level() -> None:
    parent = make_root_context(verbosity_level=LogVerbosityLevel.MAXIMUM)
    child = make_child_context(parent)

    assert child.verbosity_level == LogVerbosityLevel.MAXIMUM.value


def test_c06_child_can_override_verbosity_level() -> None:
    parent = make_root_context(verbosity_level=LogVerbosityLevel.NORMAL)
    child = make_child_context(parent, verbosity_level=LogVerbosityLevel.MINIMAL)

    assert child.verbosity_level == LogVerbosityLevel.MINIMAL.value


def test_c07_child_reset_verbosity_level_restores_parent_value() -> None:
    parent = make_root_context(verbosity_level=LogVerbosityLevel.MAXIMUM)
    child = make_child_context(parent, verbosity_level=LogVerbosityLevel.MINIMAL)

    child.reset_verbosity_level()

    assert child.verbosity_level == LogVerbosityLevel.MAXIMUM.value


def test_c08_root_reset_verbosity_level_fails() -> None:
    parent = make_root_context()

    with pytest.raises(LogContextResetError):
        parent.reset_verbosity_level()


def test_c09_child_inherits_max_str_len() -> None:
    parent = make_root_context(max_str_len=50)
    child = make_child_context(parent)

    assert child.max_str_len == 50


def test_c10_child_can_override_max_str_len() -> None:
    parent = make_root_context(max_str_len=50)
    child = make_child_context(parent, max_str_len=10)

    assert child.max_str_len == 10


def test_c11_child_reset_max_str_len_restores_parent_value() -> None:
    parent = make_root_context(max_str_len=50)
    child = make_child_context(parent, max_str_len=10)

    child.reset_max_str_len()

    assert child.max_str_len == 50


def test_c12_root_reset_max_str_len_restores_default() -> None:
    root = make_root_context(max_str_len=50)

    root.reset_max_str_len()

    assert root.max_str_len == DEFAULT_MAX_STR_LEN


def test_c13_child_inherits_max_items() -> None:
    parent = make_root_context(max_items=5)
    child = make_child_context(parent)

    assert child.max_items == 5


def test_c14_child_can_override_max_items() -> None:
    parent = make_root_context(max_items=5)
    child = make_child_context(parent, max_items=2)

    assert child.max_items == 2


def test_c15_child_reset_max_items_restores_parent_value() -> None:
    parent = make_root_context(max_items=5)
    child = make_child_context(parent, max_items=2)

    child.reset_max_items()

    assert child.max_items == 5


def test_c16_root_reset_max_items_restores_default() -> None:
    root = make_root_context(max_items=5)

    root.reset_max_items()

    assert root.max_items == DEFAULT_MAX_ITEMS


def test_c17_child_inherits_log_error_handling_policy() -> None:
    parent = make_root_context(log_error_handling_policy=LogErrorHandlingPolicy.IGNORE)
    child = make_child_context(parent)

    assert child.log_error_handling_policy is LogErrorHandlingPolicy.IGNORE


def test_c18_child_can_override_log_error_handling_policy() -> None:
    parent = make_root_context(log_error_handling_policy=LogErrorHandlingPolicy.RAISE)
    child = make_child_context(
        parent,
        log_error_handling_policy=LogErrorHandlingPolicy.PRINT_STDERR,
    )

    assert child.log_error_handling_policy is LogErrorHandlingPolicy.PRINT_STDERR


def test_c19_child_reset_log_error_handling_policy_restores_parent_value() -> None:
    parent = make_root_context(log_error_handling_policy=LogErrorHandlingPolicy.IGNORE)
    child = make_child_context(
        parent,
        log_error_handling_policy=LogErrorHandlingPolicy.PRINT_STDERR,
    )

    child.reset_log_error_handling_policy()

    assert child.log_error_handling_policy is LogErrorHandlingPolicy.IGNORE


def test_c20_root_reset_log_error_handling_policy_fails() -> None:
    root = make_root_context()

    with pytest.raises(LogContextResetError):
        root.reset_log_error_handling_policy()


def test_c21_child_inherits_log_adapter_resolver() -> None:
    parent = make_root_context()
    parent.set_log_adapter_resolver(make_user_resolver)
    child = make_child_context(parent)

    assert child.log_adapter_resolver is make_user_resolver


def test_c22_child_can_override_log_adapter_resolver() -> None:
    parent = make_root_context()
    parent.set_log_adapter_resolver(make_user_resolver)

    def child_resolver(value: Any) -> None:
        _ = value
        return None

    child = make_child_context(parent)
    child.set_log_adapter_resolver(child_resolver)

    assert child.log_adapter_resolver is child_resolver


def test_c23_child_reset_log_adapter_resolver_restores_parent_resolver() -> None:
    parent = make_root_context()
    parent.set_log_adapter_resolver(make_user_resolver)

    def child_resolver(value: Any) -> None:
        _ = value
        return None

    child = make_child_context(parent)
    child.set_log_adapter_resolver(child_resolver)

    child.reset_log_adapter_resolver()

    assert child.log_adapter_resolver is make_user_resolver


def test_c24_root_log_adapter_resolver_is_none_by_default() -> None:
    root = make_root_context()

    assert root.log_adapter_resolver is None


# ---------- D: event policy semantics ----------


def test_d01_event_policy_none_enables_all_events() -> None:
    ctx = make_root_context()

    assert ctx.event_policy is None
    assert ctx.is_event_enabled("event.x") is True


def test_d02_event_policy_is_used_when_present() -> None:
    policy = RecordingEventPolicy(enabled=False)
    ctx = make_root_context(event_policy=policy)

    assert ctx.is_event_enabled("event.x") is False
    assert policy.checked_events == ["event.x"]


def test_d03_set_event_policy_is_used() -> None:
    policy = RecordingEventPolicy(enabled=False)
    ctx = make_root_context()

    ctx.set_event_policy(cast(LogEventPolicy, policy))

    assert ctx.event_policy is policy
    assert ctx.is_event_enabled("event.x") is False


def test_d04_reset_event_policy_enables_all_events() -> None:
    ctx = make_root_context(event_policy=RecordingEventPolicy(enabled=False))

    ctx.reset_event_policy()

    assert ctx.event_policy is None
    assert ctx.is_event_enabled("event.x") is True


def test_d05_child_does_not_inherit_parent_event_policy() -> None:
    parent = make_root_context(event_policy=RecordingEventPolicy(enabled=False))
    child = make_child_context(parent)

    assert child.event_policy is None
    assert child.is_event_enabled("event.x") is True


def test_d06_disabled_event_is_not_logged() -> None:
    sink = make_sink()
    ctx = make_root_context(
        log_sink=sink,
        event_policy=RecordingEventPolicy(enabled=False),
    )

    ctx.log_event(
        event="event.x",
        level=LogLevel.INFO,
        payload={"x": 1},
    )

    assert sink.events == []


def test_d07_enabled_event_is_logged() -> None:
    sink = make_sink()
    policy = RecordingEventPolicy(enabled=True)
    ctx = make_root_context(
        log_sink=sink,
        event_policy=policy,
    )

    ctx.log_event(
        event="event.x",
        level=LogLevel.INFO,
        payload={"x": 1},
    )

    assert policy.checked_events == ["event.x"]
    assert len(sink.events) == 1


# ---------- E: log_event construction ----------


def test_e01_log_event_builds_log_event_with_defaults() -> None:
    sink = make_sink()
    ctx = make_root_context(log_sink=sink, namespace="test.ns")

    before = time.time()

    ctx.log_event(
        event="event.x",
        level=LogLevel.INFO,
        payload={"x": 1},
    )

    after = time.time()

    assert len(sink.events) == 1

    logged = sink.events[0]

    assert logged.level is LogLevel.INFO
    assert logged.event_namespace == "test.ns"
    assert logged.event_name == "event.x"
    assert logged.event_type is None
    assert before <= logged.timestamp <= after
    assert logged.entity_id is None
    assert logged.payload == {"x": 1}
    assert logged.source_path is None
    assert logged.source_line is None
    assert logged.source_func is None


def test_e02_log_event_uses_explicit_metadata() -> None:
    sink = make_sink()
    ctx = make_root_context(log_sink=sink)

    ctx.log_event(
        event="event.x",
        level=LogLevel.WARNING,
        payload={"x": 1},
        event_namespace="custom.ns",
        event_type="operation",
        entity_id="entity-1",
        source_path="/tmp/a.py",
        source_line=10,
        source_func="func",
    )

    logged = sink.events[0]

    assert logged.level is LogLevel.WARNING
    assert logged.event_namespace == "custom.ns"
    assert logged.event_name == "event.x"
    assert logged.event_type == "operation"
    assert logged.entity_id == "entity-1"
    assert logged.payload == {"x": 1}
    assert logged.source_path == "/tmp/a.py"
    assert logged.source_line == 10
    assert logged.source_func == "func"


def test_e03_log_event_uses_not_defined_namespace_when_context_namespace_missing() -> None:
    sink = make_sink()
    ctx = make_root_context(log_sink=sink, namespace=None)

    ctx.log_event(
        event="event.x",
        level=LogLevel.INFO,
        payload={},
    )

    logged = sink.events[0]

    assert logged.event_namespace == "<not defined>"


def test_e04_log_event_passes_payload_verbatim() -> None:
    sink = make_sink()
    ctx = make_root_context(log_sink=sink)
    payload = {"x": object()}

    ctx.log_event(
        event="event.x",
        level=LogLevel.INFO,
        payload=payload,
    )

    assert sink.events[0].payload is payload


# ---------- F: level helper methods ----------


def assert_single_logged_event(
    sink: RecordingLogSink,
    *,
    expected_level: LogLevel,
) -> None:
    assert len(sink.events) == 1

    logged = sink.events[0]

    assert logged.level is expected_level
    assert logged.event_namespace == "ns"
    assert logged.event_name == "event.x"
    assert logged.event_type == "type"
    assert logged.entity_id == "id"
    assert logged.source_path == "path"
    assert logged.source_line == 123
    assert logged.source_func == "func"
    assert logged.payload == {"x": 1}


def test_f01_log_debug_event_uses_debug_level() -> None:
    sink = make_sink()
    ctx = make_root_context(log_sink=sink)

    ctx.log_debug_event(
        event="event.x",
        payload={"x": 1},
        event_namespace="ns",
        event_type="type",
        entity_id="id",
        source_path="path",
        source_line=123,
        source_func="func",
    )

    assert_single_logged_event(sink, expected_level=LogLevel.DEBUG)


def test_f02_log_info_event_uses_info_level() -> None:
    sink = make_sink()
    ctx = make_root_context(log_sink=sink)

    ctx.log_info_event(
        event="event.x",
        payload={"x": 1},
        event_namespace="ns",
        event_type="type",
        entity_id="id",
        source_path="path",
        source_line=123,
        source_func="func",
    )

    assert_single_logged_event(sink, expected_level=LogLevel.INFO)


def test_f03_log_warning_event_uses_warning_level() -> None:
    sink = make_sink()
    ctx = make_root_context(log_sink=sink)

    ctx.log_warning_event(
        event="event.x",
        payload={"x": 1},
        event_namespace="ns",
        event_type="type",
        entity_id="id",
        source_path="path",
        source_line=123,
        source_func="func",
    )

    assert_single_logged_event(sink, expected_level=LogLevel.WARNING)


def test_f04_log_error_event_uses_error_level() -> None:
    sink = make_sink()
    ctx = make_root_context(log_sink=sink)

    ctx.log_error_event(
        event="event.x",
        payload={"x": 1},
        event_namespace="ns",
        event_type="type",
        entity_id="id",
        source_path="path",
        source_line=123,
        source_func="func",
    )

    assert_single_logged_event(sink, expected_level=LogLevel.ERROR)


# ---------- G: log sink failure handling policy ----------


def test_g01_log_sink_error_raise_policy_raises_log_context_unable_to_log() -> None:
    sink = make_sink()
    sink.raise_exc = RuntimeError("boom")
    ctx = make_root_context(
        log_sink=sink,
        log_error_handling_policy=LogErrorHandlingPolicy.RAISE,
    )

    with pytest.raises(LogContextUnableToLog) as exc_info:
        ctx.log_info_event("event.x", {"x": 1})

    assert isinstance(exc_info.value.__cause__, RuntimeError)


def test_g02_log_sink_error_ignore_policy_suppresses_error() -> None:
    sink = make_sink()
    sink.raise_exc = RuntimeError("boom")
    ctx = make_root_context(
        log_sink=sink,
        log_error_handling_policy=LogErrorHandlingPolicy.IGNORE,
    )

    ctx.log_info_event("event.x", {"x": 1})

    assert sink.events == []


def test_g03_log_sink_error_print_stderr_policy_prints_once(
    capsys: pytest.CaptureFixture[str],
) -> None:
    sink = make_sink()
    sink.raise_exc = RuntimeError("boom")
    ctx = make_root_context(
        log_sink=sink,
        log_error_handling_policy=LogErrorHandlingPolicy.PRINT_STDERR,
    )

    ctx.log_info_event("event.x", {"x": 1})
    ctx.log_info_event("event.x", {"x": 1})

    captured = capsys.readouterr()

    assert captured.err.count("LogContext.log_event() failed") == 1


def test_g04_successful_log_resets_printed_error_flag(capsys: pytest.CaptureFixture[str]) -> None:
    sink = make_sink()
    ctx = make_root_context(
        log_sink=sink,
        log_error_handling_policy=LogErrorHandlingPolicy.PRINT_STDERR,
    )

    sink.raise_exc = RuntimeError("boom-1")
    ctx.log_info_event("event.x", {"x": 1})
    ctx.log_info_event("event.x", {"x": 1})

    sink.raise_exc = None
    ctx.log_info_event("event.x", {"x": 1})

    sink.raise_exc = RuntimeError("boom-2")
    ctx.log_info_event("event.x", {"x": 1})

    captured = capsys.readouterr()

    assert captured.err.count("LogContext.log_event() failed") == 2


# ---------- H: error payload building ----------


def test_h01_build_error_payload_uses_to_log_payload_dict() -> None:
    ctx = make_root_context()

    class CustomError(Exception):
        def to_log_payload(self) -> dict[str, Any]:
            return {"kind": "custom", "x": 1}

    assert ctx.build_error_payload(CustomError()) == {"kind": "custom", "x": 1}


def test_h02_build_error_payload_copies_to_log_payload_result() -> None:
    ctx = make_root_context()
    provided = {"kind": "custom"}

    class CustomError(Exception):
        def to_log_payload(self) -> dict[str, Any]:
            return provided

    result = ctx.build_error_payload(CustomError())

    assert result == provided
    assert result is not provided


def test_h03_build_error_payload_ignores_non_dict_to_log_payload() -> None:
    ctx = make_root_context()

    class CustomError(Exception):
        def to_log_payload(self) -> list[str]:
            return ["bad"]

    err = CustomError("boom")
    result = ctx.build_error_payload(err)

    assert result["kind"] == "CustomError"
    assert result["message"] == "boom"


def test_h04_build_error_payload_ignores_to_log_payload_exception() -> None:
    ctx = make_root_context()

    class CustomError(Exception):
        def to_log_payload(self) -> dict[str, Any]:
            raise RuntimeError("payload failed")

    err = CustomError("boom")
    result = ctx.build_error_payload(err)

    assert result["kind"] == "CustomError"
    assert result["message"] == "boom"


def test_h05_build_error_payload_includes_code_and_code_desc_when_present() -> None:
    ctx = make_root_context()

    class CustomError(Exception):
        code = "E001"
        code_desc = "Something failed"

    err = CustomError("boom")
    result = ctx.build_error_payload(err)

    assert result["code"] == "E001"
    assert result["code_desc"] == "Something failed"
    assert result["kind"] == "CustomError"
    assert result["message"] == "boom"


def test_h06_build_error_payload_omits_code_fields_when_missing() -> None:
    ctx = make_root_context()

    err = RuntimeError("boom")
    result = ctx.build_error_payload(err)

    assert "code" not in result
    assert "code_desc" not in result
    assert result["kind"] == "RuntimeError"
    assert result["message"] == "boom"


# ---------- I: error logged marker ----------


def test_i01_is_error_logged_false_by_default() -> None:
    ctx = make_root_context()
    err = RuntimeError("boom")

    assert ctx.is_error_logged(err) is False


def test_i02_mark_error_logged_sets_marker() -> None:
    ctx = make_root_context()
    err = RuntimeError("boom")

    ctx.mark_error_logged(err)

    assert ctx.is_error_logged(err) is True
    assert getattr(err, ERR_LOGGED_FLAG) is True


def test_i03_is_error_logged_suppresses_getattr_exception() -> None:
    ctx = make_root_context()

    class BrokenGetattrError(Exception):
        def __getattribute__(self, name: str) -> Any:
            if name == ERR_LOGGED_FLAG:
                raise RuntimeError("getattr failed")
            return super().__getattribute__(name)

    err = BrokenGetattrError("boom")

    assert ctx.is_error_logged(err) is False


def test_i04_mark_error_logged_suppresses_setattr_exception() -> None:
    ctx = make_root_context()

    class BrokenSetattrError(Exception):
        def __setattr__(self, name: str, value: Any) -> None:
            if name == ERR_LOGGED_FLAG:
                raise RuntimeError("setattr failed")
            super().__setattr__(name, value)

    err = BrokenSetattrError("boom")

    ctx.mark_error_logged(err)

    assert ctx.is_error_logged(err) is False


# ---------- J: normalization wrappers ----------


def test_j01_normalize_primitive_for_log_uses_context_max_str_len() -> None:
    ctx = make_root_context(max_str_len=3)

    assert ctx.normalize_primitive_for_log("abcdef") == "abc..."


def test_j02_normalize_value_for_log_uses_context_limits() -> None:
    ctx = make_root_context(max_str_len=3, max_items=2)

    result = ctx.normalize_value_for_log(["abcdef", "b", "c"])

    assert result == ["abc...", "b", "... (1 more)"]


def test_j03_normalize_value_for_log_unbounded_disables_item_limit() -> None:
    ctx = make_root_context(max_items=2)

    result = ctx.normalize_value_for_log([1, 2, 3], unbounded=True)

    assert result == [1, 2, 3]


def test_j04_normalize_list_for_log_uses_context_limits() -> None:
    ctx = make_root_context(max_str_len=3, max_items=2)

    result = ctx.normalize_list_for_log(["abcdef", "b", "c"])

    assert result == ["abc...", "b", "... (1 more)"]


def test_j05_normalize_list_for_log_unbounded_disables_item_limit() -> None:
    ctx = make_root_context(max_items=2)

    result = ctx.normalize_list_for_log([1, 2, 3], unbounded=True)

    assert result == [1, 2, 3]


def test_j06_normalize_dict_for_log_uses_context_limits() -> None:
    ctx = make_root_context(max_str_len=3, max_items=2)

    result = ctx.normalize_dict_for_log(
        {
            "abcdef": "abcdef",
            "b": "b",
            "c": "c",
        }
    )

    assert result == {
        "abc...": "abc...",
        "b": "b",
        "__more__": "1 more keys",
    }


def test_j07_normalize_dict_for_log_unbounded_disables_item_limit() -> None:
    ctx = make_root_context(max_items=2)

    result = ctx.normalize_dict_for_log(
        {
            "a": 1,
            "b": 2,
            "c": 3,
        },
        unbounded=True,
    )

    assert result == {
        "a": 1,
        "b": 2,
        "c": 3,
    }


def test_j08_normalization_uses_inherited_parent_limits() -> None:
    parent = make_root_context(max_items=2)
    child = make_child_context(parent)

    result = child.normalize_list_for_log([1, 2, 3])

    assert result == [1, 2, "... (1 more)"]


def test_j09_normalization_uses_child_overrides() -> None:
    parent = make_root_context(max_items=2)
    child = make_child_context(parent, max_items=3)

    result = child.normalize_list_for_log([1, 2, 3, 4])

    assert result == [1, 2, 3, "... (1 more)"]


def test_j10_normalization_uses_log_adapter_resolver() -> None:
    ctx = make_root_context()
    ctx.set_log_adapter_resolver(make_user_resolver)

    result = ctx.normalize_value_for_log(User("alice"))

    assert result == {
        "kind": "user",
        "name": "alice",
        "verbosity_level": "NORMAL",
    }


def test_j11_child_normalization_inherits_log_adapter_resolver() -> None:
    parent = make_root_context()
    parent.set_log_adapter_resolver(make_user_resolver)
    child = make_child_context(parent)

    result = child.normalize_value_for_log(User("alice"))

    assert result == {
        "kind": "user",
        "name": "alice",
        "verbosity_level": "NORMAL",
    }


# ---------- K: setter validation ----------


def test_k01_set_log_sink_none_fails() -> None:
    ctx = make_root_context()

    with pytest.raises(ValueError):
        ctx.set_log_sink(cast(Any, None))


def test_k02_set_log_sink_invalid_type_fails() -> None:
    ctx = make_root_context()

    with pytest.raises(TypeError):
        ctx.set_log_sink(cast(Any, object()))


def test_k03_set_event_policy_none_fails() -> None:
    ctx = make_root_context()

    with pytest.raises(ValueError):
        ctx.set_event_policy(cast(Any, None))


def test_k04_set_event_policy_invalid_type_fails() -> None:
    ctx = make_root_context()

    with pytest.raises(TypeError):
        ctx.set_event_policy(cast(Any, object()))


def test_k05_set_verbosity_level_none_fails() -> None:
    ctx = make_root_context()

    with pytest.raises(ValueError):
        ctx.set_verbosity_level(cast(Any, None))


def test_k06_set_verbosity_level_invalid_type_fails() -> None:
    ctx = make_root_context()

    with pytest.raises(TypeError):
        ctx.set_verbosity_level(cast(Any, "NORMAL"))


def test_k07_set_max_str_len_none_fails() -> None:
    ctx = make_root_context()

    with pytest.raises(ValueError):
        ctx.set_max_str_len(cast(Any, None))


def test_k08_set_max_str_len_invalid_type_fails() -> None:
    ctx = make_root_context()

    with pytest.raises(TypeError):
        ctx.set_max_str_len(cast(Any, "200"))


@pytest.mark.parametrize("value", [0, -1])
def test_k09_set_max_str_len_invalid_value_fails(value: int) -> None:
    ctx = make_root_context()

    with pytest.raises(ValueError):
        ctx.set_max_str_len(value)


def test_k10_set_max_items_none_fails() -> None:
    ctx = make_root_context()

    with pytest.raises(ValueError):
        ctx.set_max_items(cast(Any, None))


def test_k11_set_max_items_invalid_type_fails() -> None:
    ctx = make_root_context()

    with pytest.raises(TypeError):
        ctx.set_max_items(cast(Any, "10"))


@pytest.mark.parametrize("value", [0, -1])
def test_k12_set_max_items_invalid_value_fails(value: int) -> None:
    ctx = make_root_context()

    with pytest.raises(ValueError):
        ctx.set_max_items(value)


def test_k13_set_log_adapter_resolver_none_fails() -> None:
    ctx = make_root_context()

    with pytest.raises(ValueError):
        ctx.set_log_adapter_resolver(cast(Any, None))


def test_k14_set_log_adapter_resolver_non_callable_fails() -> None:
    ctx = make_root_context()

    with pytest.raises(TypeError):
        ctx.set_log_adapter_resolver(cast(Any, object()))


def test_k15_set_log_error_handling_policy_none_fails() -> None:
    ctx = make_root_context()

    with pytest.raises(ValueError):
        ctx.set_log_error_handling_policy(cast(Any, None))


def test_k16_set_log_error_handling_policy_invalid_type_fails() -> None:
    ctx = make_root_context()

    with pytest.raises(TypeError):
        ctx.set_log_error_handling_policy(cast(Any, "RAISE"))


# ---------- L: thread-safety smoke tests ----------


def test_l01_concurrent_getters_and_setters_do_not_fail() -> None:
    ctx = make_root_context()
    stop_reading = threading.Event()
    errors: list[BaseException] = []

    def reader() -> None:
        try:
            while not stop_reading.is_set():
                _ = ctx.namespace
                _ = ctx.log_sink
                _ = ctx.event_policy
                _ = ctx.verbosity_level
                _ = ctx.max_str_len
                _ = ctx.max_items
                _ = ctx.log_error_handling_policy
                _ = ctx.log_adapter_resolver
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=reader)
    thread.start()

    try:
        for index in range(100):
            ctx.set_max_str_len(index + 1)
            ctx.set_max_items(index + 1)
            ctx.set_verbosity_level(LogVerbosityLevel.NORMAL)
            ctx.set_log_error_handling_policy(LogErrorHandlingPolicy.RAISE)
            ctx.set_log_adapter_resolver(make_user_resolver)
            ctx.reset_log_adapter_resolver()
    finally:
        stop_reading.set()
        thread.join(timeout=2.0)

    assert not thread.is_alive()
    assert errors == []


def test_l02_concurrent_log_event_and_policy_updates_do_not_fail() -> None:
    sink = make_sink()
    ctx = make_root_context(log_sink=sink)
    stop_logging = threading.Event()
    errors: list[BaseException] = []

    def logger() -> None:
        try:
            while not stop_logging.is_set():
                ctx.log_info_event("event.x", {"x": 1})
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=logger)
    thread.start()

    try:
        for index in range(100):
            ctx.set_event_policy(cast(LogEventPolicy, RecordingEventPolicy(enabled=index % 2 == 0)))
            ctx.reset_event_policy()
    finally:
        stop_logging.set()
        thread.join(timeout=2.0)

    assert not thread.is_alive()
    assert errors == []

    for event in sink.events:
        assert event.event_name == "event.x"
        assert event.payload == {"x": 1}


# ---------- M: public API / exports ----------


def test_m01_module_exports_public_names() -> None:
    module = sys.modules[LogContext.__module__]

    assert set(module.__all__) == {
        "LogVerbosityLevel",
        "LogErrorHandlingPolicy",
        "LogContext",
    }
