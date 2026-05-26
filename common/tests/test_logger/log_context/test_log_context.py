# tests/test_logger/log_context/test_log_context.py

from __future__ import annotations

import threading
import time
from typing import Any, cast
from collections.abc import Mapping

import pytest

# noinspection PyProtectedMember
from mvx.common.logger.log_context.log_context import (
    ERR_LOGGED_FLAG,
    LogContext,
    LogErrorHandlingPolicy,
)
from mvx.common.logger.errors import LogContextResetError, LogContextUnableToLog

from mvx.common.logger.models import (
    LogEvent,
    LogEventMeta,
    LogEventPolicyProto,
    LogLevel,
    LogPayloadProcessorProto,
    LogSinkProto,
)


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
        self.checked_events: list[LogEventMeta] = []

    def is_event_enabled(self, event: LogEventMeta) -> bool:
        self.checked_events.append(event)
        return self.enabled


class RecordingPayloadProcessor:
    def __init__(self) -> None:
        self.normalize_payload_calls: list[tuple[Mapping[str, Any], bool]] = []
        self.normalize_value_calls: list[tuple[Any, bool]] = []

    def normalize_payload(
        self,
        payload: Mapping[str, Any],
        *,
        unbounded: bool = False,
    ) -> dict[str, Any]:
        self.normalize_payload_calls.append((payload, unbounded))
        return {"normalized_payload": True}

    def normalize_value_for_log(
        self,
        value: Any,
        *,
        unbounded: bool = False,
    ) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None:
        self.normalize_value_calls.append((value, unbounded))
        return {"normalized_value": True}

    def get_plain_verbosity_level(self) -> str | None:
        _ = self
        return "NORMAL"


class ExplodingPayloadProvider:
    def to_log_payload(self) -> dict[str, Any]:
        raise AssertionError("payload must not be normalized")


def make_sink() -> RecordingLogSink:
    return RecordingLogSink()


def make_root_context(
    *,
    namespace: str | None = "test.ns",
    log_sink: RecordingLogSink | None = None,
    event_policy: RecordingEventPolicy | None = None,
    payload_processor: LogPayloadProcessorProto | None = None,
    log_error_handling_policy: LogErrorHandlingPolicy | None = None,
) -> LogContext:
    sink = log_sink if log_sink is not None else make_sink()

    if payload_processor is None:
        payload_processor = RecordingPayloadProcessor()

    return LogContext(
        namespace=namespace,
        log_sink=cast(LogSinkProto, sink),
        event_policy=cast(LogEventPolicyProto | None, event_policy),
        payload_processor=payload_processor,
        log_error_handling_policy=log_error_handling_policy,
    )


def make_child_context(
    parent: LogContext,
    *,
    namespace: str | None = "child.ns",
    log_sink: RecordingLogSink | None = None,
    event_policy: RecordingEventPolicy | None = None,
    payload_processor: LogPayloadProcessorProto | None = None,
    log_error_handling_policy: LogErrorHandlingPolicy | None = None,
) -> LogContext:
    return LogContext(
        namespace=namespace,
        parent=parent,
        log_sink=cast(LogSinkProto | None, log_sink),
        event_policy=cast(LogEventPolicyProto | None, event_policy),
        payload_processor=payload_processor,
        log_error_handling_policy=log_error_handling_policy,
    )


def make_log_event_meta(
    *,
    event_name: str = "event.x",
    event_namespace: str = "test.ns",
    entity_id: str | None = None,
    source_path: str | None = None,
    source_line: int | None = None,
    source_func: str | None = None,
) -> LogEventMeta:
    return LogEventMeta(
        event_namespace=event_namespace,
        event_name=event_name,
        entity_id=entity_id,
        source_path=source_path,
        source_line=source_line,
        source_func=source_func,
    )


def make_log_context_untyped(**kwargs: Any) -> LogContext:
    return cast(Any, LogContext)(**kwargs)


# ---------- A: constructor / root context validation ----------


def test_a01_root_context_requires_log_sink() -> None:
    with pytest.raises(ValueError):
        make_log_context_untyped(
            payload_processor=RecordingPayloadProcessor(),
        )


def test_a02_root_context_requires_payload_processor() -> None:
    with pytest.raises(ValueError):
        make_log_context_untyped(
            log_sink=cast(LogSinkProto, make_sink()),
        )


def test_a03_root_context_uses_default_error_policy() -> None:
    ctx = make_root_context(
        log_error_handling_policy=None,
    )

    assert ctx.log_error_handling_policy is LogErrorHandlingPolicy.PRINT_STDERR


def test_a04_root_context_accepts_custom_payload_processor_and_error_policy() -> None:
    payload_processor = RecordingPayloadProcessor()

    ctx = make_root_context(
        payload_processor=payload_processor,
        log_error_handling_policy=LogErrorHandlingPolicy.IGNORE,
    )

    assert ctx.payload_processor is payload_processor
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
            payload_processor=RecordingPayloadProcessor(),
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
        make_log_context_untyped(
            log_sink=object(),
            payload_processor=RecordingPayloadProcessor(),
        )


def test_b02_invalid_event_policy_type_fails() -> None:
    with pytest.raises(TypeError):
        LogContext(
            log_sink=cast(LogSinkProto, make_sink()),
            event_policy=cast(Any, object()),
            payload_processor=RecordingPayloadProcessor(),
        )


def test_b03_invalid_payload_processor_type_fails() -> None:
    with pytest.raises(TypeError):
        LogContext(
            log_sink=cast(LogSinkProto, make_sink()),
            payload_processor=cast(Any, object()),
        )


def test_b04_invalid_log_error_handling_policy_type_fails() -> None:
    with pytest.raises(TypeError):
        LogContext(
            log_sink=cast(LogSinkProto, make_sink()),
            payload_processor=RecordingPayloadProcessor(),
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
    root = make_root_context()

    with pytest.raises(LogContextResetError):
        root.reset_log_sink()


def test_c05_child_inherits_payload_processor_from_parent() -> None:
    payload_processor = RecordingPayloadProcessor()
    parent = make_root_context(payload_processor=payload_processor)
    child = make_child_context(parent)

    assert child.payload_processor is payload_processor


def test_c06_child_can_override_payload_processor() -> None:
    parent_processor = RecordingPayloadProcessor()
    child_processor = RecordingPayloadProcessor()

    parent = make_root_context(payload_processor=parent_processor)
    child = make_child_context(parent, payload_processor=child_processor)

    assert child.payload_processor is child_processor


def test_c07_child_reset_payload_processor_restores_parent_processor() -> None:
    parent_processor = RecordingPayloadProcessor()
    child_processor = RecordingPayloadProcessor()

    parent = make_root_context(payload_processor=parent_processor)
    child = make_child_context(parent, payload_processor=child_processor)

    child.reset_payload_processor()

    assert child.payload_processor is parent.payload_processor


def test_c08_root_reset_payload_processor_fails() -> None:
    root = make_root_context()

    with pytest.raises(LogContextResetError):
        root.reset_payload_processor()


def test_c09_grandchild_inherits_nearest_payload_processor_override() -> None:
    root_processor = RecordingPayloadProcessor()
    branch_processor = RecordingPayloadProcessor()

    root = make_root_context(payload_processor=root_processor)
    branch = make_child_context(root, payload_processor=branch_processor)
    leaf = make_child_context(branch)

    assert leaf.payload_processor is branch_processor


def test_c10_grandchild_falls_back_to_root_payload_processor_when_no_override_exists() -> None:
    root_processor = RecordingPayloadProcessor()

    root = make_root_context(payload_processor=root_processor)
    branch = make_child_context(root)
    leaf = make_child_context(branch)

    assert leaf.payload_processor is root_processor


def test_c11_child_inherits_log_error_handling_policy() -> None:
    parent = make_root_context(log_error_handling_policy=LogErrorHandlingPolicy.IGNORE)
    child = make_child_context(parent)

    assert child.log_error_handling_policy is LogErrorHandlingPolicy.IGNORE


def test_c12_child_can_override_log_error_handling_policy() -> None:
    parent = make_root_context(log_error_handling_policy=LogErrorHandlingPolicy.RAISE)
    child = make_child_context(
        parent,
        log_error_handling_policy=LogErrorHandlingPolicy.PRINT_STDERR,
    )

    assert child.log_error_handling_policy is LogErrorHandlingPolicy.PRINT_STDERR


def test_c13_child_reset_log_error_handling_policy_restores_parent_value() -> None:
    parent = make_root_context(log_error_handling_policy=LogErrorHandlingPolicy.IGNORE)
    child = make_child_context(
        parent,
        log_error_handling_policy=LogErrorHandlingPolicy.PRINT_STDERR,
    )

    child.reset_log_error_handling_policy()

    assert child.log_error_handling_policy is LogErrorHandlingPolicy.IGNORE


def test_c14_root_reset_log_error_handling_policy_fails() -> None:
    root = make_root_context()

    with pytest.raises(LogContextResetError):
        root.reset_log_error_handling_policy()


# ---------- D: event policy semantics ----------


def test_d01_event_policy_none_enables_all_events() -> None:
    ctx = make_root_context()
    event = make_log_event_meta()

    assert ctx.event_policy is None
    assert ctx.is_event_enabled(event) is True


def test_d02_event_policy_is_used_when_present() -> None:
    policy = RecordingEventPolicy(enabled=False)
    ctx = make_root_context(event_policy=policy)
    event = make_log_event_meta(event_name="event.x")

    assert ctx.is_event_enabled(event) is False
    assert policy.checked_events == [event]


def test_d03_set_event_policy_is_used() -> None:
    policy = RecordingEventPolicy(enabled=False)
    ctx = make_root_context()
    event = make_log_event_meta(event_name="event.x")

    ctx.set_event_policy(cast(LogEventPolicyProto, policy))

    assert ctx.event_policy is policy
    assert ctx.is_event_enabled(event) is False
    assert policy.checked_events == [event]


def test_d04_reset_event_policy_enables_all_events() -> None:
    ctx = make_root_context(event_policy=RecordingEventPolicy(enabled=False))
    event = make_log_event_meta(event_name="event.x")

    ctx.reset_event_policy()

    assert ctx.event_policy is None
    assert ctx.is_event_enabled(event) is True


def test_d05_child_does_not_inherit_parent_event_policy() -> None:
    parent = make_root_context(event_policy=RecordingEventPolicy(enabled=False))
    child = make_child_context(parent)
    event = make_log_event_meta(event_name="event.x")

    assert child.event_policy is None
    assert child.is_event_enabled(event) is True


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

    assert len(policy.checked_events) == 1
    assert policy.checked_events[0].event_name == "event.x"

    assert len(sink.events) == 1
    assert sink.events[0].meta.event_name == "event.x"


def test_d08_event_policy_receives_log_event_meta_before_payload_normalization() -> None:
    sink = make_sink()
    policy = RecordingEventPolicy(enabled=True)
    processor = RecordingPayloadProcessor()
    ctx = make_root_context(
        log_sink=sink,
        event_policy=policy,
        payload_processor=cast(LogPayloadProcessorProto, processor),
    )
    payload = {"x": object()}

    ctx.log_event(
        event="event.x",
        level=LogLevel.WARNING,
        payload=payload,
        event_namespace="custom.ns",
        event_type="operation",
        entity_id="entity-1",
        source_path="/tmp/a.py",
        source_line=10,
        source_func="func",
    )

    assert len(policy.checked_events) == 1

    checked = policy.checked_events[0]

    assert checked.event_namespace == "custom.ns"
    assert checked.event_name == "event.x"
    assert checked.entity_id == "entity-1"
    assert checked.source_path == "/tmp/a.py"
    assert checked.source_line == 10
    assert checked.source_func == "func"

    assert len(sink.events) == 1

    logged = sink.events[0]

    assert logged.level is LogLevel.WARNING
    assert logged.meta is checked
    assert logged.event_type == "operation"
    assert logged.payload == {"normalized_payload": True}
    assert processor.normalize_payload_calls == [(payload, False)]


def test_d09_disabled_event_does_not_normalize_payload() -> None:
    sink = make_sink()
    ctx = make_root_context(
        log_sink=sink,
        event_policy=RecordingEventPolicy(enabled=False),
    )

    ctx.log_event(
        event="event.x",
        level=LogLevel.INFO,
        payload={"provider": ExplodingPayloadProvider()},
    )

    assert sink.events == []


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
    assert logged.meta.event_namespace == "test.ns"
    assert logged.meta.event_name == "event.x"
    assert logged.event_type is None
    assert before <= logged.timestamp <= after
    assert logged.meta.entity_id is None
    assert logged.payload == {"normalized_payload": True}
    assert logged.meta.source_path is None
    assert logged.meta.source_line is None
    assert logged.meta.source_func is None


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
    assert logged.meta.event_namespace == "custom.ns"
    assert logged.meta.event_name == "event.x"
    assert logged.event_type == "operation"
    assert logged.meta.entity_id == "entity-1"
    assert logged.payload == {"normalized_payload": True}
    assert logged.meta.source_path == "/tmp/a.py"
    assert logged.meta.source_line == 10
    assert logged.meta.source_func == "func"


def test_e03_log_event_uses_not_defined_namespace_when_context_namespace_missing() -> None:
    sink = make_sink()
    ctx = make_root_context(log_sink=sink, namespace=None)

    ctx.log_event(
        event="event.x",
        level=LogLevel.INFO,
        payload={},
    )

    logged = sink.events[0]

    assert logged.meta.event_namespace == "<not defined>"


def test_e04_log_event_normalizes_payload_by_default() -> None:
    sink = make_sink()
    processor = RecordingPayloadProcessor()
    ctx = make_root_context(
        log_sink=sink,
        payload_processor=cast(LogPayloadProcessorProto, processor),
    )
    payload = {"x": object()}

    ctx.log_event(
        event="event.x",
        level=LogLevel.INFO,
        payload=payload,
    )

    assert sink.events[0].payload == {"normalized_payload": True}
    assert processor.normalize_payload_calls == [(payload, False)]


def test_e05_log_event_can_skip_payload_normalization() -> None:
    sink = make_sink()
    ctx = make_root_context(log_sink=sink)
    payload = {"x": object()}

    ctx.log_event(
        event="event.x",
        level=LogLevel.INFO,
        payload=payload,
        skip_payload_normalization=True,
    )

    assert sink.events[0].payload is payload


def test_e06_policy_meta_and_logged_event_meta_are_same_object() -> None:
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

    assert len(policy.checked_events) == 1
    assert len(sink.events) == 1
    assert sink.events[0].meta is policy.checked_events[0]


def test_e07_log_event_payload_normalization_uses_effective_payload_processor() -> None:
    sink = make_sink()
    processor = RecordingPayloadProcessor()
    ctx = make_root_context(
        log_sink=sink,
        payload_processor=cast(LogPayloadProcessorProto, processor),
    )
    payload = {"value": object()}

    ctx.log_event(
        event="event.x",
        level=LogLevel.INFO,
        payload=payload,
    )

    assert sink.events[0].payload == {"normalized_payload": True}
    assert processor.normalize_payload_calls == [(payload, False)]


def test_e08_emit_log_event_bypasses_event_policy() -> None:
    sink = make_sink()
    policy = RecordingEventPolicy(enabled=False)
    ctx = make_root_context(
        log_sink=sink,
        event_policy=policy,
    )

    meta = make_log_event_meta(event_name="event.x")
    event = LogEvent(
        level=LogLevel.INFO,
        meta=meta,
        event_type="manual",
        timestamp=time.time(),
        payload={"x": 1},
    )

    ctx.emit_log_event(event)

    assert policy.checked_events == []
    assert sink.events == [event]


def test_e09_emit_log_event_does_not_normalize_payload() -> None:
    sink = make_sink()
    ctx = make_root_context(log_sink=sink)
    payload = {"x": object()}

    event = LogEvent(
        level=LogLevel.INFO,
        meta=make_log_event_meta(event_name="event.x"),
        event_type="manual",
        timestamp=time.time(),
        payload=payload,
    )

    ctx.emit_log_event(event)

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
    assert logged.meta.event_namespace == "ns"
    assert logged.meta.event_name == "event.x"
    assert logged.event_type == "type"
    assert logged.meta.entity_id == "id"
    assert logged.meta.source_path == "path"
    assert logged.meta.source_line == 123
    assert logged.meta.source_func == "func"
    assert logged.payload == {"normalized_payload": True}


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

    assert captured.err.count("LogContext log event failed") == 1


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

    assert captured.err.count("LogContext log event failed") == 2


# ---------- H: error payload building ----------


def test_h01_build_error_payload_uses_to_log_payload_dict() -> None:
    ctx = make_root_context()

    class CustomError(Exception):
        def to_log_payload(self) -> dict[str, Any]:
            _ = self
            return {"kind": "custom", "x": 1}

    assert ctx.build_error_payload(CustomError()) == {"kind": "custom", "x": 1}


def test_h02_build_error_payload_copies_to_log_payload_result() -> None:
    ctx = make_root_context()
    provided = {"kind": "custom"}

    class CustomError(Exception):
        def to_log_payload(self) -> dict[str, Any]:
            _ = self
            return provided

    result = ctx.build_error_payload(CustomError())

    assert result == provided
    assert result is not provided


def test_h03_build_error_payload_ignores_non_dict_to_log_payload() -> None:
    ctx = make_root_context()

    class CustomError(Exception):
        def to_log_payload(self) -> list[str]:
            _ = self
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


# ---------- J: payload processor delegation wrappers ----------


def test_j01_normalize_payload_delegates_to_root_payload_processor() -> None:
    processor = RecordingPayloadProcessor()
    ctx = make_root_context(
        payload_processor=cast(LogPayloadProcessorProto, processor),
    )
    payload = {"x": object()}

    result = ctx.normalize_payload(payload)

    assert result == {"normalized_payload": True}
    assert processor.normalize_payload_calls == [(payload, False)]


def test_j02_normalize_payload_forwards_unbounded_to_processor() -> None:
    processor = RecordingPayloadProcessor()
    ctx = make_root_context(
        payload_processor=cast(LogPayloadProcessorProto, processor),
    )
    payload = {"x": object()}

    result = ctx.normalize_payload(payload, unbounded=True)

    assert result == {"normalized_payload": True}
    assert processor.normalize_payload_calls == [(payload, True)]


def test_j03_normalize_value_for_log_delegates_to_root_payload_processor() -> None:
    processor = RecordingPayloadProcessor()
    ctx = make_root_context(
        payload_processor=cast(LogPayloadProcessorProto, processor),
    )
    value = object()

    result = ctx.normalize_value_for_log(value)

    assert result == {"normalized_value": True}
    assert processor.normalize_value_calls == [(value, False)]


def test_j04_normalize_value_for_log_forwards_unbounded_to_processor() -> None:
    processor = RecordingPayloadProcessor()
    ctx = make_root_context(
        payload_processor=cast(LogPayloadProcessorProto, processor),
    )
    value = object()

    result = ctx.normalize_value_for_log(value, unbounded=True)

    assert result == {"normalized_value": True}
    assert processor.normalize_value_calls == [(value, True)]


def test_j05_normalization_uses_inherited_parent_payload_processor() -> None:
    processor = RecordingPayloadProcessor()
    parent = make_root_context(
        payload_processor=cast(LogPayloadProcessorProto, processor),
    )
    child = make_child_context(parent)
    value = object()

    result = child.normalize_value_for_log(value)

    assert child.payload_processor is processor
    assert result == {"normalized_value": True}
    assert processor.normalize_value_calls == [(value, False)]


def test_j06_normalization_uses_child_payload_processor_override() -> None:
    parent_processor = RecordingPayloadProcessor()
    child_processor = RecordingPayloadProcessor()

    parent = make_root_context(
        payload_processor=cast(LogPayloadProcessorProto, parent_processor),
    )
    child = make_child_context(
        parent,
        payload_processor=cast(LogPayloadProcessorProto, child_processor),
    )
    value = object()

    result = child.normalize_value_for_log(value)

    assert child.payload_processor is child_processor
    assert result == {"normalized_value": True}
    assert parent_processor.normalize_value_calls == []
    assert child_processor.normalize_value_calls == [(value, False)]


def test_j07_normalization_uses_restored_parent_payload_processor_after_reset() -> None:
    parent_processor = RecordingPayloadProcessor()
    child_processor = RecordingPayloadProcessor()

    parent = make_root_context(
        payload_processor=cast(LogPayloadProcessorProto, parent_processor),
    )
    child = make_child_context(
        parent,
        payload_processor=cast(LogPayloadProcessorProto, child_processor),
    )
    value = object()

    child.reset_payload_processor()

    result = child.normalize_value_for_log(value)

    assert child.payload_processor is parent_processor
    assert result == {"normalized_value": True}
    assert parent_processor.normalize_value_calls == [(value, False)]
    assert child_processor.normalize_value_calls == []


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


def test_k05_set_payload_processor_none_fails() -> None:
    ctx = make_root_context()

    with pytest.raises(ValueError):
        ctx.set_payload_processor(cast(Any, None))


def test_k06_set_payload_processor_invalid_type_fails() -> None:
    ctx = make_root_context()

    with pytest.raises(TypeError):
        ctx.set_payload_processor(cast(Any, object()))


def test_k07_set_payload_processor_accepts_valid_processor() -> None:
    ctx = make_root_context()
    processor = RecordingPayloadProcessor()
    value = object()

    ctx.set_payload_processor(cast(LogPayloadProcessorProto, processor))

    result = ctx.normalize_value_for_log(value)

    assert ctx.payload_processor is processor
    assert result == {"normalized_value": True}
    assert processor.normalize_value_calls == [(value, False)]


def test_k08_set_log_error_handling_policy_none_fails() -> None:
    ctx = make_root_context()

    with pytest.raises(ValueError):
        ctx.set_log_error_handling_policy(cast(Any, None))


def test_k09_set_log_error_handling_policy_invalid_type_fails() -> None:
    ctx = make_root_context()

    with pytest.raises(TypeError):
        ctx.set_log_error_handling_policy(cast(Any, "RAISE"))


def test_k10_set_log_error_handling_policy_accepts_valid_policy() -> None:
    ctx = make_root_context()

    ctx.set_log_error_handling_policy(LogErrorHandlingPolicy.IGNORE)

    assert ctx.log_error_handling_policy is LogErrorHandlingPolicy.IGNORE


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
                _ = ctx.payload_processor
                _ = ctx.log_error_handling_policy
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=reader)
    thread.start()

    try:
        for index in range(100):
            ctx.set_log_sink(cast(LogSinkProto, make_sink()))
            ctx.set_event_policy(
                cast(LogEventPolicyProto, RecordingEventPolicy(enabled=index % 2 == 0))
            )
            ctx.reset_event_policy()
            ctx.set_payload_processor(cast(LogPayloadProcessorProto, RecordingPayloadProcessor()))
            ctx.set_log_error_handling_policy(LogErrorHandlingPolicy.RAISE)
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
            ctx.set_event_policy(
                cast(LogEventPolicyProto, RecordingEventPolicy(enabled=index % 2 == 0))
            )
            ctx.reset_event_policy()
    finally:
        stop_logging.set()
        thread.join(timeout=2.0)

    assert not thread.is_alive()
    assert errors == []

    for event in sink.events:
        assert event.meta.event_name == "event.x"
        assert event.payload == {"normalized_payload": True}
