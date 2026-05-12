# common/tests/test_logger/log_components/test_log_invocation.py
from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import Any, Awaitable, Mapping

import pytest

from mvx.common.logger.models import LogLevel

# noinspection PyProtectedMember
from mvx.common.logger.log_components.log_invocation import (
    PayloadFormatter,
    _InvocationEventType,
    _apply_verbosity_filter,
    _build_logged_kwargs,
    _build_logged_result,
    _extract_func_arguments,
    _inject_context_payload,
    _resolve_context,
    _resolve_entity_id,
    _resolve_fields,
    log_invocation,
)
from mvx.common.logger.log_components.protocols import LogContextProto


@dataclass(frozen=True)
class RecordedEvent:
    event: str
    level: LogLevel
    payload: Mapping[str, Any]
    event_type: str | None
    entity_id: str | None


class RecordingContext(LogContextProto):
    def __init__(
        self,
        *,
        enabled: bool = True,
        verbosity_level: str = "NORMAL",
        already_logged_errors: set[int] | None = None,
    ) -> None:
        self.enabled = enabled
        self._verbosity_level = verbosity_level
        self.events: list[RecordedEvent] = []
        self.normalize_calls: list[tuple[Any, bool]] = []
        self.error_payload_calls: list[BaseException] = []
        self.marked_errors: list[BaseException] = []
        self._logged_error_ids: set[int] = set(already_logged_errors or set())

    def is_event_enabled(self, event: str) -> bool:
        return self.enabled

    @property
    def verbosity_level(self) -> str:
        return self._verbosity_level

    def normalize_value_for_log(
        self,
        value: Any,
        *,
        unbounded: bool = False,
    ) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None:
        self.normalize_calls.append((value, unbounded))

        if isinstance(value, (str, int, float, bool, bytes)) or value is None:
            return value

        if isinstance(value, tuple):
            return [self.normalize_value_for_log(item) for item in value]

        if isinstance(value, list):
            if unbounded:
                return [self.normalize_value_for_log(item) for item in value]

            if len(value) > 2:
                return [
                    self.normalize_value_for_log(value[0]),
                    self.normalize_value_for_log(value[1]),
                    "...",
                ]

            return [self.normalize_value_for_log(item) for item in value]

        if isinstance(value, dict):
            return {str(k): self.normalize_value_for_log(v) for k, v in value.items()}

        return f"<{type(value).__name__}>"

    def build_error_payload(self, err: BaseException) -> Mapping[str, Any]:
        self.error_payload_calls.append(err)
        return {
            "kind": type(err).__name__,
            "message": str(err),
        }

    def is_error_logged(self, err: BaseException) -> bool:
        return id(err) in self._logged_error_ids

    def mark_error_logged(self, err: BaseException) -> None:
        self.marked_errors.append(err)
        self._logged_error_ids.add(id(err))

    def log_event(
        self,
        event: str,
        level: LogLevel,
        payload: Mapping[str, Any],
        *,
        event_namespace: str | None = None,
        event_type: str | None = None,
        entity_id: str | None = None,
        source_path: str | None = None,
        source_line: int | None = None,
        source_func: str | None = None,
    ) -> None:
        if not self.enabled:
            return

        self.events.append(
            RecordedEvent(
                event=event,
                level=level,
                payload=dict(payload),
                event_type=event_type,
                entity_id=entity_id,
            )
        )


class DisabledEventRecordingContext(RecordingContext):
    def is_event_enabled(self, event: str) -> bool:
        return False

    def log_event(
        self,
        event: str,
        level: LogLevel,
        payload: Mapping[str, Any],
        *,
        event_namespace: str | None = None,
        event_type: str | None = None,
        entity_id: str | None = None,
        source_path: str | None = None,
        source_line: int | None = None,
        source_func: str | None = None,
    ) -> None:
        self.events.append(
            RecordedEvent(
                event=event,
                level=level,
                payload=dict(payload),
                event_type=event_type,
                entity_id=entity_id,
            )
        )


class ContextProvider:
    def __init__(self, ctx: RecordingContext) -> None:
        self.ctx = ctx

    def get_log_context(self) -> LogContextProto:
        return self.ctx


class EntityProvider(ContextProvider):
    def __init__(self, ctx: RecordingContext, identity: str = "entity-1") -> None:
        super().__init__(ctx)
        self._identity = identity

    @property
    def identity(self) -> str:
        return self._identity


@dataclass
class Inner:
    value: str
    items: list[int]


@dataclass
class Outer:
    inner: Inner
    name: str


class BrokenAttribute:
    @property
    def broken(self) -> str:
        raise RuntimeError("broken attribute")


class CustomError(RuntimeError):
    pass


class SuppressedError(RuntimeError):
    pass


def make_asserting_formatter(
    *,
    expected_ctx: LogContextProto,
    expected_event_type: _InvocationEventType,
    expected_event: str,
    expected_fields: dict[str, Any],
    result: dict[str, Any],
) -> PayloadFormatter:
    def formatter(
        ctx_arg: LogContextProto,
        event_type: _InvocationEventType,
        event: str,
        fields: dict[str, Any],
    ) -> dict[str, Any]:
        assert ctx_arg is expected_ctx
        assert event_type == expected_event_type
        assert event == expected_event
        assert fields == expected_fields
        return result

    return formatter


def make_returning_formatter(result: dict[str, Any]) -> PayloadFormatter:
    def formatter(
        ctx_arg: LogContextProto,
        event_type: _InvocationEventType,
        event: str,
        fields: dict[str, Any],
    ) -> dict[str, Any]:
        _ = ctx_arg
        _ = event_type
        _ = event
        _ = fields

        return result

    return formatter


def make_raising_formatter() -> PayloadFormatter:
    def formatter(
        ctx_arg: LogContextProto,
        event_type: _InvocationEventType,
        event: str,
        fields: dict[str, Any],
    ) -> dict[str, Any]:
        _ = ctx_arg
        _ = event_type
        _ = event
        _ = fields
        raise RuntimeError("formatter failed")

    return formatter


# A. Context and entity resolution


def test_a01_resolve_context_from_first_argument_provider() -> None:
    ctx = RecordingContext()
    provider = ContextProvider(ctx)

    assert _resolve_context((provider,)) is ctx


def test_a02_resolve_context_raises_when_first_argument_has_no_context() -> None:
    with pytest.raises(RuntimeError, match="no logger context found"):
        _resolve_context((object(),))


def test_a03_resolve_context_raises_when_no_arguments() -> None:
    with pytest.raises(RuntimeError, match="no logger context found"):
        _resolve_context(())


def test_a04_resolve_entity_id_from_first_argument_provider() -> None:
    ctx = RecordingContext()
    provider = EntityProvider(ctx, identity="abc-123")

    assert _resolve_entity_id((provider,)) == "abc-123"


def test_a05_resolve_entity_id_returns_none_without_provider() -> None:
    assert _resolve_entity_id((object(),)) is None
    assert _resolve_entity_id(()) is None


# B. Function argument extraction


def test_b01_extract_func_arguments_binds_args_kwargs_and_defaults() -> None:
    def target(self: Any, a: int, b: str = "default", *, c: bool = True) -> None:
        _ = self
        _ = a
        _ = b
        _ = c
        pass

    signature = inspect.signature(target)

    result = _extract_func_arguments(
        signature,
        args=(object(), 10),
        kwargs={"c": False},
    )

    assert result["a"] == 10
    assert result["b"] == "default"
    assert result["c"] is False
    assert "self" in result


def test_b02_extract_func_arguments_falls_back_to_kwargs_on_bind_error() -> None:
    def target(a: int) -> None:
        _ = a
        pass

    signature = inspect.signature(target)

    result = _extract_func_arguments(
        signature,
        args=(1, 2),
        kwargs={"x": "fallback"},
    )

    assert result == {"x": "fallback"}


# C. Verbosity filter


@pytest.mark.parametrize(
    ("raw_spec", "verbosity", "expected"),
    [
        ("name", "NORMAL", "name"),
        (" NORMAL:name ", "NORMAL", "name"),
        ("MINIMAL,NORMAL:name", "NORMAL", "name"),
        ("MINIMAL,NORMAL:name", "MAXIMUM", None),
        (":name", "NORMAL", "name"),
        ("NORMAL:", "NORMAL", None),
        ("   ", "NORMAL", None),
        ("NORMAL, : name", "NORMAL", "name"),
    ],
)
def test_c01_apply_verbosity_filter(
    raw_spec: str,
    verbosity: str,
    expected: str | None,
) -> None:
    assert _apply_verbosity_filter(raw_spec, verbosity) == expected


# D. Field resolution


def test_d01_resolve_fields_extracts_plain_argument() -> None:
    result = _resolve_fields(
        ("name",),
        {"name": "alpha"},
        verbosity_level="NORMAL",
    )

    assert len(result) == 1
    assert result[0].alias == "name"
    assert result[0].value == "alpha"
    assert result[0].unbounded_items is False


def test_d02_resolve_fields_extracts_nested_attribute() -> None:
    obj = Outer(inner=Inner(value="v1", items=[1, 2, 3]), name="outer")

    result = _resolve_fields(
        ("inner.value",),
        {"inner": obj.inner},
        verbosity_level="NORMAL",
    )

    assert len(result) == 1
    assert result[0].alias == "value"
    assert result[0].value == "v1"


def test_d03_resolve_fields_extracts_len_pseudo_attribute() -> None:
    items = [1, 2, 3]

    result = _resolve_fields(
        ("items.len()",),
        {"items": items},
        verbosity_level="NORMAL",
    )

    assert len(result) == 1
    assert result[0].alias == "len()"
    assert result[0].value == 3


def test_d04_resolve_fields_supports_alias() -> None:
    items = [1, 2, 3]

    result = _resolve_fields(
        ("item_count=items.len()",),
        {"items": items},
        verbosity_level="NORMAL",
    )

    assert len(result) == 1
    assert result[0].alias == "item_count"
    assert result[0].value == 3


def test_d05_resolve_fields_supports_unbounded_suffix() -> None:
    result = _resolve_fields(
        ("payload=data!",),
        {"data": [1, 2, 3]},
        verbosity_level="NORMAL",
    )

    assert len(result) == 1
    assert result[0].alias == "payload"
    assert result[0].value == [1, 2, 3]
    assert result[0].unbounded_items is True


def test_d06_resolve_fields_applies_verbosity_filter() -> None:
    result = _resolve_fields(
        (
            "MINIMAL:name",
            "NORMAL:age",
            "MAXIMUM:title",
        ),
        {
            "name": "alpha",
            "age": 10,
            "title": "captain",
        },
        verbosity_level="NORMAL",
    )

    assert [(item.alias, item.value) for item in result] == [("age", 10)]


def test_d07_resolve_fields_skips_empty_invalid_missing_and_failed_specs() -> None:
    result = _resolve_fields(
        (
            "",
            "   ",
            "missing",
            "= name",
            "alias=",
            "data.unknown",
            "broken.broken",
            "items.len()!",
        ),
        {
            "name": "alpha",
            "data": object(),
            "broken": BrokenAttribute(),
            "items": [1, 2, 3],
        },
        verbosity_level="NORMAL",
    )

    assert len(result) == 2
    assert result[0].alias == "name"
    assert result[0].value == "alpha"
    assert result[1].alias == "len()"
    assert result[1].value == 3
    assert result[1].unbounded_items is True


def test_d08_resolve_fields_skips_len_failure() -> None:
    result = _resolve_fields(
        ("value.len()",),
        {"value": object()},
        verbosity_level="NORMAL",
    )

    assert result == []


# E. Context payload injection


def test_e01_inject_context_payload_uses_normalized_resolved_fields() -> None:
    ctx = RecordingContext()
    target: dict[str, Any] = {}

    result = _inject_context_payload(
        ctx=ctx,
        event="op.test",
        event_type=_InvocationEventType.INVOKE,
        field_specs=("name", "items!"),
        source_kwargs={"name": "alpha", "items": [1, 2, 3]},
        payload_formatter=None,
        target_payload=target,
    )

    assert result is target
    assert target == {
        "name": "alpha",
        "items": [1, 2, 3],
    }
    assert ([1, 2, 3], True) in ctx.normalize_calls


def test_e02_inject_context_payload_uses_formatter_output() -> None:
    ctx = RecordingContext()
    target: dict[str, Any] = {}

    formatter = make_asserting_formatter(
        expected_ctx=ctx,
        expected_event_type=_InvocationEventType.SUCCESS,
        expected_event="op.test",
        expected_fields={"name": "alpha"},
        result={"formatted": "yes"},
    )

    result = _inject_context_payload(
        ctx=ctx,
        event="op.test",
        event_type=_InvocationEventType.SUCCESS,
        field_specs=("name",),
        source_kwargs={"name": "alpha"},
        payload_formatter=formatter,
        target_payload=target,
    )

    assert result is target
    assert target == {"formatted": "yes"}


def test_e03_inject_context_payload_calls_formatter_even_without_fields() -> None:
    ctx = RecordingContext()
    target: dict[str, Any] = {}

    formatter = make_asserting_formatter(
        expected_ctx=ctx,
        expected_event_type=_InvocationEventType.INVOKE,
        expected_event="op.test",
        expected_fields={},
        result={"formatted": True},
    )

    _inject_context_payload(
        ctx=ctx,
        event="op.test",
        event_type=_InvocationEventType.INVOKE,
        field_specs=(),
        source_kwargs={},
        payload_formatter=formatter,
        target_payload=target,
    )

    assert target == {"formatted": True}


def test_e04_inject_context_payload_falls_back_when_formatter_raises() -> None:
    ctx = RecordingContext()
    target: dict[str, Any] = {}

    _inject_context_payload(
        ctx=ctx,
        event="op.test",
        event_type=_InvocationEventType.INVOKE,
        field_specs=("name",),
        source_kwargs={"name": "alpha"},
        payload_formatter=make_raising_formatter(),
        target_payload=target,
    )

    assert target == {"name": "alpha"}


def test_e05_inject_context_payload_places_system_keys_under_context() -> None:
    ctx = RecordingContext()
    target: dict[str, Any] = {
        "error": {"kind": "ExistingError"},
    }

    _inject_context_payload(
        ctx=ctx,
        event="op.test",
        event_type=_InvocationEventType.FAILED,
        field_specs=(),
        source_kwargs={},
        payload_formatter=make_returning_formatter(
            {
                "error": {"kind": "ContextError"},
                "custom": "value",
            }
        ),
        target_payload=target,
    )

    assert target == {
        "error": {"kind": "ExistingError"},
        "context": {
            "error": {"kind": "ContextError"},
            "custom": "value",
        },
    }


def test_e06_inject_context_payload_merges_system_keys_into_existing_context() -> None:
    ctx = RecordingContext()
    target: dict[str, Any] = {
        "context": {"before": 1},
    }

    _inject_context_payload(
        ctx=ctx,
        event="op.test",
        event_type=_InvocationEventType.INVOKE,
        field_specs=(),
        source_kwargs={},
        payload_formatter=make_returning_formatter({"kwargs": {"x": 1}}),
        target_payload=target,
    )

    assert target == {
        "context": {
            "before": 1,
            "kwargs": {"x": 1},
        }
    }


def test_e07_inject_context_payload_does_nothing_without_fields_or_formatter() -> None:
    ctx = RecordingContext()
    target: dict[str, Any] = {"existing": True}

    result = _inject_context_payload(
        ctx=ctx,
        event="op.test",
        event_type=_InvocationEventType.INVOKE,
        field_specs=(),
        source_kwargs={},
        payload_formatter=None,
        target_payload=target,
    )

    assert result is target
    assert target == {"existing": True}


def test_e08_inject_context_payload_falls_back_when_formatter_returns_non_dict() -> None:
    ctx = RecordingContext()
    target: dict[str, Any] = {}

    def formatter(
        ctx_arg: LogContextProto,
        event_type: _InvocationEventType,
        event: str,
        fields: dict[str, Any],
    ) -> Any:
        _ = ctx_arg
        _ = event_type
        _ = event
        _ = fields
        return "not-a-dict"

    _inject_context_payload(
        ctx=ctx,
        event="op.test",
        event_type=_InvocationEventType.INVOKE,
        field_specs=("name",),
        source_kwargs={"name": "alpha"},
        payload_formatter=formatter,
        target_payload=target,
    )

    assert target == {"name": "alpha"}


# F. Logged kwargs


def test_f01_build_logged_kwargs_normalizes_resolved_fields() -> None:
    ctx = RecordingContext()

    result = _build_logged_kwargs(
        ctx=ctx,
        field_specs=("name", "values!"),
        source_kwargs={
            "name": "alpha",
            "values": [1, 2, 3],
        },
    )

    assert result == {
        "name": "alpha",
        "values": [1, 2, 3],
    }
    assert ([1, 2, 3], True) in ctx.normalize_calls


def test_f02_build_logged_kwargs_returns_empty_dict_when_nothing_resolved() -> None:
    ctx = RecordingContext()

    result = _build_logged_kwargs(
        ctx=ctx,
        field_specs=("missing",),
        source_kwargs={},
    )

    assert result == {}


# G. Logged result


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        10,
        1.5,
        "alpha",
    ],
)
def test_g01_build_logged_result_logs_primitive_as_whole_value(value: Any) -> None:
    ctx = RecordingContext()

    assert _build_logged_result(ctx=ctx, field_specs=("ignored",), result_obj=value) == value


def test_g02_build_logged_result_logs_list_as_whole_value_without_specs() -> None:
    ctx = RecordingContext()

    result = _build_logged_result(
        ctx=ctx,
        field_specs=(),
        result_obj=[1, 2, 3],
    )

    assert result == [1, 2, "..."]


def test_g03_build_logged_result_extracts_list_indexes() -> None:
    ctx = RecordingContext()
    item = Inner(value="v1", items=[1, 2, 3])

    result = _build_logged_result(
        ctx=ctx,
        field_specs=("0", "alias=1", "item_value=2.value", "count=2.items.len()"),
        result_obj=("zero", "one", item),
    )

    assert result == {
        "item0": "zero",
        "alias": "one",
        "item_value": "v1",
        "count": 3,
    }


def test_g04_build_logged_result_list_index_specs_support_unbounded_suffix() -> None:
    ctx = RecordingContext()

    result = _build_logged_result(
        ctx=ctx,
        field_specs=("payload=0!",),
        result_obj=([1, 2, 3],),
    )

    assert result == {"payload": [1, 2, 3]}
    assert ([1, 2, 3], True) in ctx.normalize_calls


def test_g05_build_logged_result_list_mode_falls_back_when_specs_do_not_resolve() -> None:
    ctx = RecordingContext()

    result = _build_logged_result(
        ctx=ctx,
        field_specs=("missing", "10", "0.unknown"),
        result_obj=[object()],
    )

    assert result == ["<object>"]


def test_g06_build_logged_result_logs_dict_as_whole_value_ignoring_specs() -> None:
    ctx = RecordingContext()

    result = _build_logged_result(
        ctx=ctx,
        field_specs=("ignored",),
        result_obj={"a": 1},
    )

    assert result == {"a": 1}


def test_g07_build_logged_result_logs_composite_as_type_name_without_specs() -> None:
    ctx = RecordingContext()

    result = _build_logged_result(
        ctx=ctx,
        field_specs=(),
        result_obj=Inner(value="v1", items=[]),
    )

    assert result == "<Inner>"


def test_g08_build_logged_result_extracts_composite_fields() -> None:
    ctx = RecordingContext()
    obj = Outer(inner=Inner(value="v1", items=[1, 2, 3]), name="outer")

    result = _build_logged_result(
        ctx=ctx,
        field_specs=("name", "inner_value=inner.value", "count=inner.items.len()"),
        result_obj=obj,
    )

    assert result == {
        "name": "outer",
        "inner_value": "v1",
        "count": 3,
    }


def test_g09_build_logged_result_composite_supports_unbounded_suffix() -> None:
    ctx = RecordingContext()
    obj = Inner(value="v1", items=[1, 2, 3])

    result = _build_logged_result(
        ctx=ctx,
        field_specs=("items!",),
        result_obj=obj,
    )

    assert result == {"items": [1, 2, 3]}
    assert ([1, 2, 3], True) in ctx.normalize_calls


def test_g10_build_logged_result_composite_falls_back_when_nothing_resolved() -> None:
    ctx = RecordingContext()

    result = _build_logged_result(
        ctx=ctx,
        field_specs=("missing", "broken"),
        result_obj=BrokenAttribute(),
    )

    assert result == "<BrokenAttribute>"


# H. Decorator validation


@pytest.mark.parametrize(
    "event",
    [
        "",
        "op-test",
        "op:test",
        "op test",
        "op1",
    ],
)
def test_h01_log_invocation_rejects_invalid_event_names(event: str) -> None:
    with pytest.raises(ValueError, match="Invalid event name"):

        @log_invocation(event)
        def target() -> None:
            pass


@pytest.mark.parametrize(
    "event",
    [
        "op",
        "op.test",
        "Op_Test.Name",
    ],
)
def test_h02_log_invocation_accepts_valid_event_names(event: str) -> None:
    @log_invocation(event, ctx=RecordingContext())
    def target() -> str:
        return "ok"

    assert target() == "ok"


# I. Sync decorator path


def test_i01_log_invocation_sync_success_emits_invoke_and_success() -> None:
    ctx = RecordingContext()

    @log_invocation(
        "op.sync",
        ctx=ctx,
        invoke_level=LogLevel.INFO,
        success_level=LogLevel.WARNING,
        context_fields=("name",),
        log_kwargs_on_invoke=("count",),
        log_result_on_success=("value",),
    )
    def target(name: str, count: int) -> Inner:
        _ = name
        _ = count
        return Inner(value="done", items=[1, 2])

    result = target("alpha", 3)

    assert result == Inner(value="done", items=[1, 2])
    assert len(ctx.events) == 2

    invoke = ctx.events[0]
    assert invoke.event == "op.sync"
    assert invoke.level == LogLevel.INFO
    assert invoke.event_type == _InvocationEventType.INVOKE.value
    assert invoke.payload == {
        "name": "alpha",
        "kwargs": {"count": 3},
    }

    success = ctx.events[1]
    assert success.level == LogLevel.WARNING
    assert success.event_type == _InvocationEventType.SUCCESS.value
    assert success.payload == {
        "name": "alpha",
        "result": {"value": "done"},
    }


def test_i02_log_invocation_sync_uses_context_provider_and_entity_provider() -> None:
    ctx = RecordingContext()
    owner = EntityProvider(ctx, identity="owner-1")

    @log_invocation("op.entity")
    def target(self: EntityProvider, value: int) -> int:
        _ = self
        return value + 1

    assert target(owner, 10) == 11

    assert len(ctx.events) == 2
    assert ctx.events[0].entity_id == "owner-1"
    assert ctx.events[1].entity_id == "owner-1"


def test_i03_log_invocation_sync_uses_explicit_entity_id_getter_over_provider() -> None:
    ctx = RecordingContext()
    owner = EntityProvider(ctx, identity="owner-1")

    @log_invocation(
        "op.entity",
        entity_id_getter=lambda: "explicit-id",
    )
    def target(self: EntityProvider) -> str:
        _ = self
        return "ok"

    assert target(owner) == "ok"

    assert ctx.events[0].entity_id == "explicit-id"
    assert ctx.events[1].entity_id == "explicit-id"


def test_i04_log_invocation_sync_invoke_logs_closures() -> None:
    ctx = RecordingContext()

    @log_invocation(
        "op.closures",
        ctx=ctx,
        log_closures_on_invoke={"token": "abc", "items": [1, 2, 3]},
    )
    def target() -> str:
        return "ok"

    assert target() == "ok"

    assert ctx.events[0].payload == {
        "closures": {
            "token": "abc",
            "items": [1, 2, "..."],
        }
    }


def test_i05_log_invocation_sync_closure_normalization_failure_becomes_unknown() -> None:
    class FailingNormalizeContext(RecordingContext):
        def normalize_value_for_log(
            self,
            value: Any,
            *,
            unbounded: bool = False,
        ) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None:
            raise RuntimeError("normalization failed")

    ctx = FailingNormalizeContext()

    @log_invocation(
        "op.closures",
        ctx=ctx,
        log_closures_on_invoke={"token": "abc"},
    )
    def target() -> str:
        return "ok"

    assert target() == "ok"

    assert ctx.events[0].payload == {
        "closures": {
            "token": "<unknown>",
        }
    }


def test_i06_log_invocation_sync_success_without_result_logging_has_empty_success_payload() -> None:
    ctx = RecordingContext()

    @log_invocation("op.sync", ctx=ctx)
    def target() -> str:
        return "ok"

    assert target() == "ok"

    assert ctx.events[1].event_type == _InvocationEventType.SUCCESS.value
    assert ctx.events[1].payload == {}


def test_i07_log_invocation_sync_event_disabled_emits_nothing_on_success() -> None:
    ctx = RecordingContext(enabled=False)

    @log_invocation("op.disabled", ctx=ctx)
    def target() -> str:
        return "ok"

    assert target() == "ok"
    assert ctx.events == []


def test_i08_log_invocation_sync_event_disabled_still_logs_failure() -> None:
    ctx = DisabledEventRecordingContext()

    @log_invocation("op.disabled.fail", ctx=ctx)
    def target() -> None:
        raise CustomError("boom")

    with pytest.raises(CustomError, match="boom"):
        target()

    assert len(ctx.events) == 1
    assert ctx.events[0].event == "op.disabled.fail"
    assert ctx.events[0].event_type == _InvocationEventType.FAILED.value
    assert ctx.events[0].level == LogLevel.ERROR
    assert ctx.events[0].payload == {
        "error": {
            "kind": "CustomError",
            "message": "boom",
        }
    }


def test_i09_log_invocation_sync_failure_logs_error_and_reraises() -> None:
    ctx = RecordingContext()
    err = CustomError("boom")

    @log_invocation("op.fail", ctx=ctx)
    def target() -> None:
        raise err

    with pytest.raises(CustomError, match="boom"):
        target()

    assert len(ctx.events) == 2
    assert ctx.events[0].event_type == _InvocationEventType.INVOKE.value
    assert ctx.events[1].event_type == _InvocationEventType.FAILED.value
    assert ctx.events[1].level == LogLevel.ERROR
    assert ctx.events[1].payload == {
        "error": {
            "kind": "CustomError",
            "message": "boom",
        }
    }
    assert ctx.marked_errors == [err]


def test_i10_log_invocation_sync_failure_suppresses_already_logged_error_payload() -> None:
    err = CustomError("boom")
    ctx = RecordingContext(already_logged_errors={id(err)})

    @log_invocation(
        "op.fail",
        ctx=ctx,
    )
    def target() -> None:
        raise err

    with pytest.raises(CustomError, match="boom"):
        target()

    assert ctx.events[1].event_type == _InvocationEventType.FAILED.value
    assert ctx.events[1].level == LogLevel.DEBUG
    assert ctx.events[1].payload == {}
    assert ctx.error_payload_calls == []


def test_i11_log_invocation_sync_failure_policy_force_log() -> None:
    ctx = RecordingContext()

    @log_invocation(
        "op.fail",
        ctx=ctx,
        log_error_policy=((CustomError, True),),
    )
    def target() -> None:
        raise CustomError("boom")

    with pytest.raises(CustomError):
        target()

    assert ctx.events[1].level == LogLevel.ERROR
    assert ctx.events[1].payload["error"] == {
        "kind": "CustomError",
        "message": "boom",
    }


def test_i12_log_invocation_sync_failure_policy_suppress_error_payload() -> None:
    ctx = RecordingContext()

    @log_invocation(
        "op.fail",
        ctx=ctx,
        log_error_policy=((SuppressedError, False),),
        error_level_suppressed=LogLevel.INFO,
    )
    def target() -> None:
        raise SuppressedError("quiet")

    with pytest.raises(SuppressedError):
        target()

    assert ctx.events[1].level == LogLevel.INFO
    assert ctx.events[1].payload == {}
    assert len(ctx.marked_errors) == 1


def test_i13_log_invocation_sync_failure_context_formatter_system_keys_go_under_context() -> None:
    ctx = RecordingContext()

    @log_invocation(
        "op.fail",
        ctx=ctx,
        context_formatter=make_returning_formatter({"error": {"kind": "context-error"}}),
    )
    def target() -> None:
        raise CustomError("boom")

    with pytest.raises(CustomError):
        target()

    assert ctx.events[1].payload == {
        "context": {
            "error": {
                "kind": "context-error",
            }
        },
        "error": {
            "kind": "CustomError",
            "message": "boom",
        },
    }


def test_i14_log_invocation_sync_cancelled_logs_cancelled_and_reraises() -> None:
    ctx = RecordingContext()
    err = asyncio.CancelledError("stop")

    @log_invocation("op.cancel", ctx=ctx)
    def target() -> None:
        raise err

    with pytest.raises(asyncio.CancelledError):
        target()

    assert len(ctx.events) == 2
    assert ctx.events[1].event_type == _InvocationEventType.CANCELLED.value
    assert ctx.events[1].level == LogLevel.INFO
    assert ctx.events[1].payload == {
        "cancelled": True,
        "error": {
            "kind": "CancelledError",
            "message": "stop",
        },
    }
    assert ctx.marked_errors == [err]


def test_i15_log_invocation_sync_cancelled_skips_when_error_already_logged() -> None:
    err = asyncio.CancelledError("stop")
    ctx = RecordingContext(already_logged_errors={id(err)})

    @log_invocation("op.cancel", ctx=ctx)
    def target() -> None:
        raise err

    with pytest.raises(asyncio.CancelledError):
        target()

    assert len(ctx.events) == 1
    assert ctx.events[0].event_type == _InvocationEventType.INVOKE.value


def test_i16_log_invocation_sync_event_disabled_still_logs_cancelled() -> None:
    ctx = DisabledEventRecordingContext()
    err = asyncio.CancelledError("stop")

    @log_invocation("op.disabled.cancel", ctx=ctx)
    def target() -> None:
        raise err

    with pytest.raises(asyncio.CancelledError):
        target()

    assert len(ctx.events) == 1
    assert ctx.events[0].event == "op.disabled.cancel"
    assert ctx.events[0].event_type == _InvocationEventType.CANCELLED.value
    assert ctx.events[0].level == LogLevel.INFO
    assert ctx.events[0].payload == {
        "cancelled": True,
        "error": {
            "kind": "CancelledError",
            "message": "stop",
        },
    }
    assert ctx.marked_errors == [err]


def test_i17_log_invocation_sync_failure_policy_uses_first_matching_rule() -> None:
    ctx = RecordingContext()

    @log_invocation(
        "op.policy",
        ctx=ctx,
        log_error_policy=((RuntimeError, False), (CustomError, True)),
        error_level_suppressed=LogLevel.INFO,
    )
    def target() -> None:
        raise CustomError("boom")

    with pytest.raises(CustomError, match="boom"):
        target()

    assert ctx.events[1].event_type == _InvocationEventType.FAILED.value
    assert ctx.events[1].level == LogLevel.INFO
    assert ctx.events[1].payload == {}
    assert len(ctx.marked_errors) == 1


def test_i18_log_invocation_sync_failure_policy_force_log_ignores_already_logged_marker() -> None:
    err = CustomError("boom")
    ctx = RecordingContext(already_logged_errors={id(err)})

    @log_invocation(
        "op.policy",
        ctx=ctx,
        log_error_policy=((CustomError, True),),
    )
    def target() -> None:
        raise err

    with pytest.raises(CustomError, match="boom"):
        target()

    assert ctx.events[1].event_type == _InvocationEventType.FAILED.value
    assert ctx.events[1].level == LogLevel.ERROR
    assert ctx.events[1].payload == {
        "error": {
            "kind": "CustomError",
            "message": "boom",
        }
    }
    assert ctx.error_payload_calls == [err]
    assert ctx.marked_errors == [err]


def test_i19_log_invocation_sync_entity_id_getter_error_propagates_before_logging() -> None:
    ctx = RecordingContext()

    def get_entity_id() -> str:
        raise RuntimeError("id failed")

    @log_invocation(
        "op.entity",
        ctx=ctx,
        entity_id_getter=get_entity_id,
    )
    def target() -> str:
        return "ok"

    with pytest.raises(RuntimeError, match="id failed"):
        target()

    assert ctx.events == []


# J. Sync function returning awaitable


@pytest.mark.asyncio
async def test_j01_log_invocation_sync_returning_awaitable_success_logs_after_await() -> None:
    ctx = RecordingContext()

    async def inner() -> str:
        return "done"

    @log_invocation(
        "op.awaitable",
        ctx=ctx,
        log_result_on_success=(),
    )
    def target() -> Awaitable[str]:
        return inner()

    result = target()

    assert len(ctx.events) == 1
    assert ctx.events[0].event_type == _InvocationEventType.INVOKE.value

    awaited = await result

    assert awaited == "done"
    assert len(ctx.events) == 2
    assert ctx.events[1].event_type == _InvocationEventType.SUCCESS.value
    assert ctx.events[1].payload == {"result": "done"}


@pytest.mark.asyncio
async def test_j02_log_invocation_sync_returning_awaitable_failure_logs_after_await() -> None:
    ctx = RecordingContext()

    async def inner() -> str:
        raise CustomError("boom")

    @log_invocation("op.awaitable", ctx=ctx)
    def target() -> Awaitable[str]:
        return inner()

    result = target()

    with pytest.raises(CustomError, match="boom"):
        await result

    assert len(ctx.events) == 2
    assert ctx.events[1].event_type == _InvocationEventType.FAILED.value
    assert ctx.events[1].payload == {
        "error": {
            "kind": "CustomError",
            "message": "boom",
        }
    }


@pytest.mark.asyncio
async def test_j03_log_invocation_sync_returning_awaitable_cancelled_logs_after_await() -> None:
    ctx = RecordingContext()

    async def inner() -> str:
        raise asyncio.CancelledError("stop")

    @log_invocation("op.awaitable", ctx=ctx)
    def target() -> Awaitable[str]:
        return inner()

    result = target()

    with pytest.raises(asyncio.CancelledError):
        await result

    assert len(ctx.events) == 2
    assert ctx.events[1].event_type == _InvocationEventType.CANCELLED.value
    assert ctx.events[1].payload["cancelled"] is True


@pytest.mark.asyncio
async def test_j04_log_invocation_sync_returning_awaitable_event_disabled_still_logs_failure_after_await() -> (
    None
):
    ctx = DisabledEventRecordingContext()

    async def inner() -> str:
        raise CustomError("boom")

    @log_invocation("op.disabled.awaitable", ctx=ctx)
    def target() -> Awaitable[str]:
        return inner()

    result = target()

    assert ctx.events == []

    with pytest.raises(CustomError, match="boom"):
        await result

    assert len(ctx.events) == 1
    assert ctx.events[0].event_type == _InvocationEventType.FAILED.value
    assert ctx.events[0].payload == {
        "error": {
            "kind": "CustomError",
            "message": "boom",
        }
    }


@pytest.mark.asyncio
async def test_j05_log_invocation_sync_returning_awaitable_event_disabled_still_logs_cancelled_after_await() -> (
    None
):
    ctx = DisabledEventRecordingContext()

    async def inner() -> str:
        raise asyncio.CancelledError("stop")

    @log_invocation("op.disabled.awaitable", ctx=ctx)
    def target() -> Awaitable[str]:
        return inner()

    result = target()

    assert ctx.events == []

    with pytest.raises(asyncio.CancelledError):
        await result

    assert len(ctx.events) == 1
    assert ctx.events[0].event_type == _InvocationEventType.CANCELLED.value
    assert ctx.events[0].payload == {
        "cancelled": True,
        "error": {
            "kind": "CancelledError",
            "message": "stop",
        },
    }


# K. Async decorator path


@pytest.mark.asyncio
async def test_k01_log_invocation_async_success_emits_invoke_and_success() -> None:
    ctx = RecordingContext()

    @log_invocation(
        "op.async",
        ctx=ctx,
        context_fields=("name",),
        log_kwargs_on_invoke=("count",),
        log_result_on_success=("value",),
    )
    async def target(name: str, count: int) -> Inner:
        _ = name
        _ = count
        return Inner(value="done", items=[1, 2])

    result = await target("alpha", 3)

    assert result == Inner(value="done", items=[1, 2])
    assert len(ctx.events) == 2
    assert ctx.events[0].event_type == _InvocationEventType.INVOKE.value
    assert ctx.events[0].payload == {
        "name": "alpha",
        "kwargs": {"count": 3},
    }
    assert ctx.events[1].event_type == _InvocationEventType.SUCCESS.value
    assert ctx.events[1].payload == {
        "name": "alpha",
        "result": {"value": "done"},
    }


@pytest.mark.asyncio
async def test_k02_log_invocation_async_uses_provider_context() -> None:
    ctx = RecordingContext()
    owner = EntityProvider(ctx, identity="async-owner")

    @log_invocation("op.async")
    async def target(self: EntityProvider, value: int) -> int:
        _ = self
        return value + 1

    assert await target(owner, 10) == 11

    assert len(ctx.events) == 2
    assert ctx.events[0].entity_id == "async-owner"
    assert ctx.events[1].entity_id == "async-owner"


@pytest.mark.asyncio
async def test_k03_log_invocation_async_event_disabled_emits_nothing_on_success() -> None:
    ctx = RecordingContext(enabled=False)

    @log_invocation("op.disabled", ctx=ctx)
    async def target() -> str:
        return "ok"

    assert await target() == "ok"
    assert ctx.events == []


@pytest.mark.asyncio
async def test_k04_log_invocation_async_event_disabled_still_logs_failure() -> None:
    ctx = DisabledEventRecordingContext()

    @log_invocation("op.disabled.async.fail", ctx=ctx)
    async def target() -> None:
        raise CustomError("boom")

    with pytest.raises(CustomError, match="boom"):
        await target()

    assert len(ctx.events) == 1
    assert ctx.events[0].event == "op.disabled.async.fail"
    assert ctx.events[0].event_type == _InvocationEventType.FAILED.value
    assert ctx.events[0].level == LogLevel.ERROR
    assert ctx.events[0].payload == {
        "error": {
            "kind": "CustomError",
            "message": "boom",
        }
    }


@pytest.mark.asyncio
async def test_k05_log_invocation_async_failure_logs_error_and_reraises() -> None:
    ctx = RecordingContext()

    @log_invocation("op.async.fail", ctx=ctx)
    async def target() -> None:
        raise CustomError("boom")

    with pytest.raises(CustomError, match="boom"):
        await target()

    assert len(ctx.events) == 2
    assert ctx.events[1].event_type == _InvocationEventType.FAILED.value
    assert ctx.events[1].payload == {
        "error": {
            "kind": "CustomError",
            "message": "boom",
        }
    }


@pytest.mark.asyncio
async def test_k06_log_invocation_async_failure_policy_suppresses_error_payload() -> None:
    ctx = RecordingContext()

    @log_invocation(
        "op.async.fail",
        ctx=ctx,
        log_error_policy=((SuppressedError, False),),
        error_level_suppressed=LogLevel.INFO,
    )
    async def target() -> None:
        raise SuppressedError("quiet")

    with pytest.raises(SuppressedError):
        await target()

    assert ctx.events[1].event_type == _InvocationEventType.FAILED.value
    assert ctx.events[1].level == LogLevel.INFO
    assert ctx.events[1].payload == {}


@pytest.mark.asyncio
async def test_k07_log_invocation_async_cancelled_logs_cancelled_and_reraises() -> None:
    ctx = RecordingContext()

    @log_invocation("op.async.cancel", ctx=ctx)
    async def target() -> None:
        raise asyncio.CancelledError("stop")

    with pytest.raises(asyncio.CancelledError):
        await target()

    assert len(ctx.events) == 2
    assert ctx.events[1].event_type == _InvocationEventType.CANCELLED.value
    assert ctx.events[1].payload == {
        "cancelled": True,
        "error": {
            "kind": "CancelledError",
            "message": "stop",
        },
    }


@pytest.mark.asyncio
async def test_k08_log_invocation_async_cancelled_skips_when_error_already_logged() -> None:
    err = asyncio.CancelledError("stop")
    ctx = RecordingContext(already_logged_errors={id(err)})

    @log_invocation("op.async.cancel", ctx=ctx)
    async def target() -> None:
        raise err

    with pytest.raises(asyncio.CancelledError):
        await target()

    assert len(ctx.events) == 1
    assert ctx.events[0].event_type == _InvocationEventType.INVOKE.value


@pytest.mark.asyncio
async def test_k09_log_invocation_async_event_disabled_still_logs_cancelled() -> None:
    ctx = DisabledEventRecordingContext()

    @log_invocation("op.disabled.async.cancel", ctx=ctx)
    async def target() -> None:
        raise asyncio.CancelledError("stop")

    with pytest.raises(asyncio.CancelledError):
        await target()

    assert len(ctx.events) == 1
    assert ctx.events[0].event == "op.disabled.async.cancel"
    assert ctx.events[0].event_type == _InvocationEventType.CANCELLED.value
    assert ctx.events[0].level == LogLevel.INFO
    assert ctx.events[0].payload == {
        "cancelled": True,
        "error": {
            "kind": "CancelledError",
            "message": "stop",
        },
    }


# L. Metadata preservation


def test_l01_log_invocation_preserves_sync_function_metadata() -> None:
    ctx = RecordingContext()

    @log_invocation("op.meta", ctx=ctx)
    def target() -> str:
        """Original docstring."""
        return "ok"

    assert target.__name__ == "target"
    assert target.__doc__ == "Original docstring."


@pytest.mark.asyncio
async def test_l02_log_invocation_preserves_async_function_metadata() -> None:
    ctx = RecordingContext()

    @log_invocation("op.meta", ctx=ctx)
    async def target() -> str:
        """Original async docstring."""
        return "ok"

    assert target.__name__ == "target"
    assert target.__doc__ == "Original async docstring."
    assert await target() == "ok"
