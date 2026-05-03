from __future__ import annotations

import logging
from typing import Any, Dict, Awaitable

import pytest
import asyncio

from mvx.logger.invocation_logging import (
    log_invocation,
    _build_logged_kwargs,
    _build_logged_result,
)

import mvx.logger.invocation_logging as inv_mod

# ---------- helpers ----------


def _get_logger(name: str) -> logging.Logger:
    """
    Return a logger configured for tests (no handlers, DEBUG, propagate enabled).
    """
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.propagate = True
    logger.setLevel(logging.DEBUG)
    return logger


# ---------- A: logger selection ----------


def test_a1_logger_from_get_logger(caplog: pytest.LogCaptureFixture) -> None:
    """
    Method on an object with get_logger() must use that logger.
    """

    test_logger = _get_logger("test.op_logging.a1")

    class ObjWithGetLogger:
        def __init__(self, logger: logging.Logger) -> None:
            self._logger = logger

        def get_logger(self) -> logging.Logger:
            return self._logger

        @log_invocation(evt_prefix="test.a")
        def do(self) -> str:
            return "ok"

    obj = ObjWithGetLogger(test_logger)

    with caplog.at_level(logging.DEBUG, logger=test_logger.name):
        result = obj.do()

    assert result == "ok"

    # Expect two records: invoke + success
    assert len(caplog.records) == 2
    for rec in caplog.records:
        assert rec.name == test_logger.name
        assert rec.evt in ("test.a.invoke", "test.a.success")


def test_a2_logger_from_logger_attr(caplog: pytest.LogCaptureFixture) -> None:
    """
    Method on an object with .logger attribute must use that logger.
    """

    test_logger = _get_logger("test.op_logging.a2")

    class ObjWithLoggerAttr:
        def __init__(self, logger: logging.Logger) -> None:
            self.logger = logger

        @log_invocation(evt_prefix="test.a")
        def do(self) -> str:
            return "ok"

    obj = ObjWithLoggerAttr(test_logger)

    with caplog.at_level(logging.DEBUG, logger=test_logger.name):
        result = obj.do()

    assert result == "ok"
    assert len(caplog.records) == 2
    for rec in caplog.records:
        assert rec.name == test_logger.name
        assert rec.evt in ("test.a.invoke", "test.a.success")


def test_a3_logger_from_module_fallback(caplog: pytest.LogCaptureFixture) -> None:
    """
    Function without self/get_logger/logger must fallback to module logger.
    """

    @log_invocation(evt_prefix="test.a")
    def func() -> str:
        return "ok"

    # Fallback logger name is func.__module__ (which is __name__ here)
    with caplog.at_level(logging.DEBUG, logger=__name__):
        result = func()

    assert result == "ok"
    assert len(caplog.records) == 2
    for rec in caplog.records:
        assert rec.name == __name__
        assert rec.evt in ("test.a.invoke", "test.a.success")


def test_a4_explicit_logger_overrides_object_loggers(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Explicit logger parameter must override get_logger()/logger on the object.
    """

    base_logger = _get_logger("test.op_logging.a4.base")
    override_logger = _get_logger("test.op_logging.a4.override")

    class ObjWithBoth:
        def __init__(self, base: logging.Logger) -> None:
            self._base = base
            self.logger = base

        def get_logger(self) -> logging.Logger:
            return self._base

        @log_invocation(
            evt_prefix="test.a",
            logger=override_logger,
        )
        def do(self) -> str:
            return "ok"

    obj = ObjWithBoth(base_logger)

    with caplog.at_level(logging.DEBUG, logger=override_logger.name):
        result = obj.do()

    assert result == "ok"
    assert len(caplog.records) == 2
    for rec in caplog.records:
        # must use override logger, not base_logger
        assert rec.name == override_logger.name
        assert rec.evt in ("test.a.invoke", "test.a.success")


def test_a5_broken_get_logger_falls_back_to_module_logger(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    If get_logger() raises, decorator must fall back to module-level logger.
    """

    class ObjWithBrokenGetLogger:
        def get_logger(self) -> logging.Logger:
            raise RuntimeError("get_logger failed")

        @log_invocation(evt_prefix="test.a")
        def do(self) -> str:
            return "ok"

    obj = ObjWithBrokenGetLogger()

    # Fallback logger is logging.getLogger(func.__module__) for the wrapped method.
    # For a bound method defined in this module, that should be this module's name.
    module_logger_name = __name__

    with caplog.at_level(logging.DEBUG, logger=module_logger_name):
        result = obj.do()

    assert result == "ok"
    assert len(caplog.records) == 2
    evts = {rec.evt for rec in caplog.records}
    assert evts == {"test.a.invoke", "test.a.success"}


# ---------- B: Happy-path (success) ----------


def test_b1_happy_path_basic(caplog: pytest.LogCaptureFixture) -> None:
    """
    Successful call must emit invoke + success with derived op name.
    """

    @log_invocation(evt_prefix="redis.ctx")
    def do_something() -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        result = do_something()

    assert result == "ok"

    assert len(caplog.records) == 2

    evts = [rec.evt for rec in caplog.records]
    assert evts == ["redis.ctx.invoke", "redis.ctx.success"]

    data_invoke = caplog.records[0].data
    assert data_invoke == {}
    data_success = caplog.records[1].data
    assert data_success == {}


def test_b2_happy_path_empty_prefix(caplog: pytest.LogCaptureFixture) -> None:
    """
    Empty evt_prefix must yield bare 'invoke'/'success' events.
    """

    @log_invocation(evt_prefix="")
    def do_something() -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = do_something()

    assert len(caplog.records) == 2

    evts = [rec.evt for rec in caplog.records]
    assert evts == ["invoke", "success"]


def test_b3_happy_path_custom_success_level(caplog: pytest.LogCaptureFixture) -> None:
    """
    Success_level must override invoke_level for success event.
    """

    @log_invocation(evt_prefix="redis.ctx", invoke_level=logging.DEBUG, success_level=logging.INFO)
    def do_something() -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = do_something()

    assert len(caplog.records) == 2

    invoke_rec, success_rec = caplog.records

    assert invoke_rec.levelno == logging.DEBUG
    assert success_rec.levelno == logging.INFO


# ---------- C: error-path (exceptions) ----------


def test_c1_error_basic_exception(caplog: pytest.LogCaptureFixture) -> None:
    """
    Failing call must emit invoke + failed and re-raise the original exception.
    """

    @log_invocation(evt_prefix="redis.ctx")
    def do_fail() -> None:
        raise ValueError("boom")

    with caplog.at_level(logging.DEBUG, logger=__name__):
        with pytest.raises(ValueError) as excinfo:
            do_fail()

    assert str(excinfo.value) == "boom"

    assert len(caplog.records) == 2

    evts = [rec.evt for rec in caplog.records]
    assert evts == ["redis.ctx.invoke", "redis.ctx.failed"]

    error_payload = caplog.records[1].data["error"]
    assert error_payload["kind"] == "ValueError"
    assert error_payload["message"] == "boom"


class DummyToLogExtra(Exception):
    """Synthetic exception with to_log_extra() for payload mapping."""

    def __init__(self, payload: Dict[str, Any]) -> None:
        super().__init__("dummy")
        self._payload = payload

    def to_log_payload(self) -> Dict[str, Any]:
        return self._payload


def test_c2_error_with_to_log_extra(caplog: pytest.LogCaptureFixture) -> None:
    """
    Exception exposing to_log_extra() must use its payload verbatim.
    """

    payload = {"code": 1234, "code_desc": "SYNTHETIC", "extra": "x"}

    @log_invocation(evt_prefix="redis.ctx")
    def do_fail() -> None:
        raise DummyToLogExtra(payload)

    with caplog.at_level(logging.DEBUG, logger=__name__):
        with pytest.raises(DummyToLogExtra):
            do_fail()

    assert len(caplog.records) == 2

    error_payload = caplog.records[1].data["error"]
    assert error_payload == payload


def test_c3_error_custom_error_level(caplog: pytest.LogCaptureFixture) -> None:
    """
    Error_level must define log level for failed event.
    """

    @log_invocation(evt_prefix="redis.ctx", error_level=logging.WARNING)
    def do_fail() -> None:
        raise RuntimeError("oops")

    with caplog.at_level(logging.DEBUG, logger=__name__):
        with pytest.raises(RuntimeError):
            do_fail()

    assert len(caplog.records) == 2

    failed_rec = caplog.records[1]
    assert failed_rec.evt == "redis.ctx.failed"
    assert failed_rec.levelno == logging.WARNING


# ---------- D: BaseException behavior ----------


def test_d1_keyboard_interrupt_not_caught(caplog: pytest.LogCaptureFixture) -> None:
    """
    KeyboardInterrupt (BaseException) must propagate, only invoke may be logged.
    """

    @log_invocation(evt_prefix="redis.ctx")
    def do_interrupt() -> None:
        raise KeyboardInterrupt()

    with caplog.at_level(logging.DEBUG, logger=__name__):
        with pytest.raises(KeyboardInterrupt):
            do_interrupt()

    # Only invoke event is logged; no failed event because we do not catch BaseException.
    assert len(caplog.records) == 1

    rec = caplog.records[0]
    assert rec.evt == "redis.ctx.invoke"


# ---------- E: _build_logged_kwarg ----------


def test_e1_build_logged_kwargs_basic() -> None:
    """
    Only requested fields present in source_kwargs must be included.
    """
    field_names = ("x", "y")
    source_kwargs: Dict[str, Any] = {"x": 1, "y": "abc", "z": 999}

    result = _build_logged_kwargs(field_names, source_kwargs)

    assert isinstance(result, dict)
    assert set(result.keys()) == {"x", "y"}
    assert result["x"] == 1
    assert result["y"] == "abc"


def test_e2_build_logged_kwargs_empty_fields() -> None:
    """
    Empty field_names must result in an empty dict, regardless of source_kwargs.
    """
    field_names: tuple[str, ...] = ()
    source_kwargs = {"a": 1, "b": 2}

    result = _build_logged_kwargs(field_names, source_kwargs)

    assert result == {}


def test_e3_build_logged_kwargs_non_primitive_value() -> None:
    """
    Non-primitive values must be represented as '<TypeName>'.
    """

    class Dummy:
        pass

    field_names = ("obj",)
    source_kwargs = {"obj": Dummy()}

    result = _build_logged_kwargs(field_names, source_kwargs)

    assert isinstance(result, dict)
    assert set(result.keys()) == {"obj"}
    assert result["obj"] == "<Dummy>"


def test_e4_build_logged_kwargs_list_and_dict_values() -> None:
    """
    list/dict kwargs must be normalized one level deep with truncation and summary.
    """
    # list with more than 10 items
    lst = list(range(15))

    # dict with more than 10 keys
    payload = {f"k{i}": i for i in range(14)}

    field_names = ("items", "payload")
    source_kwargs = {"items": lst, "payload": payload}

    result = _build_logged_kwargs(field_names, source_kwargs)

    assert isinstance(result, dict)
    assert set(result.keys()) == {"items", "payload"}

    # items: list normalization
    items_norm = result["items"]
    assert isinstance(items_norm, list)
    assert items_norm[:10] == list(range(10))
    assert items_norm[-1] == "... (5 more)"

    # payload: dict normalization
    payload_norm = result["payload"]
    assert isinstance(payload_norm, dict)
    # 10 keys + '__more__'
    assert len(payload_norm) == 11
    for i in range(10):
        assert payload_norm[f"k{i}"] == i
    assert payload_norm["__more__"] == "4 more keys"


def test_e5_build_logged_kwargs_dotted_path_no_alias() -> None:
    """
    Dotted path without alias must resolve through attrs and use last segment as key.
    """

    class Obj:
        def __init__(self, value: int) -> None:
            self.op_name = value

    field_names = ("op.op_name",)
    source_kwargs: Dict[str, Any] = {"op": Obj(42)}

    result = _build_logged_kwargs(field_names, source_kwargs)

    assert isinstance(result, dict)

    # Alias is derived from the last path segment: "op_name"
    assert set(result.keys()) == {"op_name"}
    assert result["op_name"] == 42


def test_e6_build_logged_kwargs_dotted_path_with_alias() -> None:
    """
    'alias=path' must store value under alias key.
    """

    class Obj:
        def __init__(self, name: str) -> None:
            self.op_name = name

    field_names = ("name=op.op_name",)
    source_kwargs: Dict[str, Any] = {"op": Obj("create_user")}

    result = _build_logged_kwargs(field_names, source_kwargs)

    assert isinstance(result, dict)
    assert set(result.keys()) == {"name"}
    assert result["name"] == "create_user"


def test_e7_build_logged_kwargs_missing_kw_or_attr_ignored() -> None:
    """
    If kw or attribute is missing on the path, spec must be ignored.
    """

    class Obj:
        def __init__(self, value: int) -> None:
            self.exist = value

    # Missing kw: "x" not in kwargs, "obj.missing" attr also missing
    field_names = ("x", "obj.missing")
    source_kwargs: Dict[str, Any] = {"obj": Obj(10)}

    result = _build_logged_kwargs(field_names, source_kwargs)

    # Both specs must be ignored, nothing logged
    assert result == {}


def test_e8_build_logged_kwargs_path_to_composite_value() -> None:
    """
    If path resolves to composite value, it must be logged as '<TypeName>'.
    """

    class Inner:
        pass

    class Obj:
        def __init__(self) -> None:
            self.inner = Inner()

    field_names = ("op.inner",)
    source_kwargs: Dict[str, Any] = {"op": Obj()}

    result = _build_logged_kwargs(field_names, source_kwargs)

    assert isinstance(result, dict)
    # Alias derived from last segment: "inner"
    assert set(result.keys()) == {"inner"}
    assert result["inner"] == "<Inner>"


def test_e9_build_logged_kwargs_unbounded_dict_uses_all_items() -> None:
    """
    'payload!' must log full dict without '__more__' and without item limit.
    """
    # 20 keys > default MAX_ITEMS (10)
    payload = {f"k{i}": i for i in range(20)}
    field_names = ("payload!",)
    source_kwargs: Dict[str, Any] = {"payload": payload}

    result = _build_logged_kwargs(field_names, source_kwargs)

    assert isinstance(result, dict)
    assert set(result.keys()) == {"payload"}

    payload_norm = result["payload"]
    assert isinstance(payload_norm, dict)
    # All keys must be present, no '__more__'
    assert len(payload_norm) == 20
    for i in range(20):
        assert payload_norm[f"k{i}"] == i
    assert "__more__" not in payload_norm


def test_e10_build_logged_kwargs_unbounded_list_uses_all_items() -> None:
    """
    G5.10: 'items!' must log full list without summary element and without item limit.
    """
    items = list(range(25))
    field_names = ("items!",)
    source_kwargs: Dict[str, Any] = {"items": items}

    result = _build_logged_kwargs(field_names, source_kwargs)

    assert isinstance(result, dict)
    assert set(result.keys()) == {"items"}

    items_norm = result["items"]
    assert isinstance(items_norm, list)
    # All elements must be present, no summary string at the end
    assert items_norm == items


def test_e11_build_logged_kwargs_uses_to_log_payload_for_object() -> None:
    """
    If kwarg value implements to_log_payload(), its payload must be used as-is.
    """

    class ObjWithPayload:
        def __init__(self, user_id: str) -> None:
            self.user_id = user_id

        def to_log_payload(self) -> dict[str, Any]:
            # Deliberately more than MAX_ITEMS keys to ensure no '__more__' appears.
            return {f"k{i}": i for i in range(20)}

    field_names = ("obj",)
    source_kwargs: Dict[str, Any] = {"obj": ObjWithPayload("u-1")}

    result = _build_logged_kwargs(field_names, source_kwargs)

    assert isinstance(result, dict)
    assert set(result.keys()) == {"obj"}

    obj_payload = result["obj"]
    assert isinstance(obj_payload, dict)
    # All keys must be present, no '__more__'
    assert len(obj_payload) == 20
    for i in range(20):
        assert obj_payload[f"k{i}"] == i
    assert "__more__" not in obj_payload


def test_e12_build_logged_kwargs_profile_default() -> None:
    """
    Profile-aware specs must resolve only for matching active profile ("default").
    """
    field_specs = (
        "default,full:sharding_key",
        "full:op_params",
        "op_id",
    )

    source_kwargs: Dict[str, Any] = {
        "sharding_key": "sh-1",
        "op_params": {"a": 1},
        "op_id": "op-1",
    }

    result = _build_logged_kwargs(
        field_specs,
        source_kwargs,
        active_profile="default",
    )

    # For "default":
    # - "default,full:sharding_key" -> "sharding_key" included
    # - "full:op_params" -> excluded
    # - "op_id" (legacy) -> always included
    assert isinstance(result, dict)
    assert set(result.keys()) == {"sharding_key", "op_id"}
    assert result["sharding_key"] == "sh-1"
    assert result["op_id"] == "op-1"


def test_e13_build_logged_kwargs_profile_full() -> None:
    """
    Profile-aware specs must resolve for 'full' profile as expected.
    """
    field_specs = (
        "default,full:sharding_key",
        "full:op_params!",
        "op_id",
        "debug,audit:raw_payload",
    )

    source_kwargs: Dict[str, Any] = {
        "sharding_key": "sh-1",
        "op_params": {"a": 1},
        "op_id": "op-1",
        "raw_payload": {"x": 1},
    }

    result = _build_logged_kwargs(
        field_specs,
        source_kwargs,
        active_profile="full",
    )

    # For "full":
    # - "default,full:sharding_key" -> included
    # - "full:op_params!" -> included (without item limit)
    # - "op_id" (legacy) -> always included
    # - "debug,audit:raw_payload" -> excluded
    assert isinstance(result, dict)
    assert set(result.keys()) == {"sharding_key", "op_params", "op_id"}

    assert result["sharding_key"] == "sh-1"
    assert result["op_id"] == "op-1"

    op_params_norm = result["op_params"]
    assert isinstance(op_params_norm, dict)
    # All items must be present, no '__more__' because of '!'
    assert op_params_norm == {"a": 1}
    assert "__more__" not in op_params_norm


def test_e14_build_logged_kwargs_profile_non_matching() -> None:
    """
    Profile-aware specs must be skipped when active profile is not listed.
    """
    field_specs = (
        "default,full:sharding_key",
        "full:op_params",
        "debug:raw_payload",
        "op_id",
    )

    source_kwargs: Dict[str, Any] = {
        "sharding_key": "sh-1",
        "op_params": {"a": 1},
        "raw_payload": {"x": 1},
        "op_id": "op-1",
    }

    result = _build_logged_kwargs(
        field_specs,
        source_kwargs,
        active_profile="audit",
    )

    # For "audit":
    # - "default,full:sharding_key" -> excluded
    # - "full:op_params" -> excluded
    # - "debug:raw_payload" -> excluded
    # - "op_id" (legacy) -> included
    assert isinstance(result, dict)
    assert set(result.keys()) == {"op_id"}
    assert result["op_id"] == "op-1"


def test_e15_build_logged_kwargs_len_tail_on_kwarg() -> None:
    """
    '.len()' tail in field spec must apply len() to the resolved value.
    """

    field_specs = (
        "orders.len()",
        "orders_count=orders.len()",
    )
    source_kwargs: Dict[str, Any] = {"orders": [10, 20, 30, 40]}

    result = _build_logged_kwargs(field_specs, source_kwargs)

    assert isinstance(result, dict)
    # For plain 'orders.len()' alias is derived from last segment: 'len()'
    assert result["len()"] == 4
    # For alias form value must be the same
    assert result["orders_count"] == 4


# ---------- F: _build_logged_result ----------
def test_f1_build_logged_result_primitive_ignores_specs() -> None:
    """
    For primitive result, specs are ignored and whole result is normalized.
    """
    specs: tuple[str, ...] = ("value", "ignored.path")
    result_obj = 123

    result = _build_logged_result(specs, result_obj)

    assert result == 123  # primitive passes through unchanged


def test_f2_build_logged_result_list_ignores_specs() -> None:
    """
    For list/tuple/dict result, specs are ignored and whole result is normalized.
    """
    specs: tuple[str, ...] = ("items", "ignored.path")
    result_obj = list(range(15))

    result = _build_logged_result(specs, result_obj)

    assert isinstance(result, list)
    assert result[:10] == list(range(10))
    assert result[-1] == "... (5 more)"


def test_f3_build_logged_result_dict_ignores_specs() -> None:
    """
    For dict result, specs are ignored and whole result is normalized.
    """
    specs: tuple[str, ...] = ("payload",)
    result_obj = {f"k{i}": i for i in range(12)}

    result = _build_logged_result(specs, result_obj)

    assert isinstance(result, dict)
    assert len(result) == 11
    for i in range(10):
        assert result[f"k{i}"] == i
    assert result["__more__"] == "2 more keys"


def test_f4_build_logged_result_composite_no_specs_falls_back_to_typename() -> None:
    """
    For composite result and empty specs, log as '<TypeName>'.
    """

    class DummyResult:
        def __init__(self, x: int) -> None:
            self.x = x

    specs: tuple[str, ...] = ()
    result_obj = DummyResult(10)

    result = _build_logged_result(specs, result_obj)

    assert result == "<DummyResult>"


def test_f5_build_logged_result_composite_with_fields() -> None:
    """
    For composite result and non-empty specs, log selected fields as dict.
    """

    class Result:
        def __init__(self, status: str, task_id: str, ttl_ms: int) -> None:
            self.status = status
            self.task_id = task_id
            self.ttl_ms = ttl_ms

    specs = ("status", "task_id")
    result_obj = Result("NEW", "id-123", 5000)

    result = _build_logged_result(specs, result_obj)

    assert isinstance(result, dict)
    assert result == {"status": "NEW", "task_id": "id-123"}


def test_f6_build_logged_result_composite_with_alias_and_nested_attr() -> None:
    """
    Composite result must support alias=path specs.
    """

    class Payload:
        def __init__(self, user_id: str) -> None:
            self.user_id = user_id

    class Result:
        def __init__(self, payload: Payload) -> None:
            self.payload = payload

    specs = ("uid=payload.user_id",)
    result_obj = Result(Payload("user-42"))

    result = _build_logged_result(specs, result_obj)

    assert isinstance(result, dict)
    assert result == {"uid": "user-42"}


def test_f7_build_logged_result_composite_all_specs_fail_fallback_typename() -> None:
    """
    If no fields resolved for composite result, fall back to '<TypeName>'.
    """

    class Result:
        def __init__(self) -> None:
            self.ok = True

    specs = ("missing", "payload.user_id")
    result_obj = Result()

    result = _build_logged_result(specs, result_obj)

    assert result == "<Result>"


def test_f8_build_logged_result_tuple_with_index_and_alias() -> None:
    """
    For tuple result, indexed specs with alias must extract tuple elements and attrs.
    """

    class Payload:
        def __init__(self, ttl_ms: int) -> None:
            self.ttl_ms = ttl_ms

    # Simulate: (status: str, op_id: str, payload: Payload)
    result_obj = ("SUCCESS", "op-123", Payload(60000))

    specs: tuple[str, ...] = (
        "status=0",  # first element (string)
        "op_id=1",  # second element (string)
        "ttl_ms=2.ttl_ms",  # third element's attribute
    )

    result = _build_logged_result(specs, result_obj)

    assert isinstance(result, dict)
    assert result == {
        "status": "SUCCESS",
        "op_id": "op-123",
        "ttl_ms": 60000,
    }


def test_f9_build_logged_result_tuple_without_alias_uses_derived_names() -> None:
    """
    For tuple result, specs without alias must derive names from path or index.
    """

    class Payload:
        def __init__(self, ttl_ms: int) -> None:
            self.ttl_ms = ttl_ms

    result_obj = ("PENDING", "op-999", Payload(120000))

    specs: tuple[str, ...] = (
        "0",  # first element -> alias item0
        "2.ttl_ms",  # third element's attribute -> alias ttl_ms
    )

    result = _build_logged_result(specs, result_obj)

    assert isinstance(result, dict)
    # "0" -> alias "item0"
    # "2.ttl_ms" -> alias "ttl_ms"
    assert result["item0"] == "PENDING"
    assert result["ttl_ms"] == 120000
    assert set(result.keys()) == {"item0", "ttl_ms"}


def test_f10_build_logged_result_unbounded_dict_field() -> None:
    """
    'payload!' in log_result_on_success must log full dict field without '__more__'.
    """

    class Result:
        def __init__(self, payload: Dict[str, Any]) -> None:
            self.payload = payload

    payload = {f"k{i}": i for i in range(20)}
    specs: tuple[str, ...] = ("payload!",)
    result_obj = Result(payload)

    result = _build_logged_result(specs, result_obj)

    assert isinstance(result, dict)
    assert set(result.keys()) == {"payload"}

    payload_norm = result["payload"]
    assert isinstance(payload_norm, dict)
    # All keys, no '__more__'
    assert len(payload_norm) == 20
    for i in range(20):
        assert payload_norm[f"k{i}"] == i
    assert "__more__" not in payload_norm


def test_f11_build_logged_result_unbounded_list_field() -> None:
    """
    'items!' in log_result_on_success must log full list field without summary element.
    """

    class Result:
        def __init__(self, items: list[int]) -> None:
            self.items = items

    items = list(range(25))
    specs: tuple[str, ...] = ("items!",)
    result_obj = Result(items)

    result = _build_logged_result(specs, result_obj)

    assert isinstance(result, dict)
    assert set(result.keys()) == {"items"}

    items_norm = result["items"]
    assert isinstance(items_norm, list)
    # All elements, no '... (N more)' tail
    assert items_norm == items


def test_f12_build_logged_result_tuple_with_len_tail() -> None:
    """
    For tuple result, '.len()' tail in specs must apply len() to the selected element.
    """
    # Single-element tuple: first element is a list
    result_obj = (["a", "b", "c"],)

    specs: tuple[str, ...] = ("len0=0.len()",)

    result = _build_logged_result(specs, result_obj)

    assert isinstance(result, dict)
    assert result == {"len0": 3}


def test_f13_build_logged_result_composite_with_len_tail() -> None:
    """
    For composite result, '.len()' tail must apply len() to the resolved attribute.
    """

    class Result:
        def __init__(self, items: list[int]) -> None:
            self.items = items

    specs: tuple[str, ...] = ("items_len=items.len()",)
    result_obj = Result([1, 2, 3, 4, 5])

    result = _build_logged_result(specs, result_obj)

    assert isinstance(result, dict)
    assert result == {"items_len": 5}


# ---------- G: log_kwargs_on_invoke behavior ----------


def test_g1_log_fields_basic_single_kwarg(caplog: pytest.LogCaptureFixture) -> None:
    """
    log_fields_on_invoke must log only selected kwargs on invoke.
    """

    @log_invocation(evt_prefix="test.h", log_kwargs_on_invoke=("x",))
    def fn(*, x: int, y: int) -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        result = fn(x=1, y=2)

    assert result == "ok"
    assert len(caplog.records) == 2

    invoke_rec, success_rec = caplog.records

    assert invoke_rec.evt == "test.h.invoke"
    assert success_rec.evt == "test.h.success"

    # invoke: kwargs must contain only "x"
    invoke_data = invoke_rec.data
    assert "kwargs" in invoke_data
    assert invoke_data["kwargs"] == {"x": 1}

    # success: no kwargs are logged
    success_data = success_rec.data
    assert "kwargs" not in success_data


def test_g2_log_fields_missing_names_ignored(caplog: pytest.LogCaptureFixture) -> None:
    """
    Fields listed in log_fields_on_invoke but absent in kwargs must be ignored.
    """

    @log_invocation(evt_prefix="test.h", log_kwargs_on_invoke=("x", "z"))
    def fn(*, x: int, y: int) -> str:
        _ = x, y
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn(x=10, y=20)

    assert len(caplog.records) == 2
    invoke_rec = caplog.records[0]

    assert invoke_rec.evt == "test.h.invoke"
    data = invoke_rec.data
    assert "kwargs" in data
    # Only "x" is present in kwargs, "z" is ignored
    assert data["kwargs"] == {"x": 10}


def test_g3_log_fields_normalizes_string_and_list(caplog: pytest.LogCaptureFixture) -> None:
    """
    log_fields_on_invoke must normalize long strings and long lists.
    """

    long_str = "x" * 250
    long_list = list(range(15))

    @log_invocation(evt_prefix="test.h", log_kwargs_on_invoke=("s", "items"))
    def fn(*, s: str, items: list[int]) -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn(s=long_str, items=long_list)

    assert len(caplog.records) == 2
    invoke_rec = caplog.records[0]

    assert invoke_rec.evt == "test.h.invoke"
    data = invoke_rec.data
    assert "kwargs" in data

    kwargs_logged = data["kwargs"]
    assert set(kwargs_logged.keys()) == {"s", "items"}

    # String must be truncated to 200 chars + "..."
    s_norm = kwargs_logged["s"]
    assert isinstance(s_norm, str)
    assert s_norm.startswith("x" * 200)
    assert s_norm.endswith("...")
    assert len(s_norm) == 203

    # List must be truncated to 10 items + summary
    items_norm = kwargs_logged["items"]
    assert isinstance(items_norm, list)
    assert items_norm[:10] == list(range(10))
    assert items_norm[-1] == "... (5 more)"
    assert len(items_norm) == 11


def test_g4_log_fields_dotted_path_without_alias(caplog: pytest.LogCaptureFixture) -> None:
    """
    Dotted path without alias must log attribute under the last path segment name.
    """

    class Op:
        def __init__(self, name: str) -> None:
            self.op_name = name

    @log_invocation(
        evt_prefix="test.h",
        log_kwargs_on_invoke=("op.op_name",),
    )
    def fn(*, sharding_key: str, op: Op) -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn(sharding_key="crew-1", op=Op("create_user"))

    assert len(caplog.records) == 2
    invoke_rec = caplog.records[0]

    assert invoke_rec.evt == "test.h.invoke"
    data = invoke_rec.data
    assert "kwargs" in data
    # op.op_name -> key "op_name"
    assert data["kwargs"] == {"op_name": "create_user"}


def test_g5_log_fields_dotted_path_with_alias(caplog: pytest.LogCaptureFixture) -> None:
    """
    'alias=path' must log attribute under alias key.
    """

    class Op:
        def __init__(self, name: str) -> None:
            self.op_name = name

    @log_invocation(
        evt_prefix="test.h",
        log_kwargs_on_invoke=("name=op.op_name",),
    )
    def fn(*, sharding_key: str, op: Op) -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn(sharding_key="crew-1", op=Op("create_user"))

    assert len(caplog.records) == 2
    invoke_rec = caplog.records[0]

    assert invoke_rec.evt == "test.h.invoke"
    data = invoke_rec.data
    assert "kwargs" in data
    # alias=op.op_name -> key "name"
    assert data["kwargs"] == {"name": "create_user"}


def test_g6_log_kwargs_profile_default(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Profile-aware log_kwargs_on_invoke must respect 'default' profile.
    """
    # Force active log profile to 'default' inside invocation_logging module.
    monkeypatch.setattr(inv_mod, "get_active_log_profile", lambda: "default")

    class Op:
        def __init__(self, name: str) -> None:
            self.op_name = name

    @log_invocation(
        evt_prefix="test.g.profile",
        log_kwargs_on_invoke=(
            "default,full: sharding_key",
            "full: op_params!",
            "op_id",
            "debug,audit: raw_payload",
        ),
    )
    def fn(
        *,
        sharding_key: str,
        op_id: str,
        op: Op,
        op_params: dict[str, Any],
        raw_payload: dict[str, Any],
    ) -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        result = fn(
            sharding_key="crew-1",
            op_id="op-123",
            op=Op("create_user"),
            op_params={"x": 1},
            raw_payload={"y": 2},
        )

    assert result == "ok"
    assert len(caplog.records) == 2

    invoke_rec, success_rec = caplog.records
    assert invoke_rec.evt == "test.g.profile.invoke"
    assert success_rec.evt == "test.g.profile.success"

    invoke_data = invoke_rec.data
    assert "kwargs" in invoke_data
    # For 'default' profile:
    # - "default,full:sharding_key" -> included
    # - "full:op_params!"           -> skipped
    # - "op_id"                     -> included
    # - "debug,audit:raw_payload"   -> skipped
    assert invoke_data["kwargs"] == {
        "sharding_key": "crew-1",
        "op_id": "op-123",
    }

    # Success payload must not contain kwargs.
    assert "kwargs" not in success_rec.data


def test_g7_log_kwargs_profile_full(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Profile-aware log_kwargs_on_invoke must respect 'full' profile.
    """
    monkeypatch.setattr(inv_mod, "get_active_log_profile", lambda: "full")

    class Op:
        def __init__(self, name: str) -> None:
            self.op_name = name

    @log_invocation(
        evt_prefix="test.g.profile",
        log_kwargs_on_invoke=(
            "default,full:sharding_key",
            "full:op_params!",
            "op_id",
            "debug,audit:raw_payload",
        ),
    )
    def fn(
        *,
        sharding_key: str,
        op_id: str,
        op: Op,
        op_params: dict[str, Any],
        raw_payload: dict[str, Any],
    ) -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn(
            sharding_key="crew-1",
            op_id="op-123",
            op=Op("create_user"),
            op_params={"x": 1, "y": 2},
            raw_payload={"z": 3},
        )

    assert len(caplog.records) == 2
    invoke_rec, success_rec = caplog.records

    assert invoke_rec.evt == "test.g.profile.invoke"
    invoke_data = invoke_rec.data
    assert "kwargs" in invoke_data

    kwargs_logged = invoke_data["kwargs"]

    # For 'full' profile:
    # - "default,full:sharding_key" -> included
    # - "full:op_params!"           -> included (without item limit)
    # - "op_id"                     -> included
    # - "debug,audit:raw_payload"   -> skipped
    assert set(kwargs_logged.keys()) == {"sharding_key", "op_params", "op_id"}
    assert kwargs_logged["sharding_key"] == "crew-1"
    assert kwargs_logged["op_id"] == "op-123"

    op_params_norm = kwargs_logged["op_params"]
    assert isinstance(op_params_norm, dict)
    # All items must be present, no '__more__' due to '!'
    assert op_params_norm == {"x": 1, "y": 2}
    assert "__more__" not in op_params_norm

    # Success payload must not contain kwargs.
    assert "kwargs" not in success_rec.data


def test_g8_log_kwargs_profile_debug_only_specific_fields(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    For 'debug' profile, only debug/audit specs plus legacy specs must be logged.
    """
    monkeypatch.setattr(inv_mod, "get_active_log_profile", lambda: "debug")

    @log_invocation(
        evt_prefix="test.g.profile",
        log_kwargs_on_invoke=(
            "default,full:sharding_key",
            "full:op_params",
            "debug,audit:raw_payload",
            "op_id",
        ),
    )
    def fn(
        *, sharding_key: str, op_id: str, op_params: dict[str, Any], raw_payload: dict[str, Any]
    ) -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn(
            sharding_key="crew-1",
            op_id="op-123",
            op_params={"x": 1},
            raw_payload={"y": 2},
        )

    assert len(caplog.records) == 2
    invoke_rec, success_rec = caplog.records

    assert invoke_rec.evt == "test.g.profile.invoke"
    invoke_data = invoke_rec.data
    assert "kwargs" in invoke_data

    kwargs_logged = invoke_data["kwargs"]
    # For 'debug':
    # - "default,full:sharding_key" -> skipped
    # - "full:op_params"           -> skipped
    # - "debug,audit:raw_payload"  -> included
    # - "op_id" (legacy)           -> included
    assert set(kwargs_logged.keys()) == {"raw_payload", "op_id"}
    assert kwargs_logged["op_id"] == "op-123"

    raw_payload_norm = kwargs_logged["raw_payload"]
    assert isinstance(raw_payload_norm, dict)
    assert raw_payload_norm == {"y": 2}

    # Success payload must not contain kwargs.
    assert "kwargs" not in success_rec.data


def test_g9_log_kwargs_uses_bound_arguments_for_positionals(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Positional arguments must be visible under their parameter names
    in log_kwargs_on_invoke.
    """

    @log_invocation(
        evt_prefix="test.g.bound",
        log_kwargs_on_invoke=("x", "y"),
    )
    def fn(x: int, y: int) -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        result = fn(10, 20)

    assert result == "ok"
    assert len(caplog.records) == 2

    invoke_rec, success_rec = caplog.records
    assert invoke_rec.evt == "test.g.bound.invoke"
    assert success_rec.evt == "test.g.bound.success"

    invoke_data = invoke_rec.data
    assert "kwargs" in invoke_data
    # Both positional args must be available under their parameter names.
    assert invoke_data["kwargs"] == {"x": 10, "y": 20}


def test_g10_log_kwargs_for_method_with_positional_args(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    For bound methods, positional args (including 'self') must be bound to
    parameter names; specs must be able to read non-self args by name.
    """

    class Manager:
        def __init__(self) -> None:
            self._last: list[str] | None = None

        @log_invocation(
            evt_prefix="test.g.method",
            log_kwargs_on_invoke=("sharding_keys",),
        )
        def set_managed_shards(self, sharding_keys: list[str]) -> None:
            self._last = sharding_keys

    mgr = Manager()

    with caplog.at_level(logging.DEBUG, logger=__name__):
        mgr.set_managed_shards(["tenant-g1"])

    assert len(caplog.records) == 2
    invoke_rec, success_rec = caplog.records

    assert invoke_rec.evt == "test.g.method.invoke"
    assert success_rec.evt == "test.g.method.success"

    invoke_data = invoke_rec.data
    assert "kwargs" in invoke_data
    # sharding_keys is passed positionally, but must be visible by name.
    assert invoke_data["kwargs"] == {"sharding_keys": ["tenant-g1"]}


def test_g11_log_kwargs_uses_defaults_from_signature(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Parameters with defaults must appear in bound-argument mapping and
    be loggable even when not explicitly passed by caller.
    """

    @log_invocation(
        evt_prefix="test.g.defaults",
        log_kwargs_on_invoke=("limit",),
    )
    def fn(x: int, limit: int = 10) -> str:
        return f"{x}:{limit}"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        result = fn(5)

    assert result == "5:10"
    assert len(caplog.records) == 2

    invoke_rec, success_rec = caplog.records
    assert invoke_rec.evt == "test.g.defaults.invoke"
    assert success_rec.evt == "test.g.defaults.success"

    invoke_data = invoke_rec.data
    assert "kwargs" in invoke_data
    # 'limit' must be taken from the default value via bound.apply_defaults().
    assert invoke_data["kwargs"] == {"limit": 10}


def test_g12_log_kwargs_supports_len_tail(caplog: pytest.LogCaptureFixture) -> None:
    """
    log_kwargs_on_invoke specs must support '.len()' tail for kwargs.
    """

    @log_invocation(
        evt_prefix="test.g.len",
        log_kwargs_on_invoke=(
            "orders.len()",
            "orders_count=orders.len()",
        ),
    )
    def fn(*, orders: list[int]) -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        result = fn(orders=[1, 2, 3])

    assert result == "ok"
    assert len(caplog.records) == 2
    invoke_rec, success_rec = caplog.records

    assert invoke_rec.evt == "test.g.len.invoke"
    assert success_rec.evt == "test.g.len.success"

    invoke_data = invoke_rec.data
    assert "kwargs" in invoke_data

    kwargs_logged = invoke_data["kwargs"]
    # 'orders.len()' -> alias 'len()'
    assert kwargs_logged["len()"] == 3
    assert kwargs_logged["orders_count"] == 3


# ---------- H: log_result_on_success behavior ----------


def test_h1_result_not_logged_when_param_not_provided(caplog: pytest.LogCaptureFixture) -> None:
    """
    If log_result_on_success is None (default), result must not be logged.
    """

    @log_invocation(evt_prefix="test.i")
    def fn() -> int:
        return 123

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn()

    assert len(caplog.records) == 2
    invoke_rec, success_rec = caplog.records
    assert success_rec.evt == "test.i.success"
    assert "result" not in success_rec.data


def test_h2_result_primitive_logged_as_whole(caplog: pytest.LogCaptureFixture) -> None:
    """
    For primitive result and log_result_on_success provided, whole result is logged.
    """

    @log_invocation(evt_prefix="test.i", log_result_on_success=())
    def fn() -> bool:
        return True

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn()

    assert len(caplog.records) == 2
    success_rec = caplog.records[1]
    assert success_rec.evt == "test.i.success"
    assert success_rec.data["result"] is True


def test_h3_result_list_logged_as_whole_with_truncation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    For list result and log_result_on_success provided, whole result is logged with truncation.
    """

    @log_invocation(evt_prefix="test.i", log_result_on_success=())
    def fn() -> list[int]:
        return list(range(15))

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn()

    assert len(caplog.records) == 2
    success_rec = caplog.records[1]
    assert success_rec.evt == "test.i.success"

    result = success_rec.data["result"]
    assert isinstance(result, list)
    assert result[:10] == list(range(10))
    assert result[-1] == "... (5 more)"


def test_h4_result_composite_logged_as_typename_when_specs_empty(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    For composite result and empty log_result_on_success, log '<TypeName>'.
    """

    class Result:
        def __init__(self, status: str) -> None:
            self.status = status

    @log_invocation(evt_prefix="test.i", log_result_on_success=())
    def fn() -> Result:
        return Result("OK")

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn()

    assert len(caplog.records) == 2
    success_rec = caplog.records[1]
    assert success_rec.evt == "test.i.success"
    assert success_rec.data["result"] == "<Result>"


def test_h5_result_composite_logged_with_selected_fields(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    For composite result and non-empty log_result_on_success, log selected fields only.
    """

    class Result:
        def __init__(self, status: str, task_id: str, ttl_ms: int) -> None:
            self.status = status
            self.task_id = task_id
            self.ttl_ms = ttl_ms

    @log_invocation(
        evt_prefix="test.i",
        log_result_on_success=("status", "task_id"),
    )
    def fn() -> Result:
        return Result("NEW", "task-123", 5000)

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn()

    assert len(caplog.records) == 2
    success_rec = caplog.records[1]
    assert success_rec.evt == "test.i.success"

    result_block = success_rec.data["result"]
    assert isinstance(result_block, dict)
    assert result_block == {"status": "NEW", "task_id": "task-123"}


def test_h6_result_tuple_logged_with_indexed_specs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    For tuple result, indexed log_result_on_success specs must log selected pieces.
    """

    class Payload:
        def __init__(self, ttl_ms: int) -> None:
            self.ttl_ms = ttl_ms

    @log_invocation(
        evt_prefix="test.i",
        log_result_on_success=(
            "status=0",  # first tuple element
            "op_id=1",  # second tuple element
            "ttl_ms=2.ttl_ms",  # attr of third element
        ),
    )
    def fn() -> tuple[str, str, Payload]:
        return ("SUCCESS", "op-777", Payload(45000))

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn()

    assert len(caplog.records) == 2
    success_rec = caplog.records[1]
    assert success_rec.evt == "test.i.success"

    result_block = success_rec.data["result"]
    assert isinstance(result_block, dict)
    assert result_block == {
        "status": "SUCCESS",
        "op_id": "op-777",
        "ttl_ms": 45000,
    }


# ---------- I: log_error_policy and error-logged flag behavior ----------


def test_i1_default_single_failed_with_error(caplog: pytest.LogCaptureFixture) -> None:
    """
    Without log_error_policy, a failing call must emit failed with error payload.
    """

    @log_invocation(evt_prefix="test.j")
    def do_fail() -> None:
        raise ValueError("boom")

    with caplog.at_level(logging.DEBUG, logger=__name__):
        with pytest.raises(ValueError) as excinfo:
            do_fail()

    assert str(excinfo.value) == "boom"
    assert len(caplog.records) == 2

    evts = [rec.evt for rec in caplog.records]
    assert evts == ["test.j.invoke", "test.j.failed"]

    failed_rec = caplog.records[1]
    error_payload = failed_rec.data["error"]
    assert error_payload["kind"] == "ValueError"
    assert error_payload["message"] == "boom"


def test_i2_nested_default_flag_deduplicates_error_payload(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Without policy, nested operations must log error payload only once per exception instance.
    """

    @log_invocation(evt_prefix="test.i.inner")
    def inner() -> None:
        raise ValueError("boom")

    @log_invocation(evt_prefix="test.i.outer")
    def outer() -> None:
        inner()

    with caplog.at_level(logging.DEBUG, logger=__name__):
        with pytest.raises(ValueError):
            outer()

    # We expect 4 records:
    # outer.invoke, inner.invoke, inner.failed, outer.failed
    assert len(caplog.records) == 4

    failed_records = [r for r in caplog.records if r.evt.endswith(".failed")]
    assert {r.evt for r in failed_records} == {"test.i.inner.failed", "test.i.outer.failed"}

    inner_failed = next(r for r in failed_records if r.evt == "test.i.inner.failed")
    outer_failed = next(r for r in failed_records if r.evt == "test.i.outer.failed")

    # inner: first point of failure -> has error payload
    assert "error" in inner_failed.data

    # outer: same exception instance already logged -> no error payload
    assert "error" not in outer_failed.data


def test_i3_default_each_call_logs_error_once_per_instance(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Default policy logs error payload once per exception instance, not per function.
    """

    @log_invocation(evt_prefix="test.i")
    def do_fail() -> None:
        raise ValueError("boom")

    with caplog.at_level(logging.DEBUG, logger=__name__):
        with pytest.raises(ValueError):
            do_fail()
        with pytest.raises(ValueError):
            do_fail()

    # Two calls -> each produces invoke + failed
    assert len(caplog.records) == 4

    failed_records = [r for r in caplog.records if r.evt == "test.i.failed"]
    assert len(failed_records) == 2

    for rec in failed_records:
        # Each call creates its own ValueError instance -> both failed events have error payload
        assert "error" in rec.data
        assert rec.data["error"]["kind"] == "ValueError"
        assert rec.data["error"]["message"] == "boom"


def test_i4_policy_force_log_true_overrides_flag(caplog: pytest.LogCaptureFixture) -> None:
    """
    log_error_policy with force_log=True must log error payload even if the flag is already set.
    """

    @log_invocation(evt_prefix="test.i.inner")
    def inner() -> None:
        raise ValueError("boom")

    @log_invocation(
        evt_prefix="test.i.outer",
        log_error_policy=((ValueError, True),),
    )
    def outer() -> None:
        inner()

    with caplog.at_level(logging.DEBUG, logger=__name__):
        with pytest.raises(ValueError):
            outer()

    failed_records = [r for r in caplog.records if r.evt.endswith(".failed")]
    assert {r.evt for r in failed_records} == {"test.i.inner.failed", "test.i.outer.failed"}

    inner_failed = next(r for r in failed_records if r.evt == "test.i.inner.failed")
    outer_failed = next(r for r in failed_records if r.evt == "test.i.outer.failed")

    # Both failed events must have error payload
    assert "error" in inner_failed.data
    assert "error" in outer_failed.data
    assert inner_failed.data["error"]["kind"] == "ValueError"
    assert outer_failed.data["error"]["kind"] == "ValueError"


def test_i5_policy_force_log_false_blocks_payload_and_sets_flag(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    log_error_policy with force_log=False must block payload and mark the error as logged.
    """

    @log_invocation(
        evt_prefix="test.i.inner",
        log_error_policy=((ValueError, False),),
    )
    def inner() -> None:
        raise ValueError("boom")

    @log_invocation(evt_prefix="test.i.outer")
    def outer() -> None:
        inner()

    with caplog.at_level(logging.DEBUG, logger=__name__):
        with pytest.raises(ValueError):
            outer()

    failed_records = [r for r in caplog.records if r.evt.endswith(".failed")]
    assert {r.evt for r in failed_records} == {"test.i.inner.failed", "test.i.outer.failed"}

    inner_failed = next(r for r in failed_records if r.evt == "test.i.inner.failed")
    outer_failed = next(r for r in failed_records if r.evt == "test.i.outer.failed")

    # Policy says: never log payload on this operation for ValueError
    assert "error" not in inner_failed.data
    # Flag must be set, so outer (default behavior) also logs without payload
    assert "error" not in outer_failed.data


def test_i6_policy_first_matching_rule_wins(caplog: pytest.LogCaptureFixture) -> None:
    """
    The first matching rule in log_error_policy must win for isinstance checks.
    """

    class MyError(ValueError):
        pass

    @log_invocation(
        evt_prefix="test.i",
        log_error_policy=(
            (ValueError, False),
            (Exception, True),
        ),
    )
    def do_fail() -> None:
        raise MyError("boom")

    with caplog.at_level(logging.DEBUG, logger=__name__):
        with pytest.raises(MyError):
            do_fail()

    assert len(caplog.records) == 2
    invoke_rec, failed_rec = caplog.records

    assert invoke_rec.evt == "test.i.invoke"
    assert failed_rec.evt == "test.i.failed"

    # Must trigger the first rule (ValueError, False) -> no error payload
    assert "error" not in failed_rec.data
    # Failed event logged at suppressed level (error_level_suppressed by default)
    assert failed_rec.levelno == logging.DEBUG


def test_i7_nested_runtime_error_uses_default_flag_policy(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    For types without any log_error_policy, default flag policy still applies.
    """

    @log_invocation(evt_prefix="test.i.inner")
    def inner() -> None:
        raise RuntimeError("boom")

    @log_invocation(evt_prefix="test.i.outer")
    def outer() -> None:
        inner()

    with caplog.at_level(logging.DEBUG, logger=__name__):
        with pytest.raises(RuntimeError):
            outer()

    failed_records = [r for r in caplog.records if r.evt.endswith(".failed")]
    assert {r.evt for r in failed_records} == {"test.i.inner.failed", "test.i.outer.failed"}

    inner_failed = next(r for r in failed_records if r.evt == "test.i.inner.failed")
    outer_failed = next(r for r in failed_records if r.evt == "test.i.outer.failed")

    # Default behavior: first failed has error payload, second only base_data
    assert "error" in inner_failed.data
    assert inner_failed.data["error"]["kind"] == "RuntimeError"
    assert "error" not in outer_failed.data


def test_i8_policy_on_base_domain_error_covers_subclasses(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Rule for a base domain error type must apply to its subclasses via isinstance().
    """

    class DomainError(Exception):
        pass

    class ValidationError(DomainError):
        pass

    @log_invocation(
        evt_prefix="test.i",
        log_error_policy=((DomainError, False),),
    )
    def do_fail() -> None:
        raise ValidationError("invalid")

    with caplog.at_level(logging.DEBUG, logger=__name__):
        with pytest.raises(ValidationError):
            do_fail()

    assert len(caplog.records) == 2
    invoke_rec, failed_rec = caplog.records

    assert invoke_rec.evt == "test.i.invoke"
    assert failed_rec.evt == "test.i.failed"

    # Rule on DomainError must catch ValidationError and block payload
    assert "error" not in failed_rec.data
    # Suppressed level must be used
    assert failed_rec.levelno == logging.DEBUG


# ---------- J: closures behavior ----------


def test_j1_no_log_closures_on_invoke_does_not_log_closures_section(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    When log_closures_on_invoke is not provided, invoke payload must not contain 'closures'.
    """

    @log_invocation(evt_prefix="test.j1")
    def fn() -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        result = fn()

    assert result == "ok"
    assert len(caplog.records) == 2

    invoke_rec, success_rec = caplog.records

    assert invoke_rec.evt == "test.j1.invoke"
    assert "closures" not in invoke_rec.data

    assert success_rec.evt == "test.j1.success"
    assert "closures" not in success_rec.data


def test_j2_log_closures_on_invoke_present_only_in_invoke_path(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    log_closures_on_invoke must add 'closures' only to the invoke payload,
    not to success payload.
    """

    @log_invocation(
        evt_prefix="test.j2",
        log_closures_on_invoke={"tenant": "tenant-a1", "user_domain": "domain-x"},
    )
    def fn() -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        result = fn()

    assert result == "ok"
    assert len(caplog.records) == 2

    invoke_rec, success_rec = caplog.records

    assert invoke_rec.evt == "test.j2.invoke"
    closures = invoke_rec.data.get("closures")
    assert isinstance(closures, dict)
    assert closures == {"tenant": "tenant-a1", "user_domain": "domain-x"}

    assert success_rec.evt == "test.j2.success"
    # Success payload must NOT contain 'closures'
    assert "closures" not in success_rec.data


def test_j3_log_closures_on_invoke_present_only_in_invoke_error_path(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    log_closures_on_invoke must add 'closures' only to the invoke payload,
    not to failed payload.
    """

    @log_invocation(
        evt_prefix="test.j3",
        log_closures_on_invoke={"tenant": "tenant-a1"},
    )
    def fn() -> None:
        raise ValueError("boom")

    with caplog.at_level(logging.DEBUG, logger=__name__):
        with pytest.raises(ValueError):
            fn()

    assert len(caplog.records) == 2
    invoke_rec, failed_rec = caplog.records

    assert invoke_rec.evt == "test.j3.invoke"
    assert invoke_rec.data.get("closures") == {"tenant": "tenant-a1"}

    assert failed_rec.evt == "test.j3.failed"
    # Failed payload must NOT contain 'closures'
    assert "closures" not in failed_rec.data


def test_j4_log_closures_on_invoke_uses_unknown_on_normalization_error(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    If normalize_value_for_log raises for a closure value, that entry must be
    set to '<unknown>' instead of failing the decorator.
    """

    # Save original function to restore behavior for other values, if needed
    original_normalize = inv_mod.normalize_value_for_log

    def broken_normalize(value: Any, *, max_items: int | None = None) -> Any:
        raise RuntimeError("normalization failed")

    monkeypatch.setattr(inv_mod, "normalize_value_for_log", broken_normalize)

    @log_invocation(
        evt_prefix="test.j4",
        log_closures_on_invoke={"bad": object()},
    )
    def fn() -> None:
        return None

    with caplog.at_level(logging.DEBUG, logger=__name__):
        fn()

    assert len(caplog.records) == 2
    invoke_rec, success_rec = caplog.records

    assert invoke_rec.evt == "test.j4.invoke"
    closures = invoke_rec.data.get("closures")
    assert isinstance(closures, dict)
    assert closures == {"bad": "<unknown>"}

    # Success payload must not contain closures
    assert success_rec.evt == "test.j4.success"
    assert "closures" not in success_rec.data

    # Restore original normalize_value_for_log just in case (monkeypatch does it on teardown, но явно не помешает)
    monkeypatch.setattr(inv_mod, "normalize_value_for_log", original_normalize)


# ---------- K: async support (async def + sync returning awaitable) ----------


@pytest.mark.asyncio
async def test_k1_async_success_emits_invoke_and_success(caplog: pytest.LogCaptureFixture) -> None:
    """
    Async function must emit invoke + success only after await completes.
    """

    @log_invocation(evt_prefix="test.k.async", log_result_on_success=("value",))
    async def fn(*, value: int) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"value": value}

    with caplog.at_level(logging.DEBUG, logger=__name__):
        result = await fn(value=123)

    assert result == {"value": 123}
    assert len(caplog.records) == 2
    assert [r.evt for r in caplog.records] == ["test.k.async.invoke", "test.k.async.success"]


@pytest.mark.asyncio
async def test_k2_async_failure_emits_invoke_and_failed(caplog: pytest.LogCaptureFixture) -> None:
    """
    Async function raising Exception must emit invoke + failed and re-raise.
    """

    @log_invocation(evt_prefix="test.k.async")
    async def fn() -> None:
        await asyncio.sleep(0)
        raise ValueError("boom")

    with caplog.at_level(logging.DEBUG, logger=__name__):
        with pytest.raises(ValueError) as excinfo:
            await fn()

    assert str(excinfo.value) == "boom"
    assert len(caplog.records) == 2
    assert [r.evt for r in caplog.records] == ["test.k.async.invoke", "test.k.async.failed"]

    error_payload = caplog.records[1].data["error"]
    assert error_payload["kind"] == "ValueError"
    assert error_payload["message"] == "boom"


@pytest.mark.asyncio
async def test_k3_async_cancel_emits_invoke_and_cancelled(caplog: pytest.LogCaptureFixture) -> None:
    """
    Async function cancelled via task.cancel() must emit invoke + cancelled and re-raise CancelledError.
    """

    @log_invocation(evt_prefix="test.k.async")
    async def fn() -> None:
        await asyncio.sleep(3600)

    with caplog.at_level(logging.DEBUG, logger=__name__):
        task = asyncio.create_task(fn())
        await asyncio.sleep(0)  # let it start
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    assert len(caplog.records) == 2
    assert [r.evt for r in caplog.records] == ["test.k.async.invoke", "test.k.async.cancelled"]

    cancelled_rec = caplog.records[1]
    assert cancelled_rec.data.get("cancelled") is True
    assert cancelled_rec.data["error"]["kind"] == "CancelledError"


@pytest.mark.asyncio
async def test_k4_sync_returning_awaitable_logs_success_on_await(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Sync function returning awaitable must log invoke immediately, and success only when awaited.
    """

    async def inner(value: int) -> str:
        await asyncio.sleep(0)
        return f"ok:{value}"

    @log_invocation(evt_prefix="test.k.sync_awaitable")
    def fn(*, value: int) -> Awaitable[str]:
        return inner(value)

    with caplog.at_level(logging.DEBUG, logger=__name__):
        aw = fn(value=7)

        # invoke must already be logged at call-time
        assert len(caplog.records) == 1
        assert caplog.records[0].evt == "test.k.sync_awaitable.invoke"

        result = await aw

    assert result == "ok:7"
    assert len(caplog.records) == 2
    assert caplog.records[1].evt == "test.k.sync_awaitable.success"


@pytest.mark.asyncio
async def test_k5_sync_returning_awaitable_logs_failed_on_await(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Sync function returning awaitable that raises must log failed only when awaited.
    """

    async def inner() -> None:
        await asyncio.sleep(0)
        raise RuntimeError("oops")

    @log_invocation(evt_prefix="test.k.sync_awaitable")
    def fn() -> Awaitable[None]:
        return inner()

    with caplog.at_level(logging.DEBUG, logger=__name__):
        aw = fn()

        # invoke logged immediately
        assert len(caplog.records) == 1
        assert caplog.records[0].evt == "test.k.sync_awaitable.invoke"

        with pytest.raises(RuntimeError) as excinfo:
            await aw

    assert str(excinfo.value) == "oops"
    assert len(caplog.records) == 2
    assert caplog.records[1].evt == "test.k.sync_awaitable.failed"
    assert caplog.records[1].data["error"]["kind"] == "RuntimeError"
    assert caplog.records[1].data["error"]["message"] == "oops"


@pytest.mark.asyncio
async def test_k6_sync_returning_awaitable_logs_cancelled_on_await(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Sync function returning awaitable must log cancelled if awaited task is cancelled.
    """

    async def inner() -> None:
        await asyncio.sleep(3600)

    @log_invocation(evt_prefix="test.k.sync_awaitable")
    def fn() -> Awaitable[None]:
        return inner()

    with caplog.at_level(logging.DEBUG, logger=__name__):
        aw = fn()

        # invoke logged immediately
        assert len(caplog.records) == 1
        assert caplog.records[0].evt == "test.k.sync_awaitable.invoke"

        task = asyncio.create_task(aw)
        await asyncio.sleep(0)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    assert len(caplog.records) == 2
    assert caplog.records[1].evt == "test.k.sync_awaitable.cancelled"
    assert caplog.records[1].data.get("cancelled") is True
    assert caplog.records[1].data["error"]["kind"] == "CancelledError"


@pytest.mark.asyncio
async def test_k7_cancel_level_override_applies(caplog: pytest.LogCaptureFixture) -> None:
    """
    cancel_level must control the log level of the cancelled event.
    """

    @log_invocation(evt_prefix="test.k.levels", cancel_level=logging.WARNING)
    async def fn() -> None:
        await asyncio.sleep(3600)

    with caplog.at_level(logging.DEBUG, logger=__name__):
        task = asyncio.create_task(fn())
        await asyncio.sleep(0)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    assert len(caplog.records) == 2
    invoke_rec, cancelled_rec = caplog.records
    assert invoke_rec.evt == "test.k.levels.invoke"
    assert cancelled_rec.evt == "test.k.levels.cancelled"
    assert cancelled_rec.levelno == logging.WARNING


@pytest.mark.asyncio
async def test_k8_async_success_logs_result_only_in_success(
    caplog: pytest.LogCaptureFixture,
) -> None:
    @log_invocation(evt_prefix="test.k.result", log_result_on_success=())
    async def fn() -> int:
        await asyncio.sleep(0)
        return 123

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = await fn()

    assert len(caplog.records) == 2
    invoke_rec, success_rec = caplog.records
    assert invoke_rec.evt == "test.k.result.invoke"
    assert "result" not in invoke_rec.data
    assert success_rec.evt == "test.k.result.success"
    assert success_rec.data["result"] == 123


def test_k9_sync_returning_awaitable_not_awaited_emits_only_invoke(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def inner() -> str:
        await asyncio.sleep(0)
        return "ok"

    @log_invocation(evt_prefix="test.k.not_awaited")
    def fn() -> Awaitable[str]:
        return inner()

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn()  # not awaited

    assert len(caplog.records) == 1
    assert caplog.records[0].evt == "test.k.not_awaited.invoke"


# ---------- L: context_fields + context_formatter (new behavior) ----------


def test_l1_context_fields_without_formatter_are_flat_merged_into_invoke_and_success(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Without formatter, context_fields must behave like old flat merge:
    fields appear at the top level for both invoke and success.
    """

    @log_invocation(
        evt_prefix="test.l.flat",
        context_fields=("engine_id", "host", "port"),
    )
    def fn(*, engine_id: str, host: str, port: int) -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        result = fn(engine_id="e-1", host="ldap.local", port=389)

    assert result == "ok"
    assert len(caplog.records) == 2

    invoke_rec, success_rec = caplog.records
    assert invoke_rec.evt == "test.l.flat.invoke"
    assert success_rec.evt == "test.l.flat.success"

    assert invoke_rec.data == {"engine_id": "e-1", "host": "ldap.local", "port": 389}
    assert success_rec.data == {"engine_id": "e-1", "host": "ldap.local", "port": 389}


def test_l2_context_formatter_is_called_per_event_and_can_shape_payload(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Formatter must be called per-event and may return different shapes for each event.
    """
    calls: list[tuple[Any, str, dict[str, Any]]] = []

    def formatter(event_type: Any, event_prefix: str, fields: dict[str, Any]) -> dict[str, Any]:
        # Capture calls for assertions.
        calls.append((event_type, event_prefix, dict(fields)))

        # Build a nested "connection" structure.
        conn = {
            "host": fields.get("host"),
            "port": fields.get("port"),
        }
        return {
            "engine_id": fields.get("engine_id"),
            "connection": conn,
            "phase": getattr(event_type, "value", str(event_type)),
        }

    @log_invocation(
        evt_prefix="test.l.shape",
        context_fields=("engine_id", "host", "port"),
        context_formatter=formatter,
    )
    def fn(*, engine_id: str, host: str, port: int) -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn(engine_id="e-2", host="dc.local", port=636)

    assert len(caplog.records) == 2
    invoke_rec, success_rec = caplog.records

    assert invoke_rec.evt == "test.l.shape.invoke"
    assert success_rec.evt == "test.l.shape.success"

    # Formatter-shaped payload must be present in both invoke and success.
    assert invoke_rec.data["engine_id"] == "e-2"
    assert invoke_rec.data["connection"] == {"host": "dc.local", "port": 636}
    assert invoke_rec.data["phase"] == "invoke"

    assert success_rec.data["engine_id"] == "e-2"
    assert success_rec.data["connection"] == {"host": "dc.local", "port": 636}
    assert success_rec.data["phase"] == "success"

    # Ensure formatter was called twice with correct event_prefix and fields.
    assert len(calls) == 2
    assert calls[0][1] == "test.l.shape"
    assert calls[1][1] == "test.l.shape"
    assert calls[0][2] == {"engine_id": "e-2", "host": "dc.local", "port": 636}
    assert calls[1][2] == {"engine_id": "e-2", "host": "dc.local", "port": 636}

    # Ensure event types are invoke/success (by value).
    assert getattr(calls[0][0], "value", None) == "invoke"
    assert getattr(calls[1][0], "value", None) == "success"


def test_l3_formatter_is_called_even_when_context_fields_empty(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Formatter must be called even when fields == {} (no context_fields provided).
    """
    calls: list[tuple[Any, str, dict[str, Any]]] = []

    def formatter(event_type: Any, event_prefix: str, fields: dict[str, Any]) -> dict[str, Any]:
        calls.append((event_type, event_prefix, dict(fields)))
        return {"phase": getattr(event_type, "value", str(event_type)), "x": 1}

    @log_invocation(
        evt_prefix="test.l.empty_fields",
        context_fields=(),
        context_formatter=formatter,
    )
    def fn() -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn()

    assert len(caplog.records) == 2
    invoke_rec, success_rec = caplog.records

    assert invoke_rec.data == {"phase": "invoke", "x": 1}
    assert success_rec.data == {"phase": "success", "x": 1}

    # Formatter called twice with empty fields
    assert len(calls) == 2
    assert calls[0][1] == "test.l.empty_fields"
    assert calls[1][1] == "test.l.empty_fields"
    assert calls[0][2] == {}
    assert calls[1][2] == {}


def test_l4_collision_with_system_keys_wraps_context_under_context_key_on_invoke(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    If formatter output contains any system keys, the entire context fragment must
    be wrapped under top-level 'context' (not merged).
    """

    def formatter(event_type: Any, event_prefix: str, fields: dict[str, Any]) -> dict[str, Any]:
        _ = event_type, event_prefix
        return {"kwargs": "bad", "engine_id": fields.get("engine_id")}

    @log_invocation(
        evt_prefix="test.l.collision_invoke",
        context_fields=("engine_id",),
        context_formatter=formatter,
    )
    def fn(*, engine_id: str) -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn(engine_id="e-3")

    assert len(caplog.records) == 2
    invoke_rec = caplog.records[0]
    assert invoke_rec.evt == "test.l.collision_invoke.invoke"

    # Collision key 'kwargs' forces wrap
    assert "context" in invoke_rec.data
    assert invoke_rec.data["context"] == {"kwargs": "bad", "engine_id": "e-3"}
    # Must NOT create top-level 'kwargs' from context formatter output
    assert "kwargs" not in invoke_rec.data


def test_l5_collision_on_failed_keeps_system_error_and_wraps_formatter_output(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    On FAILED, if formatter output contains 'error' key, it must be wrapped under 'context'
    while the system error payload remains at top-level 'error'.
    """

    def formatter(event_type: Any, event_prefix: str, fields: dict[str, Any]) -> dict[str, Any]:
        _ = event_type, event_prefix
        return {"error": "x", "engine_id": fields.get("engine_id")}

    @log_invocation(
        evt_prefix="test.l.collision_failed",
        context_fields=("engine_id",),
        context_formatter=formatter,
    )
    def fn(*, engine_id: str) -> None:
        raise ValueError("boom")

    with caplog.at_level(logging.DEBUG, logger=__name__):
        with pytest.raises(ValueError):
            fn(engine_id="e-4")

    assert len(caplog.records) == 2
    invoke_rec, failed_rec = caplog.records
    assert failed_rec.evt == "test.l.collision_failed.failed"

    # System error payload present
    assert "error" in failed_rec.data
    assert failed_rec.data["error"]["kind"] == "ValueError"
    assert failed_rec.data["error"]["message"] == "boom"

    # Formatter output wrapped
    assert failed_rec.data["context"] == {"error": "x", "engine_id": "e-4"}


def test_l6_formatter_exception_falls_back_to_flat_fields(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    If formatter raises, logging must fall back to flat `fields` and not affect execution.
    """

    def formatter(event_type: Any, event_prefix: str, fields: dict[str, Any]) -> dict[str, Any]:
        _ = event_type, event_prefix, fields
        raise RuntimeError("formatter failed")

    @log_invocation(
        evt_prefix="test.l.fallback_exc",
        context_fields=("engine_id",),
        context_formatter=formatter,
    )
    def fn(*, engine_id: str) -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        result = fn(engine_id="e-5")

    assert result == "ok"
    assert len(caplog.records) == 2

    invoke_rec, success_rec = caplog.records
    assert invoke_rec.data == {"engine_id": "e-5"}
    assert success_rec.data == {"engine_id": "e-5"}


def test_l7_formatter_non_dict_falls_back_to_flat_fields(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    If formatter returns non-dict, logging must fall back to flat `fields`.
    """

    def formatter(event_type: Any, event_prefix: str, fields: dict[str, Any]) -> Any:
        _ = event_type, event_prefix, fields
        return "not-a-dict"

    @log_invocation(
        evt_prefix="test.l.fallback_type",
        context_fields=("engine_id",),
        context_formatter=formatter,  # type: ignore[arg-type]
    )
    def fn(*, engine_id: str) -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn(engine_id="e-6")

    assert len(caplog.records) == 2
    invoke_rec, success_rec = caplog.records
    assert invoke_rec.data == {"engine_id": "e-6"}
    assert success_rec.data == {"engine_id": "e-6"}


def test_l9_context_fields_select_only_listed_fields(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    context_fields must include only listed fields and ignore other kwargs.
    """

    @log_invocation(
        evt_prefix="test.l.precedence",
        context_fields=("engine_id",),
    )
    def fn(*, engine_id: str, host: str) -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn(engine_id="e-8", host="ignored.local")

    assert len(caplog.records) == 2
    invoke_rec, success_rec = caplog.records

    assert invoke_rec.data == {"engine_id": "e-8"}
    assert success_rec.data == {"engine_id": "e-8"}


@pytest.mark.asyncio
async def test_l10_formatter_is_called_for_cancelled_and_payload_contains_context(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Formatter must be called for CANCELLED event; cancelled payload must include context.
    """
    calls: list[str] = []

    def formatter(event_type: Any, event_prefix: str, fields: dict[str, Any]) -> dict[str, Any]:
        calls.append(getattr(event_type, "value", str(event_type)))
        _ = event_prefix, fields
        return {"phase": getattr(event_type, "value", str(event_type)), "x": 1}

    @log_invocation(
        evt_prefix="test.l.cancel",
        context_fields=(),
        context_formatter=formatter,
        cancel_level=logging.INFO,
    )
    async def fn() -> None:
        await asyncio.sleep(3600)

    with caplog.at_level(logging.DEBUG, logger=__name__):
        task = asyncio.create_task(fn())
        await asyncio.sleep(0)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    assert len(caplog.records) == 2
    assert [r.evt for r in caplog.records] == ["test.l.cancel.invoke", "test.l.cancel.cancelled"]

    cancelled_rec = caplog.records[1]
    # System cancellation keys preserved
    assert cancelled_rec.data.get("cancelled") is True
    assert cancelled_rec.data["error"]["kind"] == "CancelledError"
    # Context from formatter merged (no collision)
    assert cancelled_rec.data["phase"] == "cancelled"
    assert cancelled_rec.data["x"] == 1

    # Formatter called at least for invoke + cancelled (exactly two here).
    assert calls == ["invoke", "cancelled"]


def test_l11_context_fields_len_tail_resolves_via_self(caplog: pytest.LogCaptureFixture) -> None:
    """
    context_fields specs must support '.len()' tail on attributes resolved via 'self'.
    """

    class Manager:
        def __init__(self) -> None:
            self._orders: list[str] = ["a", "b", "c"]

        @log_invocation(
            evt_prefix="test.l.len_self",
            context_fields=("self._orders.len()",),
        )
        def fn(self) -> str:
            # Body doesn't matter for context; we only care that len() is applied
            return "ok"

    mgr = Manager()

    with caplog.at_level(logging.DEBUG, logger=__name__):
        result = mgr.fn()

    assert result == "ok"
    assert len(caplog.records) == 2

    invoke_rec, success_rec = caplog.records
    assert invoke_rec.evt == "test.l.len_self.invoke"
    assert success_rec.evt == "test.l.len_self.success"

    # Alias is derived from last segment: 'len()'
    assert invoke_rec.data == {"len()": 3}
    assert success_rec.data == {"len()": 3}


def test_l12_context_fields_resolved_per_event_on_mutation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    context_fields must be resolved separately for each event:
    INVOKE sees pre-mutation value, SUCCESS sees post-mutation value.
    """

    class Manager:
        def __init__(self) -> None:
            self.state = "before"

        @log_invocation(
            evt_prefix="test.l.mutation",
            context_fields=("self.state",),
        )
        def fn(self) -> str:
            # Mutate state during the call
            self.state = "after"
            return "ok"

    mgr = Manager()

    with caplog.at_level(logging.DEBUG, logger=__name__):
        result = mgr.fn()

    assert result == "ok"
    assert len(caplog.records) == 2

    invoke_rec, success_rec = caplog.records
    assert invoke_rec.evt == "test.l.mutation.invoke"
    assert success_rec.evt == "test.l.mutation.success"

    # INVOKE context built before fn() body runs
    assert invoke_rec.data == {"state": "before"}
    # SUCCESS context built after fn() completed and mutated state
    assert success_rec.data == {"state": "after"}


# ---------- M: activation_profiles (new behavior) ----------


def test_m1_activation_profiles_none_keeps_default_behavior(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    activation_profiles=None must keep legacy behavior: invoke+success are logged.
    """
    monkeypatch.setattr(inv_mod, "get_active_log_profile", lambda: "audit")

    @log_invocation(evt_prefix="test.m.none", activation_profiles=None)
    def fn() -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn()

    assert [r.evt for r in caplog.records] == ["test.m.none.invoke", "test.m.none.success"]


def test_m2_activation_profiles_match_logs_invoke_and_success(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    When active profile is allowed, invoke+success must be logged.
    """
    monkeypatch.setattr(inv_mod, "get_active_log_profile", lambda: "debug")

    @log_invocation(evt_prefix="test.m.match", activation_profiles=("debug", "audit"))
    def fn() -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn()

    assert [r.evt for r in caplog.records] == ["test.m.match.invoke", "test.m.match.success"]


def test_m3_activation_profiles_mismatch_suppresses_invoke_and_success(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    When active profile is not allowed, invoke+success must NOT be logged at all.
    """
    monkeypatch.setattr(inv_mod, "get_active_log_profile", lambda: "audit")

    @log_invocation(evt_prefix="test.m.mismatch", activation_profiles=("debug",))
    def fn() -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn()

    assert caplog.records == []


def test_m4_activation_profiles_mismatch_still_logs_failed(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    FAILED must be logged regardless of activation_profiles (even if invoke was suppressed).
    """
    monkeypatch.setattr(inv_mod, "get_active_log_profile", lambda: "audit")

    @log_invocation(evt_prefix="test.m.fail", activation_profiles=("debug",))
    def fn() -> None:
        raise ValueError("boom")

    with caplog.at_level(logging.DEBUG, logger=__name__):
        with pytest.raises(ValueError):
            fn()

    # Only failed (no invoke)
    assert [r.evt for r in caplog.records] == ["test.m.fail.failed"]
    assert caplog.records[0].data["error"]["kind"] == "ValueError"
    assert caplog.records[0].data["error"]["message"] == "boom"


@pytest.mark.asyncio
async def test_m5_activation_profiles_mismatch_still_logs_cancelled_async_def(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    CANCELLED must be logged regardless of activation_profiles (even if invoke was suppressed).
    """
    monkeypatch.setattr(inv_mod, "get_active_log_profile", lambda: "audit")

    @log_invocation(evt_prefix="test.m.cancel", activation_profiles=("debug",))
    async def fn() -> None:
        await asyncio.sleep(3600)

    with caplog.at_level(logging.DEBUG, logger=__name__):
        task = asyncio.create_task(fn())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    # Only cancelled (no invoke)
    assert [r.evt for r in caplog.records] == ["test.m.cancel.cancelled"]
    cancelled_rec = caplog.records[0]
    assert cancelled_rec.data.get("cancelled") is True
    assert cancelled_rec.data["error"]["kind"] == "CancelledError"


def test_m6_activation_profiles_empty_tuple_never_logs_invoke_or_success(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    activation_profiles=() must suppress invoke+success always.
    """
    monkeypatch.setattr(inv_mod, "get_active_log_profile", lambda: "default")

    @log_invocation(evt_prefix="test.m.empty", activation_profiles=())
    def fn() -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn()

    assert caplog.records == []


@pytest.mark.asyncio
async def test_m7_sync_returning_awaitable_mismatch_not_awaited_emits_nothing(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    For sync returning awaitable, if invoke is suppressed and the awaitable is not awaited,
    there must be no records at all.
    """
    monkeypatch.setattr(inv_mod, "get_active_log_profile", lambda: "audit")

    async def inner() -> str:
        await asyncio.sleep(0)
        return "ok"

    @log_invocation(evt_prefix="test.m.aw.na", activation_profiles=("debug",))
    def fn() -> Awaitable[str]:
        return inner()

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn()  # not awaited

    assert caplog.records == []


@pytest.mark.asyncio
async def test_m8_sync_returning_awaitable_mismatch_success_is_suppressed(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    For sync returning awaitable, if normal logging is suppressed, then:
      - invoke is not logged at call-time
      - success is not logged after await
    """
    monkeypatch.setattr(inv_mod, "get_active_log_profile", lambda: "audit")

    async def inner() -> str:
        await asyncio.sleep(0)
        return "ok"

    @log_invocation(evt_prefix="test.m.aw.ok", activation_profiles=("debug",))
    def fn() -> Awaitable[str]:
        return inner()

    with caplog.at_level(logging.DEBUG, logger=__name__):
        aw = fn()
        assert caplog.records == []  # invoke suppressed
        result = await aw

    assert result == "ok"
    assert caplog.records == []  # success suppressed too


@pytest.mark.asyncio
async def test_m9_sync_returning_awaitable_mismatch_still_logs_failed_on_await(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    For sync returning awaitable, FAILED must be logged on await even if invoke/success are suppressed.
    """
    monkeypatch.setattr(inv_mod, "get_active_log_profile", lambda: "audit")

    async def inner() -> None:
        await asyncio.sleep(0)
        raise RuntimeError("oops")

    @log_invocation(evt_prefix="test.m.aw.fail", activation_profiles=("debug",))
    def fn() -> Awaitable[None]:
        return inner()

    with caplog.at_level(logging.DEBUG, logger=__name__):
        aw = fn()
        assert caplog.records == []  # invoke suppressed
        with pytest.raises(RuntimeError):
            await aw

    assert [r.evt for r in caplog.records] == ["test.m.aw.fail.failed"]
    rec = caplog.records[0]
    assert rec.data["error"]["kind"] == "RuntimeError"
    assert rec.data["error"]["message"] == "oops"


@pytest.mark.asyncio
async def test_m10_sync_returning_awaitable_mismatch_still_logs_cancelled_on_await(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    For sync returning awaitable, CANCELLED must be logged on await cancellation
    even if invoke/success are suppressed.
    """
    monkeypatch.setattr(inv_mod, "get_active_log_profile", lambda: "audit")

    async def inner() -> None:
        await asyncio.sleep(3600)

    @log_invocation(evt_prefix="test.m.aw.cancel", activation_profiles=("debug",))
    def fn() -> Awaitable[None]:
        return inner()

    with caplog.at_level(logging.DEBUG, logger=__name__):
        aw = fn()
        assert caplog.records == []  # invoke suppressed

        task = asyncio.create_task(aw)
        await asyncio.sleep(0)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    assert [r.evt for r in caplog.records] == ["test.m.aw.cancel.cancelled"]
    rec = caplog.records[0]
    assert rec.data.get("cancelled") is True
    assert rec.data["error"]["kind"] == "CancelledError"


def test_m11_activation_profiles_empty_active_profile_suppresses_normal(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    If get_active_log_profile() returns a falsy value (e.g. ''), normal logging must be suppressed
    when activation_profiles is not None.
    """
    monkeypatch.setattr(inv_mod, "get_active_log_profile", lambda: "")

    @log_invocation(evt_prefix="test.m.falsy", activation_profiles=("default",))
    def fn() -> str:
        return "ok"

    with caplog.at_level(logging.DEBUG, logger=__name__):
        _ = fn()

    assert caplog.records == []
