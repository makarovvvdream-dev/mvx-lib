from __future__ import annotations

import logging
from typing import Any, Dict, List

import pytest

from mvx.logger.config import LoggerConfig
from mvx.logger.logger_service import LoggerService, TraceIdFilter
from mvx.logger.mvx_logger import MvxLogger
from mvx.logger.adapter_registry import (
    register_log_adapter,
    set_active_log_profile,
)
from mvx.logger import adapter_registry as ar_mod
from mvx.logger.payload_helpers import (
    normalize_value_for_log,
    normalize_list_for_log,
    normalize_dict_for_log,
    normalize_primitive,
)
from mvx.logger.log_errors_helpers import ERR_LOGGED_FLAG
from mvx.logger.trace_context import NO_TRACE


@pytest.fixture(autouse=True)
def _reset_logging_and_registry() -> None:
    """
    Reset logging state and adapter registry around each test in this module.

    This fixture:
      - snapshots/restores logger class;
      - snapshots/restores root handlers/filters/level;
      - clears adapter registry and resets active profile.
    """
    old_logger_class = logging.getLoggerClass()
    root = logging.getLogger()
    old_level = root.level
    old_handlers = list(root.handlers)
    old_filters = list(root.filters)

    ar_mod._ADAPTERS.clear()  # type: ignore[attr-defined]
    set_active_log_profile("default")

    try:
        root.handlers.clear()
        root.filters.clear()
        root.setLevel(logging.NOTSET)
        yield
    finally:
        root.handlers.clear()
        root.filters.clear()
        for h in old_handlers:
            root.addHandler(h)
        for f in old_filters:
            root.addFilter(f)
        root.setLevel(old_level)
        logging.setLoggerClass(old_logger_class)

        ar_mod._ADAPTERS.clear()  # type: ignore[attr-defined]
        set_active_log_profile("default")


def _init_logger_service(profile: str = "default") -> None:
    """
    Helper: initialize LoggerService with a given profile.
    """
    cfg = LoggerConfig(
        level="DEBUG",
        sink="stderr",
        formatter="UVICORN",
        profile=profile,
    )
    LoggerService(cfg, apply_cfg=True)


def test_a1_mvxlogger_instances_created_by_logging_getlogger() -> None:
    """
    After LoggerService.apply, logging.getLogger(...) must return MvxLogger instances.
    """
    _init_logger_service(profile="default")

    logger = logging.getLogger("mvx.test.mvxlogger.basic")
    assert isinstance(logger, MvxLogger)


def test_a2_normalize_value_matches_payload_helpers_logic() -> None:
    """
    MvxLogger.normalize_value must behave like normalize_value_for_log.
    """

    class Custom:
        def __init__(self, x: int) -> None:
            self.x = x

    _init_logger_service(profile="default")
    logger = logging.getLogger("mvx.test.mvxlogger.norm")
    assert isinstance(logger, MvxLogger)

    obj = Custom(5)

    direct = normalize_value_for_log(obj)
    via_logger = logger.normalize_value(obj)

    assert via_logger == direct


def test_a3_normalize_helpers_delegate_to_payload_helpers() -> None:
    """
    Primitive/list/dict normalization helpers on MvxLogger must delegate to
    the underlying payload_helpers functions.
    """
    _init_logger_service(profile="default")
    logger = logging.getLogger("mvx.test.mvxlogger.norm_helpers")
    assert isinstance(logger, MvxLogger)

    # Primitive
    assert logger.normalize_primitive("hello") == normalize_primitive("hello")

    # List
    value_list = [1, "x", {"a": 1}]
    assert logger.normalize_list(value_list) == normalize_list_for_log(value_list)

    # Dict
    value_dict = {"a": 1, "b": [1, 2]}
    assert logger.normalize_dict(value_dict) == normalize_dict_for_log(value_dict)


def test_a4_get_adapter_exposes_registry_resolution() -> None:
    """
    MvxLogger.get_adapter must resolve adapters from the registry with
    inheritance and profile support.
    """

    class Base:
        pass

    class Sub(Base):
        pass

    def base_adapter(obj: Any) -> Dict[str, Any]:
        _ = obj
        return {"kind": "base"}

    register_log_adapter(Base, base_adapter, profile="default")

    _init_logger_service(profile="default")
    logger = logging.getLogger("mvx.test.mvxlogger.adapter")
    assert isinstance(logger, MvxLogger)

    b = Base()
    s = Sub()

    adapter_b = logger.get_adapter(b)
    adapter_s = logger.get_adapter(s)

    assert adapter_b is base_adapter
    assert adapter_s is base_adapter
    assert adapter_b(b) == {"kind": "base"}
    assert adapter_s(s) == {"kind": "base"}


def test_a5_log_profile_reflects_active_profile() -> None:
    """
    MvxLogger.log_profile must reflect the active adapter profile.
    """
    _init_logger_service(profile="default")
    logger = logging.getLogger("mvx.test.mvxlogger.profile")
    assert isinstance(logger, MvxLogger)

    assert logger.log_profile == "default"

    set_active_log_profile("full")
    assert logger.log_profile == "full"


class _ListHandler(logging.Handler):
    """
    Simple in-memory handler to capture LogRecords for testing.
    """

    def __init__(self) -> None:
        super().__init__()
        self.records: List[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def test_a6_trace_helpers_set_and_reset_trace_id() -> None:
    """
    Trace helpers on MvxLogger must set trace_id, and TraceIdFilter must
    reflect it in LogRecord.
    """
    _init_logger_service(profile="default")
    logger = logging.getLogger("mvx.test.mvxlogger.trace")
    assert isinstance(logger, MvxLogger)

    # Before setting, trace_id should be NO_TRACE.
    assert logger.trace_id == NO_TRACE

    token = logger.set_trace_id("op-123")
    try:
        assert logger.trace_id == "op-123"

        handler = _ListHandler()
        handler.addFilter(TraceIdFilter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        assert logger.trace_id == "op-123"
        logger.log_info_event("test.trace", data={})

        assert handler.records
        rec = handler.records[0]
        assert getattr(rec, "trace_id") == "op-123"
        assert getattr(rec, "evt") == "test.trace"
        assert isinstance(getattr(rec, "data"), dict)
    finally:
        logger.reset_trace_id(token)


def test_a7_event_helpers_emit_structured_events() -> None:
    """
    MvxLogger.log_*_event helpers must emit structured events with evt and data set.
    """
    _init_logger_service(profile="default")
    logger = logging.getLogger("mvx.test.mvxlogger.events")
    assert isinstance(logger, MvxLogger)

    handler = _ListHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    logger.log_info_event("test.event", data={"x": 1, "y": "ok"})

    assert len(handler.records) == 1
    rec = handler.records[0]

    assert rec.evt == "test.event"
    assert rec.levelno == logging.INFO
    assert rec.data == {"x": 1, "y": "ok"}


def test_a8_build_error_payload_delegates_to_event_helpers() -> None:
    """
    MvxLogger.build_error_payload must delegate to helpers.build_error_payload.
    """

    class CustomError(Exception):
        def __init__(self, code: int, msg: str) -> None:
            super().__init__(msg)
            self.code = code

    _init_logger_service(profile="default")
    logger = logging.getLogger("mvx.test.mvxlogger.error_payload")
    assert isinstance(logger, MvxLogger)

    err = CustomError(42, "boom")
    payload = logger.build_error_payload(err)

    assert payload["kind"] == "CustomError"
    assert payload["message"] == "boom"
    assert payload["code"] == 42


def test_a9_error_flag_helpers_work_via_mvxlogger() -> None:
    """
    MvxLogger.is_error_logged and mark_error_logged must delegate to error_flag_helpers.
    """
    _init_logger_service(profile="default")
    logger = logging.getLogger("mvx.test.mvxlogger.error_flags")
    assert isinstance(logger, MvxLogger)

    err = RuntimeError("boom")

    # Initially the error must not be marked as logged.
    assert logger.is_error_logged(err) is False
    assert not hasattr(err, ERR_LOGGED_FLAG)

    # After mark_error_logged, the flag must be set and is_error_logged must be True.
    logger.mark_error_logged(err)

    assert logger.is_error_logged(err) is True
    assert getattr(err, ERR_LOGGED_FLAG) is True
