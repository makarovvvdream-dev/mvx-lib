from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import cast

import pytest
from uvicorn.logging import DefaultFormatter

from mvx.logger.config import LoggerConfig
from mvx.logger.logger_service import (
    LoggerService,
    TraceIdFilter,
)
from mvx.logger.trace_context import NO_TRACE
from mvx.logger.adapter_registry import get_active_log_profile, set_active_log_profile
from mvx.logger.mvx_logger import MvxLogger

# ---------- Helpers ----------


@pytest.fixture(autouse=True)
def clean_root_logger() -> None:
    """
    Ensure root logger state is isolated between tests.

    We snapshot handlers/level/filters and restore them after each test.
    """
    root = logging.getLogger()
    old_level = root.level
    old_handlers = list(root.handlers)
    old_filters = list(root.filters)

    try:
        # Start each test with a clean root.
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


@pytest.fixture(autouse=True)
def reset_active_profile() -> None:
    """
    Ensure active log profile is reset to 'default' between tests.

    This isolates tests that rely on adapter profile wiring.
    """
    set_active_log_profile("default")
    try:
        yield
    finally:
        set_active_log_profile("default")


def make_record(
    msg: str, level: int = logging.INFO, name: str = "test.logger"
) -> logging.LogRecord:
    """
    Helper: create a basic LogRecord instance.
    """
    return logging.LogRecord(
        name=name,
        level=level,
        pathname=__file__,
        lineno=42,
        msg=msg,
        args=(),
        exc_info=None,
    )


# ---------- Group a: TraceIdFilter ----------


def test_a1_filter_injects_trace_id_evt_and_data_for_plain_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    When get_trace_id() returns a value, TraceIdFilter must inject:
      - trace_id from get_trace_id()
      - evt equal to formatted message
      - data as empty dict
    """
    # Patch get_trace_id inside logger_service module.
    from mvx.logger import logger_service as ls_mod

    def fake_get_trace_id() -> str:
        return "trace-123"

    monkeypatch.setattr(ls_mod, "get_trace_id", fake_get_trace_id)

    record = make_record("hello world")
    flt = TraceIdFilter()

    assert flt.filter(record) is True

    assert getattr(record, "trace_id") == "trace-123"
    assert getattr(record, "evt") == "hello world"
    assert isinstance(getattr(record, "data"), dict)
    assert record.data == {}


def test_a2_filter_does_not_override_existing_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    If record already has trace_id, evt and data, TraceIdFilter must keep them as-is.
    """
    from mvx.logger import logger_service as ls_mod

    def fake_get_trace_id() -> str:
        return "trace-should-not-be-used"

    monkeypatch.setattr(ls_mod, "get_trace_id", fake_get_trace_id)

    record = make_record("original message")
    record.trace_id = "existing-trace"
    record.evt = "existing.evt"
    record.data = {"foo": "bar"}

    flt = TraceIdFilter()
    assert flt.filter(record) is True

    assert record.trace_id == "existing-trace"
    assert record.evt == "existing.evt"
    assert record.data == {"foo": "bar"}


def test_a3_filter_fallbacks_to_no_trace_on_get_trace_id_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    If get_trace_id() raises, TraceIdFilter must fall back to NO_TRACE
    and still provide evt and data.
    """
    from mvx.logger import logger_service as ls_mod

    def broken_get_trace_id() -> str:
        raise RuntimeError("boom")

    monkeypatch.setattr(ls_mod, "get_trace_id", broken_get_trace_id)

    record = make_record("fallback test")
    flt = TraceIdFilter()
    assert flt.filter(record) is True

    assert getattr(record, "trace_id") == NO_TRACE
    assert getattr(record, "evt") == "fallback test"
    assert isinstance(getattr(record, "data"), dict)
    assert record.data == {}


# ---------- Group b: basic LoggerService init/apply ----------


def test_b1_init_with_apply_cfg_applies_config_once() -> None:
    """
    LoggerService(..., apply_cfg=True) must configure root logger according
    to LoggerConfig.
    """
    cfg = LoggerConfig(
        level="INFO",
        sink="stderr",
        formatter="UVICORN",
        format="%(levelprefix)s %(name)s: %(message)s",
    )

    service = LoggerService(cfg, apply_cfg=True)
    # Just to silence "unused variable" warnings and ensure object exists.
    assert isinstance(service, LoggerService)

    root = logging.getLogger()
    # Root level should match cfg.level.
    assert root.level == logging.INFO

    assert len(root.handlers) == 1
    handler = root.handlers[0]

    from logging import StreamHandler

    # Handler type and stream.
    assert isinstance(handler, StreamHandler)
    assert handler.stream is sys.stderr

    # Formatter type.
    assert isinstance(handler.formatter, DefaultFormatter)
    assert handler.formatter._fmt == cfg.format  # type: ignore[attr-defined]

    # TraceIdFilter must be attached.
    assert any(isinstance(f, TraceIdFilter) for f in handler.filters)


def test_b2_init_without_apply_cfg_does_not_touch_root() -> None:
    """
    LoggerService(..., apply_cfg=False) must not change root logger.
    """
    root = logging.getLogger()
    root.setLevel(logging.WARNING)
    root.handlers.clear()

    cfg = LoggerConfig(
        level="INFO",
        sink="stderr",
        formatter="UVICORN",
    )

    LoggerService(cfg, apply_cfg=False)

    # Root logger must remain untouched.
    assert root.level == logging.WARNING
    assert root.handlers == []


def test_b3_apply_uses_current_config() -> None:
    """
    apply() must apply current LoggerConfig when called explicitly.
    """
    root = logging.getLogger()
    root.setLevel(logging.ERROR)
    root.handlers.clear()

    cfg = LoggerConfig(
        level="DEBUG",
        sink="stderr",
        formatter="UVICORN",
    )

    service = LoggerService(cfg, apply_cfg=False)
    service.apply(reconfigure=True)

    assert root.level == logging.DEBUG
    assert len(root.handlers) == 1
    handler = root.handlers[0]

    from logging import StreamHandler

    assert isinstance(handler, StreamHandler)
    assert handler.stream is sys.stderr
    assert any(isinstance(f, TraceIdFilter) for f in handler.filters)


# ---------- Group c: reconfigure / _cfg_applied_once semantics ----------


def test_c1_first_apply_always_sets_handler() -> None:
    """
    First apply() must attach a handler regardless of reconfigure flag.
    """
    root = logging.getLogger()
    root.setLevel(logging.ERROR)
    root.handlers.clear()

    cfg = LoggerConfig(level="INFO", sink="stderr", formatter="UVICORN")
    service = LoggerService(cfg, apply_cfg=False)

    service.apply(reconfigure=False)

    assert len(root.handlers) == 1
    assert any(isinstance(f, TraceIdFilter) for f in root.handlers[0].filters)


def test_c2_reapply_with_reconfigure_false_keeps_existing_handlers() -> None:
    """
    If configuration was already applied once and reconfigure=False,
    apply() must not clear/replace handlers.
    """
    root = logging.getLogger()
    root.handlers.clear()

    cfg = LoggerConfig(level="INFO", sink="stderr", formatter="UVICORN")
    service = LoggerService(cfg, apply_cfg=True)

    # At this point we have 1 handler attached by __init__.
    initial_handlers = list(root.handlers)
    assert len(initial_handlers) == 1

    # Second apply with reconfigure=False should keep existing handlers.
    service.apply(reconfigure=False)

    assert root.handlers == initial_handlers


def test_c3_reapply_with_reconfigure_true_replaces_handler() -> None:
    """
    If reconfigure=True, apply() must clear existing handlers and attach a fresh one.
    """
    root = logging.getLogger()
    root.handlers.clear()

    cfg = LoggerConfig(level="INFO", sink="stderr", formatter="UVICORN")
    service = LoggerService(cfg, apply_cfg=True)

    assert len(root.handlers) == 1
    first_handler = root.handlers[0]

    # Manually add another handler to root to simulate extra configuration.
    extra_handler = logging.StreamHandler()
    root.addHandler(extra_handler)
    assert len(root.handlers) == 2

    # Re-apply with reconfigure=True should wipe both and add a single fresh handler.
    service.apply(reconfigure=True)

    assert len(root.handlers) == 1
    new_handler = root.handlers[0]
    assert new_handler is not first_handler
    assert new_handler is not extra_handler
    assert any(isinstance(f, TraceIdFilter) for f in new_handler.filters)


# ---------- Group d: update_config ----------


def test_d1_update_config_replaces_internal_cfg_and_applies() -> None:
    """
    update_config(cfg, apply_cfg=True) must update in-memory cfg and reconfigure root.
    """
    root = logging.getLogger()
    root.handlers.clear()

    cfg1 = LoggerConfig(level="INFO", sink="stderr", formatter="UVICORN")
    cfg2 = LoggerConfig(
        level="DEBUG", sink="stdout", formatter="CUSTOM", format="%(levelname)s %(message)s"
    )

    service = LoggerService(cfg1, apply_cfg=True)
    assert service.get_config().level == "INFO"

    service.update_config(cfg2, apply_cfg=True)

    # Config updated.
    current_cfg = service.get_config()
    assert current_cfg.level == "DEBUG"
    assert current_cfg.sink == "stdout"
    assert current_cfg.formatter == "CUSTOM"
    assert current_cfg.format == "%(levelname)s %(message)s"

    # Root reconfigured according to cfg2.
    root = logging.getLogger()
    assert root.level == logging.DEBUG
    assert len(root.handlers) == 1
    handler = root.handlers[0]

    from logging import StreamHandler

    assert isinstance(handler, StreamHandler)
    assert handler.stream is sys.stdout
    assert isinstance(handler.formatter, logging.Formatter)
    assert handler.formatter._fmt == "%(levelname)s %(message)s"  # type: ignore[attr-defined]
    assert any(isinstance(f, TraceIdFilter) for f in handler.filters)


def test_d2_update_config_without_apply_does_not_touch_root() -> None:
    """
    update_config(cfg, apply_cfg=False) must not change root logger configuration.
    """
    root = logging.getLogger()
    root.handlers.clear()

    cfg1 = LoggerConfig(level="INFO", sink="stderr", formatter="UVICORN")
    cfg2 = LoggerConfig(
        level="DEBUG", sink="stdout", formatter="CUSTOM", format="%(levelname)s %(message)s"
    )

    service = LoggerService(cfg1, apply_cfg=True)
    before_handlers = list(root.handlers)
    before_level = root.level

    service.update_config(cfg2, apply_cfg=False)

    # In-memory config updated.
    assert service.get_config().level == "DEBUG"

    # Root logger unchanged.
    assert root.level == before_level
    assert root.handlers == before_handlers


# ---------- Group e: sink behavior ----------


def test_e1_sink_stdout_creates_stream_handler_stdout() -> None:
    """
    When sink='stdout', LoggerService must configure StreamHandler(sys.stdout).
    """
    cfg = LoggerConfig(level="INFO", sink="stdout", formatter="UVICORN")
    LoggerService(cfg, apply_cfg=True)

    root = logging.getLogger()
    assert len(root.handlers) == 1
    handler = root.handlers[0]

    from logging import StreamHandler

    assert isinstance(handler, StreamHandler)
    assert handler.stream is sys.stdout


def test_e2_sink_stderr_creates_stream_handler_stderr() -> None:
    """
    When sink='stderr', LoggerService must configure StreamHandler(sys.stderr).
    """
    cfg = LoggerConfig(level="INFO", sink="stderr", formatter="UVICORN")
    LoggerService(cfg, apply_cfg=True)

    root = logging.getLogger()
    assert len(root.handlers) == 1
    handler = root.handlers[0]

    from logging import StreamHandler

    assert isinstance(handler, StreamHandler)
    assert handler.stream is sys.stderr


def test_e3_sink_file_creates_file_handler_and_creates_directory(tmp_path: Path) -> None:
    """
    When sink='file', LoggerService must create FileHandler and ensure directory exists.
    """
    log_dir = tmp_path / "logs"
    log_file = log_dir / "app.log"

    cfg = LoggerConfig(
        level="INFO",
        sink="file",
        file_path=str(log_file),
        formatter="CUSTOM",
        format="%(levelname)s %(message)s",
    )

    LoggerService(cfg, apply_cfg=True)

    # Directory must be created.
    assert log_dir.is_dir()

    root = logging.getLogger()
    assert len(root.handlers) == 1
    handler = root.handlers[0]

    from logging import FileHandler

    assert isinstance(handler, FileHandler)
    # baseFilename is an absolute path.
    assert Path(cast(FileHandler, handler).baseFilename) == log_file


# ---------- Group f: get_config ----------


def test_f1_get_config_returns_deep_copy() -> None:
    """
    get_config() must return a deep copy, modifications to it must not affect internal config.
    """
    cfg = LoggerConfig(level="INFO", sink="stderr", formatter="UVICORN")
    service = LoggerService(cfg, apply_cfg=False)

    copy_cfg = service.get_config()
    assert copy_cfg == cfg
    assert copy_cfg is not cfg

    # Modify the copy.
    copy_cfg.level = "DEBUG"

    # Internal config must stay unchanged.
    internal_cfg = service.get_config()
    assert internal_cfg.level == "INFO"


# ---------- Group g: log profile wiring ----------


def test_g1_apply_sets_active_log_profile_from_config() -> None:
    """
    LoggerService.apply (via __init__ with apply_cfg=True) must set
    the active log profile according to LoggerConfig.profile.
    """
    cfg = LoggerConfig(
        level="INFO",
        sink="stderr",
        formatter="UVICORN",
        profile="debug",
    )

    # Before creating LoggerService, active profile should be 'default'
    assert get_active_log_profile() == "default"

    LoggerService(cfg, apply_cfg=True)

    # After apply, active profile must be 'debug'
    assert get_active_log_profile() == "debug"


def test_g2_update_config_with_apply_true_updates_active_profile() -> None:
    """
    update_config(cfg, apply_cfg=True) must also update the active log profile.
    """
    cfg1 = LoggerConfig(
        level="INFO",
        sink="stderr",
        formatter="UVICORN",
        profile="default",
    )
    service = LoggerService(cfg1, apply_cfg=True)
    assert get_active_log_profile() == "default"

    cfg2 = LoggerConfig(
        level="DEBUG",
        sink="stdout",
        formatter="CUSTOM",
        format="%(levelname)s %(message)s",
        profile="audit",
    )
    service.update_config(cfg2, apply_cfg=True)

    # Active profile must be updated to 'audit'
    assert get_active_log_profile() == "audit"


def test_g3_update_config_without_apply_does_not_change_active_profile() -> None:
    """
    update_config(cfg, apply_cfg=False) must not change the active log profile.
    """
    cfg1 = LoggerConfig(
        level="INFO",
        sink="stderr",
        formatter="UVICORN",
        profile="debug",
    )
    service = LoggerService(cfg1, apply_cfg=True)
    assert get_active_log_profile() == "debug"

    cfg2 = LoggerConfig(
        level="ERROR",
        sink="stderr",
        formatter="UVICORN",
        profile="audit",
    )
    service.update_config(cfg2, apply_cfg=False)

    # Active profile must remain 'debug' because apply_cfg=False
    assert get_active_log_profile() == "debug"


def test_z1_logger_service_sets_logger_class_to_mvxlogger() -> None:
    """
    LoggerService.apply must set MvxLogger as the logger class for new loggers.
    """
    cfg = LoggerConfig(
        level="INFO",
        sink="stderr",
        formatter="UVICORN",
        profile="default",
    )
    LoggerService(cfg, apply_cfg=True)

    logger = logging.getLogger("mvx.test.mvxlogger.from_service")
    assert isinstance(logger, MvxLogger)
