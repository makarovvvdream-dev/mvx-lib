# tests/test_logger/test_package_bootstrap.py
from __future__ import annotations

import mvx.common.logger as logger_pack

from mvx.common.logger import LogLevel


def test_a01_bootstrap_exposes_root_log_context() -> None:
    logger_pack.reset_logger()

    root = logger_pack.get_root_log_context()

    assert root.is_root
    assert root.namespace == logger_pack.ROOT_LOG_CONTEXT_NAMESPACE


def test_a02_bootstrap_registers_default_stderr_sink() -> None:
    logger_pack.reset_logger()

    sink = logger_pack.get_log_sink(logger_pack.DEFAULT_ROOT_LOG_SINK_NAME)

    assert sink is not None
    assert logger_pack.get_configured_log_sink_names() == (logger_pack.DEFAULT_ROOT_LOG_SINK_NAME,)
    assert logger_pack.has_configured_log_sinks()


def test_a03_root_context_is_bound_to_default_stderr_sink() -> None:
    logger_pack.reset_logger()

    root = logger_pack.get_root_log_context()
    sink = logger_pack.get_log_sink(logger_pack.DEFAULT_ROOT_LOG_SINK_NAME)

    assert sink is not None
    assert root.get_local_log_sink() is sink
    assert root.log_sink is sink


def test_a04_root_context_can_log_event_after_bootstrap() -> None:
    logger_pack.reset_logger()

    root = logger_pack.get_root_log_context()

    root.log_event(
        event="bootstrap.smoke",
        level=LogLevel.INFO,
        payload={"ok": True},
    )


def test_a05_reset_logger_restores_default_environment() -> None:
    logger_pack.reset_logger()

    sink = logger_pack.configure_log_sink(
        name="test",
        sink_cls=logger_pack.StreamLogSink,
    )

    logger_pack.configure_log_context(
        "mvx.test",
        log_sink=sink,
    )

    assert logger_pack.get_log_sink("test") is not None
    assert logger_pack.has_log_context("mvx.test")

    logger_pack.reset_logger()

    assert logger_pack.get_log_sink("test") is None
    assert not logger_pack.has_log_context("mvx.test")
    assert logger_pack.get_configured_log_sink_names() == (logger_pack.DEFAULT_ROOT_LOG_SINK_NAME,)

    root = logger_pack.get_root_log_context()
    default_sink = logger_pack.get_log_sink(logger_pack.DEFAULT_ROOT_LOG_SINK_NAME)

    assert default_sink is not None
    assert root.get_local_log_sink() is default_sink
