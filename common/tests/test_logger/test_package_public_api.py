# tests/test_logger/test_package_public_api.py
from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import pytest

import mvx.common.logger as logger_pack

from mvx.common.logger import (
    LogEvent,
    LogSinkDescriptor,
    LogSinkConfigurationConflictError,
    LogSinkIsInUseError,
    LogVerbosityLevel,
)


class RecordingSink:
    def __init__(self, marker: str = "default") -> None:
        self.marker = marker
        self.events: list[LogEvent] = []

    def log(self, event: LogEvent) -> None:
        self.events.append(event)


class RecordingSinkClass:
    created_sinks: list[RecordingSink] = []
    terminated_markers: list[str] = []

    @classmethod
    def reset(cls) -> None:
        cls.created_sinks.clear()
        cls.terminated_markers.clear()

    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor:
        marker = kwargs.get("marker", "default")

        return LogSinkDescriptor(
            sink_type="recording",
            resource_key=(marker,),
            config_key=(),
        )

    @classmethod
    def create(cls, **kwargs: Any) -> tuple[RecordingSink, Callable[[], None]]:
        marker = kwargs.get("marker", "default")
        sink = RecordingSink(marker=marker)
        cls.created_sinks.append(sink)

        def terminator() -> None:
            cls.terminated_markers.append(marker)

        return sink, terminator


@pytest.fixture(autouse=True)
def reset_logger_environment() -> Iterator[None]:
    RecordingSinkClass.reset()
    logger_pack.reset_logger()

    yield

    logger_pack.reset_logger()
    RecordingSinkClass.reset()


# ==== A. configure_log_sink / get_log_sink =================================================


def test_a01_configure_log_sink_registers_sink() -> None:
    sink = logger_pack.configure_log_sink(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="a",
    )

    assert isinstance(sink, RecordingSink)
    assert sink.marker == "a"
    assert logger_pack.get_log_sink("test") is sink
    assert logger_pack.get_configured_log_sink_names() == (
        logger_pack.DEFAULT_ROOT_LOG_SINK_NAME,
        "test",
    )


def test_a02_configure_log_sink_is_idempotent_for_same_descriptor() -> None:
    first = logger_pack.configure_log_sink(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="a",
    )
    second = logger_pack.configure_log_sink(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="a",
    )

    assert second is first
    assert len(RecordingSinkClass.created_sinks) == 1


def test_a03_configure_log_sink_rejects_conflicting_descriptor() -> None:
    logger_pack.configure_log_sink(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="a",
    )

    with pytest.raises(LogSinkConfigurationConflictError):
        logger_pack.configure_log_sink(
            name="test",
            sink_cls=RecordingSinkClass,
            marker="b",
        )


def test_a04_get_log_sink_returns_none_for_unknown_name() -> None:
    assert logger_pack.get_log_sink("missing") is None


def test_a05_has_configured_log_sinks_is_true_after_bootstrap() -> None:
    assert logger_pack.has_configured_log_sinks()


# ==== B. configure_log_context / get_log_context ==========================================


def test_b01_configure_log_context_creates_context_chain() -> None:
    leaf = logger_pack.configure_log_context("mvx.test.child")

    assert logger_pack.get_log_context("mvx") is not None
    assert logger_pack.get_log_context("mvx.test") is not None
    assert logger_pack.get_log_context("mvx.test.child") is leaf

    assert logger_pack.get_log_context_namespaces() == (
        logger_pack.ROOT_LOG_CONTEXT_NAMESPACE,
        "mvx",
        "mvx.test",
        "mvx.test.child",
    )


def test_b02_configure_log_context_returns_existing_context() -> None:
    first = logger_pack.configure_log_context("mvx.test")
    second = logger_pack.configure_log_context("mvx.test")

    assert second is first


def test_b03_configure_log_context_applies_sink_to_existing_context() -> None:
    ctx = logger_pack.configure_log_context("mvx.test")

    sink = logger_pack.configure_log_sink(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="a",
    )

    same_ctx = logger_pack.configure_log_context(
        "mvx.test",
        log_sink=sink,
    )

    assert same_ctx is ctx
    assert ctx.get_local_log_sink() is sink
    assert ctx.log_sink is sink


def test_b04_configure_log_context_applies_runtime_settings_to_existing_context() -> None:
    ctx = logger_pack.configure_log_context("mvx.test")

    same_ctx = logger_pack.configure_log_context(
        "mvx.test",
        verbosity_level=LogVerbosityLevel.MAXIMUM,
        max_str_len=10,
        max_items=3,
    )

    assert same_ctx is ctx
    assert ctx.verbosity_level == LogVerbosityLevel.MAXIMUM.value
    assert ctx.max_str_len == 10
    assert ctx.max_items == 3


def test_b05_child_context_inherits_parent_sink() -> None:
    sink = logger_pack.configure_log_sink(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="a",
    )

    parent = logger_pack.configure_log_context(
        "mvx.test",
        log_sink=sink,
    )
    child = logger_pack.configure_log_context("mvx.test.child")

    assert parent.get_local_log_sink() is sink
    assert child.get_local_log_sink() is None
    assert child.log_sink is sink


def test_b06_has_log_context_reports_existing_and_missing_contexts() -> None:
    logger_pack.configure_log_context("mvx.test")

    assert logger_pack.has_log_context("mvx.test")
    assert not logger_pack.has_log_context("mvx.missing")


# ==== C. reset_log_contexts ================================================================


def test_c01_reset_log_contexts_removes_non_root_contexts() -> None:
    logger_pack.configure_log_context("mvx.test.child")

    assert logger_pack.has_log_context("mvx")
    assert logger_pack.has_log_context("mvx.test")
    assert logger_pack.has_log_context("mvx.test.child")

    logger_pack.reset_log_contexts()

    assert not logger_pack.has_log_context("mvx")
    assert not logger_pack.has_log_context("mvx.test")
    assert not logger_pack.has_log_context("mvx.test.child")

    assert logger_pack.get_log_context_namespaces() == (logger_pack.ROOT_LOG_CONTEXT_NAMESPACE,)


def test_c02_reset_log_contexts_keeps_root_context_and_sink() -> None:
    root_before = logger_pack.get_root_log_context()
    sink_before = root_before.log_sink

    logger_pack.configure_log_context("mvx.test")
    logger_pack.reset_log_contexts()

    root_after = logger_pack.get_root_log_context()

    assert root_after is root_before
    assert root_after.log_sink is sink_before


# ==== D. reset_logger ======================================================================


def test_d01_reset_logger_removes_custom_sinks_and_contexts() -> None:
    custom_sink = logger_pack.configure_log_sink(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="a",
    )

    logger_pack.configure_log_context(
        "mvx.test",
        log_sink=custom_sink,
    )

    assert logger_pack.get_log_sink("test") is custom_sink
    assert logger_pack.has_log_context("mvx.test")

    logger_pack.reset_logger()

    assert logger_pack.get_log_sink("test") is None
    assert not logger_pack.has_log_context("mvx.test")
    assert logger_pack.get_configured_log_sink_names() == (logger_pack.DEFAULT_ROOT_LOG_SINK_NAME,)


def test_d02_reset_logger_replaces_root_context() -> None:
    root_before = logger_pack.get_root_log_context()

    logger_pack.reset_logger()

    root_after = logger_pack.get_root_log_context()

    assert root_after is not root_before
    assert root_after.is_root
    assert root_after.namespace == logger_pack.ROOT_LOG_CONTEXT_NAMESPACE


def test_d03_reset_logger_calls_custom_sink_terminator() -> None:
    logger_pack.configure_log_sink(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="a",
    )

    logger_pack.reset_logger()

    assert RecordingSinkClass.terminated_markers == ["a"]


# ==== E. close_log_sink ====================================================================


def test_e01_close_log_sink_returns_false_for_unknown_sink() -> None:
    assert logger_pack.close_log_sink("missing") is False


def test_e02_close_log_sink_closes_unused_sink() -> None:
    sink = logger_pack.configure_log_sink(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="a",
    )

    assert logger_pack.get_log_sink("test") is sink

    assert logger_pack.close_log_sink("test") is True

    assert logger_pack.get_log_sink("test") is None
    assert RecordingSinkClass.terminated_markers == ["a"]


def test_e03_close_log_sink_rejects_root_sink() -> None:
    with pytest.raises(LogSinkIsInUseError) as exc_info:
        logger_pack.close_log_sink(logger_pack.DEFAULT_ROOT_LOG_SINK_NAME)

    payload = exc_info.value.to_log_payload()

    assert payload["details"]["sink_name"] == logger_pack.DEFAULT_ROOT_LOG_SINK_NAME
    assert payload["details"]["context_namespaces"] == ("<root>",)


def test_e04_close_log_sink_rejects_locally_used_sink() -> None:
    sink = logger_pack.configure_log_sink(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="a",
    )

    logger_pack.configure_log_context(
        "mvx.test",
        log_sink=sink,
    )

    with pytest.raises(LogSinkIsInUseError) as exc_info:
        logger_pack.close_log_sink("test")

    payload = exc_info.value.to_log_payload()

    assert payload["details"]["sink_name"] == "test"
    assert payload["details"]["context_namespaces"] == ("mvx.test",)


def test_e05_close_log_sink_allows_close_after_context_resets_local_sink() -> None:
    sink = logger_pack.configure_log_sink(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="a",
    )

    ctx = logger_pack.configure_log_context(
        "mvx.test",
        log_sink=sink,
    )

    ctx.reset_log_sink()

    assert logger_pack.close_log_sink("test") is True
    assert logger_pack.get_log_sink("test") is None
    assert RecordingSinkClass.terminated_markers == ["a"]


def test_e06_close_log_sink_ignores_inherited_usage() -> None:
    sink = logger_pack.configure_log_sink(
        name="test",
        sink_cls=RecordingSinkClass,
        marker="a",
    )

    parent = logger_pack.configure_log_context(
        "mvx.test",
        log_sink=sink,
    )
    child = logger_pack.configure_log_context("mvx.test.child")

    assert child.log_sink is sink
    assert child.get_local_log_sink() is None

    parent.reset_log_sink()

    assert logger_pack.close_log_sink("test") is True
    assert logger_pack.get_log_sink("test") is None
    assert RecordingSinkClass.terminated_markers == ["a"]
