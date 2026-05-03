from __future__ import annotations

import logging
from typing import Any, Mapping

import pytest

from mvx.logger.log_events_helpers import (
    log_event,
    log_debug_event,
    log_info_event,
    log_warning_event,
    log_error_event,
)

from mvx.logger.logger_service import TraceIdFilter

from mvx.logger.trace_context import (
    set_trace_id,
    reset_trace_id,
    NO_TRACE,
)


# --------- helpers for tests ---------


def _get_logger(name: str) -> logging.Logger:
    """
    Return a logger with no handlers (handlers are irrelevant for caplog).
    """
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.propagate = True
    logger.setLevel(logging.DEBUG)
    return logger


# --------- tests: log_event and wrappers ---------


def test_log_event_minimal_payload(caplog: pytest.LogCaptureFixture) -> None:
    logger = _get_logger("test.event_helpers.minimal")
    evt = "test.evt.minimal"

    with caplog.at_level(logging.INFO, logger=logger.name):
        log_event(logger, evt, data=None, level=logging.INFO)

    assert len(caplog.records) == 1
    record = caplog.records[0]

    # msg is always the evt string
    assert record.getMessage() == evt
    assert record.levelno == logging.INFO

    # extras are present as documented
    assert getattr(record, "evt") == evt
    data = getattr(record, "data")
    assert isinstance(data, dict)
    assert data == {}  # empty payload converted to dict


def test_log_event_with_payload(caplog: pytest.LogCaptureFixture) -> None:
    logger = _get_logger("test.event_helpers.payload")
    evt = "test.evt.payload"
    payload: Mapping[str, Any] = {"foo": "bar", "answer": 42}

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        log_event(logger, evt, data=payload, level=logging.DEBUG)

    assert len(caplog.records) == 1
    record = caplog.records[0]

    assert record.getMessage() == evt
    assert record.levelno == logging.DEBUG
    assert getattr(record, "evt") == evt

    data = getattr(record, "data")
    assert isinstance(data, dict)
    # must be a copy, not the same object
    assert data is not payload
    assert data == payload


@pytest.mark.parametrize(
    "wrapper, level",
    [
        (log_debug_event, logging.DEBUG),
        (log_info_event, logging.INFO),
        (log_warning_event, logging.WARNING),
        (log_error_event, logging.ERROR),
    ],
)
def test_log_level_wrappers(
    caplog: pytest.LogCaptureFixture,
    wrapper,
    level: int,
) -> None:
    logger = _get_logger(f"test.event_helpers.wrapper.{level}")
    evt = f"test.evt.wrapper.{level}"
    payload = {"k": "v"}

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        wrapper(logger, evt, data=payload)

    assert len(caplog.records) == 1
    record = caplog.records[0]

    assert record.getMessage() == evt
    assert record.levelno == level
    assert getattr(record, "evt") == evt

    data = getattr(record, "data")
    assert isinstance(data, dict)
    assert data == payload


# --------- tests: integration with trace_context / TraceIdFilter ---------


def test_trace_id_injected_from_context_for_log_event(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    log_event() + logger with TraceIdFilter must yield trace_id taken from trace_context.
    """
    logger = _get_logger("test.event_helpers.trace_ctx.event")
    logger.addFilter(TraceIdFilter())

    evt = "test.evt.trace_ctx"
    token = set_trace_id("ctx-123")
    try:
        with caplog.at_level(logging.INFO, logger=logger.name):
            log_info_event(logger, evt, data={"k": "v"})

        assert len(caplog.records) == 1
        record = caplog.records[0]

        assert record.getMessage() == evt
        assert getattr(record, "evt") == evt
        assert getattr(record, "trace_id") == "ctx-123"
    finally:
        reset_trace_id(token)


def test_trace_id_defaults_to_no_trace_when_context_not_set(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    When trace_context is not set, TraceIdFilter must set trace_id to NO_TRACE.
    """
    logger = _get_logger("test.event_helpers.trace_ctx.default")
    logger.addFilter(TraceIdFilter())

    evt = "test.evt.no_ctx"

    # Intentionally do NOT call set_trace_id() here.
    with caplog.at_level(logging.INFO, logger=logger.name):
        log_info_event(logger, evt, data=None)

    assert len(caplog.records) == 1
    record = caplog.records[0]

    assert record.getMessage() == evt
    assert getattr(record, "evt") == evt
    assert getattr(record, "trace_id") == NO_TRACE
