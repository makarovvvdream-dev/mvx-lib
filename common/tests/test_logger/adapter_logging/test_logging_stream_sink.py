# tests/test_logger/adapter_logging/test_logging_stream_sink.py

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
import logging
import threading
import sys

import pytest

from mvx.common.logger.adapter_logging.logging_configs import (
    LogStreamOutput,
    LoggingStreamConfig,
)
from mvx.common.logger.adapter_logging.logging_stream_sink import (
    DEFAULT_STREAM_LOGGER_NAME,
    StreamLogSink,
)
from mvx.common.logger.models import (
    LogEvent,
    LogLevel,
)

# ---- Test helpers ------------------------------------------------------------------------


def make_event(
    *,
    level: int = logging.INFO,
    event_namespace: str = "mvx.test",
    event_name: str = "event.done",
    event_type: str = "operation",
    timestamp: float = 1_700_000_000.123,
    entity_id: str = "entity-1",
    payload: Mapping[str, Any] | None = None,
    source_path: str = "/tmp/source.py",
    source_line: int = 42,
    source_func: str = "run",
) -> LogEvent:
    return LogEvent(
        level=level,
        event_namespace=event_namespace,
        event_name=event_name,
        event_type=event_type,
        timestamp=timestamp,
        entity_id=entity_id,
        payload=payload if payload is not None else {"result": "ok"},
        source_path=source_path,
        source_line=source_line,
        source_func=source_func,
    )


class RecordingFilter(logging.Filter):
    def __init__(self, *, allow: bool = True) -> None:
        super().__init__()
        self.allow = allow
        self.records: list[logging.LogRecord] = []

    def filter(self, record: logging.LogRecord) -> bool:
        self.records.append(record)
        return self.allow


class CustomFormatter(logging.Formatter):
    pass


def custom_formatter_factory(log_format: str, date_format: str) -> logging.Formatter:
    return CustomFormatter(fmt=log_format, datefmt=date_format)


def get_only_handler(sink: StreamLogSink) -> logging.Handler:
    handlers = sink._logger.handlers  # noqa: SLF001

    assert len(handlers) == 1
    return handlers[0]


# ---- A. Constructor ----------------------------------------------------------------------


def test_a01_constructor_uses_default_logger_name() -> None:
    sink = StreamLogSink()
    try:
        assert sink._logger_name == DEFAULT_STREAM_LOGGER_NAME  # noqa: SLF001
        assert sink._logger.name == DEFAULT_STREAM_LOGGER_NAME  # noqa: SLF001
    finally:
        sink.close()


def test_a02_constructor_accepts_custom_logger_name() -> None:
    sink = StreamLogSink(logger_name="custom.logger")
    try:
        assert sink._logger_name == "custom.logger"  # noqa: SLF001
        assert sink._logger.name == "custom.logger"  # noqa: SLF001
    finally:
        sink.close()


def test_a03_constructor_uses_default_config_when_config_is_none() -> None:
    sink = StreamLogSink()
    try:
        assert isinstance(sink._config, LoggingStreamConfig)  # noqa: SLF001
        assert sink._config.stream_output is LogStreamOutput.STDERR  # noqa: SLF001
    finally:
        sink.close()


def test_a04_constructor_uses_provided_config() -> None:
    config = LoggingStreamConfig(stream_output=LogStreamOutput.STDOUT)

    sink = StreamLogSink(config=config)
    try:
        assert sink._config is config  # noqa: SLF001
    finally:
        sink.close()


def test_a05_constructor_installs_one_handler() -> None:
    sink = StreamLogSink()
    try:
        handler = get_only_handler(sink)

        assert handler in sink._logger.handlers  # noqa: SLF001
    finally:
        sink.close()


def test_a06_constructor_disables_logger_propagation() -> None:
    sink = StreamLogSink()
    try:
        assert sink._logger.propagate is False  # noqa: SLF001
    finally:
        sink.close()


def test_a07_constructor_sets_closed_flag_to_false() -> None:
    sink = StreamLogSink()
    try:
        assert sink._closed is False  # noqa: SLF001
    finally:
        sink.close()


# ---- B. build_descriptor -----------------------------------------------------------------


def test_b01_build_descriptor_with_defaults() -> None:
    descriptor = StreamLogSink.build_descriptor()

    assert descriptor.sink_type == "stream"
    assert descriptor.resource_key == (
        "stream",
        DEFAULT_STREAM_LOGGER_NAME,
        LogStreamOutput.STDERR.value,
    )
    assert descriptor.config_key == (
        "level",
        LogLevel.INFO.value,
        "log_format",
        LoggingStreamConfig().log_format,
        "date_format",
        LoggingStreamConfig().date_format,
        "filters",
        (),
    )


def test_b02_build_descriptor_uses_logger_name() -> None:
    descriptor = StreamLogSink.build_descriptor(logger_name="custom.logger")

    assert descriptor.resource_key == (
        "stream",
        "custom.logger",
        LogStreamOutput.STDERR.value,
    )


def test_b03_build_descriptor_uses_stdout_config() -> None:
    config = LoggingStreamConfig(stream_output=LogStreamOutput.STDOUT)

    descriptor = StreamLogSink.build_descriptor(config=config)

    assert descriptor.resource_key == (
        "stream",
        DEFAULT_STREAM_LOGGER_NAME,
        LogStreamOutput.STDOUT.value,
    )


def test_b04_build_descriptor_uses_level_format_date_and_filters() -> None:
    filter_a = RecordingFilter()
    filter_b = RecordingFilter()

    config = LoggingStreamConfig(
        level=LogLevel.ERROR,
        log_format="%(message)s",
        date_format="%H:%M:%S",
        filters=(filter_a, filter_b),
    )

    descriptor = StreamLogSink.build_descriptor(
        logger_name="custom.logger",
        config=config,
    )

    assert descriptor.config_key == (
        "level",
        LogLevel.ERROR.value,
        "log_format",
        "%(message)s",
        "date_format",
        "%H:%M:%S",
        "filters",
        (
            type(filter_a).__qualname__,
            type(filter_b).__qualname__,
        ),
    )


# ---- C. create / terminator --------------------------------------------------------------


def test_c01_create_returns_sink_and_terminator() -> None:
    sink, terminator = StreamLogSink.create()

    try:
        assert isinstance(sink, StreamLogSink)
        assert callable(terminator)
    finally:
        terminator()


def test_c02_create_passes_kwargs_to_constructor() -> None:
    config = LoggingStreamConfig(stream_output=LogStreamOutput.STDOUT)

    sink, terminator = StreamLogSink.create(
        logger_name="created.logger",
        config=config,
    )

    try:
        assert isinstance(sink, StreamLogSink)
        assert sink._logger_name == "created.logger"  # noqa: SLF001
        assert sink._config is config  # noqa: SLF001
    finally:
        terminator()


def test_c03_terminator_closes_sink() -> None:
    sink, terminator = StreamLogSink.create()

    assert isinstance(sink, StreamLogSink)

    terminator()

    assert sink._closed is True  # noqa: SLF001
    assert sink._logger.handlers == []  # noqa: SLF001


def test_c04_terminator_is_idempotent() -> None:
    sink, terminator = StreamLogSink.create()

    assert isinstance(sink, StreamLogSink)

    terminator()
    terminator()

    assert sink._closed is True  # noqa: SLF001
    assert sink._logger.handlers == []  # noqa: SLF001


def test_c05_concurrent_terminator_calls_are_safe() -> None:
    sink, terminator = StreamLogSink.create()

    errors: list[BaseException] = []
    start_barrier = threading.Barrier(8)

    def target() -> None:
        try:
            start_barrier.wait(timeout=2.0)
            terminator()
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=target) for _ in range(8)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join(timeout=2.0)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []

    assert isinstance(sink, StreamLogSink)
    assert sink._closed is True  # noqa: SLF001
    assert sink._logger.handlers == []  # noqa: SLF001


# ---- D. log() output ---------------------------------------------------------------------


def test_d01_log_writes_to_stdout(capsys: pytest.CaptureFixture[str]) -> None:
    config = LoggingStreamConfig(
        stream_output=LogStreamOutput.STDOUT,
        log_format="%(message)s",
    )
    sink = StreamLogSink(config=config)

    try:
        sink.log(make_event())
        captured = capsys.readouterr()

        assert captured.out.strip() == "mvx.test.entity-1.event.done [operation]"
        assert captured.err == ""
    finally:
        sink.close()


def test_d02_log_writes_to_stderr(capsys: pytest.CaptureFixture[str]) -> None:
    config = LoggingStreamConfig(log_format="%(message)s")
    sink = StreamLogSink(config=config)

    try:
        sink.log(make_event())
        captured = capsys.readouterr()

        assert captured.out == ""
        assert captured.err.strip() == "mvx.test.entity-1.event.done [operation]"
    finally:
        sink.close()


def test_d03_log_uses_logger_name_in_record(capsys: pytest.CaptureFixture[str]) -> None:
    config = LoggingStreamConfig(
        stream_output=LogStreamOutput.STDOUT,
        log_format="%(name)s:%(message)s",
    )
    sink = StreamLogSink(
        logger_name="custom.logger",
        config=config,
    )

    try:
        sink.log(make_event())
        captured = capsys.readouterr()

        assert captured.out.strip() == ("custom.logger:mvx.test.entity-1.event.done [operation]")
    finally:
        sink.close()


def test_d04_log_uses_custom_formatter_fields(capsys: pytest.CaptureFixture[str]) -> None:
    config = LoggingStreamConfig(
        stream_output=LogStreamOutput.STDOUT,
        log_format="%(event_name)s:%(event_type)s:%(entity_id)s:%(payload)s",
    )
    sink = StreamLogSink(config=config)

    try:
        sink.log(make_event(payload={"x": 1}))
        captured = capsys.readouterr()

        assert captured.out.strip() == "event.done:operation:entity-1:{'x': 1}"
    finally:
        sink.close()


def test_d05_log_respects_level(capsys: pytest.CaptureFixture[str]) -> None:
    config = LoggingStreamConfig(
        stream_output=LogStreamOutput.STDOUT,
        level=LogLevel.ERROR,
        log_format="%(levelname)s:%(message)s",
    )
    sink = StreamLogSink(config=config)

    try:
        sink.log(make_event(level=logging.INFO))
        sink.log(make_event(level=logging.ERROR, event_name="error.event"))

        captured = capsys.readouterr()

        assert captured.out.strip() == "ERROR:mvx.test.entity-1.error.event [operation]"
    finally:
        sink.close()


def test_d06_log_respects_blocking_filter(capsys: pytest.CaptureFixture[str]) -> None:
    blocking_filter = RecordingFilter(allow=False)
    config = LoggingStreamConfig(
        stream_output=LogStreamOutput.STDOUT,
        log_format="%(message)s",
        filters=(blocking_filter,),
    )
    sink = StreamLogSink(config=config)
    event = make_event()

    try:
        sink.log(event)
        captured = capsys.readouterr()

        assert captured.out == ""
        assert captured.err == ""
        assert len(blocking_filter.records) == 1
    finally:
        sink.close()


def test_d07_log_respects_allowing_filter(capsys: pytest.CaptureFixture[str]) -> None:
    allowing_filter = RecordingFilter()
    config = LoggingStreamConfig(
        stream_output=LogStreamOutput.STDOUT,
        log_format="%(message)s",
        filters=(allowing_filter,),
    )
    sink = StreamLogSink(config=config)
    event = make_event()

    try:
        sink.log(event)
        captured = capsys.readouterr()

        assert captured.out.strip() == "mvx.test.entity-1.event.done [operation]"
        assert len(allowing_filter.records) == 1
    finally:
        sink.close()


def test_d08_log_after_close_is_ignored(capsys: pytest.CaptureFixture[str]) -> None:
    config = LoggingStreamConfig(
        stream_output=LogStreamOutput.STDOUT,
        log_format="%(message)s",
    )
    sink = StreamLogSink(config=config)

    sink.close()
    sink.log(make_event())

    captured = capsys.readouterr()

    assert captured.out == ""
    assert captured.err == ""


# ---- E. close() --------------------------------------------------------------------------


def test_e01_close_sets_closed_flag() -> None:
    sink = StreamLogSink()

    sink.close()

    assert sink._closed is True  # noqa: SLF001


def test_e02_close_removes_handler_from_logger() -> None:
    sink = StreamLogSink()

    assert sink._logger.handlers != []  # noqa: SLF001

    sink.close()

    assert sink._logger.handlers == []  # noqa: SLF001


def test_e03_close_is_idempotent() -> None:
    sink = StreamLogSink()

    sink.close()
    sink.close()

    assert sink._closed is True  # noqa: SLF001
    assert sink._logger.handlers == []  # noqa: SLF001


def test_e04_close_does_not_close_stdout() -> None:
    config = LoggingStreamConfig(stream_output=LogStreamOutput.STDOUT)
    sink = StreamLogSink(config=config)

    sink.close()

    assert not sys.stdout.closed


def test_e05_close_does_not_close_stderr() -> None:
    sink = StreamLogSink()

    sink.close()

    assert not sys.stderr.closed


# ---- F. Thread-safety --------------------------------------------------------------------


def test_f01_concurrent_log_calls_are_serialized(capsys: pytest.CaptureFixture[str]) -> None:
    config = LoggingStreamConfig(
        stream_output=LogStreamOutput.STDOUT,
        log_format="%(message)s",
    )
    sink = StreamLogSink(config=config)

    errors: list[BaseException] = []
    errors_lock = threading.Lock()

    workers_count = 8
    events_per_worker = 25
    start_barrier = threading.Barrier(workers_count)

    def target(worker_index: int) -> None:
        try:
            start_barrier.wait(timeout=2.0)

            for event_index in range(events_per_worker):
                sink.log(
                    make_event(
                        event_name=f"event-{worker_index}-{event_index}",
                        entity_id=f"entity-{worker_index}",
                    )
                )

        except BaseException as exc:
            with errors_lock:
                errors.append(exc)

    threads = [
        threading.Thread(target=target, args=(worker_index,))
        for worker_index in range(workers_count)
    ]

    try:
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join(timeout=3.0)

        assert all(not thread.is_alive() for thread in threads)
        assert errors == []

        captured = capsys.readouterr()
        lines = [line for line in captured.out.splitlines() if line]

        assert len(lines) == workers_count * events_per_worker

    finally:
        sink.close()


def test_f02_concurrent_log_and_close_do_not_raise() -> None:
    config = LoggingStreamConfig(
        stream_output=LogStreamOutput.STDOUT,
        log_format="%(message)s",
    )
    sink = StreamLogSink(config=config)

    errors: list[BaseException] = []
    errors_lock = threading.Lock()

    workers_count = 8
    start_barrier = threading.Barrier(workers_count + 1)

    def logger_target(worker_index: int) -> None:
        try:
            start_barrier.wait(timeout=2.0)

            for event_index in range(50):
                sink.log(
                    make_event(
                        event_name=f"event-{worker_index}-{event_index}",
                        entity_id=f"entity-{worker_index}",
                    )
                )

        except BaseException as exc:
            with errors_lock:
                errors.append(exc)

    def close_target() -> None:
        try:
            start_barrier.wait(timeout=2.0)
            sink.close()

        except BaseException as exc:
            with errors_lock:
                errors.append(exc)

    threads = [
        threading.Thread(target=logger_target, args=(worker_index,))
        for worker_index in range(workers_count)
    ]
    threads.append(threading.Thread(target=close_target))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join(timeout=3.0)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert sink._closed is True  # noqa: SLF001
