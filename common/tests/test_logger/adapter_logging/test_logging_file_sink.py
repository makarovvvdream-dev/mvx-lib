# tests/test_logger/adapter_logging/test_logging_file_sink.py
from __future__ import annotations

from typing import Any
from collections.abc import Mapping

import logging
import pathlib
import threading
import time

import pytest

from mvx.common.logger.models import LogEvent, LogEventMeta, LogLevel
from mvx.common.logger.asyncio_log_sink import AsyncioLogSinkError, AsyncioLogSinkState


from mvx.common.logger.adapter_logging.logging_configs import LoggingFileConfig

from mvx.common.logger.adapter_logging.logging_file_sink import (
    DEFAULT_FILE_LOGGER_NAME,
    FileLogSink,
)

# ---- Test helpers ------------------------------------------------------------------------


def make_event(
    *,
    level: int = logging.INFO,
    event_namespace: str | None = "mvx.test",
    event_name: str = "event.done",
    event_type: str | None = "operation",
    timestamp: float = 1_700_000_000.123,
    entity_id: str | None = "entity-1",
    payload: Mapping[str, Any] | None = None,
    source_path: str | None = "/tmp/source.py",
    source_line: int | None = 42,
    source_func: str | None = "run",
) -> LogEvent:
    return LogEvent(
        level=level,
        meta=LogEventMeta(
            event_namespace=event_namespace,
            event_name=event_name,
            entity_id=entity_id,
            source_path=source_path,
            source_line=source_line,
            source_func=source_func,
        ),
        event_type=event_type,
        timestamp=timestamp,
        payload=payload if payload is not None else {"result": "ok"},
    )


class RecordingFilter(logging.Filter):
    def __init__(self, *, allow: bool = True) -> None:
        super().__init__()
        self.allow = allow
        self.records: list[logging.LogRecord] = []

    def filter(self, record: logging.LogRecord) -> bool:
        self.records.append(record)
        return self.allow


def make_config(
    file_path: pathlib.Path,
    *,
    level: LogLevel = LogLevel.INFO,
    log_format: str = "%(message)s",
    mode: str = "w",
    encoding: str = "utf-8",
    filters: tuple[logging.Filter, ...] | None = None,
) -> LoggingFileConfig:
    return LoggingFileConfig(
        file_path=file_path,
        level=level,
        log_format=log_format,
        mode=mode,
        encoding=encoding,
        filters=filters,
    )


def create_file_sink(
    file_path: pathlib.Path,
    *,
    logger_name: str = DEFAULT_FILE_LOGGER_NAME,
    config: LoggingFileConfig | None = None,
) -> tuple[FileLogSink, Any]:
    sink, terminator = FileLogSink.create(
        logger_name=logger_name,
        config=config if config is not None else make_config(file_path),
    )

    assert isinstance(sink, FileLogSink)
    return sink, terminator


def wait_for_file_text(
    file_path: pathlib.Path,
    expected: str,
    *,
    timeout: float = 2.0,
) -> str:
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if file_path.exists():
            text = file_path.read_text(encoding="utf-8")
            if expected in text:
                return text

        time.sleep(0.01)

    if file_path.exists():
        return file_path.read_text(encoding="utf-8")

    return ""


def wait_for_line_count(
    file_path: pathlib.Path,
    expected_count: int,
    *,
    timeout: float = 2.0,
) -> list[str]:
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if file_path.exists():
            lines = file_path.read_text(encoding="utf-8").splitlines()
            if len(lines) == expected_count:
                return lines

        time.sleep(0.01)

    if file_path.exists():
        return file_path.read_text(encoding="utf-8").splitlines()

    return []


def terminate_safely(terminator: Any) -> None:
    try:
        terminator()
    except Exception:
        return


# ---- A. build_descriptor -----------------------------------------------------------------


def test_a01_build_descriptor_uses_resolved_file_path(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"
    config = make_config(file_path)

    descriptor = FileLogSink.build_descriptor(config=config)

    assert descriptor.sink_type == "file"
    assert descriptor.resource_key == (
        "file",
        str(file_path.expanduser().resolve()),
    )


def test_a02_build_descriptor_uses_logger_name_in_config_key(tmp_path: pathlib.Path) -> None:
    config = make_config(tmp_path / "app.log")

    descriptor = FileLogSink.build_descriptor(
        logger_name="custom.file.logger",
        config=config,
    )

    assert descriptor.config_key[0:2] == (
        "logger_name",
        "custom.file.logger",
    )


def test_a03_build_descriptor_rejects_missing_config() -> None:
    with pytest.raises(TypeError, match="config must be an instance of LoggingFileConfig"):
        FileLogSink.build_descriptor()


def test_a04_build_descriptor_rejects_invalid_config() -> None:
    with pytest.raises(TypeError, match="config must be an instance of LoggingFileConfig"):
        FileLogSink.build_descriptor(config=object())


def test_a05_build_descriptor_contains_level_format_date_mode_encoding_and_filters(
    tmp_path: pathlib.Path,
) -> None:
    filter_a = RecordingFilter()
    filter_b = RecordingFilter()
    config = LoggingFileConfig(
        file_path=tmp_path / "app.log",
        level=LogLevel.ERROR,
        log_format="%(levelname)s:%(message)s",
        date_format="%H:%M:%S",
        mode="a",
        encoding="utf-8",
        filters=(filter_a, filter_b),
    )

    descriptor = FileLogSink.build_descriptor(config=config)

    assert descriptor.config_key == (
        "logger_name",
        DEFAULT_FILE_LOGGER_NAME,
        "level",
        LogLevel.ERROR.value,
        "log_format",
        "%(levelname)s:%(message)s",
        "date_format",
        "%H:%M:%S",
        "mode",
        "a",
        "encoding",
        "utf-8",
        "filters",
        (
            type(filter_a).__qualname__,
            type(filter_b).__qualname__,
        ),
    )


def test_a06_same_path_as_string_and_path_object_produce_same_resource_key(
    tmp_path: pathlib.Path,
) -> None:
    file_path = tmp_path / "app.log"

    first = FileLogSink.build_descriptor(config=make_config(file_path))
    second = FileLogSink.build_descriptor(config=make_config(pathlib.Path(str(file_path))))

    assert first.resource_key == second.resource_key


# ---- B. create / constructor -------------------------------------------------------------


def test_b01_create_returns_file_log_sink_and_terminator(tmp_path: pathlib.Path) -> None:
    sink, terminator = create_file_sink(tmp_path / "app.log")

    try:
        assert isinstance(sink, FileLogSink)
        assert callable(terminator)
    finally:
        terminate_safely(terminator)


def test_b02_create_passes_logger_name_and_config(tmp_path: pathlib.Path) -> None:
    config = make_config(tmp_path / "app.log")

    sink, terminator = create_file_sink(
        tmp_path / "app.log",
        logger_name="custom.file.logger",
        config=config,
    )

    try:
        assert sink._logger_name == "custom.file.logger"  # noqa: SLF001
        assert sink._config is config  # noqa: SLF001
        assert sink._logger.name == "custom.file.logger"  # noqa: SLF001
    finally:
        terminate_safely(terminator)


def test_b03_constructor_installs_file_handler(tmp_path: pathlib.Path) -> None:
    sink, terminator = create_file_sink(tmp_path / "app.log")

    try:
        assert len(sink._logger.handlers) == 1  # noqa: SLF001
        assert sink._handler in sink._logger.handlers  # noqa: SLF001
        assert isinstance(sink._handler, logging.FileHandler)  # noqa: SLF001
    finally:
        terminate_safely(terminator)


def test_b04_constructor_applies_logger_level(tmp_path: pathlib.Path) -> None:
    config = make_config(tmp_path / "app.log", level=LogLevel.ERROR)

    sink, terminator = create_file_sink(tmp_path / "app.log", config=config)

    try:
        assert sink._logger.level == logging.ERROR  # noqa: SLF001
        assert sink._handler.level == logging.ERROR  # noqa: SLF001
    finally:
        terminate_safely(terminator)


def test_b05_constructor_disables_logger_propagation(tmp_path: pathlib.Path) -> None:
    sink, terminator = create_file_sink(tmp_path / "app.log")

    try:
        assert sink._logger.propagate is False  # noqa: SLF001
    finally:
        terminate_safely(terminator)


# ---- C. dispatch / file writing ----------------------------------------------------------


def test_c01_log_event_writes_message_to_file(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"
    sink, terminator = create_file_sink(file_path)

    try:
        sink.log(make_event())

        text = wait_for_file_text(file_path, "mvx.test.entity-1.event.done [operation]")

        assert "mvx.test.entity-1.event.done [operation]" in text
    finally:
        terminate_safely(terminator)


def test_c02_log_event_uses_logger_name_in_format(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"
    config = make_config(file_path, log_format="%(name)s:%(message)s")

    sink, terminator = create_file_sink(
        file_path,
        logger_name="custom.file.logger",
        config=config,
    )

    try:
        sink.log(make_event())

        text = wait_for_file_text(
            file_path,
            "custom.file.logger:mvx.test.entity-1.event.done [operation]",
        )

        assert "custom.file.logger:mvx.test.entity-1.event.done [operation]" in text
    finally:
        terminate_safely(terminator)


def test_c03_log_event_can_format_source_fields(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"
    config = make_config(
        file_path,
        log_format="%(pathname)s:%(lineno)d:%(funcName)s:%(message)s",
    )

    sink, terminator = create_file_sink(file_path, config=config)

    try:
        sink.log(
            make_event(
                source_path="/tmp/source.py",
                source_line=123,
                source_func="func_name",
            )
        )

        text = wait_for_file_text(
            file_path,
            "/tmp/source.py:123:func_name:mvx.test.entity-1.event.done [operation]",
        )

        assert "/tmp/source.py:123:func_name:" in text
    finally:
        terminate_safely(terminator)


def test_c04_log_event_can_format_payload_and_custom_fields(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"
    config = make_config(
        file_path,
        log_format="%(event_name)s:%(event_type)s:%(entity_id)s:%(payload)s",
    )

    sink, terminator = create_file_sink(file_path, config=config)

    try:
        sink.log(make_event(payload={"x": 1}))

        text = wait_for_file_text(
            file_path,
            "event.done:operation:entity-1:{'x': 1}",
        )

        assert "event.done:operation:entity-1:{'x': 1}" in text
    finally:
        terminate_safely(terminator)


def test_c05_log_event_respects_level_filtering(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"
    config = make_config(
        file_path,
        level=LogLevel.ERROR,
        log_format="%(levelname)s:%(message)s",
    )

    sink, terminator = create_file_sink(file_path, config=config)

    try:
        sink.log(make_event(level=logging.INFO))
        sink.log(make_event(level=logging.ERROR, event_name="error.event"))

        text = wait_for_file_text(
            file_path,
            "ERROR:mvx.test.entity-1.error.event [operation]",
        )

        assert "INFO:" not in text
        assert "ERROR:mvx.test.entity-1.error.event [operation]" in text
    finally:
        terminate_safely(terminator)


def test_c06_blocking_filter_prevents_file_write(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"
    blocking_filter = RecordingFilter(allow=False)
    config = make_config(
        file_path,
        filters=(blocking_filter,),
    )

    sink, terminator = create_file_sink(file_path, config=config)

    try:
        sink.log(make_event())
        time.sleep(0.1)

        text = file_path.read_text(encoding="utf-8") if file_path.exists() else ""

        assert text == ""
        assert len(blocking_filter.records) == 1
    finally:
        terminate_safely(terminator)


def test_c07_allowing_filter_allows_file_write(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"
    allowing_filter = RecordingFilter()
    config = make_config(file_path, filters=(allowing_filter,))

    sink, terminator = create_file_sink(file_path, config=config)

    try:
        sink.log(make_event())

        text = wait_for_file_text(file_path, "mvx.test.entity-1.event.done [operation]")

        assert "mvx.test.entity-1.event.done [operation]" in text
        assert len(allowing_filter.records) == 1
    finally:
        terminate_safely(terminator)


def test_c08_multiple_events_are_written_in_order(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"
    sink, terminator = create_file_sink(file_path)

    try:
        sink.log(make_event(event_name="first"))
        sink.log(make_event(event_name="second"))
        sink.log(make_event(event_name="third"))

        lines = wait_for_line_count(file_path, 3)

        assert lines == [
            "mvx.test.entity-1.first [operation]",
            "mvx.test.entity-1.second [operation]",
            "mvx.test.entity-1.third [operation]",
        ]
    finally:
        terminate_safely(terminator)


def test_c09_log_event_with_missing_optional_fields_is_written(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"
    sink, terminator = create_file_sink(file_path)

    try:
        sink.log(
            make_event(
                event_namespace=None,
                entity_id=None,
                event_type=None,
                source_path=None,
                source_line=None,
                source_func=None,
            )
        )

        text = wait_for_file_text(file_path, "event.done")

        assert "event.done" in text
    finally:
        terminate_safely(terminator)


def test_c10_log_event_with_missing_source_fields_uses_fallbacks(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"
    config = make_config(
        file_path,
        log_format="%(pathname)s:%(lineno)d:%(funcName)s:%(message)s",
    )

    sink, terminator = create_file_sink(file_path, config=config)

    try:
        sink.log(
            make_event(
                source_path=None,
                source_line=None,
                source_func=None,
            )
        )

        text = wait_for_file_text(
            file_path,
            "<not defined>:-1:<not defined>:mvx.test.entity-1.event.done [operation]",
        )

        assert "<not defined>:-1:<not defined>:" in text
    finally:
        terminate_safely(terminator)


# ---- D. lifecycle / cleanup --------------------------------------------------------------


def test_d01_manual_start_stop_closes_handler(tmp_path: pathlib.Path) -> None:
    sink, terminator = create_file_sink(tmp_path / "app.log")

    try:
        assert sink.get_status() is AsyncioLogSinkState.RUNNING
        assert sink.stop().wait().success

        assert sink._handler.stream is None  # noqa: SLF001
        assert sink._logger.handlers == []  # noqa: SLF001
    finally:
        terminate_safely(terminator)


def test_d02_terminator_after_started_sink_closes_handler_and_loop(tmp_path: pathlib.Path) -> None:
    sink, terminator = create_file_sink(tmp_path / "app.log")

    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    terminator()

    assert sink._handler.stream is None  # noqa: SLF001
    assert sink._logger.handlers == []  # noqa: SLF001
    assert sink.created_loop.is_closed() if hasattr(sink, "created_loop") else True


def test_d03_terminator_is_idempotent(tmp_path: pathlib.Path) -> None:
    sink, terminator = create_file_sink(tmp_path / "app.log")

    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    terminator()
    terminator()

    assert sink._handler.stream is None  # noqa: SLF001
    assert sink._logger.handlers == []  # noqa: SLF001


def test_d04_terminator_without_start_closes_handler(tmp_path: pathlib.Path) -> None:
    sink, terminator = create_file_sink(tmp_path / "app.log")

    terminator()

    assert sink._handler.stream is None  # noqa: SLF001
    assert sink._logger.handlers == []  # noqa: SLF001


def test_d05_log_after_manual_stop_is_rejected(tmp_path: pathlib.Path) -> None:
    sink, terminator = create_file_sink(tmp_path / "app.log")

    try:
        assert sink.get_status() is AsyncioLogSinkState.RUNNING
        assert sink.stop().wait().success

        with pytest.raises(AsyncioLogSinkError):
            sink.log(make_event())
    finally:
        terminate_safely(terminator)


# ---- E. descriptor path behavior ---------------------------------------------------------


def test_e01_descriptor_uses_absolute_resolved_path(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "subdir" / ".." / "app.log"
    config = make_config(file_path)

    descriptor = FileLogSink.build_descriptor(config=config)

    assert descriptor.resource_key == (
        "file",
        str(file_path.expanduser().resolve()),
    )


def test_e02_descriptor_differs_for_different_paths(tmp_path: pathlib.Path) -> None:
    first = FileLogSink.build_descriptor(config=make_config(tmp_path / "a.log"))
    second = FileLogSink.build_descriptor(config=make_config(tmp_path / "b.log"))

    assert first.resource_key != second.resource_key


def test_e03_descriptor_differs_for_different_modes(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"

    first = FileLogSink.build_descriptor(config=make_config(file_path, mode="a"))
    second = FileLogSink.build_descriptor(config=make_config(file_path, mode="w"))

    assert first.resource_key == second.resource_key
    assert first.config_key != second.config_key


def test_e04_descriptor_differs_for_different_encoding(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"

    first = FileLogSink.build_descriptor(config=make_config(file_path, encoding="utf-8"))
    second = FileLogSink.build_descriptor(config=make_config(file_path, encoding="utf-16"))

    assert first.resource_key == second.resource_key
    assert first.config_key != second.config_key


# ---- F. concurrency ----------------------------------------------------------------------


def test_f01_concurrent_log_calls_are_all_written(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"
    sink, terminator = create_file_sink(file_path)

    workers_count = 8
    events_per_worker = 20
    start_barrier = threading.Barrier(workers_count)

    errors: list[BaseException] = []
    errors_lock = threading.Lock()

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

        lines = wait_for_line_count(
            file_path,
            workers_count * events_per_worker,
            timeout=3.0,
        )

        assert len(lines) == workers_count * events_per_worker
    finally:
        terminate_safely(terminator)


def test_f02_concurrent_log_and_terminator_do_not_hang(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"
    sink, terminator = create_file_sink(file_path)

    workers_count = 8
    start_barrier = threading.Barrier(workers_count + 1)

    errors: list[BaseException] = []
    errors_lock = threading.Lock()

    def log_target(worker_index: int) -> None:
        try:
            start_barrier.wait(timeout=2.0)

            for event_index in range(50):
                try:
                    sink.log(
                        make_event(
                            event_name=f"event-{worker_index}-{event_index}",
                            entity_id=f"entity-{worker_index}",
                        )
                    )
                except AsyncioLogSinkError:
                    return

        except BaseException as exc:
            with errors_lock:
                errors.append(exc)

    def terminate_target() -> None:
        try:
            start_barrier.wait(timeout=2.0)
            terminator()

        except BaseException as exc:
            with errors_lock:
                errors.append(exc)

    threads = [
        threading.Thread(target=log_target, args=(worker_index,))
        for worker_index in range(workers_count)
    ]
    threads.append(threading.Thread(target=terminate_target))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join(timeout=3.0)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
