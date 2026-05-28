# tests/test_logger/adapter_logging/test_logging_configs.py
from __future__ import annotations

import logging
import os
import pathlib
import sys

import pytest

from mvx.common.logger.models import LogLevel

from mvx.common.logger.adapter_logging.logging_configs import (
    DEFAULT_DATE_FORMAT,
    DEFAULT_LOG_FORMAT,
    LogStreamOutput,
    LoggingFileConfig,
    LoggingStreamConfig,
)

# ---- Test helpers ------------------------------------------------------------------------


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


def make_record(
    *,
    name: str = "test.logger",
    level: int = logging.INFO,
    msg: str = "message",
) -> logging.LogRecord:
    record = logging.LogRecord(
        name=name,
        level=level,
        pathname=__file__,
        lineno=1,
        msg=msg,
        args=(),
        exc_info=None,
    )

    record.event_name = "event"
    record.event_type = "type"
    record.entity_id = "entity"
    record.payload = {"x": 1}

    return record


# ---- A. LoggingStreamConfig constructor --------------------------------------------------


def test_a01_stream_config_uses_default_values() -> None:
    config = LoggingStreamConfig()

    assert config.level is LogLevel.INFO
    assert config.log_format == DEFAULT_LOG_FORMAT
    assert config.date_format == DEFAULT_DATE_FORMAT
    assert config.filters == ()
    assert config.stream_output is LogStreamOutput.STDERR


def test_a02_stream_config_accepts_stdout_output() -> None:
    config = LoggingStreamConfig(LogStreamOutput.STDOUT)

    assert config.stream_output is LogStreamOutput.STDOUT


def test_a03_stream_config_accepts_stderr_output() -> None:
    config = LoggingStreamConfig()

    assert config.stream_output is LogStreamOutput.STDERR


def test_a04_stream_config_accepts_custom_level() -> None:
    config = LoggingStreamConfig(level=LogLevel.DEBUG)

    assert config.level is LogLevel.DEBUG


def test_a05_stream_config_accepts_custom_formats() -> None:
    config = LoggingStreamConfig(
        log_format="%(levelname)s %(message)s",
        date_format="%H:%M:%S",
    )

    assert config.log_format == "%(levelname)s %(message)s"
    assert config.date_format == "%H:%M:%S"


def test_a06_stream_config_accepts_formatter_factory() -> None:
    config = LoggingStreamConfig(formatter_factory=custom_formatter_factory)

    logger = logging.Logger("test.a06")
    handler = config.apply_config_to_logger(logger)

    assert isinstance(handler.formatter, CustomFormatter)


def test_a07_stream_config_accepts_filters_tuple() -> None:
    filter_a = RecordingFilter()
    filter_b = RecordingFilter()

    config = LoggingStreamConfig(filters=(filter_a, filter_b))

    assert config.filters == (filter_a, filter_b)


@pytest.mark.parametrize("level", [logging.INFO, "INFO", object(), None])
def test_a08_stream_config_rejects_invalid_level(level: object) -> None:
    with pytest.raises(TypeError, match="level must be an instance of LogLevel"):
        LoggingStreamConfig(level=level)  # type: ignore[arg-type]


@pytest.mark.parametrize("formatter_factory", [object(), "factory", 123])
def test_a09_stream_config_rejects_non_callable_formatter_factory(
    formatter_factory: object,
) -> None:
    with pytest.raises(TypeError, match="formatter_factory must be callable"):
        LoggingStreamConfig(formatter_factory=formatter_factory)  # type: ignore[arg-type]


@pytest.mark.parametrize("filters", [[], [RecordingFilter()], object(), "filter"])
def test_a10_stream_config_rejects_non_tuple_filters(filters: object) -> None:
    with pytest.raises(TypeError, match="filters must be a tuple"):
        LoggingStreamConfig(filters=filters)  # type: ignore[arg-type]


def test_a11_stream_config_rejects_tuple_with_non_filter_item() -> None:
    with pytest.raises(TypeError, match="filters must contain only logging.Filter"):
        LoggingStreamConfig(filters=(RecordingFilter(), object()))  # type: ignore[arg-type]


@pytest.mark.parametrize("stream_output", ["stdout", "stderr", object(), None])
def test_a12_stream_config_rejects_invalid_stream_output(stream_output: object) -> None:
    with pytest.raises(TypeError, match="stream_output must be an instance of LogStreamOutput"):
        LoggingStreamConfig(stream_output=stream_output)  # type: ignore[arg-type]


# ---- B. LoggingStreamConfig handler creation ---------------------------------------------


def test_b01_stream_config_get_handler_returns_stdout_handler() -> None:
    config = LoggingStreamConfig(stream_output=LogStreamOutput.STDOUT)

    handler = config._get_handler()

    assert isinstance(handler, logging.StreamHandler)
    assert handler.stream is sys.stdout


def test_b02_stream_config_get_handler_returns_stderr_handler() -> None:
    config = LoggingStreamConfig()

    handler = config._get_handler()

    assert isinstance(handler, logging.StreamHandler)
    assert handler.stream is sys.stderr


def test_b03_stream_config_apply_config_installs_stdout_handler() -> None:
    config = LoggingStreamConfig(LogStreamOutput.STDOUT)
    logger = logging.Logger("test.b03")

    handler = config.apply_config_to_logger(logger)

    assert handler in logger.handlers
    assert isinstance(handler, logging.StreamHandler)
    assert handler.stream is sys.stdout


def test_b04_stream_config_apply_config_installs_stderr_handler() -> None:
    config = LoggingStreamConfig()
    logger = logging.Logger("test.b04")

    handler = config.apply_config_to_logger(logger)

    assert handler in logger.handlers
    assert isinstance(handler, logging.StreamHandler)
    assert handler.stream is sys.stderr


# ---- C. LoggingFileConfig constructor ----------------------------------------------------


def test_c01_file_config_stores_path_mode_and_encoding(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"

    config = LoggingFileConfig(
        file_path=file_path,
        mode="w",
    )

    assert config.file_path == file_path
    assert config.mode == "w"
    assert config.encoding == "utf-8"


def test_c02_file_config_accepts_string_path(tmp_path: pathlib.Path) -> None:
    file_path = str(tmp_path / "app.log")

    config = LoggingFileConfig(file_path=file_path)

    assert config.file_path == file_path


def test_c03_file_config_accepts_pathlike_path(tmp_path: pathlib.Path) -> None:
    file_path: os.PathLike[str] = tmp_path / "app.log"

    config = LoggingFileConfig(file_path=file_path)

    assert config.file_path == file_path


def test_c04_file_config_uses_default_mode_and_encoding(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"

    config = LoggingFileConfig(file_path=file_path)

    assert config.mode == "a"
    assert config.encoding == "utf-8"


def test_c05_file_config_inherits_base_level_and_formats(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"

    config = LoggingFileConfig(
        file_path=file_path,
        level=LogLevel.ERROR,
        log_format="%(message)s",
        date_format="%H:%M",
    )

    assert config.level is LogLevel.ERROR
    assert config.log_format == "%(message)s"
    assert config.date_format == "%H:%M"


def test_c06_file_config_rejects_invalid_base_level(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"

    with pytest.raises(TypeError, match="level must be an instance of LogLevel"):
        LoggingFileConfig(file_path=file_path, level="INFO")  # type: ignore[arg-type]


# ---- D. LoggingFileConfig handler creation -----------------------------------------------


def test_d01_file_config_get_handler_returns_file_handler(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"
    config = LoggingFileConfig(file_path=file_path)

    handler = config._get_handler()
    try:
        assert isinstance(handler, logging.FileHandler)
        assert pathlib.Path(handler.baseFilename) == file_path
    finally:
        handler.close()


def test_d02_file_config_handler_writes_to_file(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"
    config = LoggingFileConfig(
        file_path=file_path,
        mode="w",
        log_format="%(message)s",
    )

    logger = logging.Logger("test.d02")
    handler = config.apply_config_to_logger(logger)

    logger.handle(make_record(msg="hello"))
    handler.flush()

    assert file_path.read_text(encoding="utf-8").strip() == "hello"

    handler.close()


def test_d03_file_config_respects_write_mode(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"
    file_path.write_text("old\n", encoding="utf-8")

    config = LoggingFileConfig(
        file_path=file_path,
        mode="w",
        log_format="%(message)s",
    )

    logger = logging.Logger("test.d03")
    handler = config.apply_config_to_logger(logger)

    logger.handle(make_record(msg="new"))
    handler.flush()
    handler.close()

    assert file_path.read_text(encoding="utf-8").strip() == "new"


def test_d04_file_config_respects_append_mode(tmp_path: pathlib.Path) -> None:
    file_path = tmp_path / "app.log"
    file_path.write_text("old\n", encoding="utf-8")

    config = LoggingFileConfig(
        file_path=file_path,
        log_format="%(message)s",
    )

    logger = logging.Logger("test.d04")
    handler = config.apply_config_to_logger(logger)

    logger.handle(make_record(msg="new"))
    handler.flush()
    handler.close()

    assert file_path.read_text(encoding="utf-8").splitlines() == ["old", "new"]


# ---- E. apply_config_to_logger common behavior -------------------------------------------


def test_e01_apply_config_sets_logger_level() -> None:
    config = LoggingStreamConfig(level=LogLevel.ERROR)
    logger = logging.Logger("test.e01")

    config.apply_config_to_logger(logger)

    assert logger.level == logging.ERROR


def test_e02_apply_config_sets_handler_level() -> None:
    config = LoggingStreamConfig(level=LogLevel.WARNING)
    logger = logging.Logger("test.e02")

    handler = config.apply_config_to_logger(logger)

    assert handler.level == logging.WARNING


def test_e03_apply_config_disables_propagation() -> None:
    config = LoggingStreamConfig()
    logger = logging.Logger("test.e03")
    logger.propagate = True

    config.apply_config_to_logger(logger)

    assert logger.propagate is False


def test_e04_apply_config_adds_handler_to_logger() -> None:
    config = LoggingStreamConfig()
    logger = logging.Logger("test.e04")

    handler = config.apply_config_to_logger(logger)

    assert logger.handlers == [handler]


def test_e05_apply_config_clears_logger_filters() -> None:
    config = LoggingStreamConfig()
    logger = logging.Logger("test.e05")
    logger.addFilter(RecordingFilter())

    config.apply_config_to_logger(logger)

    assert logger.filters == []


def test_e06_apply_config_attaches_filters_to_handler() -> None:
    filter_a = RecordingFilter()
    filter_b = RecordingFilter()

    config = LoggingStreamConfig(filters=(filter_a, filter_b))
    logger = logging.Logger("test.e06")

    handler = config.apply_config_to_logger(logger)

    assert handler.filters == [filter_a, filter_b]


def test_e07_apply_config_uses_default_formatter_when_factory_is_not_provided() -> None:
    config = LoggingStreamConfig()
    logger = logging.Logger("test.e07")

    handler = config.apply_config_to_logger(logger)

    assert isinstance(handler.formatter, logging.Formatter)
    assert not isinstance(handler.formatter, CustomFormatter)


def test_e08_apply_config_uses_custom_formatter_factory() -> None:
    config = LoggingStreamConfig(formatter_factory=custom_formatter_factory)
    logger = logging.Logger("test.e08")

    handler = config.apply_config_to_logger(logger)

    assert isinstance(handler.formatter, CustomFormatter)


def test_e09_custom_formatter_factory_receives_log_format_and_date_format() -> None:
    observed: dict[str, str] = {}

    def factory(log_format: str, date_format: str) -> logging.Formatter:
        observed["log_format"] = log_format
        observed["date_format"] = date_format
        return logging.Formatter(log_format, date_format)

    config = LoggingStreamConfig(
        log_format="%(message)s",
        date_format="%H:%M:%S",
        formatter_factory=factory,
    )
    logger = logging.Logger("test.e09")

    config.apply_config_to_logger(logger)

    assert observed == {
        "log_format": "%(message)s",
        "date_format": "%H:%M:%S",
    }


def test_e10_apply_config_replaces_existing_handler() -> None:
    config = LoggingStreamConfig()
    logger = logging.Logger("test.e10")

    old_handler = logging.StreamHandler(sys.stderr)
    logger.addHandler(old_handler)

    new_handler = config.apply_config_to_logger(logger)

    assert logger.handlers == [new_handler]
    assert old_handler not in logger.handlers


def test_e11_apply_config_closes_replaced_file_handler(tmp_path: pathlib.Path) -> None:
    old_file_path = tmp_path / "old.log"
    old_handler = logging.FileHandler(old_file_path)

    logger = logging.Logger("test.e11")
    logger.addHandler(old_handler)

    config = LoggingStreamConfig()

    config.apply_config_to_logger(logger)

    assert old_handler.stream is None


def test_e12_apply_config_does_not_close_replaced_stdout_stream_handler() -> None:
    logger = logging.Logger("test.e12")
    old_handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(old_handler)

    config = LoggingStreamConfig()

    config.apply_config_to_logger(logger)

    assert not sys.stdout.closed


def test_e13_apply_config_does_not_close_replaced_stderr_stream_handler() -> None:
    logger = logging.Logger("test.e13")
    old_handler = logging.StreamHandler(sys.stderr)
    logger.addHandler(old_handler)

    config = LoggingStreamConfig(stream_output=LogStreamOutput.STDOUT)

    config.apply_config_to_logger(logger)

    assert not sys.stderr.closed


def test_e14_filter_blocks_record_when_attached_to_handler() -> None:
    blocking_filter = RecordingFilter(allow=False)
    config = LoggingFileConfig(
        file_path=pathlib.Path("/tmp") / "unused.log",
        filters=(blocking_filter,),
    )

    logger = logging.Logger("test.e14")
    handler = config.apply_config_to_logger(logger)
    try:
        record = make_record(msg="blocked")

        logger.handle(record)

        assert blocking_filter.records == [record]
    finally:
        handler.close()


# ---- F. Formatting behavior ---------------------------------------------------------------


def test_f01_default_format_can_format_custom_log_event_fields() -> None:
    config = LoggingStreamConfig()
    logger = logging.Logger("test.f01")

    handler = config.apply_config_to_logger(logger)

    formatted = handler.format(make_record())

    assert "INFO" in formatted
    assert "message" in formatted
    assert "{'x': 1}" in formatted


def test_f02_custom_format_is_used() -> None:
    config = LoggingStreamConfig(log_format="%(levelname)s:%(message)s")
    logger = logging.Logger("test.f02")

    handler = config.apply_config_to_logger(logger)

    assert handler.format(make_record(msg="hello")) == "INFO:hello"


def test_f03_custom_date_format_is_used() -> None:
    config = LoggingStreamConfig(
        log_format="%(asctime)s %(message)s",
        date_format="%Y",
    )
    logger = logging.Logger("test.f03")

    handler = config.apply_config_to_logger(logger)

    formatted = handler.format(make_record(msg="hello"))

    assert formatted.endswith("hello")
    assert formatted[:4].isdigit()


def test_f04_custom_format_can_format_event_fields() -> None:
    config = LoggingStreamConfig(
        log_format="%(event_name)s [%(event_type)s:%(entity_id)s] %(payload)s"
    )
    logger = logging.Logger("test.f04")

    handler = config.apply_config_to_logger(logger)

    formatted = handler.format(make_record())

    assert formatted == "event [type:entity] {'x': 1}"


# ---- G. Abstract base behavior ------------------------------------------------------------


def test_g01_logging_base_config_cannot_be_instantiated_directly() -> None:
    from mvx.common.logger.adapter_logging.logging_configs import LoggingBaseConfig

    with pytest.raises(TypeError):
        # noinspection PyAbstractClass
        LoggingBaseConfig()


def test_g02_subclass_without_get_handler_cannot_be_instantiated() -> None:
    from mvx.common.logger.adapter_logging.logging_configs import LoggingBaseConfig

    # noinspection PyAbstractClass
    class BrokenConfig(LoggingBaseConfig):
        pass

    with pytest.raises(TypeError):
        # noinspection PyAbstractClass
        BrokenConfig()


# ---- H. Enum behavior ---------------------------------------------------------------------


def test_h01_log_stream_output_values() -> None:
    assert LogStreamOutput.STDOUT.value == "stdout"
    assert LogStreamOutput.STDERR.value == "stderr"


def test_h02_log_stream_output_constructs_from_string() -> None:
    assert LogStreamOutput("stdout") is LogStreamOutput.STDOUT
    assert LogStreamOutput("stderr") is LogStreamOutput.STDERR
