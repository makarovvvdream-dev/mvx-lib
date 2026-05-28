# src/mvx/common/logger/adapter_logging/logging_file_sink.py
"""
File sink backed by Python's standard logging package.

The sink adapts `LogEvent` objects to standard-library `logging.LogRecord`
objects and writes them to a configured file handler.

`FileLogSink` uses `AsyncioLogSink` as its asynchronous delivery runtime, so
public `log()` calls enqueue events while file delivery is handled by the sink
runtime.
"""

from __future__ import annotations

from typing import Any
from pathlib import Path

import threading
import logging

from ..asyncio_log_sink import AsyncioLogSink
from ..models import (
    LogEvent,
    LogSinkDescriptor,
    LogSinkProto,
    LogSinkTerminator,
)
from .logging_configs import LoggingFileConfig
from .log_record_factory import make_log_record_from_event

__all__ = ("FileLogSink",)


DEFAULT_FILE_LOGGER_NAME = "mvx.file_log_sink"


class FileLogSink(AsyncioLogSink):
    """
    Ready-to-use asynchronous sink for file output.

    `FileLogSink` owns an internal `logging.Logger` configured by
    `LoggingFileConfig`. It delivers prepared `LogEvent` objects to the configured
    file through the `AsyncioLogSink` runtime.
    """

    def __init__(
        self,
        *,
        logger_name: str = DEFAULT_FILE_LOGGER_NAME,
        config: LoggingFileConfig,
        **kwargs: Any,
    ) -> None:
        """
        Create a file sink.

        :param logger_name: name assigned to the internal standard-library logger.
        :param config: file logging configuration used to create the file handler.
        :param kwargs: additional keyword arguments passed to `AsyncioLogSink`.
        :raises TypeError: if an argument has an invalid type.
        :raises ValueError: if `logger_name` or `config` is None, or if `logger_name`
            is empty.
        """
        if logger_name is None:
            raise ValueError("logger_name is mandatory, must not be None")

        if not isinstance(logger_name, str):
            raise TypeError("logger_name must be a string")

        if not logger_name:
            raise ValueError("logger_name must not be empty")

        if config is None:
            raise ValueError("config is mandatory, must not be None")

        if not isinstance(config, LoggingFileConfig):
            raise TypeError("config must be an instance of LoggingFileConfig")

        super().__init__(**kwargs)

        self._logger_name = logger_name
        self._logger = logging.Logger(logger_name)

        self._config = config
        self._handler = self._config.apply_config_to_logger(self._logger)

        self._handler_lock = threading.RLock()
        self._handler_closed = False

    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor:
        """
        Build a descriptor for a file sink configuration.

        The descriptor is used by the package-level sink registry to detect idempotent
        configuration requests and conflicts. The file path is expanded and resolved
        before it is stored in the descriptor resource key.

        :param kwargs: file sink construction arguments.
        :return: file sink descriptor.
        :raises TypeError: if an argument has an invalid type.
        :raises ValueError: if `logger_name` is None or empty.
        """
        logger_name = kwargs.get("logger_name", DEFAULT_FILE_LOGGER_NAME)

        if logger_name is None:
            raise ValueError("logger_name must not be None")

        if not isinstance(logger_name, str):
            raise TypeError("logger_name must be a string")

        if not logger_name:
            raise ValueError("logger_name must not be empty")

        config = kwargs.get("config")

        if not isinstance(config, LoggingFileConfig):
            raise TypeError("config must be an instance of LoggingFileConfig")

        file_path = Path(config.file_path).expanduser().resolve()

        return LogSinkDescriptor(
            sink_type="file",
            resource_key=(
                "file",
                str(file_path),
            ),
            config_key=(
                "logger_name",
                logger_name,
                "level",
                config.level.value,
                "log_format",
                config.log_format,
                "date_format",
                config.date_format,
                "mode",
                config.mode,
                "encoding",
                config.encoding,
                "filters",
                tuple(type(_filter).__qualname__ for _filter in config.filters),
            ),
        )

    @classmethod
    def create(cls, **kwargs: Any) -> tuple[LogSinkProto, LogSinkTerminator]:
        """
        Create a file sink and its terminator.

        The terminator stops the asynchronous sink runtime and closes the file handler.
        The file handler is closed even if the sink was created but never started.

        :param kwargs: arguments passed to `FileLogSink`.
        :return: pair containing the created sink and an idempotent terminator.
        :raises TypeError: if the created sink is not a `FileLogSink` instance.
        """
        sink, base_terminator = super().create(**kwargs)

        if not isinstance(sink, FileLogSink):
            raise TypeError("created sink must be an instance of FileLogSink")

        file_sink = sink

        def terminator() -> None:
            try:
                base_terminator()
            finally:
                file_sink._close_handler()

        return file_sink, terminator

    async def _dispatch_core(self, event: LogEvent) -> None:
        """
        Deliver one prepared event to the configured file handler.

        :param event: prepared event to write.
        :return: None.
        """
        record = make_log_record_from_event(self._logger_name, event)
        self._logger.handle(record)

    async def _on_stopped(self) -> None:
        """
        Close file resources during graceful sink shutdown.

        :return: None.
        """
        self._close_handler()

    def _close_handler(self) -> None:
        """
        Close the installed file handler once.

        The method is shared by graceful stop and the terminator path.

        :return: None.
        """
        with self._handler_lock:
            if self._handler_closed:
                return

            self._handler_closed = True

            self._logger.removeHandler(self._handler)
            self._handler.close()
