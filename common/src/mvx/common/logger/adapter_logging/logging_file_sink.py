# src/mvx/common/logger/adapter_logging/logging_file_sink.py
"""
Asynchronous file sink backed by Python's standard logging package.

The sink adapts MVX ``LogEvent`` objects to ``logging.LogRecord`` objects and
writes them to a configured file handler. It uses ``AsyncioLogSink`` as the
buffered asynchronous runtime, so public ``log()`` calls remain thread-safe and
non-blocking with respect to actual file delivery.
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
    Asynchronous logger-backed sink for file output.

    Args:
        logger_name: Name assigned to the internal standard ``logging.Logger``.
        config: File logging configuration used to create the file handler.
        **kwargs: Additional keyword arguments passed to ``AsyncioLogSink``.

    Raises:
        TypeError: If ``logger_name`` is not a string or ``config`` is not a
            ``LoggingFileConfig`` instance.
        ValueError: If ``logger_name`` or ``config`` is ``None``, or if
            ``logger_name`` is empty.
    """

    def __init__(
        self,
        *,
        logger_name: str = DEFAULT_FILE_LOGGER_NAME,
        config: LoggingFileConfig,
        **kwargs: Any,
    ) -> None:

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
        Build the registry descriptor for this file sink class.

        Args:
            **kwargs: Sink construction arguments. Recognized keys are
                ``logger_name`` and ``config``.

        Returns:
            Descriptor used by ``LogSinkRegistry`` to identify compatible file
            sink registrations.
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

        Args:
            **kwargs: Arguments passed to ``FileLogSink``.

        Returns:
            A pair containing the created sink and an idempotent terminator.

        Notes:
            The returned terminator always closes the file handler, even if the
            sink was created but never started.
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
        Deliver one log event to the configured file handler.

        Args:
            event: MVX log event to write.
        """
        record = make_log_record_from_event(self._logger_name, event)
        self._logger.handle(record)

    async def _on_stopped(self) -> None:
        """
        Close file resources during graceful sink shutdown.
        """
        self._close_handler()

    def _close_handler(self) -> None:
        """
        Close the installed file handler once.

        The method is idempotent and is shared by graceful stop and by the
        terminator path used when the sink was never started.
        """
        with self._handler_lock:
            if self._handler_closed:
                return

            self._handler_closed = True

            self._logger.removeHandler(self._handler)
            self._handler.close()
