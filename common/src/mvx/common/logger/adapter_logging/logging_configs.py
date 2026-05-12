# src/mvx/common/logger/adapter_logging/logging_configs.py
"""
Configuration helpers for sinks backed by Python's standard logging package.

This module defines small configuration objects that create and configure
``logging.Handler`` instances for logger-backed sinks. The configuration layer
owns formatter, filter, level and handler setup, while concrete sinks remain
responsible for delivering ``LogEvent`` objects.
"""

from __future__ import annotations

from typing import Callable, TypeAlias
from abc import ABC, abstractmethod
from enum import StrEnum

import sys
import logging
import os

from ..models import LogLevel

__all__ = (
    "DEFAULT_LOG_FORMAT",
    "DEFAULT_DATE_FORMAT",
    "FormatterFactory",
    "LogFilter",
    "LogStreamOutput",
    "LoggingBaseConfig",
    "LoggingStreamConfig",
    "LoggingFileConfig",
)


# ---- Logging configs ---------------------------------------------------------------------


DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s: %(message)s %(payload)s"

DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


# Formatter factory signature:
#   input:  log format string, date format string
#   output: configured logging.Formatter instance
FormatterFactory: TypeAlias = Callable[[str, str], logging.Formatter]

# Logging filter signature:
#   input:  logging.LogRecord
#   output: True to keep the record, False to drop it
# A logging.Filter instance is also accepted because stdlib logging supports it.
LogFilter: TypeAlias = logging.Filter | Callable[[logging.LogRecord], bool]


class LoggingBaseConfig(ABC):
    """
    Base configuration for logger-backed sinks.

    Subclasses provide the concrete handler, while the base class applies the
    shared logger setup: formatter, filters, levels, propagation and handler
    replacement.

    Args:
        level: Minimum log level accepted by the logger and handler.
        log_format: Format string passed to ``logging.Formatter``.
        date_format: Date/time format passed to ``logging.Formatter``.
        formatter_factory: Optional factory used to create a custom formatter.
        filters: Optional tuple of logging filters or filter callables attached
            to the created handler.
    """

    __slots__ = ("_level", "_log_format", "_date_format", "_formatter_factory", "_filters")

    def __init__(
        self,
        level: LogLevel = LogLevel.INFO,
        *,
        log_format: str = DEFAULT_LOG_FORMAT,
        date_format: str = DEFAULT_DATE_FORMAT,
        formatter_factory: FormatterFactory | None = None,
        filters: tuple[LogFilter, ...] | None = None,
    ) -> None:

        if not isinstance(level, LogLevel):
            raise TypeError("level must be an instance of LogLevel")

        if not isinstance(log_format, str):
            raise TypeError("log_format must be a string")

        if not isinstance(date_format, str):
            raise TypeError("date_format must be a string")

        if formatter_factory is not None and not callable(formatter_factory):
            raise TypeError("formatter_factory must be callable")

        if filters is not None:
            if not isinstance(filters, tuple):
                raise TypeError("filters must be a tuple of logging.Filter instances or callables")

            for _filter in filters:
                if not isinstance(_filter, logging.Filter) and not callable(_filter):
                    raise TypeError(
                        "filters must contain only logging.Filter instances or callables"
                    )

        self._level = level
        self._log_format = log_format
        self._date_format = date_format
        self._formatter_factory = formatter_factory
        self._filters = filters if filters is not None else ()

    @property
    def level(self) -> LogLevel:
        return self._level

    @property
    def log_format(self) -> str:
        return self._log_format

    @property
    def date_format(self) -> str:
        return self._date_format

    @property
    def filters(self) -> tuple[LogFilter, ...]:
        return self._filters

    @abstractmethod
    def _get_handler(self) -> logging.Handler:
        """
        Create a logging handler for this configuration.

        Returns:
            A new ``logging.Handler`` instance.
        """
        raise NotImplementedError()

    def apply_config_to_logger(self, logger: logging.Logger) -> logging.Handler:
        """
        Apply this configuration to a logger.

        Existing handlers are removed before the new handler is attached.
        Replaced file handlers are closed; standard stream handlers are detached
        but not closed.

        Args:
            logger: Logger instance to configure.

        Returns:
            The handler created and attached to the logger.

        Raises:
            TypeError: If ``logger`` is not a ``logging.Logger`` instance, or if
                a custom formatter factory returns a non-formatter object.
        """
        if not isinstance(logger, logging.Logger):
            raise TypeError("logger must be an instance of logging.Logger")

        handler = self._get_handler()

        if self._formatter_factory is not None:
            formatter = self._formatter_factory(self.log_format, self.date_format)

            if not isinstance(formatter, logging.Formatter):
                raise TypeError("formatter_factory must return logging.Formatter")

            handler.setFormatter(formatter)
        else:
            handler.setFormatter(
                logging.Formatter(
                    fmt=self.log_format,
                    datefmt=self.date_format,
                )
            )

        for _filter in self._filters:
            handler.addFilter(_filter)

        handler.setLevel(int(self.level))

        for old_handler in tuple(logger.handlers):
            logger.removeHandler(old_handler)

            if isinstance(old_handler, logging.FileHandler):
                old_handler.close()

        logger.filters.clear()
        logger.propagate = False

        logger.setLevel(int(self.level))
        logger.addHandler(handler)

        return handler


class LogStreamOutput(StrEnum):
    """
    Supported standard stream targets for stream-based logging.
    """

    STDOUT = "stdout"
    STDERR = "stderr"


class LoggingStreamConfig(LoggingBaseConfig):
    """
    Configuration for a logger-backed stdout or stderr sink.

    Args:
        stream_output: Standard stream target used by the created handler.
        level: Minimum log level accepted by the logger and handler.
        log_format: Format string passed to ``logging.Formatter``.
        date_format: Date/time format passed to ``logging.Formatter``.
        formatter_factory: Optional factory used to create a custom formatter.
        filters: Optional tuple of logging filters or filter callables attached
            to the created handler.
    """

    __slots__ = ("_stream_output",)

    def __init__(
        self,
        stream_output: LogStreamOutput = LogStreamOutput.STDERR,
        level: LogLevel = LogLevel.INFO,
        *,
        log_format: str = DEFAULT_LOG_FORMAT,
        date_format: str = DEFAULT_DATE_FORMAT,
        formatter_factory: FormatterFactory | None = None,
        filters: tuple[LogFilter, ...] | None = None,
    ) -> None:
        super().__init__(
            level,
            log_format=log_format,
            date_format=date_format,
            formatter_factory=formatter_factory,
            filters=filters,
        )
        if not isinstance(stream_output, LogStreamOutput):
            raise TypeError("stream_output must be an instance of LogStreamOutput")

        self._stream_output = stream_output

    @property
    def stream_output(self) -> LogStreamOutput:
        return self._stream_output

    def _get_handler(self) -> logging.Handler:
        if self._stream_output is LogStreamOutput.STDOUT:
            return logging.StreamHandler(sys.stdout)

        return logging.StreamHandler(sys.stderr)


class LoggingFileConfig(LoggingBaseConfig):
    """
    Configuration for a logger-backed file sink.

    Args:
        file_path: Path to the log file.
        level: Minimum log level accepted by the logger and handler.
        mode: File opening mode passed to ``logging.FileHandler``.
        encoding: File encoding passed to ``logging.FileHandler``.
        log_format: Format string passed to ``logging.Formatter``.
        date_format: Date/time format passed to ``logging.Formatter``.
        formatter_factory: Optional factory used to create a custom formatter.
        filters: Optional tuple of logging filters or filter callables attached
            to the created handler.
    """

    __slots__ = ("_file_path", "_mode", "_encoding")

    def __init__(
        self,
        file_path: str | os.PathLike[str],
        level: LogLevel = LogLevel.INFO,
        *,
        mode: str = "a",
        encoding: str = "utf-8",
        log_format: str = DEFAULT_LOG_FORMAT,
        date_format: str = DEFAULT_DATE_FORMAT,
        formatter_factory: FormatterFactory | None = None,
        filters: tuple[LogFilter, ...] | None = None,
    ) -> None:
        super().__init__(
            level,
            log_format=log_format,
            date_format=date_format,
            formatter_factory=formatter_factory,
            filters=filters,
        )

        if not isinstance(file_path, (str, os.PathLike)):
            raise TypeError("file_path must be a string or os.PathLike")

        if not isinstance(mode, str):
            raise TypeError("mode must be a string")

        if not isinstance(encoding, str):
            raise TypeError("encoding must be a string")

        self._file_path = file_path
        self._mode = mode
        self._encoding = encoding

    @property
    def file_path(self) -> str | os.PathLike[str]:
        return self._file_path

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def encoding(self) -> str:
        return self._encoding

    def _get_handler(self) -> logging.Handler:
        return logging.FileHandler(
            filename=self._file_path,
            mode=self._mode,
            encoding=self._encoding,
        )
