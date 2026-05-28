# src/mvx/common/logger/adapter_logging/logging_stream_sink.py
"""
Stream sink backed by Python's standard logging package.

The sink adapts `LogEvent` objects to standard-library `logging.LogRecord`
objects and delivers them to stdout or stderr through `LoggingStreamConfig`.
"""

from __future__ import annotations

from typing import Any
import logging
import threading

from ..models import (
    LogEvent,
    LogSinkDescriptor,
    LogSinkProto,
    LogSinkTerminator,
)
from .logging_configs import LoggingStreamConfig
from .log_record_factory import make_log_record_from_event

__all__ = ("StreamLogSink",)


DEFAULT_STREAM_LOGGER_NAME = "mvx.stream_log_sink"


class StreamLogSink:
    """
    Ready-to-use synchronous sink for standard streams.

    `StreamLogSink` owns an internal `logging.Logger` configured by
    `LoggingStreamConfig`. It delivers prepared `LogEvent` objects to the
    configured standard stream.
    """

    def __init__(
        self,
        *,
        logger_name: str = DEFAULT_STREAM_LOGGER_NAME,
        config: LoggingStreamConfig | None = None,
    ) -> None:
        """
        Create a stream sink.

        :param logger_name: name assigned to the internal standard-library logger.
        :param config: optional stream logging configuration. If omitted, a default
            `LoggingStreamConfig` is used.
        :raises TypeError: if an argument has an invalid type.
        :raises ValueError: if `logger_name` is None or empty.
        """
        if logger_name is None:
            raise ValueError("logger_name is mandatory, must not be None")

        if not isinstance(logger_name, str):
            raise TypeError("logger_name must be a string")

        if not logger_name:
            raise ValueError("logger_name must not be empty")

        if config is not None and not isinstance(config, LoggingStreamConfig):
            raise TypeError("config must be an instance of LoggingStreamConfig")

        self._lock = threading.RLock()
        self._closed = False

        self._logger_name = logger_name
        self._logger = logging.Logger(logger_name)

        self._config = config if config is not None else LoggingStreamConfig()
        self._handler = self._config.apply_config_to_logger(self._logger)

    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor:
        """
        Build a descriptor for a stream sink configuration.

        The descriptor is used by the package-level sink registry to detect idempotent
        configuration requests and conflicts.

        :param kwargs: stream sink construction arguments.
        :return: stream sink descriptor.
        :raises TypeError: if an argument has an invalid type.
        :raises ValueError: if `logger_name` is None or empty.
        """
        logger_name = kwargs.get("logger_name", DEFAULT_STREAM_LOGGER_NAME)

        if logger_name is None:
            raise ValueError("logger_name must not be None")

        if not isinstance(logger_name, str):
            raise TypeError("logger_name must be a string")

        if not logger_name:
            raise ValueError("logger_name must not be empty")

        config = kwargs.get("config")

        if config is None:
            config = LoggingStreamConfig()

        if not isinstance(config, LoggingStreamConfig):
            raise TypeError("config must be an instance of LoggingStreamConfig")

        return LogSinkDescriptor(
            sink_type="stream",
            resource_key=(
                "stream",
                logger_name,
                config.stream_output.value,
            ),
            config_key=(
                "level",
                config.level.value,
                "log_format",
                config.log_format,
                "date_format",
                config.date_format,
                "filters",
                tuple(type(_filter).__qualname__ for _filter in config.filters),
            ),
        )

    @classmethod
    def create(cls, **kwargs: Any) -> tuple[LogSinkProto, LogSinkTerminator]:
        """
        Create a stream sink and its terminator.

        :param kwargs: arguments passed to `StreamLogSink`.
        :return: pair containing the created sink and an idempotent terminator.
        """
        sink = cls(**kwargs)

        terminator_lock = threading.Lock()
        terminated = False

        def terminator() -> None:
            nonlocal terminated

            with terminator_lock:
                if terminated:
                    return

                terminated = True

            sink.close()

        return sink, terminator

    def log(self, event: LogEvent) -> None:
        """
        Deliver a prepared log event to the configured stream.

        Calls made after `close()` are ignored.

        :param event: prepared event to deliver.
        :return: None.
        """
        with self._lock:
            if self._closed:
                return

            record = make_log_record_from_event(self._logger_name, event)
            self._logger.handle(record)

    def close(self) -> None:
        """
        Close this sink.

        The method removes the installed handler from the internal logger. Standard
        output and error streams are detached but not closed. Repeated calls are
        ignored.

        :return: None.
        """
        with self._lock:
            if self._closed:
                return

            self._closed = True

            self._logger.removeHandler(self._handler)

            if isinstance(self._handler, logging.FileHandler):
                self._handler.close()
