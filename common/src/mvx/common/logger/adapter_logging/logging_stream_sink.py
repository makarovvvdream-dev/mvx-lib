# src/mvx/common/logger/adapter_logging/logging_stream_sink.py
"""
Synchronous stream sink backed by Python's standard logging package.

The sink adapts MVX ``LogEvent`` objects to ``logging.LogRecord`` objects and
delivers them to either ``stdout`` or ``stderr`` through ``LoggingStreamConfig``.
It is intended as the default lightweight sink for console and bootstrap
logging.
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
    Synchronous logger-backed sink for standard streams.

    Args:
        logger_name: Name assigned to the internal standard ``logging.Logger``.
        config: Optional stream logging configuration. If omitted, a default
            ``LoggingStreamConfig`` targeting ``stderr`` is used.

    Raises:
        TypeError: If ``logger_name`` is not a string or ``config`` is not a
            ``LoggingStreamConfig`` instance.
        ValueError: If ``logger_name`` is ``None`` or empty.
    """

    def __init__(
        self,
        *,
        logger_name: str = DEFAULT_STREAM_LOGGER_NAME,
        config: LoggingStreamConfig | None = None,
    ) -> None:
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
        Build the registry descriptor for this stream sink class.

        Args:
            **kwargs: Sink construction arguments. Recognized keys are
                ``logger_name`` and ``config``.

        Returns:
            Descriptor used by ``LogSinkRegistry`` to identify compatible stream
            sink registrations.
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

        Args:
            **kwargs: Arguments passed to ``StreamLogSink``.

        Returns:
            A pair containing the created sink and an idempotent terminator.
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
        Deliver a log event to the configured stream.

        Args:
            event: MVX log event to deliver.

        Notes:
            Calls made after ``close()`` are ignored.
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
        """
        with self._lock:
            if self._closed:
                return

            self._closed = True

            self._logger.removeHandler(self._handler)

            if isinstance(self._handler, logging.FileHandler):
                self._handler.close()
