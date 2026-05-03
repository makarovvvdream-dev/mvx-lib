# common/src/mvx/common/logger/logger_params.py
from __future__ import annotations
from typing import Callable, TypeAlias, Protocol
from enum import StrEnum, IntEnum
import logging
import os
from pathlib import Path

from .errors import LoggerParamsError, LoggerParamsViolation

__all__ = (
    "LogLevel",
    "LogSink",
    "LogFormatter",
    "LoggerConfig",
)

class LogLevel(IntEnum):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


class

class LogSink(StrEnum):
    STDOUT = "stdout"
    STDERR = "stderr"
    FILE = "file"

class LogFormatter(StrEnum):
    DEFAULT = "default"
    CUSTOM = "custom"

LogFormatterFields: TypeAlias = tuple[str, ...]
LogFormatterFactory: TypeAlias = Callable[[None], tuple[logging.Formatter, tuple[str,...]]]

class LoggerConfigProto(Protocol):
    @property
    def level(self) -> LogLevel: ...

    @property
    def sink(self) -> LogSink: ...

    @property
    def file_path(self) -> Path | None: ...

    def get_formatter(self) -> logging.Formatter: ...






class LoggerConfig:
    __slots__ = (
        "_level",
        "_sink",
        "_log_file",
        "_formatter",
        "_log_format_fields",
        "_namespace",
        "_profile",
    )

    def __init__(
        self,
        *,
        level: LogLevel = LogLevel.INFO,
        sink: LogSink = LogSink.STDOUT,
        file_path: Path | None = None,
        log_format: str = "%(asctime)s %(levelname)s %(name)s: %(message)s",
        formatter_factory: Callable[[str], tuple[logging.Formatter, tuple[str,...]]] | None = None,
        namespace: str | None = None,
        profile: str = "default",
    ) -> None:

        # -- check 'level'
        if not isinstance(level, LogLevel):
            raise LoggerParamsError(
                arg="level",
                violation=LoggerParamsViolation.MUST_BE_LOG_LEVEL,
            )

        self._level = level

        # -- check 'sink'
        if not isinstance(sink, LogSink):
            raise LoggerParamsError(
                arg="sink",
                violation=LoggerParamsViolation.MUST_BE_LOG_SINK,
            )

        self._sink = sink

        # -- check 'file_path'
        if sink is LogSink.FILE:
            if file_path is None:
                raise LoggerParamsError(
                    arg="file_path",
                    violation=LoggerParamsViolation.MUST_BE_PROVIDED_WHEN_SINK_IS_FILE,
                )
            if not isinstance(file_path, Path):
                raise LoggerParamsError(
                    arg="file_path",
                    violation=LoggerParamsViolation.MUST_BE_PATH,
                    value=file_path,
                )




        if file_path is not None:
            try:
                # noinspection PyTypeChecker
                file_path_str = os.fspath(file_path)
            except TypeError:
                raise LoggerParamsError(
                    arg="file_path",
                    violation=LoggerParamsViolation.MUST_BE_PATH_LIKE,
                    value=file_path,
                )

            if not isinstance(file_path_str, str):
                raise LoggerParamsError(
                    arg="file_path",
                    violation=LoggerParamsViolation.MUST_BE_PATH_LIKE,
                    value=file_path,
                )

            file_path_str = file_path_str.strip()
            if not file_path_str:
                raise LoggerParamsError(
                    arg="file_path",
                    violation=LoggerParamsViolation.MUST_NOT_BE_EMPTY_WHEN_PROVIDED,
                )

            normalized_file_path = Path(file_path_str)

        if sink == LogSink.FILE and normalized_file_path is None:
            raise LoggerParamsError(
                arg="file_path",
                violation=LoggerParamsViolation.REQUIRES_FILE_PATH_FOR_FILE,
            )

        # -- check 'log_format'
        if not isinstance(log_format, str):
            raise LoggerParamsError(
                arg="log_format", violation=LoggerParamsViolation.MUST_BE_STRING
            )
        log_format = log_format.strip()
        if not log_format:
            raise LoggerParamsError(
                arg="log_format", violation=LoggerParamsViolation.MUST_NOT_BE_EMPTY
            )

        # -- check 'formatter'
        if formatter is not None:
            if not isinstance(formatter, type) or not issubclass(formatter, logging.Formatter):
                raise LoggerParamsError(
                    arg="formatter",
                    violation=LoggerParamsViolation.MUST_BE_TYPE_OF_FORMATTER_WHEN_PROVIDED,
                    value=formatter,
                )

        # -- check 'namespace'
        if namespace is not None:
            if not isinstance(namespace, str):
                raise LoggerParamsError(
                    arg="namespace", violation=LoggerParamsViolation.MUST_BE_STRING
                )
            namespace = namespace.strip()
            if not namespace:
                raise LoggerParamsError(
                    arg="namespace",
                    violation=LoggerParamsViolation.MUST_NOT_BE_EMPTY_WHEN_PROVIDED,
                )

        # -- check 'profile'
        if not isinstance(profile, str):
            raise LoggerParamsError(arg="profile", violation=LoggerParamsViolation.MUST_BE_STRING)
        profile = profile.strip()
        if not profile:
            raise LoggerParamsError(
                arg="profile", violation=LoggerParamsViolation.MUST_NOT_BE_EMPTY
            )

        self._level = level
        self._sink = sink
        self._file_path = normalized_file_path
        self._log_format = log_format
        self._formatter = formatter
        self._namespace = namespace
        self._profile = profile

    @property
    def level(self) -> LogLevel:
        return self._level

    @property
    def sink(self) -> LogSink:
        return self._sink

    @property
    def file_path(self) -> Path | None:
        return self._file_path

    @property
    def log_format(self) -> str:
        return self._log_format

    @property
    def formatter(self) -> type[logging.Formatter] | None:
        return self._formatter

    @property
    def namespace(self) -> str | None:
        return self._namespace

    @property
    def profile(self) -> str:
        return self._profile

    def __str__(self) -> str:
        res: str = "LoggerParams:"
        res += f"\n\tlevel: {self._level.value}"
        res += f"\n\tsink: {self._sink.value}"
        res += f"\n\tfile_path: {self._file_path if self._file_path is not None else '<not set>'}"
        res += f"\n\tlog_format: {self._log_format}"
        res += f"\n\tformatter: {self._formatter.__name__ if self._formatter is not None else '<not set>'}"
        res += f"\n\tnamespace: {self._namespace if self._namespace is not None else '<not set>'}"
        res += f"\n\tprofile: {self._profile}"
        return res

    def __repr__(self) -> str:
        return str(self)
