# src/mvx/networking/engines/tcp_stream_engine/metric_events.py
from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from time import monotonic_ns
from typing import Generic, TypeVar, Self

from mvx.common.metrics import MetricEvent

__all__ = (
    "TcpStreamOpenResult",
    "TcpStreamOpenMetricEvent",
    "TcpStreamCloseResult",
    "TcpStreamCloseMetricEvent",
    "TcpStreamStartTlsResult",
    "TcpStreamStartTlsMetricEvent",
    "TcpStreamCryptoCodecAttachResult",
    "TcpStreamCryptoCodecAttachMetricEvent",
    "TcpStreamCryptoCodecDetachResult",
    "TcpStreamCryptoCodecDetachMetricEvent",
    "TcpStreamStreamReadResult",
    "TcpStreamStreamReadMetricEvent",
    "TcpStreamStreamWriteResult",
    "TcpStreamStreamWriteMetricEvent",
    "TcpStreamDrainResult",
    "TcpStreamDrainMetricEvent",
)

ResultT = TypeVar("ResultT", bound=StrEnum)


@dataclass(slots=True)
class _TimedMetricEvent(MetricEvent, Generic[ResultT]):
    _started_ns: int = field(init=False, default_factory=monotonic_ns, repr=False)
    _finished_ns: int | None = field(init=False, default=None, repr=False)
    _result: ResultT | None = field(init=False, default=None, repr=False)
    _is_sent: bool = field(init=False, default=False, repr=False)

    @property
    def result(self) -> ResultT:
        result = self._result
        if result is None:
            raise RuntimeError("metric event result is not set")

        return result

    @property
    def duration_ns(self) -> int:
        finished_ns = self._finished_ns
        if finished_ns is None:
            return monotonic_ns() - self._started_ns

        return finished_ns - self._started_ns

    def _set_result(self, result: ResultT) -> Self:
        if result is None:
            raise ValueError("argument 'result' must not be None")

        if self._is_sent:
            raise RuntimeError("metric event is already sent")

        if self._result is not None:
            raise RuntimeError("metric event result is already set")

        self._result = result
        self._finished_ns = monotonic_ns()

        return self

    def _pre_send_check(self) -> None:
        if self._is_sent:
            raise RuntimeError("metric event is already sent")

        if self._result is None:
            raise RuntimeError("metric event result is not set")

        if self._finished_ns is None:
            raise RuntimeError("metric event finish time is not set")

        self._is_sent = True

    @property
    def event_type(self) -> str:
        raise NotImplementedError()


# ---- Open event --------------------------------------------------------------------------


class TcpStreamOpenResult(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    ALREADY_OPENED = "ALREADY_OPENED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass(slots=True)
class TcpStreamOpenMetricEvent(_TimedMetricEvent[TcpStreamOpenResult]):
    use_ssl: bool

    @property
    def event_type(self) -> str:
        return "tcp_stream.open"


# ---- Close event -------------------------------------------------------------------------


class TcpStreamCloseResult(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    NOT_OPENED = "NOT_OPENED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass(slots=True)
class TcpStreamCloseMetricEvent(_TimedMetricEvent[TcpStreamCloseResult]):
    @property
    def event_type(self) -> str:
        return "tcp_stream.close"


# ---- Start TLS event ---------------------------------------------------------------------


class TcpStreamStartTlsResult(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"
    REFUSED_NOT_OPENED = "REFUSED_NOT_OPENED"
    REFUSED_ALREADY_UNDER_SSL = "REFUSED_ALREADY_UNDER_SSL"
    REFUSED_START_TLS_ALREADY_ACTIVE = "REFUSED_START_TLS_ALREADY_ACTIVE"
    REFUSED_CRYPTO_CODEC_ATTACHED = "REFUSED_CRYPTO_CODEC_ATTACHED"
    TLS_FAILED = "TLS_FAILED"


@dataclass(slots=True)
class TcpStreamStartTlsMetricEvent(_TimedMetricEvent[TcpStreamStartTlsResult]):
    @property
    def event_type(self) -> str:
        return "tcp_stream.start_tls"


# ---- Crypto codec attach event -----------------------------------------------------------


class TcpStreamCryptoCodecAttachResult(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    REFUSED_NOT_OPENED = "REFUSED_NOT_OPENED"
    REFUSED_ALREADY_UNDER_SSL = "REFUSED_ALREADY_UNDER_SSL"
    REFUSED_START_TLS_ACTIVE = "REFUSED_START_TLS_ACTIVE"
    REFUSED_ALREADY_ATTACHED = "REFUSED_ALREADY_ATTACHED"


@dataclass(slots=True)
class TcpStreamCryptoCodecAttachMetricEvent(_TimedMetricEvent[TcpStreamCryptoCodecAttachResult]):
    @property
    def event_type(self) -> str:
        return "tcp_stream.crypto_codec.attach"


# ---- Crypto codec detach event -----------------------------------------------------------


class TcpStreamCryptoCodecDetachResult(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    REFUSED_NOT_OPENED = "REFUSED_NOT_OPENED"
    REFUSED_NOT_ATTACHED = "REFUSED_NOT_ATTACHED"


@dataclass(slots=True)
class TcpStreamCryptoCodecDetachMetricEvent(_TimedMetricEvent[TcpStreamCryptoCodecDetachResult]):
    @property
    def event_type(self) -> str:
        return "tcp_stream.crypto_codec.detach"


# ---- Stream read event -------------------------------------------------------------------


class TcpStreamStreamReadResult(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    TIMED_OUT = "TIMED_OUT"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TLS_FAILED = "TLS_FAILED"
    REMOTE_DISCONNECTED = "REMOTE_DISCONNECTED"


@dataclass(slots=True)
class TcpStreamStreamReadMetricEvent(_TimedMetricEvent[TcpStreamStreamReadResult]):
    bytes_count: int = 0

    @property
    def event_type(self) -> str:
        return "tcp_stream.stream_read"

    def _set_result(
        self,
        result: TcpStreamStreamReadResult,
        *,
        bytes_count: int = 0,
    ) -> Self:
        if isinstance(bytes_count, bool) or not isinstance(bytes_count, int):
            raise TypeError("argument 'bytes_count' must be integer when provided")

        if bytes_count < 0:
            raise ValueError("argument 'bytes_count' must not be negative")

        if self._is_sent:
            raise RuntimeError("metric event is already sent")

        if self._result is not None:
            raise RuntimeError("metric event result is already set")

        self.bytes_count = bytes_count
        _TimedMetricEvent._set_result(self, result)

        return self


# ---- Stream write event ------------------------------------------------------------------


class TcpStreamStreamWriteResult(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TLS_FAILED = "TLS_FAILED"


@dataclass(slots=True)
class TcpStreamStreamWriteMetricEvent(_TimedMetricEvent[TcpStreamStreamWriteResult]):
    bytes_count: int = 0

    @property
    def event_type(self) -> str:
        return "tcp_stream.stream_write"

    def _set_result(
        self,
        result: TcpStreamStreamWriteResult,
        *,
        bytes_count: int = 0,
    ) -> Self:

        if isinstance(bytes_count, bool) or not isinstance(bytes_count, int):
            raise TypeError("argument 'bytes_count' must be integer when provided")

        if bytes_count < 0:
            raise ValueError("argument 'bytes_count' must not be negative")

        if self._is_sent:
            raise RuntimeError("metric event is already sent")

        if self._result is not None:
            raise RuntimeError("metric event result is already set")

        self.bytes_count = bytes_count
        _TimedMetricEvent._set_result(self, result)

        return self


# ---- Drain event -------------------------------------------------------------------------


class TcpStreamDrainResult(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    TIMED_OUT = "TIMED_OUT"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TLS_FAILED = "TLS_FAILED"


@dataclass(slots=True)
class TcpStreamDrainMetricEvent(_TimedMetricEvent[TcpStreamDrainResult]):
    @property
    def event_type(self) -> str:
        return "tcp_stream.drain"
