# src/mvx/networking/engines/tcp_stream_engine/tcp_stream_engine.py
from __future__ import annotations

from typing import Any, Mapping
from dataclasses import dataclass
from enum import StrEnum

from ...metrics import Metric, MetricEvent, MetricsRecorderProto

__all__ = (
    "TcpStreamOpenAttemptOutcome",
    "TcpStreamOpenAttemptMetricEvent",
    "TcpStreamOpenAttemptsMetric",
    "TcpStreamCloseAttemptOutcome",
    "TcpStreamCloseAttemptMetricEvent",
    "TcpStreamCloseAttemptsMetric",
    "TcpStreamStartTlsAttemptOutcome",
    "TcpStreamStartTlsAttemptMetricEvent",
    "TcpStreamStartTlsAttemptsMetric",
    "TcpStreamCryptoCodecAttachAttemptOutcome",
    "TcpStreamCryptoCodecAttachAttemptMetricEvent",
    "TcpStreamCryptoCodecAttachAttemptsMetric",
    "TcpStreamCryptoCodecDetachAttemptOutcome",
    "TcpStreamCryptoCodecDetachAttemptMetricEvent",
    "TcpStreamCryptoCodecDetachAttemptsMetric",
    "TcpStreamStreamReadAttemptOutcome",
    "TcpStreamStreamReadAttemptMetricEvent",
    "TcpStreamStreamReadAttemptsMetric",
    "TcpStreamStreamWriteAttemptOutcome",
    "TcpStreamStreamWriteAttemptMetricEvent",
    "TcpStreamStreamWriteAttemptsMetric",
    "TcpStreamDrainAttemptOutcome",
    "TcpStreamDrainAttemptMetricEvent",
    "TcpStreamDrainAttemptsMetric",
    "TcpStreamBytesReceivedMetricEvent",
    "TcpStreamBytesReceivedMetric",
    "TcpStreamBytesSentMetricEvent",
    "TcpStreamBytesSentMetric",
    "TcpStreamRemoteDisconnectMetricEvent",
    "TcpStreamRemoteDisconnectMetric",
    "TcpStreamAbortiveCloseMetricEvent",
    "TcpStreamAbortiveCloseMetric",
    "MetricEvent",
    "MetricsRecorderProto",
)


# ---- Open attempts metric ----------------------------------------------------------------

# tcp_stream.open.attempts_total
# tcp_stream.open.success_total
# tcp_stream.open.already_opened_total
# tcp_stream.open.failure_total
# tcp_stream.open.cancelled_total


class TcpStreamOpenAttemptOutcome(StrEnum):
    SUCCESS = "SUCCESS"
    ALREADY_OPENED = "ALREADY_OPENED"
    FAILURE = "FAILURE"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True, slots=True)
class TcpStreamOpenAttemptMetricEvent(MetricEvent):
    use_ssl: bool
    outcome: TcpStreamOpenAttemptOutcome

    @property
    def event_type(self) -> str:
        return "tcp_stream.open.attempt"


class TcpStreamOpenAttemptsMetric(Metric):
    def __init__(self) -> None:
        self._total = 0
        self._success_total = 0
        self._already_opened_total = 0
        self._failure_total = 0
        self._cancelled_total = 0

    @property
    def metric_name(self) -> str:
        return "tcp_stream.open.attempts"

    def handle_event(self, event: MetricEvent) -> bool:
        if not isinstance(event, TcpStreamOpenAttemptMetricEvent):
            return False

        self._total += 1

        if event.outcome is TcpStreamOpenAttemptOutcome.SUCCESS:
            self._success_total += 1

        elif event.outcome is TcpStreamOpenAttemptOutcome.ALREADY_OPENED:
            self._already_opened_total += 1

        elif event.outcome is TcpStreamOpenAttemptOutcome.FAILURE:
            self._failure_total += 1

        elif event.outcome is TcpStreamOpenAttemptOutcome.CANCELLED:
            self._cancelled_total += 1

        return True

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "total": self._total,
                "success_total": self._success_total,
                "already_opened_total": self._already_opened_total,
                "failure_total": self._failure_total,
                "cancelled_total": self._cancelled_total,
            },
        }


# ---- Close attempts metric ---------------------------------------------------------------

# tcp_stream.close.attempts_total
# tcp_stream.close.success_total
# tcp_stream.close.not_opened_total
# tcp_stream.close.failure_total
# tcp_stream.close.cancelled_total


class TcpStreamCloseAttemptOutcome(StrEnum):
    SUCCESS = "SUCCESS"
    NOT_OPENED = "NOT_OPENED"
    FAILURE = "FAILURE"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True, slots=True)
class TcpStreamCloseAttemptMetricEvent(MetricEvent):
    outcome: TcpStreamCloseAttemptOutcome

    @property
    def event_type(self) -> str:
        return "tcp_stream.close.attempt"


class TcpStreamCloseAttemptsMetric(Metric):
    def __init__(self) -> None:
        self._total = 0
        self._success_total = 0
        self._not_opened_total = 0
        self._failure_total = 0
        self._cancelled_total = 0

    @property
    def metric_name(self) -> str:
        return "tcp_stream.close.attempts"

    def handle_event(self, event: MetricEvent) -> bool:
        if not isinstance(event, TcpStreamCloseAttemptMetricEvent):
            return False

        self._total += 1

        if event.outcome is TcpStreamCloseAttemptOutcome.SUCCESS:
            self._success_total += 1

        elif event.outcome is TcpStreamCloseAttemptOutcome.NOT_OPENED:
            self._not_opened_total += 1

        elif event.outcome is TcpStreamCloseAttemptOutcome.FAILURE:
            self._failure_total += 1

        elif event.outcome is TcpStreamCloseAttemptOutcome.CANCELLED:
            self._cancelled_total += 1

        return True

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "total": self._total,
                "success_total": self._success_total,
                "not_opened_total": self._not_opened_total,
                "failure_total": self._failure_total,
                "cancelled_total": self._cancelled_total,
            },
        }


# ---- Start TLS metric --------------------------------------------------------------------

# tcp_stream.start_tls.attempts_total
# tcp_stream.start_tls.success_total
# tcp_stream.start_tls.failure_total
# tcp_stream.start_tls.cancelled_total
# tcp_stream.start_tls.timeout_total
# tcp_stream.start_tls.refused_not_opened_total
# tcp_stream.start_tls.refused_already_under_ssl_total
# tcp_stream.start_tls.refused_start_tls_already_active_total
# tcp_stream.start_tls.refused_crypto_codec_attached_total
# tcp_stream.start_tls.tls_error_total


class TcpStreamStartTlsAttemptOutcome(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"
    REFUSED_NOT_OPENED = "REFUSED_NOT_OPENED"
    REFUSED_ALREADY_UNDER_SSL = "REFUSED_ALREADY_UNDER_SSL"
    REFUSED_START_TLS_ALREADY_ACTIVE = "REFUSED_START_TLS_ALREADY_ACTIVE"
    REFUSED_CRYPTO_CODEC_ATTACHED = "REFUSED_CRYPTO_CODEC_ATTACHED"
    TLS_ERROR = "TLS_ERROR"


@dataclass(frozen=True, slots=True)
class TcpStreamStartTlsAttemptMetricEvent(MetricEvent):
    outcome: TcpStreamStartTlsAttemptOutcome

    @property
    def event_type(self) -> str:
        return "tcp_stream.start_tls.attempt"


class TcpStreamStartTlsAttemptsMetric(Metric):
    def __init__(self) -> None:
        self._total = 0
        self._success_total = 0
        self._failure_total = 0
        self._cancelled_total = 0
        self._timeout_total = 0
        self._refused_not_opened_total = 0
        self._refused_already_under_ssl_total = 0
        self._refused_start_tls_already_active_total = 0
        self._refused_crypto_codec_attached_total = 0
        self._tls_error_total = 0

    @property
    def metric_name(self) -> str:
        return "tcp_stream.start_tls.attempts"

    def handle_event(self, event: MetricEvent) -> bool:
        if not isinstance(event, TcpStreamStartTlsAttemptMetricEvent):
            return False

        self._total += 1

        if event.outcome is TcpStreamStartTlsAttemptOutcome.SUCCESS:
            self._success_total += 1

        elif event.outcome is TcpStreamStartTlsAttemptOutcome.FAILURE:
            self._failure_total += 1

        elif event.outcome is TcpStreamStartTlsAttemptOutcome.CANCELLED:
            self._cancelled_total += 1

        elif event.outcome is TcpStreamStartTlsAttemptOutcome.TIMEOUT:
            self._timeout_total += 1

        elif event.outcome is TcpStreamStartTlsAttemptOutcome.REFUSED_NOT_OPENED:
            self._refused_not_opened_total += 1

        elif event.outcome is TcpStreamStartTlsAttemptOutcome.REFUSED_ALREADY_UNDER_SSL:
            self._refused_already_under_ssl_total += 1

        elif event.outcome is TcpStreamStartTlsAttemptOutcome.REFUSED_START_TLS_ALREADY_ACTIVE:
            self._refused_start_tls_already_active_total += 1

        elif event.outcome is TcpStreamStartTlsAttemptOutcome.REFUSED_CRYPTO_CODEC_ATTACHED:
            self._refused_crypto_codec_attached_total += 1

        elif event.outcome is TcpStreamStartTlsAttemptOutcome.TLS_ERROR:
            self._tls_error_total += 1

        return True

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "total": self._total,
                "success_total": self._success_total,
                "failure_total": self._failure_total,
                "cancelled_total": self._cancelled_total,
                "timeout_total": self._timeout_total,
                "refused_not_opened_total": self._refused_not_opened_total,
                "refused_already_under_ssl_total": self._refused_already_under_ssl_total,
                "refused_start_tls_already_active_total": (
                    self._refused_start_tls_already_active_total
                ),
                "refused_crypto_codec_attached_total": self._refused_crypto_codec_attached_total,
                "tls_error_total": self._tls_error_total,
            },
        }


# ---- Crypto Codec Attach metric ----------------------------------------------------------

# tcp_stream.crypto_codec.attach.attempts_total
# tcp_stream.crypto_codec.attach.success_total
# tcp_stream.crypto_codec.attach.failure_total
# tcp_stream.crypto_codec.attach.refused_not_opened_total
# tcp_stream.crypto_codec.attach.refused_already_under_ssl_total
# tcp_stream.crypto_codec.attach.refused_start_tls_active_total
# tcp_stream.crypto_codec.attach.refused_already_attached_total


class TcpStreamCryptoCodecAttachAttemptOutcome(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    REFUSED_NOT_OPENED = "REFUSED_NOT_OPENED"
    REFUSED_ALREADY_UNDER_SSL = "REFUSED_ALREADY_UNDER_SSL"
    REFUSED_START_TLS_ACTIVE = "REFUSED_START_TLS_ACTIVE"
    REFUSED_ALREADY_ATTACHED = "REFUSED_ALREADY_ATTACHED"


@dataclass(frozen=True, slots=True)
class TcpStreamCryptoCodecAttachAttemptMetricEvent(MetricEvent):
    outcome: TcpStreamCryptoCodecAttachAttemptOutcome

    @property
    def event_type(self) -> str:
        return "tcp_stream.crypto_codec.attach.attempt"


class TcpStreamCryptoCodecAttachAttemptsMetric(Metric):
    def __init__(self) -> None:
        self._total = 0
        self._success_total = 0
        self._failure_total = 0
        self._refused_not_opened_total = 0
        self._refused_already_under_ssl_total = 0
        self._refused_start_tls_active_total = 0
        self._refused_already_attached_total = 0

    @property
    def metric_name(self) -> str:
        return "tcp_stream.crypto_codec.attach.attempts"

    def handle_event(self, event: MetricEvent) -> bool:
        if not isinstance(event, TcpStreamCryptoCodecAttachAttemptMetricEvent):
            return False

        self._total += 1

        if event.outcome is TcpStreamCryptoCodecAttachAttemptOutcome.SUCCESS:
            self._success_total += 1

        elif event.outcome is TcpStreamCryptoCodecAttachAttemptOutcome.FAILURE:
            self._failure_total += 1

        elif event.outcome is TcpStreamCryptoCodecAttachAttemptOutcome.REFUSED_NOT_OPENED:
            self._refused_not_opened_total += 1

        elif event.outcome is TcpStreamCryptoCodecAttachAttemptOutcome.REFUSED_ALREADY_UNDER_SSL:
            self._refused_already_under_ssl_total += 1

        elif event.outcome is TcpStreamCryptoCodecAttachAttemptOutcome.REFUSED_START_TLS_ACTIVE:
            self._refused_start_tls_active_total += 1

        elif event.outcome is TcpStreamCryptoCodecAttachAttemptOutcome.REFUSED_ALREADY_ATTACHED:
            self._refused_already_attached_total += 1

        return True

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "total": self._total,
                "success_total": self._success_total,
                "failure_total": self._failure_total,
                "refused_not_opened_total": self._refused_not_opened_total,
                "refused_already_under_ssl_total": self._refused_already_under_ssl_total,
                "refused_start_tls_active_total": self._refused_start_tls_active_total,
                "refused_already_attached_total": self._refused_already_attached_total,
            },
        }


# ---- Crypto Codec Detach metric ----------------------------------------------------------

# tcp_stream.crypto_codec.detach.attempts_total
# tcp_stream.crypto_codec.detach.success_total
# tcp_stream.crypto_codec.detach.failure_total
# tcp_stream.crypto_codec.detach.refused_not_opened_total
# tcp_stream.crypto_codec.detach.refused_not_attached_total


class TcpStreamCryptoCodecDetachAttemptOutcome(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    REFUSED_NOT_OPENED = "REFUSED_NOT_OPENED"
    REFUSED_NOT_ATTACHED = "REFUSED_NOT_ATTACHED"


@dataclass(frozen=True, slots=True)
class TcpStreamCryptoCodecDetachAttemptMetricEvent(MetricEvent):
    outcome: TcpStreamCryptoCodecDetachAttemptOutcome

    @property
    def event_type(self) -> str:
        return "tcp_stream.crypto_codec.detach.attempt"


class TcpStreamCryptoCodecDetachAttemptsMetric(Metric):
    def __init__(self) -> None:
        self._total = 0
        self._success_total = 0
        self._failure_total = 0
        self._refused_not_opened_total = 0
        self._refused_not_attached_total = 0

    @property
    def metric_name(self) -> str:
        return "tcp_stream.crypto_codec.detach.attempts"

    def handle_event(self, event: MetricEvent) -> bool:
        if not isinstance(event, TcpStreamCryptoCodecDetachAttemptMetricEvent):
            return False

        self._total += 1

        if event.outcome is TcpStreamCryptoCodecDetachAttemptOutcome.SUCCESS:
            self._success_total += 1

        elif event.outcome is TcpStreamCryptoCodecDetachAttemptOutcome.FAILURE:
            self._failure_total += 1

        elif event.outcome is TcpStreamCryptoCodecDetachAttemptOutcome.REFUSED_NOT_OPENED:
            self._refused_not_opened_total += 1

        elif event.outcome is TcpStreamCryptoCodecDetachAttemptOutcome.REFUSED_NOT_ATTACHED:
            self._refused_not_attached_total += 1

        return True

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "total": self._total,
                "success_total": self._success_total,
                "failure_total": self._failure_total,
                "refused_not_opened_total": self._refused_not_opened_total,
                "refused_not_attached_total": self._refused_not_attached_total,
            },
        }


# ---- Stream Read metric ------------------------------------------------------------------

# tcp_stream.stream_read.attempts_total
# tcp_stream.stream_read.success_total
# tcp_stream.stream_read.timeout_total
# tcp_stream.stream_read.error_total
# tcp_stream.stream_read.cancelled_total
# tcp_stream.stream_read.tls_error_total


class TcpStreamStreamReadAttemptOutcome(StrEnum):
    SUCCESS = "SUCCESS"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"
    TLS_ERROR = "TLS_ERROR"


@dataclass(frozen=True, slots=True)
class TcpStreamStreamReadAttemptMetricEvent(MetricEvent):
    outcome: TcpStreamStreamReadAttemptOutcome

    @property
    def event_type(self) -> str:
        return "tcp_stream.stream_read.attempt"


class TcpStreamStreamReadAttemptsMetric(Metric):
    def __init__(self) -> None:
        self._total = 0
        self._success_total = 0
        self._timeout_total = 0
        self._error_total = 0
        self._cancelled_total = 0
        self._tls_error_total = 0

    @property
    def metric_name(self) -> str:
        return "tcp_stream.stream_read.attempts"

    def handle_event(self, event: MetricEvent) -> bool:
        if not isinstance(event, TcpStreamStreamReadAttemptMetricEvent):
            return False

        self._total += 1

        if event.outcome is TcpStreamStreamReadAttemptOutcome.SUCCESS:
            self._success_total += 1

        elif event.outcome is TcpStreamStreamReadAttemptOutcome.TIMEOUT:
            self._timeout_total += 1

        elif event.outcome is TcpStreamStreamReadAttemptOutcome.ERROR:
            self._error_total += 1

        elif event.outcome is TcpStreamStreamReadAttemptOutcome.CANCELLED:
            self._cancelled_total += 1

        elif event.outcome is TcpStreamStreamReadAttemptOutcome.TLS_ERROR:
            self._tls_error_total += 1

        return True

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "total": self._total,
                "success_total": self._success_total,
                "timeout_total": self._timeout_total,
                "error_total": self._error_total,
                "cancelled_total": self._cancelled_total,
                "tls_error_total": self._tls_error_total,
            },
        }


# ---- Stream Write metric -----------------------------------------------------------------

# tcp_stream.stream_write.attempts_total
# tcp_stream.stream_write.success_total
# tcp_stream.stream_write.error_total
# tcp_stream.stream_write.tls_error_total


class TcpStreamStreamWriteAttemptOutcome(StrEnum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    TLS_ERROR = "TLS_ERROR"


@dataclass(frozen=True, slots=True)
class TcpStreamStreamWriteAttemptMetricEvent(MetricEvent):
    outcome: TcpStreamStreamWriteAttemptOutcome

    @property
    def event_type(self) -> str:
        return "tcp_stream.stream_write.attempt"


class TcpStreamStreamWriteAttemptsMetric(Metric):
    def __init__(self) -> None:
        self._total = 0
        self._success_total = 0
        self._error_total = 0
        self._tls_error_total = 0

    @property
    def metric_name(self) -> str:
        return "tcp_stream.stream_write.attempts"

    def handle_event(self, event: MetricEvent) -> bool:
        if not isinstance(event, TcpStreamStreamWriteAttemptMetricEvent):
            return False

        self._total += 1

        if event.outcome is TcpStreamStreamWriteAttemptOutcome.SUCCESS:
            self._success_total += 1

        elif event.outcome is TcpStreamStreamWriteAttemptOutcome.ERROR:
            self._error_total += 1

        elif event.outcome is TcpStreamStreamWriteAttemptOutcome.TLS_ERROR:
            self._tls_error_total += 1

        return True

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "total": self._total,
                "success_total": self._success_total,
                "error_total": self._error_total,
                "tls_error_total": self._tls_error_total,
            },
        }


# ---- Drain metric ------------------------------------------------------------------------

# tcp_stream.drain.attempts_total
# tcp_stream.drain.success_total
# tcp_stream.drain.timeout_total
# tcp_stream.drain.error_total
# tcp_stream.drain.cancelled_total
# tcp_stream.drain.tls_error_total


class TcpStreamDrainAttemptOutcome(StrEnum):
    SUCCESS = "SUCCESS"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"
    TLS_ERROR = "TLS_ERROR"


@dataclass(frozen=True, slots=True)
class TcpStreamDrainAttemptMetricEvent(MetricEvent):
    outcome: TcpStreamDrainAttemptOutcome

    @property
    def event_type(self) -> str:
        return "tcp_stream.drain.attempt"


class TcpStreamDrainAttemptsMetric(Metric):
    def __init__(self) -> None:
        self._total = 0
        self._success_total = 0
        self._timeout_total = 0
        self._error_total = 0
        self._cancelled_total = 0
        self._tls_error_total = 0

    @property
    def metric_name(self) -> str:
        return "tcp_stream.drain.attempts"

    def handle_event(self, event: MetricEvent) -> bool:
        if not isinstance(event, TcpStreamDrainAttemptMetricEvent):
            return False

        self._total += 1

        if event.outcome is TcpStreamDrainAttemptOutcome.SUCCESS:
            self._success_total += 1

        elif event.outcome is TcpStreamDrainAttemptOutcome.TIMEOUT:
            self._timeout_total += 1

        elif event.outcome is TcpStreamDrainAttemptOutcome.ERROR:
            self._error_total += 1

        elif event.outcome is TcpStreamDrainAttemptOutcome.CANCELLED:
            self._cancelled_total += 1

        elif event.outcome is TcpStreamDrainAttemptOutcome.TLS_ERROR:
            self._tls_error_total += 1

        return True

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "total": self._total,
                "success_total": self._success_total,
                "timeout_total": self._timeout_total,
                "error_total": self._error_total,
                "cancelled_total": self._cancelled_total,
                "tls_error_total": self._tls_error_total,
            },
        }


# ---- Bytes Received metric ---------------------------------------------------------------

# tcp_stream.bytes.received_total


@dataclass(frozen=True, slots=True)
class TcpStreamBytesReceivedMetricEvent(MetricEvent):
    size: int

    @property
    def event_type(self) -> str:
        return "tcp_stream.bytes.received"


class TcpStreamBytesReceivedMetric(Metric):
    def __init__(self) -> None:
        self._total = 0

    @property
    def metric_name(self) -> str:
        return "tcp_stream.bytes.received"

    def handle_event(self, event: MetricEvent) -> bool:
        if not isinstance(event, TcpStreamBytesReceivedMetricEvent):
            return False

        self._total += event.size
        return True

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "total": self._total,
            },
        }


# ---- Bytes Sent metric -------------------------------------------------------------------

# tcp_stream.bytes.sent_total


@dataclass(frozen=True, slots=True)
class TcpStreamBytesSentMetricEvent(MetricEvent):
    size: int

    @property
    def event_type(self) -> str:
        return "tcp_stream.bytes.sent"


class TcpStreamBytesSentMetric(Metric):
    def __init__(self) -> None:
        self._total = 0

    @property
    def metric_name(self) -> str:
        return "tcp_stream.bytes.sent"

    def handle_event(self, event: MetricEvent) -> bool:
        if not isinstance(event, TcpStreamBytesSentMetricEvent):
            return False

        self._total += event.size
        return True

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "total": self._total,
            },
        }


# ---- Remote Disconnect metric ------------------------------------------------------------

# tcp_stream.remote_disconnect_total


@dataclass(frozen=True, slots=True)
class TcpStreamRemoteDisconnectMetricEvent(MetricEvent):
    @property
    def event_type(self) -> str:
        return "tcp_stream.remote_disconnect"


class TcpStreamRemoteDisconnectMetric(Metric):
    def __init__(self) -> None:
        self._total = 0

    @property
    def metric_name(self) -> str:
        return "tcp_stream.remote_disconnect"

    def handle_event(self, event: MetricEvent) -> bool:
        if not isinstance(event, TcpStreamRemoteDisconnectMetricEvent):
            return False

        self._total += 1
        return True

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "total": self._total,
            },
        }


# ---- Abortive Close metric ---------------------------------------------------------------

# tcp_stream.abortive_close_total


@dataclass(frozen=True, slots=True)
class TcpStreamAbortiveCloseMetricEvent(MetricEvent):
    @property
    def event_type(self) -> str:
        return "tcp_stream.abortive_close"


class TcpStreamAbortiveCloseMetric(Metric):
    def __init__(self) -> None:
        self._total = 0

    @property
    def metric_name(self) -> str:
        return "tcp_stream.abortive_close"

    def handle_event(self, event: MetricEvent) -> bool:
        if not isinstance(event, TcpStreamAbortiveCloseMetricEvent):
            return False

        self._total += 1
        return True

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "total": self._total,
            },
        }
