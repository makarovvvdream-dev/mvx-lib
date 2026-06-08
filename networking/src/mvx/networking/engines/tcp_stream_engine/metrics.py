# src/mvx/networking/engines/tcp_stream_engine/metrics.py
from __future__ import annotations

from typing import Any, Mapping

from mvx.common.metrics import Metric, MetricEvent

from .metric_events import (
    TcpStreamCloseMetricEvent,
    TcpStreamCloseResult,
    TcpStreamCryptoCodecAttachMetricEvent,
    TcpStreamCryptoCodecAttachResult,
    TcpStreamCryptoCodecDetachMetricEvent,
    TcpStreamCryptoCodecDetachResult,
    TcpStreamDrainMetricEvent,
    TcpStreamDrainResult,
    TcpStreamOpenMetricEvent,
    TcpStreamOpenResult,
    TcpStreamStartTlsMetricEvent,
    TcpStreamStartTlsResult,
    TcpStreamStreamReadMetricEvent,
    TcpStreamStreamReadResult,
    TcpStreamStreamWriteMetricEvent,
    TcpStreamStreamWriteResult,
)

__all__ = (
    "TcpStreamOperationAttemptsMetric",
    "TcpStreamOperationLatencyMetric",
    "TcpStreamIoBytesMetric",
    "TcpStreamRemoteDisconnectMetric",
    "TcpStreamAbortiveCloseMetric",
)


def _average_or_zero(*, total: int, count: int) -> int:
    if count == 0:
        return 0

    return total // count


# ---- Operation attempts metric -----------------------------------------------------------
#
# Metric:
#   tcp_stream.operation.attempts
#
# Dimensions:
#   open_total
#   open_success_total
#   open_already_opened_total
#   open_failure_total
#   open_cancelled_total
#   open_plain_success_total
#   open_ssl_success_total
#
#   close_total
#   close_success_total
#   close_not_opened_total
#   close_failure_total
#   close_cancelled_total
#
#   start_tls_total
#   start_tls_success_total
#   start_tls_failure_total
#   start_tls_cancelled_total
#   start_tls_timeout_total
#   start_tls_refused_not_opened_total
#   start_tls_refused_already_under_ssl_total
#   start_tls_refused_start_tls_already_active_total
#   start_tls_refused_crypto_codec_attached_total
#   start_tls_tls_error_total
#
#   crypto_codec_attach_total
#   crypto_codec_attach_success_total
#   crypto_codec_attach_failure_total
#   crypto_codec_attach_cancelled_total
#   crypto_codec_attach_refused_not_opened_total
#   crypto_codec_attach_refused_already_under_ssl_total
#   crypto_codec_attach_refused_start_tls_active_total
#   crypto_codec_attach_refused_already_attached_total
#
#   crypto_codec_detach_total
#   crypto_codec_detach_success_total
#   crypto_codec_detach_failure_total
#   crypto_codec_detach_cancelled_total
#   crypto_codec_detach_refused_not_opened_total
#   crypto_codec_detach_refused_not_attached_total
#
#   stream_read_total
#   stream_read_success_total
#   stream_read_timeout_total
#   stream_read_error_total
#   stream_read_cancelled_total
#   stream_read_tls_error_total
#   stream_read_remote_disconnect_total
#
#   stream_write_total
#   stream_write_success_total
#   stream_write_error_total
#   stream_write_tls_error_total
#
#   drain_total
#   drain_success_total
#   drain_timeout_total
#   drain_error_total
#   drain_cancelled_total
#   drain_tls_error_total


class TcpStreamOperationAttemptsMetric(Metric):
    def __init__(self) -> None:
        self._open_total = 0
        self._open_success_total = 0
        self._open_already_opened_total = 0
        self._open_failure_total = 0
        self._open_cancelled_total = 0
        self._open_plain_success_total = 0
        self._open_ssl_success_total = 0

        self._close_total = 0
        self._close_success_total = 0
        self._close_not_opened_total = 0
        self._close_failure_total = 0
        self._close_cancelled_total = 0

        self._start_tls_total = 0
        self._start_tls_success_total = 0
        self._start_tls_failure_total = 0
        self._start_tls_cancelled_total = 0
        self._start_tls_timeout_total = 0
        self._start_tls_refused_not_opened_total = 0
        self._start_tls_refused_already_under_ssl_total = 0
        self._start_tls_refused_start_tls_already_active_total = 0
        self._start_tls_refused_crypto_codec_attached_total = 0
        self._start_tls_tls_error_total = 0

        self._crypto_codec_attach_total = 0
        self._crypto_codec_attach_success_total = 0
        self._crypto_codec_attach_failure_total = 0
        self._crypto_codec_attach_cancelled_total = 0
        self._crypto_codec_attach_refused_not_opened_total = 0
        self._crypto_codec_attach_refused_already_under_ssl_total = 0
        self._crypto_codec_attach_refused_start_tls_active_total = 0
        self._crypto_codec_attach_refused_already_attached_total = 0

        self._crypto_codec_detach_total = 0
        self._crypto_codec_detach_success_total = 0
        self._crypto_codec_detach_failure_total = 0
        self._crypto_codec_detach_cancelled_total = 0
        self._crypto_codec_detach_refused_not_opened_total = 0
        self._crypto_codec_detach_refused_not_attached_total = 0

        self._stream_read_total = 0
        self._stream_read_success_total = 0
        self._stream_read_timeout_total = 0
        self._stream_read_error_total = 0
        self._stream_read_cancelled_total = 0
        self._stream_read_tls_error_total = 0
        self._stream_read_remote_disconnect_total = 0

        self._stream_write_total = 0
        self._stream_write_success_total = 0
        self._stream_write_error_total = 0
        self._stream_write_tls_error_total = 0

        self._drain_total = 0
        self._drain_success_total = 0
        self._drain_timeout_total = 0
        self._drain_error_total = 0
        self._drain_cancelled_total = 0
        self._drain_tls_error_total = 0

    @property
    def metric_name(self) -> str:
        return "tcp_stream.operation.attempts"

    def handle_event(self, event: MetricEvent) -> bool:
        if isinstance(event, TcpStreamOpenMetricEvent):
            self._handle_open_event(event)
            return True

        if isinstance(event, TcpStreamCloseMetricEvent):
            self._handle_close_event(event)
            return True

        if isinstance(event, TcpStreamStartTlsMetricEvent):
            self._handle_start_tls_event(event)
            return True

        if isinstance(event, TcpStreamCryptoCodecAttachMetricEvent):
            self._handle_crypto_codec_attach_event(event)
            return True

        if isinstance(event, TcpStreamCryptoCodecDetachMetricEvent):
            self._handle_crypto_codec_detach_event(event)
            return True

        if isinstance(event, TcpStreamStreamReadMetricEvent):
            self._handle_stream_read_event(event)
            return True

        if isinstance(event, TcpStreamStreamWriteMetricEvent):
            self._handle_stream_write_event(event)
            return True

        if isinstance(event, TcpStreamDrainMetricEvent):
            self._handle_drain_event(event)
            return True

        return False

    def _handle_open_event(self, event: TcpStreamOpenMetricEvent) -> None:
        self._open_total += 1

        if event.result is TcpStreamOpenResult.SUCCEEDED:
            self._open_success_total += 1

            if event.use_ssl:
                self._open_ssl_success_total += 1
            else:
                self._open_plain_success_total += 1

        elif event.result is TcpStreamOpenResult.ALREADY_OPENED:
            self._open_already_opened_total += 1

        elif event.result is TcpStreamOpenResult.FAILED:
            self._open_failure_total += 1

        elif event.result is TcpStreamOpenResult.CANCELLED:
            self._open_cancelled_total += 1

    def _handle_close_event(self, event: TcpStreamCloseMetricEvent) -> None:
        self._close_total += 1

        if event.result is TcpStreamCloseResult.SUCCEEDED:
            self._close_success_total += 1

        elif event.result is TcpStreamCloseResult.NOT_OPENED:
            self._close_not_opened_total += 1

        elif event.result is TcpStreamCloseResult.FAILED:
            self._close_failure_total += 1

        elif event.result is TcpStreamCloseResult.CANCELLED:
            self._close_cancelled_total += 1

    def _handle_start_tls_event(self, event: TcpStreamStartTlsMetricEvent) -> None:
        self._start_tls_total += 1

        if event.result is TcpStreamStartTlsResult.SUCCEEDED:
            self._start_tls_success_total += 1

        elif event.result is TcpStreamStartTlsResult.FAILED:
            self._start_tls_failure_total += 1

        elif event.result is TcpStreamStartTlsResult.CANCELLED:
            self._start_tls_cancelled_total += 1

        elif event.result is TcpStreamStartTlsResult.TIMED_OUT:
            self._start_tls_timeout_total += 1

        elif event.result is TcpStreamStartTlsResult.REFUSED_NOT_OPENED:
            self._start_tls_refused_not_opened_total += 1

        elif event.result is TcpStreamStartTlsResult.REFUSED_ALREADY_UNDER_SSL:
            self._start_tls_refused_already_under_ssl_total += 1

        elif event.result is TcpStreamStartTlsResult.REFUSED_START_TLS_ALREADY_ACTIVE:
            self._start_tls_refused_start_tls_already_active_total += 1

        elif event.result is TcpStreamStartTlsResult.REFUSED_CRYPTO_CODEC_ATTACHED:
            self._start_tls_refused_crypto_codec_attached_total += 1

        elif event.result is TcpStreamStartTlsResult.TLS_FAILED:
            self._start_tls_tls_error_total += 1

    def _handle_crypto_codec_attach_event(
        self,
        event: TcpStreamCryptoCodecAttachMetricEvent,
    ) -> None:
        self._crypto_codec_attach_total += 1

        if event.result is TcpStreamCryptoCodecAttachResult.SUCCEEDED:
            self._crypto_codec_attach_success_total += 1

        elif event.result is TcpStreamCryptoCodecAttachResult.FAILED:
            self._crypto_codec_attach_failure_total += 1

        elif event.result is TcpStreamCryptoCodecAttachResult.CANCELLED:
            self._crypto_codec_attach_cancelled_total += 1

        elif event.result is TcpStreamCryptoCodecAttachResult.REFUSED_NOT_OPENED:
            self._crypto_codec_attach_refused_not_opened_total += 1

        elif event.result is TcpStreamCryptoCodecAttachResult.REFUSED_ALREADY_UNDER_SSL:
            self._crypto_codec_attach_refused_already_under_ssl_total += 1

        elif event.result is TcpStreamCryptoCodecAttachResult.REFUSED_START_TLS_ACTIVE:
            self._crypto_codec_attach_refused_start_tls_active_total += 1

        elif event.result is TcpStreamCryptoCodecAttachResult.REFUSED_ALREADY_ATTACHED:
            self._crypto_codec_attach_refused_already_attached_total += 1

    def _handle_crypto_codec_detach_event(
        self,
        event: TcpStreamCryptoCodecDetachMetricEvent,
    ) -> None:
        self._crypto_codec_detach_total += 1

        if event.result is TcpStreamCryptoCodecDetachResult.SUCCEEDED:
            self._crypto_codec_detach_success_total += 1

        elif event.result is TcpStreamCryptoCodecDetachResult.FAILED:
            self._crypto_codec_detach_failure_total += 1

        elif event.result is TcpStreamCryptoCodecDetachResult.CANCELLED:
            self._crypto_codec_detach_cancelled_total += 1

        elif event.result is TcpStreamCryptoCodecDetachResult.REFUSED_NOT_OPENED:
            self._crypto_codec_detach_refused_not_opened_total += 1

        elif event.result is TcpStreamCryptoCodecDetachResult.REFUSED_NOT_ATTACHED:
            self._crypto_codec_detach_refused_not_attached_total += 1

    def _handle_stream_read_event(self, event: TcpStreamStreamReadMetricEvent) -> None:
        self._stream_read_total += 1

        if event.result is TcpStreamStreamReadResult.SUCCEEDED:
            self._stream_read_success_total += 1

        elif event.result is TcpStreamStreamReadResult.TIMED_OUT:
            self._stream_read_timeout_total += 1

        elif event.result is TcpStreamStreamReadResult.FAILED:
            self._stream_read_error_total += 1

        elif event.result is TcpStreamStreamReadResult.CANCELLED:
            self._stream_read_cancelled_total += 1

        elif event.result is TcpStreamStreamReadResult.TLS_FAILED:
            self._stream_read_tls_error_total += 1

        elif event.result is TcpStreamStreamReadResult.REMOTE_DISCONNECTED:
            self._stream_read_remote_disconnect_total += 1

    def _handle_stream_write_event(self, event: TcpStreamStreamWriteMetricEvent) -> None:
        self._stream_write_total += 1

        if event.result is TcpStreamStreamWriteResult.SUCCEEDED:
            self._stream_write_success_total += 1

        elif event.result is TcpStreamStreamWriteResult.FAILED:
            self._stream_write_error_total += 1

        elif event.result is TcpStreamStreamWriteResult.TLS_FAILED:
            self._stream_write_tls_error_total += 1

    def _handle_drain_event(self, event: TcpStreamDrainMetricEvent) -> None:
        self._drain_total += 1

        if event.result is TcpStreamDrainResult.SUCCEEDED:
            self._drain_success_total += 1

        elif event.result is TcpStreamDrainResult.TIMED_OUT:
            self._drain_timeout_total += 1

        elif event.result is TcpStreamDrainResult.FAILED:
            self._drain_error_total += 1

        elif event.result is TcpStreamDrainResult.CANCELLED:
            self._drain_cancelled_total += 1

        elif event.result is TcpStreamDrainResult.TLS_FAILED:
            self._drain_tls_error_total += 1

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "open_total": self._open_total,
                "open_success_total": self._open_success_total,
                "open_already_opened_total": self._open_already_opened_total,
                "open_failure_total": self._open_failure_total,
                "open_cancelled_total": self._open_cancelled_total,
                "open_plain_success_total": self._open_plain_success_total,
                "open_ssl_success_total": self._open_ssl_success_total,
                "close_total": self._close_total,
                "close_success_total": self._close_success_total,
                "close_not_opened_total": self._close_not_opened_total,
                "close_failure_total": self._close_failure_total,
                "close_cancelled_total": self._close_cancelled_total,
                "start_tls_total": self._start_tls_total,
                "start_tls_success_total": self._start_tls_success_total,
                "start_tls_failure_total": self._start_tls_failure_total,
                "start_tls_cancelled_total": self._start_tls_cancelled_total,
                "start_tls_timeout_total": self._start_tls_timeout_total,
                "start_tls_refused_not_opened_total": self._start_tls_refused_not_opened_total,
                "start_tls_refused_already_under_ssl_total": (
                    self._start_tls_refused_already_under_ssl_total
                ),
                "start_tls_refused_start_tls_already_active_total": (
                    self._start_tls_refused_start_tls_already_active_total
                ),
                "start_tls_refused_crypto_codec_attached_total": (
                    self._start_tls_refused_crypto_codec_attached_total
                ),
                "start_tls_tls_error_total": self._start_tls_tls_error_total,
                "crypto_codec_attach_total": self._crypto_codec_attach_total,
                "crypto_codec_attach_success_total": self._crypto_codec_attach_success_total,
                "crypto_codec_attach_failure_total": self._crypto_codec_attach_failure_total,
                "crypto_codec_attach_cancelled_total": self._crypto_codec_attach_cancelled_total,
                "crypto_codec_attach_refused_not_opened_total": (
                    self._crypto_codec_attach_refused_not_opened_total
                ),
                "crypto_codec_attach_refused_already_under_ssl_total": (
                    self._crypto_codec_attach_refused_already_under_ssl_total
                ),
                "crypto_codec_attach_refused_start_tls_active_total": (
                    self._crypto_codec_attach_refused_start_tls_active_total
                ),
                "crypto_codec_attach_refused_already_attached_total": (
                    self._crypto_codec_attach_refused_already_attached_total
                ),
                "crypto_codec_detach_total": self._crypto_codec_detach_total,
                "crypto_codec_detach_success_total": self._crypto_codec_detach_success_total,
                "crypto_codec_detach_failure_total": self._crypto_codec_detach_failure_total,
                "crypto_codec_detach_cancelled_total": self._crypto_codec_detach_cancelled_total,
                "crypto_codec_detach_refused_not_opened_total": (
                    self._crypto_codec_detach_refused_not_opened_total
                ),
                "crypto_codec_detach_refused_not_attached_total": (
                    self._crypto_codec_detach_refused_not_attached_total
                ),
                "stream_read_total": self._stream_read_total,
                "stream_read_success_total": self._stream_read_success_total,
                "stream_read_timeout_total": self._stream_read_timeout_total,
                "stream_read_error_total": self._stream_read_error_total,
                "stream_read_cancelled_total": self._stream_read_cancelled_total,
                "stream_read_tls_error_total": self._stream_read_tls_error_total,
                "stream_read_remote_disconnect_total": self._stream_read_remote_disconnect_total,
                "stream_write_total": self._stream_write_total,
                "stream_write_success_total": self._stream_write_success_total,
                "stream_write_error_total": self._stream_write_error_total,
                "stream_write_tls_error_total": self._stream_write_tls_error_total,
                "drain_total": self._drain_total,
                "drain_success_total": self._drain_success_total,
                "drain_timeout_total": self._drain_timeout_total,
                "drain_error_total": self._drain_error_total,
                "drain_cancelled_total": self._drain_cancelled_total,
                "drain_tls_error_total": self._drain_tls_error_total,
            },
        }


# ---- Operation latency metric ------------------------------------------------------------
#
# Metric:
#   tcp_stream.operation.latency
#
# Dimensions:
#   open_success_latency_average_ns
#   open_success_latency_max_ns
#   close_success_latency_average_ns
#   close_success_latency_max_ns
#   start_tls_success_latency_average_ns
#   start_tls_success_latency_max_ns
#   crypto_codec_attach_success_latency_average_ns
#   crypto_codec_attach_success_latency_max_ns
#   crypto_codec_detach_success_latency_average_ns
#   crypto_codec_detach_success_latency_max_ns
#   stream_read_success_latency_average_ns
#   stream_read_success_latency_max_ns
#   stream_write_success_latency_average_ns
#   stream_write_success_latency_max_ns
#   drain_success_latency_average_ns
#   drain_success_latency_max_ns


class TcpStreamOperationLatencyMetric(Metric):
    def __init__(self) -> None:
        self._open_success_total = 0
        self._open_success_latency_total_ns = 0
        self._open_success_latency_max_ns = 0

        self._close_success_total = 0
        self._close_success_latency_total_ns = 0
        self._close_success_latency_max_ns = 0

        self._start_tls_success_total = 0
        self._start_tls_success_latency_total_ns = 0
        self._start_tls_success_latency_max_ns = 0

        self._crypto_codec_attach_success_total = 0
        self._crypto_codec_attach_success_latency_total_ns = 0
        self._crypto_codec_attach_success_latency_max_ns = 0

        self._crypto_codec_detach_success_total = 0
        self._crypto_codec_detach_success_latency_total_ns = 0
        self._crypto_codec_detach_success_latency_max_ns = 0

        self._stream_read_success_total = 0
        self._stream_read_success_latency_total_ns = 0
        self._stream_read_success_latency_max_ns = 0

        self._stream_write_success_total = 0
        self._stream_write_success_latency_total_ns = 0
        self._stream_write_success_latency_max_ns = 0

        self._drain_success_total = 0
        self._drain_success_latency_total_ns = 0
        self._drain_success_latency_max_ns = 0

    @property
    def metric_name(self) -> str:
        return "tcp_stream.operation.latency"

    def handle_event(self, event: MetricEvent) -> bool:
        if isinstance(event, TcpStreamOpenMetricEvent):
            if event.result is not TcpStreamOpenResult.SUCCEEDED:
                return False
            self._open_success_total += 1
            self._open_success_latency_total_ns += event.duration_ns
            self._open_success_latency_max_ns = max(
                self._open_success_latency_max_ns,
                event.duration_ns,
            )
            return True

        if isinstance(event, TcpStreamCloseMetricEvent):
            if event.result is not TcpStreamCloseResult.SUCCEEDED:
                return False
            self._close_success_total += 1
            self._close_success_latency_total_ns += event.duration_ns
            self._close_success_latency_max_ns = max(
                self._close_success_latency_max_ns,
                event.duration_ns,
            )
            return True

        if isinstance(event, TcpStreamStartTlsMetricEvent):
            if event.result is not TcpStreamStartTlsResult.SUCCEEDED:
                return False
            self._start_tls_success_total += 1
            self._start_tls_success_latency_total_ns += event.duration_ns
            self._start_tls_success_latency_max_ns = max(
                self._start_tls_success_latency_max_ns,
                event.duration_ns,
            )
            return True

        if isinstance(event, TcpStreamCryptoCodecAttachMetricEvent):
            if event.result is not TcpStreamCryptoCodecAttachResult.SUCCEEDED:
                return False
            self._crypto_codec_attach_success_total += 1
            self._crypto_codec_attach_success_latency_total_ns += event.duration_ns
            self._crypto_codec_attach_success_latency_max_ns = max(
                self._crypto_codec_attach_success_latency_max_ns,
                event.duration_ns,
            )
            return True

        if isinstance(event, TcpStreamCryptoCodecDetachMetricEvent):
            if event.result is not TcpStreamCryptoCodecDetachResult.SUCCEEDED:
                return False
            self._crypto_codec_detach_success_total += 1
            self._crypto_codec_detach_success_latency_total_ns += event.duration_ns
            self._crypto_codec_detach_success_latency_max_ns = max(
                self._crypto_codec_detach_success_latency_max_ns,
                event.duration_ns,
            )
            return True

        if isinstance(event, TcpStreamStreamReadMetricEvent):
            if event.result is not TcpStreamStreamReadResult.SUCCEEDED:
                return False
            self._stream_read_success_total += 1
            self._stream_read_success_latency_total_ns += event.duration_ns
            self._stream_read_success_latency_max_ns = max(
                self._stream_read_success_latency_max_ns,
                event.duration_ns,
            )
            return True

        if isinstance(event, TcpStreamStreamWriteMetricEvent):
            if event.result is not TcpStreamStreamWriteResult.SUCCEEDED:
                return False
            self._stream_write_success_total += 1
            self._stream_write_success_latency_total_ns += event.duration_ns
            self._stream_write_success_latency_max_ns = max(
                self._stream_write_success_latency_max_ns,
                event.duration_ns,
            )
            return True

        if isinstance(event, TcpStreamDrainMetricEvent):
            if event.result is not TcpStreamDrainResult.SUCCEEDED:
                return False
            self._drain_success_total += 1
            self._drain_success_latency_total_ns += event.duration_ns
            self._drain_success_latency_max_ns = max(
                self._drain_success_latency_max_ns,
                event.duration_ns,
            )
            return True

        return False

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "open_success_latency_average_ns": _average_or_zero(
                    total=self._open_success_latency_total_ns,
                    count=self._open_success_total,
                ),
                "open_success_latency_max_ns": self._open_success_latency_max_ns,
                "close_success_latency_average_ns": _average_or_zero(
                    total=self._close_success_latency_total_ns,
                    count=self._close_success_total,
                ),
                "close_success_latency_max_ns": self._close_success_latency_max_ns,
                "start_tls_success_latency_average_ns": _average_or_zero(
                    total=self._start_tls_success_latency_total_ns,
                    count=self._start_tls_success_total,
                ),
                "start_tls_success_latency_max_ns": self._start_tls_success_latency_max_ns,
                "crypto_codec_attach_success_latency_average_ns": _average_or_zero(
                    total=self._crypto_codec_attach_success_latency_total_ns,
                    count=self._crypto_codec_attach_success_total,
                ),
                "crypto_codec_attach_success_latency_max_ns": (
                    self._crypto_codec_attach_success_latency_max_ns
                ),
                "crypto_codec_detach_success_latency_average_ns": _average_or_zero(
                    total=self._crypto_codec_detach_success_latency_total_ns,
                    count=self._crypto_codec_detach_success_total,
                ),
                "crypto_codec_detach_success_latency_max_ns": (
                    self._crypto_codec_detach_success_latency_max_ns
                ),
                "stream_read_success_latency_average_ns": _average_or_zero(
                    total=self._stream_read_success_latency_total_ns,
                    count=self._stream_read_success_total,
                ),
                "stream_read_success_latency_max_ns": self._stream_read_success_latency_max_ns,
                "stream_write_success_latency_average_ns": _average_or_zero(
                    total=self._stream_write_success_latency_total_ns,
                    count=self._stream_write_success_total,
                ),
                "stream_write_success_latency_max_ns": self._stream_write_success_latency_max_ns,
                "drain_success_latency_average_ns": _average_or_zero(
                    total=self._drain_success_latency_total_ns,
                    count=self._drain_success_total,
                ),
                "drain_success_latency_max_ns": self._drain_success_latency_max_ns,
            },
        }


# ---- I/O bytes metric --------------------------------------------------------------------
#
# Metric:
#   tcp_stream.io.bytes
#
# Dimensions:
#   received_total
#   sent_total
#   read_success_bytes_average
#   write_success_bytes_average


class TcpStreamIoBytesMetric(Metric):
    def __init__(self) -> None:
        self._received_total = 0
        self._sent_total = 0
        self._read_success_total = 0
        self._write_success_total = 0

    @property
    def metric_name(self) -> str:
        return "tcp_stream.io.bytes"

    def handle_event(self, event: MetricEvent) -> bool:
        if isinstance(event, TcpStreamStreamReadMetricEvent):
            if event.result is not TcpStreamStreamReadResult.SUCCEEDED:
                return False

            self._received_total += event.bytes_count
            self._read_success_total += 1
            return True

        if isinstance(event, TcpStreamStreamWriteMetricEvent):
            if event.result is not TcpStreamStreamWriteResult.SUCCEEDED:
                return False

            self._sent_total += event.bytes_count
            self._write_success_total += 1
            return True

        return False

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "received_total": self._received_total,
                "sent_total": self._sent_total,
                "read_success_bytes_average": _average_or_zero(
                    total=self._received_total,
                    count=self._read_success_total,
                ),
                "write_success_bytes_average": _average_or_zero(
                    total=self._sent_total,
                    count=self._write_success_total,
                ),
            },
        }


# ---- Remote disconnect metric ------------------------------------------------------------
#
# Metric:
#   tcp_stream.remote_disconnect
#
# Dimensions:
#   total


class TcpStreamRemoteDisconnectMetric(Metric):
    def __init__(self) -> None:
        self._total = 0

    @property
    def metric_name(self) -> str:
        return "tcp_stream.remote_disconnect"

    def handle_event(self, event: MetricEvent) -> bool:
        if not isinstance(event, TcpStreamStreamReadMetricEvent):
            return False

        if event.result is not TcpStreamStreamReadResult.REMOTE_DISCONNECTED:
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


# ---- Abortive close metric ---------------------------------------------------------------
#
# Metric:
#   tcp_stream.abortive_close
#
# Dimensions:
#   total


class TcpStreamAbortiveCloseMetric(Metric):
    def __init__(self) -> None:
        self._total = 0

    @property
    def metric_name(self) -> str:
        return "tcp_stream.abortive_close"

    def handle_event(self, event: MetricEvent) -> bool:
        if isinstance(event, TcpStreamStartTlsMetricEvent):
            if event.result not in (
                TcpStreamStartTlsResult.FAILED,
                TcpStreamStartTlsResult.TIMED_OUT,
                TcpStreamStartTlsResult.TLS_FAILED,
            ):
                return False

        elif isinstance(event, TcpStreamStreamReadMetricEvent):
            if event.result not in (
                TcpStreamStreamReadResult.FAILED,
                TcpStreamStreamReadResult.TLS_FAILED,
                TcpStreamStreamReadResult.REMOTE_DISCONNECTED,
            ):
                return False

        elif isinstance(event, TcpStreamDrainMetricEvent):
            if event.result not in (
                TcpStreamDrainResult.FAILED,
                TcpStreamDrainResult.TLS_FAILED,
            ):
                return False

        else:
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
