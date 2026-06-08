# tests/engines/tcp_stream_engine/test_metrics.py

"""
Tests for tcp_stream_engine out-of-the-box metrics.

Grouping rule:
  - Group a: operation attempts metric
  - Group b: operation latency metric
  - Group c: I/O bytes metric
  - Group d: remote disconnect metric
  - Group e: abortive close metric
  - Group f: cross-metric behavior

Naming rule:
  Each test name starts with test_<group><num>_, e.g. test_a1_...
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast
from enum import StrEnum

import pytest

from mvx.common.metrics import MetricEvent

from mvx.networking.engines.tcp_stream_engine.metric_events import (
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
from mvx.networking.engines.tcp_stream_engine.metrics import (
    TcpStreamAbortiveCloseMetric,
    TcpStreamIoBytesMetric,
    TcpStreamOperationAttemptsMetric,
    TcpStreamOperationLatencyMetric,
    TcpStreamRemoteDisconnectMetric,
)


@dataclass(frozen=True, slots=True)
class _UnrelatedMetricEvent(MetricEvent):
    @property
    def event_type(self) -> str:
        return "unrelated"


class _UnknownMetricResult(StrEnum):
    UNKNOWN = "UNKNOWN"


def _dimensions(metric: Any) -> dict[str, int]:
    snapshot = metric.snapshot()
    dimensions = snapshot["dimensions"]

    assert isinstance(dimensions, dict)

    # noinspection PyUnnecessaryCast
    return cast(dict[str, int], dimensions)


def _zero_dimensions(metric: Any) -> dict[str, int]:
    return {key: 0 for key in _dimensions(metric)}


def _finished_event(
    event: Any,
    result: Any,
    *,
    duration_ns: int = 10,
    bytes_count: int | None = None,
) -> MetricEvent:
    if bytes_count is None:
        # noinspection PyProtectedMember
        event._set_result(result)
    else:
        # noinspection PyProtectedMember
        event._set_result(result, bytes_count=bytes_count)

    # noinspection PyProtectedMember
    event._started_ns = 0
    # noinspection PyProtectedMember
    event._finished_ns = duration_ns

    return cast(MetricEvent, event)


# -------------------------
# Group a: operation attempts metric
# -------------------------


def test_a1_operation_attempts_metric_has_expected_identity_and_initial_snapshot() -> None:
    metric = TcpStreamOperationAttemptsMetric()

    assert metric.metric_name == "tcp_stream.operation.attempts"
    assert _dimensions(metric) == {
        "open_total": 0,
        "open_success_total": 0,
        "open_already_opened_total": 0,
        "open_failure_total": 0,
        "open_cancelled_total": 0,
        "open_plain_success_total": 0,
        "open_ssl_success_total": 0,
        "close_total": 0,
        "close_success_total": 0,
        "close_not_opened_total": 0,
        "close_failure_total": 0,
        "close_cancelled_total": 0,
        "start_tls_total": 0,
        "start_tls_success_total": 0,
        "start_tls_failure_total": 0,
        "start_tls_cancelled_total": 0,
        "start_tls_timeout_total": 0,
        "start_tls_refused_not_opened_total": 0,
        "start_tls_refused_already_under_ssl_total": 0,
        "start_tls_refused_start_tls_already_active_total": 0,
        "start_tls_refused_crypto_codec_attached_total": 0,
        "start_tls_tls_error_total": 0,
        "crypto_codec_attach_total": 0,
        "crypto_codec_attach_success_total": 0,
        "crypto_codec_attach_failure_total": 0,
        "crypto_codec_attach_cancelled_total": 0,
        "crypto_codec_attach_refused_not_opened_total": 0,
        "crypto_codec_attach_refused_already_under_ssl_total": 0,
        "crypto_codec_attach_refused_start_tls_active_total": 0,
        "crypto_codec_attach_refused_already_attached_total": 0,
        "crypto_codec_detach_total": 0,
        "crypto_codec_detach_success_total": 0,
        "crypto_codec_detach_failure_total": 0,
        "crypto_codec_detach_cancelled_total": 0,
        "crypto_codec_detach_refused_not_opened_total": 0,
        "crypto_codec_detach_refused_not_attached_total": 0,
        "stream_read_total": 0,
        "stream_read_success_total": 0,
        "stream_read_timeout_total": 0,
        "stream_read_error_total": 0,
        "stream_read_cancelled_total": 0,
        "stream_read_tls_error_total": 0,
        "stream_read_remote_disconnect_total": 0,
        "stream_write_total": 0,
        "stream_write_success_total": 0,
        "stream_write_error_total": 0,
        "stream_write_tls_error_total": 0,
        "drain_total": 0,
        "drain_success_total": 0,
        "drain_timeout_total": 0,
        "drain_error_total": 0,
        "drain_cancelled_total": 0,
        "drain_tls_error_total": 0,
    }


@pytest.mark.parametrize(
    ("event", "expected_changed"),
    [
        (
            _finished_event(
                TcpStreamOpenMetricEvent(use_ssl=False),
                TcpStreamOpenResult.SUCCEEDED,
            ),
            {
                "open_total": 1,
                "open_success_total": 1,
                "open_plain_success_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamOpenMetricEvent(use_ssl=True),
                TcpStreamOpenResult.SUCCEEDED,
            ),
            {
                "open_total": 1,
                "open_success_total": 1,
                "open_ssl_success_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamOpenMetricEvent(use_ssl=False),
                TcpStreamOpenResult.ALREADY_OPENED,
            ),
            {
                "open_total": 1,
                "open_already_opened_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamOpenMetricEvent(use_ssl=False),
                TcpStreamOpenResult.FAILED,
            ),
            {
                "open_total": 1,
                "open_failure_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamOpenMetricEvent(use_ssl=False),
                TcpStreamOpenResult.CANCELLED,
            ),
            {
                "open_total": 1,
                "open_cancelled_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamCloseMetricEvent(),
                TcpStreamCloseResult.SUCCEEDED,
            ),
            {
                "close_total": 1,
                "close_success_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamCloseMetricEvent(),
                TcpStreamCloseResult.NOT_OPENED,
            ),
            {
                "close_total": 1,
                "close_not_opened_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamCloseMetricEvent(),
                TcpStreamCloseResult.FAILED,
            ),
            {
                "close_total": 1,
                "close_failure_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamCloseMetricEvent(),
                TcpStreamCloseResult.CANCELLED,
            ),
            {
                "close_total": 1,
                "close_cancelled_total": 1,
            },
        ),
    ],
)
def test_a2_operation_attempts_metric_counts_open_and_close_events(
    event: MetricEvent,
    expected_changed: dict[str, int],
) -> None:
    metric = TcpStreamOperationAttemptsMetric()

    changed = metric.handle_event(event)

    expected = _zero_dimensions(metric)
    expected.update(expected_changed)

    assert changed is True
    assert _dimensions(metric) == expected


@pytest.mark.parametrize(
    ("event", "expected_changed"),
    [
        (
            _finished_event(
                TcpStreamStartTlsMetricEvent(),
                TcpStreamStartTlsResult.SUCCEEDED,
            ),
            {
                "start_tls_total": 1,
                "start_tls_success_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamStartTlsMetricEvent(),
                TcpStreamStartTlsResult.FAILED,
            ),
            {
                "start_tls_total": 1,
                "start_tls_failure_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamStartTlsMetricEvent(),
                TcpStreamStartTlsResult.CANCELLED,
            ),
            {
                "start_tls_total": 1,
                "start_tls_cancelled_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamStartTlsMetricEvent(),
                TcpStreamStartTlsResult.TIMED_OUT,
            ),
            {
                "start_tls_total": 1,
                "start_tls_timeout_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamStartTlsMetricEvent(),
                TcpStreamStartTlsResult.REFUSED_NOT_OPENED,
            ),
            {
                "start_tls_total": 1,
                "start_tls_refused_not_opened_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamStartTlsMetricEvent(),
                TcpStreamStartTlsResult.REFUSED_ALREADY_UNDER_SSL,
            ),
            {
                "start_tls_total": 1,
                "start_tls_refused_already_under_ssl_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamStartTlsMetricEvent(),
                TcpStreamStartTlsResult.REFUSED_START_TLS_ALREADY_ACTIVE,
            ),
            {
                "start_tls_total": 1,
                "start_tls_refused_start_tls_already_active_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamStartTlsMetricEvent(),
                TcpStreamStartTlsResult.REFUSED_CRYPTO_CODEC_ATTACHED,
            ),
            {
                "start_tls_total": 1,
                "start_tls_refused_crypto_codec_attached_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamStartTlsMetricEvent(),
                TcpStreamStartTlsResult.TLS_FAILED,
            ),
            {
                "start_tls_total": 1,
                "start_tls_tls_error_total": 1,
            },
        ),
    ],
)
def test_a3_operation_attempts_metric_counts_start_tls_events(
    event: MetricEvent,
    expected_changed: dict[str, int],
) -> None:
    metric = TcpStreamOperationAttemptsMetric()

    changed = metric.handle_event(event)

    expected = _zero_dimensions(metric)
    expected.update(expected_changed)

    assert changed is True
    assert _dimensions(metric) == expected


@pytest.mark.parametrize(
    ("event", "expected_changed"),
    [
        (
            _finished_event(
                TcpStreamCryptoCodecAttachMetricEvent(),
                TcpStreamCryptoCodecAttachResult.SUCCEEDED,
            ),
            {
                "crypto_codec_attach_total": 1,
                "crypto_codec_attach_success_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamCryptoCodecAttachMetricEvent(),
                TcpStreamCryptoCodecAttachResult.FAILED,
            ),
            {
                "crypto_codec_attach_total": 1,
                "crypto_codec_attach_failure_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamCryptoCodecAttachMetricEvent(),
                TcpStreamCryptoCodecAttachResult.CANCELLED,
            ),
            {
                "crypto_codec_attach_total": 1,
                "crypto_codec_attach_cancelled_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamCryptoCodecAttachMetricEvent(),
                TcpStreamCryptoCodecAttachResult.REFUSED_NOT_OPENED,
            ),
            {
                "crypto_codec_attach_total": 1,
                "crypto_codec_attach_refused_not_opened_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamCryptoCodecAttachMetricEvent(),
                TcpStreamCryptoCodecAttachResult.REFUSED_ALREADY_UNDER_SSL,
            ),
            {
                "crypto_codec_attach_total": 1,
                "crypto_codec_attach_refused_already_under_ssl_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamCryptoCodecAttachMetricEvent(),
                TcpStreamCryptoCodecAttachResult.REFUSED_START_TLS_ACTIVE,
            ),
            {
                "crypto_codec_attach_total": 1,
                "crypto_codec_attach_refused_start_tls_active_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamCryptoCodecAttachMetricEvent(),
                TcpStreamCryptoCodecAttachResult.REFUSED_ALREADY_ATTACHED,
            ),
            {
                "crypto_codec_attach_total": 1,
                "crypto_codec_attach_refused_already_attached_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamCryptoCodecDetachMetricEvent(),
                TcpStreamCryptoCodecDetachResult.SUCCEEDED,
            ),
            {
                "crypto_codec_detach_total": 1,
                "crypto_codec_detach_success_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamCryptoCodecDetachMetricEvent(),
                TcpStreamCryptoCodecDetachResult.FAILED,
            ),
            {
                "crypto_codec_detach_total": 1,
                "crypto_codec_detach_failure_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamCryptoCodecDetachMetricEvent(),
                TcpStreamCryptoCodecDetachResult.CANCELLED,
            ),
            {
                "crypto_codec_detach_total": 1,
                "crypto_codec_detach_cancelled_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamCryptoCodecDetachMetricEvent(),
                TcpStreamCryptoCodecDetachResult.REFUSED_NOT_OPENED,
            ),
            {
                "crypto_codec_detach_total": 1,
                "crypto_codec_detach_refused_not_opened_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamCryptoCodecDetachMetricEvent(),
                TcpStreamCryptoCodecDetachResult.REFUSED_NOT_ATTACHED,
            ),
            {
                "crypto_codec_detach_total": 1,
                "crypto_codec_detach_refused_not_attached_total": 1,
            },
        ),
    ],
)
def test_a4_operation_attempts_metric_counts_crypto_codec_events(
    event: MetricEvent,
    expected_changed: dict[str, int],
) -> None:
    metric = TcpStreamOperationAttemptsMetric()

    changed = metric.handle_event(event)

    expected = _zero_dimensions(metric)
    expected.update(expected_changed)

    assert changed is True
    assert _dimensions(metric) == expected


@pytest.mark.parametrize(
    ("event", "expected_changed"),
    [
        (
            _finished_event(
                TcpStreamStreamReadMetricEvent(),
                TcpStreamStreamReadResult.SUCCEEDED,
                bytes_count=3,
            ),
            {
                "stream_read_total": 1,
                "stream_read_success_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamStreamReadMetricEvent(),
                TcpStreamStreamReadResult.TIMED_OUT,
            ),
            {
                "stream_read_total": 1,
                "stream_read_timeout_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamStreamReadMetricEvent(),
                TcpStreamStreamReadResult.FAILED,
            ),
            {
                "stream_read_total": 1,
                "stream_read_error_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamStreamReadMetricEvent(),
                TcpStreamStreamReadResult.CANCELLED,
            ),
            {
                "stream_read_total": 1,
                "stream_read_cancelled_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamStreamReadMetricEvent(),
                TcpStreamStreamReadResult.TLS_FAILED,
            ),
            {
                "stream_read_total": 1,
                "stream_read_tls_error_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamStreamReadMetricEvent(),
                TcpStreamStreamReadResult.REMOTE_DISCONNECTED,
            ),
            {
                "stream_read_total": 1,
                "stream_read_remote_disconnect_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamStreamWriteMetricEvent(),
                TcpStreamStreamWriteResult.SUCCEEDED,
                bytes_count=3,
            ),
            {
                "stream_write_total": 1,
                "stream_write_success_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamStreamWriteMetricEvent(),
                TcpStreamStreamWriteResult.FAILED,
            ),
            {
                "stream_write_total": 1,
                "stream_write_error_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamStreamWriteMetricEvent(),
                TcpStreamStreamWriteResult.TLS_FAILED,
            ),
            {
                "stream_write_total": 1,
                "stream_write_tls_error_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamDrainMetricEvent(),
                TcpStreamDrainResult.SUCCEEDED,
            ),
            {
                "drain_total": 1,
                "drain_success_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamDrainMetricEvent(),
                TcpStreamDrainResult.TIMED_OUT,
            ),
            {
                "drain_total": 1,
                "drain_timeout_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamDrainMetricEvent(),
                TcpStreamDrainResult.FAILED,
            ),
            {
                "drain_total": 1,
                "drain_error_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamDrainMetricEvent(),
                TcpStreamDrainResult.CANCELLED,
            ),
            {
                "drain_total": 1,
                "drain_cancelled_total": 1,
            },
        ),
        (
            _finished_event(
                TcpStreamDrainMetricEvent(),
                TcpStreamDrainResult.TLS_FAILED,
            ),
            {
                "drain_total": 1,
                "drain_tls_error_total": 1,
            },
        ),
    ],
)
def test_a5_operation_attempts_metric_counts_io_and_drain_events(
    event: MetricEvent,
    expected_changed: dict[str, int],
) -> None:
    metric = TcpStreamOperationAttemptsMetric()

    changed = metric.handle_event(event)

    expected = _zero_dimensions(metric)
    expected.update(expected_changed)

    assert changed is True
    assert _dimensions(metric) == expected


def test_a6_operation_attempts_metric_ignores_unrelated_event() -> None:
    metric = TcpStreamOperationAttemptsMetric()

    changed = metric.handle_event(_UnrelatedMetricEvent())

    assert changed is False
    assert _dimensions(metric) == _zero_dimensions(metric)


@pytest.mark.parametrize(
    ("event", "total_dimension"),
    [
        (
            _finished_event(
                TcpStreamOpenMetricEvent(use_ssl=False),
                _UnknownMetricResult.UNKNOWN,
            ),
            "open_total",
        ),
        (
            _finished_event(
                TcpStreamCloseMetricEvent(),
                _UnknownMetricResult.UNKNOWN,
            ),
            "close_total",
        ),
        (
            _finished_event(
                TcpStreamStartTlsMetricEvent(),
                _UnknownMetricResult.UNKNOWN,
            ),
            "start_tls_total",
        ),
        (
            _finished_event(
                TcpStreamCryptoCodecAttachMetricEvent(),
                _UnknownMetricResult.UNKNOWN,
            ),
            "crypto_codec_attach_total",
        ),
        (
            _finished_event(
                TcpStreamCryptoCodecDetachMetricEvent(),
                _UnknownMetricResult.UNKNOWN,
            ),
            "crypto_codec_detach_total",
        ),
        (
            _finished_event(
                TcpStreamStreamReadMetricEvent(),
                _UnknownMetricResult.UNKNOWN,
            ),
            "stream_read_total",
        ),
        (
            _finished_event(
                TcpStreamStreamWriteMetricEvent(),
                _UnknownMetricResult.UNKNOWN,
            ),
            "stream_write_total",
        ),
        (
            _finished_event(
                TcpStreamDrainMetricEvent(),
                _UnknownMetricResult.UNKNOWN,
            ),
            "drain_total",
        ),
    ],
)
def test_a7_operation_attempts_metric_counts_total_but_ignores_unknown_result_bucket(
    event: MetricEvent,
    total_dimension: str,
) -> None:
    metric = TcpStreamOperationAttemptsMetric()

    changed = metric.handle_event(event)

    expected = _zero_dimensions(metric)
    expected[total_dimension] = 1

    assert changed is True
    assert _dimensions(metric) == expected


# -------------------------
# Group b: operation latency metric
# -------------------------


def test_b1_operation_latency_metric_has_expected_identity_and_initial_snapshot() -> None:
    metric = TcpStreamOperationLatencyMetric()

    assert metric.metric_name == "tcp_stream.operation.latency"
    assert _dimensions(metric) == {
        "open_success_latency_average_ns": 0,
        "open_success_latency_max_ns": 0,
        "close_success_latency_average_ns": 0,
        "close_success_latency_max_ns": 0,
        "start_tls_success_latency_average_ns": 0,
        "start_tls_success_latency_max_ns": 0,
        "crypto_codec_attach_success_latency_average_ns": 0,
        "crypto_codec_attach_success_latency_max_ns": 0,
        "crypto_codec_detach_success_latency_average_ns": 0,
        "crypto_codec_detach_success_latency_max_ns": 0,
        "stream_read_success_latency_average_ns": 0,
        "stream_read_success_latency_max_ns": 0,
        "stream_write_success_latency_average_ns": 0,
        "stream_write_success_latency_max_ns": 0,
        "drain_success_latency_average_ns": 0,
        "drain_success_latency_max_ns": 0,
    }


@pytest.mark.parametrize(
    ("event_one", "event_two", "average_dimension", "max_dimension"),
    [
        (
            _finished_event(
                TcpStreamOpenMetricEvent(use_ssl=False),
                TcpStreamOpenResult.SUCCEEDED,
                duration_ns=10,
            ),
            _finished_event(
                TcpStreamOpenMetricEvent(use_ssl=False),
                TcpStreamOpenResult.SUCCEEDED,
                duration_ns=30,
            ),
            "open_success_latency_average_ns",
            "open_success_latency_max_ns",
        ),
        (
            _finished_event(
                TcpStreamCloseMetricEvent(),
                TcpStreamCloseResult.SUCCEEDED,
                duration_ns=10,
            ),
            _finished_event(
                TcpStreamCloseMetricEvent(),
                TcpStreamCloseResult.SUCCEEDED,
                duration_ns=30,
            ),
            "close_success_latency_average_ns",
            "close_success_latency_max_ns",
        ),
        (
            _finished_event(
                TcpStreamStartTlsMetricEvent(),
                TcpStreamStartTlsResult.SUCCEEDED,
                duration_ns=10,
            ),
            _finished_event(
                TcpStreamStartTlsMetricEvent(),
                TcpStreamStartTlsResult.SUCCEEDED,
                duration_ns=30,
            ),
            "start_tls_success_latency_average_ns",
            "start_tls_success_latency_max_ns",
        ),
        (
            _finished_event(
                TcpStreamCryptoCodecAttachMetricEvent(),
                TcpStreamCryptoCodecAttachResult.SUCCEEDED,
                duration_ns=10,
            ),
            _finished_event(
                TcpStreamCryptoCodecAttachMetricEvent(),
                TcpStreamCryptoCodecAttachResult.SUCCEEDED,
                duration_ns=30,
            ),
            "crypto_codec_attach_success_latency_average_ns",
            "crypto_codec_attach_success_latency_max_ns",
        ),
        (
            _finished_event(
                TcpStreamCryptoCodecDetachMetricEvent(),
                TcpStreamCryptoCodecDetachResult.SUCCEEDED,
                duration_ns=10,
            ),
            _finished_event(
                TcpStreamCryptoCodecDetachMetricEvent(),
                TcpStreamCryptoCodecDetachResult.SUCCEEDED,
                duration_ns=30,
            ),
            "crypto_codec_detach_success_latency_average_ns",
            "crypto_codec_detach_success_latency_max_ns",
        ),
        (
            _finished_event(
                TcpStreamStreamReadMetricEvent(),
                TcpStreamStreamReadResult.SUCCEEDED,
                duration_ns=10,
                bytes_count=1,
            ),
            _finished_event(
                TcpStreamStreamReadMetricEvent(),
                TcpStreamStreamReadResult.SUCCEEDED,
                duration_ns=30,
                bytes_count=1,
            ),
            "stream_read_success_latency_average_ns",
            "stream_read_success_latency_max_ns",
        ),
        (
            _finished_event(
                TcpStreamStreamWriteMetricEvent(),
                TcpStreamStreamWriteResult.SUCCEEDED,
                duration_ns=10,
                bytes_count=1,
            ),
            _finished_event(
                TcpStreamStreamWriteMetricEvent(),
                TcpStreamStreamWriteResult.SUCCEEDED,
                duration_ns=30,
                bytes_count=1,
            ),
            "stream_write_success_latency_average_ns",
            "stream_write_success_latency_max_ns",
        ),
        (
            _finished_event(
                TcpStreamDrainMetricEvent(),
                TcpStreamDrainResult.SUCCEEDED,
                duration_ns=10,
            ),
            _finished_event(
                TcpStreamDrainMetricEvent(),
                TcpStreamDrainResult.SUCCEEDED,
                duration_ns=30,
            ),
            "drain_success_latency_average_ns",
            "drain_success_latency_max_ns",
        ),
    ],
)
def test_b2_operation_latency_metric_counts_success_average_and_max_per_operation(
    event_one: MetricEvent,
    event_two: MetricEvent,
    average_dimension: str,
    max_dimension: str,
) -> None:
    metric = TcpStreamOperationLatencyMetric()

    assert metric.handle_event(event_one) is True
    assert metric.handle_event(event_two) is True

    dimensions = _dimensions(metric)

    assert dimensions[average_dimension] == 20
    assert dimensions[max_dimension] == 30

    for key, value in dimensions.items():
        if key not in (average_dimension, max_dimension):
            assert value == 0


@pytest.mark.parametrize(
    "event",
    [
        _finished_event(
            TcpStreamOpenMetricEvent(use_ssl=False),
            TcpStreamOpenResult.FAILED,
        ),
        _finished_event(
            TcpStreamCloseMetricEvent(),
            TcpStreamCloseResult.NOT_OPENED,
        ),
        _finished_event(
            TcpStreamStartTlsMetricEvent(),
            TcpStreamStartTlsResult.FAILED,
        ),
        _finished_event(
            TcpStreamCryptoCodecAttachMetricEvent(),
            TcpStreamCryptoCodecAttachResult.REFUSED_NOT_OPENED,
        ),
        _finished_event(
            TcpStreamCryptoCodecDetachMetricEvent(),
            TcpStreamCryptoCodecDetachResult.REFUSED_NOT_ATTACHED,
        ),
        _finished_event(
            TcpStreamStreamReadMetricEvent(),
            TcpStreamStreamReadResult.REMOTE_DISCONNECTED,
        ),
        _finished_event(
            TcpStreamStreamWriteMetricEvent(),
            TcpStreamStreamWriteResult.FAILED,
        ),
        _finished_event(
            TcpStreamDrainMetricEvent(),
            TcpStreamDrainResult.TIMED_OUT,
        ),
    ],
)
def test_b3_operation_latency_metric_ignores_non_success_events(event: MetricEvent) -> None:
    metric = TcpStreamOperationLatencyMetric()

    changed = metric.handle_event(event)

    assert changed is False
    assert _dimensions(metric) == _zero_dimensions(metric)


def test_b4_operation_latency_metric_ignores_unrelated_event() -> None:
    metric = TcpStreamOperationLatencyMetric()

    changed = metric.handle_event(_UnrelatedMetricEvent())

    assert changed is False
    assert _dimensions(metric) == _zero_dimensions(metric)


# -------------------------
# Group c: I/O bytes metric
# -------------------------


def test_c1_io_bytes_metric_has_expected_identity_and_initial_snapshot() -> None:
    metric = TcpStreamIoBytesMetric()

    assert metric.metric_name == "tcp_stream.io.bytes"
    assert _dimensions(metric) == {
        "received_total": 0,
        "sent_total": 0,
        "read_success_bytes_average": 0,
        "write_success_bytes_average": 0,
    }


def test_c2_io_bytes_metric_counts_received_and_sent_bytes() -> None:
    metric = TcpStreamIoBytesMetric()

    assert (
        metric.handle_event(
            _finished_event(
                TcpStreamStreamReadMetricEvent(),
                TcpStreamStreamReadResult.SUCCEEDED,
                bytes_count=10,
            )
        )
        is True
    )
    assert (
        metric.handle_event(
            _finished_event(
                TcpStreamStreamReadMetricEvent(),
                TcpStreamStreamReadResult.SUCCEEDED,
                bytes_count=30,
            )
        )
        is True
    )
    assert (
        metric.handle_event(
            _finished_event(
                TcpStreamStreamWriteMetricEvent(),
                TcpStreamStreamWriteResult.SUCCEEDED,
                bytes_count=5,
            )
        )
        is True
    )
    assert (
        metric.handle_event(
            _finished_event(
                TcpStreamStreamWriteMetricEvent(),
                TcpStreamStreamWriteResult.SUCCEEDED,
                bytes_count=15,
            )
        )
        is True
    )

    assert _dimensions(metric) == {
        "received_total": 40,
        "sent_total": 20,
        "read_success_bytes_average": 20,
        "write_success_bytes_average": 10,
    }


@pytest.mark.parametrize(
    "event",
    [
        _finished_event(
            TcpStreamStreamReadMetricEvent(),
            TcpStreamStreamReadResult.FAILED,
            bytes_count=10,
        ),
        _finished_event(
            TcpStreamStreamWriteMetricEvent(),
            TcpStreamStreamWriteResult.FAILED,
            bytes_count=10,
        ),
        _finished_event(
            TcpStreamDrainMetricEvent(),
            TcpStreamDrainResult.SUCCEEDED,
        ),
    ],
)
def test_c3_io_bytes_metric_ignores_non_success_or_unrelated_events(
    event: MetricEvent,
) -> None:
    metric = TcpStreamIoBytesMetric()

    changed = metric.handle_event(event)

    assert changed is False
    assert _dimensions(metric) == {
        "received_total": 0,
        "sent_total": 0,
        "read_success_bytes_average": 0,
        "write_success_bytes_average": 0,
    }


# -------------------------
# Group d: remote disconnect metric
# -------------------------


def test_d1_remote_disconnect_metric_has_expected_identity_and_initial_snapshot() -> None:
    metric = TcpStreamRemoteDisconnectMetric()

    assert metric.metric_name == "tcp_stream.remote_disconnect"
    assert _dimensions(metric) == {
        "total": 0,
    }


def test_d2_remote_disconnect_metric_counts_remote_disconnect_events() -> None:
    metric = TcpStreamRemoteDisconnectMetric()

    assert (
        metric.handle_event(
            _finished_event(
                TcpStreamStreamReadMetricEvent(),
                TcpStreamStreamReadResult.REMOTE_DISCONNECTED,
                duration_ns=10,
            )
        )
        is True
    )
    assert (
        metric.handle_event(
            _finished_event(
                TcpStreamStreamReadMetricEvent(),
                TcpStreamStreamReadResult.REMOTE_DISCONNECTED,
                duration_ns=20,
            )
        )
        is True
    )

    assert _dimensions(metric) == {
        "total": 2,
    }


@pytest.mark.parametrize(
    "event",
    [
        _finished_event(
            TcpStreamStreamReadMetricEvent(),
            TcpStreamStreamReadResult.SUCCEEDED,
            bytes_count=1,
        ),
        _finished_event(
            TcpStreamStreamWriteMetricEvent(),
            TcpStreamStreamWriteResult.FAILED,
        ),
        _UnrelatedMetricEvent(),
    ],
)
def test_d3_remote_disconnect_metric_ignores_other_events(event: MetricEvent) -> None:
    metric = TcpStreamRemoteDisconnectMetric()

    changed = metric.handle_event(event)

    assert changed is False
    assert _dimensions(metric) == {
        "total": 0,
    }


# -------------------------
# Group e: abortive close metric
# -------------------------


def test_e1_abortive_close_metric_has_expected_identity_and_initial_snapshot() -> None:
    metric = TcpStreamAbortiveCloseMetric()

    assert metric.metric_name == "tcp_stream.abortive_close"
    assert _dimensions(metric) == {
        "total": 0,
    }


@pytest.mark.parametrize(
    "event",
    [
        _finished_event(
            TcpStreamStartTlsMetricEvent(),
            TcpStreamStartTlsResult.FAILED,
        ),
        _finished_event(
            TcpStreamStartTlsMetricEvent(),
            TcpStreamStartTlsResult.TIMED_OUT,
        ),
        _finished_event(
            TcpStreamStartTlsMetricEvent(),
            TcpStreamStartTlsResult.TLS_FAILED,
        ),
        _finished_event(
            TcpStreamStreamReadMetricEvent(),
            TcpStreamStreamReadResult.FAILED,
        ),
        _finished_event(
            TcpStreamStreamReadMetricEvent(),
            TcpStreamStreamReadResult.TLS_FAILED,
        ),
        _finished_event(
            TcpStreamStreamReadMetricEvent(),
            TcpStreamStreamReadResult.REMOTE_DISCONNECTED,
        ),
        _finished_event(
            TcpStreamDrainMetricEvent(),
            TcpStreamDrainResult.FAILED,
        ),
        _finished_event(
            TcpStreamDrainMetricEvent(),
            TcpStreamDrainResult.TLS_FAILED,
        ),
    ],
)
def test_e2_abortive_close_metric_counts_abortive_events(event: MetricEvent) -> None:
    metric = TcpStreamAbortiveCloseMetric()

    changed = metric.handle_event(event)

    assert changed is True
    assert _dimensions(metric) == {
        "total": 1,
    }


@pytest.mark.parametrize(
    "event",
    [
        _finished_event(
            TcpStreamOpenMetricEvent(use_ssl=False),
            TcpStreamOpenResult.SUCCEEDED,
        ),
        _finished_event(
            TcpStreamCloseMetricEvent(),
            TcpStreamCloseResult.SUCCEEDED,
        ),
        _finished_event(
            TcpStreamStartTlsMetricEvent(),
            TcpStreamStartTlsResult.SUCCEEDED,
        ),
        _finished_event(
            TcpStreamStreamReadMetricEvent(),
            TcpStreamStreamReadResult.SUCCEEDED,
            bytes_count=1,
        ),
        _finished_event(
            TcpStreamStreamWriteMetricEvent(),
            TcpStreamStreamWriteResult.FAILED,
        ),
        _finished_event(
            TcpStreamDrainMetricEvent(),
            TcpStreamDrainResult.SUCCEEDED,
        ),
        _UnrelatedMetricEvent(),
    ],
)
def test_e3_abortive_close_metric_ignores_non_abortive_events(event: MetricEvent) -> None:
    metric = TcpStreamAbortiveCloseMetric()

    changed = metric.handle_event(event)

    assert changed is False
    assert _dimensions(metric) == {
        "total": 0,
    }


# -------------------------
# Group f: cross-metric behavior
# -------------------------


def test_f1_each_metric_ignores_unrelated_event() -> None:
    metrics = [
        TcpStreamOperationAttemptsMetric(),
        TcpStreamOperationLatencyMetric(),
        TcpStreamIoBytesMetric(),
        TcpStreamRemoteDisconnectMetric(),
        TcpStreamAbortiveCloseMetric(),
    ]

    for metric in metrics:
        assert metric.handle_event(_UnrelatedMetricEvent()) is False
        assert _dimensions(metric) == _zero_dimensions(metric)
