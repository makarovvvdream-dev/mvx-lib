# tests/engines/tcp_stream_engine/test_metrics.py

"""
Tests for tcp_stream_engine metrics.

Grouping rule:
  - Group a: open attempts metric
  - Group b: close attempts metric
  - Group c: start_tls attempts metric
  - Group d: crypto codec attach attempts metric
  - Group e: crypto codec detach attempts metric
  - Group f: stream read attempts metric
  - Group g: stream write attempts metric
  - Group h: drain attempts metric
  - Group i: bytes received metric
  - Group j: bytes sent metric
  - Group k: remote disconnect metric
  - Group l: abortive close metric
  - Group m: cross-metric behavior

Naming rule:
  Each test name starts with test_<group><num>_, e.g. test_a1_...
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from mvx.networking.engines.tcp_stream_engine.metrics import (
    TcpStreamCloseAttemptMetricEvent,
    TcpStreamCloseAttemptOutcome,
    TcpStreamCloseAttemptsMetric,
    TcpStreamCryptoCodecAttachAttemptMetricEvent,
    TcpStreamCryptoCodecAttachAttemptOutcome,
    TcpStreamCryptoCodecAttachAttemptsMetric,
    TcpStreamCryptoCodecDetachAttemptMetricEvent,
    TcpStreamCryptoCodecDetachAttemptOutcome,
    TcpStreamCryptoCodecDetachAttemptsMetric,
    TcpStreamOpenAttemptMetricEvent,
    TcpStreamOpenAttemptOutcome,
    TcpStreamOpenAttemptsMetric,
    TcpStreamStartTlsAttemptMetricEvent,
    TcpStreamStartTlsAttemptOutcome,
    TcpStreamStartTlsAttemptsMetric,
    TcpStreamStreamReadAttemptMetricEvent,
    TcpStreamStreamReadAttemptOutcome,
    TcpStreamStreamReadAttemptsMetric,
    TcpStreamStreamWriteAttemptMetricEvent,
    TcpStreamStreamWriteAttemptOutcome,
    TcpStreamStreamWriteAttemptsMetric,
    TcpStreamDrainAttemptMetricEvent,
    TcpStreamDrainAttemptOutcome,
    TcpStreamDrainAttemptsMetric,
    TcpStreamBytesReceivedMetricEvent,
    TcpStreamBytesReceivedMetric,
    TcpStreamBytesSentMetricEvent,
    TcpStreamBytesSentMetric,
    TcpStreamRemoteDisconnectMetricEvent,
    TcpStreamRemoteDisconnectMetric,
    TcpStreamAbortiveCloseMetricEvent,
    TcpStreamAbortiveCloseMetric,
)


def _dimensions(metric: Any) -> dict[str, int]:
    snapshot = metric.snapshot()
    dimensions = snapshot["dimensions"]

    assert isinstance(dimensions, dict)

    # noinspection PyUnnecessaryCast
    return cast(dict[str, int], dimensions)


def _assert_single_dimension_incremented(
    *,
    dimensions: dict[str, int],
    dimension: str,
) -> None:
    assert dimensions["total"] == 1
    assert dimensions[dimension] == 1

    for key, value in dimensions.items():
        if key not in ("total", dimension):
            assert value == 0


# -------------------------
# Group a: open attempts metric
# -------------------------


def test_a1_open_attempt_metric_has_expected_identity_and_initial_snapshot() -> None:
    metric = TcpStreamOpenAttemptsMetric()

    assert metric.metric_name == "tcp_stream.open.attempts"
    assert metric.snapshot() == {
        "name": "tcp_stream.open.attempts",
        "dimensions": {
            "total": 0,
            "success_total": 0,
            "already_opened_total": 0,
            "failure_total": 0,
            "cancelled_total": 0,
        },
    }


def test_a2_open_attempt_metric_event_has_expected_type_and_fields() -> None:
    event = TcpStreamOpenAttemptMetricEvent(
        use_ssl=True,
        outcome=TcpStreamOpenAttemptOutcome.SUCCESS,
    )

    assert event.event_type == "tcp_stream.open.attempt"
    assert event.use_ssl is True
    assert event.outcome is TcpStreamOpenAttemptOutcome.SUCCESS


@pytest.mark.parametrize(
    ("outcome", "dimension"),
    [
        (TcpStreamOpenAttemptOutcome.SUCCESS, "success_total"),
        (TcpStreamOpenAttemptOutcome.ALREADY_OPENED, "already_opened_total"),
        (TcpStreamOpenAttemptOutcome.FAILURE, "failure_total"),
        (TcpStreamOpenAttemptOutcome.CANCELLED, "cancelled_total"),
    ],
)
def test_a3_open_attempt_metric_counts_each_outcome(
    outcome: TcpStreamOpenAttemptOutcome,
    dimension: str,
) -> None:
    metric = TcpStreamOpenAttemptsMetric()

    changed = metric.handle_event(
        TcpStreamOpenAttemptMetricEvent(
            use_ssl=False,
            outcome=outcome,
        )
    )

    assert changed is True
    _assert_single_dimension_incremented(
        dimensions=_dimensions(metric),
        dimension=dimension,
    )


def test_a4_open_attempt_metric_ignores_unrelated_event() -> None:
    metric = TcpStreamOpenAttemptsMetric()

    changed = metric.handle_event(
        TcpStreamCloseAttemptMetricEvent(
            outcome=TcpStreamCloseAttemptOutcome.SUCCESS,
        )
    )

    assert changed is False
    assert _dimensions(metric) == {
        "total": 0,
        "success_total": 0,
        "already_opened_total": 0,
        "failure_total": 0,
        "cancelled_total": 0,
    }


def test_a5_open_attempt_metric_accumulates_multiple_events() -> None:
    metric = TcpStreamOpenAttemptsMetric()

    events = [
        TcpStreamOpenAttemptMetricEvent(
            use_ssl=False,
            outcome=TcpStreamOpenAttemptOutcome.SUCCESS,
        ),
        TcpStreamOpenAttemptMetricEvent(
            use_ssl=True,
            outcome=TcpStreamOpenAttemptOutcome.SUCCESS,
        ),
        TcpStreamOpenAttemptMetricEvent(
            use_ssl=False,
            outcome=TcpStreamOpenAttemptOutcome.ALREADY_OPENED,
        ),
        TcpStreamOpenAttemptMetricEvent(
            use_ssl=False,
            outcome=TcpStreamOpenAttemptOutcome.FAILURE,
        ),
        TcpStreamOpenAttemptMetricEvent(
            use_ssl=False,
            outcome=TcpStreamOpenAttemptOutcome.CANCELLED,
        ),
    ]

    for event in events:
        assert metric.handle_event(event) is True

    assert _dimensions(metric) == {
        "total": 5,
        "success_total": 2,
        "already_opened_total": 1,
        "failure_total": 1,
        "cancelled_total": 1,
    }


# -------------------------
# Group b: close attempts metric
# -------------------------


def test_b1_close_attempt_metric_has_expected_identity_and_initial_snapshot() -> None:
    metric = TcpStreamCloseAttemptsMetric()

    assert metric.metric_name == "tcp_stream.close.attempts"
    assert metric.snapshot() == {
        "name": "tcp_stream.close.attempts",
        "dimensions": {
            "total": 0,
            "success_total": 0,
            "not_opened_total": 0,
            "failure_total": 0,
            "cancelled_total": 0,
        },
    }


def test_b2_close_attempt_metric_event_has_expected_type_and_fields() -> None:
    event = TcpStreamCloseAttemptMetricEvent(
        outcome=TcpStreamCloseAttemptOutcome.NOT_OPENED,
    )

    assert event.event_type == "tcp_stream.close.attempt"
    assert event.outcome is TcpStreamCloseAttemptOutcome.NOT_OPENED


@pytest.mark.parametrize(
    ("outcome", "dimension"),
    [
        (TcpStreamCloseAttemptOutcome.SUCCESS, "success_total"),
        (TcpStreamCloseAttemptOutcome.NOT_OPENED, "not_opened_total"),
        (TcpStreamCloseAttemptOutcome.FAILURE, "failure_total"),
        (TcpStreamCloseAttemptOutcome.CANCELLED, "cancelled_total"),
    ],
)
def test_b3_close_attempt_metric_counts_each_outcome(
    outcome: TcpStreamCloseAttemptOutcome,
    dimension: str,
) -> None:
    metric = TcpStreamCloseAttemptsMetric()

    changed = metric.handle_event(TcpStreamCloseAttemptMetricEvent(outcome=outcome))

    assert changed is True
    _assert_single_dimension_incremented(
        dimensions=_dimensions(metric),
        dimension=dimension,
    )


def test_b4_close_attempt_metric_ignores_unrelated_event() -> None:
    metric = TcpStreamCloseAttemptsMetric()

    changed = metric.handle_event(
        TcpStreamOpenAttemptMetricEvent(
            use_ssl=False,
            outcome=TcpStreamOpenAttemptOutcome.SUCCESS,
        )
    )

    assert changed is False
    assert _dimensions(metric) == {
        "total": 0,
        "success_total": 0,
        "not_opened_total": 0,
        "failure_total": 0,
        "cancelled_total": 0,
    }


def test_b5_close_attempt_metric_accumulates_multiple_events() -> None:
    metric = TcpStreamCloseAttemptsMetric()

    events = [
        TcpStreamCloseAttemptMetricEvent(
            outcome=TcpStreamCloseAttemptOutcome.SUCCESS,
        ),
        TcpStreamCloseAttemptMetricEvent(
            outcome=TcpStreamCloseAttemptOutcome.NOT_OPENED,
        ),
        TcpStreamCloseAttemptMetricEvent(
            outcome=TcpStreamCloseAttemptOutcome.NOT_OPENED,
        ),
        TcpStreamCloseAttemptMetricEvent(
            outcome=TcpStreamCloseAttemptOutcome.FAILURE,
        ),
        TcpStreamCloseAttemptMetricEvent(
            outcome=TcpStreamCloseAttemptOutcome.CANCELLED,
        ),
    ]

    for event in events:
        assert metric.handle_event(event) is True

    assert _dimensions(metric) == {
        "total": 5,
        "success_total": 1,
        "not_opened_total": 2,
        "failure_total": 1,
        "cancelled_total": 1,
    }


# -------------------------
# Group c: start_tls attempts metric
# -------------------------


def test_c1_start_tls_attempt_metric_has_expected_identity_and_initial_snapshot() -> None:
    metric = TcpStreamStartTlsAttemptsMetric()

    assert metric.metric_name == "tcp_stream.start_tls.attempts"
    assert metric.snapshot() == {
        "name": "tcp_stream.start_tls.attempts",
        "dimensions": {
            "total": 0,
            "success_total": 0,
            "failure_total": 0,
            "cancelled_total": 0,
            "timeout_total": 0,
            "refused_not_opened_total": 0,
            "refused_already_under_ssl_total": 0,
            "refused_start_tls_already_active_total": 0,
            "refused_crypto_codec_attached_total": 0,
            "tls_error_total": 0,
        },
    }


def test_c2_start_tls_attempt_metric_event_has_expected_type_and_fields() -> None:
    event = TcpStreamStartTlsAttemptMetricEvent(
        outcome=TcpStreamStartTlsAttemptOutcome.TLS_ERROR,
    )

    assert event.event_type == "tcp_stream.start_tls.attempt"
    assert event.outcome is TcpStreamStartTlsAttemptOutcome.TLS_ERROR


@pytest.mark.parametrize(
    ("outcome", "dimension"),
    [
        (TcpStreamStartTlsAttemptOutcome.SUCCESS, "success_total"),
        (TcpStreamStartTlsAttemptOutcome.FAILURE, "failure_total"),
        (TcpStreamStartTlsAttemptOutcome.CANCELLED, "cancelled_total"),
        (TcpStreamStartTlsAttemptOutcome.TIMEOUT, "timeout_total"),
        (
            TcpStreamStartTlsAttemptOutcome.REFUSED_NOT_OPENED,
            "refused_not_opened_total",
        ),
        (
            TcpStreamStartTlsAttemptOutcome.REFUSED_ALREADY_UNDER_SSL,
            "refused_already_under_ssl_total",
        ),
        (
            TcpStreamStartTlsAttemptOutcome.REFUSED_START_TLS_ALREADY_ACTIVE,
            "refused_start_tls_already_active_total",
        ),
        (
            TcpStreamStartTlsAttemptOutcome.REFUSED_CRYPTO_CODEC_ATTACHED,
            "refused_crypto_codec_attached_total",
        ),
        (TcpStreamStartTlsAttemptOutcome.TLS_ERROR, "tls_error_total"),
    ],
)
def test_c3_start_tls_attempt_metric_counts_each_outcome(
    outcome: TcpStreamStartTlsAttemptOutcome,
    dimension: str,
) -> None:
    metric = TcpStreamStartTlsAttemptsMetric()

    changed = metric.handle_event(TcpStreamStartTlsAttemptMetricEvent(outcome=outcome))

    assert changed is True
    _assert_single_dimension_incremented(
        dimensions=_dimensions(metric),
        dimension=dimension,
    )


def test_c4_start_tls_attempt_metric_ignores_unrelated_event() -> None:
    metric = TcpStreamStartTlsAttemptsMetric()

    changed = metric.handle_event(
        TcpStreamCloseAttemptMetricEvent(
            outcome=TcpStreamCloseAttemptOutcome.SUCCESS,
        )
    )

    assert changed is False
    assert _dimensions(metric) == {
        "total": 0,
        "success_total": 0,
        "failure_total": 0,
        "cancelled_total": 0,
        "timeout_total": 0,
        "refused_not_opened_total": 0,
        "refused_already_under_ssl_total": 0,
        "refused_start_tls_already_active_total": 0,
        "refused_crypto_codec_attached_total": 0,
        "tls_error_total": 0,
    }


def test_c5_start_tls_attempt_metric_accumulates_multiple_events() -> None:
    metric = TcpStreamStartTlsAttemptsMetric()

    events = [
        TcpStreamStartTlsAttemptMetricEvent(
            outcome=TcpStreamStartTlsAttemptOutcome.SUCCESS,
        ),
        TcpStreamStartTlsAttemptMetricEvent(
            outcome=TcpStreamStartTlsAttemptOutcome.FAILURE,
        ),
        TcpStreamStartTlsAttemptMetricEvent(
            outcome=TcpStreamStartTlsAttemptOutcome.CANCELLED,
        ),
        TcpStreamStartTlsAttemptMetricEvent(
            outcome=TcpStreamStartTlsAttemptOutcome.TIMEOUT,
        ),
        TcpStreamStartTlsAttemptMetricEvent(
            outcome=TcpStreamStartTlsAttemptOutcome.REFUSED_NOT_OPENED,
        ),
        TcpStreamStartTlsAttemptMetricEvent(
            outcome=TcpStreamStartTlsAttemptOutcome.REFUSED_ALREADY_UNDER_SSL,
        ),
        TcpStreamStartTlsAttemptMetricEvent(
            outcome=TcpStreamStartTlsAttemptOutcome.REFUSED_START_TLS_ALREADY_ACTIVE,
        ),
        TcpStreamStartTlsAttemptMetricEvent(
            outcome=TcpStreamStartTlsAttemptOutcome.REFUSED_CRYPTO_CODEC_ATTACHED,
        ),
        TcpStreamStartTlsAttemptMetricEvent(
            outcome=TcpStreamStartTlsAttemptOutcome.TLS_ERROR,
        ),
    ]

    for event in events:
        assert metric.handle_event(event) is True

    assert _dimensions(metric) == {
        "total": 9,
        "success_total": 1,
        "failure_total": 1,
        "cancelled_total": 1,
        "timeout_total": 1,
        "refused_not_opened_total": 1,
        "refused_already_under_ssl_total": 1,
        "refused_start_tls_already_active_total": 1,
        "refused_crypto_codec_attached_total": 1,
        "tls_error_total": 1,
    }


# -------------------------
# Group d: crypto codec attach attempts metric
# -------------------------


def test_d1_crypto_codec_attach_attempt_metric_has_expected_identity_and_initial_snapshot() -> None:
    metric = TcpStreamCryptoCodecAttachAttemptsMetric()

    assert metric.metric_name == "tcp_stream.crypto_codec.attach.attempts"
    assert metric.snapshot() == {
        "name": "tcp_stream.crypto_codec.attach.attempts",
        "dimensions": {
            "total": 0,
            "success_total": 0,
            "failure_total": 0,
            "refused_not_opened_total": 0,
            "refused_already_under_ssl_total": 0,
            "refused_start_tls_active_total": 0,
            "refused_already_attached_total": 0,
        },
    }


def test_d2_crypto_codec_attach_attempt_metric_event_has_expected_type_and_fields() -> None:
    event = TcpStreamCryptoCodecAttachAttemptMetricEvent(
        outcome=TcpStreamCryptoCodecAttachAttemptOutcome.REFUSED_ALREADY_ATTACHED,
    )

    assert event.event_type == "tcp_stream.crypto_codec.attach.attempt"
    assert event.outcome is TcpStreamCryptoCodecAttachAttemptOutcome.REFUSED_ALREADY_ATTACHED


@pytest.mark.parametrize(
    ("outcome", "dimension"),
    [
        (TcpStreamCryptoCodecAttachAttemptOutcome.SUCCESS, "success_total"),
        (TcpStreamCryptoCodecAttachAttemptOutcome.FAILURE, "failure_total"),
        (
            TcpStreamCryptoCodecAttachAttemptOutcome.REFUSED_NOT_OPENED,
            "refused_not_opened_total",
        ),
        (
            TcpStreamCryptoCodecAttachAttemptOutcome.REFUSED_ALREADY_UNDER_SSL,
            "refused_already_under_ssl_total",
        ),
        (
            TcpStreamCryptoCodecAttachAttemptOutcome.REFUSED_START_TLS_ACTIVE,
            "refused_start_tls_active_total",
        ),
        (
            TcpStreamCryptoCodecAttachAttemptOutcome.REFUSED_ALREADY_ATTACHED,
            "refused_already_attached_total",
        ),
    ],
)
def test_d3_crypto_codec_attach_attempt_metric_counts_each_outcome(
    outcome: TcpStreamCryptoCodecAttachAttemptOutcome,
    dimension: str,
) -> None:
    metric = TcpStreamCryptoCodecAttachAttemptsMetric()

    changed = metric.handle_event(TcpStreamCryptoCodecAttachAttemptMetricEvent(outcome=outcome))

    assert changed is True
    _assert_single_dimension_incremented(
        dimensions=_dimensions(metric),
        dimension=dimension,
    )


def test_d4_crypto_codec_attach_attempt_metric_ignores_unrelated_event() -> None:
    metric = TcpStreamCryptoCodecAttachAttemptsMetric()

    changed = metric.handle_event(
        TcpStreamCryptoCodecDetachAttemptMetricEvent(
            outcome=TcpStreamCryptoCodecDetachAttemptOutcome.SUCCESS,
        )
    )

    assert changed is False
    assert _dimensions(metric) == {
        "total": 0,
        "success_total": 0,
        "failure_total": 0,
        "refused_not_opened_total": 0,
        "refused_already_under_ssl_total": 0,
        "refused_start_tls_active_total": 0,
        "refused_already_attached_total": 0,
    }


def test_d5_crypto_codec_attach_attempt_metric_accumulates_multiple_events() -> None:
    metric = TcpStreamCryptoCodecAttachAttemptsMetric()

    events = [
        TcpStreamCryptoCodecAttachAttemptMetricEvent(
            outcome=TcpStreamCryptoCodecAttachAttemptOutcome.SUCCESS,
        ),
        TcpStreamCryptoCodecAttachAttemptMetricEvent(
            outcome=TcpStreamCryptoCodecAttachAttemptOutcome.FAILURE,
        ),
        TcpStreamCryptoCodecAttachAttemptMetricEvent(
            outcome=TcpStreamCryptoCodecAttachAttemptOutcome.REFUSED_NOT_OPENED,
        ),
        TcpStreamCryptoCodecAttachAttemptMetricEvent(
            outcome=TcpStreamCryptoCodecAttachAttemptOutcome.REFUSED_ALREADY_UNDER_SSL,
        ),
        TcpStreamCryptoCodecAttachAttemptMetricEvent(
            outcome=TcpStreamCryptoCodecAttachAttemptOutcome.REFUSED_START_TLS_ACTIVE,
        ),
        TcpStreamCryptoCodecAttachAttemptMetricEvent(
            outcome=TcpStreamCryptoCodecAttachAttemptOutcome.REFUSED_ALREADY_ATTACHED,
        ),
    ]

    for event in events:
        assert metric.handle_event(event) is True

    assert _dimensions(metric) == {
        "total": 6,
        "success_total": 1,
        "failure_total": 1,
        "refused_not_opened_total": 1,
        "refused_already_under_ssl_total": 1,
        "refused_start_tls_active_total": 1,
        "refused_already_attached_total": 1,
    }


# -------------------------
# Group e: crypto codec detach attempts metric
# -------------------------


def test_e1_crypto_codec_detach_attempt_metric_has_expected_identity_and_initial_snapshot() -> None:
    metric = TcpStreamCryptoCodecDetachAttemptsMetric()

    assert metric.metric_name == "tcp_stream.crypto_codec.detach.attempts"
    assert metric.snapshot() == {
        "name": "tcp_stream.crypto_codec.detach.attempts",
        "dimensions": {
            "total": 0,
            "success_total": 0,
            "failure_total": 0,
            "refused_not_opened_total": 0,
            "refused_not_attached_total": 0,
        },
    }


def test_e2_crypto_codec_detach_attempt_metric_event_has_expected_type_and_fields() -> None:
    event = TcpStreamCryptoCodecDetachAttemptMetricEvent(
        outcome=TcpStreamCryptoCodecDetachAttemptOutcome.REFUSED_NOT_ATTACHED,
    )

    assert event.event_type == "tcp_stream.crypto_codec.detach.attempt"
    assert event.outcome is TcpStreamCryptoCodecDetachAttemptOutcome.REFUSED_NOT_ATTACHED


@pytest.mark.parametrize(
    ("outcome", "dimension"),
    [
        (TcpStreamCryptoCodecDetachAttemptOutcome.SUCCESS, "success_total"),
        (TcpStreamCryptoCodecDetachAttemptOutcome.FAILURE, "failure_total"),
        (
            TcpStreamCryptoCodecDetachAttemptOutcome.REFUSED_NOT_OPENED,
            "refused_not_opened_total",
        ),
        (
            TcpStreamCryptoCodecDetachAttemptOutcome.REFUSED_NOT_ATTACHED,
            "refused_not_attached_total",
        ),
    ],
)
def test_e3_crypto_codec_detach_attempt_metric_counts_each_outcome(
    outcome: TcpStreamCryptoCodecDetachAttemptOutcome,
    dimension: str,
) -> None:
    metric = TcpStreamCryptoCodecDetachAttemptsMetric()

    changed = metric.handle_event(TcpStreamCryptoCodecDetachAttemptMetricEvent(outcome=outcome))

    assert changed is True
    _assert_single_dimension_incremented(
        dimensions=_dimensions(metric),
        dimension=dimension,
    )


def test_e4_crypto_codec_detach_attempt_metric_ignores_unrelated_event() -> None:
    metric = TcpStreamCryptoCodecDetachAttemptsMetric()

    changed = metric.handle_event(
        TcpStreamCryptoCodecAttachAttemptMetricEvent(
            outcome=TcpStreamCryptoCodecAttachAttemptOutcome.SUCCESS,
        )
    )

    assert changed is False
    assert _dimensions(metric) == {
        "total": 0,
        "success_total": 0,
        "failure_total": 0,
        "refused_not_opened_total": 0,
        "refused_not_attached_total": 0,
    }


def test_e5_crypto_codec_detach_attempt_metric_accumulates_multiple_events() -> None:
    metric = TcpStreamCryptoCodecDetachAttemptsMetric()

    events = [
        TcpStreamCryptoCodecDetachAttemptMetricEvent(
            outcome=TcpStreamCryptoCodecDetachAttemptOutcome.SUCCESS,
        ),
        TcpStreamCryptoCodecDetachAttemptMetricEvent(
            outcome=TcpStreamCryptoCodecDetachAttemptOutcome.FAILURE,
        ),
        TcpStreamCryptoCodecDetachAttemptMetricEvent(
            outcome=TcpStreamCryptoCodecDetachAttemptOutcome.REFUSED_NOT_OPENED,
        ),
        TcpStreamCryptoCodecDetachAttemptMetricEvent(
            outcome=TcpStreamCryptoCodecDetachAttemptOutcome.REFUSED_NOT_ATTACHED,
        ),
    ]

    for event in events:
        assert metric.handle_event(event) is True

    assert _dimensions(metric) == {
        "total": 4,
        "success_total": 1,
        "failure_total": 1,
        "refused_not_opened_total": 1,
        "refused_not_attached_total": 1,
    }


# -------------------------
# Group f: cross-metric behavior
# -------------------------


def test_f1_each_metric_ignores_all_other_metric_event_types() -> None:
    metrics_and_unrelated_events = [
        (
            TcpStreamOpenAttemptsMetric(),
            TcpStreamCloseAttemptMetricEvent(
                outcome=TcpStreamCloseAttemptOutcome.SUCCESS,
            ),
        ),
        (
            TcpStreamCloseAttemptsMetric(),
            TcpStreamStartTlsAttemptMetricEvent(
                outcome=TcpStreamStartTlsAttemptOutcome.SUCCESS,
            ),
        ),
        (
            TcpStreamStartTlsAttemptsMetric(),
            TcpStreamCryptoCodecAttachAttemptMetricEvent(
                outcome=TcpStreamCryptoCodecAttachAttemptOutcome.SUCCESS,
            ),
        ),
        (
            TcpStreamCryptoCodecAttachAttemptsMetric(),
            TcpStreamCryptoCodecDetachAttemptMetricEvent(
                outcome=TcpStreamCryptoCodecDetachAttemptOutcome.SUCCESS,
            ),
        ),
        (
            TcpStreamCryptoCodecDetachAttemptsMetric(),
            TcpStreamOpenAttemptMetricEvent(
                use_ssl=False,
                outcome=TcpStreamOpenAttemptOutcome.SUCCESS,
            ),
        ),
    ]

    for metric, event in metrics_and_unrelated_events:
        assert metric.handle_event(event) is False
        assert _dimensions(metric)["total"] == 0


# -------------------------
# Group f: stream read attempts metric
# -------------------------


def test_f1_stream_read_attempt_metric_has_expected_identity_and_initial_snapshot() -> None:
    metric = TcpStreamStreamReadAttemptsMetric()

    assert metric.metric_name == "tcp_stream.stream_read.attempts"
    assert metric.snapshot() == {
        "name": "tcp_stream.stream_read.attempts",
        "dimensions": {
            "total": 0,
            "success_total": 0,
            "timeout_total": 0,
            "error_total": 0,
            "cancelled_total": 0,
            "tls_error_total": 0,
        },
    }


def test_f2_stream_read_attempt_metric_event_has_expected_type_and_fields() -> None:
    event = TcpStreamStreamReadAttemptMetricEvent(
        outcome=TcpStreamStreamReadAttemptOutcome.SUCCESS,
    )

    assert event.event_type == "tcp_stream.stream_read.attempt"
    assert event.outcome is TcpStreamStreamReadAttemptOutcome.SUCCESS


@pytest.mark.parametrize(
    ("outcome", "dimension"),
    [
        (TcpStreamStreamReadAttemptOutcome.SUCCESS, "success_total"),
        (TcpStreamStreamReadAttemptOutcome.TIMEOUT, "timeout_total"),
        (TcpStreamStreamReadAttemptOutcome.ERROR, "error_total"),
        (TcpStreamStreamReadAttemptOutcome.CANCELLED, "cancelled_total"),
        (TcpStreamStreamReadAttemptOutcome.TLS_ERROR, "tls_error_total"),
    ],
)
def test_f3_stream_read_attempt_metric_counts_each_outcome(
    outcome: TcpStreamStreamReadAttemptOutcome,
    dimension: str,
) -> None:
    metric = TcpStreamStreamReadAttemptsMetric()

    changed = metric.handle_event(TcpStreamStreamReadAttemptMetricEvent(outcome=outcome))

    assert changed is True
    _assert_single_dimension_incremented(
        dimensions=_dimensions(metric),
        dimension=dimension,
    )


def test_f4_stream_read_attempt_metric_ignores_unrelated_event() -> None:
    metric = TcpStreamStreamReadAttemptsMetric()

    changed = metric.handle_event(
        TcpStreamDrainAttemptMetricEvent(outcome=TcpStreamDrainAttemptOutcome.SUCCESS)
    )

    assert changed is False
    assert _dimensions(metric) == {
        "total": 0,
        "success_total": 0,
        "timeout_total": 0,
        "error_total": 0,
        "cancelled_total": 0,
        "tls_error_total": 0,
    }


def test_f5_stream_read_attempt_metric_accumulates_multiple_events() -> None:
    metric = TcpStreamStreamReadAttemptsMetric()

    events = [
        TcpStreamStreamReadAttemptMetricEvent(
            outcome=TcpStreamStreamReadAttemptOutcome.SUCCESS,
        ),
        TcpStreamStreamReadAttemptMetricEvent(
            outcome=TcpStreamStreamReadAttemptOutcome.TIMEOUT,
        ),
        TcpStreamStreamReadAttemptMetricEvent(
            outcome=TcpStreamStreamReadAttemptOutcome.ERROR,
        ),
        TcpStreamStreamReadAttemptMetricEvent(
            outcome=TcpStreamStreamReadAttemptOutcome.CANCELLED,
        ),
        TcpStreamStreamReadAttemptMetricEvent(
            outcome=TcpStreamStreamReadAttemptOutcome.TLS_ERROR,
        ),
    ]

    for event in events:
        assert metric.handle_event(event) is True

    assert _dimensions(metric) == {
        "total": 5,
        "success_total": 1,
        "timeout_total": 1,
        "error_total": 1,
        "cancelled_total": 1,
        "tls_error_total": 1,
    }


# -------------------------
# Group g: stream write attempts metric
# -------------------------


def test_g1_stream_write_attempt_metric_has_expected_identity_and_initial_snapshot() -> None:
    metric = TcpStreamStreamWriteAttemptsMetric()

    assert metric.metric_name == "tcp_stream.stream_write.attempts"
    assert metric.snapshot() == {
        "name": "tcp_stream.stream_write.attempts",
        "dimensions": {
            "total": 0,
            "success_total": 0,
            "error_total": 0,
            "tls_error_total": 0,
        },
    }


def test_g2_stream_write_attempt_metric_event_has_expected_type_and_fields() -> None:
    event = TcpStreamStreamWriteAttemptMetricEvent(
        outcome=TcpStreamStreamWriteAttemptOutcome.TLS_ERROR,
    )

    assert event.event_type == "tcp_stream.stream_write.attempt"
    assert event.outcome is TcpStreamStreamWriteAttemptOutcome.TLS_ERROR


@pytest.mark.parametrize(
    ("outcome", "dimension"),
    [
        (TcpStreamStreamWriteAttemptOutcome.SUCCESS, "success_total"),
        (TcpStreamStreamWriteAttemptOutcome.ERROR, "error_total"),
        (TcpStreamStreamWriteAttemptOutcome.TLS_ERROR, "tls_error_total"),
    ],
)
def test_g3_stream_write_attempt_metric_counts_each_outcome(
    outcome: TcpStreamStreamWriteAttemptOutcome,
    dimension: str,
) -> None:
    metric = TcpStreamStreamWriteAttemptsMetric()

    changed = metric.handle_event(TcpStreamStreamWriteAttemptMetricEvent(outcome=outcome))

    assert changed is True
    _assert_single_dimension_incremented(
        dimensions=_dimensions(metric),
        dimension=dimension,
    )


def test_g4_stream_write_attempt_metric_ignores_unrelated_event() -> None:
    metric = TcpStreamStreamWriteAttemptsMetric()

    changed = metric.handle_event(
        TcpStreamStreamReadAttemptMetricEvent(
            outcome=TcpStreamStreamReadAttemptOutcome.SUCCESS,
        )
    )

    assert changed is False
    assert _dimensions(metric) == {
        "total": 0,
        "success_total": 0,
        "error_total": 0,
        "tls_error_total": 0,
    }


def test_g5_stream_write_attempt_metric_accumulates_multiple_events() -> None:
    metric = TcpStreamStreamWriteAttemptsMetric()

    events = [
        TcpStreamStreamWriteAttemptMetricEvent(
            outcome=TcpStreamStreamWriteAttemptOutcome.SUCCESS,
        ),
        TcpStreamStreamWriteAttemptMetricEvent(
            outcome=TcpStreamStreamWriteAttemptOutcome.SUCCESS,
        ),
        TcpStreamStreamWriteAttemptMetricEvent(
            outcome=TcpStreamStreamWriteAttemptOutcome.ERROR,
        ),
        TcpStreamStreamWriteAttemptMetricEvent(
            outcome=TcpStreamStreamWriteAttemptOutcome.TLS_ERROR,
        ),
    ]

    for event in events:
        assert metric.handle_event(event) is True

    assert _dimensions(metric) == {
        "total": 4,
        "success_total": 2,
        "error_total": 1,
        "tls_error_total": 1,
    }


# -------------------------
# Group h: drain attempts metric
# -------------------------


def test_h1_drain_attempt_metric_has_expected_identity_and_initial_snapshot() -> None:
    metric = TcpStreamDrainAttemptsMetric()

    assert metric.metric_name == "tcp_stream.drain.attempts"
    assert metric.snapshot() == {
        "name": "tcp_stream.drain.attempts",
        "dimensions": {
            "total": 0,
            "success_total": 0,
            "timeout_total": 0,
            "error_total": 0,
            "cancelled_total": 0,
            "tls_error_total": 0,
        },
    }


def test_h2_drain_attempt_metric_event_has_expected_type_and_fields() -> None:
    event = TcpStreamDrainAttemptMetricEvent(
        outcome=TcpStreamDrainAttemptOutcome.CANCELLED,
    )

    assert event.event_type == "tcp_stream.drain.attempt"
    assert event.outcome is TcpStreamDrainAttemptOutcome.CANCELLED


@pytest.mark.parametrize(
    ("outcome", "dimension"),
    [
        (TcpStreamDrainAttemptOutcome.SUCCESS, "success_total"),
        (TcpStreamDrainAttemptOutcome.TIMEOUT, "timeout_total"),
        (TcpStreamDrainAttemptOutcome.ERROR, "error_total"),
        (TcpStreamDrainAttemptOutcome.CANCELLED, "cancelled_total"),
        (TcpStreamDrainAttemptOutcome.TLS_ERROR, "tls_error_total"),
    ],
)
def test_h3_drain_attempt_metric_counts_each_outcome(
    outcome: TcpStreamDrainAttemptOutcome,
    dimension: str,
) -> None:
    metric = TcpStreamDrainAttemptsMetric()

    changed = metric.handle_event(TcpStreamDrainAttemptMetricEvent(outcome=outcome))

    assert changed is True
    _assert_single_dimension_incremented(
        dimensions=_dimensions(metric),
        dimension=dimension,
    )


def test_h4_drain_attempt_metric_ignores_unrelated_event() -> None:
    metric = TcpStreamDrainAttemptsMetric()

    changed = metric.handle_event(
        TcpStreamStreamWriteAttemptMetricEvent(
            outcome=TcpStreamStreamWriteAttemptOutcome.SUCCESS,
        )
    )

    assert changed is False
    assert _dimensions(metric) == {
        "total": 0,
        "success_total": 0,
        "timeout_total": 0,
        "error_total": 0,
        "cancelled_total": 0,
        "tls_error_total": 0,
    }


def test_h5_drain_attempt_metric_accumulates_multiple_events() -> None:
    metric = TcpStreamDrainAttemptsMetric()

    events = [
        TcpStreamDrainAttemptMetricEvent(outcome=TcpStreamDrainAttemptOutcome.SUCCESS),
        TcpStreamDrainAttemptMetricEvent(outcome=TcpStreamDrainAttemptOutcome.TIMEOUT),
        TcpStreamDrainAttemptMetricEvent(outcome=TcpStreamDrainAttemptOutcome.ERROR),
        TcpStreamDrainAttemptMetricEvent(outcome=TcpStreamDrainAttemptOutcome.CANCELLED),
        TcpStreamDrainAttemptMetricEvent(outcome=TcpStreamDrainAttemptOutcome.TLS_ERROR),
    ]

    for event in events:
        assert metric.handle_event(event) is True

    assert _dimensions(metric) == {
        "total": 5,
        "success_total": 1,
        "timeout_total": 1,
        "error_total": 1,
        "cancelled_total": 1,
        "tls_error_total": 1,
    }


# -------------------------
# Group i: bytes received metric
# -------------------------


def test_i1_bytes_received_metric_has_expected_identity_and_initial_snapshot() -> None:
    metric = TcpStreamBytesReceivedMetric()

    assert metric.metric_name == "tcp_stream.bytes.received"
    assert metric.snapshot() == {
        "name": "tcp_stream.bytes.received",
        "dimensions": {
            "total": 0,
        },
    }


def test_i2_bytes_received_metric_event_has_expected_type_and_fields() -> None:
    event = TcpStreamBytesReceivedMetricEvent(size=12)

    assert event.event_type == "tcp_stream.bytes.received"
    assert event.size == 12


def test_i3_bytes_received_metric_accumulates_sizes() -> None:
    metric = TcpStreamBytesReceivedMetric()

    assert metric.handle_event(TcpStreamBytesReceivedMetricEvent(size=12)) is True
    assert metric.handle_event(TcpStreamBytesReceivedMetricEvent(size=8)) is True

    assert _dimensions(metric) == {
        "total": 20,
    }


def test_i4_bytes_received_metric_ignores_unrelated_event() -> None:
    metric = TcpStreamBytesReceivedMetric()

    changed = metric.handle_event(TcpStreamBytesSentMetricEvent(size=10))

    assert changed is False
    assert _dimensions(metric) == {
        "total": 0,
    }


# -------------------------
# Group j: bytes sent metric
# -------------------------


def test_j1_bytes_sent_metric_has_expected_identity_and_initial_snapshot() -> None:
    metric = TcpStreamBytesSentMetric()

    assert metric.metric_name == "tcp_stream.bytes.sent"
    assert metric.snapshot() == {
        "name": "tcp_stream.bytes.sent",
        "dimensions": {
            "total": 0,
        },
    }


def test_j2_bytes_sent_metric_event_has_expected_type_and_fields() -> None:
    event = TcpStreamBytesSentMetricEvent(size=15)

    assert event.event_type == "tcp_stream.bytes.sent"
    assert event.size == 15


def test_j3_bytes_sent_metric_accumulates_sizes() -> None:
    metric = TcpStreamBytesSentMetric()

    assert metric.handle_event(TcpStreamBytesSentMetricEvent(size=15)) is True
    assert metric.handle_event(TcpStreamBytesSentMetricEvent(size=5)) is True

    assert _dimensions(metric) == {
        "total": 20,
    }


def test_j4_bytes_sent_metric_ignores_unrelated_event() -> None:
    metric = TcpStreamBytesSentMetric()

    changed = metric.handle_event(TcpStreamBytesReceivedMetricEvent(size=10))

    assert changed is False
    assert _dimensions(metric) == {
        "total": 0,
    }


# -------------------------
# Group k: remote disconnect metric
# -------------------------


def test_k1_remote_disconnect_metric_has_expected_identity_and_initial_snapshot() -> None:
    metric = TcpStreamRemoteDisconnectMetric()

    assert metric.metric_name == "tcp_stream.remote_disconnect"
    assert metric.snapshot() == {
        "name": "tcp_stream.remote_disconnect",
        "dimensions": {
            "total": 0,
        },
    }


def test_k2_remote_disconnect_metric_event_has_expected_type() -> None:
    event = TcpStreamRemoteDisconnectMetricEvent()

    assert event.event_type == "tcp_stream.remote_disconnect"


def test_k3_remote_disconnect_metric_counts_events() -> None:
    metric = TcpStreamRemoteDisconnectMetric()

    assert metric.handle_event(TcpStreamRemoteDisconnectMetricEvent()) is True
    assert metric.handle_event(TcpStreamRemoteDisconnectMetricEvent()) is True

    assert _dimensions(metric) == {
        "total": 2,
    }


def test_k4_remote_disconnect_metric_ignores_unrelated_event() -> None:
    metric = TcpStreamRemoteDisconnectMetric()

    changed = metric.handle_event(TcpStreamAbortiveCloseMetricEvent())

    assert changed is False
    assert _dimensions(metric) == {
        "total": 0,
    }


# -------------------------
# Group l: abortive close metric
# -------------------------


def test_l1_abortive_close_metric_has_expected_identity_and_initial_snapshot() -> None:
    metric = TcpStreamAbortiveCloseMetric()

    assert metric.metric_name == "tcp_stream.abortive_close"
    assert metric.snapshot() == {
        "name": "tcp_stream.abortive_close",
        "dimensions": {
            "total": 0,
        },
    }


def test_l2_abortive_close_metric_event_has_expected_type() -> None:
    event = TcpStreamAbortiveCloseMetricEvent()

    assert event.event_type == "tcp_stream.abortive_close"


def test_l3_abortive_close_metric_counts_events() -> None:
    metric = TcpStreamAbortiveCloseMetric()

    assert metric.handle_event(TcpStreamAbortiveCloseMetricEvent()) is True
    assert metric.handle_event(TcpStreamAbortiveCloseMetricEvent()) is True
    assert metric.handle_event(TcpStreamAbortiveCloseMetricEvent()) is True

    assert _dimensions(metric) == {
        "total": 3,
    }


def test_l4_abortive_close_metric_ignores_unrelated_event() -> None:
    metric = TcpStreamAbortiveCloseMetric()

    changed = metric.handle_event(TcpStreamRemoteDisconnectMetricEvent())

    assert changed is False
    assert _dimensions(metric) == {
        "total": 0,
    }
