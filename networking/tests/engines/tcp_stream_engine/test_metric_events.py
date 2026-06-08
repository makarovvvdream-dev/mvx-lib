# tests/engines/tcp_stream_engine/test_metric_events.py

"""
Tests for tcp_stream_engine metric events.

Grouping rule:
  - Group a: open metric event
  - Group b: close metric event
  - Group c: start_tls metric event
  - Group d: crypto codec attach metric event
  - Group e: crypto codec detach metric event
  - Group f: stream read metric event
  - Group g: stream write metric event
  - Group h: drain metric event
  - Group i: common timed metric event behavior

Naming rule:
  Each test name starts with test_<group><num>_, e.g. test_a1_...
"""

from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from mvx.networking.engines.tcp_stream_engine.metric_events import (
    _TimedMetricEvent,
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


class _BareTimedMetricEvent(_TimedMetricEvent[TcpStreamCloseResult]):
    pass


# -------------------------
# Group a: open metric event
# -------------------------


@pytest.mark.parametrize(
    "result",
    [
        TcpStreamOpenResult.SUCCEEDED,
        TcpStreamOpenResult.ALREADY_OPENED,
        TcpStreamOpenResult.FAILED,
        TcpStreamOpenResult.CANCELLED,
    ],
)
def test_a1_open_metric_event_has_expected_type_and_fields(
    result: TcpStreamOpenResult,
) -> None:
    event = TcpStreamOpenMetricEvent(use_ssl=True)

    assert event.event_type == "tcp_stream.open"
    assert event.use_ssl is True

    with pytest.raises(RuntimeError, match="result is not set"):
        _ = event.result

    # noinspection PyProtectedMember
    returned = event._set_result(result)

    assert returned is event
    assert event.result is result
    assert event.duration_ns >= 0


# -------------------------
# Group b: close metric event
# -------------------------


@pytest.mark.parametrize(
    "result",
    [
        TcpStreamCloseResult.SUCCEEDED,
        TcpStreamCloseResult.NOT_OPENED,
        TcpStreamCloseResult.FAILED,
        TcpStreamCloseResult.CANCELLED,
    ],
)
def test_b1_close_metric_event_has_expected_type_and_fields(
    result: TcpStreamCloseResult,
) -> None:
    event = TcpStreamCloseMetricEvent()

    assert event.event_type == "tcp_stream.close"

    with pytest.raises(RuntimeError, match="result is not set"):
        _ = event.result

    # noinspection PyProtectedMember
    returned = event._set_result(result)

    assert returned is event
    assert event.result is result
    assert event.duration_ns >= 0


# -------------------------
# Group c: start_tls metric event
# -------------------------


@pytest.mark.parametrize(
    "result",
    [
        TcpStreamStartTlsResult.SUCCEEDED,
        TcpStreamStartTlsResult.FAILED,
        TcpStreamStartTlsResult.CANCELLED,
        TcpStreamStartTlsResult.TIMED_OUT,
        TcpStreamStartTlsResult.REFUSED_NOT_OPENED,
        TcpStreamStartTlsResult.REFUSED_ALREADY_UNDER_SSL,
        TcpStreamStartTlsResult.REFUSED_START_TLS_ALREADY_ACTIVE,
        TcpStreamStartTlsResult.REFUSED_CRYPTO_CODEC_ATTACHED,
        TcpStreamStartTlsResult.TLS_FAILED,
    ],
)
def test_c1_start_tls_metric_event_has_expected_type_and_fields(
    result: TcpStreamStartTlsResult,
) -> None:
    event = TcpStreamStartTlsMetricEvent()

    assert event.event_type == "tcp_stream.start_tls"

    with pytest.raises(RuntimeError, match="result is not set"):
        _ = event.result

    # noinspection PyProtectedMember
    returned = event._set_result(result)

    assert returned is event
    assert event.result is result
    assert event.duration_ns >= 0


# -------------------------
# Group d: crypto codec attach metric event
# -------------------------


@pytest.mark.parametrize(
    "result",
    [
        TcpStreamCryptoCodecAttachResult.SUCCEEDED,
        TcpStreamCryptoCodecAttachResult.FAILED,
        TcpStreamCryptoCodecAttachResult.CANCELLED,
        TcpStreamCryptoCodecAttachResult.REFUSED_NOT_OPENED,
        TcpStreamCryptoCodecAttachResult.REFUSED_ALREADY_UNDER_SSL,
        TcpStreamCryptoCodecAttachResult.REFUSED_START_TLS_ACTIVE,
        TcpStreamCryptoCodecAttachResult.REFUSED_ALREADY_ATTACHED,
    ],
)
def test_d1_crypto_codec_attach_metric_event_has_expected_type_and_fields(
    result: TcpStreamCryptoCodecAttachResult,
) -> None:
    event = TcpStreamCryptoCodecAttachMetricEvent()

    assert event.event_type == "tcp_stream.crypto_codec.attach"

    with pytest.raises(RuntimeError, match="result is not set"):
        _ = event.result

    # noinspection PyProtectedMember
    returned = event._set_result(result)

    assert returned is event
    assert event.result is result
    assert event.duration_ns >= 0


# -------------------------
# Group e: crypto codec detach metric event
# -------------------------


@pytest.mark.parametrize(
    "result",
    [
        TcpStreamCryptoCodecDetachResult.SUCCEEDED,
        TcpStreamCryptoCodecDetachResult.FAILED,
        TcpStreamCryptoCodecDetachResult.CANCELLED,
        TcpStreamCryptoCodecDetachResult.REFUSED_NOT_OPENED,
        TcpStreamCryptoCodecDetachResult.REFUSED_NOT_ATTACHED,
    ],
)
def test_e1_crypto_codec_detach_metric_event_has_expected_type_and_fields(
    result: TcpStreamCryptoCodecDetachResult,
) -> None:
    event = TcpStreamCryptoCodecDetachMetricEvent()

    assert event.event_type == "tcp_stream.crypto_codec.detach"

    with pytest.raises(RuntimeError, match="result is not set"):
        _ = event.result

    # noinspection PyProtectedMember
    returned = event._set_result(result)

    assert returned is event
    assert event.result is result
    assert event.duration_ns >= 0


# -------------------------
# Group f: stream read metric event
# -------------------------


@pytest.mark.parametrize(
    "result",
    [
        TcpStreamStreamReadResult.SUCCEEDED,
        TcpStreamStreamReadResult.TIMED_OUT,
        TcpStreamStreamReadResult.FAILED,
        TcpStreamStreamReadResult.CANCELLED,
        TcpStreamStreamReadResult.TLS_FAILED,
        TcpStreamStreamReadResult.REMOTE_DISCONNECTED,
    ],
)
def test_f1_stream_read_metric_event_has_expected_type_and_fields(
    result: TcpStreamStreamReadResult,
) -> None:
    event = TcpStreamStreamReadMetricEvent()

    assert event.event_type == "tcp_stream.stream_read"
    assert event.bytes_count == 0

    with pytest.raises(RuntimeError, match="result is not set"):
        _ = event.result

    # noinspection PyProtectedMember
    returned = event._set_result(result, bytes_count=456)

    assert returned is event
    assert event.result is result
    assert event.duration_ns >= 0
    assert event.bytes_count == 456


def test_f2_stream_read_metric_event_defaults_bytes_count_to_zero() -> None:
    event = TcpStreamStreamReadMetricEvent()

    assert event.bytes_count == 0

    # noinspection PyProtectedMember
    event._set_result(TcpStreamStreamReadResult.SUCCEEDED)

    assert event.result is TcpStreamStreamReadResult.SUCCEEDED
    assert event.bytes_count == 0


@pytest.mark.parametrize(
    "bytes_count",
    [
        True,
        1.5,
        "1",
        None,
    ],
)
def test_f3_stream_read_metric_event_rejects_invalid_bytes_count_type(
    bytes_count: object,
) -> None:
    event = TcpStreamStreamReadMetricEvent()

    with pytest.raises(TypeError, match="bytes_count"):
        # noinspection PyProtectedMember
        event._set_result(
            TcpStreamStreamReadResult.SUCCEEDED,
            bytes_count=bytes_count,  # type: ignore[arg-type]
        )


def test_f4_stream_read_metric_event_rejects_negative_bytes_count() -> None:
    event = TcpStreamStreamReadMetricEvent()

    with pytest.raises(ValueError, match="bytes_count"):
        # noinspection PyProtectedMember
        event._set_result(
            TcpStreamStreamReadResult.SUCCEEDED,
            bytes_count=-1,
        )


def test_f5_stream_read_metric_event_rejects_setting_result_after_send() -> None:
    event = TcpStreamStreamReadMetricEvent()

    # noinspection PyProtectedMember
    event._set_result(TcpStreamStreamReadResult.SUCCEEDED, bytes_count=123)

    # noinspection PyProtectedMember
    event._pre_send_check()

    with pytest.raises(RuntimeError, match="already sent"):
        # noinspection PyProtectedMember
        event._set_result(TcpStreamStreamReadResult.FAILED, bytes_count=456)


def test_f6_stream_read_metric_event_rejects_setting_result_twice() -> None:
    event = TcpStreamStreamReadMetricEvent()

    # noinspection PyProtectedMember
    event._set_result(TcpStreamStreamReadResult.SUCCEEDED, bytes_count=123)

    with pytest.raises(RuntimeError, match="result is already set"):
        # noinspection PyProtectedMember
        event._set_result(TcpStreamStreamReadResult.FAILED, bytes_count=456)


# -------------------------
# Group g: stream write metric event
# -------------------------


@pytest.mark.parametrize(
    "result",
    [
        TcpStreamStreamWriteResult.SUCCEEDED,
        TcpStreamStreamWriteResult.FAILED,
        TcpStreamStreamWriteResult.TLS_FAILED,
    ],
)
def test_g1_stream_write_metric_event_has_expected_type_and_fields(
    result: TcpStreamStreamWriteResult,
) -> None:
    event = TcpStreamStreamWriteMetricEvent()

    assert event.event_type == "tcp_stream.stream_write"
    assert event.bytes_count == 0

    with pytest.raises(RuntimeError, match="result is not set"):
        _ = event.result

    # noinspection PyProtectedMember
    returned = event._set_result(result, bytes_count=456)

    assert returned is event
    assert event.result is result
    assert event.duration_ns >= 0
    assert event.bytes_count == 456


def test_g2_stream_write_metric_event_defaults_bytes_count_to_zero() -> None:
    event = TcpStreamStreamWriteMetricEvent()

    assert event.bytes_count == 0

    # noinspection PyProtectedMember
    event._set_result(TcpStreamStreamWriteResult.SUCCEEDED)

    assert event.result is TcpStreamStreamWriteResult.SUCCEEDED
    assert event.bytes_count == 0


@pytest.mark.parametrize(
    "bytes_count",
    [
        True,
        1.5,
        "1",
        None,
    ],
)
def test_g3_stream_write_metric_event_rejects_invalid_bytes_count_type(
    bytes_count: object,
) -> None:
    event = TcpStreamStreamWriteMetricEvent()

    with pytest.raises(TypeError, match="bytes_count"):
        # noinspection PyProtectedMember
        event._set_result(
            TcpStreamStreamWriteResult.SUCCEEDED,
            bytes_count=bytes_count,  # type: ignore[arg-type]
        )


def test_g4_stream_write_metric_event_rejects_negative_bytes_count() -> None:
    event = TcpStreamStreamWriteMetricEvent()

    with pytest.raises(ValueError, match="bytes_count"):
        # noinspection PyProtectedMember
        event._set_result(
            TcpStreamStreamWriteResult.SUCCEEDED,
            bytes_count=-1,
        )


def test_g5_stream_write_metric_event_rejects_setting_result_after_send() -> None:
    event = TcpStreamStreamWriteMetricEvent()

    # noinspection PyProtectedMember
    event._set_result(TcpStreamStreamWriteResult.SUCCEEDED, bytes_count=123)

    # noinspection PyProtectedMember
    event._pre_send_check()

    with pytest.raises(RuntimeError, match="already sent"):
        # noinspection PyProtectedMember
        event._set_result(TcpStreamStreamWriteResult.FAILED, bytes_count=456)


def test_g6_stream_write_metric_event_rejects_setting_result_twice() -> None:
    event = TcpStreamStreamWriteMetricEvent()

    # noinspection PyProtectedMember
    event._set_result(TcpStreamStreamWriteResult.SUCCEEDED, bytes_count=123)

    with pytest.raises(RuntimeError, match="result is already set"):
        # noinspection PyProtectedMember
        event._set_result(TcpStreamStreamWriteResult.FAILED, bytes_count=456)


# -------------------------
# Group h: drain metric event
# -------------------------


@pytest.mark.parametrize(
    "result",
    [
        TcpStreamDrainResult.SUCCEEDED,
        TcpStreamDrainResult.TIMED_OUT,
        TcpStreamDrainResult.FAILED,
        TcpStreamDrainResult.CANCELLED,
        TcpStreamDrainResult.TLS_FAILED,
    ],
)
def test_h1_drain_metric_event_has_expected_type_and_fields(
    result: TcpStreamDrainResult,
) -> None:
    event = TcpStreamDrainMetricEvent()

    assert event.event_type == "tcp_stream.drain"

    with pytest.raises(RuntimeError, match="result is not set"):
        _ = event.result

    # noinspection PyProtectedMember
    returned = event._set_result(result)

    assert returned is event
    assert event.result is result
    assert event.duration_ns >= 0


# -------------------------
# Group i: common timed metric event behavior
# -------------------------


def test_i1_metric_event_duration_grows_before_result_is_set() -> None:
    event = TcpStreamCloseMetricEvent()

    first_duration = event.duration_ns
    second_duration = event.duration_ns

    assert first_duration >= 0
    assert second_duration >= first_duration


def test_i2_metric_event_duration_is_stable_after_result_is_set() -> None:
    event = TcpStreamCloseMetricEvent()

    # noinspection PyProtectedMember
    event._set_result(TcpStreamCloseResult.SUCCEEDED)

    first_duration = event.duration_ns
    second_duration = event.duration_ns

    assert first_duration >= 0
    assert second_duration == first_duration


def test_i3_metric_event_rejects_none_result() -> None:
    event = TcpStreamCloseMetricEvent()

    with pytest.raises(ValueError, match="result"):
        # noinspection PyProtectedMember
        event._set_result(None)  # type: ignore[arg-type]


def test_i4_metric_event_rejects_setting_result_twice() -> None:
    event = TcpStreamCloseMetricEvent()

    # noinspection PyProtectedMember
    event._set_result(TcpStreamCloseResult.SUCCEEDED)

    with pytest.raises(RuntimeError, match="result is already set"):
        # noinspection PyProtectedMember
        event._set_result(TcpStreamCloseResult.FAILED)


def test_i5_metric_event_rejects_send_before_result_is_set() -> None:
    event = TcpStreamCloseMetricEvent()

    with pytest.raises(RuntimeError, match="result is not set"):
        # noinspection PyProtectedMember
        event._pre_send_check()


def test_i6_metric_event_rejects_send_when_finish_time_is_not_set() -> None:
    event = TcpStreamCloseMetricEvent()

    # noinspection PyProtectedMember
    event._result = TcpStreamCloseResult.SUCCEEDED

    with pytest.raises(RuntimeError, match="finish time is not set"):
        # noinspection PyProtectedMember
        event._pre_send_check()


def test_i7_metric_event_rejects_double_send() -> None:
    event = TcpStreamCloseMetricEvent()

    # noinspection PyProtectedMember
    event._set_result(TcpStreamCloseResult.SUCCEEDED)

    # noinspection PyProtectedMember
    event._pre_send_check()

    with pytest.raises(RuntimeError, match="already sent"):
        # noinspection PyProtectedMember
        event._pre_send_check()


def test_i8_metric_event_rejects_setting_result_after_send() -> None:
    event = TcpStreamCloseMetricEvent()

    # noinspection PyProtectedMember
    event._set_result(TcpStreamCloseResult.SUCCEEDED)

    # noinspection PyProtectedMember
    event._pre_send_check()

    with pytest.raises(RuntimeError, match="already sent"):
        # noinspection PyProtectedMember
        event._set_result(TcpStreamCloseResult.FAILED)


def test_i9_base_metric_event_requires_event_type_implementation() -> None:
    event = _BareTimedMetricEvent()

    with pytest.raises(NotImplementedError):
        _ = event.event_type
