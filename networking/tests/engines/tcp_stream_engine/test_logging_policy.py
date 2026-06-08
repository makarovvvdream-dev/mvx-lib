# tests/engines/tcp_stream_engine/test_logging_policy.py
"""
Tests for mvx.networking.engines.tcp_stream_engine.logging_policy.

Grouping rule:
  - Group a: event constants and mode values
  - Group b: config builder validation
  - Group c: silent mode
  - Group d: normal mode
  - Group e: inspection mode
  - Group f: unrelated events

Naming rule:
  Each test name starts with test_<group><num>_, e.g. test_a1_...
"""

from __future__ import annotations

from typing import cast
from dataclasses import dataclass

import pytest

from mvx.common.logger import LogEventMeta
from mvx.networking.engines.tcp_stream_engine.logging_policy import (
    TCP_STREAM_ENGINE_ALL_LOG_EVENTS,
    TCP_STREAM_ENGINE_HOT_PATH_LOG_EVENTS,
    TCP_STREAM_ENGINE_LOG_EVENT_ABORTIVE_CLOSE,
    TCP_STREAM_ENGINE_LOG_EVENT_ATTACH_CRYPTO_CODEC,
    TCP_STREAM_ENGINE_LOG_EVENT_CLOSE,
    TCP_STREAM_ENGINE_LOG_EVENT_DETACH_CRYPTO_CODEC,
    TCP_STREAM_ENGINE_LOG_EVENT_DRAIN,
    TCP_STREAM_ENGINE_LOG_EVENT_OPEN,
    TCP_STREAM_ENGINE_LOG_EVENT_READ,
    TCP_STREAM_ENGINE_LOG_EVENT_START_TLS,
    TCP_STREAM_ENGINE_LOG_EVENT_WRITE,
    TCP_STREAM_ENGINE_NORMAL_LOG_EVENTS,
    TcpStreamEngineLogPolicyMode,
    tcp_stream_engine_event_policy,
    tcp_stream_engine_event_policy_config,
)
from mvx.common.logger.pattern_event_policy import (
    PatternLogEventPolicyAction,
    PatternLogEventPolicyConfig,
)


@dataclass(frozen=True, slots=True)
class _FakeLogEventMeta:
    event_namespace: str | None
    event_name: str | None
    entity_id: str | None = "engine-1"
    source_path: str | None = "src/mvx/networking/engines/tcp_stream_engine/tcp_stream_engine.py"
    source_line: int | None = 100
    source_func: str | None = None


def _event(event: str) -> LogEventMeta:
    namespace, name = event.rsplit(".", maxsplit=1)

    return cast(
        LogEventMeta,
        cast(
            object,
            _FakeLogEventMeta(
                event_namespace=namespace,
                event_name=name,
                source_func=name,
            ),
        ),
    )


# -------------------------
# Group a: event constants and mode values
# -------------------------


def test_a1_event_name_constants_are_stable() -> None:
    """TCP stream engine event name constants are stable."""
    assert TCP_STREAM_ENGINE_LOG_EVENT_OPEN == "tcp_stream_engine.open"
    assert TCP_STREAM_ENGINE_LOG_EVENT_CLOSE == "tcp_stream_engine.close"
    assert TCP_STREAM_ENGINE_LOG_EVENT_START_TLS == "tcp_stream_engine.start_tls"
    assert (
        TCP_STREAM_ENGINE_LOG_EVENT_ATTACH_CRYPTO_CODEC == "tcp_stream_engine.attach_crypto_codec"
    )
    assert (
        TCP_STREAM_ENGINE_LOG_EVENT_DETACH_CRYPTO_CODEC == "tcp_stream_engine.detach_crypto_codec"
    )
    assert TCP_STREAM_ENGINE_LOG_EVENT_READ == "tcp_stream_engine.read"
    assert TCP_STREAM_ENGINE_LOG_EVENT_WRITE == "tcp_stream_engine.write"
    assert TCP_STREAM_ENGINE_LOG_EVENT_DRAIN == "tcp_stream_engine.drain"
    assert TCP_STREAM_ENGINE_LOG_EVENT_ABORTIVE_CLOSE == "tcp_stream_engine.abortive_close"


def test_a2_normal_events_are_stable() -> None:
    """Normal event group is stable."""
    assert TCP_STREAM_ENGINE_NORMAL_LOG_EVENTS == (
        TCP_STREAM_ENGINE_LOG_EVENT_OPEN,
        TCP_STREAM_ENGINE_LOG_EVENT_CLOSE,
        TCP_STREAM_ENGINE_LOG_EVENT_START_TLS,
        TCP_STREAM_ENGINE_LOG_EVENT_ATTACH_CRYPTO_CODEC,
        TCP_STREAM_ENGINE_LOG_EVENT_DETACH_CRYPTO_CODEC,
        TCP_STREAM_ENGINE_LOG_EVENT_ABORTIVE_CLOSE,
    )


def test_a3_hot_path_events_are_stable() -> None:
    """Hot path event group is stable."""
    assert TCP_STREAM_ENGINE_HOT_PATH_LOG_EVENTS == (
        TCP_STREAM_ENGINE_LOG_EVENT_READ,
        TCP_STREAM_ENGINE_LOG_EVENT_WRITE,
        TCP_STREAM_ENGINE_LOG_EVENT_DRAIN,
    )


def test_a4_all_events_are_normal_plus_hot_path() -> None:
    """All event group is normal events plus hot path events."""
    assert TCP_STREAM_ENGINE_ALL_LOG_EVENTS == (
        *TCP_STREAM_ENGINE_NORMAL_LOG_EVENTS,
        *TCP_STREAM_ENGINE_HOT_PATH_LOG_EVENTS,
    )


def test_a5_mode_values_are_stable() -> None:
    """TCP stream engine log policy mode values are stable."""
    assert TcpStreamEngineLogPolicyMode.SILENT.value == "silent"
    assert TcpStreamEngineLogPolicyMode.NORMAL.value == "normal"
    assert TcpStreamEngineLogPolicyMode.INSPECTION.value == "inspection"


# -------------------------
# Group b: config builder validation
# -------------------------


def test_b1_config_builder_rejects_invalid_mode_type() -> None:
    """Config builder rejects non-TcpStreamEngineLogPolicyMode mode."""
    with pytest.raises(
        TypeError,
        match="argument 'mode' must be an instance of 'TcpStreamEngineLogPolicyMode'",
    ):
        tcp_stream_engine_event_policy_config(mode=cast(TcpStreamEngineLogPolicyMode, "normal"))


def test_b2_policy_builder_rejects_invalid_mode_type() -> None:
    """Policy builder rejects non-TcpStreamEngineLogPolicyMode mode."""
    with pytest.raises(
        TypeError,
        match="argument 'mode' must be an instance of 'TcpStreamEngineLogPolicyMode'",
    ):
        tcp_stream_engine_event_policy(mode=cast(TcpStreamEngineLogPolicyMode, "normal"))


# -------------------------
# Group c: silent mode
# -------------------------


def test_c1_silent_config_has_default_disabled_and_no_rules() -> None:
    """Silent config disables default and contains no allow rules."""
    config = tcp_stream_engine_event_policy_config(mode=TcpStreamEngineLogPolicyMode.SILENT)

    assert isinstance(config, PatternLogEventPolicyConfig)
    assert config.default_enabled is False
    assert config.rules == ()


@pytest.mark.parametrize("event", TCP_STREAM_ENGINE_ALL_LOG_EVENTS)
def test_c2_silent_policy_disables_all_tcp_stream_engine_events(event: str) -> None:
    """Silent policy disables all TCP stream engine events."""
    policy = tcp_stream_engine_event_policy(mode=TcpStreamEngineLogPolicyMode.SILENT)

    assert policy.is_event_enabled(_event(event)) is False


# -------------------------
# Group d: normal mode
# -------------------------


def test_d1_normal_config_allows_only_normal_events() -> None:
    """Normal config allows only normal event group."""
    # noinspection PyArgumentEqualDefault
    config = tcp_stream_engine_event_policy_config(mode=TcpStreamEngineLogPolicyMode.NORMAL)

    assert config.default_enabled is False
    assert len(config.rules) == 1
    assert config.rules[0].action is PatternLogEventPolicyAction.ALLOW
    assert config.rules[0].events == TCP_STREAM_ENGINE_NORMAL_LOG_EVENTS


@pytest.mark.parametrize("event", TCP_STREAM_ENGINE_NORMAL_LOG_EVENTS)
def test_d2_normal_policy_enables_normal_events(event: str) -> None:
    """Normal policy enables normal events."""
    # noinspection PyArgumentEqualDefault
    policy = tcp_stream_engine_event_policy(mode=TcpStreamEngineLogPolicyMode.NORMAL)

    assert policy.is_event_enabled(_event(event)) is True


@pytest.mark.parametrize("event", TCP_STREAM_ENGINE_HOT_PATH_LOG_EVENTS)
def test_d3_normal_policy_disables_hot_path_events(event: str) -> None:
    """Normal policy disables hot path events."""
    # noinspection PyArgumentEqualDefault
    policy = tcp_stream_engine_event_policy(mode=TcpStreamEngineLogPolicyMode.NORMAL)

    assert policy.is_event_enabled(_event(event)) is False


def test_d4_normal_mode_is_default_for_config_builder() -> None:
    """Config builder uses normal mode by default."""
    default_config = tcp_stream_engine_event_policy_config()
    # noinspection PyArgumentEqualDefault
    normal_config = tcp_stream_engine_event_policy_config(mode=TcpStreamEngineLogPolicyMode.NORMAL)

    assert default_config == normal_config


def test_d5_normal_mode_is_default_for_policy_builder() -> None:
    """Policy builder uses normal mode by default."""
    policy = tcp_stream_engine_event_policy()

    assert policy.is_event_enabled(_event(TCP_STREAM_ENGINE_LOG_EVENT_OPEN)) is True
    assert policy.is_event_enabled(_event(TCP_STREAM_ENGINE_LOG_EVENT_READ)) is False


# -------------------------
# Group e: inspection mode
# -------------------------


def test_e1_inspection_config_allows_all_events() -> None:
    """Inspection config allows all TCP stream engine events."""
    config = tcp_stream_engine_event_policy_config(mode=TcpStreamEngineLogPolicyMode.INSPECTION)

    assert config.default_enabled is False
    assert len(config.rules) == 1
    assert config.rules[0].action is PatternLogEventPolicyAction.ALLOW
    assert config.rules[0].events == TCP_STREAM_ENGINE_ALL_LOG_EVENTS


@pytest.mark.parametrize("event", TCP_STREAM_ENGINE_ALL_LOG_EVENTS)
def test_e2_inspection_policy_enables_all_tcp_stream_engine_events(event: str) -> None:
    """Inspection policy enables all TCP stream engine events."""
    policy = tcp_stream_engine_event_policy(mode=TcpStreamEngineLogPolicyMode.INSPECTION)

    assert policy.is_event_enabled(_event(event)) is True


# -------------------------
# Group f: unrelated events
# -------------------------


@pytest.mark.parametrize(
    "mode",
    [
        TcpStreamEngineLogPolicyMode.SILENT,
        TcpStreamEngineLogPolicyMode.NORMAL,
        TcpStreamEngineLogPolicyMode.INSPECTION,
    ],
)
def test_f1_policy_disables_unrelated_events_in_all_modes(
    mode: TcpStreamEngineLogPolicyMode,
) -> None:
    """TCP stream engine policy disables unrelated events in all modes."""
    policy = tcp_stream_engine_event_policy(mode=mode)

    assert policy.is_event_enabled(_event("other_engine.open")) is False
