# src/mvx/networking/engines/tcp_stream_engine/logging_policy.py

from __future__ import annotations

from enum import StrEnum

from mvx.common.logger.pattern_event_policy import (
    PatternLogEventPolicy,
    PatternLogEventPolicyAction,
    PatternLogEventPolicyConfig,
    PatternLogEventPolicyRuleConfig,
)

__all__ = (
    "TcpStreamEngineLogPolicyMode",
    "TCP_STREAM_ENGINE_LOG_EVENT_OPEN",
    "TCP_STREAM_ENGINE_LOG_EVENT_CLOSE",
    "TCP_STREAM_ENGINE_LOG_EVENT_START_TLS",
    "TCP_STREAM_ENGINE_LOG_EVENT_ATTACH_CRYPTO_CODEC",
    "TCP_STREAM_ENGINE_LOG_EVENT_DETACH_CRYPTO_CODEC",
    "TCP_STREAM_ENGINE_LOG_EVENT_READ",
    "TCP_STREAM_ENGINE_LOG_EVENT_WRITE",
    "TCP_STREAM_ENGINE_LOG_EVENT_DRAIN",
    "TCP_STREAM_ENGINE_LOG_EVENT_ABORTIVE_CLOSE",
    "TCP_STREAM_ENGINE_NORMAL_LOG_EVENTS",
    "TCP_STREAM_ENGINE_HOT_PATH_LOG_EVENTS",
    "TCP_STREAM_ENGINE_ALL_LOG_EVENTS",
    "tcp_stream_engine_event_policy_config",
    "tcp_stream_engine_event_policy",
)


class TcpStreamEngineLogPolicyMode(StrEnum):
    SILENT = "silent"
    NORMAL = "normal"
    INSPECTION = "inspection"


TCP_STREAM_ENGINE_LOG_EVENT_OPEN = "tcp_stream_engine.open"
TCP_STREAM_ENGINE_LOG_EVENT_CLOSE = "tcp_stream_engine.close"
TCP_STREAM_ENGINE_LOG_EVENT_START_TLS = "tcp_stream_engine.start_tls"
TCP_STREAM_ENGINE_LOG_EVENT_ATTACH_CRYPTO_CODEC = "tcp_stream_engine.attach_crypto_codec"
TCP_STREAM_ENGINE_LOG_EVENT_DETACH_CRYPTO_CODEC = "tcp_stream_engine.detach_crypto_codec"
TCP_STREAM_ENGINE_LOG_EVENT_READ = "tcp_stream_engine.read"
TCP_STREAM_ENGINE_LOG_EVENT_WRITE = "tcp_stream_engine.write"
TCP_STREAM_ENGINE_LOG_EVENT_DRAIN = "tcp_stream_engine.drain"
TCP_STREAM_ENGINE_LOG_EVENT_ABORTIVE_CLOSE = "tcp_stream_engine.abortive_close"


TCP_STREAM_ENGINE_NORMAL_LOG_EVENTS: tuple[str, ...] = (
    TCP_STREAM_ENGINE_LOG_EVENT_OPEN,
    TCP_STREAM_ENGINE_LOG_EVENT_CLOSE,
    TCP_STREAM_ENGINE_LOG_EVENT_START_TLS,
    TCP_STREAM_ENGINE_LOG_EVENT_ATTACH_CRYPTO_CODEC,
    TCP_STREAM_ENGINE_LOG_EVENT_DETACH_CRYPTO_CODEC,
    TCP_STREAM_ENGINE_LOG_EVENT_ABORTIVE_CLOSE,
)


TCP_STREAM_ENGINE_HOT_PATH_LOG_EVENTS: tuple[str, ...] = (
    TCP_STREAM_ENGINE_LOG_EVENT_READ,
    TCP_STREAM_ENGINE_LOG_EVENT_WRITE,
    TCP_STREAM_ENGINE_LOG_EVENT_DRAIN,
)


TCP_STREAM_ENGINE_ALL_LOG_EVENTS: tuple[str, ...] = (
    *TCP_STREAM_ENGINE_NORMAL_LOG_EVENTS,
    *TCP_STREAM_ENGINE_HOT_PATH_LOG_EVENTS,
)


def tcp_stream_engine_event_policy_config(
    *,
    mode: TcpStreamEngineLogPolicyMode = TcpStreamEngineLogPolicyMode.NORMAL,
) -> PatternLogEventPolicyConfig:
    if not isinstance(mode, TcpStreamEngineLogPolicyMode):
        raise TypeError("argument 'mode' must be an instance of 'TcpStreamEngineLogPolicyMode'")

    if mode is TcpStreamEngineLogPolicyMode.SILENT:
        return PatternLogEventPolicyConfig(default_enabled=False)

    events: tuple[str, ...]

    if mode is TcpStreamEngineLogPolicyMode.NORMAL:
        events = TCP_STREAM_ENGINE_NORMAL_LOG_EVENTS
    else:
        events = TCP_STREAM_ENGINE_ALL_LOG_EVENTS

    return PatternLogEventPolicyConfig(
        default_enabled=False,
        rules=(
            PatternLogEventPolicyRuleConfig(
                action=PatternLogEventPolicyAction.ALLOW,
                events=events,
            ),
        ),
    )


def tcp_stream_engine_event_policy(
    *,
    mode: TcpStreamEngineLogPolicyMode = TcpStreamEngineLogPolicyMode.NORMAL,
) -> PatternLogEventPolicy:
    return PatternLogEventPolicy(
        tcp_stream_engine_event_policy_config(
            mode=mode,
        )
    )
