# src/mvx/common/metrics/metrics_runtime/logging_policy.py

from __future__ import annotations

from enum import StrEnum

from mvx.common.logger.pattern_event_policy import (
    PatternLogEventPolicy,
    PatternLogEventPolicyAction,
    PatternLogEventPolicyConfig,
    PatternLogEventPolicyRuleConfig,
)

__all__ = (
    "MetricsRuntimeLogPolicyMode",
    "metrics_runtime_event_policy_config",
    "metrics_runtime_event_policy",
)


class MetricsRuntimeLogPolicyMode(StrEnum):
    SILENT = "silent"
    NORMAL = "normal"
    INSPECTION = "inspection"


_METRICS_RUNTIME_LOG_EVENT_START = "metrics_runtime.start"
_METRICS_RUNTIME_LOG_EVENT_SHUTDOWN = "metrics_runtime.shutdown"

_METRICS_RUNTIME_LOG_EVENT_CREATE_RECORDER = "metrics_runtime.create_recorder"
_METRICS_RUNTIME_LOG_EVENT_STOP_RECORDER = "metrics_runtime.stop_recorder"
_METRICS_RUNTIME_LOG_EVENT_STOP_AND_REMOVE_RECORDER = "metrics_runtime.stop_and_remove_recorder"

_METRICS_RUNTIME_LOG_EVENT_GET_RECORDER = "metrics_runtime.get_recorder"
_METRICS_RUNTIME_LOG_EVENT_TRY_GET_RECORDER = "metrics_runtime.try_get_recorder"
_METRICS_RUNTIME_LOG_EVENT_LIST_RECORDER_IDS = "metrics_runtime.list_recorder_ids"


_METRICS_RUNTIME_NORMAL_LOG_EVENTS: tuple[str, ...] = (
    _METRICS_RUNTIME_LOG_EVENT_START,
    _METRICS_RUNTIME_LOG_EVENT_SHUTDOWN,
    _METRICS_RUNTIME_LOG_EVENT_CREATE_RECORDER,
    _METRICS_RUNTIME_LOG_EVENT_STOP_RECORDER,
    _METRICS_RUNTIME_LOG_EVENT_STOP_AND_REMOVE_RECORDER,
)


_METRICS_RUNTIME_INSPECTION_LOG_EVENTS: tuple[str, ...] = (
    _METRICS_RUNTIME_LOG_EVENT_GET_RECORDER,
    _METRICS_RUNTIME_LOG_EVENT_TRY_GET_RECORDER,
    _METRICS_RUNTIME_LOG_EVENT_LIST_RECORDER_IDS,
)


_METRICS_RUNTIME_ALL_LOG_EVENTS: tuple[str, ...] = (
    *_METRICS_RUNTIME_NORMAL_LOG_EVENTS,
    *_METRICS_RUNTIME_INSPECTION_LOG_EVENTS,
)


def metrics_runtime_event_policy_config(
    *,
    mode: MetricsRuntimeLogPolicyMode = MetricsRuntimeLogPolicyMode.NORMAL,
) -> PatternLogEventPolicyConfig:
    if not isinstance(mode, MetricsRuntimeLogPolicyMode):
        raise TypeError("argument 'mode' must be an instance of 'MetricsRuntimeLogPolicyMode'")

    if mode is MetricsRuntimeLogPolicyMode.SILENT:
        return PatternLogEventPolicyConfig(default_enabled=False)

    events: tuple[str, ...]

    if mode is MetricsRuntimeLogPolicyMode.NORMAL:
        events = _METRICS_RUNTIME_NORMAL_LOG_EVENTS
    else:
        events = _METRICS_RUNTIME_ALL_LOG_EVENTS

    return PatternLogEventPolicyConfig(
        default_enabled=False,
        rules=(
            PatternLogEventPolicyRuleConfig(
                action=PatternLogEventPolicyAction.ALLOW,
                events=events,
            ),
        ),
    )


def metrics_runtime_event_policy(
    *,
    mode: MetricsRuntimeLogPolicyMode = MetricsRuntimeLogPolicyMode.NORMAL,
) -> PatternLogEventPolicy:
    return PatternLogEventPolicy(
        metrics_runtime_event_policy_config(
            mode=mode,
        )
    )
