# src/mvx/common/metrics/asyncio_metrics_recorder/logging_policy.py

from __future__ import annotations

from enum import StrEnum

from mvx.common.logger.pattern_event_policy import (
    PatternLogEventPolicy,
    PatternLogEventPolicyAction,
    PatternLogEventPolicyConfig,
    PatternLogEventPolicyRuleConfig,
)

__all__ = (
    "AsyncioMetricsRecorderLogPolicyMode",
    "asyncio_metrics_recorder_event_policy_config",
    "asyncio_metrics_recorder_event_policy",
)


class AsyncioMetricsRecorderLogPolicyMode(StrEnum):
    SILENT = "silent"
    NORMAL = "normal"
    INSPECTION = "inspection"


_ASYNCIO_METRICS_RECORDER_LOG_EVENT_START = "asyncio_metrics_recorder.start"
_ASYNCIO_METRICS_RECORDER_LOG_EVENT_STOP = "asyncio_metrics_recorder.stop"
_ASYNCIO_METRICS_RECORDER_LOG_EVENT_REGISTER_METRIC = "asyncio_metrics_recorder.register_metric"
_ASYNCIO_METRICS_RECORDER_LOG_EVENT_GET_METRIC_SNAPSHOTS = (
    "asyncio_metrics_recorder.get_metric_snapshots"
)
_ASYNCIO_METRICS_RECORDER_LOG_EVENT_ITER_METRICS = "asyncio_metrics_recorder.iter_metrics"

_ASYNCIO_METRICS_RECORDER_LOG_EVENT_DISPATCH_ERROR = "metrics_recorder.dispatch_error"
_ASYNCIO_METRICS_RECORDER_LOG_EVENT_CLEANUP_TASK_CREATION_ERROR = (
    "metrics_recorder.cleanup.task_creation_error"
)
_ASYNCIO_METRICS_RECORDER_LOG_EVENT_CLEANUP_FAILED = "metrics_recorder.cleanup.failed"


_ASYNCIO_METRICS_RECORDER_NORMAL_LOG_EVENTS: tuple[str, ...] = (
    _ASYNCIO_METRICS_RECORDER_LOG_EVENT_START,
    _ASYNCIO_METRICS_RECORDER_LOG_EVENT_STOP,
    _ASYNCIO_METRICS_RECORDER_LOG_EVENT_REGISTER_METRIC,
    _ASYNCIO_METRICS_RECORDER_LOG_EVENT_DISPATCH_ERROR,
    _ASYNCIO_METRICS_RECORDER_LOG_EVENT_CLEANUP_TASK_CREATION_ERROR,
    _ASYNCIO_METRICS_RECORDER_LOG_EVENT_CLEANUP_FAILED,
)


_ASYNCIO_METRICS_RECORDER_INSPECTION_LOG_EVENTS: tuple[str, ...] = (
    _ASYNCIO_METRICS_RECORDER_LOG_EVENT_GET_METRIC_SNAPSHOTS,
    _ASYNCIO_METRICS_RECORDER_LOG_EVENT_ITER_METRICS,
)


_ASYNCIO_METRICS_RECORDER_ALL_LOG_EVENTS: tuple[str, ...] = (
    *_ASYNCIO_METRICS_RECORDER_NORMAL_LOG_EVENTS,
    *_ASYNCIO_METRICS_RECORDER_INSPECTION_LOG_EVENTS,
)


def asyncio_metrics_recorder_event_policy_config(
    *,
    mode: AsyncioMetricsRecorderLogPolicyMode = AsyncioMetricsRecorderLogPolicyMode.NORMAL,
) -> PatternLogEventPolicyConfig:
    if not isinstance(mode, AsyncioMetricsRecorderLogPolicyMode):
        raise TypeError(
            "argument 'mode' must be an instance of 'AsyncioMetricsRecorderLogPolicyMode'"
        )

    if mode is AsyncioMetricsRecorderLogPolicyMode.SILENT:
        return PatternLogEventPolicyConfig(default_enabled=False)

    events: tuple[str, ...]

    if mode is AsyncioMetricsRecorderLogPolicyMode.NORMAL:
        events = _ASYNCIO_METRICS_RECORDER_NORMAL_LOG_EVENTS
    else:
        events = _ASYNCIO_METRICS_RECORDER_ALL_LOG_EVENTS

    return PatternLogEventPolicyConfig(
        default_enabled=False,
        rules=(
            PatternLogEventPolicyRuleConfig(
                action=PatternLogEventPolicyAction.ALLOW,
                events=events,
            ),
        ),
    )


def asyncio_metrics_recorder_event_policy(
    *,
    mode: AsyncioMetricsRecorderLogPolicyMode = AsyncioMetricsRecorderLogPolicyMode.NORMAL,
) -> PatternLogEventPolicy:
    return PatternLogEventPolicy(
        asyncio_metrics_recorder_event_policy_config(
            mode=mode,
        )
    )
