# tests/test_metrics/asyncio_metrics_recorder/test_metrics_recorder_logging_policy.py

from __future__ import annotations

from typing import Any, cast

import pytest

from mvx.common.logger.models import LogEventMeta
from mvx.common.logger.pattern_event_policy import (
    PatternLogEventPolicy,
    PatternLogEventPolicyAction,
    PatternLogEventPolicyConfig,
)


from mvx.common.metrics.asyncio_metrics_recorder.logging_policy import (
    AsyncioMetricsRecorderLogPolicyMode,
    asyncio_metrics_recorder_event_policy,
    asyncio_metrics_recorder_event_policy_config,
)

_NORMAL_EVENTS: tuple[str, ...] = (
    "asyncio_metrics_recorder.start",
    "asyncio_metrics_recorder.stop",
    "asyncio_metrics_recorder.register_metric",
    "metrics_recorder.dispatch_error",
    "metrics_recorder.cleanup.task_creation_error",
    "metrics_recorder.cleanup.failed",
)


_INSPECTION_EVENTS: tuple[str, ...] = (
    "asyncio_metrics_recorder.get_metric_snapshots",
    "asyncio_metrics_recorder.iter_metrics",
)


_ALL_EVENTS: tuple[str, ...] = (
    *_NORMAL_EVENTS,
    *_INSPECTION_EVENTS,
)


def _event_meta(event_name: str) -> LogEventMeta:
    return LogEventMeta(
        event_namespace=None,
        event_name=event_name,
        entity_id=None,
        source_path=None,
        source_line=None,
        source_func=None,
    )


# -------------------------
# Group a: policy config
# -------------------------


def test_a01_silent_config_disables_events_by_default() -> None:
    config = asyncio_metrics_recorder_event_policy_config(
        mode=AsyncioMetricsRecorderLogPolicyMode.SILENT,
    )

    assert isinstance(config, PatternLogEventPolicyConfig)
    assert config.default_enabled is False
    assert config.rules == ()


def test_a02_normal_config_allows_normal_events_only() -> None:
    # noinspection PyArgumentEqualDefault
    config = asyncio_metrics_recorder_event_policy_config(
        mode=AsyncioMetricsRecorderLogPolicyMode.NORMAL,
    )

    assert config.default_enabled is False
    assert len(config.rules) == 1

    rule = config.rules[0]

    assert rule.action is PatternLogEventPolicyAction.ALLOW
    assert rule.events == _NORMAL_EVENTS


def test_a03_inspection_config_allows_all_recorder_events() -> None:
    config = asyncio_metrics_recorder_event_policy_config(
        mode=AsyncioMetricsRecorderLogPolicyMode.INSPECTION,
    )

    assert config.default_enabled is False
    assert len(config.rules) == 1

    rule = config.rules[0]

    assert rule.action is PatternLogEventPolicyAction.ALLOW
    assert rule.events == _ALL_EVENTS


def test_a04_config_rejects_invalid_mode() -> None:
    with pytest.raises(TypeError, match="mode"):
        asyncio_metrics_recorder_event_policy_config(
            mode=cast(Any, "normal"),
        )


# -------------------------
# Group b: policy behavior
# -------------------------


def test_b01_silent_policy_disables_known_and_unknown_events() -> None:
    policy = asyncio_metrics_recorder_event_policy(
        mode=AsyncioMetricsRecorderLogPolicyMode.SILENT,
    )

    assert isinstance(policy, PatternLogEventPolicy)

    for event_name in _ALL_EVENTS:
        assert policy.is_event_enabled(_event_meta(event_name)) is False

    assert policy.is_event_enabled(_event_meta("asyncio_metrics_recorder.unknown")) is False


def test_b02_normal_policy_enables_normal_events() -> None:
    # noinspection PyArgumentEqualDefault
    policy = asyncio_metrics_recorder_event_policy(
        mode=AsyncioMetricsRecorderLogPolicyMode.NORMAL,
    )

    for event_name in _NORMAL_EVENTS:
        assert policy.is_event_enabled(_event_meta(event_name)) is True


def test_b03_normal_policy_disables_inspection_and_unknown_events() -> None:
    # noinspection PyArgumentEqualDefault
    policy = asyncio_metrics_recorder_event_policy(
        mode=AsyncioMetricsRecorderLogPolicyMode.NORMAL,
    )

    for event_name in _INSPECTION_EVENTS:
        assert policy.is_event_enabled(_event_meta(event_name)) is False

    assert policy.is_event_enabled(_event_meta("asyncio_metrics_recorder.unknown")) is False


def test_b04_inspection_policy_enables_all_recorder_events() -> None:
    policy = asyncio_metrics_recorder_event_policy(
        mode=AsyncioMetricsRecorderLogPolicyMode.INSPECTION,
    )

    for event_name in _ALL_EVENTS:
        assert policy.is_event_enabled(_event_meta(event_name)) is True


def test_b05_inspection_policy_disables_unknown_events() -> None:
    policy = asyncio_metrics_recorder_event_policy(
        mode=AsyncioMetricsRecorderLogPolicyMode.INSPECTION,
    )

    assert policy.is_event_enabled(_event_meta("asyncio_metrics_recorder.unknown")) is False


def test_b06_policy_factory_rejects_invalid_mode() -> None:
    with pytest.raises(TypeError, match="mode"):
        asyncio_metrics_recorder_event_policy(
            mode=cast(Any, "normal"),
        )
