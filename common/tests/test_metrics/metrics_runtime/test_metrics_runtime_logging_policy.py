# tests/test_metrics/metrics_runtime/test_metrics_runtime_logging_policy.py

from __future__ import annotations

from typing import Any, cast

import pytest

from mvx.common.logger.pattern_event_policy import (
    PatternLogEventPolicy,
    PatternLogEventPolicyAction,
    PatternLogEventPolicyConfig,
)

from mvx.common.logger.models import LogEventMeta

from mvx.common.metrics.metrics_runtime.logging_policy import (
    MetricsRuntimeLogPolicyMode,
    metrics_runtime_event_policy,
    metrics_runtime_event_policy_config,
)

_NORMAL_EVENTS: tuple[str, ...] = (
    "metrics_runtime.start",
    "metrics_runtime.shutdown",
    "metrics_runtime.create_recorder",
    "metrics_runtime.stop_recorder",
    "metrics_runtime.stop_and_remove_recorder",
)


_INSPECTION_EVENTS: tuple[str, ...] = (
    "metrics_runtime.get_recorder",
    "metrics_runtime.try_get_recorder",
    "metrics_runtime.list_recorder_ids",
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
    config = metrics_runtime_event_policy_config(
        mode=MetricsRuntimeLogPolicyMode.SILENT,
    )

    assert isinstance(config, PatternLogEventPolicyConfig)
    assert config.default_enabled is False
    assert config.rules == ()


def test_a02_normal_config_allows_normal_events_only() -> None:
    # noinspection PyArgumentEqualDefault
    config = metrics_runtime_event_policy_config(
        mode=MetricsRuntimeLogPolicyMode.NORMAL,
    )

    assert config.default_enabled is False
    assert len(config.rules) == 1

    rule = config.rules[0]

    assert rule.action is PatternLogEventPolicyAction.ALLOW
    assert rule.events == _NORMAL_EVENTS


def test_a03_inspection_config_allows_all_runtime_events() -> None:
    config = metrics_runtime_event_policy_config(
        mode=MetricsRuntimeLogPolicyMode.INSPECTION,
    )

    assert config.default_enabled is False
    assert len(config.rules) == 1

    rule = config.rules[0]

    assert rule.action is PatternLogEventPolicyAction.ALLOW
    assert rule.events == _ALL_EVENTS


def test_a04_config_rejects_invalid_mode() -> None:
    with pytest.raises(TypeError, match="mode"):
        metrics_runtime_event_policy_config(
            mode=cast(Any, "normal"),
        )


# -------------------------
# Group b: policy behavior
# -------------------------


def test_b01_silent_policy_disables_known_and_unknown_events() -> None:
    policy = metrics_runtime_event_policy(
        mode=MetricsRuntimeLogPolicyMode.SILENT,
    )

    assert isinstance(policy, PatternLogEventPolicy)

    for event_name in _ALL_EVENTS:
        assert policy.is_event_enabled(_event_meta(event_name)) is False

    assert policy.is_event_enabled(_event_meta("metrics_runtime.unknown")) is False


def test_b02_normal_policy_enables_normal_events() -> None:
    # noinspection PyArgumentEqualDefault
    policy = metrics_runtime_event_policy(
        mode=MetricsRuntimeLogPolicyMode.NORMAL,
    )

    for event_name in _NORMAL_EVENTS:
        assert policy.is_event_enabled(_event_meta(event_name)) is True


def test_b03_normal_policy_disables_inspection_and_unknown_events() -> None:
    # noinspection PyArgumentEqualDefault
    policy = metrics_runtime_event_policy(
        mode=MetricsRuntimeLogPolicyMode.NORMAL,
    )

    for event_name in _INSPECTION_EVENTS:
        assert policy.is_event_enabled(_event_meta(event_name)) is False

    assert policy.is_event_enabled(_event_meta("metrics_runtime.unknown")) is False


def test_b04_inspection_policy_enables_all_runtime_events() -> None:
    policy = metrics_runtime_event_policy(
        mode=MetricsRuntimeLogPolicyMode.INSPECTION,
    )

    for event_name in _ALL_EVENTS:
        assert policy.is_event_enabled(_event_meta(event_name)) is True


def test_b05_inspection_policy_disables_unknown_events() -> None:
    policy = metrics_runtime_event_policy(
        mode=MetricsRuntimeLogPolicyMode.INSPECTION,
    )

    assert policy.is_event_enabled(_event_meta("metrics_runtime.unknown")) is False


def test_b06_policy_factory_rejects_invalid_mode() -> None:
    with pytest.raises(TypeError, match="mode"):
        metrics_runtime_event_policy(
            mode=cast(Any, "normal"),
        )
