from __future__ import annotations

from collections.abc import Callable, Mapping
from types import ModuleType
from typing import Any

import pytest


@pytest.fixture
def metric_changed_hook_example(
    load_example_module: Callable[..., ModuleType],
) -> ModuleType:
    return load_example_module(
        "metrics",
        "metric_changed_hook.py",
    )


def get_dimensions(snapshot: Mapping[str, Any]) -> Mapping[str, Any]:
    dimensions = snapshot["dimensions"]
    assert isinstance(dimensions, Mapping)
    return dimensions


def test_a01_event_has_expected_event_type(
    metric_changed_hook_example: ModuleType,
) -> None:
    event = metric_changed_hook_example.DocumentSaveAttemptMetricEvent(
        outcome=metric_changed_hook_example.DocumentSaveAttemptOutcome.SUCCESS,
    )

    assert event.event_type == "document_storage.save.attempt"


def test_a02_unknown_event_has_expected_event_type(
    metric_changed_hook_example: ModuleType,
) -> None:
    event = metric_changed_hook_example.UnknownMetricEvent()

    assert event.event_type == "unknown.event"


def test_b01_metric_ignores_unknown_event(
    metric_changed_hook_example: ModuleType,
) -> None:
    metric = metric_changed_hook_example.DocumentSaveAttemptsMetric()

    handled = metric.handle_event(metric_changed_hook_example.UnknownMetricEvent())

    assert handled is False
    assert get_dimensions(metric.snapshot()) == {
        "total": 0,
        "success_total": 0,
        "failure_total": 0,
    }


def test_b02_metric_counts_success_attempt(
    metric_changed_hook_example: ModuleType,
) -> None:
    metric = metric_changed_hook_example.DocumentSaveAttemptsMetric()

    handled = metric.handle_event(
        metric_changed_hook_example.DocumentSaveAttemptMetricEvent(
            outcome=metric_changed_hook_example.DocumentSaveAttemptOutcome.SUCCESS,
        )
    )

    assert handled is True
    assert get_dimensions(metric.snapshot()) == {
        "total": 1,
        "success_total": 1,
        "failure_total": 0,
    }


def test_b03_metric_counts_failure_attempt(
    metric_changed_hook_example: ModuleType,
) -> None:
    metric = metric_changed_hook_example.DocumentSaveAttemptsMetric()

    handled = metric.handle_event(
        metric_changed_hook_example.DocumentSaveAttemptMetricEvent(
            outcome=metric_changed_hook_example.DocumentSaveAttemptOutcome.FAILURE,
        )
    )

    assert handled is True
    assert get_dimensions(metric.snapshot()) == {
        "total": 1,
        "success_total": 0,
        "failure_total": 1,
    }


@pytest.mark.asyncio
async def test_c01_metric_changed_hook_records_only_accepted_events(
    metric_changed_hook_example: ModuleType,
) -> None:
    result = await metric_changed_hook_example.run_metric_changed_hook_example()

    metric_changes = result["metric_changes"]

    assert len(metric_changes) == 3
    assert [change["event_type"] for change in metric_changes] == [
        "document_storage.save.attempt",
        "document_storage.save.attempt",
        "document_storage.save.attempt",
    ]
    assert [change["metric_name"] for change in metric_changes] == [
        "document_storage.save.attempts",
        "document_storage.save.attempts",
        "document_storage.save.attempts",
    ]


@pytest.mark.asyncio
async def test_c02_metric_changed_hook_records_progressive_snapshots(
    metric_changed_hook_example: ModuleType,
) -> None:
    result = await metric_changed_hook_example.run_metric_changed_hook_example()

    metric_changes = result["metric_changes"]

    assert metric_changes == [
        {
            "metric_name": "document_storage.save.attempts",
            "event_type": "document_storage.save.attempt",
            "snapshot": {
                "name": "document_storage.save.attempts",
                "dimensions": {
                    "total": 1,
                    "success_total": 1,
                    "failure_total": 0,
                },
            },
        },
        {
            "metric_name": "document_storage.save.attempts",
            "event_type": "document_storage.save.attempt",
            "snapshot": {
                "name": "document_storage.save.attempts",
                "dimensions": {
                    "total": 2,
                    "success_total": 2,
                    "failure_total": 0,
                },
            },
        },
        {
            "metric_name": "document_storage.save.attempts",
            "event_type": "document_storage.save.attempt",
            "snapshot": {
                "name": "document_storage.save.attempts",
                "dimensions": {
                    "total": 3,
                    "success_total": 2,
                    "failure_total": 1,
                },
            },
        },
    ]


@pytest.mark.asyncio
async def test_c03_metric_changed_hook_example_returns_final_snapshot(
    metric_changed_hook_example: ModuleType,
) -> None:
    result = await metric_changed_hook_example.run_metric_changed_hook_example()

    assert result["snapshots"] == {
        "document_storage.save.attempts": {
            "name": "document_storage.save.attempts",
            "dimensions": {
                "total": 3,
                "success_total": 2,
                "failure_total": 1,
            },
        },
    }
