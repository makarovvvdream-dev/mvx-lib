from __future__ import annotations

from collections.abc import Callable, Mapping
from types import ModuleType
from typing import Any

import pytest

from mvx.common.metrics import AsyncioMetricsRecorderLoopUnavailableError


@pytest.fixture
def manual_recorder_lifecycle_example(
    load_example_module: Callable[..., ModuleType],
) -> ModuleType:
    return load_example_module(
        "metrics",
        "manual_recorder_lifecycle.py",
    )


def get_dimensions(snapshot: Mapping[str, Any]) -> Mapping[str, Any]:
    dimensions = snapshot["dimensions"]
    assert isinstance(dimensions, Mapping)
    return dimensions


def test_a01_direct_recorder_creation_without_running_loop_fails(
    manual_recorder_lifecycle_example: ModuleType,
) -> None:
    with pytest.raises(AsyncioMetricsRecorderLoopUnavailableError):
        manual_recorder_lifecycle_example.AsyncioMetricsRecorder("document_storage")


def test_b01_event_has_expected_event_type(
    manual_recorder_lifecycle_example: ModuleType,
) -> None:
    event = manual_recorder_lifecycle_example.DocumentSaveAttemptMetricEvent(
        outcome=manual_recorder_lifecycle_example.DocumentSaveAttemptOutcome.SUCCESS,
    )

    assert event.event_type == "document_storage.save.attempt"


def test_c01_metric_counts_success_attempt(
    manual_recorder_lifecycle_example: ModuleType,
) -> None:
    metric = manual_recorder_lifecycle_example.DocumentSaveAttemptsMetric()

    handled = metric.handle_event(
        manual_recorder_lifecycle_example.DocumentSaveAttemptMetricEvent(
            outcome=manual_recorder_lifecycle_example.DocumentSaveAttemptOutcome.SUCCESS,
        )
    )

    assert handled is True
    assert get_dimensions(metric.snapshot()) == {
        "total": 1,
        "success_total": 1,
        "failure_total": 0,
    }


def test_c02_metric_counts_failure_attempt(
    manual_recorder_lifecycle_example: ModuleType,
) -> None:
    metric = manual_recorder_lifecycle_example.DocumentSaveAttemptsMetric()

    handled = metric.handle_event(
        manual_recorder_lifecycle_example.DocumentSaveAttemptMetricEvent(
            outcome=manual_recorder_lifecycle_example.DocumentSaveAttemptOutcome.FAILURE,
        )
    )

    assert handled is True
    assert get_dimensions(metric.snapshot()) == {
        "total": 1,
        "success_total": 0,
        "failure_total": 1,
    }


@pytest.mark.asyncio
async def test_d01_manual_recorder_example_returns_expected_snapshot(
    manual_recorder_lifecycle_example: ModuleType,
) -> None:
    snapshots = await manual_recorder_lifecycle_example.run_manual_recorder_example()

    assert snapshots == {
        "document_storage.save.attempts": {
            "name": "document_storage.save.attempts",
            "dimensions": {
                "total": 3,
                "success_total": 2,
                "failure_total": 1,
            },
        },
    }
