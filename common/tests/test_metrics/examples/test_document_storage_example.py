from __future__ import annotations

from collections.abc import Callable, Mapping
from types import ModuleType
from typing import Any

import pytest

from mvx.common.metrics import MetricEvent, MetricsRuntime


@pytest.fixture
def document_storage_example(
    load_example_module: Callable[..., ModuleType],
) -> ModuleType:
    return load_example_module(
        "metrics",
        "document_storage.py",
    )


class UnknownMetricEvent(MetricEvent):
    @property
    def event_type(self) -> str:
        return "unknown.event"


def get_dimensions(snapshot: Mapping[str, Any]) -> Mapping[str, Any]:
    dimensions = snapshot["dimensions"]
    assert isinstance(dimensions, Mapping)
    return dimensions


def test_a01_event_has_expected_event_type(
    document_storage_example: ModuleType,
) -> None:
    event = document_storage_example.DocumentSaveAttemptMetricEvent(
        outcome=document_storage_example.DocumentSaveAttemptOutcome.SUCCESS,
    )

    assert event.event_type == "document_storage.save.attempt"


def test_b01_metric_ignores_unrelated_event(
    document_storage_example: ModuleType,
) -> None:
    metric = document_storage_example.DocumentSaveAttemptsMetric()

    handled = metric.handle_event(UnknownMetricEvent())

    assert handled is False
    assert get_dimensions(metric.snapshot()) == {
        "total": 0,
        "success_total": 0,
        "failure_total": 0,
    }


def test_b02_metric_counts_success_attempt(
    document_storage_example: ModuleType,
) -> None:
    metric = document_storage_example.DocumentSaveAttemptsMetric()

    handled = metric.handle_event(
        document_storage_example.DocumentSaveAttemptMetricEvent(
            outcome=document_storage_example.DocumentSaveAttemptOutcome.SUCCESS,
        )
    )

    assert handled is True
    assert get_dimensions(metric.snapshot()) == {
        "total": 1,
        "success_total": 1,
        "failure_total": 0,
    }


def test_b03_metric_counts_failure_attempt(
    document_storage_example: ModuleType,
) -> None:
    metric = document_storage_example.DocumentSaveAttemptsMetric()

    handled = metric.handle_event(
        document_storage_example.DocumentSaveAttemptMetricEvent(
            outcome=document_storage_example.DocumentSaveAttemptOutcome.FAILURE,
        )
    )

    assert handled is True
    assert get_dimensions(metric.snapshot()) == {
        "total": 1,
        "success_total": 0,
        "failure_total": 1,
    }


def test_c01_document_storage_works_without_metrics_recorder(
    document_storage_example: ModuleType,
) -> None:
    storage = document_storage_example.DocumentStorage()

    storage.save_document("doc-001", "First document")


def test_c02_document_storage_raises_without_metrics_recorder(
    document_storage_example: ModuleType,
) -> None:
    storage = document_storage_example.DocumentStorage()

    with pytest.raises(ValueError, match="document_id must not be empty"):
        storage.save_document("", "Broken document")


def test_d01_document_storage_updates_snapshot_through_metrics_runtime(
    document_storage_example: ModuleType,
) -> None:
    runtime = MetricsRuntime(namespace="test.metrics")
    runtime.start()

    try:
        recorder = runtime.create_recorder("document_storage")
        storage = document_storage_example.DocumentStorage(metrics_recorder=recorder)

        storage.save_document("doc-001", "First document")
        storage.save_document("doc-002", "Second document")

        with pytest.raises(ValueError, match="document_id must not be empty"):
            storage.save_document("", "Broken document")

        snapshots = recorder.get_metric_snapshots()

    finally:
        runtime.shutdown()

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
