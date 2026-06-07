from __future__ import annotations

from collections.abc import Callable, Mapping
from types import ModuleType
from typing import Any

import pytest


@pytest.fixture
def custom_recorder_example(
    load_example_module: Callable[..., ModuleType],
) -> ModuleType:
    return load_example_module(
        "metrics",
        "custom_recorder.py",
    )


def get_dimensions(snapshot: Mapping[str, Any]) -> Mapping[str, Any]:
    dimensions = snapshot["dimensions"]
    assert isinstance(dimensions, Mapping)
    return dimensions


def test_a01_event_has_expected_event_type(
    custom_recorder_example: ModuleType,
) -> None:
    event = custom_recorder_example.DocumentSaveAttemptMetricEvent(
        outcome=custom_recorder_example.DocumentSaveAttemptOutcome.SUCCESS,
    )

    assert event.event_type == "document_storage.save.attempt"


def test_b01_metric_counts_success_attempt(
    custom_recorder_example: ModuleType,
) -> None:
    metric = custom_recorder_example.DocumentSaveAttemptsMetric()

    handled = metric.handle_event(
        custom_recorder_example.DocumentSaveAttemptMetricEvent(
            outcome=custom_recorder_example.DocumentSaveAttemptOutcome.SUCCESS,
        )
    )

    assert handled is True
    assert get_dimensions(metric.snapshot()) == {
        "total": 1,
        "success_total": 1,
        "failure_total": 0,
    }


def test_b02_metric_counts_failure_attempt(
    custom_recorder_example: ModuleType,
) -> None:
    metric = custom_recorder_example.DocumentSaveAttemptsMetric()

    handled = metric.handle_event(
        custom_recorder_example.DocumentSaveAttemptMetricEvent(
            outcome=custom_recorder_example.DocumentSaveAttemptOutcome.FAILURE,
        )
    )

    assert handled is True
    assert get_dimensions(metric.snapshot()) == {
        "total": 1,
        "success_total": 0,
        "failure_total": 1,
    }


@pytest.mark.asyncio
async def test_c01_publisher_starts_closed(
    custom_recorder_example: ModuleType,
) -> None:
    publisher = custom_recorder_example.InMemoryMetricChangePublisher()

    assert publisher.is_open is False
    assert publisher.records == []


@pytest.mark.asyncio
async def test_c02_publisher_open_and_close(
    custom_recorder_example: ModuleType,
) -> None:
    publisher = custom_recorder_example.InMemoryMetricChangePublisher()

    await publisher.open()
    assert publisher.is_open is True

    await publisher.close()
    assert publisher.is_open is False


@pytest.mark.asyncio
async def test_c03_publisher_rejects_publish_when_closed(
    custom_recorder_example: ModuleType,
) -> None:
    publisher = custom_recorder_example.InMemoryMetricChangePublisher()

    with pytest.raises(RuntimeError, match="publisher is not open"):
        await publisher.publish({"value": 1})


@pytest.mark.asyncio
async def test_c04_publisher_records_published_values(
    custom_recorder_example: ModuleType,
) -> None:
    publisher = custom_recorder_example.InMemoryMetricChangePublisher()

    await publisher.open()
    await publisher.publish({"value": 1})
    await publisher.publish({"value": 2})

    assert publisher.records == [
        {"value": 1},
        {"value": 2},
    ]


@pytest.mark.asyncio
async def test_d01_custom_recorder_closes_publisher_after_example(
    custom_recorder_example: ModuleType,
) -> None:
    result = await custom_recorder_example.run_custom_recorder_example()

    assert result["publisher_is_open"] is False


@pytest.mark.asyncio
async def test_d02_custom_recorder_returns_final_snapshot(
    custom_recorder_example: ModuleType,
) -> None:
    result = await custom_recorder_example.run_custom_recorder_example()

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


@pytest.mark.asyncio
async def test_d03_custom_recorder_publishes_progressive_records(
    custom_recorder_example: ModuleType,
) -> None:
    result = await custom_recorder_example.run_custom_recorder_example()

    assert result["published_records"] == [
        {
            "entity_id": "document_storage",
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
            "entity_id": "document_storage",
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
            "entity_id": "document_storage",
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
