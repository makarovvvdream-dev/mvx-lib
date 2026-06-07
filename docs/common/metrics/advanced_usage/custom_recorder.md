# Custom recorder

```{contents} Contents:
:depth: 1
:local:
```

A custom recorder is useful when metric events should be processed normally, but the recorder also needs additional
behavior around processing.

The usual extension point is subclassing `AsyncioMetricsRecorder`.

A custom recorder can use recorder hooks to attach resources, react after metric state changes, and clean up resources
during shutdown.

This page shows a small example: a recorder that publishes a record every time a metric accepts an event.

## When to create a custom recorder

Do not create a custom recorder just to define new measurements.

For new measurements, create a new `Metric`.

Use a custom recorder when you need to change recorder-side behavior, for example:

* publish changed metric state somewhere;
* notify observers;
* attach a local diagnostic sink;
* connect to a custom backend;
* open and close integration resources together with recorder lifecycle.

The production component should still receive only a recorder and emit events through it.

## Recorder hooks used by the example

The example uses three hooks.

```python
async def _on_starting(self) -> None:
    ...
```

Runs during recorder startup.

```python
async def _on_metric_changed(
        self,
        *,
        metric: Metric,
        event: MetricEvent,
) -> None:
    ...
```

Runs after a metric accepts an event and updates its state.

```python
async def _on_stopped(self) -> None:
    ...
```

Runs during recorder shutdown.

These hooks let the recorder manage integration behavior without changing production code or metric classes.

## Example idea

The example defines an in-memory publisher:

```python
class InMemoryMetricChangePublisher:
    ...
```

and a custom recorder:

```python
class PublishingMetricsRecorder(AsyncioMetricsRecorder):
    ...
```

The recorder:

* opens the publisher in `_on_starting()`;
* publishes a metric-change record in `_on_metric_changed()`;
* closes the publisher in `_on_stopped()`.

The metric itself remains ordinary.

The production event remains ordinary.

Only recorder behavior is extended.

## Custom recorder implementation

The custom recorder receives a publisher and stores it.

```python
class PublishingMetricsRecorder(AsyncioMetricsRecorder):
    def __init__(
            self,
            entity_id: str,
            *,
            publisher: InMemoryMetricChangePublisher,
    ) -> None:
        super().__init__(entity_id)
        self._publisher = publisher
```

The startup hook opens the publisher:

```python
async def _on_starting(self) -> None:
    await self._publisher.open()
```

The shutdown hook closes it:

```python
async def _on_stopped(self) -> None:
    await self._publisher.close()
```

The metric-changed hook publishes updated metric state:

```python
async def _on_metric_changed(
        self,
        *,
        metric: Metric,
        event: MetricEvent,
) -> None:
    await self._publisher.publish(
        {
            "entity_id": self.entity_id,
            "metric_name": metric.metric_name,
            "event_type": event.event_type,
            "snapshot": metric.snapshot(),
        }
    )
```

This hook runs only after the metric has accepted the event.

At that point, `metric.snapshot()` returns the updated metric state.

## Complete runnable example

The full runnable version of this example is available as a source file:

{download}`Download custom_recorder.py <../../../../common/examples/metrics/custom_recorder.py>`

The same file is covered by tests, so the documented example is checked as real Python code.

```python
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pprint import pprint
from typing import Any, Mapping

from mvx.common.metrics import AsyncioMetricsRecorder, Metric, MetricEvent


class DocumentSaveAttemptOutcome(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


@dataclass(frozen=True, slots=True)
class DocumentSaveAttemptMetricEvent(MetricEvent):
    outcome: DocumentSaveAttemptOutcome

    @property
    def event_type(self) -> str:
        return "document_storage.save.attempt"


class DocumentSaveAttemptsMetric(Metric):
    def __init__(self) -> None:
        self._total = 0
        self._success_total = 0
        self._failure_total = 0

    @property
    def metric_name(self) -> str:
        return "document_storage.save.attempts"

    def handle_event(self, event: MetricEvent) -> bool:
        if not isinstance(event, DocumentSaveAttemptMetricEvent):
            return False

        self._total += 1

        if event.outcome is DocumentSaveAttemptOutcome.SUCCESS:
            self._success_total += 1

        elif event.outcome is DocumentSaveAttemptOutcome.FAILURE:
            self._failure_total += 1

        return True

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "total": self._total,
                "success_total": self._success_total,
                "failure_total": self._failure_total,
            },
        }


class InMemoryMetricChangePublisher:
    def __init__(self) -> None:
        self._is_open = False
        self._records: list[Mapping[str, Any]] = []

    @property
    def is_open(self) -> bool:
        return self._is_open

    @property
    def records(self) -> list[Mapping[str, Any]]:
        return list(self._records)

    async def open(self) -> None:
        self._is_open = True

    async def close(self) -> None:
        self._is_open = False

    async def publish(self, record: Mapping[str, Any]) -> None:
        if not self._is_open:
            raise RuntimeError("publisher is not open")

        self._records.append(record)


class PublishingMetricsRecorder(AsyncioMetricsRecorder):
    def __init__(
            self,
            entity_id: str,
            *,
            publisher: InMemoryMetricChangePublisher,
    ) -> None:
        super().__init__(entity_id)
        self._publisher = publisher

    async def _on_starting(self) -> None:
        await self._publisher.open()

    async def _on_stopped(self) -> None:
        await self._publisher.close()

    async def _on_metric_changed(
            self,
            *,
            metric: Metric,
            event: MetricEvent,
    ) -> None:
        await self._publisher.publish(
            {
                "entity_id": self.entity_id,
                "metric_name": metric.metric_name,
                "event_type": event.event_type,
                "snapshot": metric.snapshot(),
            }
        )


async def run_custom_recorder_example() -> Mapping[str, Any]:
    publisher = InMemoryMetricChangePublisher()
    recorder = PublishingMetricsRecorder(
        "document_storage",
        publisher=publisher,
    )

    start_result = await recorder.start()
    if not start_result.success:
        assert start_result.error is not None
        raise start_result.error

    try:
        recorder.register_metric(metric=DocumentSaveAttemptsMetric())

        recorder.register_event(
            event=DocumentSaveAttemptMetricEvent(
                outcome=DocumentSaveAttemptOutcome.SUCCESS,
            )
        )
        recorder.register_event(
            event=DocumentSaveAttemptMetricEvent(
                outcome=DocumentSaveAttemptOutcome.SUCCESS,
            )
        )
        recorder.register_event(
            event=DocumentSaveAttemptMetricEvent(
                outcome=DocumentSaveAttemptOutcome.FAILURE,
            )
        )

        stop_result = await recorder.stop()
        if not stop_result.success:
            assert stop_result.error is not None
            raise stop_result.error

        return {
            "publisher_is_open": publisher.is_open,
            "published_records": publisher.records,
            "snapshots": recorder.get_metric_snapshots(),
        }

    except Exception:
        stop_result = await recorder.stop()
        if not stop_result.success:
            assert stop_result.error is not None
            raise stop_result.error
        raise


async def main() -> None:
    result = await run_custom_recorder_example()
    pprint(result)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
```

## Running the example

From the `common` package directory, run:

```bash
python examples/metrics/custom_recorder.py
```

The result contains:

```text
publisher_is_open
published_records
snapshots
```

`publisher_is_open` is `False` at the end, because `_on_stopped()` closed the publisher.

`published_records` contains one record for each accepted metric event.

`snapshots` contains the final metric state.

## What the published records show

The custom recorder publishes progressive metric snapshots.

After the first accepted event:

```text
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
}
```

After all accepted events, the final snapshot is:

```text
{
    "document_storage.save.attempts": {
        "name": "document_storage.save.attempts",
        "dimensions": {
            "total": 3,
            "success_total": 2,
            "failure_total": 1,
        },
    },
}
```

## What this example demonstrates

The example demonstrates four recorder-extension ideas.

First, recorder startup can prepare integration resources:

```python
async def _on_starting(self) -> None:
    await self._publisher.open()
```

Second, recorder shutdown can clean them up:

```python
async def _on_stopped(self) -> None:
    await self._publisher.close()
```

Third, `_on_metric_changed()` can react after metric state changes:

```python
async def _on_metric_changed(
        self,
        *,
        metric: Metric,
        event: MetricEvent,
) -> None:
    ...
```

Fourth, production code does not change.

The production component still emits ordinary metric events through a recorder.

## What custom recorders should not do

A custom recorder should not contain business operation logic.

It should not decide whether `save_document()` succeeds or fails.

It should not replace metric classes.

It should not mutate metric private state directly.

It should extend recorder-side behavior around event processing.

If you need a new measurement, create a metric.

If you need a new reaction to accepted metric changes, a custom recorder or hook may be appropriate.

## Summary

A custom recorder extends recorder-side behavior.

The example recorder opens a publisher on startup, publishes changed metric snapshots after accepted events, and closes
the publisher on shutdown.

This keeps integration behavior out of production code and keeps measurement logic inside metrics.
