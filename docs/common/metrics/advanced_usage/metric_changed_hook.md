# Metric changed hook

```{contents} Contents:
:depth: 1
:local:
```

`_on_metric_changed()` is a recorder extension point.

It runs after a metric accepts an event and updates its state.

This makes it different from raw event interception.

The hook is not called when an event is merely submitted to the recorder. It is called only when a registered metric
handles that event.

## What the hook is for

Use `_on_metric_changed()` when you need to react after metric state has changed.

Typical uses include:

* collecting test probes;
* notifying local observers;
* forwarding changed metric state;
* feeding a lightweight diagnostic view;
* triggering an adapter after aggregation.

The hook is not part of production code.

It belongs to the recorder side of the metrics pipeline.

## Hook position in the pipeline

The hook runs after `handle_event()` returns `True`.

```text
event
   |
   v
recorder dispatch
   |
   v
metric.handle_event(event)
   |
   +--> False
   |       |
   |       v
   |    hook is not called
   |
   +--> True
           |
           v
        metric state updated
           |
           v
        _on_metric_changed(metric=metric, event=event)
```

This means the hook sees the metric after it has already processed the event.

If the hook reads `metric.snapshot()`, it gets the updated state.

## Hook signature

A recorder subclass can override:

```python
async def _on_metric_changed(
        self,
        *,
        metric: Metric,
        event: MetricEvent,
) -> None:
    ...
```

The hook receives:

* `metric` — the metric that accepted the event;
* `event` — the event accepted by that metric.

The base implementation does nothing.

## Example recorder

The example defines a recorder that records every accepted metric change.

```python
class ObservingMetricsRecorder(AsyncioMetricsRecorder):
    def __init__(self, entity_id: str) -> None:
        super().__init__(entity_id)
        self.metric_changes: list[Mapping[str, Any]] = []

    async def _on_metric_changed(
            self,
            *,
            metric: Metric,
            event: MetricEvent,
    ) -> None:
        self.metric_changes.append(
            {
                "metric_name": metric.metric_name,
                "event_type": event.event_type,
                "snapshot": metric.snapshot(),
            }
        )
```

This recorder does not change metric logic.

It only observes accepted metric changes.

## Accepted events only

The example sends four events:

```text
SUCCESS
unknown event
SUCCESS
FAILURE
```

Only three of them are accepted by `DocumentSaveAttemptsMetric`.

The unknown event is ignored by the metric, so `_on_metric_changed()` is not called for it.

The result contains three hook records:

```python
[
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
```

This demonstrates the important rule:

```text
submitted event
   |
   v
not enough

accepted by metric
   |
   v
hook is called
```

## Complete runnable example

The full runnable version of this example is available as a source file:

{download}`Download metric_changed_hook.py <../../../../common/examples/metrics/metric_changed_hook.py>`

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


@dataclass(frozen=True, slots=True)
class UnknownMetricEvent(MetricEvent):
    @property
    def event_type(self) -> str:
        return "unknown.event"


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


class ObservingMetricsRecorder(AsyncioMetricsRecorder):
    def __init__(self, entity_id: str) -> None:
        super().__init__(entity_id)
        self.metric_changes: list[Mapping[str, Any]] = []

    async def _on_metric_changed(
            self,
            *,
            metric: Metric,
            event: MetricEvent,
    ) -> None:
        self.metric_changes.append(
            {
                "metric_name": metric.metric_name,
                "event_type": event.event_type,
                "snapshot": metric.snapshot(),
            }
        )


async def run_metric_changed_hook_example() -> Mapping[str, Any]:
    recorder = ObservingMetricsRecorder("document_storage")

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
        recorder.register_event(event=UnknownMetricEvent())
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
            "snapshots": recorder.get_metric_snapshots(),
            "metric_changes": recorder.metric_changes,
        }

    except Exception:
        stop_result = await recorder.stop()
        if not stop_result.success:
            assert stop_result.error is not None
            raise stop_result.error
        raise


async def main() -> None:
    result = await run_metric_changed_hook_example()
    pprint(result)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
```

## Running the example

From the `common` package directory, run:

```bash
python examples/metrics/metric_changed_hook.py
```

The output contains two parts:

```text
snapshots
metric_changes
```

`snapshots` contains the final metric state.

`metric_changes` contains the hook records captured after each accepted event.

## What this example shows

The example shows three important points.

First, `_on_metric_changed()` runs after metric state changes.

That is why each stored snapshot shows the progressive metric state:

```text
after first accepted event:
    total = 1

after second accepted event:
    total = 2

after third accepted event:
    total = 3
```

Second, ignored events do not trigger the hook.

`UnknownMetricEvent` is submitted to the recorder, but `DocumentSaveAttemptsMetric` returns `False`, so the hook is not
called for that event.

Third, the hook belongs to the recorder extension layer.

The production code does not know about it.

The metric does not call it.

The recorder calls it after a metric accepts an event.

## What the hook should not do

The hook should not replace metric logic.

A metric should still own its own interpretation and state.

The hook should also avoid heavy synchronous work that blocks recorder processing.

If an integration needs slow I/O, batching, retries, or external delivery, use the hook as a trigger and move that work
into an appropriate adapter or background component.

## Summary

`_on_metric_changed()` is called after a metric accepts an event and updates its state.

It is not called for ignored events.

It receives the metric and the event that changed it.

It is useful for observers, probes, local diagnostics, and adapter triggers.

It is a recorder extension point, not a production-code concern.
