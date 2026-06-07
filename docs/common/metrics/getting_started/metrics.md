# Metrics

```{contents} Contents:
:depth: 2
:local:
```

`Metric` is the component that turns metric events into measured state.

A metric event describes what happened.

A metric decides what that event means quantitatively.

This is the main responsibility split in `MVX Metrics`:

```text
production code emits facts
metrics interpret facts
```

A metric is not just a counter. It is an aggregate that owns its internal measured state and exposes that state through
a snapshot.

## Core idea

A metric receives events and decides whether they matter.

If an event is relevant, the metric updates its own state.

If an event is not relevant, the metric ignores it.

The production component does not update metric state directly. It only emits metric events.

For example, `DocumentStorage.save_document()` may emit this event:

```python
DocumentSaveAttemptMetricEvent(
    outcome=DocumentSaveAttemptOutcome.SUCCESS,
    duration_ms=2.5,
)
```

One metric may use this event to count save attempts.

Another metric may use the same event to calculate average save duration.

The production method emits one structured fact. Different metrics can interpret that fact in different ways.

## The extended document example

The first example used this event:

```python
@dataclass(frozen=True, slots=True)
class DocumentSaveAttemptMetricEvent(MetricEvent):
    outcome: DocumentSaveAttemptOutcome

    @property
    def event_type(self) -> str:
        return "document_storage.save.attempt"
```

For this article, we extend the event with operation duration:

```python
@dataclass(frozen=True, slots=True)
class DocumentSaveAttemptMetricEvent(MetricEvent):
    outcome: DocumentSaveAttemptOutcome
    duration_ms: float

    @property
    def event_type(self) -> str:
        return "document_storage.save.attempt"
```

The event still describes one domain fact:

```text
a document save attempt happened
```

But now it carries more useful data for metrics:

```text
outcome
duration_ms
```

This lets different metrics use the same event for different measurements.

## First metric: save attempts

The first metric counts save attempts by outcome.

```python
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
```

This metric uses only one part of the event payload:

```text
outcome
```

It does not care about `duration_ms`.

That is fine.

A metric does not have to use every field carried by an event.

## Second metric: average save duration

The second metric calculates average duration of save attempts.

```python
class DocumentSaveAverageDurationMetric(Metric):
    def __init__(self) -> None:
        self._total = 0
        self._duration_ms_total = 0.0

    @property
    def metric_name(self) -> str:
        return "document_storage.save.average_duration"

    def handle_event(self, event: MetricEvent) -> bool:
        if not isinstance(event, DocumentSaveAttemptMetricEvent):
            return False

        self._total += 1
        self._duration_ms_total += event.duration_ms

        return True

    def snapshot(self) -> Mapping[str, Any]:
        average_duration_ms = 0.0

        if self._total > 0:
            average_duration_ms = self._duration_ms_total / self._total

        return {
            "name": self.metric_name,
            "dimensions": {
                "total": self._total,
                "duration_ms_total": self._duration_ms_total,
                "average_duration_ms": average_duration_ms,
            },
        }
```

This metric uses another part of the same event payload:

```text
duration_ms
```

It does not care whether the attempt succeeded or failed.

For this simple example, it measures average duration for all attempts.

A real component could define a more specific metric later, for example average duration for successful attempts only.

## Two metrics, one event

Both metrics consume the same event type:

```text
document_storage.save.attempt
```

But they interpret it differently.

```text
DocumentSaveAttemptsMetric
    uses outcome
    updates total, success_total, failure_total

DocumentSaveAverageDurationMetric
    uses duration_ms
    updates total, duration_ms_total, average_duration_ms
```

This is the important point.

The event is not owned by one metric.

The event is a domain fact.

Any registered metric can decide whether that fact is useful.

## Registering both metrics

The production component registers both metrics:

```python
def _register_metrics(self) -> None:
    if self._metrics_recorder is None:
        return

    metrics = (
        DocumentSaveAttemptsMetric(),
        DocumentSaveAverageDurationMetric(),
    )

    for metric in metrics:
        try:
            self._metrics_recorder.register_metric(metric=metric)
        except Exception:
            pass
```

The component owns the domain meaning of the metrics, so it registers them near the business code.

The recorder does not need to know what these metrics mean.

It only stores registered metrics and later dispatches events to them.

## Emitting the extended event

The business method now measures duration and includes it in the event.

```python
from time import perf_counter


def save_document(self, document_id: str, content: str) -> None:
    started_at = perf_counter()

    try:
        if not document_id:
            raise ValueError("document_id must not be empty")

        if not content:
            raise ValueError("content must not be empty")

    except Exception:
        duration_ms = (perf_counter() - started_at) * 1000

        self._send_metric_event(
            DocumentSaveAttemptMetricEvent(
                outcome=DocumentSaveAttemptOutcome.FAILURE,
                duration_ms=duration_ms,
            )
        )
        raise

    duration_ms = (perf_counter() - started_at) * 1000

    self._send_metric_event(
        DocumentSaveAttemptMetricEvent(
            outcome=DocumentSaveAttemptOutcome.SUCCESS,
            duration_ms=duration_ms,
        )
    )
```

The method still does not update counters.

It does not calculate average duration.

It only emits a richer event.

The metric layer decides how to use that event.

## Snapshot from the attempts metric

After two successful saves and one failed save, the attempts metric may expose this snapshot:

```python
{
    "name": "document_storage.save.attempts",
    "dimensions": {
        "total": 3,
        "success_total": 2,
        "failure_total": 1,
    },
}
```

This snapshot is produced by `DocumentSaveAttemptsMetric`.

It reflects how that metric interpreted the events.

## Snapshot from the average duration metric

The average duration metric exposes a different snapshot:

```python
{
    "name": "document_storage.save.average_duration",
    "dimensions": {
        "total": 3,
        "duration_ms_total": 7.5,
        "average_duration_ms": 2.5,
    },
}
```

This snapshot is produced by `DocumentSaveAverageDurationMetric`.

The exact duration values depend on the real runtime execution.

The important part is the shape:

```text
total
duration_ms_total
average_duration_ms
```

## Recorder snapshot

When application code asks the recorder for snapshots:

```python
snapshots = recorder.get_metric_snapshots()
```

the result contains both registered metrics:

```python
{
    "document_storage.save.attempts": {
        "name": "document_storage.save.attempts",
        "dimensions": {
            "total": 3,
            "success_total": 2,
            "failure_total": 1,
        },
    },
    "document_storage.save.average_duration": {
        "name": "document_storage.save.average_duration",
        "dimensions": {
            "total": 3,
            "duration_ms_total": 7.5,
            "average_duration_ms": 2.5,
        },
    },
}
```

The recorder does not build these metric states itself.

It asks each metric for its snapshot and returns the snapshots by metric name.

## Metric responsibilities

A metric has three main responsibilities.

### 1. Name itself

A metric exposes a stable name:

```python
@property
def metric_name(self) -> str:
    return "document_storage.save.attempts"
```

The metric name identifies the aggregate.

It is used as the key in recorder snapshots.

### 2. Handle relevant events

A metric receives events through `handle_event()`.

```python
def handle_event(self, event: MetricEvent) -> bool:
    if not isinstance(event, DocumentSaveAttemptMetricEvent):
        return False

    ...
    return True
```

The return value tells whether the metric accepted the event.

`False` means:

```text
this event is not for this metric
```

`True` means:

```text
this metric handled the event
```

### 3. Expose a snapshot

A metric exposes its current state through `snapshot()`.

```python
def snapshot(self) -> Mapping[str, Any]:
    return {
        "name": self.metric_name,
        "dimensions": {
            ...
        },
    }
```

A snapshot is the public view of the metric state.

It is what tests, diagnostics, dashboards, exporters, or other code can inspect.

## Metric state is private

The internal state of a metric belongs to that metric.

For example:

```python
self._total = 0
self._success_total = 0
self._failure_total = 0
```

or:

```python
self._total = 0
self._duration_ms_total = 0.0
```

Production code should not modify these values.

Other metrics should not modify these values.

The recorder should not modify these values directly.

Only the metric updates its own state when it accepts an event.

## Why metrics live near domain code

Metrics should normally be defined near the component that owns the domain meaning.

`DocumentStorage` knows what a save attempt means.

It knows what `SUCCESS` and `FAILURE` mean.

It knows which operation duration is worth measuring.

The generic recorder infrastructure does not need that knowledge.

This keeps the separation clear:

```text
domain code
    defines events and metrics

metrics infrastructure
    records, dispatches, processes, and exposes snapshots
```

## What metrics should not do

A metric should not own production behavior.

It should not call business methods.

It should not decide whether the business operation succeeds or fails.

It should not export to a backend directly as part of its core responsibility.

Its job is narrower:

```text
accept relevant events
update measured state
expose snapshot
```

Delivery and integration belong to recorders, runtime, hooks, adapters, or higher-level infrastructure.

## Summary

A `Metric` is an aggregate.

It consumes metric events.

It decides which events are relevant.

It updates its own measured state.

It exposes that state through a snapshot.

Several metrics can consume the same event and interpret different parts of its payload.

That is why production code emits structured events instead of updating counters directly.
