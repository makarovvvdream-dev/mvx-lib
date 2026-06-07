# Starting to use MVX Metrics

```{contents} Contents:
:depth: 1
:local:
```

`MVX Metrics` is usually introduced from production code.

The starting point is not a recorder, a queue, or an exporter. The starting point is a component whose business methods
need measurable runtime visibility.

This page shows the basic pattern:

1. start with a production class;
2. decide what should be measured;
3. define metric events and metrics near that class;
4. accept a metrics recorder in the constructor;
5. register metrics;
6. emit metric events from business methods;
7. wire everything from application code through `MetricsRuntime`;
8. read metric snapshots.

The example is intentionally small. It uses one business method, one event type, and one metric.

More advanced patterns, such as multiple metrics consuming the same event, richer snapshots, recorder hooks, and
external adapters, are covered later.

## Production code first

Assume there is a small production component:

```python
class DocumentStorage:
    def save_document(self, document_id: str, content: str) -> None:
        try:
            if not document_id:
                raise ValueError("document_id must not be empty")

            if not content:
                raise ValueError("content must not be empty")

        except Exception:
            self._send_metric_event(
                DocumentSaveAttemptMetricEvent(
                    outcome=DocumentSaveAttemptOutcome.FAILURE,
                )
            )
            raise

        self._send_metric_event(
            DocumentSaveAttemptMetricEvent(
                outcome=DocumentSaveAttemptOutcome.SUCCESS,
            )
        )
```

The method has business behavior.

Now suppose the application needs to observe how this method behaves at runtime.

For the first version, we want to know:

* how many save attempts happened;
* how many save attempts completed successfully;
* how many save attempts failed.

The production method should not manually update raw counters. It should only report what happened.

The metric will decide how that event changes the measured state.

## Step 1. Define what should be measured

For this example, one metric is enough:

```text
document_storage.save.attempts
```

It will count:

* total save attempts;
* successful save attempts;
* failed save attempts.

At this stage, this is a domain decision.

The `DocumentStorage` domain knows that `save_document()` can succeed or fail. The metrics infrastructure does not need
to know what a document is, why saving can fail, or what the outcome means for the application.

## Step 2. Define metric events and metrics near the business code

`Metric events` describe what happened.

`Metrics` consume those events and keep measured state.

The event type used by this example is:

```python
@dataclass(frozen=True, slots=True)
class DocumentSaveAttemptMetricEvent(MetricEvent):
    outcome: DocumentSaveAttemptOutcome

    @property
    def event_type(self) -> str:
        return "document_storage.save.attempt"
```

The metric that consumes this event is:

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

The important point is the responsibility split.

`DocumentSaveAttemptMetricEvent` says that a document save attempt happened and records its outcome.

`DocumentSaveAttemptsMetric` decides whether that event is relevant and how its internal state should change.

The production component does not update `total`, `success_total`, or `failure_total` directly. It even doesn't know
that they exist.

## Step 3. Add MVX Metrics to the production component

The production class accepts a recorder instance, or `None`.

When no recorder is provided, the component works without metrics.

```python
class DocumentStorage:
    def __init__(
            self,
            *,
            metrics_recorder: MetricsRecorderProto | None = None,
    ) -> None:
        self._metrics_recorder = metrics_recorder
        self._register_metrics()
```

This keeps metrics optional. Thus the component can be still used in tests, scripts, or applications that do not
configure metrics at all.

## Step 4. Register metrics

The component registers the metrics it owns.

```python
def _register_metrics(self) -> None:
    if self._metrics_recorder is None:
        return

    metrics = (
        DocumentSaveAttemptsMetric(),
    )

    for metric in metrics:
        try:
            self._metrics_recorder.register_metric(metric=metric)
        except Exception:
            pass
```

The metrics are defined near the domain component because the domain component owns the meaning of the metric.

The `recorder` does not need to know what the metric means. It only stores registered metrics and dispatches events to
them.

## Step 5. Add one helper for metric events

Use one helper method as the single emission point inside the component. It is not a requirement, it is just convenient.

```python
def _send_metric_event(self, event: MetricEvent) -> None:
    if self._metrics_recorder is None:
        return

    try:
        self._metrics_recorder.register_event(event=event)
    except Exception:
        pass
```

This keeps the business method clean.

It also keeps the component resilient: a metrics infrastructure failure should not normally break the business
operation.

## Step 6. Emit events from business methods

Now the business method emits metric events around its actual behavior.

```python
def save_document(self, document_id: str, content: str) -> None:
    try:
        if not document_id:
            raise ValueError("document_id must not be empty")

        if not content:
            raise ValueError("content must not be empty")

    except Exception:
        self._send_metric_event(
            DocumentSaveAttemptMetricEvent(
                outcome=DocumentSaveAttemptOutcome.FAILURE,
            )
        )
        raise

    self._send_metric_event(
        DocumentSaveAttemptMetricEvent(
            outcome=DocumentSaveAttemptOutcome.SUCCESS,
        )
    )
```

The method still performs its own work. It does not know how metrics are stored, delivered, exported, or inspected. It
only reports the domain fact: saving a document succeeded or failed.

## Step 7. Wire metrics with MetricsRuntime

The production component only accepts a recorder.

Application code decides whether metrics are enabled and which recorder should be passed to the component.

The production component only accepts a recorder.

For ordinary sync/async code, the simplest wiring option is using `MetricsRuntime`.

```python
def main() -> None:
    runtime = MetricsRuntime(namespace="example.metrics")
    runtime.start()

    try:
        recorder = runtime.create_recorder("document_storage")

        storage = DocumentStorage(metrics_recorder=recorder)

        storage.save_document("doc-001", "First document")
        storage.save_document("doc-002", "Second document")

        try:
            storage.save_document("", "Broken document")
        except ValueError:
            pass

        snapshots = recorder.get_metric_snapshots()
        pprint(snapshots)

    finally:
        runtime.shutdown()
```

This is the application-side part of the example:

* `MetricsRuntime` owns the runtime side of metrics processing.
* `DocumentStorage` only receives a recorder and does not know how that recorder is created, how it works, how the
  events are handled, or how metric snapshots will be inspected.

## Step 8. Read metric snapshots

Out of the box, the simplest way to inspect collected metrics is to read snapshots from the recorder.

```python
snapshots = recorder.get_metric_snapshots()
pprint(snapshots)
```

For this example, the snapshot shows three save attempts:

* two successful attempts;
* one failed attempt.

The complete output is shown below in the runnable example section.

## Complete runnable example

The full runnable version of this example is available as a source file:

{download}`Download document_storage.py <../../../../common/examples/metrics/document_storage.py>`

The same file is covered by tests, so the documented example is checked as real Python code.

```python
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pprint import pprint
from typing import Any, Mapping

from mvx.common.metrics import Metric, MetricEvent, MetricsRecorderProto, MetricsRuntime


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


class DocumentStorage:
    def __init__(
            self,
            *,
            metrics_recorder: MetricsRecorderProto | None = None,
    ) -> None:
        self._metrics_recorder = metrics_recorder
        self._register_metrics()

    def _register_metrics(self) -> None:
        if self._metrics_recorder is None:
            return

        metrics = (
            DocumentSaveAttemptsMetric(),
        )

        for metric in metrics:
            try:
                self._metrics_recorder.register_metric(metric=metric)
            except Exception:
                pass

    def _send_metric_event(self, event: MetricEvent) -> None:
        if self._metrics_recorder is None:
            return

        try:
            self._metrics_recorder.register_event(event=event)
        except Exception:
            pass

    def save_document(self, document_id: str, content: str) -> None:
        try:
            if not document_id:
                raise ValueError("document_id must not be empty")

            if not content:
                raise ValueError("content must not be empty")

        except Exception:
            self._send_metric_event(
                DocumentSaveAttemptMetricEvent(
                    outcome=DocumentSaveAttemptOutcome.FAILURE,
                )
            )
            raise

        self._send_metric_event(
            DocumentSaveAttemptMetricEvent(
                outcome=DocumentSaveAttemptOutcome.SUCCESS,
            )
        )


def main() -> None:
    runtime = MetricsRuntime(namespace="example.metrics")
    runtime.start()

    try:
        recorder = runtime.create_recorder("document_storage")

        storage = DocumentStorage(metrics_recorder=recorder)

        storage.save_document("doc-001", "First document")
        storage.save_document("doc-002", "Second document")

        try:
            storage.save_document("", "Broken document")
        except ValueError:
            pass

        snapshots = recorder.get_metric_snapshots()
        pprint(snapshots)

    finally:
        runtime.shutdown()


if __name__ == "__main__":
    main()
```

This is the complete production-side and application-side example in one module.

## Run the example

From the `common` package directory, run:

```bash
python examples/metrics/document_storage.py
```

The output should show one registered metric snapshot:

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

## What this example keeps simple

This first example intentionally keeps the model small.

It uses:

* one production class;
* one business method;
* one event type;
* one metric;
* one recorder;
* one runtime;
* one snapshot read.

A real component may define several metrics, emit several event types, expose richer snapshots, or use custom recorders
and adapters.

Those topics are covered in later sections.
