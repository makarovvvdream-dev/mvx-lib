# Starting to use MVX Metrics - Behind the scenes

```{contents} Contents:
:depth: 2
:local:
```

This page explains what happens inside `MVX Metrics` when the `DocumentStorage` example runs.

It intentionally avoids internal implementation details. The goal is to show the path of one metric event from
production code to a metric snapshot.

The example has four visible parts:

* `DocumentStorage` as the production component;
* `DocumentSaveAttemptMetricEvent` as the event;
* `DocumentSaveAttemptsMetric` as the metric;
* `MetricsRuntime` and recorder as the metrics infrastructure components.

## The short version

When `save_document()` is called, the `production component` does not update counters directly. Instead, it emits a
metric `event`:

```text
document save attempt happened
outcome = SUCCESS or FAILURE
```

The `recorder` receives this `event` and passes it to registered `metrics`.

The `metric` checks whether the `event` is relevant to it.

If it is relevant, the `metric` updates its internal measured state.

Later, the application asks the `recorder` for snapshots and sees the current aggregated result.

The path is:

```text
DocumentStorage.save_document()
    -> DocumentSaveAttemptMetricEvent
    -> recorder.register_event(...)
    -> DocumentSaveAttemptsMetric.handle_event(...)
    -> metric state changes
    -> recorder.get_metric_snapshots()
```

## Step 1. The application creates a runtime

The example starts with:

```python
runtime = MetricsRuntime(namespace="example.metrics")
runtime.start()
```

`MetricsRuntime` creates and owns a separate runtime environment for metrics processing. When started, it runs metrics
infrastructure in a dedicated thread with its own asyncio event loop. Recorders created by this runtime use that managed
loop for event processing.

For this first example, it is enough to think about `MetricsRuntime` as the object that isolates metric processing from
production code.

The `production component` does not create this runtime. Application code does.

`DocumentStorage` only receives a `recorder` and emits metric `events` through that `recorder`. The actual processing of
those events happens behind the recorder boundary, inside the runtime-managed metrics infrastructure.

## Step 2. The application creates a recorder

Next, the example creates a `recorder`:

```python
recorder = runtime.create_recorder("document_storage")
```

The `recorder` is the object passed into the `production component`.

It is the boundary between production code and metrics infrastructure.

From the point of view of `DocumentStorage`, the `recorder` has only two important responsibilities:

* accept `metrics` during setup;
* accept `metric events` during work.

The `production component` does not need to know how the `recorder` is started, stopped, or managed.

## Step 3. The component receives the recorder

The application passes the `recorder` to `DocumentStorage`:

```python
storage = DocumentStorage(metrics_recorder=recorder)
```

Inside the `production component`, the `recorder` is optional.

If no `recorder` is passed, `DocumentStorage` still works normally.

If a `recorder` is passed, the `production component` registers its `metric`:

```python
self._metrics_recorder = metrics_recorder
self._register_metrics()
```

This means the `production component` says:

```text
These are the metrics that understand my events.
```

It does not say where those metrics will be exported.

It does not say how snapshots will be used.

It only registers the metric objects that belong to this component.

## Step 4. The component registers its metric

The example registers one `metric`:

```python
DocumentSaveAttemptsMetric()
```

This `metric` knows how to interpret save-attempt events.

It starts with empty state:

```text
total = 0
success_total = 0
failure_total = 0
```

The `recorder` stores this `metric`.

At this point, nothing has been measured yet.

The `recorder` only knows:

```text
When events arrive, this metric should be given a chance to handle them.
```

## Step 5. A business method is called

Now the application calls:

```python
storage.save_document("doc-001", "First document")
```

The method performs its business work.

In this small example, the work is only validation. In a real component, this could be a database write, network
request, file operation, protocol command, or domain operation.

If the method succeeds, it emits:

```python
DocumentSaveAttemptMetricEvent(
    outcome=DocumentSaveAttemptOutcome.SUCCESS,
)
```

If the method fails, it emits:

```python
DocumentSaveAttemptMetricEvent(
    outcome=DocumentSaveAttemptOutcome.FAILURE,
)
```

The method does not increment either `success_total` or `failure_total`.

It only reports the fact that happened during fulfillment of the business method.

## Step 6. The helper sends the event

The `production component` sends events through the helper:

```python
self._send_metric_event(...)
```

This helper checks whether a `recorder` exists.

If there is no `recorder`, it returns immediately.

If there is a `recorder`, it calls:

```python
self._metrics_recorder.register_event(event=event)
```

This keeps the production code simple.

The business method does not need to repeat `recorder` checks everywhere.

It also keeps metrics optional. The component can run with or without metrics.

## Step 7. The recorder receives the event

The `production component` sends the `event` through the `recorder`:

```python
self._metrics_recorder.register_event(event=event)
```

This call is the boundary between the **production hot path** and **metrics processing**.

For the production method, event emission is a short synchronous handoff: the method creates a structured `event` and
passes it to the `recorder`.

The `recorder` accepts the `event` and places it into its internal processing buffer. After that, the production method
can continue its own work.

In the `MetricsRuntime` setup used by this example, the actual event processing is isolated from production code. The
runtime owns a separate thread with its own `asyncio` event loop, and the `recorder` processes buffered events there.

So the path is:

```text
DocumentStorage.save_document()
    -> recorder.register_event(event)
    -> recorder internal buffer
    -> runtime thread / asyncio event loop
    -> recorder dispatches the event to registered metrics
```

At this point, the `event` is still only a structured fact:

```text
event_type = document_storage.save.attempt
outcome = SUCCESS
```

The `recorder` does not interpret the business meaning of this `event`. Its job is to accept the `event` at the
production
boundary, move it into the runtime-managed processing side, and dispatch it to registered metrics.

## Step 8. The metric decides whether the event matters

The `recorder` gives the `event` to the `metric`.

The `metric` runs:

```python
handle_event(event)
```

Inside `DocumentSaveAttemptsMetric`, the first question is:

```python
if not isinstance(event, DocumentSaveAttemptMetricEvent):
    return False
```

This means:

```text
This metric only handles document save attempt events.
Other events are ignored.
```

If the `event` is relevant, the `metric` updates its measured state.

For a successful event:

```text
total += 1
success_total += 1
```

For a failed event:

```text
total += 1
failure_total += 1
```

This is the key idea.

The `production component` emits facts.

The `metric` turns facts into measurements.

## Step 9. Several calls build aggregated state

The example calls:

```python
storage.save_document("doc-001", "First document")
storage.save_document("doc-002", "Second document")
```

Both calls succeed.

Then it calls:

```python
storage.save_document("", "Broken document")
```

This call fails validation and raises `ValueError`.

The example catches that error so the script can continue.

After these three calls, the `metric` has seen:

```text
SUCCESS
SUCCESS
FAILURE
```

So its state becomes:

```text
total = 3
success_total = 2
failure_total = 1
```

No production method manually created this final result.

It is the result of metric aggregation.

## Step 10. The application reads snapshots

At the end, the example asks the `recorder`:

```python
snapshots = recorder.get_metric_snapshots()
```

The `recorder` asks registered `metrics` for their current snapshots.

The `metric` returns a plain structured view of its current state:

```text
{
    "name": "document_storage.save.attempts",
    "dimensions": {
        "total": 3,
        "success_total": 2,
        "failure_total": 1,
    },
}
```

The `recorder` returns snapshots as a mapping by metric name:

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

This is the built-in way to inspect metrics in the simple in-memory setup.

## What each part owns

The example has a clear ownership model.

### Production component

`DocumentStorage` owns the business method:

* it knows when a save attempt succeeds or fails
* it emits metric events
* it does not own metrics infrastructure

### Metric event

`DocumentSaveAttemptMetricEvent` describes what happened:

* it carries the outcome
* it does not count anything

### Metric

`DocumentSaveAttemptsMetric` owns the measured state:

* it decides which events matter
* it updates totals
* it exposes a snapshot

### Recorder

The `recorder` connects `events` to `metrics`:

* it accepts registered metrics
* it accepts metric events
* it lets the application read snapshots
* it does not know business meaning

### Runtime

`MetricsRuntime` owns the managed runtime around recorders:

* it makes wiring easier for application code
* it keeps metrics processing outside the production code processing

## Why this structure matters

* This structure keeps observability flexible.
* The production `component` only depends on a small `recorder` contract.
* The `metric` meaning stays near the domain code.
* The `recorder` and `runtime` remain generic infrastructure.
* The application decides how metrics are wired.

Later, the same `production component` can be used with a different recorder, adapter, exporter, dashboard, test probe,
or monitoring backend without changing the business method.

That is the purpose of the intermediate metrics layer.

