# Ownership model

```{contents} Contents:
:depth: 1
:local:
```

`MVX Metrics` uses explicit ownership boundaries.

Each part of the system owns one kind of responsibility:

```text
application code
   |
   v
owns MetricsRuntime lifecycle

MetricsRuntime
   |
   v
owns recorder environment and recorder registry

recorder
   |
   v
owns registered metric instances for one measured scope

metric
   |
   v
owns measured state

production component
   |
   v
owns business behavior and emits metric events
```

This page explains these ownership boundaries.

It does not describe the full event processing path. That is covered in the processing pipeline page.

## Application owns the runtime

Application code owns `MetricsRuntime`.

It decides:

* whether metrics are enabled;
* when metrics processing starts;
* which runtime namespace is used;
* which recorders are created;
* when the runtime is shut down.

Typical wiring:

```python
runtime = MetricsRuntime(namespace="example.metrics")
runtime.start()

try:
    recorder = runtime.create_recorder("document_storage")
    storage = DocumentStorage(metrics_recorder=recorder)

    storage.save_document("doc-001", "First document")

finally:
    runtime.shutdown()
```

The production component does not create the runtime.

The production component receives a recorder.

This keeps runtime lifecycle out of business code.

## MetricsRuntime owns the recorder environment

`MetricsRuntime` owns the execution environment used by recorders created inside it.

It owns:

* a dedicated thread;
* an `asyncio` event loop inside that thread;
* the registry of runtime-created recorders;
* runtime startup and shutdown.

A runtime-created recorder belongs to that runtime.

```text
MetricsRuntime(namespace="example.metrics")
   |
   v
recorder: document_storage
```

For larger systems, one runtime can own many recorders:

```text
MetricsRuntime(namespace="networking.metrics")
   |
   +--> recorder: connection-001
   |
   +--> recorder: connection-002
   |
   +--> recorder: connection-003
```

The usual model is one runtime for an application or large subsystem, with multiple recorders inside it.

## Recorder owns metric instances

A recorder owns the metric instances registered inside it.

```text
recorder: document_storage
   |
   +--> DocumentSaveAttemptsMetric
   |
   +--> DocumentSaveAverageDurationMetric
```

If the same metric class is used in several recorders, each recorder still owns its own metric instance.

```text
recorder: connection-001
   |
   v
TcpStreamBytesReceivedMetric(total=1200)

recorder: connection-002
   |
   v
TcpStreamBytesReceivedMetric(total=8750)
```

The metric class is reusable.

The metric state is not shared.

This allows per-entity metrics.

## Recorder owns one measured scope

A recorder represents one measured scope.

The scope may be:

* one component;
* one connection;
* one session;
* one worker;
* one protocol client;
* one domain object.

The recorder identity should normally match that scope.

```text
recorder identity: connection-001
   |
   v
metrics for connection-001
```

This does not mean the recorder understands the domain.

The recorder does not know what a connection is.

It only owns the metric instances associated with that recorder identity.

## Metric owns measured state

A metric owns its internal measured state.

For example:

```python
class DocumentSaveAttemptsMetric(Metric):
    def __init__(self) -> None:
        self._total = 0
        self._success_total = 0
        self._failure_total = 0
```

Only the metric updates this state.

The production component does not update it.

The recorder does not update it directly.

Other metrics do not update it.

The metric changes its state when it accepts an event in `handle_event()`.

```text
MetricEvent
   |
   v
metric.handle_event(event)
   |
   v
metric updates its own state
```

## Metric owns interpretation

A metric also owns interpretation.

The recorder can deliver events to the metric, but the metric decides whether the event matters.

```python
def handle_event(self, event: MetricEvent) -> bool:
    if not isinstance(event, DocumentSaveAttemptMetricEvent):
        return False

    ...
    return True
```

This keeps business measurement rules inside metric classes.

For example:

```text
DocumentSaveAttemptsMetric
   |
   v
knows how save-attempt outcomes change attempt counters

DocumentSaveAverageDurationMetric
   |
   v
knows how save-attempt durations change duration state
```

The recorder remains generic.

## Production component owns business behavior

The production component owns the business operation.

For example:

```python
class DocumentStorage:
    def save_document(self, document_id: str, content: str) -> None:
        ...
```

The production component decides when a domain operation succeeds or fails.

It emits a metric event to describe that fact.

It does not own metric state.

It does not own recorder processing.

It does not own runtime lifecycle.

In the usual integration pattern, the component receives an optional recorder:

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

This keeps metrics integration explicit but optional.

## Snapshot ownership

A metric creates its own snapshot.

```python
def snapshot(self) -> Mapping[str, Any]:
    return {
        "name": self.metric_name,
        "dimensions": {
            ...
        },
    }
```

The recorder collects snapshots from metrics registered inside it.

```python
snapshots = recorder.get_metric_snapshots()
```

So ownership is layered:

```text
metric
   |
   v
creates metric snapshot

recorder
   |
   v
collects snapshots for its registered metrics

application code
   |
   v
reads recorder snapshots
```

A snapshot is public state, but it is still produced by the metric that owns the underlying measured state.

## Ownership summary

The ownership model can be summarized as:

```text
Application
   |
   v
owns MetricsRuntime lifecycle

MetricsRuntime
   |
   v
owns recorder registry and processing environment

Recorder
   |
   v
owns metric instances for one measured scope

Metric
   |
   v
owns measured state and interpretation

Production component
   |
   v
owns business behavior and emits events
```

Each owner has a narrow role.

This is what keeps the system modular.

## Why this matters

The ownership model keeps the main parts replaceable.

Production code can keep emitting the same events.

Metrics can change their internal state or snapshot shape.

Recorders can change processing or delivery behavior.

Runtime wiring can change at application level.

External adapters can be added later.

The important part is that these changes do not require one component to take ownership of another component's job.

```text
production code
   |
   v
does not own metric state

metric
   |
   v
does not own runtime lifecycle

recorder
   |
   v
does not own business meaning

runtime
   |
   v
does not own domain behavior
```

That separation is the core of the architecture.
