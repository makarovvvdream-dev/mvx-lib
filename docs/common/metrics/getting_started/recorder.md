# Recorders

```{contents} Contents:
:depth: 1
:local:
```

A recorder is the component that receives metric events from production code and delivers them to the metrics it knows
about.

A recorder:

* owns registered metric instances;
* accepts metric events from production code;
* places accepted events into its internal processing buffer;
* processes events on the metrics side;
* dispatches events to registered metrics;
* exposes metric snapshots.

It does not interpret business meaning.

It does not know what `SUCCESS`, `FAILURE`, `duration_ms`, or `bytes_received` mean for a particular domain.

Metrics own that interpretation.

Production code emits metric events through the recorder:

```python
self._metrics_recorder.register_event(event=event)
```

From the production method's point of view, this is a short synchronous handoff.

The method creates a structured event, passes it to the recorder, and continues its own work.

Thus a recorder is the boundary between production code and metrics processing.

## Recorder as a scope

A recorder represents one metrics scope.

That scope can be one component, one connection, one session, one worker, one protocol client, or another measured
entity.

For example, a connection pool can use one recorder per connection:

```text
connection-001 -> recorder(entity_id="connection-001")
connection-002 -> recorder(entity_id="connection-002")
connection-003 -> recorder(entity_id="connection-003")
```

Each recorder stores metric state for its own connection.

The metric classes may be the same, but the metric instances are separate:

```text
connection-001 recorder
    TcpStreamBytesReceivedMetric(total=1200)

connection-002 recorder
    TcpStreamBytesReceivedMetric(total=8750)
```

This gives each measured entity isolated metrics.

## Entity id

`entity_id` identifies the measured scope represented by a recorder.

Examples:

```text
connection-42
session-a17f
worker-3
document-storage-main
```

If a recorder represents `connection-42`, then snapshots from that recorder describe `connection-42`.

The recorder still does not need to know what a connection is. The id only gives application code a stable way to
associate recorder state with the measured object.

## Runtime-managed processing

The simplest way to use a recorder in regular application code is to create it through `MetricsRuntime`.

```python
recorder = runtime.create_recorder("document_storage")
```

Then pass it to a production component:

```python
storage = DocumentStorage(metrics_recorder=recorder)
```

In this mode, the runtime creates, starts, owns, and later stops the recorder inside its managed processing environment.

`MetricsRuntime` owns a separate thread with its own `asyncio` event loop. A runtime-created recorder uses that
environment for event processing.

The path is:

```text
production code
    -> recorder.register_event(event)
    -> recorder internal buffer
    -> runtime-owned thread / asyncio event loop
    -> recorder dispatches event to registered metrics
```

This keeps metric processing outside the production hot path.

The component only sees the recorder contract. It does not manage recorder startup, event loop ownership, processing
lifecycle, or shutdown.

Although the default recorder is built on top of `asyncio`, production code that uses it through `MetricsRuntime` does
not have to be asynchronous.

The production component can be synchronous:

```python
def save_document(self, document_id: str, content: str) -> None:
    ...
    self._send_metric_event(event)
```

or asynchronous:

```python
async def save_document(self, document_id: str, content: str) -> None:
    ...
    self._send_metric_event(event)
```

In both cases, event emission is a regular recorder call from the production side.

The runtime owns the `asyncio` event loop used for metrics processing. Production code does not need to run inside that
loop and does not need to be converted to async only because metrics are enabled.

A recorder can also be used without `MetricsRuntime`.

In that case, the application owns more of the machinery: the recorder must be created in an environment with an
available `asyncio` event loop, started explicitly, used, and stopped correctly.

The recorder has its own lifecycle, status, processing buffer, event registration API, metric registration API, snapshot
inspection API, and post-processing extension points.

This direct usage model is useful for custom integrations and applications that already own a suitable event-loop
environment.

Manual recorder lifecycle management is covered in the advanced usage section.

## Registering metrics

A component registers the metrics it owns:

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

The recorder stores those metric instances.

Later, when events arrive, the recorder gives each registered metric a chance to handle the event.

## Sending events

The component usually centralizes event emission in one helper:

```python
def _send_metric_event(self, event: MetricEvent) -> None:
    if self._metrics_recorder is None:
        return

    try:
        self._metrics_recorder.register_event(event=event)
    except Exception:
        pass
```

This keeps metrics optional and keeps the business method clean.

If no recorder is provided, the component works without metrics.

If the recorder fails, this example does not let metrics infrastructure break the business operation.

## Dispatching events

When the recorder processes an event, it passes the event to registered metrics.

A metric accepts or ignores the event:

```python
def handle_event(self, event: MetricEvent) -> bool:
    if not isinstance(event, DocumentSaveAttemptMetricEvent):
        return False

    ...
    return True
```

The recorder does not decide which values to update.

The metric decides that.

## Snapshots

The simplest way to inspect recorder state is:

```python
snapshots = recorder.get_metric_snapshots()
```

This returns snapshots for metrics registered in that recorder.

If the recorder represents one connection, these snapshots describe that connection.

If the recorder represents one component instance, these snapshots describe that component instance.

## What a recorder is not

A recorder is not a metric.

It is not a monitoring backend.

It is not a dashboard.

It does not define domain meaning.

It does not update metric dimensions directly.

It connects production event emission with metrics-side processing.

## Summary

A recorder is the metrics boundary for one measured scope.

It accepts events, buffers them, processes them on the metrics side, dispatches them to registered metrics, and exposes
snapshots.

`entity_id` lets application code associate a recorder with a specific measured entity, such as one connection, one
session, one worker, or one component instance.
