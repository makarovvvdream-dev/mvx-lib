# Recorders

This page documents recorder-related public API.

A recorder is the component that receives metric events, owns registered metric instances, processes accepted events,
and exposes metric snapshots.

The recorder API has two levels:

* `MetricsRecorderProto` — the small contract used by production components;
* `AsyncioMetricsRecorder` — the default asynchronous recorder implementation.

## Public API

```{eval-rst}
.. autoclass:: mvx.common.metrics.MetricsRecorderProto
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoenum:: mvx.common.metrics.AsyncioMetricsRecorderQueueOverflowPolicy

.. autoenum:: mvx.common.metrics.AsyncioMetricsRecorderOp

.. autoclass:: mvx.common.metrics.AsyncioMetricsRecorderOpResult
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.AsyncioMetricsRecorderWaitHandle
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoenum:: mvx.common.metrics.AsyncioMetricsRecorderState

.. autoclass:: mvx.common.metrics.AsyncioMetricsRecorder
   :members:
   :member-order: bysource
   :class-doc-from: class
```

## Recorder contract

`MetricsRecorderProto` is the narrow interface expected by production code.

A component that emits metric events should depend on this contract, not on a concrete recorder implementation.

Typical component constructor:

```python
class DocumentStorage:
    def __init__(
            self,
            *,
            metrics_recorder: MetricsRecorderProto | None = None,
    ) -> None:
        self._metrics_recorder = metrics_recorder
```

This keeps metrics optional and keeps the component independent from recorder implementation details.

## Default recorder implementation

`AsyncioMetricsRecorder` is the default recorder implementation.

It is bound to a running `asyncio` event loop at construction time.

For normal application wiring, recorders are usually created through `MetricsRuntime`, which provides the required
thread and event loop.

Direct `AsyncioMetricsRecorder` construction is useful for advanced cases where the application already owns the
recorder execution environment.

## Lifecycle operations

`start()` and `stop()` return `AsyncioMetricsRecorderWaitHandle`.

The handle can be awaited:

```python
result = await recorder.start()
```

or waited synchronously:

```python
result = recorder.start().wait()
```

Both forms return `AsyncioMetricsRecorderOpResult`.

Check the result when managing recorder lifecycle directly:

```python
result = await recorder.start()
if not result.success:
    raise result.error
```

Runtime-created recorders are started and stopped by `MetricsRuntime`.

## Queue overflow policy

`AsyncioMetricsRecorderQueueOverflowPolicy` controls what happens when the recorder reaches its configured pending-event
limit.

`RAISE_ERROR` makes overflow visible by raising `AsyncioMetricsRecorderQueueOverflowError`.

`DROP` drops the new event.

The policy is configured when creating the recorder directly or when creating recorders through `MetricsRuntime`.

## Metric registration

Metrics are registered with:

```python
recorder.register_metric(metric=DocumentSaveAttemptsMetric())
```

The recorder stores metrics by `metric_name`.

When events are processed, registered metrics receive those events through their `handle_event()` method.

## Event registration

Metric events are submitted with:

```python
recorder.register_event(event=event)
```

This is the production handoff point.

The recorder accepts the event, places it into its processing path, and dispatches it to registered metrics on the
metrics side.

If the recorder is still in `VIRGIN` state, event registration schedules recorder startup.

## Snapshots and inspection

Recorder snapshots are read with:

```python
snapshots = recorder.get_metric_snapshots()
```

The result is a mapping from metric name to metric snapshot.

Registered metrics can also be inspected with:

```python
metrics = recorder.iter_metrics()
```

These methods are inspection APIs. They do not submit new metric events.

## Extension hooks

`AsyncioMetricsRecorder` provides protected hooks for subclasses:

* `_on_starting()`;
* `_on_stopped()`;
* `_on_metric_changed(metric=..., event=...)`.

These hooks are documented here because they are part of the extension surface of the default recorder.

Use them when implementing custom recorder behavior.

For example, `_on_metric_changed()` is called only after a metric accepts an event and updates its state.

## Logging

`AsyncioMetricsRecorder` supports `MVX Logger` integration through an optional `log_context`.

Selected public recorder methods are instrumented with `log_invocation`.

`register_event()` is intentionally not wrapped with `log_invocation`, because it is the hot-path metric event handoff.

Ready-to-use recorder logging policies are documented separately in the logging policies API page.

