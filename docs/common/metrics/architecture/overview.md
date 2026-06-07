# Architecture

```{contents} Contents:
:depth: 1
:local:
```

`MVX Metrics` is built as a metric event processing pipeline.

The architecture is centered around a small set of cooperating components:

* production components emit metric events;
* recorders receive events and process them on the metrics side;
* metrics aggregate accepted events into internal state;
* snapshots expose current metric state;
* `MetricsRuntime` provides an execution environment for recorders.

The goal of the architecture is not to choose a final monitoring backend.

The goal is to separate production code from metric processing, while still making metric state observable from the
beginning.

## Architectural goal

`MVX Metrics` separates several responsibilities that are often mixed together in application code.

```text
production component
    owns business behavior

metric event
    carries operation facts

recorder
    is the handoff and processing boundary

metric
    owns measurement state

snapshot
    exposes current state

runtime
    owns recorder execution environment
```

This separation is what allows production code to emit structured metric events without knowing how those events will be
aggregated, inspected, tested, exported, or delivered later.

## Main runtime shape

At runtime, a production component normally holds only a recorder reference.

```text
Application
    MetricsRuntime
        Recorder(entity_id="document_storage")
            DocumentSaveAttemptsMetric
            DocumentSaveAverageDurationMetric

Production component
    DocumentStorage(metrics_recorder=recorder)
```

The production component does not own the runtime.

It does not own the recorder lifecycle.

It does not own metric state.

It only emits metric events through the recorder.

The recorder owns metric instances for one measured scope.

The runtime owns the environment in which runtime-created recorders process events.

## Main processing pipeline

The normal processing path is:

```text
production method
    -> create MetricEvent
    -> recorder.register_event(event)
    -> recorder internal buffer
    -> recorder processing side
    -> metric.handle_event(event)
    -> metric state changes
    -> snapshot exposes current state
```

When a recorder is created by `MetricsRuntime`, the recorder processing side runs in the runtime-owned thread with its
own `asyncio` event loop.

The production method does only a short synchronous handoff to the recorder.

Metric handling happens after that handoff, on the metrics-processing side.

## Production boundary

The recorder is the boundary seen by production code.

Production code calls:

```python
self._metrics_recorder.register_event(event=event)
```

At that point, the event crosses from production code into metrics infrastructure.

This boundary is deliberately small.

Production code creates a metric event.

The recorder accepts it.

Everything else belongs to the metrics side.

## Metric ownership

A metric owns its measured state.

The recorder can give events to metrics, but it does not decide what an event means.

A metric decides whether an event is relevant:

```text
event accepted
    -> metric updates its own state

event ignored
    -> metric state is unchanged
```

This keeps metric interpretation close to metric implementation.

The recorder remains generic.

## Recorder ownership

A recorder owns the metric instances registered inside it.

This gives every recorder an isolated metric scope.

For example, a connection pool may create one recorder per connection:

```text
MetricsRuntime
    Recorder(entity_id="connection-001")
        TcpStreamOpenAttemptsMetric
        TcpStreamBytesReceivedMetric

    Recorder(entity_id="connection-002")
        TcpStreamOpenAttemptsMetric
        TcpStreamBytesReceivedMetric
```

The same metric classes may be used in both recorders.

The metric instances are different.

Their state is not shared.

## Runtime ownership

`MetricsRuntime` owns the execution environment for runtime-created recorders.

It owns:

* a dedicated thread;
* an `asyncio` event loop inside that thread;
* a registry of recorders created inside the runtime;
* runtime startup and shutdown.

The usual model is one runtime for an application or large subsystem, with multiple recorders inside that runtime.

```text
application
    MetricsRuntime
        recorder: document_storage
        recorder: connection-001
        recorder: connection-002
```

Creating one runtime per measured entity is not the intended model.

Recorders are the per-entity units.

The runtime is the shared execution environment.

## Snapshot boundary

Snapshots are the public read boundary of metric state.

A metric exposes its own snapshot.

A recorder collects snapshots from the metrics registered inside it.

```text
metric state
    -> metric.snapshot()
    -> recorder.get_metric_snapshots()
```

Snapshots expose current state.

They do not expose event history.

They are also the natural place where tests, diagnostics, dashboards, exporters, or external monitoring adapters can
read the current metric state.

## Extension boundaries

`MVX Metrics` has several extension points.

The important ones are:

```text
custom Metric
    defines new measurement logic

custom recorder
    changes event processing or delivery behavior

_on_metric_changed
    reacts after a metric accepts an event

snapshot consumer
    reads current state

external adapter
    publishes snapshots or metric state to another platform
```

These extension points sit outside production code.

A production component can continue emitting the same metric events while the metrics infrastructure grows around it.

## Error and lifecycle boundaries

Production code and metrics infrastructure have different failure concerns.

A production component may choose to treat recorder errors as non-fatal for business operations.

Recorder and runtime code, however, still have their own lifecycle, status, queue, overflow, and shutdown behavior.

This separation lets simple production methods stay small while the metrics infrastructure can still be managed and
tested as a real component.

Detailed lifecycle and error behavior is covered in the recorder and runtime documentation.

## Threading boundary

`MVX Metrics` does not require production code to be asynchronous.

Production code may be synchronous or asynchronous.

When using `MetricsRuntime`, the runtime owns the `asyncio` event loop used for recorder processing.

The production component does not have to run inside that loop.

The threading model is:

```text
production thread or task
    -> recorder handoff

runtime thread / asyncio event loop
    -> recorder processing
```

Manual recorder usage without `MetricsRuntime` is possible, but then the application must provide and manage the
appropriate event-loop environment.

## What this overview does not cover

This page explains the system shape.

It does not document every method, status, error, or lifecycle transition.

Those details are covered by component-specific pages:

```{toctree}
:maxdepth: 1
:caption: Architecture topics

processing_pipeline
ownership_model
recorder_processing
runtime_model
snapshot_surface
extension_model
failure_model
threading_model
```
