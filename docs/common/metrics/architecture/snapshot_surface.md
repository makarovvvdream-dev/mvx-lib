# Snapshot surface

```{contents} Contents:
:depth: 1
:local:
```

Snapshots are the read surface of `MVX Metrics`.

They expose current metric state without exposing metric internals.

A metric owns its state.

A recorder owns metric instances.

A snapshot is the structured boundary where that state becomes visible to application code, tests, diagnostics, or
integration adapters.

## What the snapshot surface separates

The snapshot surface separates three things:

```text
metric internals
   |
   v
metric snapshot
   |
   v
external consumers
```

Metric internals remain private.

External code reads snapshots.

This allows a metric to change its internal implementation without forcing callers to read private fields.

## Metric-level snapshot

Each metric defines its own snapshot.

```python
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

The metric decides which values are part of its public state.

The recorder does not build those values.

The runtime does not build those values.

The metric owns both:

* measured state;
* public snapshot shape for that state.

## Recorder-level snapshot collection

A recorder collects snapshots from the metrics registered inside it.

```python
snapshots = recorder.get_metric_snapshots()
```

Architecturally, the recorder-level snapshot surface is:

```text
recorder
   |
   v
registered metric
   |
   v
metric.snapshot()
   |
   v
mapping by metric name
```

The recorder returns a mapping:

```text
metric name
   |
   v
metric snapshot
```

For example:

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
}
```

The recorder coordinates access.

The metric owns the content.

## Scope of a recorder snapshot

A recorder snapshot is scoped to that recorder.

If the recorder represents one component instance, the snapshot describes that component instance.

If the recorder represents one connection, the snapshot describes that connection.

If an application uses one recorder per measured entity, each recorder exposes the current state for one measured
entity.

```text
recorder: connection-001
   |
   v
snapshots for connection-001

recorder: connection-002
   |
   v
snapshots for connection-002
```

The snapshot surface follows the recorder ownership model.

It is not automatically global.

## Snapshot access and recorder ownership

A recorder is an asynchronous processing component.

Its metrics live inside the recorder's processing model.

Snapshot access must respect recorder ownership.

From the caller's point of view:

```python
snapshots = recorder.get_metric_snapshots()
```

is a simple inspection call.

Internally, the recorder is responsible for collecting snapshots consistently from its registered metrics.

If the recorder is owned by `MetricsRuntime`, snapshot access is coordinated with the recorder's runtime-owned event
loop.

The caller does not need to manage that coordination directly.

## Current state, not history

A snapshot exposes current aggregated state.

It does not contain the event stream that produced that state.

```text
events processed:
   SUCCESS
   SUCCESS
   FAILURE

snapshot:
   total = 3
   success_total = 2
   failure_total = 1
```

The event history is not part of the snapshot surface.

If a project needs time-series storage, history retention, or long-term querying, that belongs to an external monitoring
or storage layer.

## Snapshot is not export

A snapshot is not an exporter.

It does not decide where metric state should go.

It only exposes state in a structured form.

```text
metric state
   |
   v
snapshot
   |
   v
optional consumer or adapter
```

Possible consumers include:

* tests;
* diagnostic tools;
* health checks;
* dashboards;
* custom exporters;
* external monitoring adapters.

This keeps the core metrics package independent from the final monitoring platform.

## Snapshot stability

Snapshot names and dimension names are part of the observable surface of a metric.

Internal metric fields may change.

Snapshot structure should change more carefully.

For example, external code may rely on:

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

Changing private fields such as `_total` affects only the metric implementation.

Changing public dimension names such as `success_total` affects tests, diagnostics, exporters, and adapters.

For this reason, snapshot shape should be treated as a public contract of the metric.

## Snapshot consumers

Snapshot consumers should depend on the public snapshot shape, not on metric internals.

Good:

```text
consumer
   |
   v
recorder.get_metric_snapshots()
   |
   v
snapshot data
```

Bad:

```text
consumer
   |
   v
metric._private_field
```

This boundary is especially important for adapters.

An adapter should not need to know how a metric stores its internal counters.

It should consume the metric snapshot.

## Integration edge

The snapshot surface is one of the main edges where `MVX Metrics` can connect to external systems later.

The core pipeline can already work without a final monitoring platform:

```text
production code
   |
   v
metric events
   |
   v
metrics
   |
   v
snapshots
```

Later, an adapter can be added at the snapshot edge:

```text
snapshots
   |
   v
adapter
   |
   v
external monitoring platform
```

That lets production code remain unchanged.

The metric event model and aggregation logic are already in place.

Only the outer integration layer changes.

## Snapshot surface in the architecture

The snapshot surface sits after metric processing.

```text
MetricEvent
   |
   v
Recorder
   |
   v
Metric.handle_event(event)
   |
   v
Metric state
   |
   v
Metric.snapshot()
   |
   v
Recorder.get_metric_snapshots()
   |
   v
External consumer
```

This placement matters.

Snapshots are not part of event emission.

They are not part of event dispatch.

They are the read side of the current metric state.

## Summary

The snapshot surface is the read boundary of `MVX Metrics`.

Metrics create snapshots from their own state.

Recorders collect snapshots from registered metrics.

Consumers read snapshots instead of metric internals.

Snapshots expose current state, not event history.

They are also a natural integration edge for tests, diagnostics, dashboards, exporters, and future monitoring adapters.
