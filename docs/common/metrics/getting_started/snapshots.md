# Snapshots

```{contents} Contents:
:depth: 1
:local:
```

A snapshot is the public view of current metric state.

Metrics keep their own internal state while they process events. A snapshot is the way to expose that state to the
outside world in a stable, structured form.

In the first example, the recorder returns snapshots like this:

```python
snapshots = recorder.get_metric_snapshots()
```

The result is a mapping by metric name:

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

## Metric snapshot

Each metric decides how to expose its own state.

For example:

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

The metric owns the internal counters.

The snapshot exposes a read-friendly representation of those counters.

The internal state can stay private. External code should use the snapshot instead of reading metric internals.

## Recorder snapshots

A recorder owns registered metric instances.

When application code calls:

```python
snapshots = recorder.get_metric_snapshots()
```

the recorder asks its metrics for their snapshots and returns them by metric name.

The shape is:

```text
metric name
    -> metric snapshot
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

If a recorder has several metrics, the result contains several metric snapshots.

## Scope

Snapshots are scoped to the recorder.

If a recorder represents one component, the snapshots describe that component.

If a recorder represents one connection, the snapshots describe that connection.

If an application uses one recorder per measured entity, each recorder exposes snapshots for its own entity.

This keeps snapshot interpretation simple:

```text
recorder scope
    -> registered metrics
    -> current snapshots
```

## What snapshots are useful for

Snapshots are useful when code needs the current aggregated metric state.

Common uses include:

* tests;
* diagnostics;
* health checks;
* local inspection;
* dashboards;
* exporters;
* custom monitoring adapters.

For example, tests can assert metric behavior directly:

```python
assert snapshots == {
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

This is especially useful because the test does not need to inspect private metric fields.

## Snapshots are current state

A snapshot describes current aggregated state at the moment it is requested.

It is not an event stream.

It is not a time-series database.

It is not a history of all metric changes.

If a metric processed three events, the snapshot shows the resulting state after those events.

```text
SUCCESS
SUCCESS
FAILURE

snapshot:
    total = 3
    success_total = 2
    failure_total = 1
```

The event history is not stored in the snapshot.

## Snapshots are not exporters

A snapshot is not a Prometheus exporter, OpenTelemetry exporter, dashboard, or storage backend.

It is the structured state that such integrations can use.

This distinction keeps the core package small:

```text
metric
    -> owns state

snapshot
    -> exposes state

adapter/exporter/dashboard
    -> may consume state later
```

The core metrics layer does not need to know where the snapshot will be used.

## Snapshot shape

The examples use this simple shape:

```python
{
    "name": "...",
    "dimensions": {
        ...
    },
}
```

`name` identifies the metric.

`dimensions` contains the values exposed by that metric.

A simple metric may expose one value:

```python
{
    "name": "tcp_stream.bytes.received",
    "dimensions": {
        "total": 4096,
    },
}
```

A richer metric may expose several related values:

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

The exact dimensions belong to the metric.

## Keep snapshots stable

Snapshot structure should be stable enough for tests, diagnostics, and integration code.

Changing metric internals should not force external code to change.

Changing snapshot names or dimension names is more visible, because external code may rely on them.

For this reason, treat metric names and snapshot dimensions as part of the metric's public surface.

## Integration point for external monitoring

Snapshots are one of the main integration points between `MVX Metrics` and external monitoring systems.

`MVX Metrics` is not a closed black box.

It does not force a final monitoring platform from the beginning, but it also does not postpone observability until
later.

This is useful when a project needs observability from the start, while the final monitoring stack is not selected yet.

The production code can already emit metric events.

Metrics can already aggregate those events.

Recorders can already expose snapshots.

Tests and diagnostics can already inspect the resulting state.

Later, when the project chooses a monitoring platform, an adapter can consume snapshots and publish the same metric
state to that platform.

The important part is that production code does not have to change.

```text
production code
    -> metric events
    -> metrics
    -> snapshots
    -> future adapter
    -> external monitoring platform
```

This is a practical observability pattern.

Start with a small internal metrics layer.

Make the code observable and testable immediately.

Choose the external platform when there is a real need.

Add the required adapter at the edge.

Keep production code unchanged.

## Summary

A snapshot is the public view of current metric state.

A metric creates its own snapshot.

A recorder returns snapshots for metrics registered inside it.

Snapshots are scoped to the recorder, useful for tests and diagnostics, and suitable as a future integration point for
dashboards, exporters, and monitoring adapters.

They expose current state, not event history.
