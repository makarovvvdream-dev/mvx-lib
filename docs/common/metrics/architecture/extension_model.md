# Extension model

```{contents} Contents:
:depth: 1
:local:
```

`MVX Metrics` is designed to grow through explicit extension points.

The core package does not need to know the final monitoring platform.

It provides stable places where new behavior can be attached without changing production code.

The main extension points are:

* custom metrics;
* custom recorders;
* recorder post-change hooks;
* snapshot consumers;
* external adapters;
* runtime integration.

## Extension boundaries

Extensions are attached around the metrics pipeline, not inside production methods.

```text
production code
   |
   v
metric events
   |
   v
recorder
   |
   v
metrics
   |
   v
snapshots
   |
   v
external consumers
```

The production component keeps emitting the same events.

Extensions decide what to do with those events or with the resulting metric state.

## Custom metrics

The most direct extension point is a new metric.

A metric can be added when existing events already carry enough information for a new measurement.

For example, one event can support several metrics:

```text
DocumentSaveAttemptMetricEvent
   |
   +--> DocumentSaveAttemptsMetric
   |
   +--> DocumentSaveAverageDurationMetric
```

The production method still emits one event.

The new metric adds another interpretation of that event.

This extension point is local to the domain package because metric meaning belongs near the domain code.

## Extending event payload

Sometimes a new metric needs data that existing events do not carry yet.

In that case, the event payload can be extended deliberately.

For example:

```python
@dataclass(frozen=True, slots=True)
class DocumentSaveAttemptMetricEvent(MetricEvent):
    outcome: DocumentSaveAttemptOutcome
    duration_ms: float

    @property
    def event_type(self) -> str:
        return "document_storage.save.attempt"
```

Now one metric can count outcomes, while another metric can calculate average duration.

```text
same event
   |
   +--> outcome counters
   |
   +--> duration aggregation
```

This is still an extension at the metric-event boundary.

The event remains a domain fact.

Metrics decide how to use its payload.

## Custom recorders

A custom recorder can change how events are processed or delivered.

The default recorder keeps events in an in-memory processing pipeline and dispatches them to registered metrics.

A custom recorder may add different behavior around that pipeline, for example:

* forwarding accepted events;
* persisting metric state;
* connecting to a custom observability system;
* changing buffering or delivery behavior;
* adding additional diagnostics around processing.

The important boundary is the recorder contract.

Production code should still talk to a recorder through the same narrow surface:

```python
self._metrics_recorder.register_event(event=event)
```

That keeps production code independent from recorder implementation details.

## Post-change hook

A recorder has a natural extension point after a metric accepts an event.

The sequence is:

```text
event dispatched to metric
   |
   v
metric accepts event
   |
   v
metric state changes
   |
   v
recorder post-change hook
```

This hook runs after the metric has already interpreted the event and updated its state.

It is useful when an extension needs to react to changed metric state rather than to raw event submission.

Possible uses include:

* notifying observers;
* feeding a test probe;
* pushing changed metric state;
* integrating with an external adapter;
* triggering local diagnostics.

This hook belongs on the recorder side, not in production code.

## Snapshot consumers

Snapshots are another extension point.

They expose current metric state in a structured form.

A consumer can read snapshots without knowing metric internals:

```text
recorder.get_metric_snapshots()
   |
   v
snapshot consumer
```

Snapshot consumers can be simple or complex.

Examples:

* test assertions;
* local diagnostic pages;
* health checks;
* command-line inspection;
* dashboard data sources;
* exporter input.

This extension point is useful because it works with already aggregated state.

The consumer does not need to replay events.

## External adapters

An external adapter can connect `MVX Metrics` to a monitoring platform.

For example:

```text
MVX Metrics snapshots
   |
   v
adapter
   |
   v
Prometheus / OpenTelemetry / StatsD / custom backend
```

The adapter is outside the production component.

The production component continues to emit metric events.

The metrics continue to aggregate those events.

The adapter consumes the resulting state and publishes it elsewhere.

This is the main reason the package can support observability early without forcing the final monitoring platform too
early.

## Runtime integration

`MetricsRuntime` gives applications a managed environment for recorders.

That environment can also be part of extension design.

For example, an application may:

* create recorders for many measured entities;
* use a custom recorder type in a future integration;
* manage recorder lifecycle centrally;
* attach monitoring infrastructure around runtime-owned recorders.

The runtime is not itself a monitoring backend.

It is the place where runtime-created recorders live and process events.

Extensions that need to manage many recorders can use the runtime as the registry and lifecycle boundary.

## What should not be extended through production code

Production methods should not become the place where monitoring integration grows.

Avoid this shape:

```text
production method
   |
   +--> update local counter
   |
   +--> send Prometheus metric
   |
   +--> send OpenTelemetry metric
   |
   +--> update dashboard state
```

That makes production code responsible for observability infrastructure.

The intended shape is:

```text
production method
   |
   v
emit metric event
   |
   v
metrics infrastructure
   |
   v
optional extensions
```

Extensions should attach after the recorder or snapshot boundaries.

## Choosing the right extension point

Different needs belong to different places.

```text
new measurement logic
   |
   v
custom Metric

new data needed by measurements
   |
   v
extend MetricEvent payload

reaction after metric changed
   |
   v
recorder post-change hook

read current state
   |
   v
snapshot consumer

publish to external platform
   |
   v
adapter / exporter

change processing behavior
   |
   v
custom recorder

manage many recorders
   |
   v
MetricsRuntime
```

This keeps extensions focused.

## Summary

`MVX Metrics` can grow without changing production code because extension points are placed around clear boundaries.

Metrics extend measurement logic.

Recorders extend processing behavior.

Hooks react after metric state changes.

Snapshots expose current state.

Adapters connect that state to external platforms.

The production component remains responsible for one thing: emitting structured metric events when meaningful domain
events happen.
