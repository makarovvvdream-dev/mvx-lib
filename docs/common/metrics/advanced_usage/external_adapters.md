# External adapters

```{contents} Contents:
:depth: 1
:local:
```

`MVX Metrics` does not require a final monitoring platform from the beginning.

Production code can emit metric events now.

Metrics can aggregate those events now.

Recorders can expose snapshots now.

An external adapter can be added later, when the project decides where metric state should go.

## What an external adapter is

An external adapter is code that connects `MVX Metrics` to another observability system.

For example:

```text
MVX Metrics
   |
   v
adapter
   |
   v
Prometheus / OpenTelemetry / StatsD / custom backend
```

The adapter lives at the edge of the metrics system.

It should not live inside production methods.

Production code should not know whether metric state is later sent to Prometheus, OpenTelemetry, StatsD, a dashboard, a
database, or a custom monitoring service.

## Why adapters belong at the edge

The core pipeline is already enough to make code observable:

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

An adapter adds one more step:

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
snapshots or hook
   |
   v
external adapter
   |
   v
external monitoring platform
```

The important part is that the production side does not change.

The same production component emits the same metric events.

The adapter is added around recorder, snapshot, or hook boundaries.

## Two integration styles

There are two common ways to connect external systems.

```text
snapshot-based adapter
hook-based adapter
```

They solve different problems.

## Snapshot-based adapter

A snapshot-based adapter reads current metric state from recorders.

```python
snapshots = recorder.get_metric_snapshots()
```

Then it transforms those snapshots into the format expected by the external system.

Conceptually:

```text
recorder.get_metric_snapshots()
   |
   v
snapshot adapter
   |
   v
external platform format
```

This style is useful when the external system can consume current state periodically or on demand.

Typical uses:

* diagnostics pages;
* health endpoints;
* pull-style exporters;
* periodic reporting;
* simple dashboards;
* tests that check adapter output.

A snapshot-based adapter works with already aggregated state.

It does not need to receive every raw metric event.

It does not need to replay history.

## Hook-based adapter

A hook-based adapter reacts when metric state changes.

This is usually done by extending recorder behavior through `_on_metric_changed()`.

The hook runs after a metric accepts an event and updates its state.

```text
metric.handle_event(event)
   |
   v
metric state changed
   |
   v
_on_metric_changed(metric=metric, event=event)
   |
   v
adapter logic
```

This style is useful when an integration should react to changes as they happen.

Typical uses:

* notifying observers;
* pushing updated metric state;
* feeding a live diagnostic view;
* triggering an export operation;
* connecting a custom backend.

The custom recorder example uses this style.

It opens a publisher on recorder startup, publishes records after metric changes, and closes the publisher on recorder
shutdown.

## Choosing between snapshot and hook adapters

Use a snapshot-based adapter when the external side can read or receive current state.

```text
current state is enough
   |
   v
use snapshots
```

Use a hook-based adapter when the integration must react after each accepted metric event.

```text
react after metric changed
   |
   v
use _on_metric_changed()
```

Both styles keep production code unchanged.

The difference is where the adapter attaches.

```text
snapshot adapter
   |
   v
attaches to the read surface

hook adapter
   |
   v
attaches to recorder processing
```

## Adapter responsibilities

An adapter owns the external integration details.

Those details may include:

* external metric naming;
* unit conversion;
* labels or tags;
* batching;
* retries;
* authentication;
* network delivery;
* serialization;
* endpoint exposure;
* backend-specific error handling.

These concerns should not leak into production methods.

They also should not normally belong inside metric classes.

Metrics should aggregate state.

Adapters should translate or deliver state.

## What MVX Metrics does not decide

`MVX Metrics` does not choose the final monitoring backend.

It does not define a Prometheus exposition format.

It does not define an OpenTelemetry exporter.

It does not define a StatsD sender.

It does not require a dashboard, collector, agent, or storage backend.

This is intentional.

The package provides the internal metrics foundation:

```text
events
metrics
recorders
snapshots
hooks
runtime
```

External platform integration can be added later at the adapter layer.

## Production code should stay unchanged

The key rule is:

```text
do not put adapter logic into production code
```

Avoid this shape:

```text
business method
   |
   +--> emit MVX metric event
   |
   +--> send Prometheus metric
   |
   +--> send OpenTelemetry metric
   |
   +--> update dashboard
```

Use this shape instead:

```text
business method
   |
   v
emit MVX metric event
   |
   v
metrics infrastructure
   |
   v
adapter at the edge
```

This keeps business code stable.

It also makes it possible to change the monitoring backend later.

## Why this pattern is useful

Many projects need observability early.

But the final monitoring stack may not be chosen at the start.

With `MVX Metrics`, the project can still add observability from the beginning:

```text
add metric events now
aggregate metrics now
test snapshots now
inspect state now
choose external platform later
add adapter later
```

This is useful for reusable libraries and long-lived applications.

The code becomes observable and testable immediately, without being tied to a premature backend decision.

## Adapter input options

An adapter can use several inputs depending on its design.

### Recorder snapshots

```python
snapshots = recorder.get_metric_snapshots()
```

Use this when the adapter wants current state.

### Runtime recorder registry

If the application owns many recorders, adapter code can be wired around the runtime-level recorder collection.

This lets an integration collect state from several measured scopes.

The exact registry traversal belongs to application integration code.

### Metric changed hook

```python
async def _on_metric_changed(
        self,
        *,
        metric: Metric,
        event: MetricEvent,
) -> None:
    ...
```

Use this when the adapter should react after metric state changes.

### Custom recorder

Use a custom recorder when adapter behavior belongs naturally to recorder lifecycle.

For example, a recorder may open an external publisher on startup and close it on shutdown.

## Adapter output

Adapter output depends on the target system.

The adapter may produce:

* flat records;
* JSON payloads;
* HTTP requests;
* Prometheus samples;
* OpenTelemetry measurements;
* StatsD packets;
* database rows;
* in-memory dashboard updates.

This output is outside the core metrics package.

The core should not need to know which form is used.

## Failure handling

External adapters introduce external failure modes.

For example:

* network timeout;
* backend unavailable;
* authentication failure;
* serialization error;
* rate limit;
* retry exhaustion.

Those failures should be handled at the adapter boundary.

They should not automatically become business operation failures.

This follows the same design principle as recorder integration: observability should not accidentally own business
behavior.

A strict application may choose to surface adapter failures.

A tolerant application may log them and continue.

The adapter owns that policy.

## Summary

External adapters connect `MVX Metrics` to monitoring platforms or custom observability systems.

They should attach at the edge: snapshots, hooks, custom recorders, or runtime-level integration.

They should not be embedded in production methods.

`MVX Metrics` lets code become observable before the final monitoring platform is chosen.

When the platform is chosen, an adapter can publish the same metric state without changing production code.
