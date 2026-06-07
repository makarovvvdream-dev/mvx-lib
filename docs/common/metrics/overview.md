# Metrics

```{contents} Contents:
:depth: 1
:local:
```

`MVX Metrics` is a lightweight structured metrics layer for Python code.

It exists because there is a practical gap between hand-written counters and full monitoring platforms.

Simple counters are easy to start with, but they often push metric logic directly into production code. A component
begins to know which counters exist, when each counter should be incremented, which outcomes should be tracked, and how
operation facts should be translated into measurements. As the code grows, observability logic starts to mix with domain
logic.

At the other end, full monitoring stacks provide powerful collection, export, storage, query, alerting, and
visualization capabilities. They are often the right final destination for operational metrics, but they are usually too
heavy to be the required foundation inside reusable library code. They bring backend choices, infrastructure decisions,
exporters, collectors, naming conventions, deployment concerns, and operational dependencies.

`MVX Metrics` is designed for the middle ground.

It should be small at the point of use: pass a metrics recorder to a component, emit metric events, register metrics,
and read snapshots. Internally, however, it is built around a formal event and aggregation model rather than direct
counter updates.

## Core idea

In `MVX Metrics`, production code does not update raw counters directly. Instead, it emits structured metric events.

The event describes what happened.

The metric decides whether that event matters and how it should change quantitative state.

This separates two responsibilities that are often mixed together:

* production code knows the domain fact;
* metrics know how domain facts become measurements.

For example, a transport component may report that a read operation completed successfully and returned a certain number
of bytes. One metric may count read attempts by outcome. Another metric may aggregate received bytes. A future metric
may calculate throughput or update a health snapshot.

The component does not need to know all of these interpretations. It emits the fact once. Metrics interpret it
independently.

This is the central idea of `MVX Metrics`: **production code emits structured facts, while metrics transform those facts
into quantitative dimensions**.

## Events, metrics, dimensions, and snapshots

`MVX Metrics` is built around four related concepts.

### Metric events

A metric event is a structured description of something that happened.

It may describe:

* an operation attempt;
* an operation outcome;
* a timeout;
* a cancellation;
* a refusal reason;
* a remote disconnect;
* the number of bytes sent or received;
* any other measurable domain fact.

The event is not a counter by itself. It is an input fact.

### Metrics

A metric is an aggregate that consumes events.

Each metric decides:

* which event types it accepts;
* which events it ignores;
* which internal dimensions should be updated;
* how its current state should be exposed.

This means a metric is not limited to a single numeric value. A metric may represent a group of related measurements.

For example, an operation-attempt metric may expose dimensions such as:

* total attempts;
* successful attempts;
* failed attempts;
* cancelled attempts;
* timeout attempts;
* refused attempts by reason.

The event-producing component does not update those dimensions directly. It only emits the domain event. The metric owns
the interpretation.

### Dimensions

Dimensions are the quantitative parts of a metric aggregate.

They are the values that change when accepted events are handled.

A simple metric may have one dimension, such as `total`.

A richer metric may have several dimensions, such as `success_total`, `failure_total`, `timeout_total`, and
`cancelled_total`.

This allows one metric to describe the shape of a domain operation rather than only a single number.

### Snapshots

A snapshot is a stable view of the current metric state.

Snapshots are the boundary where aggregated runtime state becomes visible to the outside world.

They may be used by:

* tests;
* diagnostics;
* health checks;
* dashboards;
* custom exporters;
* future monitoring adapters;
* operational inspection tools.

The snapshot is also one of the main growth points of the package. The current implementation can start with simple
in-memory state, while future integrations can decide how to expose, transform, export, or compare snapshots without
changing production code.

## Domain metrics and infrastructure

`MVX Metrics` deliberately separates domain-specific metric meaning from generic metric infrastructure.

Domain metric classes should live close to the component that owns the domain.

A TCP transport component, for example, knows which outcomes matter for `open`, `close`, `start_tls`, `read`, `write`,
and `drain`. It knows that a TLS error is different from a timeout. It knows that a refused crypto codec attachment is
different from a successful reconfiguration.

This meaning belongs near the transport component.

The recorder and runtime infrastructure should not need to know any of that.

Their job is generic:

* accept metric events;
* deliver events to registered metrics;
* let metrics update their own state;
* expose snapshots;
* provide lifecycle and delivery boundaries;
* support future adapters and exporters.

This separation keeps both sides clean.

Production components can define meaningful domain events and metrics without knowing how metric infrastructure is
implemented.

Metric infrastructure can collect, aggregate, and deliver events without knowing business or protocol semantics.

## Backend-neutral by design

`MVX Metrics` is not tied to a specific monitoring backend.

Production code should not have to know whether metrics will later be inspected in memory, exported to Prometheus,
forwarded through OpenTelemetry, sent to StatsD, written to a custom collector, or displayed in an internal diagnostic
panel.

The event-producing code should remain the same.

Changing the final observability infrastructure should be possible by changing recorders, adapters, exporters, or
snapshot consumers rather than changing the production component.

This is especially important for reusable libraries. A library should not force every application to use the same
monitoring stack. It should provide a clean observability boundary and let the application decide what happens beyond
that boundary.

## Lightweight production path

Metric emission should not make business operations heavier.

A production component should be able to emit a metric event quickly and continue its work. It should not synchronously
perform network delivery, storage, serialization for a remote backend, dashboard updates, or collector communication
inside the critical path of the operation.

`MVX Metrics` is designed around this expectation.

At the production boundary, the component hands off a structured event through a small recorder contract.

Aggregation, delivery, hooks, runtime ownership, and future exporting belong behind that boundary.

This keeps instrumentation practical for hot paths, such as network reads, writes, drains, protocol operations, or
request handlers.

## Sync, async, and thread boundaries

Production code may be synchronous or asynchronous.

It may run in the main thread, in worker threads, inside an event loop, inside callbacks, or inside reusable library
components that do not own the application runtime.

Metric infrastructure should not force all of that code into one execution model.

`MVX Metrics` separates event emission from metric processing so that production code can use a small recorder-facing
contract while the recorder and runtime manage the processing side.

This is why the package includes both an asyncio recorder and a metrics runtime.

The recorder provides event processing for asyncio-based environments.

The runtime provides a higher-level lifecycle boundary for applications or libraries that need metrics processing
without directly owning or exposing the event loop used by the recorder.

## MetricsRuntime as the practical entry point

`MetricsRuntime` exists to make metrics usable without forcing application code to manage all runtime details manually.

Without a runtime facade, users would have to answer several questions themselves:

* where should the metrics event loop live?
* who starts it?
* who stops it?
* how are recorders created?
* how are recorders shut down?
* how does synchronous code hand events to asynchronous metric processing?
* how are thread and lifecycle boundaries controlled?

`MetricsRuntime` provides a practical answer to those questions.

It owns the runtime side of metrics processing and exposes a simpler surface for creating and managing recorders.

This keeps metrics usable in regular applications and reusable libraries, not only in codebases that already have a
carefully managed asyncio observability runtime.

## Recorder hooks as a growth path

A recorder is more than a queue.

After an event is delivered to a metric and the metric accepts it, the recorder has a natural post-aggregation point:
the metric has changed.

This point is important because it is where future behavior can be attached without changing event-producing code.

Possible uses include:

* pushing updated snapshots;
* notifying observers;
* feeding dashboards;
* exporting changed metric state;
* collecting test probes;
* implementing threshold checks;
* integrating with external monitoring adapters.

This is one of the main growth paths of `MVX Metrics`.

The core package does not need to include every exporter from the beginning. It only needs to define where such behavior
belongs.

## In-memory first, adapters later

`MVX Metrics` starts with in-memory aggregation.

This is intentional.

The first responsibility of the package is to define the internal metric model and the boundary between production code
and observability infrastructure.

External adapters should be added because there is a real integration need, not because the package wants to collect
backend names.

The important part is that the core design already defines where future integrations fit:

* metric events are the input boundary;
* metrics aggregate domain facts into dimensions;
* snapshots expose current state;
* recorder hooks observe accepted metric changes;
* custom recorders can change delivery behavior;
* runtime-managed processing can isolate metric work from production code.

This keeps the core small while still leaving a clear path toward Prometheus, OpenTelemetry, StatsD, custom dashboards,
test probes, or other systems when they become necessary.

## Relationship with MVX Logger

`MVX Metrics` and `MVX Logger` are complementary.

They do not solve the same problem.

`MVX Logger` describes what happened as structured diagnostic events. It is useful when a human or diagnostic system
needs to inspect operation context, arguments, results, errors, source metadata, or payload details.

`MVX Metrics` measures how often things happen, with which outcomes, and how runtime state changes over time. It is
useful when a system needs counters, dimensions, snapshots, trends, health checks, or aggregated operational state.

A component may use both.

For example, a transport operation may log a failed read with structured error details and also emit a metric event that
increments read failure counters. The log explains the specific failure. The metric shows how often that class of
failure happens.

The two layers should remain independent:

* logging controls diagnostic event visibility and payload depth;
* metrics control quantitative aggregation and snapshots;
* neither layer should force the other to use a specific backend.

## What MVX Metrics is not

* `MVX Metrics` is not a replacement for Prometheus, OpenTelemetry, StatsD, or a monitoring dashboard.
* `MVX Metrics` is not a storage engine for time-series data.
* `MVX Metrics` is not an alerting system.
* `MVX Metrics` is not a visualization layer.
* `MVX Metrics` is not a collection of hand-written global counters.

`MVX Metrics` is a structured metric event and aggregation layer for regular Python projects. Its purpose is to
standardize how production code emits measurable domain facts, how metrics interpret those facts, how aggregated
dimensions are exposed through snapshots, and how delivery or export infrastructure can evolve without changing
event-producing code.

```{toctree}
:caption: What to read next
:maxdepth: 1

getting_started/index
architecture/overview
advanced_usage/index
api/index
```

