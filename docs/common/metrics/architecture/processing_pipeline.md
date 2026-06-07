# Processing pipeline

```{contents} Contents:
:depth: 1
:local:
```

This page explains how a metric event moves through `MVX Metrics` at runtime.

It focuses on the pipeline itself:

* where the event enters the metrics system;
* where processing is isolated from production code;
* how the recorder dispatches the event;
* how metrics accept or ignore the event;
* where metric state becomes visible.

## Overview

At runtime, the normal path is:

```text
production code
   |
   v
create MetricEvent
   |
   v
recorder.register_event(event)
   |
   v
recorder internal buffer
   |
   v
recorder processing side
   |
   v
metric.handle_event(event)
   |
   v
metric state
   |
   v
metric snapshot
```

The recorder coordinates this pipeline.

The production component creates the event and hands it to the recorder.

The metric decides whether the event changes its state.

The snapshot exposes the current state later.

## Step 1. Production code creates an event

The pipeline starts inside production code.

For example, a business method may create an event after a successful operation:

```python
DocumentSaveAttemptMetricEvent(
    outcome=DocumentSaveAttemptOutcome.SUCCESS,
)
```

or after a failed operation:

```python
DocumentSaveAttemptMetricEvent(
    outcome=DocumentSaveAttemptOutcome.FAILURE,
)
```

At this point, nothing has been aggregated yet.

The event is only an input object created by production code.

## Step 2. Production code hands the event to the recorder

The event enters `MVX Metrics` through the recorder:

```python
self._metrics_recorder.register_event(event=event)
```

This call is the production boundary.

From the production method's point of view, this is a short synchronous handoff:

```text
production method
   |
   v
recorder.register_event(event)
```

After the handoff, the production method can continue its own work.

The recorder takes responsibility for moving the event into metrics-side processing.

## Step 3. The recorder accepts the event

The recorder receives the event and places it into its internal processing buffer.

```text
recorder.register_event(event)
   |
   v
recorder internal buffer
```

This separates event emission from event processing.

The production method does not call metrics directly.

It does not run the dispatch loop.

It only hands the event to the recorder.

## Step 4. Processing moves to the recorder side

After the event is accepted, processing continues on the recorder side.

When the recorder is managed by `MetricsRuntime`, this side runs in the runtime-owned thread and its `asyncio` event
loop.

```text
recorder internal buffer
   |
   v
runtime-owned thread / asyncio event loop
   |
   v
recorder processing side
```

This is the main execution separation in the pipeline.

Production code may be synchronous or asynchronous.

Recorder processing still happens in the metrics-processing environment managed by the runtime.

## Step 5. The recorder dispatches the event to metrics

The recorder owns registered metric instances.

When it processes an event, it gives that event to the metrics it knows about.

```text
recorder processing side
   |
   v
metric.handle_event(event)
```

If several metrics are registered, each metric gets a chance to inspect the event.

```text
recorder
   |
   v
metric A.handle_event(event)
   |
   v
metric B.handle_event(event)
   |
   v
metric C.handle_event(event)
```

The recorder does not decide which dimensions should change.

It only dispatches the event.

## Step 6. A metric accepts or ignores the event

A metric decides whether the event is relevant.

The usual pattern is:

```python
def handle_event(self, event: MetricEvent) -> bool:
    if not isinstance(event, DocumentSaveAttemptMetricEvent):
        return False

    ...
    return True
```

If the metric returns `False`, the event is ignored by that metric.

```text
metric.handle_event(event)
   |
   v
False
   |
   v
metric state unchanged
```

If the metric returns `True`, the metric has accepted the event.

```text
metric.handle_event(event)
   |
   v
True
   |
   v
metric state updated
```

This return value is important for the recorder.

It tells the recorder whether the metric actually handled the event.

## Step 7. Accepted events update metric state

When a metric accepts an event, it updates its own internal state.

For example:

```python
self._total += 1

if event.outcome is DocumentSaveAttemptOutcome.SUCCESS:
    self._success_total += 1

elif event.outcome is DocumentSaveAttemptOutcome.FAILURE:
    self._failure_total += 1
```

Only the metric mutates its state.

The production component does not mutate metric state.

The recorder does not decide how metric state changes.

## Step 8. The recorder may run post-change logic

After a metric accepts an event, the recorder has a post-change point.

At that point, the metric has already updated its state.

Architecturally, this is where recorder-level extensions can react to accepted events.

```text
metric.handle_event(event)
   |
   v
metric state updated
   |
   v
recorder post-change hook
```

The base in-memory path can keep this simple.

A custom recorder or subclass can use this point for integration work, such as notifying observers or forwarding updated
state.

## Step 9. Snapshots expose current state

Snapshots are not produced during every production call unless application code asks for them.

They are read later:

```python
snapshots = recorder.get_metric_snapshots()
```

The recorder asks registered metrics for their current snapshots.

```text
metric state
   |
   v
metric.snapshot()
   |
   v
recorder.get_metric_snapshots()
```

The snapshot exposes current aggregated state.

It does not replay the pipeline.

It does not contain the event history.

## Full pipeline

The complete runtime path is:

```text
production code
   |
   v
create MetricEvent
   |
   v
recorder.register_event(event)
   |
   v
recorder internal buffer
   |
   v
recorder processing side
   |
   v
metric.handle_event(event)
   |
   +--> False
   |       |
   |       v
   |    metric ignored event
   |
   +--> True
           |
           v
        metric state updated
           |
           v
        recorder post-change hook
           |
           v
        later: metric snapshot
```

The important point is that event creation, event handoff, event processing, state mutation, and state inspection are
separate stages.

## Pipeline ownership

Each stage has one owner.

```text
production component
   |
   v
creates MetricEvent

recorder
   |
   v
accepts and processes event

metric
   |
   v
accepts or ignores event

metric
   |
   v
updates internal state

recorder
   |
   v
collects snapshots
```

This ownership is what keeps the system replaceable.

A production component can keep emitting the same events while metrics, recorders, hooks, or snapshot consumers evolve
around it.

## Pipeline with MetricsRuntime

When recorders are created by `MetricsRuntime`, the pipeline has an additional execution boundary.

```text
production thread or task
   |
   v
recorder.register_event(event)
   |
   v
recorder internal buffer
   |
   v
runtime-owned thread
   |
   v
runtime-owned asyncio event loop
   |
   v
recorder processing side
   |
   v
metric.handle_event(event)
```

The recorder remains the production boundary.

`MetricsRuntime` provides the execution environment behind that boundary.

This allows synchronous and asynchronous production code to use the same recorder-facing contract.

## What this pipeline does not include

This pipeline does not include an external monitoring backend.

It ends at metric state and snapshots.

External integration can be added later by using recorder hooks, custom recorders, snapshot consumers, or adapters.

That integration happens after the production boundary.

The production method does not need to change.

