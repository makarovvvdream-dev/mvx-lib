# Recorder processing

```{contents} Contents:
:depth: 1
:local:
```

`AsyncioMetricsRecorder` is the default asynchronous recorder implementation in `MVX Metrics`.

It is not just a list of metrics.

It is a stateful processing component that:

* is bound to an `asyncio` event loop;
* owns registered metric instances;
* accepts metric events from production code;
* controls accepted-but-not-yet-processed events;
* runs a dispatcher task;
* sends events to registered metrics;
* calls a post-change hook when a metric accepts an event;
* exposes snapshots and metric inspection methods;
* has its own lifecycle and error surface.

Most application code does not have to manage this directly when recorders are created through `MetricsRuntime`.

This page explains what the recorder does internally.

## Event loop ownership

`AsyncioMetricsRecorder` is an asyncio-based component.

When it is created directly, it must be created in a thread where an `asyncio` event loop is already running.

The recorder stores that loop and uses it as its owning loop.

This is why direct recorder usage is an advanced usage topic.

When a recorder is created by `MetricsRuntime`, the runtime creates it inside its own managed event loop. In that case,
application code does not have to provide the loop manually.

```text
MetricsRuntime
   |
   v
runtime-owned thread
   |
   v
runtime-owned asyncio event loop
   |
   v
AsyncioMetricsRecorder
```

## Lifecycle

A recorder has an explicit lifecycle.

At a high level, the important states are:

```text
VIRGIN
STARTING
RUNNING
STOPPING
STOPPED
FAILURE
CANCELLED
```

The public lifecycle methods are:

```python
recorder.start()
recorder.stop()
```

Both methods return a wait handle.

The wait handle can be used from synchronous code:

```python
result = recorder.start().wait()
```

or awaited from asynchronous code:

```python
result = await recorder.start()
```

This allows the same lifecycle operation to be observed from both sync and async callers.

## Starting

Starting the recorder schedules startup on the recorder's owning event loop.

Startup does three important things:

1. runs the `_on_starting()` hook;
2. creates the dispatcher task;
3. moves the recorder to the running state.

The hook exists so subclasses can prepare backend resources before dispatching starts.

The base recorder does not need external resources, so the default hook does nothing.

```text
start()
   |
   v
_on_starting()
   |
   v
create dispatcher task
   |
   v
RUNNING
```

If startup fails, the recorder moves to failure state and stores the error.

## Stopping

Stopping the recorder schedules shutdown on the recorder's owning event loop.

Shutdown is best-effort.

Before stopping the dispatcher, the recorder tries to flush events that were already accepted or already scheduled for
enqueueing.

Then it cancels the dispatcher as part of normal shutdown and runs `_on_stopped()`.

```text
stop()
   |
   v
best-effort flush
   |
   v
cancel dispatcher
   |
   v
_on_stopped()
   |
   v
STOPPED
```

If stopping fails, the recorder moves to failure state and stores the error.

## Public handoff

Production code emits events through:

```python
recorder.register_event(event=event)
```

This is the production-side handoff.

The method validates the event, checks recorder state, applies the pending-event limit, and schedules the event into the
recorder queue.

From the caller's point of view, this is a short synchronous call.

The actual metric processing happens later on the recorder side.

```text
production method
   |
   v
register_event(event)
   |
   v
event accepted for recorder-side processing
```

## Lazy start

`register_event()` can be called while the recorder is still in `VIRGIN` state.

In that case, the recorder starts itself.

```text
VIRGIN recorder
   |
   v
register_event(event)
   |
   v
start()
   |
   v
event scheduled for processing
```

This makes simple recorder usage less fragile.

The recorder still has lifecycle rules, but first event emission can trigger startup when the recorder has not been
started yet.

## Metric registry

A recorder owns metric instances by metric name.

Metrics are registered with:

```python
recorder.register_metric(metric=metric)
```

The recorder stores the metric instance internally.

Later, when events are processed, the dispatcher gives events to those registered metrics.

The recorder does not interpret the metric name and does not understand domain meaning.

It only maintains the registry and coordinates dispatch.

## Internal queue and pending counter

The recorder uses an internal `asyncio.Queue` for event processing.

The queue itself is intentionally unbounded.

The accepted-event limit is controlled separately through a pending counter.

The pending counter includes events that have been accepted but not yet fully processed.

```text
register_event(event)
   |
   v
pending counter +1
   |
   v
event scheduled into queue
   |
   v
dispatcher processes event
   |
   v
pending counter -1
```

This distinction matters.

The configured queue limit is not a direct `asyncio.Queue(maxsize=...)`.

It limits the number of accepted pending events.

## Overflow policy

If the pending-event limit is reached, the recorder uses its overflow policy.

The built-in policies are:

```text
RAISE_ERROR
DROP
```

With `RAISE_ERROR`, the recorder raises a queue overflow error.

With `DROP`, the new event is ignored.

```text
pending counter below limit
   |
   v
accept event

pending counter at limit
   |
   +--> RAISE_ERROR -> raise overflow error
   |
   +--> DROP        -> drop event
```

This gives applications a choice.

They can make overflow visible, or they can keep the production path non-blocking and lossy under pressure.

## Dispatcher task

The dispatcher task is the consumer side of the recorder.

It waits for events in the queue and dispatches each event to registered metrics.

```text
queue.get()
   |
   v
dispatch event
   |
   v
queue.task_done()
```

For each event, the recorder calls its dispatch core.

The dispatch core iterates over registered metrics:

```text
event
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

Each metric decides whether to accept or ignore the event.

## Metric changed hook

When a metric accepts an event, `handle_event()` returns `True`.

After that, the recorder calls the post-change hook:

```text
metric.handle_event(event)
   |
   v
True
   |
   v
_on_metric_changed(metric=metric, event=event)
```

This hook is called only after the metric has accepted the event.

The default implementation does nothing.

Subclasses can override it to attach additional behavior after metric state changes.

Typical uses include:

* notifying observers;
* feeding a test probe;
* forwarding changed metric state;
* integrating with an external adapter.

The hook is after metric interpretation, not before it.

## Ignored events

If a metric returns `False`, the recorder treats that metric as unchanged for that event.

```text
metric.handle_event(event)
   |
   v
False
   |
   v
no metric-changed hook for this metric
```

The event may still be accepted by another metric.

This is why one event can be useful to several metrics, while unrelated metrics can ignore it.

## Dispatcher failure

If dispatching fails, the recorder records that failure on the metrics infrastructure side.

A dispatcher error can move the recorder into failure state.

Unexpected dispatcher cancellation outside normal stopping is also treated as a recorder error.

The important architectural point is that dispatching has its own failure boundary.

The recorder is not a passive object. It owns a running dispatcher task and must handle task failure.

## Logging

`AsyncioMetricsRecorder` can use `MVX Logger` when a `log_context` is provided.

Logging is used for two different purposes:

* operation lifecycle logging for selected public recorder methods;
* internal error logging for dispatcher and cleanup failures.

Several public methods are instrumented with `log_invocation`:

```text
asyncio_metrics_recorder.start
asyncio_metrics_recorder.stop
asyncio_metrics_recorder.register_metric
asyncio_metrics_recorder.get_metric_snapshots
asyncio_metrics_recorder.iter_metrics
```

These logs describe recorder API operations.

For example, `start()` and `stop()` include the current recorder state as context.

`register_metric()` also logs the metric name on invocation.

Snapshot and metric-iteration calls are recorder inspection operations.

The event emission method is intentionally different:

```python
recorder.register_event(event=event)
```

`register_event()` is the production hot-path handoff. It is not wrapped with `log_invocation`.

This avoids producing a log invocation event for every metric event submitted by production code.

Metric events are processed as metrics input, not as diagnostic log events.

Internal dispatcher errors are logged separately.

If event dispatching fails while processing the queue, the recorder logs:

```text
metrics_recorder.dispatch_error
```

If dispatcher failure requires cleanup and cleanup task creation fails, the recorder logs:

```text
metrics_recorder.cleanup.task_creation_error
```

If cleanup itself fails, the recorder logs:

```text
metrics_recorder.cleanup.failed
```

These events are infrastructure diagnostics.

They describe recorder processing failures, not business metric events.

For convenience, `MVX Metrics` also provides ready-to-use event policies for recorder logging.

The policy mode is controlled by `AsyncioMetricsRecorderLogPolicyMode`:

```text
SILENT
NORMAL
INSPECTION
```

The default helper functions are:

```python
asyncio_metrics_recorder_event_policy_config(...)
asyncio_metrics_recorder_event_policy(...)
```

`SILENT` disables recorder log events by default.

`NORMAL` enables the usual lifecycle and infrastructure events:

```text
asyncio_metrics_recorder.start
asyncio_metrics_recorder.stop
asyncio_metrics_recorder.register_metric
metrics_recorder.dispatch_error
metrics_recorder.cleanup.task_creation_error
metrics_recorder.cleanup.failed
```

`INSPECTION` includes all `NORMAL` events and also enables inspection operations:

```text
asyncio_metrics_recorder.get_metric_snapshots
asyncio_metrics_recorder.iter_metrics
```

This keeps recorder logging practical out of the box.

Applications do not have to build a custom event policy just to get a reasonable recorder logging profile. They can
start with `NORMAL`, switch to `INSPECTION` when they need more visibility, or use `SILENT` when recorder logging should
be suppressed.

So the logging model is:

```text
recorder public lifecycle/configuration operations
   |
   v
log_invocation events controlled by recorder event policy

metric event emission
   |
   v
no invocation logging on register_event()

dispatcher / cleanup failures
   |
   v
internal error log events
```

This keeps normal metric event submission lightweight while still making recorder lifecycle and infrastructure failures
observable.

## Cleanup after dispatcher failure

If the dispatcher fails outside normal stopping, the recorder schedules cleanup.

Cleanup runs the stopped hook and logs cleanup failures if a log context is configured.

This keeps backend cleanup separate from production event emission.

## Snapshot inspection

The recorder can expose snapshots for registered metrics:

```python
snapshots = recorder.get_metric_snapshots()
```

If called from the recorder's owning event loop, snapshots are collected directly.

If called from another thread, the recorder schedules snapshot collection on its owning loop and waits for the result.

This keeps access consistent while preserving the recorder's event-loop ownership.

The result is a mapping:

```text
metric name
   |
   v
metric snapshot
```

## Metric iteration

The recorder also exposes registered metrics through:

```python
metrics = recorder.iter_metrics()
```

As with snapshots, the call is resolved on the recorder's owning loop when needed.

This is an inspection API, not the event-processing path.

## Thread boundary

The recorder uses a thread lock around lifecycle state, pending counters, and cross-thread coordination.

Operations such as `start()`, `stop()`, `register_metric()`, `register_event()`, `get_metric_snapshots()`, and
`iter_metrics()` can be called from outside the owning event loop.

When necessary, the recorder schedules work onto the owning loop.

This is what allows runtime-created recorders to be used by synchronous or asynchronous production code without making
production code own the metrics event loop.

## Recorder with MetricsRuntime

When a recorder is created by `MetricsRuntime`, most of this machinery is hidden from application code.

The runtime provides the owning thread and event loop.

The runtime creates the recorder in that environment.

The application receives a ready-to-use recorder.

```text
application code
   |
   v
runtime.create_recorder(...)
   |
   v
ready-to-use recorder
```

The recorder still has the same internal model.

The runtime simply owns the environment around it.

## Direct recorder usage

A recorder can be used without `MetricsRuntime`.

In that case, the application must provide the event-loop environment and manage recorder lifecycle explicitly.

This mode is useful for custom integrations, tests, and applications that already own a suitable asyncio runtime.

The details of direct lifecycle management belong to advanced usage.

## Processing summary

The recorder processing model is:

```text
register_event(event)
   |
   v
validate state and pending limit
   |
   v
schedule event into queue
   |
   v
dispatcher task reads event
   |
   v
dispatch to registered metrics
   |
   v
metric accepts or ignores event
   |
   v
if accepted: _on_metric_changed(...)
   |
   v
task_done and pending counter decrement
```

This is the core mechanism behind the simple recorder contract used by production components.
