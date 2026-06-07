# Threading model

```{contents} Contents:
:depth: 1
:local:
```

`MVX Metrics` separates production execution from metrics processing.

Production code may be synchronous or asynchronous.

Recorder processing may be asynchronous.

`MetricsRuntime` can place recorder processing into a dedicated thread with its own `asyncio` event loop.

The goal is to let production code emit metric events without becoming responsible for the metrics event loop.

## Main threading shape

With `MetricsRuntime`, the usual threading shape is:

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
recorder processing
```

The production side performs a short handoff.

The recorder side performs event processing.

The runtime owns the thread and event loop used by runtime-created recorders.

## Production code can be sync or async

The production component does not have to be asynchronous.

A synchronous method can emit metrics:

```python
def save_document(self, document_id: str, content: str) -> None:
    ...
    self._send_metric_event(event)
```

An asynchronous method can emit metrics the same way:

```python
async def save_document(self, document_id: str, content: str) -> None:
    ...
    self._send_metric_event(event)
```

In both cases, the component uses the recorder-facing contract.

The production method does not need to run inside the metrics runtime loop.

## Recorder handoff from production code

The production handoff is:

```python
self._metrics_recorder.register_event(event=event)
```

From the caller's point of view, this is a normal method call.

It does not require `await`.

It does not require the caller to own an `asyncio` loop.

It does not make the production method asynchronous.

This is important because many production components are ordinary synchronous Python classes.

Metrics should not force them to become async-only.

## Asyncio nature of the recorder

The default recorder implementation is `AsyncioMetricsRecorder`.

It is an asyncio-based component.

Its processing side runs on an owning event loop.

That event loop may be:

* the loop owned by `MetricsRuntime`;
* an application-owned loop, when the recorder is used manually.

In the common runtime-managed case, the application does not provide this loop directly.

The runtime creates it and owns it.

## Runtime-owned loop

`MetricsRuntime` starts a dedicated thread and creates an `asyncio` event loop inside that thread.

Recorders created by the runtime are created inside that loop.

```text
MetricsRuntime
   |
   v
dedicated thread
   |
   v
asyncio event loop
   |
   v
runtime-created recorders
```

This gives recorders a stable async execution environment without requiring production components to manage it.

## Cross-thread scheduling

Runtime public methods are synchronous.

Internally, when a runtime operation needs to run on the runtime loop, it schedules work into that loop and waits for
the result.

```text
caller thread
   |
   v
MetricsRuntime public method
   |
   v
schedule work on runtime loop
   |
   v
wait for result
```

This is how application code can create, find, stop, and remove recorders without directly touching the runtime event
loop.

The same idea applies to recorder inspection operations when they need to be resolved on the recorder's owning loop.

## Thread-safety guarantees

`MVX Metrics` separates thread ownership from the public recorder-facing API.

When a recorder is created through `MetricsRuntime`, the runtime owns the thread and event loop used for recorder
processing. Production code does not have to run in that thread.

The intended guarantees are:

* runtime public operations can be called from application code without manually entering the runtime event loop;
* runtime-created recorders can be passed to production components running outside the runtime thread;
* production code can emit metric events through `register_event()` without owning the recorder event loop;
* recorder inspection calls, such as snapshot reads, coordinate with the recorder's owning loop when needed;
* metric state is updated on the recorder-processing side, not directly by production code.

This means the common usage pattern is thread-safe at the integration boundary:

```text
application / production thread
   |
   v
recorder public API
   |
   v
recorder owning event loop
   |
   v
metric processing
```

The production component can be synchronous or asynchronous. It can call the recorder-facing API from its own execution
context.

The recorder is responsible for crossing into its owning event loop when the operation must be performed there.

There are also important limits.

Thread-safe API access does not mean that metric internals should be accessed or modified from arbitrary threads.

External code should not mutate metric private fields.

Metric state belongs to the metric and is updated through recorder processing.

Thread-safe API access also does not mean that the recorder has infinite capacity. If events are produced faster than
they are processed, the recorder's pending-event limit and overflow policy still apply.

So the guarantee is not:

```text
any amount of metric traffic is always accepted
```

The guarantee is:

```text
production code can safely use the recorder-facing API without managing the recorder event loop directly
```

## Recorder state and thread coordination

Recorder operations may be called from outside the recorder's owning event loop.

For that reason, recorder implementation has to coordinate access to lifecycle state, pending counters, and loop-owned
operations.

The important architectural idea is:

```text
caller side
   |
   v
thread-safe recorder API
   |
   v
recorder owning event loop
```

The caller sees a simple recorder method.

The recorder decides whether the work can be handled immediately or must be scheduled onto its owning loop.

## MetricsRuntime as the simple path

`MetricsRuntime` is the simple path for regular applications.

It hides:

* thread creation;
* event loop creation;
* recorder creation inside the correct loop;
* cross-thread scheduling;
* recorder startup;
* recorder shutdown.

Application code uses the runtime and recorders through normal method calls:

```python
runtime = MetricsRuntime(namespace="example.metrics")
runtime.start()

try:
    recorder = runtime.create_recorder("document_storage")
    storage = DocumentStorage(metrics_recorder=recorder)

finally:
    runtime.shutdown()
```

The production component receives only the recorder.

## Direct recorder usage

A recorder can also be used without `MetricsRuntime`.

In that case, the application owns the event-loop environment.

The recorder must be created where a running `asyncio` event loop is available.

The application also owns recorder startup and shutdown.

This is useful for advanced cases:

* applications that already own a suitable event loop;
* custom integration environments;
* tests that need direct recorder control;
* non-standard runtime models.

This mode gives more control, but also more responsibility.

Manual recorder lifecycle management belongs to advanced usage.

## Avoid mixing ownership

Do not make production components own runtime or loop details.

Good:

```text
application code
   |
   v
creates MetricsRuntime and recorder
   |
   v
passes recorder to production component
```

Avoid:

```text
production component
   |
   v
creates runtime
   |
   v
starts event loop
   |
   v
manages shutdown
```

A production component should focus on business behavior and metric event emission.

The application wiring owns runtime and threading decisions.

## Threading and measured scopes

Threading does not define metric scope.

Recorder identity defines metric scope.

For example, a runtime may have one thread and one loop, but many recorders:

```text
MetricsRuntime thread / loop
   |
   +--> recorder: connection-001
   |
   +--> recorder: connection-002
   |
   +--> recorder: connection-003
```

All of these recorders live in the same runtime environment.

Each recorder still has its own metric instances and its own measured scope.

## Threading and snapshots

Snapshot reads are recorder inspection operations.

From the caller's point of view:

```python
snapshots = recorder.get_metric_snapshots()
```

is a regular call.

Internally, if snapshot collection needs to happen on the recorder's owning loop, the recorder coordinates that.

The caller does not need to manage the event-loop boundary.

## What the threading model does not do

The threading model does not make metric processing magically free.

If metric processing becomes too heavy, it can still pressure the recorder buffer or runtime loop.

The design only separates production handoff from metrics-side processing.

It does not remove the cost of metric processing.

For unusually heavy metrics workloads, applications may need tuning, custom recorders, or deliberately separated
runtimes.

## Summary

`MVX Metrics` allows synchronous and asynchronous production code to use the same recorder-facing contract.

The default recorder is asyncio-based.

`MetricsRuntime` provides a dedicated thread and event loop for runtime-created recorders.

Production code does not need to own that loop.

Direct recorder usage is possible, but then the application owns the event-loop and lifecycle details.

The intended default model is simple:

```text
production code
   |
   v
recorder handoff
   |
   v
runtime-managed recorder processing
```
