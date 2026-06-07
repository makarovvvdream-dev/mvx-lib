# Runtime model

```{contents} Contents:
:depth: 1
:local:
```

`MetricsRuntime` is the synchronous management layer around an internal metrics event loop.

It gives application code a simple API for creating, finding, stopping, and removing recorders, while the actual
recorder work is executed inside a dedicated runtime thread with its own `asyncio` event loop.

The important split is:

```text
application thread
   |
   v
MetricsRuntime public API
   |
   v
runtime thread / asyncio event loop
   |
   v
AsyncioMetricsRecorder instances
```

The recorder is still the boundary used by production components.

`MetricsRuntime` owns the execution environment where runtime-created recorders live.

## Public shape

Application code uses `MetricsRuntime` through synchronous methods:

```python
runtime = MetricsRuntime(namespace="example.metrics")
runtime.start()

try:
    recorder = runtime.create_recorder("document_storage")

    storage = DocumentStorage(metrics_recorder=recorder)
    storage.save_document("doc-001", "First document")

finally:
    runtime.shutdown()
```

The public API hides the runtime thread and event loop.

Application code does not create the recorder loop and does not start recorder tasks manually.

## Runtime thread and event loop

When `start()` is called, the runtime creates a dedicated thread.

Inside that thread, it creates a new `asyncio` event loop and runs it.

```text
runtime.start()
   |
   v
create runtime thread
   |
   v
create asyncio event loop in that thread
   |
   v
run loop forever
```

This loop becomes the owning loop for recorders created by the runtime.

## Synchronous API over asynchronous internals

The runtime API is synchronous from the caller's point of view.

Internally, operations that must run in the runtime loop are scheduled into that loop and the public method waits for
the result.

```text
caller thread
   |
   v
runtime public method
   |
   v
schedule coroutine on runtime loop
   |
   v
wait for result
```

This is what lets ordinary application code manage asyncio-based recorders without owning their event loop directly.

## Runtime state

`MetricsRuntime` has its own lifecycle state.

At a high level, the important states are:

```text
VIRGIN
STARTING
RUNNING
STOPPING
CLOSED
FAILURE
```

The runtime starts in `VIRGIN`.

During startup it moves through `STARTING`.

After successful `start()`, it becomes `RUNNING`.

During shutdown it moves through `STOPPING`.

After successful `shutdown()`, it becomes `CLOSED`.

If startup or shutdown fails, it moves to `FAILURE`.

This state protects runtime operations from being called in the wrong order.

## Creating a recorder

Application code creates a recorder through the runtime:

```python
recorder = runtime.create_recorder("document_storage")
```

From the outside this is one synchronous call.

Internally, the runtime:

1. validates and normalizes the recorder id;
2. checks that the runtime is running;
3. schedules recorder creation inside the runtime event loop;
4. creates `AsyncioMetricsRecorder` in that loop;
5. starts the recorder;
6. stores it in the runtime registry only after successful startup;
7. returns the ready-to-use recorder to the caller.

The shape is:

```text
create_recorder("document_storage")
   |
   v
schedule _create_recorder_core(...) on runtime loop
   |
   v
create AsyncioMetricsRecorder
   |
   v
start recorder
   |
   v
store recorder in registry
   |
   v
return recorder
```

This matters because a recorder created by the runtime is already started and attached to the runtime-managed event
loop.

The production component receives a ready-to-use recorder:

```python
storage = DocumentStorage(metrics_recorder=recorder)
```

## Recorder options

`MetricsRuntime` can provide defaults for recorders it creates.

Those defaults include:

* recorder queue maximum size;
* recorder queue overflow policy;
* runtime log context.

When `create_recorder()` is called, application code can override these values for a specific recorder.

If no override is provided, the runtime default is used.

This keeps common recorder settings centralized while still allowing per-recorder customization.

## Recorder id and entity id

The recorder id is the key used by the runtime registry.

In simple cases, the same value also identifies the measured entity:

```python
recorder = runtime.create_recorder("document_storage")
```

For repeated entities, the id can represent a specific instance:

```python
connection_1 = runtime.create_recorder("connection-001")
connection_2 = runtime.create_recorder("connection-002")
```

Runtime-created recorders can also receive an explicit `entity_id`.

If no explicit entity id is provided, the recorder id is used as the recorder identity.

```text
recorder id
   |
   v
runtime registry key

entity id
   |
   v
measured scope identity
```

Often they are the same value.

## Recorder registry

The runtime owns a registry of recorders created inside it.

```text
MetricsRuntime(namespace="networking.metrics")
   |
   +--> recorder: connection-001
   |
   +--> recorder: connection-002
   |
   +--> recorder: connection-003
```

The runtime also tracks recorder creation and removal in progress.

Conceptually, it coordinates:

```text
active recorders
recorders being created
recorders being removed
```

This prevents overlapping create/remove operations from corrupting registry state.

## Finding recorders

Once a recorder exists, application code can retrieve it from the runtime.

The runtime provides operations for:

* getting a recorder by id;
* trying to get a recorder by id;
* listing recorder ids.

These operations use the runtime registry and are exposed through the synchronous public API.

## Stopping a recorder

A runtime-created recorder can be stopped through the runtime.

At a high level, stopping means:

```text
stop_recorder(recorder_id)
   |
   v
schedule stop on runtime loop
   |
   v
stop recorder lifecycle
```

Stopping a recorder does not have to remove it from the registry.

That distinction lets runtime code separate:

```text
stop processing
```

from:

```text
remove this recorder from runtime ownership
```

## Stop and remove

A recorder can also be stopped and removed from the runtime registry.

The conceptual flow is:

```text
stop_and_remove_recorder(recorder_id)
   |
   v
mark recorder as being removed
   |
   v
stop recorder if needed
   |
   v
remove recorder from registry
   |
   v
clear removal marker
```

This is useful when a measured entity is gone.

For example, when a connection is permanently closed, its recorder can be stopped and removed.

## Event processing path

Once a runtime-created recorder is passed to production code, event submission still goes through the recorder.

```python
self._metrics_recorder.register_event(event=event)
```

The runtime does not receive metric events directly from production components.

The path is:

```text
production code
   |
   v
recorder.register_event(event)
   |
   v
recorder internal buffer
   |
   v
runtime-owned event loop
   |
   v
recorder dispatcher
   |
   v
registered metrics
```

The runtime provides the loop and thread.

The recorder owns event processing.

The metrics own interpretation and state changes.

## Logging

`MetricsRuntime` can use `MVX Logger` when a `log_context` is provided.

Runtime logging is based on `log_invocation` around public runtime operations.

The logged runtime events are:

```text
metrics_runtime.start
metrics_runtime.shutdown
metrics_runtime.create_recorder
metrics_runtime.stop_recorder
metrics_runtime.stop_and_remove_recorder
metrics_runtime.get_recorder
metrics_runtime.try_get_recorder
metrics_runtime.list_recorder_ids
```

Lifecycle and mutation operations are the normal runtime logging surface:

```text
metrics_runtime.start
metrics_runtime.shutdown
metrics_runtime.create_recorder
metrics_runtime.stop_recorder
metrics_runtime.stop_and_remove_recorder
```

Inspection operations are separated:

```text
metrics_runtime.get_recorder
metrics_runtime.try_get_recorder
metrics_runtime.list_recorder_ids
```

This separation matters because inspection calls can be more frequent and less important for ordinary runtime
diagnostics.

For convenience, `MVX Metrics` provides ready-to-use event policies for runtime logging.

The policy mode is controlled by `MetricsRuntimeLogPolicyMode`:

```text
SILENT
NORMAL
INSPECTION
```

The helper functions are:

```python
metrics_runtime_event_policy_config(...)
metrics_runtime_event_policy(...)
```

`SILENT` disables runtime log events by default.

`NORMAL` enables the usual lifecycle and recorder-management events:

```text
metrics_runtime.start
metrics_runtime.shutdown
metrics_runtime.create_recorder
metrics_runtime.stop_recorder
metrics_runtime.stop_and_remove_recorder
```

`INSPECTION` includes all `NORMAL` events and also enables inspection operations:

```text
metrics_runtime.get_recorder
metrics_runtime.try_get_recorder
metrics_runtime.list_recorder_ids
```

This lets applications start with a practical logging profile instead of writing a custom event policy immediately.

The runtime can also pass its `log_context` to recorders created inside it. A specific recorder can override that
context when it is created.

So the logging model is:

```text
runtime lifecycle and recorder-management calls
   |
   v
normal runtime log events

runtime inspection calls
   |
   v
inspection runtime log events

runtime-created recorders
   |
   v
use runtime log_context unless overridden
```

Runtime logging is diagnostic infrastructure logging. It does not log every metric event emitted by production code.

## Shutdown

`shutdown()` closes the runtime-managed environment.

At a high level, shutdown does four things:

1. stops and removes runtime-owned recorders;
2. stops the runtime event loop;
3. joins the runtime thread;
4. clears runtime references.

The shape is:

```text
runtime.shutdown()
   |
   v
stop/remove all recorders
   |
   v
stop event loop
   |
   v
join runtime thread
   |
   v
clear thread and loop references
   |
   v
CLOSED
```

If recorder shutdown fails, the runtime collects those failures and reports shutdown failure through its own error
surface.

This keeps shutdown responsibility centralized in the runtime.

## One runtime, many recorders

The normal model is:

```text
one application or large subsystem
   |
   v
one MetricsRuntime
   |
   v
many recorders
```

For example:

```text
application
   |
   v
MetricsRuntime(namespace="application.metrics")
   |
   +--> recorder: document_storage
   |
   +--> recorder: connection-001
   |
   +--> recorder: connection-002
   |
   +--> recorder: worker-3
```

```{warning}
The usual application model is to create and keep **one** `MetricsRuntime` running for the application or for a large subsystem, and then create multiple recorders inside that runtime.

Creating a separate runtime for every component or every measured entity is not the intended usage. Each runtime owns its own thread and event loop, so creating too many runtimes can waste threads and eventually exhaust process resources.

Multiple runtimes should be reserved for cases where metrics processing is intentionally separated, for example when one runtime is not enough for a very heavy metrics workload or when subsystems must be isolated deliberately.
```

## Runtime and production code

Production components should normally depend on a recorder, not on `MetricsRuntime`.

Good:

```python
recorder = runtime.create_recorder("document_storage")
storage = DocumentStorage(metrics_recorder=recorder)
```

Avoid making the production component create or own the runtime.

Runtime ownership is an application wiring decision.

Metric event emission is a production component responsibility.

Recorder processing belongs to the runtime-managed recorder environment.

## Synchronous and asynchronous production code

A runtime-created recorder can be used by synchronous production code:

```python
def save_document(self, document_id: str, content: str) -> None:
    ...
    self._send_metric_event(event)
```

and by asynchronous production code:

```python
async def save_document(self, document_id: str, content: str) -> None:
    ...
    self._send_metric_event(event)
```

The production component does not have to run in the runtime event loop.

The runtime owns the loop used for recorder processing.

## Runtime is not a recorder

`MetricsRuntime` is not a recorder.

It does not own metric instances directly.

It does not interpret metric events.

It does not expose metric meaning.

Its job is to create and manage the environment where recorders live.

```text
MetricsRuntime
   |
   v
owns thread, event loop, recorder registry

Recorder
   |
   v
owns metrics and event processing

Metric
   |
   v
owns measurement state
```

## Runtime is not a monitoring backend

`MetricsRuntime` does not export metrics to Prometheus, OpenTelemetry, StatsD, or any other platform.

It does not store time-series history.

It does not render dashboards.

It only owns local runtime infrastructure for recorders.

External integration belongs to adapters, custom recorders, recorder hooks, or snapshot consumers.

## Summary

`MetricsRuntime` is the synchronous management layer around an internal metrics event loop.

It starts a dedicated thread, creates an `asyncio` event loop inside it, creates and starts recorders in that loop,
stores them in a registry, and shuts the whole environment down when metrics processing is no longer needed.

It also exposes runtime diagnostic logging through ready-to-use event policies.

The public API stays simple.

The internal runtime model handles the thread, loop, recorder lifecycle, registry, logging, and shutdown coordination.
