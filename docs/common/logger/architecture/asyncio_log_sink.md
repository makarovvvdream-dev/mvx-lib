# AsyncioLogSink

```{contents} Contents:
:depth: 1
:local:
```

## Overview

`AsyncioLogSink` is the logger's base runtime for sinks that need asynchronous delivery while keeping the public sink boundary synchronous.

From the outside, it still behaves like a regular sink:

```python
sink.log(event)
```

The caller does not await backend delivery.

Internally, the sink accepts the event, accounts for it, schedules it into an asyncio-owned queue, and lets a dispatcher task deliver it later.

The central design goal is simple:

```text
keep log(event) fast and thread-safe
move slow delivery work out of the logging hot path
```

This makes `AsyncioLogSink` useful for backends such as files, network collectors, databases, message queues, Redis streams, or remote logging services.

## Position in the sink model

The generic sink contract says that `log(event)` receives a completed `LogEvent` and delivers or hands it off.

`AsyncioLogSink` implements the handoff version of that contract.

```text
LogContext
   |
   v
LogEvent
   |
   v
AsyncioLogSink.log(event)
   |
   v
thread-safe handoff
   |
   v
async dispatcher
   |
   v
backend-specific delivery
```

The base class owns the runtime shell.

A concrete subclass owns the backend-specific parts:

```text
build_descriptor(...)
    describe the sink identity for package-level registration

_dispatch_core(event)
    deliver one accepted event to the backend

_on_starting()
    optional backend startup hook

_on_stopped()
    optional backend shutdown hook
```

This split is important. The base class can manage lifecycle, queueing, thread handoff, and shutdown. It cannot know what resource identifies a Redis sink, HTTP sink, file sink, or database sink. That identity belongs to the concrete subclass.

## Two construction modes

`AsyncioLogSink` supports two construction modes.

### Direct construction

Direct construction calls the class constructor directly:

```python
sink = MyAsyncSink(...)
```

This attaches the sink to the current running event loop.

Direct construction is useful when the caller already owns the event loop and wants the sink runtime to live inside it.

If no running event loop is available in the current thread, construction fails. This is intentional: direct construction does not create a loop for you.

### Package-managed creation

Package-managed creation uses the class-level factory:

```python
sink, terminator = MyAsyncSink.create(...)
```

This mode creates a dedicated runtime:

```text
create a new thread
   |
   v
create a new asyncio event loop in that thread
   |
   v
construct the sink inside that loop
   |
   v
start the sink
   |
   v
return sink and terminator
```

This is the normal mode for package-level sink registration.

The returned sink can be used from other threads through `log(event)`. The returned terminator stops the sink runtime and shuts down the dedicated event loop thread.

## Package-managed subclass requirement

`AsyncioLogSink` provides a shared `create()` implementation because runtime creation is common for async sinks.

It does not provide a real `build_descriptor()` implementation.

A concrete package-managed subclass must implement `build_descriptor()`.

The reason is architectural: descriptor identity is backend-specific.

Examples:

```text
file sink
    resource identity: resolved file path
    config identity: level, format, mode, encoding, filters

Redis sink
    resource identity: Redis URL and stream name
    config identity: delivery options, serialization mode, limits

HTTP sink
    resource identity: collector endpoint
    config identity: headers, timeouts, formatting options
```

The async base owns runtime mechanics. The subclass owns backend identity and delivery.

## Lifecycle model

The sink moves through a small state model.

Normal lifecycle:

```text
VIRGIN -> STARTING -> RUNNING -> STOPPING -> STOPPED
```

Failure lifecycle:

```text
STARTING/RUNNING/STOPPING -> FAILURE
```

Unexpected dispatcher cancellation:

```text
RUNNING -> CANCELLED
```

`FAILURE` and `CANCELLED` are terminal error states for the sink instance.

The state model protects lifecycle operations from being applied at the wrong time. For example, stopping is only valid when the sink is running.

## Start flow

Starting is requested through `start()`.

From the caller's perspective, `start()` returns immediately with a wait handle.

The actual startup work is scheduled on the sink event loop.

Startup flow:

```text
start()
   |
   v
schedule _start_core() on sink loop
   |
   v
run _on_starting()
   |
   v
create dispatcher task
   |
   v
state becomes RUNNING
```

`_on_starting()` is the startup extension hook. A subclass may use it to open backend connections, create clients, authenticate, or prepare resources.

If `_on_starting()` fails, the sink enters the failure state and the start wait handle reports that error.

## Event acceptance flow

`log(event)` is the hot-path method.

It is designed to be fast, thread-safe, and non-blocking with respect to actual backend delivery.

The acceptance flow is:

```text
log(event)
   |
   v
acquire thread lock
   |
   v
check state
   |
   v
check pending limit
   |
   v
increment pending counter
   |
   v
release thread lock
   |
   v
trigger lazy startup if needed
   |
   v
schedule queue.put_nowait(event) on sink loop
```

The method accepts events only while the sink is in one of these states:

```text
VIRGIN
STARTING
RUNNING
```

If the sink is `VIRGIN`, `log(event)` triggers lazy startup and still accepts the event.

If the sink is stopping, stopped, failed, or cancelled, `log(event)` rejects the event with an invalid-state error.

## Thread-safety boundary

`log(event)` may be called from threads other than the sink event loop thread.

The base class protects shared state with a thread lock:

```text
state
last error
start future
stop future
pending counter
```

It uses thread-safe asyncio scheduling for cross-thread handoff:

```text
loop.call_soon_threadsafe(...)
asyncio.run_coroutine_threadsafe(...)
```

This is why the public sink boundary can remain synchronous and thread-safe while backend delivery happens inside an asyncio runtime.

Subclasses should respect this boundary. Backend state that is accessed only from `_on_starting()`, `_dispatch_core()`, and `_on_stopped()` normally belongs to the sink event loop. Backend state accessed from other threads must be protected by the subclass.

## Queue and pending counter

The internal asyncio queue is intentionally unbounded.

Backpressure is controlled by a pending counter, not by the queue object itself.

This distinction matters because `log(event)` may be called from another thread. When `log(event)` schedules `queue.put_nowait(event)` with `call_soon_threadsafe()`, the event has already been accepted but may not yet be physically inside the asyncio queue.

The pending counter covers both cases:

```text
accepted but not yet enqueued
accepted and already enqueued
currently being processed by dispatcher
```

The counter is decremented after `_dispatch_core(event)` finishes and the queue task is marked done.

## Overflow behavior

When the pending counter reaches the configured limit, the overflow policy decides what happens.

There are two behaviors:

```text
DROP
    silently drop the new event

RAISE_ERROR
    raise a queue overflow error to the caller
```

Overflow is checked before the event is scheduled into the asyncio queue.

This keeps the pending limit meaningful even under cross-thread event submission.

## Dispatcher model

The dispatcher is an asyncio task created during startup.

Its job is to consume accepted events and deliver them through the subclass hook.

Dispatcher loop:

```text
wait for event from queue
   |
   v
await _dispatch_core(event)
   |
   v
mark queue item done
   |
   v
decrement pending counter
```

`_dispatch_core(event)` is the required delivery hook.

The base class does not know how to deliver to a file, Redis, HTTP endpoint, database, or message queue. It only knows when to call the subclass delivery method and how to account for the accepted event.

## Dispatcher failure

If `_dispatch_core(event)` raises, the dispatcher task fails.

That failure moves the sink into an error state unless the sink is already stopping.

A failed dispatcher means backend delivery is no longer trustworthy. The sink records the error and schedules cleanup.

Unexpected dispatcher cancellation outside normal stopping is treated separately and moves the sink into the cancelled state.

This distinction avoids confusing normal shutdown cancellation with unexpected runtime cancellation.

## Stop and flush flow

Stopping is requested through `stop()`.

Like `start()`, it returns a wait handle immediately and schedules actual work on the sink event loop.

Stop flow:

```text
stop()
   |
   v
schedule _stop_core() on sink loop
   |
   v
flush accepted events on a best-effort basis
   |
   v
cancel dispatcher
   |
   v
run _on_stopped()
   |
   v
state becomes STOPPED
```

Flushing is best-effort. It waits for already accepted events only while the dispatcher is alive. If the dispatcher finishes first, flushing cannot make progress and shutdown continues through the normal dispatcher outcome path.

`_on_stopped()` is the shutdown extension hook. A subclass may use it to close clients, handlers, connections, files, or other backend resources.

## Wait handles

`start()` and `stop()` return wait handles.

A wait handle can be used synchronously:

```python
result = sink.start().wait()
```

or asynchronously:

```python
result = await sink.start()
```

Both forms return an operation result object containing:

```text
operation name
success flag
optional error
```

This gives callers one result shape for lifecycle operations instead of requiring every caller to catch every possible startup or shutdown exception directly.

## Threaded runtime terminator

Package-managed creation returns a terminator together with the sink.

The terminator is idempotent.

Its job is to stop the sink runtime and shut down the event loop thread.

Termination flow:

```text
check current sink status
   |
   v
if running or stopping, stop sink and wait
   |
   v
schedule event loop stop
   |
   v
join runtime thread
```

The terminator cannot be called from its own event loop thread. That would deadlock the shutdown path, so it raises an error.

During event loop shutdown, remaining pending tasks are cancelled and awaited with a timeout before the loop is closed.

## Extension point summary

A typical subclass provides this shape:

```python
class CustomSink(AsyncioLogSink):
    @classmethod
    def build_descriptor(cls, **kwargs):
        ...

    async def _on_starting(self) -> None:
        ...

    async def _dispatch_core(self, event: LogEvent) -> None:
        ...

    async def _on_stopped(self) -> None:
        ...
```

Required for package-level registration:

```text
build_descriptor(...)
    describe resource and configuration identity
```

Required for delivery:

```text
_dispatch_core(event)
    deliver one accepted event
```

Optional for resource lifecycle:

```text
_on_starting()
    prepare backend resources

_on_stopped()
    close backend resources
```

A subclass should usually not override `log(event)` or `create(...)`. Those methods are the reusable runtime shell that keeps event acceptance fast, thread-safe, and compatible with the package-level sink contract.

## What the base does not provide

`AsyncioLogSink` provides runtime infrastructure, not backend policy.

It does not implement:

```text
retries
reconnect strategy
batching
dead-letter storage
schema migration
external durability guarantees
exactly-once delivery
```

Those behaviors belong to concrete sink implementations.

The base class provides a safe runtime shell. Backend semantics remain explicit in the subclass.

## Design summary

`AsyncioLogSink` keeps the public sink boundary synchronous while moving delivery into an asyncio dispatcher.

It provides thread-safe event acceptance, lifecycle state management, lazy startup, pending-event accounting, overflow handling, best-effort flushing, dispatcher failure handling, and package-managed threaded runtime creation.

Concrete subclasses provide descriptor identity and backend delivery.

The result is a reusable foundation for custom sinks that need slow or asynchronous delivery without making `log(event)` slow or blocking.
