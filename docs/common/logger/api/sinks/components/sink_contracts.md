# Sink contracts

This page documents the common contracts used to implement custom sinks.

A sink is the final delivery boundary of the logger pipeline. It receives fully prepared `LogEvent` objects and hands them off to a backend-specific delivery mechanism.

Two requirements are central to the sink contract:

```text
log(event) is on the logging hot path
log(event) may be called concurrently
```

A custom sink should keep `log(event)` fast and thread-safe.

Use this page when you want to implement a custom sink or make a sink available through the package-level sink registry.

## Contract layers

There are two levels of sink integration.

```text
Direct sink
    implement LogSinkProto.log(event)

Package-managed sink
    implement LogSinkProto on the sink instance
    implement LogSinkClassProto on the sink class
    provide build_descriptor(...)
    provide create(...)
```

A direct sink can be passed directly to `LogContext`.

A package-managed sink can be registered through `configure_log_sink()` and later retrieved, reused, closed, or reset through the package-level facade.

## Minimal sink contract

`LogSinkProto` is the minimal sink contract.

```{eval-rst}
.. autoclass:: mvx.common.logger.LogSinkProto
   :members:
```

A minimal sink only needs to implement one method:

```python
class MySink:
    def log(self, event: LogEvent) -> None:
        ...
```

The method receives a completed `LogEvent`.

At this point:

```text
event metadata already exists
event policy has already accepted the event
payload has already been prepared
LogEvent has already been created
```

The sink is not responsible for deciding whether the event should exist. It is responsible for fast, thread-safe delivery or handoff.

## Hot path expectations

`LogSinkProto.log(event)` is called on the logging hot path.

A sink implementation should keep this method fast and avoid blocking work.

In particular, `log(event)` should not normally wait for:

```text
slow file flushes
network calls
database writes
remote acknowledgements
retries
reconnect loops
long serialization work
long formatting work
```

The method should either complete delivery quickly or hand the event off to an internal runtime that performs slow work elsewhere.

Good hot-path behavior:

```text
write quickly to an already available local stream
append to an in-memory list in tests
enqueue to an internal queue
schedule work on another thread
schedule work on another event loop
hand off to a background dispatcher
```

Dangerous hot-path behavior:

```text
open a network connection
wait for a remote logging service
retry failed delivery synchronously
perform blocking database writes
flush a slow file synchronously on every event
perform large payload transformations inside the sink
```

If a sink needs waiting, retries, buffering, reconnects, batching, or slow backend delivery, that work should be moved out of `log(event)`.

Use a background thread, an internal event loop, a queue, or a base such as `AsyncioLogSink` to keep the public sink boundary responsive.

The sink protocol does not enforce this mechanically, but custom sink implementations should treat it as a design requirement.

## Thread-safety expectations

`LogSinkProto.log(event)` may be called concurrently.

A custom sink implementation should treat `log(event)` as a thread-safe public method. If the sink owns mutable state, the implementation must protect that state.

Typical mutable state that needs protection:

```text
closed flag
internal buffers
in-memory event lists
handler references
queue counters
backend client state shared across threads
```

The exact synchronization mechanism is implementation-specific.

A sink may use:

```text
locks
thread-safe queues
event-loop handoff
thread-safe scheduler APIs
atomic state managed by another runtime
```

The public behavior should be safe under concurrent calls:

```text
multiple threads may call log(event)
one thread may call log(event) while another thread closes the sink
package-level lifecycle code may call the terminator while logging activity exists
```

A sink should not corrupt its internal state under these conditions.

For sinks that hand events off to another runtime, `log(event)` should perform the handoff in a thread-safe way.

For example:

```text
use a lock around local state
use a thread-safe queue
use loop.call_soon_threadsafe(...)
use asyncio.run_coroutine_threadsafe(...)
```

If a sink cannot provide thread-safe behavior, it should document that limitation explicitly. Ready-to-use logger sinks and reusable custom sinks should normally be thread-safe.

## What a sink should do

A sink should deliver, enqueue, schedule, store, or otherwise hand off the prepared event.

Examples:

```text
write quickly to a local stream
append to a list in tests
enqueue to an async worker
schedule delivery on another runtime
hand off to a background dispatcher
```

The sink boundary is:

```text
LogEvent
   |
   v
sink.log(event)
   |
   v
fast, thread-safe delivery or backend-specific handoff
```

A sink may perform small backend-specific formatting if needed, but heavy transformation should not be placed on the hot path.

## What a sink should not do

A sink should not normally filter events.

Filtering belongs to event policy.

A sink should not normalize raw payload values.

Payload normalization happens before `LogEvent` delivery.

A sink should not depend on the internals of the context tree.

Context resolution and inheritance belong to `LogContext`.

A sink should not use slow backend behavior as part of the normal `log(event)` path.

Slow delivery belongs behind a queue, background dispatcher, thread, event loop, or another runtime boundary.

A sink should not mutate shared state without synchronization.

Thread-safety is part of the expected sink behavior.

A sink may still raise backend-specific errors. If delivery fails, `LogContext` decides how to handle that logging infrastructure failure according to `LogErrorHandlingPolicy`.

## Direct sink implementation

A direct sink is useful when application code owns sink creation and cleanup.

Example:

```python
from threading import RLock

from mvx.common.logger import LogEvent


class ListSink:
    def __init__(self) -> None:
        self._lock = RLock()
        self.events: list[LogEvent] = []

    def log(self, event: LogEvent) -> None:
        with self._lock:
            self.events.append(event)
```

This sink can be passed directly to `LogContext`.

It does not need a descriptor or terminator because it does not participate in package-level sink registration.

This kind of sink is useful for tests, local embedding, and simple in-memory delivery.

The lock in this example is not decorative. It protects the internal list when `log(event)` is called from multiple threads.

## Package-managed sink contract

`LogSinkClassProto` is the class-level contract used by package-level sink registration.

```{eval-rst}
.. autoclass:: mvx.common.logger.LogSinkClassProto
   :members:
```

A package-managed sink class provides two class methods:

```text
build_descriptor(...)
create(...)
```

This lets the package-level facade configure sinks by class:

```python
configure_log_sink(
    name="custom",
    sink_cls=MySink,
    ...,
)
```

The sink instance still implements the minimal delivery contract.

The class-level contract adds registration identity and cleanup behavior.

## Sink descriptor

`LogSinkDescriptor` describes a package-managed sink configuration.

```{eval-rst}
.. autoclass:: mvx.common.logger.LogSinkDescriptor
   :members:
   :member-order: bysource
   :class-doc-from: both
```

The descriptor is built before sink creation and used during registration.

It lets the sink registry distinguish three cases:

```text
name not registered
    create and register a new sink

same name, same descriptor
    return the existing sink

same name, different descriptor
    raise a configuration conflict
```

This makes repeated setup idempotent when the requested configuration is equivalent, while still detecting conflicting reuse of the same sink name.

## Descriptor semantics

A descriptor has three parts.

```text
sink_type
    logical sink type

resource_key
    values identifying the target resource

config_key
    values identifying relevant configuration that affects compatibility
```

The descriptor should be stable for equivalent configurations.

If two configuration requests target the same resource with the same relevant behavior, they should produce the same descriptor.

If two configuration requests would conflict, they should produce different descriptors.

For example, a file sink may use the resolved file path as the resource key and include level, format, mode, encoding, and filters in the config key.

A stream sink may use the stream target as part of the resource key and include level, format, date format, and filters in the config key.

## Terminator

`LogSinkTerminator` is the cleanup hook returned by a package-managed sink factory.

```python
LogSinkTerminator = Callable[[], None]
```

The terminator is called by package-level lifecycle management when a sink is closed or when the logger is reset.

Implementations should make the terminator idempotent and thread-safe.

Typical terminator responsibilities include:

```text
closing handlers
stopping background runtimes
flushing or closing files
removing installed hooks
releasing backend connections
```

A terminator should not assume that the sink was fully started. It may be called during defensive cleanup paths.

A terminator may run while other threads are still trying to log. The sink implementation should make this shutdown path safe.

## create() result

`LogSinkClassProto.create()` returns a pair:

```text
sink
terminator
```

The sink is the object that implements `LogSinkProto`.

The terminator releases resources owned by that package-managed sink.

For simple synchronous sinks, the terminator may call `close()`.

For asynchronous sinks, the terminator usually stops the runtime and then releases backend resources.

## Package-managed sink checklist

A package-managed custom sink should provide:

```text
log(event)
    fast, thread-safe delivery or handoff of one prepared LogEvent

build_descriptor(**kwargs)
    stable description of the requested sink configuration

create(**kwargs)
    sink construction plus idempotent, thread-safe cleanup hook
```

It should also decide which configuration values belong to `resource_key` and which belong to `config_key`.

If slow delivery is required, `create()` is usually the place to prepare the runtime that keeps `log(event)` fast.

## Choosing the right base

For a simple synchronous sink, implement `LogSinkProto` directly.

Good candidates:

```text
in-memory test sink
fast stream-like sink
thin adapter over an already non-blocking backend
```

For a sink that needs buffering, background delivery, or asynchronous backend work, use a separate runtime boundary.

Good candidates:

```text
network sink
remote collector sink
database sink
message queue sink
file sink with buffered async delivery
```

`AsyncioLogSink` is the built-in base for sinks that need asynchronous delivery behind the synchronous `log(event)` boundary. It provides a runtime shell designed to keep event acceptance thread-safe and fast while delivery happens elsewhere.

