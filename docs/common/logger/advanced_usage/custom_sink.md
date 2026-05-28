# Custom sink

```{contents} Contents:
:depth: 1
:local:
```

## Overview

A custom sink is the lowest-level delivery extension point of the logger.

It receives completed `LogEvent` objects and delivers or hands them off to a backend.

The minimal contract is small:

```python
class MySink:
    def log(self, event: LogEvent) -> None:
        ...
```

But the behavior behind that method matters.

`log(event)` is called on the logging hot path and may be called concurrently. A custom sink should keep that method fast and thread-safe.

## When to write a custom sink

Write a custom sink when ready-to-use sinks do not match the destination you need.

Good reasons:

```text
send events to an internal collector
append events to an application-owned queue
write events into a custom in-memory store for tests
integrate with a proprietary logging backend
bridge LogEvent objects into another telemetry system
```

Do not write a custom sink just to change which events are emitted or how payloads are normalized.

Use event policy for event selection.

Use payload processing for payload representation.

Use a sink only for delivery.

## Minimal direct sink

A direct sink can be created and passed directly to `LogContext`.

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

Use it directly:

```python
from mvx.common.logger import LogContext, LogPayloadProcessor


sink = ListSink()
ctx = LogContext(
    namespace="test",
    log_sink=sink,
    payload_processor=LogPayloadProcessor(),
)
```

This is enough for tests and simple local integrations.

No descriptor is needed because the sink is not registered in the package-level sink registry.

No terminator is needed unless the sink owns resources that must be closed.

## Why the lock matters

The lock in the example is not decorative.

`log(event)` may be called from multiple threads. If the sink mutates internal state, it must protect that state.

For example, this state needs synchronization:

```text
in-memory event lists
closed flags
handler references
internal counters
queues that are not already thread-safe
backend clients shared across threads
```

If a sink uses only thread-safe primitives, an explicit lock may not be needed. But the implementation must still provide safe behavior under concurrent calls.

## Hot path behavior

`log(event)` should be fast.

Avoid slow work inside this method:

```text
network calls
database writes
remote acknowledgements
retry loops
reconnect logic
slow file flushes
large serialization work
```

If delivery may be slow, `log(event)` should hand the event off to another runtime.

Typical handoff strategies:

```text
thread-safe queue
background thread
event loop callback
async dispatcher
existing non-blocking client
```

For asynchronous delivery, use `AsyncioLogSink` instead of building lifecycle machinery from scratch.

## What the sink receives

A sink receives a fully prepared `LogEvent`.

By the time `log(event)` is called:

```text
LogEventMeta has already been built
event policy has already accepted the event
payload has already been normalized unless explicitly skipped
LogEvent has already been created
```

The sink should not normally inspect raw application objects.

It should not normalize payload values.

It should not decide whether an event should be emitted.

It should deliver or hand off the event it receives.

## Formatting inside a sink

A sink may perform backend-specific formatting.

For example, a sink may convert `LogEvent` into:

```text
JSON object
logging.LogRecord
database row
message queue record
HTTP request body
```

Keep formatting small and predictable on the hot path.

If formatting is expensive, move it to the background delivery runtime together with backend I/O.

## Direct sink with close()

If a direct sink owns resources, expose an explicit close method.

```python
from threading import RLock

from mvx.common.logger import LogEvent


class ClosableListSink:
    def __init__(self) -> None:
        self._lock = RLock()
        self._closed = False
        self.events: list[LogEvent] = []

    def log(self, event: LogEvent) -> None:
        with self._lock:
            if self._closed:
                return
            self.events.append(event)

    def close(self) -> None:
        with self._lock:
            self._closed = True
```

Calls after close are ignored in this example.

A different sink may choose to raise instead. The important part is that the behavior is explicit and thread-safe.

## Package-managed custom sink

A sink that should be registered through `configure_log_sink()` must provide the package-managed sink class contract.

That means:

```text
log(event)
build_descriptor(...)
create(...)
```

Example:

```python
from threading import RLock
from typing import Any

from mvx.common.logger import (
    LogEvent,
    LogSinkDescriptor,
    LogSinkProto,
    LogSinkTerminator,
)


class MemorySink:
    def __init__(self, *, bucket: str) -> None:
        self._lock = RLock()
        self._closed = False
        self._bucket = bucket
        self.events: list[LogEvent] = []

    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor:
        bucket = kwargs["bucket"]
        return LogSinkDescriptor(
            sink_type="memory",
            resource_key=("memory", bucket),
            config_key=(),
        )

    @classmethod
    def create(cls, **kwargs: Any) -> tuple[LogSinkProto, LogSinkTerminator]:
        sink = cls(**kwargs)

        def terminator() -> None:
            sink.close()

        return sink, terminator

    def log(self, event: LogEvent) -> None:
        with self._lock:
            if self._closed:
                return
            self.events.append(event)

    def close(self) -> None:
        with self._lock:
            self._closed = True
```

Register it:

```python
from mvx.common.logger import configure_log_sink


sink = configure_log_sink(
    name="memory",
    sink_cls=MemorySink,
    bucket="test-events",
)
```

This sink can now participate in package-level sink registration and reset flows.

## Descriptor design

`build_descriptor()` should describe sink identity.

The descriptor has three parts:

```text
sink_type
    logical sink type

resource_key
    backend resource identity

config_key
    relevant configuration that affects compatibility
```

The descriptor is used by the package-level registry before sink creation.

It decides whether a repeated registration request is idempotent or conflicting.

```text
same name + same descriptor
    return existing sink

same name + different descriptor
    raise conflict
```

Use `resource_key` for the target resource.

Use `config_key` for behavior that would make the same named sink incompatible with an existing one.

## Terminator design

`create()` returns a sink and a terminator.

The terminator is called when the package-level lifecycle closes or resets the sink.

A terminator should be:

```text
idempotent
thread-safe
safe if the sink was only partially used
```

For simple sinks, the terminator can call `close()`.

For async or background sinks, the terminator should stop the runtime and release backend resources.

## When to use AsyncioLogSink instead

Do not build your own background runtime unless you need something unusual.

Use `AsyncioLogSink` when:

```text
delivery may be slow
backend API is async
sink needs queueing
sink needs background dispatch
sink needs lifecycle start/stop hooks
sink must keep log(event) fast while delivery happens elsewhere
```

With `AsyncioLogSink`, the subclass normally implements:

```text
build_descriptor(...)
_dispatch_core(event)
optional _on_starting()
optional _on_stopped()
```

The base class owns event acceptance, thread-safe handoff, lifecycle, and package-managed runtime creation.

## What a custom sink should not do

A custom sink should not normally filter events.

Filtering belongs to event policy.

A custom sink should not normalize raw payload values.

Payload normalization belongs to payload processing.

A custom sink should not do slow backend delivery directly inside `log(event)`.

Slow delivery belongs behind a queue, thread, event loop, or another runtime boundary.

A custom sink should not mutate shared state without synchronization.

Thread-safety is part of the expected sink behavior.

## Testing a custom sink

A custom sink should be tested independently from the package-level facade.

At minimum, test:

```text
log(event) accepts prepared events
internal state is protected under concurrent calls
close or terminator is idempotent
descriptor is stable for equivalent configuration
descriptor changes for conflicting configuration
```

For package-managed sinks, also test:

```text
configure_log_sink returns existing sink for same descriptor
configure_log_sink raises conflict for different descriptor
close_log_sink calls terminator
```

## Design summary

A custom sink is a delivery extension point.

The minimal direct sink implements `log(event)`.

A package-managed sink also implements `build_descriptor()` and `create()`.

`log(event)` should be fast and thread-safe.

The sink receives prepared `LogEvent` objects and should deliver or hand them off without taking over event selection or payload normalization.
