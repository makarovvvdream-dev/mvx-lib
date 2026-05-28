# Asyncio sink backend

```{contents} Contents:
:depth: 1
:local:
```

## Overview

`AsyncioLogSink` is the recommended base for custom sinks that need slow or asynchronous backend delivery.

It preserves the normal sink boundary:

```python
sink.log(event)
```

but moves actual delivery into an internal asyncio runtime.

Use it when a custom sink needs to keep `log(event)` fast and thread-safe while backend work happens elsewhere.

Typical backends:

```text
network collectors
message queues
database writers
Redis streams
HTTP endpoints
custom async file or storage layers
```

## What the base provides

`AsyncioLogSink` provides the reusable runtime shell:

```text
thread-safe log(event)
lazy startup
pending-event accounting
overflow policy
async dispatcher
start/stop lifecycle
wait handles
package-managed create()
idempotent terminator
```

A subclass provides backend-specific behavior:

```text
build_descriptor(...)
_dispatch_core(event)
optional _on_starting()
optional _on_stopped()
```

This split keeps custom backend sinks small. The subclass should focus on backend identity, connection lifecycle, and event delivery.

## Minimal subclass shape

A package-managed async sink usually has this shape:

```python
from typing import Any

from mvx.common.logger import AsyncioLogSink, LogEvent, LogSinkDescriptor


class BackendSink(AsyncioLogSink):
    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor:
        ...

    async def _on_starting(self) -> None:
        ...

    async def _dispatch_core(self, event: LogEvent) -> None:
        ...

    async def _on_stopped(self) -> None:
        ...
```

`build_descriptor()` and `_dispatch_core()` are the important required pieces for a concrete package-managed backend sink.

`_on_starting()` and `_on_stopped()` are optional hooks for backend resources.

## Descriptor responsibility

`AsyncioLogSink` provides `create()`, but it cannot provide a real descriptor for every backend.

The subclass must implement `build_descriptor()`.

The descriptor should describe:

```text
which backend resource this sink targets
which configuration values make this sink compatible or conflicting
```

Example:

```python
from typing import Any

from mvx.common.logger import LogSinkDescriptor


@classmethod
def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor:
    endpoint = kwargs["endpoint"]
    stream_name = kwargs["stream_name"]

    return LogSinkDescriptor(
        sink_type="backend",
        resource_key=("backend", endpoint, stream_name),
        config_key=(),
    )
```

A stable descriptor makes package-level registration safe:

```text
same name + same descriptor
    return existing sink

same name + different descriptor
    raise conflict
```

## Constructor responsibility

The constructor stores backend configuration and passes runtime options to `AsyncioLogSink`.

```python
from mvx.common.logger import AsyncioLogSink


class BackendSink(AsyncioLogSink):
    def __init__(
        self,
        *,
        endpoint: str,
        stream_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._endpoint = endpoint
        self._stream_name = stream_name
        self._client = None
```

Do not perform slow backend connection work inside `log(event)`.

If the backend needs startup work, use `_on_starting()`.

## Startup hook

`_on_starting()` runs before the dispatcher starts.

Use it to prepare backend resources:

```text
open connection
create async client
authenticate
prepare stream or topic
initialize backend handles
```

Example:

```python
async def _on_starting(self) -> None:
    self._client = await open_backend_client(self._endpoint)
```

If `_on_starting()` raises, startup fails and the sink enters a failure state.

The error is reported through the start wait handle or through package-managed creation.

## Dispatch hook

`_dispatch_core(event)` is called by the dispatcher for each accepted event.

This is where backend delivery happens.

```python
async def _dispatch_core(self, event: LogEvent) -> None:
    assert self._client is not None
    await self._client.write(
        stream=self._stream_name,
        payload={
            "namespace": event.meta.event_namespace,
            "name": event.meta.event_name,
            "outcome": event.event_outcome,
            "level": event.level,
            "timestamp": event.timestamp,
            "payload": dict(event.payload),
        },
    )
```

The dispatcher awaits this method.

If it raises, the dispatcher fails and the sink moves into an error state.

The base class does not retry or reconnect automatically.

## Stop hook

`_on_stopped()` runs during normal shutdown after the dispatcher has stopped.

Use it to release backend resources:

```python
async def _on_stopped(self) -> None:
    if self._client is not None:
        await self._client.close()
        self._client = None
```

If `_on_stopped()` raises during normal stop, the stop operation reports a hook failure and the sink enters a failure state.

## Toy backend example

This example uses a small async in-memory backend. It shows the structure without depending on an external service.

```python
from typing import Any

from mvx.common.logger import AsyncioLogSink, LogEvent, LogSinkDescriptor


class MemoryBackend:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []
        self.closed = False

    async def write(self, record: dict[str, Any]) -> None:
        if self.closed:
            raise RuntimeError("backend is closed")
        self.records.append(record)

    async def close(self) -> None:
        self.closed = True


class MemoryBackendSink(AsyncioLogSink):
    def __init__(
        self,
        *,
        backend: MemoryBackend,
        stream_name: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._backend = backend
        self._stream_name = stream_name

    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor:
        stream_name = kwargs["stream_name"]
        return LogSinkDescriptor(
            sink_type="memory_backend",
            resource_key=("memory_backend", stream_name),
            config_key=(),
        )

    async def _dispatch_core(self, event: LogEvent) -> None:
        await self._backend.write(
            {
                "stream": self._stream_name,
                "event_name": event.meta.event_name,
                "event_outcome": event.event_outcome,
                "payload": dict(event.payload),
            }
        )

    async def _on_stopped(self) -> None:
        await self._backend.close()
```

This sink inherits:

```text
log(event)
start()
stop()
create(...)
thread-safe event acceptance
queueing
dispatcher lifecycle
terminator creation
```

It implements only backend identity and backend delivery.

## Package-level registration

A subclass can be registered through the package-level facade.

```python
from mvx.common.logger import configure_log_sink


backend = MemoryBackend()

sink = configure_log_sink(
    name="memory_backend",
    sink_cls=MemoryBackendSink,
    backend=backend,
    stream_name="events",
)
```

Package-level registration calls `build_descriptor()` before creating the sink.

If registration creates a new sink, the inherited `create()` method builds the runtime and starts the sink.

The package-level registry stores the returned terminator and calls it when the sink is closed or the logger is reset.

## Direct construction

Direct construction is also possible, but it requires a running event loop.

```python
sink = MemoryBackendSink(
    backend=backend,
    stream_name="events",
)

await sink.start()
sink.log(event)
await sink.stop()
```

This form is useful when the caller owns the event loop and does not want a dedicated runtime thread.

Package-level registration usually uses `create()` instead.

## What not to override

Most subclasses should not override `log(event)`.

The base implementation owns:

```text
state validation
lazy startup
pending counter
overflow behavior
thread-safe queue handoff
```

Most subclasses should not override `create()`.

The base implementation owns:

```text
event loop thread creation
sink bootstrap
startup wait
terminator creation
runtime shutdown
```

Override these methods only if the sink needs a fundamentally different runtime model.

## What the base does not do

`AsyncioLogSink` does not implement backend policy.

It does not provide:

```text
retry policy
reconnect strategy
batching
dead-letter queue
durability guarantee
exactly-once delivery
schema migration
```

If the backend needs those behaviors, the concrete sink must implement them explicitly.

A simple example sink should not pretend to provide production-grade delivery semantics.

## Testing checklist

Test the subclass at the backend boundary.

At minimum, check:

```text
build_descriptor() is stable for equivalent config
build_descriptor() changes for conflicting config
log(event) accepts events quickly
_dispatch_core(event) writes the expected backend record
stop() closes backend resources
terminator is idempotent
backend delivery failure moves the sink into failure behavior
```

For package-level registration, also test:

```text
configure_log_sink returns existing sink for same descriptor
configure_log_sink raises conflict for different descriptor
close_log_sink calls the terminator
```

## Design summary

Use `AsyncioLogSink` when a sink needs asynchronous backend delivery but must preserve a fast, thread-safe `log(event)` boundary.

The base class owns runtime mechanics.

The subclass owns:

```text
backend identity through build_descriptor()
backend delivery through _dispatch_core(event)
optional startup and shutdown resource handling
```

This keeps custom async sinks focused on backend behavior instead of reimplementing lifecycle, queueing, thread handoff, and package-managed runtime creation.
