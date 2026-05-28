# AsyncioLogSink

This page documents `AsyncioLogSink` as a reusable base for custom sinks that need asynchronous delivery behind the synchronous sink interface.

For the internal runtime design, lifecycle details, queueing model, dispatcher behavior, and shutdown mechanics, see the architecture article. This page focuses on the public API and subclass extension points.

## When to use it

Use `AsyncioLogSink` when a custom sink must keep `log(event)` fast and thread-safe, but actual backend delivery may require asynchronous work.

Typical use cases:

```text
network log collectors
remote services
databases
message queues
Redis streams
file or backend sinks with buffered delivery
```

A subclass normally implements:

```text
build_descriptor(...)
_dispatch_core(event)
```

and may override:

```text
_on_starting()
_on_stopped()
```

`AsyncioLogSink` provides the common runtime and package-managed `create()` implementation. The subclass remains responsible for describing its own sink identity through `build_descriptor()` and for delivering one event through `_dispatch_core(event)`.

## Relationship to sink contracts

`AsyncioLogSink` is a base for package-managed custom sinks.

It already provides:

```text
log(event)
create(...)
```

But it cannot provide a meaningful descriptor for every possible backend.

Therefore, subclasses that participate in package-level sink registration must implement:

```text
build_descriptor(...)
```

The descriptor should describe the backend resource and relevant configuration for that concrete sink.

For example:

```text
Redis sink
    resource key: redis URL, stream name
    config key: relevant delivery options

HTTP collector sink
    resource key: collector endpoint
    config key: headers, timeout policy, formatting options
```

The async base owns the runtime. The concrete subclass owns the sink identity and backend delivery.

## Lifecycle states

`AsyncioLogSinkState` describes the runtime state of an async sink instance.

```{eval-rst}
.. autoenum:: mvx.common.logger.AsyncioLogSinkState
```

## Queue overflow policy

`AsyncioLogSinkQueueOverflowPolicy` controls what happens when the accepted-event limit is reached.

```{eval-rst}
.. autoenum:: mvx.common.logger.AsyncioLogSinkQueueOverflowPolicy
```

## Lifecycle operations

`AsyncioLogSinkOp` identifies lifecycle operations reported by wait handles.

```{eval-rst}
.. autoenum:: mvx.common.logger.AsyncioLogSinkOp
```

`AsyncioLogSinkOpResult` is returned by `AsyncioLogSinkWaitHandle.wait()` and by awaiting a wait handle.

```{eval-rst}
.. autoclass:: mvx.common.logger.AsyncioLogSinkOpResult
   :class-doc-from: both
```

## Wait handle

`AsyncioLogSinkWaitHandle` is returned by `start()` and `stop()`.

It can be used from synchronous code through `wait()` or awaited from async code.

```{eval-rst}
.. autoclass:: mvx.common.logger.AsyncioLogSinkWaitHandle
   :members: wait
   :member-order: bysource
   :class-doc-from: both
```

## Base class

`AsyncioLogSink` is the base class for asynchronous sink implementations.

It implements the synchronous `log(event)` sink boundary while moving delivery to an internal asyncio runtime.

```{eval-rst}
.. autoclass:: mvx.common.logger.AsyncioLogSink
   :members: build_descriptor, get_status, start, stop, log, create, _on_starting, _dispatch_core, _on_stopped
   :member-order: bysource
   :class-doc-from: both
```

## Required subclass methods

A concrete package-managed async sink must implement two methods.

```text
build_descriptor(...)
    describe the sink resource and configuration for registry identity

_dispatch_core(event)
    deliver one accepted event to the backend
```

`build_descriptor()` is a class-level method used before sink creation by the package-level registry.

The base implementation raises `NotImplementedError`. This is intentional: `AsyncioLogSink` cannot know which backend resource or configuration values define descriptor identity for a concrete sink.

`_dispatch_core(event)` is the per-event delivery hook called by the dispatcher task.

The base class cannot implement it because backend delivery is specific to the concrete sink.

## Optional subclass hooks

Subclasses may override these lifecycle hooks:

```text
_on_starting()
    open backend resources before the dispatcher starts

_on_stopped()
    close backend resources after the dispatcher stops
```

Use `_on_starting()` for backend setup such as opening connections, creating clients, authenticating, or preparing handles.

Use `_on_stopped()` for backend cleanup such as closing clients, handlers, files, or network connections.

If no backend setup or cleanup is needed, the inherited no-op hooks are sufficient.

## Method ownership

A typical subclass should not override `log(event)`.

The base implementation owns:

```text
state checks
lazy startup
pending-event accounting
overflow handling
thread-safe queue handoff
```

A typical subclass should not override `create(...)` either.

The base `create()` implementation owns:

```text
dedicated event loop thread creation
sink bootstrap inside that loop
startup wait
terminator creation
runtime shutdown
```

A concrete subclass normally supplies only:

```text
build_descriptor(...)
_dispatch_core(event)
optional startup/shutdown hooks
```

## Package-managed creation

`AsyncioLogSink.create()` creates a package-managed async sink runtime.

It returns:

```text
sink
terminator
```

The sink is used through the normal `LogSinkProto` boundary.

The terminator stops the sink runtime and shuts down the dedicated event loop thread.

`create()` is common runtime machinery. `build_descriptor()` is still required on the subclass so that `configure_log_sink()` can detect idempotent registrations and configuration conflicts before creation.

## Direct construction

Direct construction binds the sink to the currently running event loop.

```python
sink = MyAsyncSink(...)
```

This form requires a running event loop in the current thread.

Direct construction is useful when the caller owns the event loop and wants the sink runtime to live there.

Package-level registration usually uses `create()` instead, because `create()` builds a dedicated event loop thread and returns an idempotent terminator.

## Custom subclass shape

A concrete async sink usually has this shape:

```python
from typing import Any

from mvx.common.logger import AsyncioLogSink, LogEvent, LogSinkDescriptor


class CustomAsyncSink(AsyncioLogSink):
    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor:
        return LogSinkDescriptor(
            sink_type="custom",
            resource_key=("custom", kwargs["target"]),
            config_key=(),
        )

    async def _on_starting(self) -> None:
        ...

    async def _dispatch_core(self, event: LogEvent) -> None:
        ...

    async def _on_stopped(self) -> None:
        ...
```

Only `_dispatch_core()` and `build_descriptor()` are conceptually required for a concrete package-managed async sink.

The lifecycle hooks are optional and depend on backend needs.

## Error hierarchy

`AsyncioLogSink` has its own error family.

```{eval-rst}
.. autoclass:: mvx.common.logger.AsyncioLogSinkError

.. autoenum:: mvx.common.logger.AsyncioLogSinkErrorReason

.. autoclass:: mvx.common.logger.AsyncioLogSinkEventLoopUnavailableError

.. autoclass:: mvx.common.logger.AsyncioLogSinkInvalidStateError
   :class-doc-from: both

.. autoclass:: mvx.common.logger.AsyncioLogSinkOnStartingHookFailedError
   :class-doc-from: both

.. autoclass:: mvx.common.logger.AsyncioLogSinkOnStoppedHookFailedError
   :class-doc-from: both

.. autoclass:: mvx.common.logger.AsyncioLogSinkQueueOverflowError

.. autoclass:: mvx.common.logger.AsyncioLogSinkDispatcherCancelledError

.. autoclass:: mvx.common.logger.AsyncioLogSinkUnexpectedError
   :class-doc-from: both
```

