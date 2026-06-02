# Direct LogContext usage

```{contents} Contents:
:depth: 1
:local:
```

## Overview

The package-level facade is the normal way to configure shared logger resources:

```python
configure_log_sink(...)
configure_log_context(...)
get_log_context(...)
```

Direct `LogContext` usage is the lower-level alternative.

Instead of registering a named context in the package-level registry, application or library code creates a context object directly:

```python
from mvx.common.logger import LogContext, LogPayloadProcessor


sink = MySink()
processor = LogPayloadProcessor()

ctx = LogContext(
    namespace="my.component",
    log_sink=sink,
    payload_processor=processor,
)
```

This context is fully usable. It can emit events, normalize payloads, apply its local event policy, and deliver events through its sink.

The difference is ownership: a directly created context is owned by the code that created it, not by the package-level context registry.

## When to use direct contexts

Use direct `LogContext` construction when package-level registration is not the right ownership model.

Good cases:

```text
embedded library components
isolated tests
local diagnostic pipelines
applications that want explicit object ownership
components that should not modify global logger state
```

Direct construction is especially useful in reusable libraries.

A library can accept or create a local context without registering package-level names, resetting global logger state, or closing sinks it does not own.

## When to use the package-level facade instead

Use the package-level facade when the context or sink should be shared by name.

Good cases:

```text
application-wide logger setup
named namespace configuration
shared sinks
package-managed lifecycle
central reset/close behavior
```

For example:

```python
sink = configure_log_sink(
    name="stderr",
    sink_cls=StreamLogSink,
)

ctx = configure_log_context(
    "my.app",
    log_sink=sink,
)
```

This registers package-level resources that can be retrieved later.

Direct contexts do not appear in that registry.

## Root context

A directly created root context has no parent.

It must receive both mandatory runtime components:

```text
log sink
payload processor
```

Example:

```python
from mvx.common.logger import LogContext, LogPayloadProcessor


ctx = LogContext(
    namespace="my.component",
    log_sink=sink,
    payload_processor=LogPayloadProcessor(),
)
```

A root context cannot reset its sink or payload processor, because it has no parent fallback.

## Child contexts

A child context can be created directly by passing a parent context.

```python
child_ctx = LogContext(
    namespace="my.component.worker",
    parent=ctx,
)
```

The child context inherits selected runtime components from its parent:

```text
sink
payload processor
logging error handling policy
```

It may also define local overrides:

```python
child_ctx = LogContext(
    namespace="my.component.worker",
    parent=ctx,
    event_policy=worker_policy,
)
```

Event policy is local to the context. It is not inherited from the parent.

## Emitting events

A directly created context uses the same event pipeline as a package-managed context.

```python
ctx.log_info_event(
    event="component.started",
    payload={"component": "demo"},
)
```

The context performs the normal pipeline:

```text
build LogEventMeta
   |
   v
check local event policy
   |
   v
normalize payload
   |
   v
build LogEvent
   |
   v
emit through effective sink
```

Direct usage does not bypass event policy, payload processing, or sink delivery.

It only bypasses package-level registry ownership.

## Ownership and lifecycle

Direct contexts are not tracked by the package-level registry.

This means:

```text
get_log_context_namespaces() does not list them
get_log_context(...) cannot retrieve them
reset_log_contexts() does not remove them
reset_logger() does not reset them as contexts
```

The code that creates a direct context is responsible for its ownership model.

The same applies to sinks created directly.

If the sink has resources that must be closed, the code that owns the sink must close them.

Package-level sink lifecycle management only applies to package-managed sinks created through `configure_log_sink()`.

## Direct context with package-managed sink

A direct context can still use a package-managed sink.

```python
sink = configure_log_sink(
    name="stderr",
    sink_cls=StreamLogSink,
)

ctx = LogContext(
    namespace="local.component",
    log_sink=sink,
    payload_processor=LogPayloadProcessor(),
)
```

This is valid, but ownership is mixed:

```text
sink
    owned by package-level sink registry

context
    owned directly by caller
```

Be careful with lifecycle. Closing the package-managed sink affects the direct context because it holds the same sink object.

The package-level registry cannot know that an unregistered direct context is using that sink.

## Direct context in libraries

Direct contexts are useful for library design.

A reusable library should usually avoid changing package-level logger state without an explicit request from the application.

A good library pattern is to accept a context-like object from the caller:

```python
from mvx.common.logger import LogContextProto


class Client:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context
```

The application decides whether that context is package-managed or directly created.

The library just uses the context it was given.

## Direct context and log_invocation

`log_invocation` does not require the concrete `LogContext` class.

It requires a `LogContextProto`-compatible object.

A directly created `LogContext` satisfies that protocol, so it can be used with decorated public API methods.

```python
from mvx.common.logger import LogContextProto, log_invocation


class Client:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context

    @log_invocation("connect")
    async def connect(self) -> None:
        ...
```

The decorator resolves the context through `get_log_context()` and emits operation outcomes through that context.

## Testing pattern

Direct contexts are convenient in tests because they avoid package-level registry state.

A common pattern is to use a small in-memory sink:

```python
from threading import RLock

from mvx.common.logger import LogContext, LogEvent, LogPayloadProcessor


class ListSink:
    def __init__(self) -> None:
        self._lock = RLock()
        self.events: list[LogEvent] = []

    def log(self, event: LogEvent) -> None:
        with self._lock:
            self.events.append(event)


sink = ListSink()
ctx = LogContext(
    namespace="test",
    log_sink=sink,
    payload_processor=LogPayloadProcessor(),
)
```

The test can then assert against collected `LogEvent` objects without touching global logger state.

## What direct usage does not provide

* Direct `LogContext` construction does not provide package-level naming.
* It does not register the context.
* It does not create a package-managed sink terminator.
* It does not make `reset_logger()` responsible for the context.
* It does not prevent the caller from building inconsistent ownership relationships.

Direct usage gives explicit control. It also gives explicit responsibility.

## Design summary

Direct `LogContext` usage is the object-owned alternative to package-level configuration.

Use it when local ownership is clearer than registry ownership.

Use the package-level facade when you want named shared resources and package-managed lifecycle.

The logging pipeline is the same in both cases. The difference is how contexts and sinks are owned, discovered, and cleaned up.
