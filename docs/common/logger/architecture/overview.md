# Architecture

```{contents} Contents:
:depth: 1
:local:
```

## Overview

`MVX Logger` is built as a structured event logging pipeline.

The public surface is intentionally small: user code usually works with `LogContext`, `log_invocation`, and configured sinks. Internally, the logger is split into small components with narrow responsibilities:

* `LogContext` coordinates logging;
* `LogEventMeta` describes an event before payload normalization;
* `LogEventPolicyProto` decides whether an event is enabled;
* `LogPayloadProcessorProto` normalizes accepted payloads;
* `LogEvent` represents the completed structured event;
* `LogSinkProto` delivers the completed event;
* package-level registries keep named contexts and sinks available through the public facade.

The important design point is that these components are not just implementation details. Most of them are public protocols or public classes and can be used directly when the full package-level wiring is not needed.

## Architectural goal

The logger is designed to separate four questions that are often mixed together in application logging code.

First, **where does the event belong?**

This is answered by `LogEventMeta`: namespace, event name, entity id, and source metadata.

Second, **should the event be logged?**

This is answered by the event policy. The policy receives metadata only and controls logging width.

Third, **how much data should be included?**

This is answered by the payload processor. The processor controls logging depth and turns raw values into log-ready values.

Fourth, **where does the event go?**

This is answered by the sink. The sink receives a completed `LogEvent` and delivers it to a stream, file, queue, external service, or another backend.

Because these questions are separated, changing one part does not require rewriting the others. A project may replace the sink without changing event policies. It may replace payload normalization without changing `log_invocation`. It may use a local `LogContext` without registering a global package-level context.

## Main runtime pipeline

At runtime, a regular manual logging call follows this path:

```text
user code
   |
   v
LogContext.log_event(...)
   |
   v
build LogEventMeta
   |
   v
check event policy
   |
   v
normalize payload
   |
   v
build LogEvent
   |
   v
sink.log(event)
```

`LogContext` coordinates the pipeline, but it does not own all decisions.

It builds metadata, asks the policy whether the event is enabled, asks the payload processor to normalize accepted payloads, creates the final `LogEvent`, and passes that event to the configured sink.

If the policy rejects the event, payload normalization does not run and no `LogEvent` is created.

## Event model

The event model has two layers.

`LogEventMeta` is the selection layer. It contains the data needed before an event is accepted:

* `event_namespace`;
* `event_name`;
* `entity_id`;
* `source_path`;
* `source_line`;
* `source_func`.

`LogEvent` is the delivery layer. It contains the completed event:

* `level`;
* `meta`;
* `event_outcome`;
* `timestamp`;
* `payload`.

This split is deliberate. Event selection happens before payload normalization. A policy can reject an event cheaply, without forcing expensive object serialization or deep payload processing.

## Context tree

`LogContext` is the main coordination object.

A context may be a root context or a child context. The root context owns mandatory infrastructure: a sink and a payload processor. Child contexts may override selected parts locally or inherit them from their parent.

A context can locally define:

* log sink;
* event policy;
* payload processor;
* log error handling policy.

If a child context does not define a sink or payload processor, it resolves the value from its parent. This makes namespace-based configuration possible without duplicating the full logger setup for every namespace.

For example, a project can keep one root sink, attach a more restrictive policy to `my_app.noisy`, and attach a more verbose payload processor to `my_app.diagnostics`.

## Package-level facade and registries

The package-level API provides a convenient global facade over two internal registries:

* a sink registry;
* a context registry.

The sink registry stores named sinks configured through `configure_log_sink()`. It uses `LogSinkDescriptor` to detect whether repeated configuration requests describe the same sink or a conflicting sink.

The context registry stores named `LogContext` instances. When `configure_log_context("a.b.c")` is used, the registry creates the missing namespace chain:

```text
<root>
   |
   v
a
   |
   v
a.b
   |
   v
a.b.c
```

Only the requested leaf context receives explicitly supplied configuration. Intermediate contexts are created as structural parents unless they already exist.

This facade is useful for normal application setup, but it is not the only possible integration style. A user may also instantiate `LogContext` directly and pass a sink and payload processor without using the package-level registries.

## Sink boundary

A sink implements `LogSinkProto`:

```python
class LogSinkProto(Protocol):
    def log(self, event: LogEvent) -> None: ...
```

The sink boundary is intentionally small. A sink receives a completed `LogEvent`; it does not decide whether the event is enabled and does not normalize payloads.

This allows simple synchronous sinks and more complex asynchronous sinks to share the same interface.

Examples:

```text
LogEvent -> StreamLogSink -> stderr
LogEvent -> FileLogSink -> file
LogEvent -> custom sink -> Redis
LogEvent -> custom sink -> HTTP collector
```

Package-managed sinks use a class-level factory contract:

```python
class LogSinkClassProto(Protocol):
    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor: ...

    @classmethod
    def create(cls, **kwargs: Any) -> tuple[LogSinkProto, LogSinkTerminator]: ...
```

`build_descriptor()` describes the target resource and configuration. `create()` returns the actual sink and a terminator callable used when the sink is closed or the logger is reset.

## Async sink foundation

`AsyncioLogSink` is a base class for sinks that need asynchronous delivery while preserving the synchronous `LogSinkProto.log(event)` boundary.

It is useful for backends where delivery may involve network I/O, batching, flushing, or connection lifecycle management.

The core idea is:

```text
caller thread
   |
   v
sink.log(event)
   |
   v
schedule enqueue on sink event loop
   |
   v
async dispatcher consumes queue
   |
   v
_dispatch_core(event)
```

Subclasses implement `_dispatch_core(event)` and may override lifecycle hooks:

* `_on_starting()` for opening connections or preparing resources;
* `_dispatch_core(event)` for delivering one event;
* `_on_stopped()` for closing resources.

`AsyncioLogSink` owns the common mechanics around state transitions, lazy startup, queueing, overflow handling, flushing, stopping, and dispatcher failure mapping. A custom Redis, PostgreSQL, HTTP, or syslog sink should usually implement only backend-specific delivery logic.

## Error boundary

Logging must not accidentally take down domain code unless the application explicitly asks for that behavior.

`LogContext` handles sink failures according to `LogErrorHandlingPolicy`:

* `IGNORE` suppresses logging infrastructure errors;
* `PRINT_STDERR` reports the first repeated infrastructure error to stderr;
* `RAISE` wraps the failure in `LogContextUnableToLog` and raises it.

This policy applies to errors that happen while delivering a completed log event. It does not change domain exceptions raised by the code being logged.

`log_invocation` has its own operation-level behavior for failed and cancelled calls. It converts operation outcomes into structured events, while `LogContext` remains responsible for the actual logging pipeline and infrastructure error handling.

## Extension points

The architecture exposes several extension points.

Use `LogEventPolicyProto` when the main question is which events should be enabled.

Use `LogPayloadProcessorProto` or `LogPayloadProcessor` configuration when the main question is how values should be represented in payloads.

Use `LogPayloadProvider` when a domain object should provide its own logging representation.

Use a type-based log adapter when the object cannot or should not implement `to_log_payload()` directly.

Use `LogSinkProto` for direct custom sinks.

Use `LogSinkClassProto` when the sink should participate in package-level sink registration.

Use `AsyncioLogSink` when a sink needs asynchronous delivery behind the synchronous sink interface.

## What to read next

```{toctree}
:maxdepth: 1
:caption: Architecture topics

event_model
context_tree
policy_and_payload_separation
pattern_event_policy
sink_delivery
asyncio_log_sink
package_bootstrap
error_boundaries
thread_safety
```


 
 