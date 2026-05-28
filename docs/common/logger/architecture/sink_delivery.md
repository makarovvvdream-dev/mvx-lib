# Sink delivery

```{contents} Contents:
:depth: 1
:local:
```

## Overview

A sink is the delivery boundary of `MVX Logger`.

Everything before the sink belongs to event preparation:

```text
LogContext
   |
   v
LogEventMeta
   |
   v
event policy
   |
   v
payload processor
   |
   v
LogEvent
```

Everything after that belongs to delivery:

```text
LogEvent
   |
   v
LogSinkProto.log(event)
   |
   v
stream, file, queue, external backend, or custom destination
```

A sink receives a completed `LogEvent`. It does not create event metadata, does not decide whether an event is enabled, and does not normalize raw payload values.

## Why it exists

The logger separates event creation from event delivery.

Code that emits events should not care whether events go to `stderr`, a file, Redis, PostgreSQL, syslog, an HTTP collector, or another backend.

The emitting code works with `LogContext`:

```python
ctx.log_info_event(
    event="app.started",
    payload={"service": "demo"},
)
```

The context prepares the event. The sink delivers it.

This boundary lets projects replace delivery without changing the code that creates events.

## Minimal sink contract

The minimal sink contract is `LogSinkProto`:

```python
@runtime_checkable
class LogSinkProto(Protocol):
    def log(self, event: LogEvent) -> None: ...
```

This is deliberately small.

A sink only needs to accept a completed `LogEvent`.

That makes it possible to implement simple sinks directly:

```python
class PlainPrintSink:
    def log(self, event: LogEvent) -> None:
        print(event.meta.event_name, dict(event.payload))
```

Such a sink can be passed directly to `LogContext` or `configure_log_context()`.

## Sink responsibility

A sink is responsible for delivery.

Typical sink responsibilities include:

* formatting an accepted event for a concrete backend;
* writing the event to a stream;
* writing the event to a file;
* enqueueing the event;
* sending the event to an external service;
* managing backend-specific resources.

A sink should normally not own:

* event selection;
* payload normalization;
* domain-level interpretation of event names;
* namespace tree configuration.

Those responsibilities belong to the context, event policy, and payload processor.

## Completed event boundary

By the time a sink receives an event:

* `LogEventMeta` already exists;
* event policy has already accepted the event;
* payload has already been normalized unless normalization was explicitly skipped;
* the final `LogEvent` has already been created.

The sink boundary is therefore:

```text
completed structured event in -> backend-specific delivery out
```

This keeps sinks simple. They do not need to know how a payload was produced or why the event was accepted.

## Synchronous sinks

A synchronous sink performs delivery inside the `log()` call.

`StreamLogSink` is the built-in example.

It receives a `LogEvent`, converts it to a standard `logging.LogRecord`, and passes it to an internal standard-library logger.

Its public boundary remains the same:

```python
sink.log(event)
```

Synchronous sinks are suitable when delivery is expected to be quick and does not require a dedicated runtime.

Examples:

```text
stderr
stdout
in-memory test sink
small custom debug sink
```

A synchronous sink may still have internal locking or resource cleanup. The distinction is that delivery is performed as part of the caller's `log()` path.

## Asynchronous sinks

An asynchronous sink keeps the same public sink contract but separates the caller from actual delivery.

The caller still uses:

```python
sink.log(event)
```

but the sink can buffer the event and deliver it through its own runtime.

`FileLogSink` is the built-in example. It is backed by Python's standard logging package, but it uses `AsyncioLogSink` as its asynchronous delivery base.

This shape is useful when delivery may involve I/O or backend lifecycle:

```text
file writes
network calls
database writes
message queues
external collectors
```

The important point is that asynchronous delivery is hidden behind the same `LogSinkProto` boundary.

User code and `LogContext` do not need separate APIs for synchronous and asynchronous sinks.

## Package-managed sink classes

A sink object only needs `LogSinkProto`.

A sink class that participates in package-level registration must also implement `LogSinkClassProto`:

```python
@runtime_checkable
class LogSinkClassProto(Protocol):
    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor: ...

    @classmethod
    def create(cls, **kwargs: Any) -> tuple[LogSinkProto, LogSinkTerminator]: ...
```

This contract is used by `configure_log_sink()`.

`build_descriptor()` describes the sink registration request.

`create()` builds the sink instance and returns a terminator used for cleanup.

This separates three concerns:

```text
LogSinkProto         -> event delivery
LogSinkDescriptor   -> registration identity and conflict detection
LogSinkTerminator   -> cleanup
```

## Sink descriptor

`LogSinkDescriptor` describes a configured sink:

```python
@dataclass(frozen=True, slots=True)
class LogSinkDescriptor:
    sink_type: str
    resource_key: tuple[Any, ...]
    config_key: tuple[Any, ...] = ()
```

The descriptor is not the sink. It is the registry identity for a sink configuration.

It answers two questions:

```text
What resource does this sink target?
With which relevant configuration?
```

For example, a stream sink descriptor includes the sink type, logger name, stream target, level, format, date format, and filters.

A file sink descriptor includes the resolved file path and file-related configuration.

The descriptor lets the registry detect whether a repeated configuration request is compatible with an existing registered sink.

## Sink registry behavior

The package-level sink registry stores named sinks.

`configure_log_sink()` validates the sink name, asks the sink class to build a descriptor, and then checks the registry.

The behavior is:

```text
name not registered
    -> create sink, store sink + terminator + descriptor, return sink

name already registered with same descriptor
    -> return existing sink

name already registered with different descriptor
    -> raise LogSinkConfigurationConflictError
```

This makes repeated setup idempotent when the request is the same and explicit when the same name is reused for a different sink.

The registry does not silently replace sinks.

Silent replacement would be dangerous because contexts may already hold references to the old sink.

## Sink cleanup

A package-managed sink is stored with a `LogSinkTerminator`:

```python
LogSinkTerminator = Callable[[], None]
```

The terminator is called when the sink is unregistered or when the logger is reset.

Built-in sinks use terminators to close or stop backend resources:

* `StreamLogSink` removes its installed handler;
* `FileLogSink` stops the asynchronous sink runtime and closes the file handler.

Terminators should be idempotent. Cleanup may be requested from package-level shutdown paths where repeated or defensive calls should not corrupt the sink state.

## Closing a configured sink

`close_log_sink(name)` removes a package-managed sink from the sink registry and calls its terminator.

There is one important safety rule.

A sink cannot be closed while it is locally assigned to a registered context.

The registry checks contexts whose local sink is the sink being closed. If any are found, `close_log_sink()` raises `LogSinkIsInUseError`.

This protects contexts from keeping a local reference to a closed sink.

The check is about local sink assignment. A context that only inherits a sink from its parent is not counted as locally using that sink.

## Direct sinks vs registered sinks

A sink can be used directly without package-level registration.

```python
ctx = LogContext(
    namespace="local.component",
    log_sink=PlainPrintSink(),
    payload_processor=LogPayloadProcessor(),
)
```

In this case:

* there is no sink name;
* there is no registry descriptor;
* there is no package-managed terminator;
* cleanup is owned by the code that created the sink.

This is useful for tests, embedded usage, and local library components.

Package-level registration is useful when the sink should be shared by named contexts and managed by the logger facade.

## Built-in delivery adapters

The built-in stream and file sinks are adapters over Python's standard `logging` package.

They convert `LogEvent` into `logging.LogRecord` using `make_log_record_from_event()`.

The standard logging configuration layer owns formatter, filters, levels, propagation, and handler setup. The sink remains responsible for delivering `LogEvent` objects through that configured backend.

This keeps the MVX event model separate from the standard-library formatting and handler system.

```text
MVX LogEvent
   |
   v
make_log_record_from_event(...)
   |
   v
logging.LogRecord
   |
   v
logging.Handler
```

## Sink and error handling

If sink delivery fails during `LogContext.emit_log_event()`, the context handles that infrastructure error according to `LogErrorHandlingPolicy`.

The policy is resolved from the context tree.

The available behaviors are:

```text
IGNORE        -> suppress logging infrastructure errors
PRINT_STDERR  -> report via last-resort stderr output
RAISE         -> raise LogContextUnableToLog
```

This error boundary belongs to `LogContext`, not to the sink protocol itself.

A sink may raise backend-specific errors. The context decides whether those errors are suppressed, printed, or raised to the caller as logger infrastructure errors.

## Sink and event filtering

A sink should not normally filter events.

Filtering in a sink happens too late in the pipeline:

```text
policy already accepted the event
payload normalization already ran
LogEvent already exists
```

If the goal is to control which events exist, use event policy.

If the goal is to control how much data accepted events contain, use payload processor configuration.

If the goal is to choose a delivery backend for a namespace, attach a different sink to the relevant context.

A sink may still apply backend-specific safety checks, such as refusing delivery after close, but that is not event policy.

## Design summary

Sinks are the final delivery boundary of the logger pipeline.

`LogContext` prepares accepted structured events.

`LogSinkProto` delivers completed `LogEvent` objects.

`LogSinkClassProto` lets sink classes participate in package-level registration.

`LogSinkDescriptor` gives the registry a stable identity for conflict detection.

`LogSinkTerminator` gives the registry a cleanup hook.

This keeps event selection, payload normalization, delivery, registration, and cleanup separate instead of folding them into one large logger object.
