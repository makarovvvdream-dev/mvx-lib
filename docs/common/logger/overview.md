# Logger
```{contents} Contents:
:depth: 1
:local:
```
`MVX Logger` is a lightweight-to-use structured event logging layer for Python code.

It exists because there is a practical gap between simple logging helpers and full observability platforms.

Python's standard `logging` package is mature, stable, and very useful as an output infrastructure. However, its core model is centered around `LogRecord` objects and formatted messages. Many structured logging libraries improve the output shape, for example by producing JSON or attaching additional fields, but they often leave the larger problem unresolved: operation lifecycle logging, event selection, payload depth, domain object serialization, and event delivery remain scattered across user code.

At the other end, full observability stacks provide powerful capabilities, but they usually require infrastructure, configuration, collectors, backends, operational decisions, and additional expertise. For many projects, this is often too heavy as a required foundation.

`MVX Logger` is designed for the middle ground.

It should be small at the point of use: get a `LogContext`, attach `log_invocation`, configure a sink. Internally, however, it is built around a formal event model rather than formatted strings.

## Core idea

In `MVX Logger`, code does not format log strings, instead it emits structured events.

An event has the following fields:

- namespace;
- event name;
- event type;
- level;
- timestamp;
- entity id;
- source metadata;
- optional payload.

The payload may be minimal or detailed. It may contain operation arguments, a result, an error, object state, or any other data that is meaningful for a particular event.

The important distinction is that an event is not tied to its destination. The same `LogEvent` may be written to stderr, written to a file, sent to Redis, stored in PostgreSQL, delivered to syslog, or forwarded to an external collector. The code that creates the event does not know where the event will go.

## Logging width and depth

`MVX Logger` separates two different questions: **which events should be logged** and **how much detail should be included in them**.

### Width

Logging width answers the question:

> Which events should be written to the log?

This is controlled by policies.

A policy may allow or reject an event by namespace, event name, event type, level, or any other rule. This makes it possible to control which parts of a library are verbose, which parts are quiet, and which parts are enabled only in diagnostic mode.

For example, a policy may log only errors, enable the full lifecycle of operations, or keep `failed` and `cancelled` events while suppressing noisy `invoke` and `success` events.

### Depth

Logging depth answers a different question:

> How much data should be placed into the event payload?

This should not be decided somewhere deep inside the logger. Objects themselves usually know best how to represent their state safely and usefully in logs.

For this reason, `MVX Logger` provides several layers of depth control:

* `verbosity_level`;
* string length limits;
* collection item limits;
* object serialization through `to_log_payload()`;
* serialization adapters;
* dedicated error serialization;
* masking or shortening of sensitive and large values.

The idea is that the logger should not know the internal structure of an LDAP response, a network transport outcome, a schema descriptor, or a domain error. An object may provide a compact or detailed representation of itself, while `LogContext` applies the common normalization policy.

## LogContext as the entry point

The main entry point into the logging infrastructure is `LogContext`.

If the standard `logging.Logger` is the usual entry point for message-based logs, then `LogContext` is the MVX entry point for structured event logs.

`LogContext` is responsible for several tasks:

* accepting an event;
* applying the event selection policy;
* normalizing the payload;
* applying payload depth rules;
* handling logging errors according to policy;
* passing the event to a sink.

At the same time, `LogContext` keeps the familiar idea of namespaces and inheritance. A context can be retrieved by name, specific namespaces can be configured, and the logger works with a default bootstrap state out of the box.

User code does not have to assemble the whole logging infrastructure manually before the first use. A base context exists immediately, and more specific configuration can be added later.

## Thread-safe by design

`MVX Logger` is designed to be safe to use from multiple threads.

This matters because the logger may be called from synchronous code, asynchronous code, worker threads, background tasks, callbacks, or cleanup paths. The code that emits an event should not have to coordinate global logging state manually.

The public logging path is built around this expectation: code creates or hands off a structured event, and the logging infrastructure coordinates context access, sink registration, and sink delivery boundaries.

For simple sinks, delivery may happen synchronously. For asynchronous sinks, the caller can hand off an event quickly, while the sink performs buffering and delivery inside its own runtime. In both cases, the user-facing contract is the same: logging code should not need to know which thread or delivery mechanism is behind the sink.

## Sinks only deliver events

`MVX Logger` deliberately separates event creation from event delivery.

The code that logs does not know where the event will be sent.

`LogContext` does not know how exactly the sink delivers the event.

A sink does not know why the event was created or which domain operation it describes.

A sink receives a completed `LogEvent` and delivers it.

A simple sink may synchronously write the event to a stream or a file. A more complex sink may buffer events and deliver them asynchronously to PostgreSQL, Redis, syslog, an HTTP endpoint, or another external backend.

This allows the delivery mechanism to change without changing the code that creates events.

## Asynchronous sinks as a growth path

One of the important goals of `MVX Logger` is to provide a foundation for complex sinks that cannot simply write data synchronously in the caller's thread.

Examples include:

* PostgreSQL sink;
* Redis sink;
* syslog over TCP/TLS;
* HTTP exporter;
* remote collector;
* batch writer.

For these scenarios, the logging code should remain fast. It hands off an event synchronously and continues working. Everything else is handled by the sink: buffering, backpressure, delivery, flush, stop, and error handling.

This is especially important for async libraries. Logging should not turn domain code into a mix of business logic, network I/O, and manual queue management.

## log_invocation

`log_invocation` is one of the main practical tools provided by `MVX Logger`.

It allows operation logging to be described declaratively, through a decorator, instead of turning a function body into a sequence of manual `log()` calls.

A typical operation has a lifecycle:

* it is invoked;
* it completes successfully;
* it fails with an error;
* it is cancelled.

`log_invocation` turns this lifecycle into standardized events:

* `invoke`;
* `success`;
* `failed`;
* `cancelled`.

It can be configured to control:

* which function arguments are included in the payload;
* which values are taken from context or closure;
* whether the result should be logged;
* how errors should be serialized;
* which level should be used for each event;
* how payload generation is tied to `verbosity_level`;
* when a custom payload builder should be used.

The function body remains clean. It performs its own work instead of manually assembling log records.

Example:

```python
from mvx.common.logger import log_invocation
from dataclasses import dataclass

@dataclass
class BindOutcome:
    result: bool
    error: Exception | None

@log_invocation(
    event="ldap.bind",
    log_kwargs_on_invoke=("user_dn",),
    log_result_on_success=("ok=result",),
)
async def bind(self, *, user_dn: str, password: str) -> BindOutcome:
    ...
```

The function does not know where the events will be delivered. It does not know whether they will be logged at all. It does not know how detailed the serialized result will be.

It simply performs the operation.

The event policy decides whether the event is logged. Payload serialization rules decide how deep the payload should be. The sink decides how to deliver the final event.

## What MVX Logger is not

- `MVX Logger` is not a replacement for the standard `logging` package. It uses standard `logging` as one delivery mechanism.
- `MVX Logger` is not just a JSON formatter. Its primary unit is an event, not a formatted message wrapped into a structured output format.
- `MVX Logger` is not a full observability platform. It does not require a collector, a separate backend, an agent, a deployment model, or external infrastructure.

`MVX Logger` is an event logging layer for regular Python projects. Its purpose is to standardize how code creates events, how logging width is selected, how payload depth is controlled, and how delivery is separated from the code that logs.


```{toctree}
:caption: What to read next
:maxdepth: 1


getting_started/index
```