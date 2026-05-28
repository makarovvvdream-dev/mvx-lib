# Custom sinks

```{contents} Contents:
:depth: 1
:local:
```

`MVX Logger` is not limited to the built-in sinks.

`StreamLogSink` and `FileLogSink` cover the basic scenarios: writing to a standard stream and writing to a file. But the delivery model itself is not tied to streams or files.

A sink is a replaceable component that receives a `LogEvent` and delivers it to its destination.

This means that a project can implement its own sink for another event delivery mechanism.

For example:

* Redis;
* PostgreSQL;
* syslog;
* HTTP endpoint;
* message broker;
* external log collector;
* custom internal backend.

This page does not explain how to write a custom sink. The important point here is to show that this possibility exists and where custom sinks fit into the `MVX Logger` ecosystem.

## Why custom sinks are possible

`MVX Logger` separates event creation from event delivery from the very beginning.

Application code works with `LogContext`.

`LogContext` creates a `LogEvent` and passes it to a sink.

The sink delivers the event.

```text
user code -> LogContext -> custom sink -> destination
```

As long as a sink can accept a `LogEvent`, the rest of the logging infrastructure does not need to know where exactly the sink sends it.

This is what makes it possible to replace delivery without changing the code that emits events.

## What already exists in the package

The package already provides the basic building blocks for custom sinks.

At the lowest level, there is `LogSinkProto`. This is the minimal sink contract: an object must be able to accept a `LogEvent` through the `log()` method.

For sinks that are created and registered through `configure_log_sink()`, there is a class-level contract: the sink class must be able to build a descriptor and create a sink instance.

The descriptor describes the sink type, its resource, and its configuration. It is used by the logging infrastructure for registration and conflict control between sinks.

For complex asynchronous sinks, the package provides `AsyncioLogSink`.

This is an abstract base for sinks that need their own delivery runtime: an event queue, a dedicated event loop, a dedicated thread, startup, shutdown, and graceful termination.

This means that future sinks such as Redis, PostgreSQL, syslog, or an HTTP exporter do not need to reinvent the general mechanics of asynchronous delivery.

A custom sink only needs to implement its own part: delivering the event to a specific backend.

## Synchronous or asynchronous custom sink

A custom sink can be synchronous or asynchronous.

A synchronous sink is suitable when delivery is very simple and does not involve waiting for external I/O.

An asynchronous sink is needed when delivery may wait for a file, network, database, external service, or another backend.

This is the reason `AsyncioLogSink` exists.

It separates the code that emits an event from the runtime that performs the actual delivery.

## What remains unchanged

A custom sink does not change the user-facing logging model.

Code still gets a context and emits events:

```python
from mvx.common.logger import configure_log_context

ctx = configure_log_context("my_app")

ctx.log_info_event(
    event="order.created",
    payload={
        "order_id": "order-42",
    },
)
```

Only the sink attached to the context changes.

Today it may be `StreamLogSink`, tomorrow `FileLogSink`, and later a custom sink for Redis or PostgreSQL.

The code that creates the event does not change.
