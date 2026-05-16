# Sinks

```{contents} Contents:
:depth: 1
:local:
```

A `log sink` is responsible for event delivery.

User code creates an event through `LogContext`. The context decides whether this event should be logged and passes it to the configured sink.

After that, the sink's job is to deliver the event to its destination.

```text
user code -> LogContext -> log sink -> destination
```

The destination may be `stderr`, `stdout`, a file, or another backend.

A sink should not decide whether an event is important. That is the job of the context and the event policy.

A sink should not know the business meaning of an event. It receives a `LogEvent` and delivers it.

## Why sinks exist

Separating the context from the sink keeps two different responsibilities apart.

`LogContext` is responsible for the user-facing side of logging:

* logging namespace;
* event selection;
* payload settings;
* logging error handling;
* sink selection.

A `log sink` is responsible only for delivery:

* write an event to a stream;
* write an event to a file;
* send an event to an external backend;
* buffer an event and deliver it later.

Because of this separation, the code that emits events does not depend on the concrete delivery destination.

Today a context may write to `stderr`, tomorrow to a file, and later to Redis or PostgreSQL. The code that calls `ctx.log_info_event(...)` does not change.

## Synchronous and asynchronous sinks

A sink may be synchronous or asynchronous.

A synchronous sink delivers an event in the same thread in which the logging method was called.

This model is suitable for simple and fast delivery, for example writing to a standard stream.

An asynchronous sink separates the calling code thread from the actual event delivery.

The calling code passes an event to the sink and continues working. The sink buffers the event internally and delivers it to its destination through its own runtime.

This model is needed when delivery may involve I/O waits: file writes, network access, a database, an external service, or another slow backend.

In `MVX Logger`, this distinction is important: logging code should not deal with queues, threads, network waits, or asynchronous delivery management.

## Available sinks

The first release provides two basic sink types:

* `StreamLogSink`;
* `FileLogSink`.

They cover the most familiar scenarios where logging usually starts: writing to a stream and writing to a file.

## StreamLogSink

`StreamLogSink` is a synchronous sink for writing events to a standard stream.

It is based on Python's standard `logging` ecosystem and can write to:

* `stderr`;
* `stdout`.

The default `MVX Logger` initialization uses `StreamLogSink` configured for `stderr`.

That is why the first event can be written without additional setup:

```python
from mvx.common.logger import configure_log_context

ctx = configure_log_context("my_app")

ctx.log_info_event(
    event="app.started",
    payload={
        "service": "demo",
    },
)
```

The event will be delivered to `stderr`.

`StreamLogSink` is useful for the first run, console debugging, CLI scenarios, and cases where ordinary stream output is enough.

## FileLogSink

`FileLogSink` is a sink for writing events to a file.

It is also based on Python's standard `logging` ecosystem, but it is built on top of the asynchronous sink infrastructure of `MVX Logger`.

This means that the calling code passes an event to the sink, while the actual file write is performed separately inside the sink runtime.

This approach is used because writing to a file is an I/O operation. It may be fast, but it still should not become the responsibility of the code that emits the event.

For the user, it remains as simple as ordinary logging: the context receives an event, and the sink delivers it to a file.

## How a context gets a sink

A context can get a sink in two ways.

The first way is to inherit it from the parent context.

This is what happens in the simplest scenario:

```python
from mvx.common.logger import configure_log_context

ctx = configure_log_context("my_app")
```

If the `my_app` context does not receive its own sink, it uses the sink inherited from the root context. In the basic setup, this is `StreamLogSink` writing to `stderr`.

The second way is to pass a sink explicitly when configuring the context.

```python
from mvx.common.logger import configure_log_context

ctx = configure_log_context(
    "my_app",
    log_sink=my_sink,
)
```

After that, events from the `my_app` context will be passed to `my_sink`.

Child contexts can inherit this sink if they do not have their own.

## Registering a sink

Before assigning a sink to a context, it is usually registered through `configure_log_sink()`.

```python
from mvx.common.logger import configure_log_sink

sink = configure_log_sink(
    name="my_sink",
    sink_cls=SomeLogSink,
    config=some_config,
)
```

`configure_log_sink()` creates a sink with the specified name and returns the ready-to-use sink object.

After that, it can be passed to `configure_log_context()`:

```python
from mvx.common.logger import configure_log_context

ctx = configure_log_context(
    "my_app",
    log_sink=sink,
)
```

This explicitly connects the context and the sink.

## What to remember

A sink is an event delivery mechanism.

The context decides which events should be sent and with which settings.

The sink receives a completed event and delivers it to its destination.

A synchronous sink delivers an event in the caller's thread.

An asynchronous sink buffers an event and delivers it through its own runtime.

The code that calls `ctx.log_info_event(...)` should not depend on which sink is used behind the context.

## What to read next

```{toctree}
:maxdepth: 1
:caption: Sink usage

stream_sink
file_sink
custom_sinks
```
