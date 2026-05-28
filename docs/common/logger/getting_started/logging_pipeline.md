# Logging pipeline

```{contents} Contents:
:depth: 1
:local:
```
## Overview

`MVX Logger` processes every logging call through a fixed pipeline.

The pipeline starts when user code emits an event through `LogContext` and ends when a sink delivers a completed `LogEvent` to the destination logging system.

```text
     user code calls LogContext
                |
                v
   LogContext builds LogEventMeta
                |
                v
  event policy checks LogEventMeta
                |
                v
 payload processor normalizes payload
                |
                v
     LogContext creates LogEvent
                |
                v
      log sink delivers LogEvent
                |
                v
            destination
```

Each step has a separate responsibility. `LogContext` coordinates the process, but it does not perform all work itself. Event filtering, payload normalization, and event delivery are delegated to separate components attached to the `LogContext`.

## Step 1: user code emits an event

User code starts the pipeline by calling one of the logging methods on `LogContext`.

```python
from mvx.common.logger import configure_log_context

ctx = configure_log_context("my_app")

ctx.log_info_event(
    event="request.completed",
    payload={
        "status": 200,
        "duration_ms": 37,
    },
)
```

At this point, the input is not yet a `LogEvent`.

It is only a logging request that contains:

* event name;
* logging level;
* optional event metadata fields;
* raw payload;
* optional event type.

The code that emits the event does not decide where the event will be delivered and does not prepare the final event object manually.

## Step 2: LogContext builds event metadata

`LogContext` first prepares event metadata.

The metadata is represented by `LogEventMeta`.

It contains the fields used to identify and filter the event:

* event namespace;
* event name;
* entity id;
* source path;
* source line;
* source function.

The event namespace is usually taken from the context name, unless it is overridden explicitly.

For example, if the context is named `my_app.api` and the event name is `request.completed`, then the event belongs to the `my_app.api` logging area.

At this stage, payload is still not normalized.

## Step 3: event policy accepts or rejects the event

After metadata is prepared, `LogContext` checks the event policy configured for the context.

The event policy receives only `LogEventMeta`.

It may accept or reject the event by checking metadata fields such as namespace, event name, entity id, or source location.

The event policy does not inspect:

* payload;
* normalized payload;
* logging level;
* event type;
* timestamp;
* sink.

If the policy rejects the event, the pipeline stops here.

In that case:

* payload is not normalized;
* `LogEvent` is not created;
* nothing is passed to the sink.

If the policy accepts the event, the pipeline continues.

## Step 4: payload processor normalizes the payload

After the event is accepted, `LogContext` passes the payload to the configured payload processor.

The payload processor controls logging depth: how much data is placed into the payload and how that data is represented.

The default implementation, `LogPayloadProcessor`, can handle common payload normalization tasks:

* compact or detailed object representation;
* string length limits;
* collection item limits;
* object-provided `to_log_payload()` representation;
* external serialization adapters;
* structured error payloads;
* masking or shortening of large or sensitive values.

The result of this step is a normalized payload.

If the logging call uses `skip_payload_normalization=True`, this step is skipped and the provided payload is treated as already log-ready.

## Step 5: LogContext builds LogEvent

After metadata is accepted and payload is normalized, `LogContext` creates the final `LogEvent`.

A completed `LogEvent` contains:

* logging level;
* `LogEventMeta`;
* optional event type;
* timestamp;
* normalized payload.

The timestamp is assigned when the final event is created.

At this point, the logging request has become a completed structured event.

## Step 6: sink receives LogEvent

The completed `LogEvent` is passed to the configured sink.

The sink is responsible only for delivery.

It does not decide whether the event should be logged. That has already been handled by the event policy.

It does not normalize the payload. That has already been handled by the payload processor.

The sink receives the completed event and delivers it to its destination.

Examples:

```text
LogEvent -> StreamLogSink -> stderr
LogEvent -> FileLogSink -> file
LogEvent -> custom sink -> external backend
```

A sink may adapt `LogEvent` to another output shape, for example a standard `logging.LogRecord`, a JSON object, or a database row. This adaptation is part of delivery.

## Pipeline responsibilities

The pipeline separates responsibilities between components.

`LogContext` coordinates the pipeline:

* receives logging calls;
* builds event metadata;
* applies event policy;
* calls the payload processor;
* creates the final `LogEvent`;
* passes it to the sink.

Event policy controls logging width:

* which events are allowed into the log;
* which namespaces, event names, entities, or source locations are enabled.

Payload processor controls logging depth:

* how payload values are normalized;
* how much data is included;
* how objects and errors are represented.

Sink controls delivery:

* where the completed event goes;
* whether delivery is synchronous or asynchronous;
* how the event is adapted to the target output mechanism.

## What to remember

* The pipeline always starts with a logging call on `LogContext`.
* `LogEventMeta` is built before payload normalization.
* Event policy works before payload normalization and receives only metadata.
* Payload processor runs only for accepted events.
* `LogEvent` is created after metadata is accepted and payload is normalized.
* Sink receives only the completed `LogEvent`.
