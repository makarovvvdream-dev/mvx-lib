# Logging event (LogEvent)

```{contents} Contents:
:depth: 1
:local:
```

`LogEvent` is the final structured event produced by the logging pipeline.

User code usually does not create `LogEvent` directly. It calls a `LogContext` method and passes event input: an event name, level, optional metadata fields, and payload.

After the event is accepted and the payload is prepared, `LogContext` creates a `LogEvent` and passes it to the configured sink.

```text
     user code calls LogContext
                |
                v
   LogContext builds LogEventMeta
                |
                v
  event policy accepts the metadata
                |
                v
 payload processor normalizes payload
                |
                v
     LogContext creates LogEvent
                |
                v
       sink receives LogEvent
```

At this point, the original logging call is no longer just a request. It has become a complete structured object ready for delivery.

## What LogEvent represents

A `LogEvent` is not a formatted log line.

It is a structured object that keeps event identity, metadata, level, timestamp, and payload as separate fields.

This is important because different parts of the logger infrastructure work with different parts of the event:

* event policy works with metadata before the final event is created;
* payload processor prepares the payload;
* sink receives the completed `LogEvent` and delivers it;
* formatter or adapter may later turn the event into text, JSON, a database row, or another output shape.

The code that emits an event does not need to know how the event will be delivered or formatted.

## LogEvent and LogEventMeta

`MVX Logger` separates event metadata from event data.

Event metadata is represented by `LogEventMeta`.

It contains information used to identify and filter the event:

* event namespace;
* event name;
* entity id;
* source path;
* source line;
* source function.

The final `LogEvent` contains the completed event data:

* logging level;
* `LogEventMeta`;
* optional event type;
* timestamp;
* normalized payload.

In code, the model is intentionally small:

```python
from dataclasses import dataclass
from typing import Any
from collections.abc import Mapping

@dataclass(frozen=True, slots=True)
class LogEventMeta:
    event_namespace: str | None
    event_name: str
    entity_id: str | None
    source_path: str | None
    source_line: int | None
    source_func: str | None


@dataclass(frozen=True, slots=True)
class LogEvent:
    level: int
    meta: LogEventMeta
    event_outcome: str | None
    timestamp: float
    payload: Mapping[str, Any]
```

This article explains the role of these objects in the pipeline. Detailed field-by-field behavior belongs to the API reference.

## Metadata

Metadata describes where the event belongs and how it can be selected before payload normalization.

The most important metadata fields are:

* `event_namespace` — the logging area of the event, usually taken from the `LogContext` name;
* `event_name` — the name of the event itself;
* `entity_id` — an optional runtime entity related to the event;
* source fields — optional source path, line, and function information.

Event policy receives this metadata and decides whether the event is allowed into the log.

The policy does not need the payload for this decision. This keeps event filtering cheap and independent from payload preparation.

## Event data

Event data describes the completed log event after it has been accepted.

It includes:

* `level` — the logging level, usually one of the `LogLevel` values;
* `event_outcome` — an optional outcome of the event (success, failure, etc.);
* `timestamp` — the event creation time;
* `payload` — the normalized event data.

The timestamp is assigned when `LogContext` creates the final `LogEvent`.

The payload is normally normalized by the configured payload processor before the event is created. If the payload has already been prepared manually, normalization can be skipped with `skip_payload_normalization=True`.

## Metadata and payload separation

`LogEventMeta` and `payload` are intentionally separate.

Metadata answers:

```text
Where does this event belong?
Which event is it?
Which entity or source location is it associated with?
```

Payload answers:

```text
What data does this event carry?
```

This separation is important for the pipeline.

Event policy works before payload normalization and receives only metadata. If the policy rejects the event, the payload is not normalized and the final `LogEvent` is not created.

## LogEvent and sinks

A sink receives a completed `LogEvent`.

The sink is responsible for delivery only.

It should not:

* decide whether the event is allowed;
* normalize the payload;
* understand the business meaning of the payload.

Those decisions have already been made before the event reaches the sink.

A sink may adapt `LogEvent` to another output shape. For example, stream and file sinks adapt it to standard `logging.LogRecord` before formatting the final line.

That adaptation belongs to delivery. It does not change the role of `LogEvent` as the completed structured logging unit.

## What to remember

* `LogEvent` is the final structured event produced by the logging pipeline.
* `LogEventMeta` is created before payload normalization and is used by event policy.
* `LogEvent.payload` contains normalized event data.
* Sinks receive completed `LogEvent` objects and deliver them to their destination.
