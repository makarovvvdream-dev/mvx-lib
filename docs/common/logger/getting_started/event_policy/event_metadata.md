# Event metadata

```{contents} Contents:
:depth: 1
:local:
```

Event metadata is the part of a logging event used for event selection.

In `MVX Logger`, metadata is represented by `LogEventMeta`.

`LogContext` builds `LogEventMeta` before payload normalization and before the final `LogEvent` is created.

```text
LogContext builds LogEventMeta
        |
        v
event policy checks LogEventMeta
        |
        v
accepted event -> payload processor
rejected event -> pipeline stops
```

The event policy receives metadata only. It does not inspect the payload, normalized payload, logging level, event type, timestamp, sink, or destination.

## LogEventMeta structure

`LogEventMeta` contains the fields that identify where the event belongs and what source location it is associated with.

```python
from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class LogEventMeta:
    event_namespace: str | None
    event_name: str
    entity_id: str | None
    source_path: str | None
    source_line: int | None
    source_func: str | None
```

The object is intentionally small. It contains only the information needed for event selection.

## `event_namespace`

Type:

```text
str | None
```

`event_namespace` identifies the logging area of the event.

Usually, this value is taken from the current `LogContext` name.

For example, if the context is named:

```text
my_app.worker
```

then events emitted through that context normally belong to the `my_app.worker` namespace.

The namespace can also be overridden explicitly when emitting an event.

Typical use in event policy:

* allow or reject events from a whole application area;
* enable diagnostics for one namespace;
* keep logging rules aligned with application layers or components.

## `event_name`

Type:

```text
str
```

`event_name` identifies the concrete event.

Examples:

```text
started
request.completed
cache.lookup
operation.failed
```

The event name describes what happened. It is not a formatted log message.

Typical use in event policy:

* allow selected event names;
* reject noisy events;
* enable diagnostics for a specific operation or state transition.

## `entity_id`

Type:

```text
str | None
```

`entity_id` identifies a concrete runtime entity related to the event.

Examples:

```text
worker-1
request-42
connection-7
session-abc
```

This field is optional.

Typical use in event policy:

* enable logging for one specific entity;
* trace one request, connection, session, or worker;
* keep the rest of the application quiet while diagnosing one object or operation instance.

## Source location fields

Source location metadata is optional.

It is represented by three fields:

* `source_path`;
* `source_line`;
* `source_func`.

These fields can be used together when event selection needs to depend on the source code location associated with the event.

### `source_path`

Type:

```text
str | None
```

`source_path` stores the source file path associated with the event.

Example:

```text
app/api.py
```

Typical use in event policy:

* enable events from a specific module or file;
* suppress events from a noisy source file;
* narrow diagnostics to a specific implementation area.

### `source_line`

Type:

```text
int | None
```

`source_line` stores the source line number associated with the event.

Typical use in event policy:

* target a specific event emission point;
* combine with `source_path` for precise source-location filtering.

### `source_func`

Type:

```text
str | None
```

`source_func` stores the function name associated with the event.

Typical use in event policy:

* enable diagnostics for events associated with a specific function;
* make source-location based rules easier to read;
* combine with `source_path` when function names alone are not unique enough.

## Metadata is not payload

Event metadata and payload have different responsibilities.

Metadata is used to decide whether the event should be logged.

Payload carries the data of the event after the event has been accepted.

This separation is important because payload preparation may be expensive. It may involve object serialization, adapters, error payload construction, or collection normalization.

By checking only metadata, event policy can reject disabled events before any payload normalization work is done.

## Metadata is not output formatting

Metadata also does not describe how the event will be written.

It does not define:

* final text format;
* JSON shape;
* file path;
* stream destination;
* external backend;
* sink behavior.

Those decisions belong to sinks and formatters.

Event metadata is only about identifying and selecting events before they enter the rest of the pipeline.

## What to remember

* `LogEventMeta` is built before payload normalization.
* Event policy receives `LogEventMeta` and decides whether the event is accepted or rejected.
* `LogEventMeta` contains namespace, event name, optional entity id, and optional source location fields.
* Event policy does not inspect payload or output details.
* If an event is rejected by metadata, payload normalization and sink delivery do not happen.
