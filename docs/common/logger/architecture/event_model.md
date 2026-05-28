# Event model

```{contents} Contents:
:depth: 1
:local:
```

## Overview

The logger event model is split into two dataclasses:

* `LogEventMeta`;
* `LogEvent`.

`LogEventMeta` is created first. It describes an event before payload processing and before delivery.

`LogEvent` is created only after the event has been accepted by the event policy and its payload has been normalized.

This split keeps event selection cheap. A disabled event does not require payload normalization, timestamp creation for the final event, or sink interaction.

## Why it exists

Structured logging often mixes three different pieces of information in one object:

* event identity;
* event payload;
* delivery-specific data.

`MVX Logger` keeps these pieces separated.

`LogEventMeta` answers the question: **what event is this?**

`LogEvent` answers the question: **what exactly should be delivered to the sink?**

The distinction matters because event policies operate before payload normalization. A policy should be able to make a decision based on stable event identity, without inspecting arbitrary payload values and without forcing potentially expensive conversion of complex objects.

## LogEventMeta

`LogEventMeta` is the pre-delivery metadata object.

```python
@dataclass(frozen=True, slots=True)
class LogEventMeta:
    event_namespace: str | None
    event_name: str
    entity_id: str | None
    source_path: str | None
    source_line: int | None
    source_func: str | None
```

It is used by `LogEventPolicyProto`:

```python
class LogEventPolicyProto(Protocol):
    def is_event_enabled(self, event: LogEventMeta) -> bool: ...
```

The policy receives only metadata. It does not receive the raw payload and does not receive the final `LogEvent`.

This makes the policy boundary intentionally narrow.

## Metadata fields

### event_namespace

`event_namespace` identifies the logical logging namespace.

For manual logging through `LogContext.log_event()`, the namespace is resolved as follows:

```text
explicit event_namespace argument
        |
        v
context namespace
```

If `event_namespace` is not passed explicitly, `LogContext` uses its own `namespace` property.

Namespaces are useful for grouping events by subsystem, module, component, or logical service area.

### event_name

`event_name` is the stable name of the event.

It should describe the event itself, not the message text. For example:

```text
request.received
request.completed
connection.opened
connection.failed
payload.normalized
```

A stable event name is important because policies and downstream processors may use it for filtering, aggregation, or routing.

### entity_id

`entity_id` identifies the domain object or runtime entity related to the event.

It is optional because not every event belongs to one entity.

Examples:

```text
connection id
request id
message id
user id
job id
```

`entity_id` is metadata, not payload. It is available to event policies before payload normalization.

### source_path, source_line, source_func

These fields describe the source location associated with the event.

They are optional because not every logging path provides source information.

Manual logging code may pass these fields explicitly when it has a meaningful source location to attach.

## LogEvent

`LogEvent` is the final structured event delivered to a sink.

```python
@dataclass(frozen=True, slots=True)
class LogEvent:
    level: int
    meta: LogEventMeta
    event_outcome: str | None
    timestamp: float
    payload: Mapping[str, Any]
```

A sink receives `LogEvent` through `LogSinkProto.log()`:

```python
class LogSinkProto(Protocol):
    def log(self, event: LogEvent) -> None: ...
```

At this point the event has already passed the event policy and its payload has already been prepared for logging.

## Event fields

### level

`level` is the numeric severity level.

The package provides `LogLevel`:

```python
class LogLevel(IntEnum):
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50
```

`LogContext` helper methods map directly to these levels:

```text
log_debug_event()    -> LogLevel.DEBUG
log_info_event()     -> LogLevel.INFO
log_warning_event()  -> LogLevel.WARNING
log_error_event()    -> LogLevel.ERROR
```

`LogEvent.level` is typed as `int`, so code can still interoperate with numeric logging levels.

### meta

`meta` contains the previously created `LogEventMeta` object.

This keeps the final event connected to the same metadata that was used for policy evaluation.

### event_outcome

`event_outcome` is an optional classifier for events' outcomes.

It is different from `event_name`.

`event_name` identifies the exact event. `event_outcome` stores the outcome of the event.

For example:

```text
event_name = "connection.open"
event_outcome = "success"
```

or:

```text
event_name = "request"
event_outcome = "failure"
```

A project may leave `event_outcome` unset if event names and namespaces are sufficient.

### timestamp

`timestamp` is created when the final `LogEvent` is built.

In the current implementation, `LogContext.log_event()` uses `time.time()`.

This means the timestamp belongs to the accepted event, not to the earlier metadata object.

### payload

`payload` is the structured event data delivered to the sink.

For normal logging calls, `LogContext` obtains it through:

```python
self.payload_processor.normalize_payload(payload)
```

If `skip_payload_normalization=True` is passed, the original mapping is used directly.

That option is intentionally low-level. It is useful when the caller already has a log-ready payload and wants to avoid another normalization pass.

## Event creation pipeline

A manual event follows this sequence:

```text
LogContext.log_event(...)
   |
   v
create LogEventMeta
   |
   v
is_event_enabled(meta)
   |
   +--> False: stop
   |
   +--> True
          |
          v
      normalize payload
          |
          v
      create LogEvent
          |
          v
      emit_log_event(event)
          |
          v
      log_sink.log(event)
```

The important ordering is:

```text
metadata first
policy second
payload normalization third
sink delivery last
```

The policy never pays the cost of payload normalization for rejected events.

## Manual logging example

```python
ctx.log_info_event(
    event="connection.opened",
    event_outcome="lifecycle",
    entity_id="conn-42",
    payload={
        "remote_host": "127.0.0.1",
        "remote_port": 389,
    },
)
```

This call produces metadata equivalent to:

```python
LogEventMeta(
    event_namespace=ctx.namespace,
    event_name="connection.opened",
    entity_id="conn-42",
    source_path=None,
    source_line=None,
    source_func=None,
)
```

If the event is enabled, the payload is normalized and a final `LogEvent` is emitted.

## Event policy implications

Because event policies receive `LogEventMeta`, they should filter by metadata fields:

```python
class NoDebugConnectionsPolicy:
    def is_event_enabled(self, event: LogEventMeta) -> bool:
        return not (
            event.event_namespace == "my_app.connections"
            and event.event_name.startswith("debug.")
        )
```

A policy should not depend on payload content. Payloads are intentionally outside the policy boundary.

This keeps policies predictable and prevents hidden serialization cost from leaking into the event selection step.

## Sink implications

A sink receives only completed `LogEvent` objects.

By the time a sink sees an event:

* the event was accepted by policy;
* the payload was normalized unless explicitly skipped;
* the timestamp was assigned;
* metadata and payload are packaged together.

A sink should usually not perform event filtering. Filtering belongs to the event policy. A sink may still reject or fail delivery for backend-specific reasons, but that is an infrastructure concern, not an event selection concern.

## Direct usage

The event model is public and can be used directly.

For example, a custom sink can format events without depending on `LogContext` internals:

```python
class PlainDebugSink:
    def log(self, event: LogEvent) -> None:
        print(
            event.level,
            event.meta.event_namespace,
            event.meta.event_name,
            dict(event.payload),
        )
```

A custom policy can also be tested directly by constructing `LogEventMeta` instances without creating a full logger setup.

```python
meta = LogEventMeta(
    event_namespace="my_app.test",
    event_name="request.received",
    entity_id="req-1",
    source_path=None,
    source_line=None,
    source_func=None,
)

assert policy.is_event_enabled(meta)
```

This is one of the main benefits of the split event model: policies, sinks, and context behavior can be tested independently.

## Design summary

`LogEventMeta` is small, stable, and policy-oriented.

`LogEvent` is complete, normalized, timestamped, and sink-oriented.

The logger pipeline deliberately moves from cheap metadata to accepted structured event:

```text
LogEventMeta -> policy -> normalized payload -> LogEvent -> sink
```

This gives the logger a clear boundary between selection, transformation, and delivery.
