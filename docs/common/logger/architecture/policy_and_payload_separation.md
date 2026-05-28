# Policy and payload separation

```{contents} Contents:
:depth: 1
:local:
```

## Overview

`MVX Logger` separates event selection from payload normalization.

Event selection is handled by `LogEventPolicyProto`.

Payload normalization is handled by `LogPayloadProcessorProto`.

They are connected by `LogContext`, but they answer different questions:

```text
event policy       -> should this event be logged?
payload processor  -> how should accepted payload data be represented?
```

The two components run at different stages of the pipeline:

```text
create LogEventMeta
        |
        v
check event policy
        |
        +--> rejected: stop
        |
        v
normalize payload
        |
        v
create LogEvent
        |
        v
deliver to sink
```

If the event policy rejects an event, payload normalization does not run.

## Why it exists

Logging configuration has two independent dimensions.

The first dimension is **logging width**: which events are included at all.

The second dimension is **logging depth**: how much detail is included for events that are included.

These dimensions should not be controlled by the same component.

A project may want to log fewer events without changing the shape of accepted payloads. It may also want richer payloads for diagnostics without changing which events are enabled.

Keeping these decisions separate makes both behaviors easier to reason about.

## Logging width: event policy

Event policy controls logging width.

It receives `LogEventMeta` and returns a boolean decision:

```python
class LogEventPolicyProto(Protocol):
    def is_event_enabled(self, event: LogEventMeta) -> bool: ...
```

The metadata contains event identity and source-related information:

```text
event_namespace
event_name
entity_id
source_path
source_line
source_func
```

The policy does not receive:

```text
payload
event_outcome
level
timestamp
LogEvent
```

This boundary is intentional. The policy decides whether an event belongs in the log before the final event is created.

## Logging depth: payload processor

Payload processor controls logging depth.

It receives payload data only after the event has been accepted:

```python
class LogPayloadProcessorProto(Protocol):
    def normalize_payload(
        self,
        payload: Mapping[str, Any],
        *,
        unbounded: bool = False,
    ) -> dict[str, Any]: ...

    def normalize_value_for_log(
        self,
        value: Any,
        *,
        unbounded: bool = False,
    ) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None: ...

    def get_plain_verbosity_level(self) -> str | None: ...
```

The processor does not decide whether an event should be logged. That decision has already been made.

The processor also does not deliver events. Delivery belongs to the sink.

Its responsibility is to convert arbitrary payload values into a log-ready representation.

## Ordering in LogContext

`LogContext.log_event()` keeps the order explicit.

First it builds `LogEventMeta`:

```python
log_event_meta = LogEventMeta(
    event_namespace=event_namespace if event_namespace is not None else self.namespace,
    event_name=event,
    entity_id=entity_id,
    source_path=source_path,
    source_line=source_line,
    source_func=source_func,
)
```

Then it checks the event policy:

```python
if not self.is_event_enabled(log_event_meta):
    return
```

Only after that does it normalize payload:

```python
payload_for_log = (
    self.payload_processor.normalize_payload(payload)
    if not skip_payload_normalization
    else payload
)
```

Then it creates the final `LogEvent` and emits it.

This ordering is the core of the separation.

## What event policy should not do

An event policy should not inspect payload values.

It does not receive payload from the logger pipeline, and that is not an omission. It is the intended boundary.

A policy should normally filter by stable event metadata:

```python
class WorkerEventPolicy:
    def is_event_enabled(self, event: LogEventMeta) -> bool:
        return event.event_namespace == "my_app.worker"
```

or:

```python
class EntityPolicy:
    def is_event_enabled(self, event: LogEventMeta) -> bool:
        return event.entity_id == "job-42"
```

or:

```python
class EventNamePolicy:
    def is_event_enabled(self, event: LogEventMeta) -> bool:
        return event.event_name in {
            "job.started",
            "job.completed",
            "job.failed",
        }
```

These policies make decisions before any expensive or lossy payload conversion happens.

## What payload processor should not do

A payload processor should not filter events.

It should not return an empty payload to express that an event is disabled. It should not raise a filtering exception to stop delivery. It should not encode routing decisions into normalized payload data.

Those concerns belong elsewhere:

```text
should this event exist?       -> event policy
what should payload look like? -> payload processor
where should event go?         -> sink
```

The processor may produce compact or detailed payloads, but it should not own event selection.

## Why policy runs before payload normalization

Payload normalization may be expensive.

It may walk nested mappings and collections, shorten long strings, convert enum values, call `to_log_payload()`, use external adapters, or create fallback representations for unsupported objects.

Running that work for disabled events would waste time and could expose data that the event policy would have rejected anyway.

By checking policy first, the logger avoids unnecessary normalization.

```text
rejected event -> no payload traversal
rejected event -> no adapter lookup
rejected event -> no LogEvent creation
rejected event -> no sink call
```

This also keeps rejected events invisible to sinks and downstream output formatting.

## Why policy receives metadata only

`LogEventMeta` is the stable identity layer of an event.

It exists before the final `LogEvent` and before payload normalization.

Policy receives metadata only because metadata is the part of an event that is safe to use for selection:

* event namespace says where the event belongs;
* event name says what happened;
* entity id says which runtime or domain entity is involved;
* source fields may identify the origin of the event when they are supplied.

Payload is different. It is arbitrary user data. It can be large, nested, mutable, expensive to normalize, and domain-specific.

Using payload for event selection would couple policy to payload shape. That would make policy fragile and would blur the boundary between logging width and logging depth.

## Why payload processor is inherited but event policy is local

Payload processor is runtime infrastructure. A parent context can provide common payload-depth rules for a whole subtree.

For example, the root context can say:

```text
normal verbosity
max string length = 200
max collection items = 10
```

and child contexts can inherit those rules.

Event policy is different. It is a local selection rule for the context that emits the event.

A parent layer should not need to know the event names, entity ids, or source locations used by lower-level layers. If parent policies were inherited automatically, logging configuration would leak implementation details across layer boundaries.

So the separation also appears in context configuration:

```text
payload_processor         -> inherited runtime infrastructure
event_policy              -> local event selection
```

This allows a common payload-depth policy without forcing a common event-selection policy.

## Example: changing width without changing depth

A context can restrict enabled events while keeping the same inherited payload processor.

```python
class OnlyFailuresPolicy:
    def is_event_enabled(self, event: LogEventMeta) -> bool:
        return event.event_name.endswith(".failed")

ctx = configure_log_context(
    "my_app.worker",
    event_policy=OnlyFailuresPolicy(),
)
```

This changes logging width.

The context still uses the inherited payload processor unless a local processor is assigned.

The accepted failure events keep the same payload normalization rules as before.

## Example: changing depth without changing width

A context can use a more detailed payload processor while keeping event selection unchanged.

```python
processor = LogPayloadProcessor(
    verbosity_level=LogVerbosityLevel.MAXIMUM,
    max_str_len=1000,
    max_items=100,
)

ctx = configure_log_context(
    "my_app.worker.diagnostics",
    payload_processor=processor,
)
```

This changes logging depth.

If the context has no local event policy, events emitted through this context are allowed by default.

The event-emitting code can keep passing the same payloads. The configured processor decides how much detail reaches the final `LogEvent`.

## Example: changing both independently

A diagnostic context can define both a local event policy and a local payload processor.

```python
ctx = configure_log_context(
    "my_app.worker.diagnostics",
    event_policy=DiagnosticEventPolicy(),
    payload_processor=diagnostic_payload_processor,
)
```

The two decisions remain independent:

```text
DiagnosticEventPolicy      -> which diagnostic events are enabled
diagnostic_payload_processor -> how detailed accepted payloads are
```

This keeps configuration explicit. The policy does not need to know payload normalization settings, and the processor does not need to know which events are enabled.

## Sink boundary

The sink is downstream from both policy and payload processing.

A sink receives only completed `LogEvent` objects:

```python
class LogSinkProto(Protocol):
    def log(self, event: LogEvent) -> None: ...
```

By the time a sink is called:

* the event has been accepted by policy;
* payload normalization has run unless explicitly skipped;
* the final `LogEvent` has been created.

A sink may format, store, transmit, or enqueue the event. It should not normally act as the primary event-selection mechanism.

Filtering in a sink would happen too late to avoid payload normalization cost and would mix backend delivery with logging-width configuration.

## Skipping payload normalization

`LogContext.log_event()` supports `skip_payload_normalization=True`.

This option does not bypass event policy.

The order is still:

```text
create metadata
check policy
if accepted, use payload as-is
create LogEvent
emit to sink
```

Skipping normalization only changes how accepted payload data is placed into the final event.

It should be used when the caller already has a log-ready payload and intentionally wants to avoid the configured processor.

## Design summary

Event policy and payload processor are separate because they control different axes of logging behavior.

```text
policy    -> width -> select events by metadata
processor -> depth -> normalize payload for accepted events
sink      -> delivery -> handle completed LogEvent
```

This keeps the pipeline predictable:

```text
LogEventMeta -> event policy -> payload processor -> LogEvent -> sink
```

A rejected event stops early. An accepted event receives payload normalization. A sink only sees completed structured events.
