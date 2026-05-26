# Payload processor

```{contents} Contents:
:depth: 1
:local:
```

A payload processor controls logging depth.

It defines how event payload data is normalized before the final `LogEvent` is created.

The payload processor can be configured at the root context level and then inherited by child contexts. This makes it possible to control payload normalization policy application-wide from one place.

At the same time, a specific context can receive its own payload processor when a particular application layer or component needs different normalization rules.

In the logging pipeline, the payload processor is called after event policy accepts the event and before `LogContext` creates `LogEvent`.

```text
event policy accepts LogEventMeta
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

If event policy rejects the event, the payload processor is not called.

## Why payload processor exists

Payload processor exists to keep payload normalization separate from event selection, event creation, and delivery.

A logging system often needs to control not only which events are logged, but also how much data is included in those events.

During normal operation, compact payloads may be enough. During diagnostics, the same application layer may need more detailed object state, larger collections, or richer error data.

This should not be hardcoded in business code and should not be handled by sinks.

By assigning a payload processor to the root context, an application can define a common payload normalization policy for the whole logging hierarchy. Child contexts inherit that processor unless they receive a local one.

By assigning a payload processor to a specific context, an application layer or component can override the common behavior only where different normalization rules are needed.

The code that emits events can keep passing the same payload structure, while the configured processor decides how that data becomes log-ready.

This also keeps domain objects and infrastructure components from being exposed directly to output formatting. The processor turns raw payload values into a normalized form before the event reaches a sink.

## What payload processor receives

The payload processor receives the payload passed to the logging call.

For regular logging methods, `LogContext` calls:

```python
payload_processor.normalize_payload(payload)
```

The input is a mapping with string-like keys and arbitrary values.

The processor returns a normalized dictionary that can be stored in `LogEvent.payload`.

The processor can also normalize a single value explicitly:

```python
payload_processor.normalize_value_for_log(value)
```

`LogContext` exposes helper methods that delegate to the configured payload processor when user code needs explicit normalization.

## What payload processor does not decide

The payload processor does not decide whether an event should be logged.

That decision belongs to event policy and has already been made before payload normalization starts.

The payload processor also does not decide where the event should be delivered.

Delivery belongs to the sink.

The payload processor is responsible only for converting payload data into a log-ready representation.

## Default implementation

The package includes a default implementation named `LogPayloadProcessor`.

It is intended to cover common logging-depth requirements out of the box.

The default processor supports:

* verbosity-based payload representation;
* string length limits;
* collection item limits;
* object-provided `to_log_payload()` representation;
* external serialization adapters;
* fallback representation for unsupported objects.

It can normalize primitive values, mappings, lists, tuples, enum values, byte-like values, and objects that provide their own logging payload.

The default processor also protects the log from uncontrolled payload growth by limiting long strings and large collections according to its configuration.

## Skipping normalization

In normal logging calls, payload normalization is performed automatically.

If the payload is already prepared, normalization can be skipped explicitly:

```python
ctx.log_info_event(
    event="operation.completed",
    payload=prepared_payload,
    skip_payload_normalization=True,
)
```

In this case, the provided payload is placed into the final `LogEvent` as-is.

This should be used only when the payload is already log-ready.

## Replacing the processor

Payload processor is a protocol-level component.

A custom implementation can be assigned to a context when the default behavior is not enough.

A processor must provide these methods:

```python
from typing import Any
from collections.abc import Mapping

def normalize_payload(
    payload: Mapping[str, Any],
    *,
    unbounded: bool = False,
) -> dict[str, Any]:
    ...


def normalize_value_for_log(
    value: Any,
    *,
    unbounded: bool = False,
) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None:
    ...


def get_plain_verbosity_level() -> str | None:
    ...
```

The root context always has a payload processor. Child contexts inherit the payload processor from their parent unless a local one is assigned.

## What to remember

* Payload processor controls logging depth.
* It runs after event policy accepts the event and before `LogEvent` is created.
* It normalizes raw payload data into a log-ready dictionary.
* It does not select events and does not deliver events.
* `LogPayloadProcessor` is the default implementation included in the package.
* A custom payload processor can be assigned when a project needs different payload normalization rules.

```{toctree}
:maxdepth: 1
:caption: What to read next

payload_depth
configuring_processor
error_payloads
log_payload_processor/overview
custom_processor
```