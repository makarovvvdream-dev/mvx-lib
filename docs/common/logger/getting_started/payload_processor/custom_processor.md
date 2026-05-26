# Custom processor

```{contents} Contents:
:depth: 1
:local:
```

A custom payload processor is an implementation of the payload processor protocol.

It can be assigned to `LogContext` when the default `LogPayloadProcessor` is not the right normalization engine for a particular application or layer.

The processor has one responsibility: convert raw payload data into a log-ready representation before `LogContext` creates the final `LogEvent`.

It must not decide whether an event should be logged and must not deliver events.

## Protocol

A payload processor must provide three public methods.

```python
from collections.abc import Mapping
from typing import Any


class CustomPayloadProcessor:
    def normalize_payload(
        self,
        payload: Mapping[str, Any],
        *,
        unbounded: bool = False,
    ) -> dict[str, Any]:
        ...

    def normalize_value_for_log(
        self,
        value: Any,
        *,
        unbounded: bool = False,
    ) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None:
        ...

    def get_plain_verbosity_level(self) -> str | None:
        ...
```

A custom processor implements the public methods required by `LogPayloadProcessorProto`. The protocol is used for type checking and describes the expected interface; the processor class provides the actual implementation.

For type annotations, use `LogPayloadProcessorProto`.

```python
from mvx.common.logger import LogPayloadProcessorProto

processor: LogPayloadProcessorProto = CustomPayloadProcessor()
```

## Public facade

`LogContext` calls the processor through this protocol.

For a normal logging call, the context delegates payload normalization to:

```python
processor.normalize_payload(payload)
```

For explicit value normalization, the context delegates to:

```python
processor.normalize_value_for_log(value)
```

For verbosity-aware helpers, the context uses:

```python
processor.get_plain_verbosity_level()
```

This means a custom processor must be usable through these methods only. `LogContext` must not depend on implementation-specific attributes.

## What the processor must do

`normalize_payload()` must return the final payload dictionary for `LogEvent.payload`.

`normalize_value_for_log()` must return a value that is safe to store inside that payload.

Allowed normalized value types are:

```python
str | int | float | bool | bytes | dict[str, Any] | list[Any] | None
```

`get_plain_verbosity_level()` must return a string representation of the current verbosity mode, or `None` if the processor does not support verbosity.

A custom processor may implement any normalization rules it needs, but the result must be log-ready.

## Minimal example

This example keeps primitive values unchanged, recursively normalizes mappings and lists, and converts unsupported objects to `repr()`.

```python
from collections.abc import Mapping
from typing import Any


class SimplePayloadProcessor:
    def normalize_payload(
        self,
        payload: Mapping[str, Any],
        *,
        unbounded: bool = False,
    ) -> dict[str, Any]:
        return {
            str(key): self.normalize_value_for_log(value, unbounded=unbounded)
            for key, value in payload.items()
        }

    def normalize_value_for_log(
        self,
        value: Any,
        *,
        unbounded: bool = False,
    ) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None:
        if value is None or isinstance(value, str | int | float | bool | bytes):
            return value

        if isinstance(value, Mapping):
            return {
                str(key): self.normalize_value_for_log(item, unbounded=unbounded)
                for key, item in value.items()
            }

        if isinstance(value, list | tuple):
            return [
                self.normalize_value_for_log(item, unbounded=unbounded)
                for item in value
            ]

        return repr(value)

    def get_plain_verbosity_level(self) -> str | None:
        return None
```

Assign it to a context:

```python
from mvx.common.logger import configure_log_context

processor = SimplePayloadProcessor()

ctx = configure_log_context(
    "my_app.worker",
    payload_processor=processor,
)
```

or assign it later:

```python
ctx.set_payload_processor(processor)
```

## Boundaries

A custom processor should only normalize payload data.

It should not:

* decide whether an event is enabled;
* create `LogEvent`;
* send events to a sink;
* write logs itself;
* depend on a concrete delivery format.

Event selection belongs to event policy.

`LogEvent` creation belongs to `LogContext`.

Delivery belongs to sinks.

## What to remember

* A custom processor is any object compatible with `LogPayloadProcessorProto`.
* It must implement `normalize_payload()`, `normalize_value_for_log()`, and `get_plain_verbosity_level()`.
* It is used through the public processor facade exposed by `LogContext`.
* Its only responsibility is to turn raw payload data into log-ready payload data.
