# Custom payload processor

```{contents} Contents:
:depth: 1
:local:
```

## Overview

A custom payload processor is the heaviest payload-processing extension point.

It replaces the component that turns payload mappings and individual values into log-ready structured data.

Most projects should not start here.

Prefer smaller extension points first:

```text
LogPayloadProvider
    when an object owns its own logging representation

LogAdapter
    when external code should provide a representation for a type

LogPayloadProcessor
    when the whole normalization strategy must change
```

A custom processor is useful when the default processor's model is not enough for the application or library.

## What a payload processor does

A payload processor provides three operations:

```text
normalize_payload(payload)
    normalize a full structured payload mapping

normalize_value_for_log(value)
    normalize one selected value

get_plain_verbosity_level()
    expose verbosity as a plain string for components such as log_invocation
```

`LogContext` uses the processor when emitting normal events through `log_event()` and convenience methods.

`log_invocation` also uses the resolved context to normalize selected values for operation payloads.

## When to write a custom processor

Write a custom processor when payload normalization must follow rules that cannot be expressed with providers or adapters.

Good reasons:

```text
project-wide redaction rules
strict schema output
custom primitive handling
custom collection representation
special treatment for domain-wide base classes
integration with an existing structured logging format
consistent serialization rules across many object types
```

For example, a security-sensitive application may require every payload processor to redact fields named `password`, `token`, or `secret` at every nesting level.

That kind of rule belongs naturally in a processor.

## When not to write one

Do not write a custom processor just to customize one object type.

Use `LogPayloadProvider` when you own the object:

```python
class Session:
    def to_log_payload(self) -> dict[str, object]:
        return {
            "session_id": self.session_id,
            "user_id": self.user_id,
        }
```

Use an adapter when the object type is external or should not know about logging.

A custom processor should be reserved for changing the general normalization policy.

## Minimal processor shape

A custom processor must satisfy `LogPayloadProcessorProto`.

```python
from typing import Any
from collections.abc import Mapping


class MinimalPayloadProcessor:
    def normalize_payload(
        self,
        payload: Mapping[str, Any],
        *,
        unbounded: bool = False,
    ) -> dict[str, Any]:
        return {str(key): self.normalize_value_for_log(value) for key, value in payload.items()}

    def normalize_value_for_log(
        self,
        value: Any,
        *,
        unbounded: bool = False,
    ) -> str | int | float | bool | bytes | dict[str, Any] | list[Any] | None:
        if isinstance(value, (str, int, float, bool, bytes)) or value is None:
            return value
        return f"<{type(value).__name__}>"

    def get_plain_verbosity_level(self) -> str | None:
        return None
```

This is deliberately small. A real processor usually needs more careful handling for nested structures, limits, redaction, and custom object representation.

## Using a custom processor

A custom processor can be attached to a direct context:

```python
ctx = LogContext(
    namespace="my.component",
    log_sink=sink,
    payload_processor=MinimalPayloadProcessor(),
)
```

Or configured through the package-level facade:

```python
ctx = configure_log_context(
    "my.component",
    payload_processor=MinimalPayloadProcessor(),
)
```

Once attached, the processor is used by that context when normalizing payloads.

Child contexts inherit the payload processor unless they define a local override.

## Processor inheritance

Payload processor is inherited through the `LogContext` tree.

```text
child has local processor
    use local processor

child has no local processor
    resolve processor from parent
```

A child context can reset its local payload processor override:

```python
ctx.reset_payload_processor()
```

After reset, the child resolves the processor from its parent again.

The root context cannot reset its payload processor because it has no parent fallback.

## Normalizing mappings

`normalize_payload()` receives a mapping and returns a dictionary.

It should produce a log-ready payload object.

Typical responsibilities:

```text
convert keys to strings
normalize values
apply collection limits
apply redaction rules
avoid leaking sensitive values
avoid returning unsupported objects
```

Example redaction rule:

```python
from typing import Any
from collections.abc import Mapping


class RedactingPayloadProcessor:
    _sensitive_keys = frozenset({"password", "token", "secret"})

    def normalize_payload(
        self,
        payload: Mapping[str, Any],
        *,
        unbounded: bool = False,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in payload.items():
            key_str = str(key)
            if key_str.lower() in self._sensitive_keys:
                result[key_str] = "<redacted>"
            else:
                result[key_str] = self.normalize_value_for_log(value, unbounded=unbounded)
        return result
```

The example is intentionally simple. A production processor may need nested redaction and stricter schema rules.

## Normalizing single values

`normalize_value_for_log()` is used when a component wants one value normalized, not a full payload mapping.

Examples:

```text
log_invocation selected argument
log_invocation context field
log_invocation selected result field
custom context formatter value
manual component-level normalization
```

The method should return only log-ready values:

```text
str
int
float
bool
bytes
dict[str, Any]
list[Any]
None
```

Unsupported objects should be converted into a safe representation instead of being returned raw.

## Verbosity support

`get_plain_verbosity_level()` returns a string or `None`.

`log_invocation` uses this value for verbosity-gated field specs:

```text
MAXIMUM:payload
NORMAL,MAXIMUM:request_id
```

A custom processor may return any string convention it supports.

If it returns `None`, verbosity-gated fields are skipped.

If the processor wants to interoperate with the default logger conventions, use the same names:

```text
MINIMAL
NORMAL
MAXIMUM
```

## The unbounded flag

Both normalization methods receive `unbounded`.

The flag means that item-count limiting should be disabled for that normalization call.

It should not be interpreted as permission to ignore all safety concerns.

For example, a processor may still:

```text
redact secrets
limit string length
avoid expensive expansion
reject unsupported values
```

The exact behavior belongs to the processor implementation, but callers use `unbounded=True` to request no item-count truncation for a selected value or payload.

## Thread-safety expectations

A payload processor may be used by multiple threads through shared contexts.

If a custom processor has mutable configuration or caches, it should protect them.

Good options:

```text
make configuration immutable
use locks around mutable state
avoid shared mutable caches
use thread-safe data structures
```

The default processor protects its configuration with a lock. A custom processor should provide an equivalent safety story if it stores mutable state.

## Performance expectations

Payload normalization happens on the logging path before the event is delivered to a sink.

A processor should avoid slow work:

```text
network calls
database queries
large filesystem reads
expensive reflection over large objects
deep serialization of arbitrary graphs
```

If a value is expensive to represent, prefer a small placeholder or identifier.

The logger should not become a hidden serialization engine for arbitrary application state.

## Provider and adapter compatibility

A custom processor may choose to support `LogPayloadProvider` and adapter-like behavior, but it is not required to copy the default processor exactly.

If compatibility with existing logger patterns is desired, preserve this order:

```text
1. LogPayloadProvider
2. type-based adapter or resolver
3. built-in normalization rules
```

This keeps object-owned payload representation predictable.

If a custom processor intentionally uses different precedence, document it clearly.

## Testing a custom processor

A custom processor should be tested directly.

At minimum, test:

```text
primitive normalization
mapping key conversion
nested mapping behavior
list and tuple behavior
unsupported object behavior
sensitive field redaction
unbounded behavior
verbosity string behavior
thread-safety if mutable state exists
```

Also test it through `LogContext`:

```text
log_event() uses the processor
child contexts inherit the processor
local processor override works
reset_payload_processor() restores inherited behavior
```

If the processor is used with `log_invocation`, test verbosity-gated fields and selected result/argument normalization too.

## Design summary

A custom payload processor replaces the general payload normalization strategy.

Use it when normalization rules are project-wide or domain-wide.

Do not use it when a smaller extension point is enough.

Prefer `LogPayloadProvider` for objects that own their logging representation.

Prefer adapters for external objects.

Use a custom processor when the whole payload-processing policy must change.
