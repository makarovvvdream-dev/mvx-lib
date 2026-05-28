# External object adapters

```{contents} Contents:
:depth: 1
:local:
```

External object adapters let `LogPayloadProcessor` use a custom logging representation for objects that should not implement `to_log_payload()` themselves.

This is useful when the object type belongs to another package, is shared across layers, or should stay independent from the logger API.

The object remains unchanged. The logging representation is provided from the outside.

## Adapter resolver

Adapters are connected through `log_adapter_resolver`.

The resolver receives the original value and returns either an adapter or `None`.

```python
from typing import Any


def resolver(value: Any):
    ...
```

If the resolver returns `None`, `LogPayloadProcessor` continues with generic normalization.

If the resolver returns an adapter, the processor calls it with two arguments:

```python
adapter(value, verbosity_level)
```

The adapter must return a dictionary.

If it returns a dictionary, that dictionary is used as the custom payload representation.

If it returns something else, or raises an exception, the processor ignores the adapter result and falls back to generic normalization.

## Adapter shape

A practical adapter has this shape:

```python
from typing import Any

from mvx.common.logger import LogVerbosityLevel


def user_adapter(
    value: Any,
    verbosity_level: LogVerbosityLevel,
) -> dict[str, Any]:
    ...
```

The first argument is the original value.

The second argument is the effective verbosity level of the processor.

The adapter decides how the value should be represented in logs.

## Minimal example

Suppose there is a class that should not depend on `MVX Logger`.

```python
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ExternalUser:
    user_id: str
    email: str
    internal_token: str
```

The class does not implement `to_log_payload()`.

A separate adapter can define its logging representation:

```python
from typing import Any

from mvx.common.logger import LogVerbosityLevel


def external_user_adapter(
    value: ExternalUser,
    verbosity_level: LogVerbosityLevel,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "user_id": value.user_id,
    }

    if verbosity_level == LogVerbosityLevel.MAXIMUM:
        payload["email"] = value.email

    payload["internal_token"] = "***"

    return payload
```

The resolver chooses this adapter for `ExternalUser` values:

```python
from typing import Any


def resolver(value: Any):
    if isinstance(value, ExternalUser):
        return external_user_adapter
    return None
```

Then the resolver is passed to `LogPayloadProcessor`:

```python
from mvx.common.logger import LogPayloadProcessor

processor = LogPayloadProcessor(
    log_adapter_resolver=resolver,
)
```

When the processor normalizes an `ExternalUser` value, it uses the adapter result.

## Verbosity-aware adapters

Adapters receive the effective processor verbosity level.

This allows them to return different payloads for different logging depth modes.

```python
def external_user_adapter(
    value: ExternalUser,
    verbosity_level: LogVerbosityLevel,
) -> dict[str, Any]:
    if verbosity_level == LogVerbosityLevel.MINIMAL:
        return {
            "user_id": value.user_id,
        }

    return {
        "user_id": value.user_id,
        "email": value.email,
        "internal_token": "***",
    }
```

The adapter should still keep the payload bounded and safe.

`MAXIMUM` should mean more useful diagnostic detail, not uncontrolled object dumping.

## Priority

`to_log_payload()` has priority over adapters.

The order is:

```text
to_log_payload()
        |
        v
log_adapter_resolver
        |
        v
generic normalization rules
```

If a value provides a valid `to_log_payload()` result, the adapter resolver is not used for that value.

Adapters are used only when the object does not provide its own valid payload.

## Fallback behavior

Adapter usage is safe by design.

The processor falls back to generic normalization when:

* no resolver is configured;
* the resolver returns `None`;
* the resolver raises an exception;
* the adapter raises an exception;
* the adapter returns a value that is not a dictionary.

In these cases, logging continues and the value is handled by the usual normalization rules.

## When to use adapters

Use an adapter when the logging representation should stay outside the object type.

Typical cases:

* the object comes from another package;
* the object belongs to a lower-level layer that should not know about logging;
* different applications need different logging representations for the same object;
* logging output must be controlled by application configuration;
* sensitive fields must be masked without modifying the object class.

Use `to_log_payload()` when the logging representation is part of the object's own public behavior.

Use an adapter when the representation is external to the object.

## What to remember

* Adapters provide external logging representations for objects.
* `log_adapter_resolver` chooses an adapter for a value.
* The adapter receives the original value and the effective verbosity level.
* The adapter must return a dictionary to be accepted.
* `to_log_payload()` takes priority over adapters.
* Invalid adapter results and adapter errors are ignored, and generic normalization is used as fallback.
