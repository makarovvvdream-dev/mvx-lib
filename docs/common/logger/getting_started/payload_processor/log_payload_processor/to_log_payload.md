# Object-provided payloads

```{contents} Contents:
:depth: 1
:local:
```

Some objects should control how they appear in logs.

`LogPayloadProcessor` supports this through `to_log_payload()`.

If a value implements the payload provider protocol, the processor calls `to_log_payload()` before applying generic normalization rules.

This mechanism is useful for domain objects, operation results, descriptors, and errors that can expose a stable logging representation of themselves.

## Method shape

An object-provided payload method has this shape:

```python
def to_log_payload(self) -> dict[str, object]:
    ...
```

The method should return a dictionary.

Example:

```python
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BindOutcome:
    success: bool
    user_dn: str
    result_code: int

    def to_log_payload(self) -> dict[str, object]:
        return {
            "success": self.success,
            "user_dn": self.user_dn,
            "result_code": self.result_code,
        }
```

Usage:

```python
outcome = BindOutcome(
    success=True,
    user_dn="cn=user,dc=example,dc=org",
    result_code=0,
)

payload = {
    "result": outcome,
}

ctx.log_info_event(
    event="bind.completed",
    payload=payload,
)
```

When the payload is normalized, `LogPayloadProcessor` uses `BindOutcome.to_log_payload()` for the `result` value.

## Return value

`to_log_payload()` must return a dictionary.

If it returns a dictionary, that dictionary is used as the custom payload representation.

```python
{
    "success": True,
    "user_dn": "cn=user,dc=example,dc=org",
    "result_code": 0,
}
```

If `to_log_payload()` returns something other than a dictionary, the result is ignored and the processor falls back to generic normalization.

If `to_log_payload()` raises an exception, the exception is ignored by the processor and generic normalization is used instead.

This keeps logging safe: a broken logging representation should not break the application path that emits the event.

## Priority

Object-provided payload has priority over adapter-based representation.

The order is:

```text
to_log_payload()
        |
        v
log adapter resolver
        |
        v
generic normalization rules
```

If `to_log_payload()` returns a valid dictionary, the adapter resolver is not used for that value.

Adapters are used only when the value does not provide a valid object-provided payload.

## Where it is applied

`to_log_payload()` can be used for values passed directly to:

```python
processor.normalize_value_for_log(value)
```

It is also used for values inside payload mappings:

```python
ctx.log_info_event(
    event="operation.completed",
    payload={
        "result": outcome,
    },
)
```

And for leaf values inside lists, tuples, and mappings.

```python
ctx.log_info_event(
    event="batch.completed",
    payload={
        "results": [outcome_1, outcome_2],
    },
)
```

Each outcome value may provide its own `to_log_payload()` representation.

## Keep the payload log-ready

The dictionary returned by `to_log_payload()` should already be suitable for logging.

Use simple values when possible:

* strings;
* numbers;
* booleans;
* `None`;
* small dictionaries;
* small lists.

Avoid returning large object graphs or internal mutable state directly.

A good `to_log_payload()` result should be stable, bounded, and safe to expose in logs.

## Avoid sensitive values

`to_log_payload()` is part of the object's logging contract.

It should not expose secrets or sensitive data accidentally.

For example, an authentication result may include a user identifier and result code, but should not include a password, token, private key, or raw credential material.

```python
@dataclass(frozen=True, slots=True)
class AuthAttempt:
    user_dn: str
    password: str
    success: bool

    def to_log_payload(self) -> dict[str, object]:
        return {
            "user_dn": self.user_dn,
            "success": self.success,
            "password": "***",
        }
```

The object decides what is safe and useful to expose.

## When not to use `to_log_payload()`

Do not add `to_log_payload()` to an object only because one application wants to log it in a special way.

If the object belongs to another package, is shared across layers, or should not know about logging concerns, use an external adapter instead.

Use `to_log_payload()` when the logging representation is a natural part of the object's own public behavior.

Use adapters when the logging representation is application-specific or should stay outside the object type.

## What to remember

* `to_log_payload()` lets an object provide its own logging representation.
* `LogPayloadProcessor` uses it before adapter resolution and before generic normalization.
* The method must return a dictionary to be accepted.
* Invalid return values and exceptions are ignored, and generic normalization is used as fallback.
* The returned payload should be stable, bounded, and safe for logs.
