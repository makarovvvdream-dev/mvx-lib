# RuntimeExtendedError

`RuntimeExtendedError` is a structured variant of Python's `RuntimeError`.

It is used for runtime failures that should preserve normal `RuntimeError`
semantics while also carrying structured diagnostic context.

## Why it exists

Infrastructure and library code often needs runtime errors that are both:

- compatible with ordinary `RuntimeError` handling;
- useful for structured logs and telemetry.

A plain `RuntimeError` usually gives only a message:

```python
raise RuntimeError("stream engine is not open")
```

That is readable, but weak for diagnostics. It does not provide a stable place
for state, operation names, flags, identifiers, or source metadata.

`RuntimeExtendedError` keeps the runtime nature of the error, but adds the
structured payload behavior inherited from `StructuredError`.

## Basic usage

```python
from mvx.common.errors import RuntimeExtendedError

error = RuntimeExtendedError(
    message="Stream engine is not open",
    details={
        "operation": "read",
        "state": "CLOSED",
        "is_reader": False,
        "is_writer": True,
    },
)

payload = error.to_log_payload()
```

The payload is suitable for structured logging:

```python
payload = {
    "kind": "RuntimeExtendedError",
    "message": "Stream engine is not open",
    "details": {
        "operation": "read",
        "state": "CLOSED",
        "is_reader": False,
        "is_writer": True,
    },
}
```

## Source metadata

`RuntimeExtendedError` can optionally store source metadata:

```python
from mvx.common.errors import RuntimeExtendedError

error = RuntimeExtendedError(
    message="Unexpected runtime failure",
    module="mvx.networking.tcp_stream_engine",
    qualname="TcpStreamEngine.connect",
)
```

When present, `module` and `qualname` are included in the log payload:

```python
payload = {
    "module": "mvx.networking.tcp_stream_engine",
    "qualname": "TcpStreamEngine.connect",
    "kind": "RuntimeExtendedError",
    "message": "Unexpected runtime failure",
    "details": {},
}
```

This is useful when an error is created by generic wrapping code and the original
call site should remain visible in logs.

## Wrapping an underlying exception

`RuntimeExtendedError` can also keep an underlying exception as its cause:

```python
from mvx.common.errors import RuntimeExtendedError

try:
    result = int("not-a-number")
except ValueError as exc:
    raise RuntimeExtendedError(
        message="Failed to parse numeric value",
        details={"raw_value": "not-a-number"},
        cause=exc,
    ) from exc
```

The cause is included in the logging payload in compact form:

```python
payload = {
    "kind": "RuntimeExtendedError",
    "message": "Failed to parse numeric value",
    "details": {
        "raw_value": "not-a-number",
    },
    "cause": {
        "kind": "ValueError",
        "message": "invalid literal for int() with base 10: 'not-a-number'",
    },
}
```

## Relationship with StructuredError

`RuntimeExtendedError` inherits from both `StructuredError` and `RuntimeError`.

That means it can be handled as structured MVX error:

```python
from mvx.common.errors import StructuredError

try:
    ...
except StructuredError as exc:
    payload = exc.to_log_payload()
```

and also as an ordinary runtime error:

```python
try:
    ...
except RuntimeError:
    ...
```

This is intentional. The error belongs to the runtime-error family, but it also
participates in the MVX structured-error model.

## Design rule

Use `RuntimeExtendedError` for runtime failures in library or infrastructure
code when a plain `RuntimeError` would lose useful diagnostic context.

Do not use it for validation errors or domain-specific expected failures when a
more specific error type exists.

## API

```{eval-rst}
.. autoclass:: mvx.common.errors.RuntimeExtendedError
   :members:
   :show-inheritance:
```