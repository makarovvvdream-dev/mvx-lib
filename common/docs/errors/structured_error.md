# StructuredError

`StructuredError` is the base exception class for MVX errors that need to carry
structured diagnostic context.

It extends the built-in `Exception` with explicit fields for:

- a human-readable message;
- structured diagnostic details;
- an optional underlying cause;
- a stable logging payload.

## Why it exists

Plain Python exceptions are good for control flow and stack traces, but they are
not enough for library-level error handling.

A usual exception often gives only this:

```text
ValueError: invalid value
```

That is readable for a developer, but weak for logs, telemetry, tests and API
boundaries. It does not reliably answer:

- what kind of domain error happened;
- which object, operation or state caused it;
- whether there was an underlying low-level exception;
- what payload should be sent to structured logging.

`StructuredError` solves this by making error context explicit.

Instead of hiding everything inside a formatted string, it keeps diagnostic data
in a normal dictionary:

```python
from mvx.common.errors import StructuredError

request_id = "req-123"

raise StructuredError(
    message="Failed to process request",
    details={
        "request_id": request_id,
        "operation": "bind",
        "state": "READY",
    },
)
```

This gives the code two different representations of the same error:

- `str(error)` — readable text for humans;
- `error.to_log_payload()` — stable structured data for logs.

## Basic usage

```python
from mvx.common.errors import StructuredError


def load_entity(entity_id: str) -> None:
    raise StructuredError(
        message="Failed to load entity",
        details={"entity_id": entity_id},
    )
```

String representation:

```text
StructuredError: Failed to load entity | details={'entity_id': 'abc-123'}
```

Log payload:

```python
log_payload={
    "kind": "StructuredError",
    "message": "Failed to load entity",
    "details": {
        "entity_id": "abc-123",
    },
}
```

## Wrapping an underlying exception

`StructuredError` can keep an original exception as its cause.

```python
from mvx.common.errors import StructuredError


def parse_port(raw_port: str) -> int:
    try:
        return int(raw_port)
    except ValueError as exc:
        raise StructuredError(
            message="Failed to parse port",
            details={"raw_port": raw_port},
            cause=exc,
        ) from exc
```

The `cause` is stored on the error object and is also exposed through Python's
standard exception chaining mechanism via `__cause__`.

The logging payload will include a compact representation of the cause:

```python
log_payload ={
    "kind": "StructuredError",
    "message": "Failed to parse port",
    "details": {
        "raw_port": "abc",
    },
    "cause": {
        "kind": "ValueError",
        "message": "invalid literal for int() with base 10: 'abc'",
    },
}
```

## Details

`details` is intended for small, non-secret diagnostic context.

Good examples:

```python
details={
    "operation": "connect",
    "state": "DISCONNECTED",
    "attempt": 1,
}
```

Bad examples:

```text
details={
    "password": password,
    "token": access_token,
    "full_payload": huge_payload,
}
```

Do not put secrets, credentials, private keys, tokens or large payloads into
`details`.

The constructor copies the incoming mapping into a plain `dict`. This prevents
accidental mutation of the original object after the error has been created.

## Adding details later

Sometimes the code creates an error at one layer and adds context at another
layer before raising or logging it.

`StructuredError` supports a fluent style for that:

```python
from mvx.common.errors import StructuredError

error = StructuredError(message="Operation failed")

error.with_detail("operation", "search")
error.with_detail("state", "READY")

raise error
```

Multiple details can be merged at once:

```python
from mvx.common.errors import StructuredError

raise StructuredError(message="Operation failed").with_details(
    {
        "operation": "search",
        "state": "READY",
    }
)
```

Both helpers mutate the error object and return `self`.

## Logging

Use `to_log_payload()` when the error needs to be passed to structured logging.

```text
from mvx.common.errors import StructuredError

try:
    ...
except StructuredError as exc:
    logger.error(
        "operation.failed",
        extra={"error": exc.to_log_payload()},
    )
```

The payload shape is intentionally stable:

```python
log_payload ={
    "kind": "<ConcreteErrorClassName>",
    "message": "<message>",
    "details": {...},
    "cause": {
        "kind": "<CauseClassName>",
        "message": "<cause message>",
    },
}
```

The `cause` field is included only when an underlying exception exists.

## Design rule

`StructuredError` should not become a dumping ground for arbitrary data.

Its job is narrow:

- preserve a clear human message;
- carry small structured context;
- keep the original cause when needed;
- provide a stable logging representation.

Business-specific meaning should live in subclasses.

For example:

```text
StructuredError
└── ReasonedError
    └── InvalidFunctionArgumentError
```

`StructuredError` gives the common structure. Subclasses define the specific
semantics.

## API

```{eval-rst}
.. autoclass:: mvx.common.errors.StructuredError
   :members:
   :show-inheritance:
```