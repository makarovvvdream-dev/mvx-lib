# Error payloads

```{contents} Contents:
:depth: 1
:local:
```

Errors usually need more structure than a plain string.

When an exception is logged, the useful information is not only the message. It may also include an error kind, a stable reason code, diagnostic details, and an original cause.

`MVX Logger` provides `LogContext.build_error_payload()` for this case.

It converts an exception into a payload dictionary that can be passed to a logging method.

## Basic usage

The usual pattern is to catch an exception, build an error payload, and log it.

```python
from mvx.common.logger import configure_log_context

ctx = configure_log_context("my_app")


def run_operation() -> None:
    ...


try:
    run_operation()
except Exception as exc:
    ctx.log_error_event(
        event="operation.failed",
        payload=ctx.build_error_payload(exc),
    )
    raise
```

`build_error_payload()` returns a mapping suitable for an event payload.

The logging call still goes through the normal pipeline: event policy is checked first, then the payload is normalized by the configured payload processor unless normalization is explicitly skipped.

## Errors with `to_log_payload()`

If an exception provides a callable `to_log_payload()` method and that method returns a dictionary, `build_error_payload()` uses that dictionary.

```python
class OperationError(Exception):
    def to_log_payload(self) -> dict[str, object]:
        return {
            "kind": type(self).__name__,
            "message": str(self),
            "reason": "operation_failed",
        }
```

This lets domain errors control their own logging representation.

If `to_log_payload()` raises an exception or returns something other than a dictionary, `build_error_payload()` falls back to the generic error representation.

## Generic error representation

For ordinary exceptions, `build_error_payload()` creates a basic payload.

The generic payload always includes:

* `kind` — the exception class name;
* `message` — the string representation of the exception.

Example:

```python
try:
    raise ValueError("invalid value")
except ValueError as exc:
    payload = ctx.build_error_payload(exc)
```

Result:

```text
{
    "kind": "ValueError",
    "message": "invalid value",
}
```

If the exception has `code` or `code_desc` attributes, they are also included.

```text
{
    "code": 10,
    "code_desc": "invalid_credentials",
    "kind": "SomeError",
    "message": "authentication failed",
}
```

This keeps errors from external libraries compatible with structured logging when they expose simple diagnostic attributes.

## StructuredError

The `mvx.common.errors` package provides base error classes that already fit this model.

`StructuredError` stores:

* a human-readable message;
* a `details` dictionary;
* an optional original cause.

Its `to_log_payload()` method returns a stable dictionary with:

* `kind`;
* `message`;
* `details`;
* `cause`, when an original cause is present.

Example:

```python
from mvx.common.errors import StructuredError

err = StructuredError(
    message="operation failed",
    details={
        "operation": "sync_users",
        "user_count": 42,
    },
)

payload = ctx.build_error_payload(err)
```

Payload:

```text
{
    "kind": "StructuredError",
    "message": "operation failed",
    "details": {
        "operation": "sync_users",
        "user_count": 42,
    },
}
```

If a cause is present, it is represented as a nested dictionary containing the cause kind and message.

## ReasonedError

`ReasonedError` extends `StructuredError` with a stable reason code.

This is useful when an error should be classified by a machine-readable value, not only by its text message.

```python
from mvx.common.errors import ReasonedError

err = ReasonedError(
    message="operation rejected",
    reason="invalid_state",
    details={
        "state": "closed",
    },
)

payload = ctx.build_error_payload(err)
```

Payload:

```text
{
    "reason": "invalid_state",
    "kind": "ReasonedError",
    "message": "operation rejected",
    "details": {
        "state": "closed",
    },
}
```

The reason code is intended for diagnostics, tests, metrics, or downstream processing where matching on message text would be fragile.

## RuntimeExtendedError

`RuntimeExtendedError` combines `RuntimeError` behavior with structured diagnostic payloads.

It can also include optional source metadata:

* `module`;
* `qualname`.

These fields are included in the error payload when provided.

```python
from mvx.common.errors import RuntimeExtendedError

err = RuntimeExtendedError(
    message="runtime operation failed",
    module="my_app.worker",
    qualname="Worker.run",
    details={
        "worker_id": "worker-1",
    },
)

payload = ctx.build_error_payload(err)
```

Payload:

```text
{
    "module": "my_app.worker",
    "qualname": "Worker.run",
    "kind": "RuntimeExtendedError",
    "message": "runtime operation failed",
    "details": {
        "worker_id": "worker-1",
    },
}
```

## InvalidFunctionArgumentError

`InvalidFunctionArgumentError` is intended for argument validation failures.

It adds structured details about:

* the function name;
* the argument name;
* the validation error type;
* the offending value, when provided;
* any additional details.

```python
from mvx.common.errors import InvalidFunctionArgumentError

try:
    int("abc")
except ValueError as exc:
    err = InvalidFunctionArgumentError(
        func="parse_count",
        arg="value",
        value="abc",
        cause=exc,
    )

payload = ctx.build_error_payload(err)
```

The resulting payload is produced through `StructuredError.to_log_payload()` and contains the validation details under `details`.

## Already logged errors

The same exception object may pass through several layers.

Logging it with full details at every layer can produce repeated noisy records.

`LogContext` provides two helper methods for this case:

```python
ctx.is_error_logged(exc)
ctx.mark_error_logged(exc)
```

Example:

```python
try:
    run_operation()
except Exception as exc:
    if not ctx.is_error_logged(exc):
        ctx.log_error_event(
            event="operation.failed",
            payload=ctx.build_error_payload(exc),
        )
        ctx.mark_error_logged(exc)
    raise
```

The mark is applied to the exception object on a best-effort basis.

If the exception object does not allow setting an internal attribute, the mark operation is silently ignored.

## Normalization after building error payload

`build_error_payload()` builds a payload dictionary.

It does not bypass the normal logging pipeline.

When the payload is passed to `log_error_event()`, the configured payload processor still normalizes it by default.

```python
ctx.log_error_event(
    event="operation.failed",
    payload=ctx.build_error_payload(exc),
)
```

If the payload is already prepared exactly as it should appear in the final `LogEvent`, normalization can be skipped explicitly.

```python
ctx.log_error_event(
    event="operation.failed",
    payload=ctx.build_error_payload(exc),
    skip_payload_normalization=True,
)
```

Use this only when the payload is already log-ready and should not be changed by the configured payload processor.

## What to remember

* Use `build_error_payload()` to convert exceptions into structured event payloads.
* If an exception provides a valid `to_log_payload()` method, that payload is used.
* Otherwise, the generic payload includes at least `kind` and `message`, and may include `code` and `code_desc`.
* `StructuredError`, `ReasonedError`, `RuntimeExtendedError`, and `InvalidFunctionArgumentError` already provide structured logging payloads.
* Use `is_error_logged()` and `mark_error_logged()` to avoid repeated detailed logging of the same exception object.
