# InvalidFunctionArgumentError

`InvalidFunctionArgumentError` is a structured error for function argument
validation failures.

It is used when a function detects that one of its input arguments is invalid
and the failure should be reported with structured diagnostic context.

## Why it exists

Argument validation often starts with ordinary exceptions:

```python
raise ValueError("offset must be greater than or equal to 0")
```

That is readable, but it loses useful structured context:

- which function rejected the argument;
- which argument failed validation;
- what kind of validation error happened;
- which value caused the failure, when it is safe to log.

`InvalidFunctionArgumentError` wraps the original validation exception and turns
that information into a stable structured payload.

## Basic usage

```python
from mvx.common.errors import InvalidFunctionArgumentError


def read_range(offset: int) -> None:
    if offset < 0:
        cause = ValueError("offset must be greater than or equal to 0")

        raise InvalidFunctionArgumentError(
            func="read_range",
            arg="offset",
            value=offset,
            cause=cause,
        ) from cause
```

The resulting error message is based on the wrapped cause:

```text
InvalidFunctionArgumentError: invalid argument -> offset must be greater than or equal to 0
```

The structured details include the function name, argument name, validation error
type, and value:

```python
payload = {
    "kind": "InvalidFunctionArgumentError",
    "message": "invalid argument -> offset must be greater than or equal to 0",
    "details": {
        "func": "read_range",
        "arg": "offset",
        "error_type": "ValueError",
        "value": -1,
    },
    "cause": {
        "kind": "ValueError",
        "message": "offset must be greater than or equal to 0",
    },
}
```

## Function and argument names

The `func` and `arg` fields identify where validation failed.

```python
from mvx.common.errors import InvalidFunctionArgumentError

raise InvalidFunctionArgumentError(
    func="connect",
    arg="timeout",
    value=-5,
    cause=ValueError("timeout must be positive"),
)
```

If `func` or `arg` is `None` or an empty string, it is normalized to:

```text
<unknown>
```

This keeps the payload shape stable even when the caller cannot provide full
context.

## Value

The `value` field is optional.

Use it only when the value is safe and useful to log:

```python
from mvx.common.errors import InvalidFunctionArgumentError

raise InvalidFunctionArgumentError(
    func="set_limit",
    arg="limit",
    value=-1,
    cause=ValueError("limit must be greater than 0"),
)
```

Do not pass secrets, credentials, tokens, private keys, or large payloads as
`value`.

For sensitive values, either omit `value` or pass a redacted representation:

```python
from mvx.common.errors import InvalidFunctionArgumentError

raise InvalidFunctionArgumentError(
    func="login",
    arg="password",
    value="<redacted>",
    cause=ValueError("password is empty"),
)
```

## Extra details

Additional diagnostic context can be passed through `details`.

```python
from mvx.common.errors import InvalidFunctionArgumentError

cause = ValueError("page size is too large")

raise InvalidFunctionArgumentError(
    func="list_users",
    arg="page_size",
    value=10_000,
    cause=cause,
    details={
        "max_page_size": 1_000,
        "source": "query_params",
    },
) from cause
```

The extra details are merged into the default payload:

```python
payload = {
    "kind": "InvalidFunctionArgumentError",
    "message": "invalid argument -> page size is too large",
    "details": {
        "func": "list_users",
        "arg": "page_size",
        "error_type": "ValueError",
        "value": 10000,
        "max_page_size": 1000,
        "source": "query_params",
    },
    "cause": {
        "kind": "ValueError",
        "message": "page size is too large",
    },
}
```

## Design rule

Use `InvalidFunctionArgumentError` at function boundaries where argument
validation fails and the failure should be visible in logs or tests as
structured data.

It is especially useful for shared helper functions and infrastructure code,
where a plain `ValueError` would be too vague.

Do not use it for protocol-level errors, transport errors, or business rule
failures. Those should have their own domain-specific error types.

## API

```{eval-rst}
.. autoclass:: mvx.common.errors.InvalidFunctionArgumentError
   :members:
   :show-inheritance:
```