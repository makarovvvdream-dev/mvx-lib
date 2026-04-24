# ReasonedError

`ReasonedError` is a `StructuredError` subclass that adds a stable reason code.

It is used when an error needs not only a human-readable message and structured
details, but also a machine-readable classifier.

## Why it exists

Some errors must be interpreted programmatically.

A class name alone is often too coarse:

```text
InvalidFunctionArgumentError
```

while the actual reason may vary:

```text
empty
wrong_type
too_long
```

Relying on exception messages for this is fragile. `ReasonedError` introduces a
dedicated field (`reason_code`) for this purpose.

## Basic usage

```python
from mvx.common.errors import ReasonedError

error = ReasonedError(
    message="Invalid function argument",
    reason="empty",
    details={"argument": "username"},
)

payload = error.to_log_payload()
```

The log payload includes the reason when present:

```python
payload = {
    "reason": "empty",
    "kind": "ReasonedError",
    "message": "Invalid function argument",
    "details": {
        "argument": "username",
    },
}
```

## Reason codes

Reason codes should be:

- short;
- stable;
- machine-readable;
- safe for logging.

Good examples:

```text
empty
wrong_type
too_long
not_found
already_closed
```

Bad examples:

```text
User name is empty
Something went wrong
Invalid value passed to the function
```

## Using StrEnum

A recommended practice is to define reason codes using `StrEnum`.

This provides:

- a closed set of allowed values;
- IDE support and discoverability;
- protection against typos;
- compatibility with logging and serialization.

Example:

```python
from enum import StrEnum

from mvx.common.errors import ReasonedError


class ValidationReason(StrEnum):
    EMPTY = "empty"
    WRONG_TYPE = "wrong_type"
    TOO_LONG = "too_long"


error = ReasonedError(
    message="Invalid function argument",
    reason=ValidationReason.EMPTY,
    details={"argument": "username"},
)
```

`StrEnum` values behave like strings:

```text
reason = str(ValidationReason.EMPTY)
```

This makes them safe to use in logs, JSON, and other external representations.

## Design rule

Use `ReasonedError` when the reason must be treated as data.

If the error only needs a message and structured context, use `StructuredError`
or a direct subclass of it.

## API

```{eval-rst}
.. autoclass:: mvx.common.errors.ReasonedError
   :members:
   :show-inheritance:
```