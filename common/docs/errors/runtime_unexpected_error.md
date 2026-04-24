# RuntimeUnexpectedError

`RuntimeUnexpectedError` is a marker base class for runtime errors classified as
unexpected.

It is not meant to replace a domain-specific error hierarchy. It adds a second
classification axis: whether a runtime error represents an unexpected failure.

## Why it exists

Some errors belong to a concrete domain, but also need to be marked as
unexpected.

For example, a networking layer may have its own runtime error hierarchy:

```text
TcpStreamEngineBaseError
├── TcpStreamEngineNotOpenError
├── TcpStreamEngineUnexpectedlyClosingError
└── TcpStreamEngineUnexpectedError
```

The last error belongs to the TCP stream engine domain, but it also carries a
cross-cutting meaning:

```text
this failure is unexpected
```

`RuntimeUnexpectedError` provides that meaning as a marker.

## Basic usage

A concrete error can inherit from both its domain base error and
`RuntimeUnexpectedError`:

```text
from mvx.common.errors import RuntimeExtendedError, RuntimeUnexpectedError


class TcpStreamEngineBaseError(RuntimeExtendedError):
    pass


class TcpStreamEngineUnexpectedError(
    TcpStreamEngineBaseError,
    RuntimeUnexpectedError,
):
    pass
```

The resulting class keeps both meanings:

```text
error = TcpStreamEngineUnexpectedError(
    message="Unexpected stream engine failure",
)

is_runtime_domain_error = isinstance(error, TcpStreamEngineBaseError)
is_unexpected_error = isinstance(error, RuntimeUnexpectedError)
```

## Why it is a marker

`RuntimeUnexpectedError` is intentionally small. It does not define structured
payload fields, logging behavior, or constructor parameters.

Those responsibilities belong to the concrete domain error, usually through
`RuntimeExtendedError`.

The marker answers only one question:

```text
Should this error be classified as unexpected?
```

## Error mapping

Higher-level code can use the marker during error mapping:

```python
from mvx.common.errors import RuntimeUnexpectedError

try:
    ...
except Exception as exc:
    if isinstance(exc, RuntimeUnexpectedError):
        classification = "unexpected"
    else:
        classification = "runtime"
```

This avoids fragile checks based on class names or message text.

## Multiple inheritance

The intended pattern is multiple inheritance:

```text
class SomeUnexpectedDomainError(
    SomeDomainBaseError,
    RuntimeUnexpectedError,
):
    pass
```

The domain base class describes where the error belongs.

The marker describes how the error should be classified.

This keeps the model two-dimensional:

```text
domain hierarchy     → networking, LDAP, storage, schema, ...
classification axis  → expected / unexpected
```

## Design rule

Use `RuntimeUnexpectedError` only as a marker for concrete runtime errors that
should be classified as unexpected by higher layers.

Do not raise `RuntimeUnexpectedError` directly. Raise a concrete domain-specific
error that also inherits from it.

## API

```{eval-rst}
.. autoclass:: mvx.common.errors.RuntimeUnexpectedError
   :members:
   :show-inheritance:
```