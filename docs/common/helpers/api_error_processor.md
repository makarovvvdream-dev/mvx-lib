# api_error_processor

`api_error_processor` builds a decorator for public API exception normalization.

It is used at public boundaries of library components, where raw internal
exceptions should not leak directly to callers.

## Why it exists

Library internals may raise ordinary Python exceptions:

```text
ValueError
TypeError
AssertionError
KeyError
ZeroDivisionError
```

Those exceptions are often useful inside the implementation, but they are a poor
public API surface. If they leak directly, callers become coupled to internal
implementation details.

`api_error_processor` turns that boundary into a controlled error surface.

Declared public errors pass through unchanged. Unexpected internal exceptions are
wrapped into a configured `RuntimeExtendedError` subclass.

## Error categories

The decorator separates exceptions into three categories.

## Cancellation

`asyncio.CancelledError` is always re-raised unchanged.

Cancellation is control flow, not an application failure. It must not be wrapped.

```python
import asyncio
try:
    ...
except asyncio.CancelledError:
    raise
```

## Passthrough errors

Errors listed in `passthrough_error_types` are re-raised unchanged.

These are expected, declared API errors that callers may handle directly.

```python
from mvx.common.helpers import api_error_processor
from mvx.common.errors import RuntimeExtendedError

class ApiInputError(Exception):
    ...

class ServiceUnexpectedError(RuntimeExtendedError):  
    ...

public_api = api_error_processor(
    passthrough_error_types=(ApiInputError,),
    raise_error_type=ServiceUnexpectedError,
)
```

If the wrapped function raises `ApiInputError`, the decorator does not modify it.

## Existing RuntimeExtendedError

If the exception is already a `RuntimeExtendedError`, it is also re-raised
unchanged.

Before re-raising, the decorator fills missing source metadata:

```text
module
qualname
```

This keeps existing structured errors intact while making logs more informative.

## Unexpected errors

Any other `Exception` is treated as an unexpected internal failure.

The decorator wraps it into `raise_error_type`:

```text
raise_error_type(
    message=f"runtime unexpected error: {exc}",
    module=module,
    qualname=qualname,
    cause=exc,
)
```

The original exception is preserved as the cause.

## Constructor contract

`raise_error_type` must be a `RuntimeExtendedError` subclass.

It should support this constructor shape:

```text
error = raise_error_type(
    message="runtime unexpected error: ...",
    module="...",
    qualname="...",
    cause=original_exception,
)
```

This is the normal path.

The decorator also contains a defensive fallback for misconfigured error classes.
If the full constructor fails, it tries to instantiate the error with only the
message and then assigns the remaining fields manually.

This fallback exists to avoid double-faulting while processing an exception. In a
well-formed package, the primary constructor should always succeed.

## Sync and async support

The decorator supports both synchronous and asynchronous callables.

Synchronous function:

```text
@public_api
def compute(value: int) -> int:
    return value * 2
```

Asynchronous function:

```text
@public_api
async def fetch(value: int) -> int:
    return value
```

Coroutine detection uses `inspect.unwrap()` before checking the callable. This
allows the decorator to work correctly when the function has already been wrapped
by other decorators.

## Basic usage

```python
from mvx.common.errors import RuntimeExtendedError, RuntimeUnexpectedError
from mvx.common.helpers import api_error_processor


class ServiceError(RuntimeExtendedError):
    pass


class ServiceUnexpectedError(ServiceError, RuntimeUnexpectedError):
    pass


class ApiInputError(ValueError):
    pass


public_api = api_error_processor(
    passthrough_error_types=(ApiInputError,),
    raise_error_type=ServiceUnexpectedError,
)
```

## Synchronous example

```python
from mvx.common.errors import RuntimeExtendedError, RuntimeUnexpectedError
from mvx.common.helpers import api_error_processor


class ServiceError(RuntimeExtendedError):
    pass


class ServiceUnexpectedError(ServiceError, RuntimeUnexpectedError):
    pass


class ApiInputError(ValueError):
    pass


public_api = api_error_processor(
    passthrough_error_types=(ApiInputError,),
    raise_error_type=ServiceUnexpectedError,
)
class ExampleService:
    @public_api
    def compute(self, value: int) -> int:
        if value < 0:
            raise ApiInputError("value must be non-negative")

        if value == 13:
            raise AssertionError("unexpected internal invariant")

        return value * 2
```

Declared API errors pass through:

```text
service = ExampleService()

try:
    service.compute(-1)
except ApiInputError:
    handled = True
```

Unexpected errors are wrapped:

```text
try:
    service.compute(13)
except ServiceUnexpectedError as exc:
    payload = exc.to_log_payload()
```

Example payload:

```python
payload = {
    "module": "__main__",
    "qualname": "ExampleService.compute",
    "kind": "ServiceUnexpectedError",
    "message": "runtime unexpected error: unexpected internal invariant",
    "details": {},
    "cause": {
        "kind": "AssertionError",
        "message": "unexpected internal invariant",
    },
}
```

## Asynchronous example

```python
from mvx.common.errors import RuntimeExtendedError, RuntimeUnexpectedError
from mvx.common.helpers import api_error_processor


class ServiceError(RuntimeExtendedError):
    pass


class ServiceUnexpectedError(ServiceError, RuntimeUnexpectedError):
    pass


class ApiInputError(ValueError):
    pass


public_api = api_error_processor(
    passthrough_error_types=(ApiInputError,),
    raise_error_type=ServiceUnexpectedError,
)
class ExampleService:
    @public_api
    async def fetch(self, value: int) -> int:
        if value == 0:
            raise ZeroDivisionError("division by zero")

        return 10 // value
```

The async path uses the same exception policy:

```text
service = ExampleService()

try:
    result = await service.fetch(0)
except ServiceUnexpectedError as exc:
    payload = exc.to_log_payload()
```

## Public API pattern

The intended pattern is to create one decorator instance per component or module:

```text
public_api = api_error_processor(
    passthrough_error_types=(ServiceError,),
    raise_error_type=ServiceUnexpectedError,
)
```

Then apply it to public methods:

```text
class Service:
    @public_api
    def method(self) -> None:
        ...
```

This keeps public error behavior consistent across the component.

## Design rule

Use `api_error_processor` at public API boundaries.

Do not use it deep inside implementation code. Internal layers should raise
specific errors naturally. The public boundary is where those errors are either
allowed to pass through or are mapped into the public unexpected-error type.

The decorator should make the public surface stable without hiding useful
diagnostic information.

## API

```{eval-rst}
.. autofunction:: mvx.common.helpers.api_error_processor
```