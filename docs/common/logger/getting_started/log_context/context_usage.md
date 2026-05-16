# Context usage

```{contents} Contents:
:depth: 1
:local:
```

After a context has been created, it is used for the usual logging work: emitting events, preparing payloads, and working with errors.

This page does not describe all internal details of `LogContext`. It shows the main scenarios needed in everyday use.

## Logging events

The main way to emit an event is to call one of the context methods for a specific logging level.

```python
from mvx.common.logger import LogVerbosityLevel, configure_log_context

ctx = configure_log_context("my_app.worker")


ctx.log_debug_event(
    event="cache.lookup",
    payload={
        "key": "user:42",
    },
)

ctx.log_info_event(
    event="started",
    payload={
        "worker_id": "worker-1",
    },
)

ctx.log_warning_event(
    event="config.fallback_used",
    payload={
        "name": "APP_CONFIG_PATH",
        "fallback": "default",
    },
)

ctx.log_error_event(
    event="failed",
    payload={
        "worker_id": "worker-1",
    },
)
```

These methods differ only by the event level.

If the level should be selected explicitly, use the general `log_event()` method:

```python
from mvx.common.logger import LogLevel, configure_log_context

ctx = configure_log_context("my_app.worker")

ctx.log_event(
    event="worker.started",
    level=LogLevel.INFO,
    payload={
        "worker_id": "worker-1",
    },
)
```

In the simple case, it is enough to pass an event name and a payload.

```python
from mvx.common.logger import LogLevel, configure_log_context

ctx = configure_log_context("my_app")

ctx.log_info_event(
    event="request.completed",
    payload={
        "status": 200,
        "duration_ms": 37,
    },
)
```

The context adds the event namespace itself. If the context is named `my_app.api`, then the event `request.completed` is written as an event from the `my_app.api` area.

If needed, the namespace can be overridden explicitly with `event_namespace`.

```python
from mvx.common.logger import LogLevel, configure_log_context

ctx = configure_log_context("my_app")

ctx.log_info_event(
    event="request.completed",
    event_namespace="my_app.http",
    payload={
        "status": 200,
    },
)
```

Additional event fields can also be passed:

```python
from mvx.common.logger import LogLevel, configure_log_context

ctx = configure_log_context("my_app")

ctx.log_info_event(
    event="request.completed",
    event_namespace="my_app.http",
    event_type="success",
    entity_id="request-42",
    source_path="app/api.py",
    source_line=120,
    source_func="handle_request",
    payload={
        "status": 200,
    },
)
```

These fields are optional. They are useful when an event should be connected to a specific entity, result type, or source-code location.

Before sending an event, the context checks the event policy if one is set for that context. If the policy rejects the event, it is not passed to the sink.

## Working with exceptions

`LogContext` provides helper methods for working with exceptions.

The main method is `build_error_payload()`.

It turns an exception into a payload that can be passed to an event.

```python
from mvx.common.logger import LogLevel, configure_log_context

ctx = configure_log_context("my_app")

def run_operation()-> None:
    ...

try:
    run_operation()
except Exception as exc:
    ctx.log_error_event(
        event="operation.failed",
        payload=ctx.build_error_payload(exc),
    )
```

If the exception provides a `to_log_payload()` method, the context uses its result.

```python
class OperationError(Exception):
    def to_log_payload(self) -> dict[str, object]:
        return {
            "kind": type(self).__name__,
            "message": str(self),
            "reason": "operation_failed",
        }
```

If `to_log_payload()` is not available, or if it cannot return a valid payload, the context creates a basic error representation.

The basic payload includes:

* `kind` — the exception class name;
* `message` — the string representation of the exception.

If the exception has `code` or `code_desc` attributes, they are also added to the payload.

The `mvx.common` package already provides base error classes that fit this logging model.

`StructuredError` stores a human-readable message, a `details` dictionary, and an optional original `cause`. For logging, it provides `to_log_payload()`, which returns a stable dictionary with `kind`, `message`, `details`, and, when present, a nested description of `cause`.

```python
from mvx.common.errors import StructuredError

raise StructuredError(
    message="operation failed",
    details={
        "operation": "sync_users",
        "user_count": 42,
    },
)
```

Such an error can be passed directly to `build_error_payload()`:

```python
from mvx.common.logger import LogLevel, configure_log_context
from mvx.common.errors import StructuredError

ctx = configure_log_context("my_app")

def run_operation()-> None:
    ...

try:
    run_operation()
except StructuredError as exc:
    ctx.log_error_event(
        event="operation.failed",
        payload=ctx.build_error_payload(exc),
    )
```

`ReasonedError` extends `StructuredError` and adds a stable `reason_code`. This is useful when an error should be classified not only by its message, but also by a machine-readable reason.

```python
from mvx.common.errors import ReasonedError

raise ReasonedError(
    message="operation rejected",
    reason="invalid_state",
    details={
        "state": "closed",
    },
)
```

`RuntimeExtendedError` combines `RuntimeError` behavior with a structured payload and can additionally store `module` and `qualname`. It is useful for runtime errors that should keep the usual `RuntimeError` semantics while also being convenient for logging.

`RuntimeUnexpectedError` is a marker class for errors classified as unexpected. It does not replace a domain error hierarchy; it is intended for multiple inheritance with concrete domain errors.

`InvalidFunctionArgumentError` is used for function argument validation errors. It adds the function name, argument name, original error type, and, when provided, the argument value to `details`.

All these classes are designed to work with `LogContext`: if an error provides a valid `to_log_payload()`, the context uses it when building the error payload.

The context can also mark a specific exception object as already logged.

```python
from mvx.common.logger import LogLevel, configure_log_context

ctx = configure_log_context("my_app")

def run_operation()-> None:
    ...

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

This is useful when the same error passes through several layers of code. The first layer can log a detailed payload and mark the exception, while the next layer can check that mark and avoid writing the same detailed record again.

The mark is applied on a best-effort basis. If the exception object does not allow adding the internal attribute, the error raised while setting the mark is ignored.

## Payload normalization helper functions

Sometimes a payload is assembled manually before calling `log_info_event()` or another logging method.

In this case, the context can be used as a single normalization point for values.

```python
from mvx.common.logger import LogLevel, configure_log_context
from dataclasses import dataclass

ctx = configure_log_context("my_app")

@dataclass
class BindOutcome:
    result: bool
    error: Exception | None

# just samples
result = BindOutcome(result=True,error=None)
items = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]
metadata = {
    "some_key": "some_value",
    "another_key": "another_value",
}

payload = {
    "result": ctx.normalize_value_for_log(result),
    "items": ctx.normalize_list_for_log(items),
    "metadata": ctx.normalize_dict_for_log(metadata),
}

ctx.log_info_event(
    event="operation.completed",
    payload=payload,
)
```

The context provides several helper methods:

* `normalize_value_for_log(value)` — normalizes an arbitrary value;
* `normalize_primitive_for_log(value)` — normalizes a primitive value;
* `normalize_list_for_log(value)` — normalizes a list or tuple;
* `normalize_dict_for_log(value)` — normalizes a dictionary.

These methods use the context settings:

* `verbosity_level`;
* `max_str_len`;
* `max_items`;
* `log_adapter_resolver`.

For example, if the context has `max_str_len=100`, long strings are shortened according to this setting during payload normalization.

If `max_items=10` is set, large lists and dictionaries are limited to that number of items.

For `normalize_value_for_log()`, `normalize_list_for_log()`, and `normalize_dict_for_log()`, `unbounded=True` can be passed.

```python
from mvx.common.logger import LogLevel, configure_log_context

ctx = configure_log_context("my_app")

items = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]

payload = {
    "all_items": ctx.normalize_list_for_log(items, unbounded=True),
}
```

In this case, the `max_items` limit is not applied to this specific call.

These helper methods are useful when user code wants to prepare payload values before passing them to a logging method.

The logging methods do not normalize the payload automatically. They receive the payload provided by the caller and put it into the `LogEvent`.

The helper methods let user code apply the context settings explicitly: `verbosity_level`, `max_str_len`, `max_items`, and `log_adapter_resolver`.

This keeps manually prepared payload values consistent with the same normalization rules used elsewhere in the logger ecosystem.
