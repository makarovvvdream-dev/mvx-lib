# Invocation lifecycle

```{contents} Contents:
:depth: 1
:local:
```

## Overview

`log_invocation` represents one decorated public API operation as one event with different outcomes.

The event is the operation named in the decorator:

```python
@log_invocation("bind")
async def bind(self, dn: str, password: str) -> None:
    ...
```

Here `bind` is the event.

The lifecycle of that event is described by `event_outcome`:

```text
invoke
success
failed
cancelled
```

The decorator does not create four different events. It creates log records for the same event name with different
outcomes.

## Lifecycle shape

The shapes below describe calls where a logging context has been resolved.

The normal successful lifecycle is:

```text
call decorated function
   |
   v
resolve context and entity id
   |
   v
build LogEventMeta
   |
   v
check event policy
   |
   v
emit invoke outcome if enabled
   |
   v
run operation
   |
   v
emit success outcome if enabled
   |
   v
return original result
```

The failure lifecycle is:

```text
call decorated function
   |
   v
resolve context and entity id
   |
   v
build LogEventMeta
   |
   v
check event policy
   |
   v
emit invoke outcome if enabled
   |
   v
run operation
   |
   v
operation raises exception
   |
   v
emit failed outcome
   |
   v
re-raise original exception
```

The cancellation lifecycle is:

```text
call decorated function
   |
   v
resolve context and entity id
   |
   v
build LogEventMeta
   |
   v
check event policy
   |
   v
emit invoke outcome if enabled
   |
   v
run operation
   |
   v
operation is cancelled
   |
   v
emit cancelled outcome
   |
   v
re-raise original cancellation
```

## Setup before the operation runs

Before the wrapped operation body runs, the decorator prepares the logging frame.

It extracts bound function arguments:

```text
args + kwargs -> inspect.Signature.bind(...) -> effective_kwargs
```

Then it resolves the logging context. If `ctx` was passed to the decorator, that explicit context is used. Otherwise,
the decorator expects the first positional argument to provide a context through `get_log_context()`.

If `get_log_context()` returns `None`, decorator-driven lifecycle logging is disabled for the current call. The
decorator does not resolve `entity_id`, does not build `LogEventMeta`, does not check event policy, and does not emit
`invoke`, `success`, `failed`, or `cancelled` outcomes. The wrapped operation is executed normally.

Then it resolves `entity_id`. If `entity_id_getter` was passed to the decorator, that getter is used. Otherwise, the
decorator checks whether the first positional argument provides an `entity_id` property.

Then it builds `LogEventMeta`:

```python
LogEventMeta(
    event_namespace=effective_ctx.namespace,
    event_name=event,
    entity_id=entity_id,
    source_path=None,
    source_line=None,
    source_func=None,
)
```

The decorator does not populate source file, source line, or source function information.

## Event policy check

After metadata is created, the decorator checks the context event policy:

```python
event_enabled = effective_ctx.is_event_enabled(event_meta)
```

This decision controls normal operation tracing. If the event is enabled - invoke and success outcomes are emitted.
Otherwise, they are suppressed.

Failure and cancellation paths are still processed separately.

This gives a useful behavior: ordinary operation tracing can be filtered by policy, while exceptional outcomes remain
visible.

## Invoke outcome

The `invoke` outcome is emitted before the wrapped operation body runs if the event is enabled by policy.

The `invoke` payload may contain:

```text
closures
context fields
selected arguments under kwargs
```

The decorator emits it with the level specified in the decorator call argument `invoke_level`. The default level is
`LogLevel.DEBUG`.

Because `invoke` is emitted before the operation body runs, context fields that read mutable object state show the state
before the operation.

For example, an `open()` operation may emit:

```text
event_name     = "open"
event_outcome  = "invoke"
payload.state  = "closed"
```

## Success outcome

The `success` outcome is emitted after the wrapped operation completes successfully. It is emitted only if
`event_enabled` was `True` before the operation started.

The `success` payload may contain:

```text
context fields
result, if log_result_on_success is not None
```

The decorator emits it with the level specified in the decorator call argument `success_level`. The default level is
`LogLevel.DEBUG`.

The original result is returned unchanged.

```text
operation result -> success logging -> same result returned
```

Because `success` is emitted after the operation body completes, dynamic context fields may reflect the final state of
the operation.

For example, the same `open()` operation may emit:

```text
event_name     = "open"
event_outcome  = "success"
payload.state  = "opened"
```

## Failed outcome

The `failed` outcome is emitted when the wrapped operation raises an exception other than `asyncio.CancelledError`.

The decorator emits the failed outcome and then re-raises the original exception.

```text
operation raises err
   |
   v
emit failed outcome
   |
   v
raise same err
```

The `failed` payload may contain:

```text
context fields
error payload
```

The error payload is built through the resolved context:

```python
effective_ctx.build_error_payload(err)
```

The decorator emits it with the level specified in the decorator call argument `error_level` or
`error_level_suppressed`. The default full error level is `LogLevel.ERROR`. The default suppressed error level is
`LogLevel.DEBUG`.

The original exception is not converted into a logger exception.

## Cancelled outcome

The `cancelled` outcome is emitted when the wrapped operation raises `asyncio.CancelledError`.

Cancellation is handled separately from ordinary failure.

The decorator emits the cancelled outcome and then re-raises the same cancellation.

```text
operation is cancelled
   |
   v
emit cancelled outcome
   |
   v
raise same CancelledError
```

The `cancelled` payload contains:

```text
cancelled = True
error     = build_error_payload(cancelled_error)
```

It may also contain context fields.

The decorator emits it with the level specified in the decorator call argument `cancel_level`. The default level is
`LogLevel.INFO`.

If the same cancellation exception instance is already marked as logged, the decorator does not emit another `cancelled`
outcome.

## Outcome ordering

For successful execution:

```text
invoke -> success
```

For failed execution:

```text
invoke -> failed
```

For cancelled execution:

```text
invoke -> cancelled
```

If the event is disabled by policy, the normal tracing pair is suppressed:

```text
success path:    no invoke -> no success
failure path:    no invoke -> failed
cancelled path:  no invoke -> cancelled
```

This behavior is intentional. Event policy controls regular operation tracing. Error and cancellation outcomes still
pass through their dedicated paths.

## Context fields during lifecycle

`context_fields` are resolved separately for each emitted outcome.

The decorator captures bound function arguments once at the beginning of the wrapper call. However, field paths are
evaluated again when each outcome is being emitted.

This means context fields can reflect state changes made by the operation.

Example:

```python
from mvx.common.logger import LogContextProto, log_invocation


class Connection:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context
        self.state = "closed"

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context

    @log_invocation(
        "open",
        context_fields=("state=self.state",),
    )
    async def open(self) -> None:
        self.state = "opened"
```

The `invoke` outcome is emitted before the method body runs, so it can contain:

```text
state = "closed"
```

The `success` outcome is emitted after the method body completes, so it can contain:

```text
state = "opened"
```

## Sync functions

For a regular synchronous function, the decorator runs the function directly.

The successful shape is:

```text
emit invoke
call function
emit success
return result
```

The failure shape is:

```text
emit invoke
call function
function raises
emit failed
re-raise exception
```

## Async functions

For an `async def` function, the decorator awaits the function.

The successful shape is:

```text
emit invoke
await function
emit success
return awaited result
```

The failure shape is:

```text
emit invoke
await function
function raises
emit failed
re-raise exception
```

The cancellation shape is:

```text
emit invoke
await function
function raises CancelledError
emit cancelled
re-raise cancellation
```

## Synchronous functions returning awaitables

A synchronous function may return an awaitable. In that case, the decorator does not emit success immediately after the
synchronous call returns the awaitable. Instead, it wraps the awaitable and logs the final outcome when the awaitable
completes.

The shape is:

```text
emit invoke
call function
function returns awaitable
return wrapper awaitable
   |
   v
await original awaitable
   |
   +--> success    -> emit success, return awaited result
   +--> failure    -> emit failed, re-raise exception
   +--> cancelled  -> emit cancelled, re-raise cancellation
```

This preserves correct lifecycle logging for APIs that are synchronous at the call boundary but produce asynchronous
work.

## Error payload and repeated errors

For failures, the decorator decides whether to log a full error payload or a suppressed failed outcome.

If `log_error_policy` matches the exception type, that rule is applied.

If no policy rule applies and the exception is not already marked as logged, the decorator emits a full failed outcome
and marks the exception as logged.

If the exception is already marked as logged, the decorator emits a suppressed failed outcome.

The full failed outcome includes:

```text
error = effective_ctx.build_error_payload(err)
```

The suppressed failed outcome omits the detailed `error` payload and uses level, defined in `error_level_suppressed`
argument.

The original exception is always re-raised.

## Payload timing summary

Payload pieces are collected at different lifecycle moments.

```text
bound arguments
    captured once when wrapper is called

closures
    added to invoke payload only

log_kwargs_on_invoke
    resolved for invoke only

context_fields
    resolved separately for every emitted outcome

result
    resolved only for success, when result logging is enabled

error
    resolved only for failed or cancelled outcomes
```

This timing is important when selected fields point to mutable objects or changing object state.

## What lifecycle logging does not change

* `log_invocation` does not change the operation result.
* It does not swallow exceptions.
* It does not convert cancellation into ordinary failure.
* It does not bypass `LogContext` event policy for normal tracing.
* It does not bypass `LogContext` sink delivery or payload normalization behavior.
* It records lifecycle outcomes around the operation, then lets the operation keep its original semantics.

## Design summary

`log_invocation` logs the lifecycle of one decorated public API operation.

The decorated operation is the event.

`event_outcome` records the lifecycle stage or result:

```text
invoke
success
failed
cancelled
```

The decorator emits normal tracing outcomes only when the event is enabled by policy.

Failure and cancellation are still reported through their dedicated paths.

The original return value, exception, and cancellation semantics are preserved.
