# Error and cancellation handling

```{contents} Contents:
:depth: 1
:local:
```

## Overview

`log_invocation` records failed and cancelled operation outcomes without changing the operation semantics.

If the decorated operation raises an ordinary exception, the decorator can emit a `failed` outcome and then re-raise the
same exception.

If the decorated operation raises `asyncio.CancelledError`, the decorator can emit a `cancelled` outcome and then
re-raise the same cancellation.

The core rule is:

```text
record the outcome
preserve the original exception or cancellation
```

The decorator does not turn domain exceptions into logger exceptions, and it does not convert cancellation into ordinary
failure.

## Failure vs cancellation

Failure and cancellation are separate outcomes.

Ordinary exceptions are logged with:

```text
event_outcome = "failed"
```

`asyncio.CancelledError` is logged with:

```text
event_outcome = "cancelled"
```

This distinction matters because cancellation is part of asyncio control flow. It usually means that the operation was
stopped from outside or by task orchestration, not that the operation produced an ordinary domain failure.

## Failed outcome

The `failed` outcome is emitted when the decorated operation raises an exception other than `asyncio.CancelledError`.

The shape is:

```text
operation raises exception
   |
   v
emit failed outcome
   |
   v
re-raise same exception
```

The failed payload may contain:

```text
context fields
error payload
```

The error payload is built through the resolved context:

```python
effective_ctx.build_error_payload(err)
```

The decorator does not build the error representation directly. It asks the context to do it, so the same error-payload
rules are shared with the rest of the logger infrastructure.

## Cancelled outcome

The `cancelled` outcome is emitted when the decorated operation raises `asyncio.CancelledError`.

The shape is:

```text
operation raises CancelledError
   |
   v
emit cancelled outcome
   |
   v
re-raise same CancelledError
```

The cancelled payload contains:

```text
cancelled = True
error     = effective_ctx.build_error_payload(cancelled_error)
```

It may also contain `context_fields`.

Cancellation is not treated as a normal failed operation. It has its own outcome and its own default level.

If the same cancellation exception instance is already marked as logged, the decorator does not emit another `cancelled`
outcome.

## Levels

Failure and cancellation use separate level settings.

```python
from mvx.common.logger import LogLevel, log_invocation


@log_invocation(
    "send_request",
    error_level=LogLevel.ERROR,
    error_level_suppressed=LogLevel.DEBUG,
    cancel_level=LogLevel.INFO,
)
async def send_request(self) -> None:
    ...
```

The defaults are:

```text
failed, full error payload        LogLevel.ERROR
failed, suppressed error payload  LogLevel.DEBUG
cancelled                         LogLevel.INFO
```

This allows failures, repeated/suppressed failures, and cancellations to have different visibility in the final logs.

## Error payload

For a full failed outcome, the payload contains an `error` field.

Conceptually:

```python
{
    "error": {
        "kind": "...",
        "message": "...",
        ...
    }
}
```

The concrete shape is produced by:

```python
effective_ctx.build_error_payload(err)
```

That method can use structured data provided by the exception itself or fall back to generic error fields.

The decorator only decides whether to include the full error payload or suppress it for a given failure.

## Repeated error suppression

The decorator uses context-level error marker helpers:

```text
is_error_logged(err)
mark_error_logged(err)
```

If an exception instance has not been logged with full details yet, the decorator emits a full failed outcome and marks
that exception as logged.

If the same exception instance is seen again, the decorator emits a suppressed failed outcome.

The suppressed outcome:

```text
uses error_level_suppressed
omits the detailed error payload
still records that the operation failed
```

This avoids repeating the same error body multiple times while preserving the operation outcome.

The original exception is still re-raised.

## log_error_policy

`log_error_policy` lets a decorated operation define explicit rules for particular exception types.

The type shape is:

```python
LogErrorPolicyRule = tuple[type[BaseException], bool]
LogErrorPolicy = tuple[LogErrorPolicyRule, ...]
```

Each rule contains:

```text
exception type
force_log flag
```

The decorator checks rules in order and applies the first matching exception type.

## force_log=True

If a matching rule has `force_log=True`, the decorator emits a full failed outcome even for that exception type.

```python
@log_invocation(
    "load_config",
    log_error_policy=((FileNotFoundError, True),),
)
def load_config(self, path: str) -> dict[str, object]:
    ...
```

The failed payload includes:

```text
error = effective_ctx.build_error_payload(err)
```

The exception is marked as logged after the full failed outcome is emitted.

## force_log=False

If a matching rule has `force_log=False`, the decorator emits a suppressed failed outcome for that exception type.

```python
@log_invocation(
    "try_load_optional_config",
    log_error_policy=((FileNotFoundError, False),),
)
def try_load_optional_config(self, path: str) -> dict[str, object]:
    ...
```

The failed outcome is still emitted, but without the detailed `error` payload.

It uses `error_level_suppressed`.

The exception is marked as logged after the suppressed failed outcome is emitted.

This is useful when an exception type is expected, already logged elsewhere, or too noisy for full repeated error
payloads.

## Policy and repeated-error marker

`log_error_policy` is checked before the default repeated-error behavior.

The order is:

```text
if log_error_policy has a matching rule:
    apply that rule
else:
    use is_error_logged(err) / mark_error_logged(err)
```

A matching rule with `force_log=True` emits a full error payload.

A matching rule with `force_log=False` emits a suppressed failed outcome.

If no rule matches, the decorator uses the repeated-error marker to decide between full and suppressed failed outcomes.

## Context fields on failure and cancellation

`context_fields` are still resolved for failed and cancelled outcomes.

They are resolved at the time the failure or cancellation outcome is emitted.

This means dynamic context fields may reflect state changes made before the operation failed or was cancelled.

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
        self.state = "opening"
        raise RuntimeError("connection failed")
```

The `invoke` outcome can contain:

```text
state = "closed"
```

The `failed` outcome can contain:

```text
state = "opening"
```

## Disabled event policy

The decorator checks event policy before the operation body runs.

If the event is disabled by policy:

```text
invoke is not emitted
success is not emitted
```

Failure and cancellation paths are still processed separately.

So the possible shapes are:

```text
success path:    no invoke -> no success
failure path:    no invoke -> failed
cancelled path:  no invoke -> cancelled
```

This lets policy suppress ordinary operation tracing while still allowing exceptional outcomes to be visible.

## Exceptions during setup

Some failures happen before the decorated operation body starts.

Examples:

```text
ctx is not supplied and the first positional argument does not provide LogContextProviderProto
get_log_context() raises
entity_id_getter raises
entity_id property access raises
```

Returning `None` from `get_log_context()` is not a setup failure and is not logged as a `failed` outcome.

These are decorator integration failures, not failures of the decorated operation.

They happen before `LogEventMeta` is fully prepared and before the `invoke` outcome can be emitted.

They are intentionally not logged as `failed` outcomes of the decorated event.

Such errors usually mean the decorator was embedded incorrectly. It is better for them to surface immediately than to be
swallowed or reported as operation failures.

## Errors while logging

This article describes errors raised by the decorated operation.

It is separate from logging infrastructure failures.

If the decorator tries to emit a `LogEvent` and the configured sink fails, that is handled by `LogContext` according to
its `LogErrorHandlingPolicy`.

That policy controls logging infrastructure errors, not domain exceptions raised by the decorated operation.

In other words:

```text
operation raises exception
    -> log_invocation failed outcome, then re-raise original exception

sink delivery fails
    -> LogContext error handling policy
```

These two error boundaries should not be mixed.

## What error handling does not change

* `log_invocation` does not swallow ordinary exceptions.
* It does not swallow `asyncio.CancelledError`.
* It does not convert cancellation into failure.
* It does not convert domain exceptions into logger exceptions.
* It does not log setup/integration failures as operation failures.
* It does not guarantee that every repeated exception instance will include a full error payload.
* Its job is to record the operation outcome and preserve the original control flow.

## Design summary

Failed operations use `event_outcome="failed"`.

Cancelled operations use `event_outcome="cancelled"`.

Full failed outcomes include an error payload built by the resolved context.

Suppressed failed outcomes omit detailed error payload and use `error_level_suppressed`.

`log_error_policy` can force full or suppressed logging for selected exception types.

The original exception or cancellation is always re-raised.
