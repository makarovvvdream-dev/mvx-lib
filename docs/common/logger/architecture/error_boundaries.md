# Error boundaries

```{contents} Contents:
:depth: 1
:local:
```

## Overview

`MVX Logger` separates domain errors from logging infrastructure errors.

A domain error is an exception raised by the code being observed.

A logging infrastructure error is an exception raised while the logger itself is trying to prepare, deliver, configure, start, stop, or close logging components.

These errors belong to different boundaries:

```text
domain code
   |
   v
raises domain exception
   |
   v
log_invocation may record failed/cancelled event
   |
   v
same domain exception is re-raised
```

and:

```text
LogContext.emit_log_event(...)
   |
   v
sink.log(event) fails
   |
   v
LogErrorHandlingPolicy decides what happens
```

The logger is designed so that logging failures do not silently become domain failures unless the configured error handling policy explicitly asks for that.

## Why it exists

Logging is observational infrastructure.

Its primary job is to describe what happened, not to change what happened.

If the application code raises an exception, logging should be able to record that exception, but it should not replace it with a logging-specific error.

If a sink fails, the application may choose whether that failure should be ignored, printed to `stderr`, or raised. Different environments need different behavior:

```text
library default        -> avoid breaking user code
local debugging        -> print infrastructure failures
strict tests           -> raise logging failures
critical audit sink    -> fail when delivery fails
```

The error boundary exists to make that choice explicit.

## Error categories

The logger has several error categories.

```text
LoggerError
   |
   +-- LogContextError
   |      |
   |      +-- LogContextResetError
   |      +-- LogContextUnableToLog
   |
   +-- LogSinkConfigurationError
          |
          +-- LogSinkConfigurationConflictError
          +-- LogSinkDescriptorBuildError
          +-- LogSinkCreateError
          +-- LogSinkCloseError
          +-- LogSinkIsInUseError
```

`AsyncioLogSink` has its own runtime error family:

```text
AsyncioLogSinkError
   |
   +-- AsyncioLogSinkEventLoopUnavailableError
   +-- AsyncioLogSinkInvalidStateError
   +-- AsyncioLogSinkOnStartingHookFailedError
   +-- AsyncioLogSinkOnStoppedHookFailedError
   +-- AsyncioLogSinkQueueOverflowError
   +-- AsyncioLogSinkDispatcherCancelledError
   +-- AsyncioLogSinkUnexpectedError
```

These categories are intentionally not collapsed into one error type. Configuration errors, context reset errors, delivery errors, and asynchronous sink runtime errors happen at different boundaries.

## Domain exceptions in log_invocation

`log_invocation` observes function execution.

For a successful call, it can emit an `invoke` event and a `success` event.

For a failed call, it can emit a `failed` event.

For cancellation, it can emit a `cancelled` event.

But the decorator does not consume the original exception.

The execution shape is:

```text
try:
    result = function(...)
except asyncio.CancelledError:
    emit cancelled event
    raise
except Exception:
    emit failed event
    raise
else:
    emit success event
    return result
```

This means the original domain error remains the operation result. Logging records it, but does not translate it into a logger error.

## Infrastructure errors in LogContext

`LogContext.emit_log_event()` is the main delivery error boundary.

It calls the resolved sink:

```python
self.log_sink.log(event)
```

If the sink call succeeds, the event is delivered and the context clears its repeated-error marker.

If the sink call fails, the context reads `log_error_handling_policy` and applies it.

```text
sink.log(event) succeeds
    -> done

sink.log(event) raises
    -> apply LogErrorHandlingPolicy
```

This boundary applies to completed `LogEvent` delivery. It does not apply to arbitrary domain code outside logging.

## LogErrorHandlingPolicy

`LogErrorHandlingPolicy` defines how `LogContext` reacts to logging infrastructure failure.

```python
class LogErrorHandlingPolicy(StrEnum):
    IGNORE = "IGNORE"
    PRINT_STDERR = "PRINT_STDERR"
    RAISE = "RAISE"
```

### IGNORE

`IGNORE` suppresses the logging infrastructure error.

The failed event is not delivered, and the caller does not receive a logger exception from `emit_log_event()`.

This is useful when logging must never interfere with the observed code path.

### PRINT_STDERR

`PRINT_STDERR` reports the infrastructure error through the last-resort internal error helper.

The context prints only the first repeated delivery failure until a later event is successfully delivered.

The relevant state is local to the context:

```text
first failure        -> print to stderr
repeated failures    -> do not print again
successful delivery  -> reset repeated-error marker
next failure         -> print again
```

This avoids flooding `stderr` when a sink is persistently broken.

The root context uses `PRINT_STDERR` by default when no explicit policy is supplied.

### RAISE

`RAISE` converts the sink failure into `LogContextUnableToLog`.

The original sink exception becomes the cause.

This is useful in tests and in strict environments where logging delivery failure must be visible to the caller.

The raised error is still a logging infrastructure error. It is not a domain error produced by the observed operation.

## Inheritance of error handling policy

`log_error_handling_policy` is inherited through the context tree.

If a child context has no local policy, it resolves the policy from its parent.

```text
child local policy exists -> use it
child local policy absent -> ask parent
```

The root context must always have an effective policy. If no policy is passed to the root constructor, it defaults to `PRINT_STDERR`.

A non-root context may reset its local error handling policy and return to inherited behavior.

The root context cannot reset its error handling policy because there is no parent fallback.

## Last-resort stderr path

The logger has a small internal fallback helper:

```python
log_internal_error(message, exc)
```

It writes directly to `sys.stderr` using `print()`.

It does not use `LogContext`, `LogEvent`, a sink, or Python logger configuration.

This helper is used when the logging infrastructure cannot safely rely on itself to report a problem.

Examples:

```text
bootstrap failure
LogContext delivery failure under PRINT_STDERR
AsyncioLogSink dispatcher or cleanup internal reporting
```

This is deliberately primitive. It is the emergency lantern in the machinery room, not another logging pipeline.

## Configuration boundary

Package-level sink configuration has its own error boundary.

`configure_log_sink()` can fail before any event is emitted.

The main configuration errors are:

```text
LogSinkDescriptorBuildError
    -> sink class failed while building descriptor

LogSinkCreateError
    -> sink class failed while creating sink + terminator

LogSinkConfigurationConflictError
    -> same sink name already registered with a different descriptor
```

These errors are raised directly from configuration calls.

They are not controlled by `LogErrorHandlingPolicy`, because no event delivery is happening yet.

`LogErrorHandlingPolicy` applies to delivery through `LogContext.emit_log_event()`, not to package setup.

## Close and reset boundary

Closing or resetting sinks is also outside normal event delivery.

`close_log_sink(name)` may raise:

```text
LogSinkIsInUseError
    -> the sink is locally assigned to one or more registered contexts

LogSinkCloseError
    -> the sink terminator failed
```

`reset_logger()` may also raise `LogSinkCloseError` if one or more registered sink terminators fail during registry reset.

These errors are configuration/lifecycle errors. They are raised to the caller of the configuration operation.

They are not suppressed by a context-level error handling policy.

## Context reset boundary

Some context components are mandatory on the root context.

The root context cannot reset:

```text
log sink
payload processor
log error handling policy
```

Trying to reset one of these raises `LogContextResetError`.

This is a configuration invariant, not a delivery failure.

A root context must always have enough infrastructure to process and deliver an accepted event.

`event_policy` is different. Resetting event policy is allowed because `None` means that events are enabled for that context.

## AsyncioLogSink runtime boundary

`AsyncioLogSink` adds a second runtime boundary below `LogSinkProto`.

From the outside, it is still a sink:

```python
sink.log(event)
```

Inside, it has startup, queueing, dispatcher, flushing, stopping, and cleanup machinery.

Errors in that machinery are represented by `AsyncioLogSinkError` subclasses.

Examples:

```text
AsyncioLogSinkEventLoopUnavailableError
    -> direct construction without a running event loop

AsyncioLogSinkInvalidStateError
    -> operation is not valid for current sink state

AsyncioLogSinkQueueOverflowError
    -> pending event limit reached and policy is RAISE_ERROR

AsyncioLogSinkOnStartingHookFailedError
    -> subclass startup hook failed

AsyncioLogSinkOnStoppedHookFailedError
    -> subclass stop hook failed

AsyncioLogSinkDispatcherCancelledError
    -> dispatcher task was cancelled unexpectedly

AsyncioLogSinkUnexpectedError
    -> unexpected runtime failure wrapped by the sink
```

When these errors surface from `sink.log(event)`, `LogContext` handles them like other sink delivery errors.

When they surface from `start()` or `stop()` wait handles, they belong to the sink lifecycle operation result.

## Start and stop operation results

`AsyncioLogSink.start()` and `AsyncioLogSink.stop()` return `AsyncioLogSinkWaitHandle`.

The handle reports completion as `AsyncioLogSinkOpResult`:

```python
@dataclass(frozen=True, slots=True)
class AsyncioLogSinkOpResult:
    op_name: AsyncioLogSinkOp
    success: bool
    error: AsyncioLogSinkError | None = None
```

This keeps lifecycle errors attached to the lifecycle operation.

It also keeps them separate from event delivery errors.

```text
start failed -> lifecycle result error
log failed   -> handled by LogContext policy
stop failed  -> lifecycle result error
```

## Error payload boundary

`LogContext.build_error_payload()` converts an exception into a structured payload for failed or cancelled operation events.

The conversion rules are:

```text
1. If the exception has callable to_log_payload() and it returns dict, use that dict.
2. Otherwise, include code if present.
3. Include code_desc if present.
4. Always include kind and message unless already supplied.
```

This keeps domain errors compatible with structured logging without importing domain-specific error classes into the logger.

The method uses duck typing intentionally:

```text
exception with to_log_payload() -> custom structured payload
exception with code/code_desc   -> generic structured payload with code fields
ordinary exception              -> kind + message
```

If `to_log_payload()` itself fails or returns a non-dict value, the logger falls back to the generic representation.

## Error logged marker

`LogContext` provides two helper methods:

```python
def is_error_logged(self, err: BaseException) -> bool: ...
def mark_error_logged(self, err: BaseException) -> None: ...
```

The default implementation stores a best-effort marker on the exception instance:

```python
_mvx_error_logged = True
```

This is used by `log_invocation` to avoid logging the same exception instance with a full detailed error payload multiple times.

If the exception object does not allow arbitrary attributes, marking silently fails.

That failure is intentionally ignored. The marker is an optimization for duplicate suppression, not a correctness requirement.

## Suppressed error events in log_invocation

`log_invocation` can emit a failed event without including the full error payload.

This happens when an exception instance is already marked as logged or when `log_error_policy` asks to suppress detailed logging for a matching exception type.

The decorator still emits a failed event, but at `error_level_suppressed` and without the detailed `error` field.

The default suppressed level is `LogLevel.DEBUG`.

This gives two useful signals:

```text
full failure event       -> first detailed error payload
suppressed failure event -> same operation failed, but error details were already logged or intentionally suppressed
```

The original exception is still re-raised.

## Cancellation boundary

Cancellation is treated separately from ordinary failure.

`log_invocation` catches `asyncio.CancelledError`, emits a `cancelled` event when appropriate, marks the exception as logged, and re-raises the same cancellation.

This preserves asyncio cancellation semantics.

The logger records cancellation as an operation outcome, but does not convert it into an ordinary error and does not swallow it.

## What policies do not cover

The logger has multiple policies, and they do not cover the same boundary.

```text
LogEventPolicyProto
    -> decides whether an event is enabled before payload normalization

LogErrorHandlingPolicy
    -> decides what LogContext does when sink delivery fails

log_error_policy in log_invocation
    -> decides whether matching operation exceptions get detailed error payloads
```

These should not be mixed.

An event policy is not an exception handling policy.

A context error handling policy is not an event filter.

A `log_invocation` error policy does not control sink failures.

## Design summary

The logger keeps error boundaries explicit.

Domain exceptions remain domain exceptions. `log_invocation` records failed or cancelled outcomes and re-raises the original exception.

Sink delivery failures are handled by `LogContext` according to `LogErrorHandlingPolicy`.

Configuration and lifecycle failures are raised from the configuration or lifecycle operation that caused them.

`AsyncioLogSink` runtime failures stay in the async sink error family and surface either through sink delivery or start/stop operation results.

The last-resort `stderr` helper exists only for cases where the logging infrastructure cannot safely use itself to report its own failure.
