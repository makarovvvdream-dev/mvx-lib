# Logging infrastructure errors

```{contents} Contents:
:depth: 1
:local:
```

This article is about errors raised by the logging infrastructure itself.

It is not about application exceptions being logged as event payloads. Application exceptions are normal data for logging: they can be converted into payloads with `build_error_payload()` and emitted as regular events.

Logging infrastructure errors are different. They happen while the logger is trying to process or deliver an event.

Examples include:

* a sink failing to accept an event;
* a stream or file handler raising an error;
* a custom sink failing during handoff;
* a remote receiver becoming unavailable;
* a custom logging component raising unexpectedly.

The general principle is simple: logging should not break the application unless that behavior is explicitly configured.

## Direct logging path

When user code calls a context logging method, the direct path is:

```text
user code calls LogContext
        |
        v
LogContext builds metadata
        |
        v
event policy checks metadata
        |
        v
payload processor normalizes payload
        |
        v
LogContext creates LogEvent
        |
        v
sink accepts LogEvent
```

This article focuses on what is visible from direct `LogContext` usage.

It does not describe the internal lifecycle of asynchronous sinks, reconnect behavior, buffering strategy, or sink-specific failure states. Those details belong to sink-specific documentation.

## Where infrastructure errors may appear

The logging path is intentionally built from loosely coupled components.

That makes the logger extensible, but it also means that errors may come from components outside the core package:

* custom event policies;
* custom payload processors;
* custom sinks;
* standard `logging` handlers, formatters, and filters;
* network-based receivers such as Redis, PostgreSQL, syslog, or HTTP collectors.

The most important user-facing case is sink handoff failure: `LogContext` has already created a `LogEvent`, but the configured sink cannot accept it.

This may happen because the sink is stopped, failed, overloaded, misconfigured, or because its underlying output mechanism raises an error.

## Fail-safe default

The default behavior is intended to be fail-safe.

A logging failure should not normally interrupt application execution.

If the sink cannot accept an event, the logger reports the internal logging error to application `stderr` and suppresses repeated copies of the same failure condition.

This gives the application operator a visible signal that logging is not healthy, without turning every emitted event into another application failure.

When logging succeeds again, the internal error notification state can be cleared, so a later failure can be reported again.

## Log error handling policy

`LogContext` controls sink handoff failures through `log_error_handling_policy`.

The policy defines what happens when the context fails to pass a completed `LogEvent` to the configured sink.

Available policies are:

* `IGNORE`;
* `PRINT_STDERR`;
* `RAISE`.

## `IGNORE`

`IGNORE` suppresses logging infrastructure errors.

If the sink fails to accept an event, the error is ignored and the application continues.

This policy is useful when logging must never interfere with the application path and the application has another way to observe logging health.

The trade-off is visibility: failures inside logging may be missed.

## `PRINT_STDERR`

`PRINT_STDERR` reports logging infrastructure errors to application `stderr`.

It preserves the application flow, but still makes the problem visible.

This is the fail-safe default behavior.

Repeated errors are not printed for every failed event. The context reports the failure condition once, avoiding noisy stderr flooding while logging remains broken.

This policy is suitable for normal application runtime.

## `RAISE`

`RAISE` turns logging infrastructure errors into application-visible exceptions.

When sink handoff fails, the context raises `LogContextUnableToLog`.

This policy is useful in tests, development, and strict diagnostic scenarios where logging failures must be treated as failures of the calling path.

It should be used intentionally, because it allows logging infrastructure errors to interrupt application execution.

## Configuring the policy

The policy can be passed when configuring a context.

```python
from mvx.common.logger import (
    LogErrorHandlingPolicy,
    configure_log_context,
)

ctx = configure_log_context(
    "my_app.worker",
    log_error_handling_policy=LogErrorHandlingPolicy.PRINT_STDERR,
)
```

It can also be changed later.

```python
ctx.set_log_error_handling_policy(LogErrorHandlingPolicy.RAISE)
```

A non-root context can reset its local policy.

```python
ctx.reset_log_error_handling_policy()
```

After reset, the context uses the policy inherited from its parent.

The root context must always have a logging error handling policy, so resetting it on the root context is not allowed.

## Inheritance

`log_error_handling_policy` is an inherited context setting.

A high-level context can define a common policy for its descendants.

A child context can override it if that layer needs stricter or softer behavior.

```text
my_app                 -> PRINT_STDERR
my_app.worker          -> inherits PRINT_STDERR
my_app.worker.tests    -> RAISE
```

This makes it possible to keep normal runtime fail-safe while making selected contexts strict in tests or diagnostics.

## What this policy does not cover

`log_error_handling_policy` is not a handler for application exceptions.

Application exceptions should be logged as event payloads, usually through `build_error_payload()`.

The policy also does not describe the full internal lifecycle of asynchronous sinks.

For example, an asynchronous sink may buffer events, report backend failure internally, retry delivery, switch to fallback receivers, or reject events when its queue limit is reached. Those behaviors are part of the sink implementation and are described in sink-specific documentation.

This article only describes the policy visible at the `LogContext` boundary: what happens when the context cannot hand a completed `LogEvent` to the configured sink.

## What to remember

* Logging infrastructure errors are errors raised while the logger processes or delivers events.
* They are different from application errors being logged as payloads.
* The default behavior is fail-safe: logging should not break application execution unless configured to do so.
* `log_error_handling_policy` controls what happens when `LogContext` cannot pass a completed `LogEvent` to the sink.
* Use `PRINT_STDERR` for visible fail-safe behavior, `IGNORE` for silent suppression, and `RAISE` when logging failures must fail the caller.
