# LogContext

This page documents the object-level API for logger contexts.

A `LogContext` coordinates structured event logging for a namespace. It resolves logging infrastructure, applies event selection, normalizes payload data, creates `LogEvent` objects, and emits them through the effective sink.

Package-level functions such as `configure_log_context()` and `get_log_context()` are documented separately in the package-level API page.

## Error handling policy

`LogErrorHandlingPolicy` defines how a context reacts when logging infrastructure fails while delivering a prepared event.

```{eval-rst}
.. autoenum:: mvx.common.logger.LogErrorHandlingPolicy
```

## LogContext

`LogContext` is the main object-level API for structured logging.

A root context owns mandatory infrastructure: a sink and a payload processor. A child context can override selected components or inherit them from its parent.

```{eval-rst}
.. autoclass:: mvx.common.logger.LogContext
   :members:
   :member-order: bysource
   :class-doc-from: both
```

## Inheritance model

`LogContext` does not inherit all components in the same way.

```text
log sink
    inherited

payload processor
    inherited

logging error handling policy
    inherited

event policy
    local only
```

A child context can reset local sink, payload processor, or error handling policy to return to inherited behavior.

The root context cannot reset sink, payload processor, or error handling policy because it has no parent fallback.

Event policy is local. Resetting it is always valid and means that events emitted through that context are enabled by default.

## Manual event emission

The main manual logging method is `log_event()`.

It performs the full context pipeline:

```text
build LogEventMeta
   |
   v
check local event policy
   |
   v
normalize payload unless skipped
   |
   v
build LogEvent
   |
   v
emit through effective sink
```

Convenience methods call `log_event()` with predefined levels:

```text
log_debug_event()    -> LogLevel.DEBUG
log_info_event()     -> LogLevel.INFO
log_warning_event()  -> LogLevel.WARNING
log_error_event()    -> LogLevel.ERROR
```

## Prepared event emission

`emit_log_event()` is a lower-level method.

It expects a fully prepared `LogEvent` and sends it through the effective sink.

It does not apply event policy and does not normalize payload data.

Use it only when the caller has already built a log-ready event.

## Error payload helpers

`LogContext` also exposes helper methods used by logging components such as `log_invocation`:

```text
build_error_payload()
is_error_logged()
mark_error_logged()
```

These helpers make it possible to build structured error payloads and suppress repeated detailed error logging for the same exception instance.

## Payload normalization helpers

`LogContext` delegates payload normalization to the effective payload processor:

```text
normalize_payload()
normalize_value_for_log()
get_plain_verbosity_level()
```

These methods are useful for components that need to normalize selected values through the same rules as normal context logging.
