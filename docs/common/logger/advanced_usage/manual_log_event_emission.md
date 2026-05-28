# Manual LogEvent emission

```{contents} Contents:
:depth: 1
:local:
```

## Overview

Most code should emit events through `LogContext.log_event()` or its convenience methods:

```python
ctx.log_info_event(
    event="worker.started",
    payload={"worker_id": "w1"},
)
```

That path builds metadata, checks event policy, normalizes the payload, creates `LogEvent`, and delivers it through the effective sink.

Manual `LogEvent` emission is the lower-level alternative.

It uses:

```python
ctx.emit_log_event(log_event)
```

This method expects a fully prepared `LogEvent`.

It does not apply event policy and does not normalize payload data.

Use it only when the caller has already made those decisions.

## Normal path vs manual path

The normal context path is:

```text
ctx.log_event(...)
   |
   v
build LogEventMeta
   |
   v
check event policy
   |
   v
normalize payload
   |
   v
build LogEvent
   |
   v
emit through sink
```

The manual path is:

```text
caller builds LogEvent
   |
   v
ctx.emit_log_event(log_event)
   |
   v
emit through sink
```

Manual emission skips the earlier steps because the caller has already prepared the event.

## When to use manual emission

Use manual `LogEvent` emission when an external or higher-level component already owns event construction.

Good cases:

```text
bridging events from another logging pipeline
replaying stored LogEvent objects
forwarding events between contexts
writing tests for sink behavior
building a specialized log component above LogContext
```

In these cases, `LogContext` is used as a delivery boundary, not as the event builder.

## When not to use manual emission

Do not use `emit_log_event()` just to avoid typing a few arguments to `log_event()`.

Avoid it when the caller still needs:

```text
event policy checks
payload normalization
standard metadata construction
context namespace defaulting
level convenience methods
```

For ordinary manual logging, use:

```text
log_debug_event()
log_info_event()
log_warning_event()
log_error_event()
log_event()
```

Those methods keep the normal pipeline intact.

## Building a LogEvent manually

Manual emission requires creating both `LogEventMeta` and `LogEvent`.

```python
import time

from mvx.common.logger import LogEvent, LogEventMeta, LogLevel


meta = LogEventMeta(
    event_namespace="my.component",
    event_name="external.event",
    entity_id="entity-1",
    source_path=None,
    source_line=None,
    source_func=None,
)

log_event = LogEvent(
    level=LogLevel.INFO,
    meta=meta,
    event_outcome=None,
    timestamp=time.time(),
    payload={
        "status": "ready",
    },
)

ctx.emit_log_event(log_event)
```

The payload is used as-is.

No payload processor is applied by `emit_log_event()`.

## Payload responsibility

When using `emit_log_event()`, the caller is responsible for providing a log-ready payload.

That means:

```text
keys are suitable for structured logging
values are already normalized or intentionally raw
payload size is acceptable
sensitive data has already been handled
```

If you still want the context payload processor to normalize the payload, do it explicitly before building the event:

```python
payload = ctx.normalize_payload(
    {
        "raw_object": obj,
    }
)
```

Then use the normalized payload in `LogEvent`.

## Event policy responsibility

`emit_log_event()` does not call `is_event_enabled()`.

If manual emission should still respect event policy, the caller must check it explicitly.

```python
meta = LogEventMeta(
    event_namespace="my.component",
    event_name="external.event",
    entity_id=None,
    source_path=None,
    source_line=None,
    source_func=None,
)

if ctx.is_event_enabled(meta):
    ctx.emit_log_event(
        LogEvent(
            level=LogLevel.INFO,
            meta=meta,
            event_outcome=None,
            timestamp=time.time(),
            payload=payload,
        )
    )
```

This explicit check is useful when a custom component wants full control over event construction while still using context policy.

## Forwarding events

Manual emission can forward an existing `LogEvent` through another context.

```python
other_ctx.emit_log_event(log_event)
```

This does not rebuild metadata.

It does not re-run policy.

It does not re-normalize payload.

The event is delivered as it already exists.

This can be useful for bridging or replaying events, but it should be used deliberately.

## Testing sinks

Manual emission is useful in sink tests.

A test can build a small `LogEvent` and send it through a context or directly to a sink.

```python
event = LogEvent(
    level=LogLevel.INFO,
    meta=LogEventMeta(
        event_namespace="test",
        event_name="sink.test",
        entity_id=None,
        source_path=None,
        source_line=None,
        source_func=None,
    ),
    event_outcome=None,
    timestamp=0.0,
    payload={"value": 1},
)

ctx.emit_log_event(event)
```

This avoids coupling the test to payload normalization or event-policy behavior when the test is about delivery.

## Error handling boundary

`emit_log_event()` still uses the context's effective sink and logging error handling policy.

If the sink raises during delivery, `LogContext` applies `LogErrorHandlingPolicy`.

```text
IGNORE
    suppress delivery failure

PRINT_STDERR
    report through the last-resort stderr path

RAISE
    raise LogContextUnableToLog
```

So manual emission bypasses event building, policy, and normalization, but it does not bypass the context delivery error boundary.

## Relationship to log_invocation

`log_invocation` internally builds prepared `LogEvent` records for operation outcomes and emits them through the resolved context.

That is similar to manual emission in one sense: the component constructs the event itself.

The difference is that `log_invocation` owns a specific higher-level behavior:

```text
operation lifecycle -> invoke/success/failed/cancelled outcomes
```

Manual emission is lower-level and generic.

Use `log_invocation` for public API operation lifecycle logging.

Use manual `emit_log_event()` only when you are building another component or bridge that already owns event construction.

## Common mistakes

Avoid these mistakes:

```text
calling emit_log_event() with raw application objects in payload
expecting event policy to run automatically
expecting payload normalization to run automatically
forwarding events without considering duplicate delivery
using manual emission for ordinary application logging
```

The method is powerful because it skips normal event construction steps.

That is also what makes it easy to misuse.

## Design summary

`emit_log_event()` is the low-level delivery boundary of `LogContext`.

It accepts a prepared `LogEvent` and sends it through the effective sink.

It does not apply event policy.

It does not normalize payload data.

It still uses the context's sink delivery and logging infrastructure error handling behavior.

Use it for bridges, replays, tests, and custom logging components that already own event construction.
