# Public API method

This example shows the most common `log_invocation` usage pattern: decorating a public API method.

The decorated method represents one public operation. The decorator records that operation as one event with different outcomes.

## Example

```python
from mvx.common.logger import LogContextProto, LogEvent, LogLevel, log_invocation

class Connection:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context

    @log_invocation("open")
    async def open(self) -> None: ...
```

The `open()` method is the public API operation.

The decorator uses `"open"` as the event name:

```text
event_name = "open"
```

When the method is called successfully, the decorator emits two log records for that event:

```text
event_outcome = "invoke"
event_outcome = "success"
```

## Context resolution

The example uses method-based context resolution.

The first positional argument is `self`. The decorator uses it to obtain the logging context:

```text
self.get_log_context() -> LogContext-compatible object
```

That context supplies the event namespace and the logging pipeline.

In the tested example, the context namespace is:

```text
example
```

So both emitted records use:

```text
event_namespace = "example"
```

## Emitted records

A successful call:

```text
await connection.open()
```

emits records conceptually equivalent to:

```text
[
    {
        "level": LogLevel.DEBUG,
        "event_namespace": "example",
        "event_name": "open",
        "entity_id": None,
        "event_outcome": "invoke",
        "payload": {},
    },
    {
        "level": LogLevel.DEBUG,
        "event_namespace": "example",
        "event_name": "open",
        "entity_id": None,
        "event_outcome": "success",
        "payload": {},
    },
]
```

The payload is empty because the example does not request any context fields, invoke arguments, closure values, or result logging.

## What this example demonstrates

This example demonstrates the base lifecycle:

```text
call public API method
   |
   v
resolve context from self
   |
   v
emit invoke outcome
   |
   v
run method body
   |
   v
emit success outcome
```

It also shows the recommended starting point: put `log_invocation` on public API methods that represent meaningful operations for the caller.
