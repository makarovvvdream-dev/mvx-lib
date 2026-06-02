# Cancellation

This example shows how `log_invocation` records an operation cancelled with `asyncio.CancelledError`.

Cancellation is not logged as an ordinary failure. It has its own outcome:

```text
event_outcome = "cancelled"
```

The original cancellation is still re-raised.

## Example

```python
import asyncio

from mvx.common.logger import LogContextProto, log_invocation


class Worker:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context

    @log_invocation("run")
    async def run(self) -> None:
        raise asyncio.CancelledError()
```

The decorated public API operation is `run`.

When it raises `asyncio.CancelledError`, the decorator emits a `cancelled` outcome and then re-raises the same cancellation.

## Emitted records

A cancelled call:

```text
with pytest.raises(asyncio.CancelledError):
    await worker.run()
```

emits records conceptually equivalent to:

```text
[
    {
        "level": LogLevel.DEBUG,
        "event_name": "run",
        "event_outcome": "invoke",
        "payload": {},
    },
    {
        "level": LogLevel.INFO,
        "event_name": "run",
        "event_outcome": "cancelled",
        "payload": {
            "cancelled": True,
            "error": {
                ...
            },
        },
    },
]
```

The `cancelled` payload contains:

```text
cancelled = True
error     = build_error_payload(cancelled_error)
```

The exact `error` payload shape is produced by the resolved `LogContext`.

## Why cancellation is separate

`asyncio.CancelledError` is part of asyncio control flow.

It usually means the operation was stopped by task orchestration, shutdown, timeout handling, or another external cancellation path.

Treating cancellation as an ordinary failure would make logs less precise.

`log_invocation` therefore uses a separate outcome:

```text
failed     -> ordinary exception
cancelled  -> asyncio.CancelledError
```

## What this example demonstrates

This example demonstrates the cancellation path:

```text
invoke outcome emitted
operation raises CancelledError
cancelled outcome emitted
original CancelledError is re-raised
```

It also shows the default levels:

```text
invoke     -> LogLevel.DEBUG
cancelled  -> LogLevel.INFO
```

Use this pattern for async public API operations where cancellation is a meaningful lifecycle outcome.
