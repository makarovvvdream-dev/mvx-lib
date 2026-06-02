# Awaitable result

This example shows how `log_invocation` handles a synchronous function that returns an awaitable.

This example describes the logging-enabled path, where method-based context resolution returns a logging context.

The function itself is not `async def`, but its result must still be awaited. In this case, the decorator emits `invoke` when the function is called and emits the final outcome only after the returned awaitable completes.

## Example

```python
from collections.abc import Awaitable

from mvx.common.logger import LogContextProto, log_invocation


class Client:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context

    @log_invocation(
        "send",
        log_result_on_success=(),
    )
    def send(self) -> Awaitable[str]:
        async def run() -> str:
            return "ok"

        return run()
```

The decorated public API operation is `send`.

`send()` is a synchronous method, but it returns an awaitable object.

The decorator does not treat the returned awaitable itself as the final result. Instead, it wraps the awaitable and logs the final outcome after it is awaited.

## Emitted records

Calling the method:

```text
result_awaitable = client.send()
```

emits the `invoke` outcome immediately:

```text
[
    {
        "event_name": "send",
        "event_outcome": "invoke",
        "payload": {},
    },
]
```

At this point, `success` has not been emitted yet.

The final outcome is emitted only after the awaitable completes:

```text
result = await result_awaitable
```

The awaited result is:

```python
"ok"
```

After awaiting, the emitted records are conceptually equivalent to:

```text
[
    {
        "event_name": "send",
        "event_outcome": "invoke",
        "payload": {},
    },
    {
        "event_name": "send",
        "event_outcome": "success",
        "payload": {
            "result": "ok",
        },
    },
]
```

The `result` payload is produced from the awaited result, not from the awaitable object returned by `send()`.

## Why this matters

Some APIs are synchronous at the call boundary but start asynchronous work and return an awaitable.

For such APIs, logging `success` immediately after the function returns would be wrong. At that point, the asynchronous work has not completed yet.

`log_invocation` preserves the actual operation lifecycle:

```text
call sync function
   |
   v
emit invoke
   |
   v
function returns awaitable
   |
   v
await returned awaitable
   |
   v
emit success / failed / cancelled after awaited completion
```

## What this example demonstrates

This example demonstrates the awaitable-result path:

```text
send() call             -> invoke emitted
returned awaitable      -> no success yet
await returned value    -> success emitted
```

It also shows that `log_result_on_success=()` logs the whole awaited result under the `result` key.
