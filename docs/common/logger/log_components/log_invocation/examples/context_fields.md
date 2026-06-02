# Context fields

This example shows how `context_fields` add shared payload data to every emitted outcome of a decorated event.

It also shows an important timing detail: context fields are resolved separately for each outcome.

## Example

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

The decorated public API operation is `open`.

The selected context field is:

```text
state=self.state
```

This means:

```text
payload key  -> state
value path   -> self.state
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
        "event_name": "open",
        "event_outcome": "invoke",
        "payload": {
            "state": "closed",
        },
    },
    {
        "event_name": "open",
        "event_outcome": "success",
        "payload": {
            "state": "opened",
        },
    },
]
```

The `invoke` outcome is emitted before the method body runs, so `self.state` is still `"closed"`.

The `success` outcome is emitted after the method body completes, so `self.state` has become `"opened"`.

## Why this matters

The decorator captures the bound function arguments once when the wrapper is called.

However, `context_fields` are resolved again before each emitted outcome. Attribute paths are evaluated at that moment.

This allows context fields to describe the current operation state.

Stable identifiers are common context fields:

```text
request_id
message_id
connection_id
```

Dynamic state can also be useful:

```text
state=self.state
phase=self.phase
retry_count=self.retry_count
```

Use dynamic context fields when the value at each lifecycle point matters.

## What this example demonstrates

This example demonstrates that `context_fields` are not just copied once at invocation time.

They are evaluated for each emitted outcome:

```text
invoke  -> state before the operation body
success -> state after the operation body
```

The same rule applies to `failed` and `cancelled` outcomes: context fields are resolved when that outcome is emitted.
