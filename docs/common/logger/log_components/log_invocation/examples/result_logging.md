# Result logging

This example shows how `log_result_on_success` adds selected return-value data to the `success` payload.

Result logging is opt-in. By default, `log_invocation` does not log operation results.

## Example

```python
from dataclasses import dataclass

from mvx.common.logger import LogContextProto, LogEvent, LogLevel, log_invocation


@dataclass(frozen=True, slots=True)
class Session:
    session_id: str
    ttl_ms: int


class SessionService:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto:
        return self._log_context

    @log_invocation(
        "create_session",
        log_result_on_success=("session_id", "ttl_ms"),
    )
    def create_session(self, user_id: str) -> Session:
        return Session(session_id=f"session-{user_id}", ttl_ms=30_000)
```

The decorated public API operation is `create_session`.

The selected result fields are:

```text
session_id
ttl_ms
```

The method still returns the original `Session` object. Result logging does not replace or modify the return value.

## Emitted records

A successful call:

```text
result = service.create_session("u1")
```

returns:

```text
Session(session_id="session-u1", ttl_ms=30_000)
```

and emits records conceptually equivalent to:

```text
[
    {
        "event_name": "create_session",
        "event_outcome": "invoke",
        "payload": {},
    },
    {
        "event_name": "create_session",
        "event_outcome": "success",
        "payload": {
            "result": {
                "session_id": "session-u1",
                "ttl_ms": 30000,
            },
        },
    },
]
```

The `invoke` payload is empty because this example does not use `context_fields`, `log_kwargs_on_invoke`, or closure values.

The `success` payload contains the selected result data under the `result` key.

## Why result logging is explicit

Operation results may contain sensitive or large data.

For example:

```text
access tokens
raw backend responses
large buffers
objects with internal state
```

`log_result_on_success` makes result logging explicit. The decorated method chooses which parts of the result are safe and useful to expose.

## What this example demonstrates

This example demonstrates selected result logging for a composite result object:

```text
operation result -> selected attributes -> payload["result"]
```

It also shows that result logging happens only for the `success` outcome:

```text
invoke  -> no result
success -> selected result fields
```

Use this pattern when the result object contains a few stable diagnostic fields that are useful in logs.
