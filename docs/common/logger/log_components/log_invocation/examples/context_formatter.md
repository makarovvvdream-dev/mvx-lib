# Context formatter

This example shows how `context_formatter` can control the final payload shape built from `context_fields`.

Use this when a flat list of context fields is not enough and the payload needs a deliberate structure.

## Example

```python
from typing import Any

from mvx.common.logger import LogContextProto, log_invocation


def connection_payload(
    ctx: LogContextProto,
    event_outcome: object,
    event: str,
    fields: dict[str, Any],
) -> dict[str, Any]:
    return {
        "connection": {
            "event": event,
            "outcome": str(event_outcome),
            "state": ctx.normalize_value_for_log(fields.get("state")),
            "peer": ctx.normalize_value_for_log(fields.get("peer")),
        }
    }


class Connection:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context
        self.state = "closed"
        self.peer = "ldap.example.local"

    def get_log_context(self) -> LogContextProto:
        return self._log_context

    @log_invocation(
        "open",
        context_fields=("state=self.state", "peer=self.peer"),
        context_formatter=connection_payload,
    )
    async def open(self) -> None:
        self.state = "opened"
```

The decorated public API operation is `open`.

The selected context fields are:

```text
state=self.state
peer=self.peer
```

The formatter receives those resolved fields and returns the final payload shape.

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
            "connection": {
                "event": "open",
                "outcome": "invoke",
                "state": "closed",
                "peer": "ldap.example.local",
            },
        },
    },
    {
        "event_name": "open",
        "event_outcome": "success",
        "payload": {
            "connection": {
                "event": "open",
                "outcome": "success",
                "state": "opened",
                "peer": "ldap.example.local",
            },
        },
    },
]
```

The payload is nested under the `connection` key because the formatter returned that structure.

## Why use a formatter

Without `context_formatter`, the selected fields would be injected as top-level payload fields:

```text
{
    "state": "closed",
    "peer": "ldap.example.local",
}
```

With `context_formatter`, the example can produce a more intentional shape:

```text
{
    "connection": {
        "event": "open",
        "outcome": "invoke",
        "state": "closed",
        "peer": "ldap.example.local",
    }
}
```

This is useful when the payload should be grouped, transformed, or enriched before it is emitted.

## Raw fields and normalization

The formatter receives raw values resolved from `context_fields`.

In this example, the formatter explicitly normalizes values before returning them:

```text
ctx.normalize_value_for_log(fields.get("state"))
ctx.normalize_value_for_log(fields.get("peer"))
```

This makes the formatter responsible for the final custom shape while still reusing the context normalization rules.

## Outcome-specific data

The formatter receives both:

```text
event
event_outcome
```

That allows the payload to include operation identity and lifecycle state.

In this example:

```text
invoke  -> connection.outcome = "invoke"
success -> connection.outcome = "success"
```

The same formatter also sees dynamic context values separately for each outcome:

```text
invoke  -> connection.state = "closed"
success -> connection.state = "opened"
```

## What this example demonstrates

This example demonstrates that `context_formatter` can take selected context fields and build a custom payload structure.

It shows four important points:

```text
context_fields select the raw values
context_formatter receives event and event_outcome
context_formatter returns the final payload shape
dynamic fields are still resolved separately for each emitted outcome
```

Use this pattern when a public API operation needs a stable, meaningful payload shape that cannot be expressed as a flat list of fields.
