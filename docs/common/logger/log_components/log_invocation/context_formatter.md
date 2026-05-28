# Context formatter

```{contents} Contents:
:depth: 1
:local:
```

## Overview

`context_formatter` gives `log_invocation` full control over the payload built from `context_fields`.

Most cases do not need it.

For simple payloads, use `context_fields` directly:

```python
@log_invocation(
    "open",
    context_fields=("state=self.state",),
)
async def open(self) -> None:
    ...
```

This is enough when selected fields can be copied into the payload one by one.

Use `context_formatter` when selected fields need to be combined, renamed conditionally, grouped, transformed, or shaped differently depending on the operation outcome.

In other words:

```text
context_fields      -> select raw values
context_formatter   -> decide final payload shape
```

## Why it exists

Field specs are intentionally simple.

They can select values, follow attributes, use aliases, call `len()`, use verbosity gates, and pass `unbounded=True` to normalization.

But some payloads need more structure than a flat list of selected fields.

Examples:

```text
group several fields under one nested object
combine two fields into one derived value
produce different payload shape for invoke and success
hide or transform fields depending on operation state
rename fields based on outcome
add calculated diagnostic fields
move diagnostic data under a dedicated key
```

`context_formatter` exists for those cases.

It lets the decorator collect raw context values and then gives your code a chance to shape the payload explicitly.

## Function signature

A context formatter is a callable with this shape:

```python
from typing import Any

from mvx.common.logger import LogContextProto


def formatter(
    ctx: LogContextProto,
    event_outcome: object,
    event: str,
    fields: dict[str, Any],
) -> dict[str, Any]:
    ...
```

The actual decorator type is:

```python
PayloadFormatter = Callable[
    [LogContextProto, _InvocationEventType, str, dict[str, Any]],
    dict[str, Any],
]
```

For public documentation, the important parts are:

```text
ctx
    resolved logging context

event_outcome
    current outcome: invoke, success, failed, or cancelled

event
    decorated event name

fields
    raw values resolved from context_fields
```

The formatter should return a dictionary.

That dictionary is injected into the payload being built for the current outcome.

## Raw fields, not normalized fields

The `fields` argument contains raw resolved values.

The decorator resolves `context_fields` first:

```text
context_fields -> raw field values -> context_formatter
```

If the formatter returns a dictionary, that dictionary is injected into the target payload.

The fallback path, where no formatter is used or the formatter does not return a dictionary, normalizes individual fields automatically.

This distinction matters.

With a formatter, your code owns the shape of the produced payload. If you need normalized values inside that custom shape, call the context normalization API explicitly.

Example:

```python
from typing import Any

from mvx.common.logger import LogContextProto


def format_context(
    ctx: LogContextProto,
    event_outcome: object,
    event: str,
    fields: dict[str, Any],
) -> dict[str, Any]:
    return {
        "state": ctx.normalize_value_for_log(fields.get("state")),
    }
```

## Minimal example

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

Instead of producing flat payload fields:

```python
{
    "state": "closed",
    "peer": "ldap.example.local",
}
```

this formatter can produce a grouped payload:

```python
{
    "connection": {
        "state": "closed",
        "peer": "ldap.example.local",
    }
}
```

## Outcome-specific payloads

The formatter receives the current `event_outcome`, so it can produce different payloads for different outcomes.

```python
from typing import Any

from mvx.common.logger import LogContextProto


def format_connection_context(
    ctx: LogContextProto,
    event_outcome: object,
    event: str,
    fields: dict[str, Any],
) -> dict[str, Any]:
    state = ctx.normalize_value_for_log(fields.get("state"))

    if str(event_outcome) == "invoke":
        return {
            "before": {
                "state": state,
            }
        }

    if str(event_outcome) == "success":
        return {
            "after": {
                "state": state,
            }
        }

    return {
        "state": state,
    }
```

This is useful when the same selected value has different meaning at different lifecycle moments.

For example, `state` on `invoke` may mean state before the operation, while `state` on `success` may mean state after the operation.

## Combining fields

A formatter can combine several raw fields into one derived value.

```python
from typing import Any

from mvx.common.logger import LogContextProto


def format_range(
    ctx: LogContextProto,
    event_outcome: object,
    event: str,
    fields: dict[str, Any],
) -> dict[str, Any]:
    start = fields.get("start")
    end = fields.get("end")

    return {
        "range": ctx.normalize_value_for_log(f"{start}:{end}"),
    }
```

Used with:

```python
@log_invocation(
    "read_range",
    context_fields=("start", "end"),
    context_formatter=format_range,
)
def read_range(self, start: int, end: int) -> bytes:
    ...
```

The payload contains one derived `range` value instead of two separate fields.

## Grouping fields

A formatter can place selected fields under a domain-specific group.

```python
from typing import Any

from mvx.common.logger import LogContextProto


def format_request_context(
    ctx: LogContextProto,
    event_outcome: object,
    event: str,
    fields: dict[str, Any],
) -> dict[str, Any]:
    return {
        "request": {
            "id": ctx.normalize_value_for_log(fields.get("request_id")),
            "method": ctx.normalize_value_for_log(fields.get("method")),
        }
    }
```

Used with:

```python
@log_invocation(
    "send_request",
    context_fields=("request_id", "method"),
    context_formatter=format_request_context,
)
def send_request(self, request_id: str, method: str, payload: bytes) -> None:
    ...
```

This gives the payload a stable nested shape:

```python
{
    "request": {
        "id": "...",
        "method": "...",
    }
}
```

## Interaction with system keys

The decorator protects system payload keys.

System keys are:

```text
error
kwargs
result
cancelled
closures
```

If the formatter returns a dictionary containing any of these keys, the produced dictionary is not merged directly into the top-level payload.

Instead, it is placed under the `context` key.

For example, if the formatter returns:

```python
{
    "result": "custom value"
}
```

then the payload receives it as context data instead of overwriting the real result payload:

```python
{
    "context": {
        "result": "custom value"
    }
}
```

This prevents formatter output from accidentally replacing reserved lifecycle payload sections.

## Formatter failure

If the formatter raises an exception, the decorator ignores that formatter result and falls back to normal field handling.

If the formatter returns something other than a dictionary, the decorator also falls back to normal field handling.

Fallback means:

```text
resolved context fields are normalized one by one
then injected into the target payload
```

This keeps diagnostic formatting errors from breaking the decorated public API operation.

The formatter is a payload-shaping helper, not part of the operation semantics.

## Empty field list

The formatter is called even when no context fields were resolved.

This allows a formatter to produce payload from the event name, outcome, context, or other external logic.

Example:

```python
from typing import Any

from mvx.common.logger import LogContextProto


def format_operation_marker(
    ctx: LogContextProto,
    event_outcome: object,
    event: str,
    fields: dict[str, Any],
) -> dict[str, Any]:
    return {
        "operation": {
            "event": event,
            "outcome": str(event_outcome),
        }
    }
```

Used with:

```python
@log_invocation(
    "heartbeat",
    context_formatter=format_operation_marker,
)
def heartbeat(self) -> None:
    ...
```

Even without `context_fields`, the formatter can build a payload.

## Context fields timing still applies

When `context_fields` are used with `context_formatter`, the fields are still resolved separately for each emitted outcome.

The formatter receives the raw field values for that specific outcome.

This means a formatter can shape dynamic state differently across the operation lifecycle.

Example:

```text
invoke formatter call
    fields["state"] == "closed"

success formatter call
    fields["state"] == "opened"
```

The formatter does not freeze values at invocation time. It shapes whatever values were resolved for the current outcome.

## When to use context_formatter

Use `context_formatter` when a flat field list is not enough.

Good use cases:

```text
nested payloads
outcome-specific payload shape
derived fields
renaming based on several values
grouping fields into domain objects
adding operation markers
normalizing values manually inside a custom structure
```

Do not use it for simple one-to-one field extraction.

For simple cases, plain `context_fields` are clearer:

```python
@log_invocation(
    "open",
    context_fields=("state=self.state",),
)
async def open(self) -> None:
    ...
```

Use the formatter when you need the payload to be deliberately shaped, not merely populated.

## What context_formatter does not control

* `context_formatter` does not decide whether the event is enabled.
* It does not change the operation result.
* It does not swallow operation exceptions.
* It does not replace `log_kwargs_on_invoke`.
* It does not replace `log_result_on_success`.
* It does not replace error payload generation.
* It only controls how resolved context data is injected into the payload for the current outcome.

## Design summary

`context_formatter` is the advanced payload-shaping hook for `log_invocation`.

`context_fields` select raw values.

`context_formatter` decides how those values become payload.

It can group, combine, rename, calculate, and produce outcome-specific structures.

It is useful when the decorated public API operation needs a stable, meaningful payload shape that cannot be expressed as a simple flat list of fields.
