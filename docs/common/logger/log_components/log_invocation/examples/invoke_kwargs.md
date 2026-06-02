# Invoke kwargs

This example shows how `log_kwargs_on_invoke` adds selected function arguments to the `invoke` payload.

Unlike `context_fields`, invoke kwargs are emitted only when the operation starts. They are not repeated on `success`, `failed`, or `cancelled` outcomes.

## Example

```python
from mvx.common.logger import LogContextProto, log_invocation


class Client:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context

    @log_invocation(
        "send_request",
        log_kwargs_on_invoke=(
            "request_id",
            "method",
            "payload_size=payload.len()",
        ),
    )
    def send_request(self, request_id: str, method: str, payload: bytes) -> None: ...
```

The decorated public API operation is `send_request`.

The selected invoke kwargs are:

```text
request_id
method
payload_size=payload.len()
```

The last field uses an alias and `len()`:

```text
payload key  -> payload_size
value        -> len(payload)
```

## Emitted records

A successful call:

```text
client.send_request(
    request_id="req-1",
    method="GET",
    payload=b"abc",
)
```

emits records conceptually equivalent to:

```text
[
    {
        "event_name": "send_request",
        "event_outcome": "invoke",
        "payload": {
            "kwargs": {
                "request_id": "req-1",
                "method": "GET",
                "payload_size": 3,
            },
        },
    },
    {
        "event_name": "send_request",
        "event_outcome": "success",
        "payload": {},
    },
]
```

The `kwargs` section appears only in the `invoke` payload.

The `success` payload is empty because this example does not use `context_fields` or `log_result_on_success`.

## When to use invoke kwargs

Use `log_kwargs_on_invoke` for input values that are useful at operation start.

Good candidates are usually small, safe values:

```text
request_id
method
operation mode
payload size
resource name
```

Avoid selecting secrets or large raw values:

```text
passwords
tokens
raw payloads
large buffers
private credentials
```

If a value should appear on every outcome, use `context_fields` instead.

If a value only describes the initial call, use `log_kwargs_on_invoke`.

## What this example demonstrates

This example demonstrates that `log_kwargs_on_invoke` is scoped to the `invoke` outcome:

```text
invoke  -> payload["kwargs"]
success -> no kwargs
```

It also shows the recommended form for length fields:

```text
payload_size=payload.len()
```

For `len()` specs, use an explicit alias so the payload key is clear.
