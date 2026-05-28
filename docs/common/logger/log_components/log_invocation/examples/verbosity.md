# Verbosity

This example shows how verbosity gates control which selected fields are included in the `invoke` payload.

Verbosity gates affect field selection. They do not enable or disable the event itself.

## Example

```python
from mvx.common.logger import LogContextProto, log_invocation


class Client:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto:
        return self._log_context

    @log_invocation(
        "send_request",
        log_kwargs_on_invoke=(
            "request_id",
            "NORMAL,MAXIMUM:method",
            "MAXIMUM:payload_size=payload.len()",
        ),
    )
    def send_request(self, request_id: str, method: str, payload: bytes) -> None:
        ...
```

The decorated public API operation is `send_request`.

The selected invoke fields are:

```text
request_id
NORMAL,MAXIMUM:method
MAXIMUM:payload_size=payload.len()
```

This means:

```text
request_id
    selected at any verbosity

method
    selected at NORMAL and MAXIMUM

payload_size
    selected only at MAXIMUM
```

## Minimal verbosity

With `LogVerbosityLevel.MINIMAL`, only the ungated field is selected.

A successful call:

```text
client.send_request(
    request_id="req-1",
    method="GET",
    payload=b"abc",
)
```

emits an `invoke` payload conceptually equivalent to:

```text
{
    "kwargs": {
        "request_id": "req-1",
    }
}
```

The `method` field is skipped because it is gated for `NORMAL` and `MAXIMUM`.

The `payload_size` field is skipped because it is gated for `MAXIMUM` only.

## Normal verbosity

With `LogVerbosityLevel.NORMAL`, the ungated field and the `NORMAL,MAXIMUM` field are selected.

The `invoke` payload is conceptually equivalent to:

```text
{
    "kwargs": {
        "request_id": "req-1",
        "method": "GET",
    }
}
```

The `payload_size` field is still skipped because it is gated for `MAXIMUM` only.

## Maximum verbosity

With `LogVerbosityLevel.MAXIMUM`, all three fields are selected.

The `invoke` payload is conceptually equivalent to:

```text
{
    "kwargs": {
        "request_id": "req-1",
        "method": "GET",
        "payload_size": 3,
    }
}
```

The `payload_size` field uses an alias and `len()`:

```text
payload key  -> payload_size
value        -> len(payload)
```

## Success payload

This example uses only `log_kwargs_on_invoke`.

That means the selected fields are included only in the `invoke` payload.

The `success` payload is empty at every verbosity level:

```text
{}
```

To include data in `success`, use `context_fields` or `log_result_on_success`.

## Why use verbosity gates

Verbosity gates let a public API method expose more diagnostic data only when the current context verbosity allows it.

A common pattern is:

```text
always
    stable identifiers

NORMAL
    compact diagnostic fields

MAXIMUM
    larger or more detailed diagnostic fields
```

In this example:

```text
always
    request_id

NORMAL
    method

MAXIMUM
    payload_size
```

Do not use verbosity gates as a safety mechanism for secrets. Sensitive values should not be selected unless they are safe to log at that boundary.

## What this example demonstrates

This example demonstrates that verbosity gates control field selection:

```text
MINIMAL -> request_id
NORMAL  -> request_id, method
MAXIMUM -> request_id, method, payload_size
```

The event itself is still emitted normally. Only the selected payload fields change.
