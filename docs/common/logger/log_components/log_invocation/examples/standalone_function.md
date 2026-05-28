# Standalone function

This example shows how to use `log_invocation` with a standalone function instead of an instance method.

A standalone function has no `self`, so the decorator cannot resolve the logging context from the first positional argument. In this case, pass the context explicitly with `ctx=...`.

This example also shows how `log_closures_on_invoke` can include values captured from the surrounding scope in the `invoke` payload.

## Example

```python
from mvx.common.logger import LogContextProto, log_invocation


def make_connector(log_context: LogContextProto, target: str):
    @log_invocation(
        "connect",
        ctx=log_context,
        log_closures_on_invoke={"target": target},
    )
    async def connect() -> None:
        ...

    return connect
```

The decorated public operation is `connect`.

The function is created by `make_connector()` and closes over two values:

```text
log_context
target
```

The logging context is passed to the decorator explicitly:

```text
ctx=log_context
```

The `target` value is not an argument of `connect()`. It belongs to the outer `make_connector()` scope.

`log_closures_on_invoke` makes that value visible in the `invoke` payload:

```text
log_closures_on_invoke={"target": target}
```

## Emitted records

A successful call:

```text
connect = make_connector(log_context, target="ldap")
await connect()
```

emits records conceptually equivalent to:

```text
[
    {
        "event_name": "connect",
        "event_outcome": "invoke",
        "payload": {
            "closures": {
                "target": "ldap",
            },
        },
    },
    {
        "event_name": "connect",
        "event_outcome": "success",
        "payload": {},
    },
]
```

The `closures` section appears only in the `invoke` payload.

The `success` payload is empty because this example does not use `context_fields` or `log_result_on_success`.

## Why use this pattern

Use this pattern when the operation is represented by a function rather than a method, but it still needs structured lifecycle logging.

The explicit context solves the missing-`self` problem:

```text
method form
    self.get_log_context()

standalone function form
    ctx=log_context
```

The closure values solve a different problem: some useful diagnostic values may not be function arguments.

In this example, `target` is known when the function is created, not when it is called.

## What this example demonstrates

This example demonstrates three things:

```text
standalone functions can use log_invocation
explicit ctx provides the logging context
closure values can be added to the invoke payload
```

Use `log_closures_on_invoke` for values captured from the surrounding scope that are useful at operation start.
