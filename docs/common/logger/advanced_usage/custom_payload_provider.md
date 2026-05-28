# Custom payload provider

```{contents} Contents:
:depth: 1
:local:
```

## Overview

`LogPayloadProvider` lets an object define its own logging representation.

Instead of asking the default payload processor to guess how an object should appear in logs, the object can expose a `to_log_payload()` method:

```python
from typing import Any


class UserSession:
    def __init__(self, session_id: str, user_id: str, token: bytes) -> None:
        self.session_id = session_id
        self.user_id = user_id
        self.token = token

    def to_log_payload(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "user_id": self.user_id,
            "token_size": len(self.token),
        }
```

When this object is normalized by the default payload processor, the returned dictionary is used as the object's log representation.

This is useful when the object itself knows which fields are safe, stable, and meaningful for diagnostics.

## When to use it

Use `LogPayloadProvider` when a domain object has a clear logging representation.

Good candidates:

```text
request objects
response objects
connection descriptors
session objects
configuration snapshots
operation result objects
small domain DTOs
```

The object should be able to answer:

```text
What should this object look like in logs?
```

without exposing sensitive or overly large internal data.

## When not to use it

Do not implement `to_log_payload()` when logging representation depends on external context that the object should not know about.

Avoid it when:

```text
the object would need a logger context to decide its payload
the representation depends on the sink or output format
the representation needs expensive I/O
the object would have to expose secrets conditionally
the object is from a third-party library you should not modify
```

For third-party or external objects, use a type-based log adapter instead.

For global changes to normalization behavior, use a custom payload processor.

## Provider vs adapter vs processor

There are three different extension levels.

```text
LogPayloadProvider
    the object provides its own representation

LogAdapter
    external callable provides representation for objects it recognizes

LogPayloadProcessor
    full normalization strategy is replaced or customized
```

Use the smallest extension point that solves the problem.

If the object owns its logging representation, use `LogPayloadProvider`.

If the object cannot or should not implement `to_log_payload()`, use an adapter.

If the entire normalization strategy must change, use a custom processor.

## Precedence in the default processor

The default `LogPayloadProcessor` checks `LogPayloadProvider` before type-based adapters.

The order is:

```text
1. object implements LogPayloadProvider
2. configured LogAdapterResolver returns an adapter
3. built-in normalization rules
```

This means `to_log_payload()` wins over adapters.

If an object implements `LogPayloadProvider`, the default processor tries that first.

If the method returns a dictionary, that dictionary is used.

If the method raises or returns a non-dictionary value, the processor falls back to the remaining normalization path.

## Log-ready responsibility

The payload returned by `to_log_payload()` is expected to be log-ready.

The provider is responsible for:

```text
choosing safe fields
avoiding secrets
keeping payload size reasonable
using stable key names
returning a dictionary
```

The provider should not return raw internal state just because it is available.

A good provider exposes diagnostic identity and useful summaries.

A poor provider dumps the entire object.

## Example: safe session payload

```python
from typing import Any


class UserSession:
    def __init__(self, session_id: str, user_id: str, token: bytes) -> None:
        self.session_id = session_id
        self.user_id = user_id
        self.token = token

    def to_log_payload(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "user_id": self.user_id,
            "token_size": len(self.token),
        }
```

The token itself is not logged.

Only its size is exposed:

```python
{
    "session_id": "...",
    "user_id": "...",
    "token_size": 128,
}
```

This makes the payload useful for diagnostics without leaking credentials.

## Example: connection descriptor

```python
from typing import Any


class ConnectionInfo:
    def __init__(self, host: str, port: int, secure: bool) -> None:
        self.host = host
        self.port = port
        self.secure = secure

    def to_log_payload(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "secure": self.secure,
        }
```

This object has a simple stable logging representation.

When it appears in an event payload, the processor can use that representation instead of falling back to a generic object placeholder.

## Using provider objects in event payloads

A provider object can be placed directly inside a payload:

```python
ctx.log_info_event(
    event="session.created",
    payload={
        "session": session,
    },
)
```

The default payload processor will normalize `session` through `to_log_payload()` if `session` implements `LogPayloadProvider`.

Conceptually, the emitted payload can contain:

```python
{
    "session": {
        "session_id": "...",
        "user_id": "...",
        "token_size": 128,
    }
}
```

## Using provider objects with log_invocation

`LogPayloadProvider` also works with values selected by `log_invocation`.

For example, a result object can provide its own representation:

```python
class CreateSessionResult:
    def __init__(self, session: UserSession) -> None:
        self.session = session

    def to_log_payload(self) -> dict[str, Any]:
        return {
            "session": self.session.to_log_payload(),
        }
```

If `log_result_on_success=()` is used, the whole result is normalized.

The provider controls the result representation.

```python
@log_invocation(
    "create_session",
    log_result_on_success=(),
)
def create_session(self, user_id: str) -> CreateSessionResult:
    ...
```

The decorator still owns lifecycle logging. The provider only controls how the selected result value is represented.

## Keep provider output stable

Provider payload keys become part of your diagnostic vocabulary.

Prefer stable, boring names:

```text
session_id
user_id
host
port
state
status
```

Avoid names tied to temporary implementation details:

```text
tmp_flag
internal_obj
raw_data
step_2_value
```

Changing provider output can affect tests, dashboards, alerts, and downstream consumers.

Treat the payload shape as a small public contract of the object.

## Avoid expensive work

`to_log_payload()` may be called on the logging path.

Keep it fast.

Avoid:

```text
network calls
database queries
large filesystem reads
expensive serialization
lazy loading large data
locking slow external resources
```

The method should build a small representation from data already available on the object.

If producing the representation is expensive, consider logging only a stable identifier and collecting detailed diagnostics elsewhere.

## Thread-safety considerations

The payload processor may normalize values while application code is running concurrently.

If the provider reads mutable object state, it should either:

```text
read only stable immutable fields
protect mutable state with the object's own synchronization
return a best-effort snapshot that is safe for logging
```

The logger does not make arbitrary domain objects thread-safe.

`LogPayloadProvider` controls representation, not synchronization.

## What happens on provider failure

The default payload processor treats provider failures as normalization failures, not operation failures.

If `to_log_payload()` raises, the processor falls back to generic normalization.

If `to_log_payload()` returns a non-dictionary value, the result is ignored and generic normalization continues.

This keeps logging from breaking domain code because one provider representation failed.

A provider should still be tested. Silent fallback can hide a broken representation until logs look less useful than expected.

## Design summary

`LogPayloadProvider` is the object-owned payload representation hook.

Use it when an object knows its own safe logging shape.

Keep the returned payload small, stable, and free of sensitive data.

Prefer provider methods for domain objects you own.

Use adapters for external objects.

Use a custom processor only when the whole normalization strategy needs to change.
