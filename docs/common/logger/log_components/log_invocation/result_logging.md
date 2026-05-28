# Result logging

```{contents} Contents:
:depth: 1
:local:
```

## Overview

`log_invocation` can include operation result data in the `success` outcome.

Result logging is controlled by `log_result_on_success`.

It is opt-in:

```python
@log_invocation("create_session")
def create_session(self, user_id: str) -> Session:
    ...
```

With the default configuration, the returned `Session` is not logged.

To log result data, pass `log_result_on_success` explicitly.

## Why result logging is opt-in

Operation results may contain sensitive, large, or unstable data.

Examples:

```text
access tokens
password-derived material
raw protocol responses
large byte buffers
backend clients
objects with internal state
```

Logging full results automatically would be noisy and dangerous.

`log_invocation` requires the decorated API method to explicitly choose what result data should be written to the structured payload.

## Basic modes

`log_result_on_success` has three important modes.

```text
None
    do not log the result

()
    log the whole result

("field", ...)
    log selected result fields
```

The default is `None`.

## No result logging

When `log_result_on_success` is `None`, the `success` payload does not include a `result` field.

```python
@log_invocation(
    "create_session",
    log_result_on_success=None,
)
def create_session(self, user_id: str) -> Session:
    ...
```

This is the default behavior.

Use it when the result is not useful for diagnostics or should not be logged.

## Logging the whole result

When `log_result_on_success` is an empty tuple, the whole result is normalized and placed under the `result` key.

```python
@log_invocation(
    "count_users",
    log_result_on_success=(),
)
def count_users(self) -> int:
    return 42
```

The `success` payload contains result data conceptually like this:

```python
{
    "result": 42
}
```

Use this mode only when the full result is safe and reasonably small.

## Logging selected result fields

When `log_result_on_success` contains field specs, the decorator tries to extract selected values from the result.

```python
from dataclasses import dataclass

from mvx.common.logger import LogContextProto, log_invocation


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
        return Session(session_id=user_id, ttl_ms=30_000)
```

The `success` payload contains selected result data under the `result` key:

```python
{
    "result": {
        "session_id": "...",
        "ttl_ms": 30000,
    }
}
```

## Primitive results

Primitive results are logged as whole values.

Primitive result types are:

```text
str
int
float
bool
None
```

For these values, field specs are ignored.

```python
@log_invocation(
    "count_users",
    log_result_on_success=("some_field",),
)
def count_users(self) -> int:
    return 42
```

Even though a field spec is supplied, the logged result is the normalized primitive value:

```python
{
    "result": 42
}
```

This keeps primitive result logging simple: a primitive has no fields to select.

## List and tuple results

For list and tuple results, field specs can select items by index.

```text
0
1
user_id=1.id
ttl_ms=2.ttl_ms
```

Example:

```python
from dataclasses import dataclass

from mvx.common.logger import LogContextProto, log_invocation


@dataclass(frozen=True, slots=True)
class User:
    user_id: str


class UserService:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto:
        return self._log_context

    @log_invocation(
        "create_user",
        log_result_on_success=("status=0", "user_id=1.user_id"),
    )
    def create_user(self, name: str) -> tuple[str, User]:
        return "created", User(user_id=name)
```

The selected result data is placed under `result`:

```python
{
    "result": {
        "status": "created",
        "user_id": "...",
    }
}
```

If a list or tuple field spec cannot be resolved, that field is skipped.

If no selected fields can be resolved, the whole list or tuple is normalized.

## Dict results

For dictionary results, field specs are ignored and the whole dictionary is normalized.

```python
@log_invocation(
    "load_config",
    log_result_on_success=("version",),
)
def load_config(self) -> dict[str, object]:
    return {"version": 1, "debug": False}
```

The supplied field spec does not select only `version`.

The whole dictionary is logged under `result`:

```python
{
    "result": {
        "version": 1,
        "debug": False,
    }
}
```

This is the current behavior of `log_invocation`.

If selected dictionary result logging is needed, return a small result object instead of a dictionary, or build a result value that is already safe to log as a whole.

## Composite object results

For composite object results, field specs select attributes.

```text
session_id
user_id=user.id
token_size=token.len()
```

Example:

```python
from dataclasses import dataclass

from mvx.common.logger import LogContextProto, log_invocation


@dataclass(frozen=True, slots=True)
class Token:
    value: bytes


@dataclass(frozen=True, slots=True)
class LoginResult:
    user_id: str
    token: Token


class AuthService:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto:
        return self._log_context

    @log_invocation(
        "login",
        log_result_on_success=("user_id", "token_size=token.value.len()"),
    )
    def login(self, username: str, password: str) -> LoginResult:
        return LoginResult(user_id=username, token=Token(value=b"secret-token"))
```

The selected payload avoids logging the token itself:

```python
{
    "result": {
        "user_id": "...",
        "token_size": 12,
    }
}
```

If no field specs are supplied for a composite object, the whole object is normalized.

If field specs are supplied but none can be resolved, the object falls back to normal value normalization.

## Aliases and len()

Result field specs support the same alias and length syntax as other field specs.

Alias:

```text
session=session_id
```

Length:

```text
token_size=token.value.len()
```

For `len()` specs, use an explicit alias.

```text
payload_size=payload.len()
```

Without an explicit alias, the default payload key is derived mechanically from the last path segment, which is not useful for `len()` specs.

## Unbounded marker

A result field spec can end with `!`.

```text
headers!
```

This passes `unbounded=True` to value normalization for that selected value.

Use it only when disabling item-count limiting is intentional.

Example:

```python
@log_invocation(
    "get_headers",
    log_result_on_success=("headers!",),
)
def get_headers(self) -> HeadersResult:
    ...
```

The marker affects normalization of the selected value. It does not change field resolution.

## Resolution failures

Unresolvable result fields are skipped.

Examples:

```text
missing attribute
attribute access raises
invalid tuple/list index
len() raises
empty field spec
empty alias/path after parsing
```

For list and tuple results, if all selected fields are skipped, the whole result is normalized.

For composite object results, if all selected fields are skipped, the whole result is normalized.

This fallback is useful, but it also means that a wrong field spec can accidentally log more than intended.

For sensitive result objects, prefer tests that check the emitted payload shape.

## Timing

Result logging happens only after successful completion.

```text
operation returns result
   |
   v
build success payload
   |
   v
resolve result fields if enabled
   |
   v
emit success outcome
   |
   v
return original result
```

No result is logged for failed or cancelled operations.

The returned result object is not replaced or modified by the decorator.

## Interaction with context fields

The `success` outcome may contain both context fields and result data.

```python
@log_invocation(
    "create_session",
    context_fields=("user_id",),
    log_result_on_success=("session_id", "ttl_ms"),
)
def create_session(self, user_id: str) -> Session:
    return Session(session_id=user_id, ttl_ms=30_000)
```

Conceptually, the success payload can contain:

```python
{
    "user_id": "...",
    "result": {
        "session_id": "...",
        "ttl_ms": 30000,
    }
}
```

`context_fields` describe the operation context.

`result` describes the successful return value.

Keep those roles separate unless duplication is intentional.

## What result logging does not do

* Result logging does not run for `invoke`, `failed`, or `cancelled` outcomes.
* It does not log results by default.
* It does not select dictionary keys from dictionary results.
* It does not mutate or replace the original result.
* It does not make unsafe results safe automatically.
* It only selects and normalizes the result data that the decorated method explicitly asked to log.

## Design summary

`log_result_on_success` controls whether result data is included in the `success` outcome.

Use `None` to omit result logging.

Use an empty tuple to log the whole result.

Use field specs to log selected attributes or indexed list/tuple values.

Be especially careful with dictionaries and fallback behavior, because unresolved field specs can result in whole-value normalization.

Result logging is intentionally explicit so public API methods can expose useful diagnostics without accidentally logging full return objects.
