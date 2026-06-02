# Payload fields

```{contents} Contents:
:depth: 1
:local:
```

## Overview

`log_invocation` does not log all function arguments, result values, or object state automatically.

Payload data is selected explicitly.

The decorator has three main options that use field specifications:

```text
context_fields
log_kwargs_on_invoke
log_result_on_success
```

They all describe what data should be extracted, but they are used at different lifecycle moments:

```text
context_fields
    resolved for every emitted outcome

log_kwargs_on_invoke
    resolved only for invoke

log_result_on_success
    resolved only for success
```

This article explains the shared field-spec syntax and how each option uses it.

## Why explicit fields

Function calls often contain sensitive, large, or unstable data.

Examples:

```text
passwords
tokens
raw payloads
large byte strings
network packets
internal buffers
backend clients
```

Logging all arguments or results by default would be dangerous and noisy.

`log_invocation` uses opt-in field selection instead.

The decorated method decides what is safe and useful to place into the structured payload.

## Field spec basics

A field spec is a string that tells the decorator how to extract one value.

The simplest form selects a function argument by name:

```text
request_id
```

For a decorated method:

```python
@log_invocation(
    "send_request",
    log_kwargs_on_invoke=("request_id",),
)
def send_request(self, request_id: str, payload: bytes) -> None:
    ...
```

The selected value is added to the `kwargs` section of the `invoke` payload.

Conceptually, the payload contains:

```text
{
    "kwargs": {
        "request_id": "<normalized request_id>"
    }
}
```

## Attribute paths

A field spec can follow attributes using dot notation.

```text
request.id
request.user.name
```

Example:

```python
from dataclasses import dataclass

from mvx.common.logger import LogContextProto, log_invocation


@dataclass(frozen=True, slots=True)
class Request:
    request_id: str
    method: str


class Client:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context

    @log_invocation(
        "send_request",
        log_kwargs_on_invoke=("request.request_id", "request.method"),
    )
    def send_request(self, request: Request) -> None:
        ...
```

The field path is resolved through `getattr()`.

It does not use dictionary-key lookup for dotted segments.

For example, `request.id` means:

```python
getattr(request, "id")
```

It does not mean:

```python
request["id"]
```

## Aliases

A field spec can rename the extracted value.

```text
alias=path.to.value
```

Example:

```python
@log_invocation(
    "send_request",
    log_kwargs_on_invoke=("request_id=request.request_id",),
)
def send_request(self, request: Request) -> None:
    ...
```

The payload key becomes `request_id` even though the value was read from `request.request_id`.

Aliases are useful when the selected path is too long or when you want stable payload names independent of object attribute names.

## Default alias

If no explicit alias is supplied, the last path segment is used as the payload key.

```text
request.request_id -> request_id
request.method     -> method
```

For a top-level argument, the argument name is used:

```text
request_id -> request_id
```

For readability, prefer an explicit alias for length specs:

```text
payload_size=payload.len()
```

## Length specs

The special path segment `len()` asks the decorator to call `len()` on the current value.

```text
payload.len()
```

Example:

```python
@log_invocation(
    "send_request",
    log_kwargs_on_invoke=("payload_size=payload.len()",),
)
def send_request(self, payload: bytes) -> None:
    ...
```

The decorator resolves `payload`, calls `len(payload)`, and logs the resulting value.

If `len()` raises, the field is skipped.

## Unbounded item marker

A field spec can end with `!`.

```text
headers!
```

This tells the context value normalizer to process the selected value with `unbounded=True`.

Example:

```python
@log_invocation(
    "send_request",
    log_kwargs_on_invoke=("headers!",),
)
def send_request(self, headers: dict[str, str]) -> None:
    ...
```

Use this only for values where disabling item-count limiting is intentional.

The marker affects normalization of the selected value. It does not change field resolution itself.

## Verbosity gates

A field spec can be gated by the active plain verbosity level.

```text
MAXIMUM:request.debug_info
NORMAL,MAXIMUM:request_id
```

The part before `:` is a comma-separated set of supported verbosity levels.

The part after `:` is the actual field spec.

The decorator asks the context for its plain verbosity level:

```text
ctx.get_plain_verbosity_level()
```

If the current verbosity level is not listed, the field is skipped.

Example:

```python
@log_invocation(
    "send_request",
    log_kwargs_on_invoke=(
        "request_id",
        "MAXIMUM:payload_preview=payload",
    ),
)
def send_request(self, request_id: str, payload: bytes) -> None:
    ...
```

Here `request_id` is always selected, while `payload_preview` is selected only at `MAXIMUM` verbosity.

If the context has no plain verbosity level, verbosity-gated specs are skipped.

## Resolution failures

If a field cannot be resolved, it is skipped.

Examples:

```text
missing top-level argument
missing attribute
attribute access raises
len() raises
empty field spec
empty alias/path after parsing
verbosity gate does not match
```

A skipped field does not fail the operation.

Field selection is best-effort because logging should not make public API methods fail just because an optional diagnostic field is unavailable.

This applies to field resolution. It does not mean every decorator setup error is swallowed. Missing context, broken context provider, or broken `entity_id_getter` are integration errors and surface immediately.

## context_fields

`context_fields` add selected values to every outcome emitted for the decorated event.

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

The decorator captures the bound function arguments once, when the wrapper is called.

However, `context_fields` are resolved again before each emitted outcome. Attribute paths are evaluated at that moment.

In this example, `state` is resolved separately for `invoke` and `success`.

The `invoke` outcome is emitted before the method body runs, so it can contain:

```text
state = "closed"
```

The `success` outcome is emitted after the method body completes, so it can contain:

```text
state = "opened"
```

This is useful for values that should appear on all outcomes, but whose current value may change during the operation.

A request id or message id is usually stable context. A connection state is dynamic context.

## log_kwargs_on_invoke

`log_kwargs_on_invoke` adds selected argument values to the `invoke` payload only.

Selected values are placed under the `kwargs` key.

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
    def send_request(self, request_id: str, method: str, payload: bytes) -> None:
        ...
```

The `invoke` payload can contain:

```text
kwargs.request_id
kwargs.method
kwargs.payload_size
```

Use this option for input values that are useful at operation start but do not need to appear on `success`, `failed`, or `cancelled` outcomes.

Do not use it for secrets such as passwords or tokens.

## log_result_on_success

`log_result_on_success` controls result logging for the `success` outcome.

If it is `None`, the result is not logged.

```python
@log_invocation(
    "create_session",
    log_result_on_success=None,
)
def create_session(self, user_id: str) -> Session:
    ...
```

This is the default.

If it is an empty tuple, the whole result is normalized and placed under the `result` key.

```python
@log_invocation(
    "count_users",
    log_result_on_success=(),
)
def count_users(self) -> int:
    return 42
```

If it contains field specs, selected result fields are logged.

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

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context

    @log_invocation(
        "create_session",
        log_result_on_success=("session_id", "ttl_ms"),
    )
    def create_session(self, user_id: str) -> Session:
        return Session(session_id=user_id, ttl_ms=30_000)
```

The selected result data is placed under the `result` key.

## Result field behavior

Result field behavior depends on the result type.

### Primitive results

For primitive values, the whole result is normalized.

```text
str
int
float
bool
None
```

Field specs are ignored for primitive results.

### List and tuple results

For list and tuple results, field specs can select items by index.

```text
0
user_id=1.id
ttl_ms=2.ttl_ms
```

If no field specs are supplied, the whole list or tuple is normalized.

If field specs are supplied but none can be resolved, the whole list or tuple is normalized.

### Dict results

For dictionary results, field specs are ignored and the whole dictionary is normalized.

This is deliberate in the current implementation.

If you need selected dictionary values, return a small result object or normalize the result manually before returning it.

### Composite object results

For composite object results, field specs select attributes.

```text
session_id
user_id=user.id
token_size=token.len()
```

If no field specs are supplied, the object is normalized as a whole.

If field specs are supplied but none can be resolved, the object falls back to normal value normalization.

## Avoid duplicate payload fields

Be careful when using `context_fields` and `log_kwargs_on_invoke` together.

`context_fields` are included in every emitted outcome.

`log_kwargs_on_invoke` is included only under the `kwargs` key of the `invoke` payload.

Using the same field in both places can intentionally produce duplicate information in the `invoke` record.

For example:

```python
@log_invocation(
    "save",
    context_fields=("item_id",),
    log_kwargs_on_invoke=("item_id",),
)
```

The `invoke` payload can contain `item_id` as shared context and also under `kwargs`.

Usually, choose one location unless the duplication is intentional.

For stable identifiers that should appear on every outcome, prefer `context_fields`.

For input values that matter only at operation start, prefer `log_kwargs_on_invoke`.

## Payload shape summary

The main payload locations are:

```text
context_fields
    top-level payload by default

log_kwargs_on_invoke
    payload["kwargs"]

log_closures_on_invoke
    payload["closures"]

log_result_on_success
    payload["result"]

error payload
    payload["error"]
```

`context_formatter` can change how resolved context fields are injected into the target payload.

If produced context payload conflicts with system keys, it is placed under the `context` key instead of being merged at top level.

System keys are:

```text
error
kwargs
result
cancelled
closures
```

## Timing summary

Payload pieces are collected at different lifecycle moments.

```text
bound arguments
    captured once when wrapper is called

closures
    added to invoke payload only

log_kwargs_on_invoke
    resolved for invoke only

context_fields
    resolved separately for every emitted outcome

result
    resolved only for success, when result logging is enabled

error
    resolved only for failed or cancelled outcomes
```

This timing matters when selected fields point to mutable objects or changing object state.

## Design summary

`log_invocation` uses explicit field specs instead of logging arguments and results automatically.

Use `context_fields` for values that should appear on all outcomes.

Use `log_kwargs_on_invoke` for selected input values that matter only when the operation starts.

Use `log_result_on_success` for selected result data that should be logged after successful completion.

Field specs are best-effort and skip unresolved values.

The decorator keeps payload selection explicit, so public API methods can be logged without accidentally exposing every argument, result, or internal object.
