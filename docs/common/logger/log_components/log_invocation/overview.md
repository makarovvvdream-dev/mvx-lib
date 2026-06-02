# log_invocation

```{contents} Contents:
:depth: 1
:local:
```

## Overview

`log_invocation` is a decorator for structured logging of public API operations.

It belongs to `log_components`: a package of utility tools built on top of the core logging infrastructure.

The core logger provides the infrastructure:

```text
LogContext
LogEventMeta
LogEvent
LogEventPolicyProto
LogPayloadProcessorProto
LogSinkProto
```

`log_invocation` uses that infrastructure to solve one common task:

> Log a public method call as one named event with a clear operation outcome.

The decorator argument is the event name:

```python
from mvx.common.logger import LogContextProto, log_invocation


class LdapClient:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context

    @log_invocation("bind")
    async def bind(self, dn: str, password: str) -> None:
        ...
```

Here `bind` is the event.

For that event, the decorator can emit log records with different outcomes:

```text
invoke
success
failed
cancelled
```

The resulting event shape is:

```text
event_name     = "bind"
event_outcome  = "invoke"
```

or:

```text
event_name     = "bind"
event_outcome  = "success"
```

or:

```text
event_name     = "bind"
event_outcome  = "failed"
```

or:

```text
event_name     = "bind"
event_outcome  = "cancelled"
```

So `invoke`, `success`, `failed`, and `cancelled` are not separate events. They are outcomes of the same decorated event.

## What problem it solves

Public API methods often have the same logging shape:

```text
operation started
operation completed
operation failed
operation was cancelled
```

Without log_invocation, that usually becomes repeated boilerplate:

```text
ctx.log_debug_event(...)
try:
    result = await operation(...)
except asyncio.CancelledError:
    ctx.log_info_event(...)
    raise
except Exception:
    ctx.log_error_event(...)
    raise
else:
    ctx.log_debug_event(...)
    return result
```

`log_invocation` moves this repeated operation-lifecycle pattern to the method boundary.

The method stays focused on the operation itself. The decorator describes how that operation should be represented in structured logs.

## When to use it

Use `log_invocation` primarily on public API methods.

A good decorated method has independent, atomic value from the user's point of view. The caller can understand the operation without knowing the component's internal implementation.

Good candidates are public methods such as:

```text
connect
open
close
bind
send_request
search
create_user
update_registry
start_worker
stop_worker
```

These methods are useful diagnostic landmarks because they describe the observable behavior of the component.

For example, a user looking at logs can understand this sequence:

```text
connect invoke
connect success
bind invoke
bind failed
close invoke
close success
```

That sequence speaks in public API terms. It does not force the user to learn private helper calls, parsing steps, temporary buffers, retry counters, or implementation details.

## When not to use it

Do not decorate every internal helper.

Private methods and small implementation steps are often not meaningful as standalone operations. Decorating them can produce noisy traces and expose internal mechanics instead of documenting the public behavior of the component.

Usually avoid `log_invocation` for methods such as:

```text
_resolve_flag
_parse_one_item
_build_tmp_payload
_normalize_local_value
_increment_counter
```

Those steps may be useful inside a manual diagnostic event, but they are usually not good operation boundaries.

Also avoid the decorator in extremely hot paths where even structured operation logging would be too expensive or too noisy.

## Relationship to manual logging

`log_invocation` and manual logging are complementary.

Use `log_invocation` for the public operation frame:

```text
operation invoked
operation succeeded
operation failed
operation cancelled
```

Use manual `LogContext` events for meaningful internal milestones when those milestones are useful diagnostics but are not public API operations:

```text
cache miss
connection selected
request encoded
response decoded
retry scheduled
schema loaded
```

The decorator gives the outer frame. Manual events fill the inside of the frame when the internal steps matter.

## Context resolution

`log_invocation` needs a logging context.

It can get one in two ways.

The common method-based form resolves the context from the first positional argument:

```python
from mvx.common.logger import LogContextProto, log_invocation


class Client:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context

    @log_invocation("connect")
    async def connect(self) -> None:
        ...
```

Usually the first positional argument is `self`, and the object implements `LogContextProviderProto` structurally by providing `get_log_context()`.

The other form passes the context explicitly:

```python
from mvx.common.logger import LogContextProto, log_invocation


def make_loader(log_context: LogContextProto):
    @log_invocation("load_config", ctx=log_context)
    def load_config(path: str) -> dict[str, object]:
        return {"path": path}

    return load_config
```

When `ctx` is passed explicitly, the decorator does not resolve the context from function arguments.

If no explicit context is supplied and the first positional argument does not provide `LogContextProviderProto`, the decorator raises a runtime error.

If the first positional argument provides `LogContextProviderProto` but `get_log_context()` returns `None`, decorator-driven logging is disabled for the current call. The decorated callable is executed normally, and no `invoke`, `success`, `failed`, or `cancelled` lifecycle events are emitted.

## Event name

The first decorator argument is the event name.

```python
from mvx.common.logger import log_invocation

@log_invocation("send_request")
def send_request(request_id: str) -> None:
    ...
```

The event name should describe the public operation.

Good event names are stable and boring:

```text
connect
bind
send_request
search
close
```

Weak event names are message-like or implementation-specific:

```text
trying_to_connect
step_1
internal_parse_phase
something_happened
```

The event name must match:

```text
^[A-Za-z_.]+$
```

Valid examples:

```text
connect
send_request
ldap.bind
schema.load
```

Invalid examples:

```text
send-request
send request
request:send
```

## Event outcome

`event_outcome` describes the operation outcome or phase for the decorated event.

`log_invocation` uses four outcomes:

```text
invoke
success
failed
cancelled
```

The important user-facing rule is simple:

```text
event_name identifies the operation
event_outcome identifies the operation outcome
```

For example:

```text
event_name     = "search"
event_outcome  = "invoke"
```

and later:

```text
event_name     = "search"
event_outcome  = "success"
```

These are two log records for the same decorated event.

## What is emitted

For an enabled successful operation, `log_invocation` emits records with these outcomes:

```text
invoke
success
```

For an operation that raises an exception, it emits:

```text
invoke
failed
```

For an operation that is cancelled, it emits:

```text
invoke
cancelled
```

There is one important policy nuance.

Before emitting the `invoke` outcome, the decorator creates `LogEventMeta` and asks the context whether the event is enabled:

```text
create LogEventMeta
   |
   v
effective_ctx.is_event_enabled(meta)
```

If the event is disabled, the decorator does not emit `invoke` and does not emit `success`.

Failure and cancellation are still reported by their error paths.

This means ordinary operation tracing can be filtered out, while exceptional outcomes remain visible.

## Entity id

The decorator can attach an `entity_id` to `LogEventMeta`.

The first positional argument may implement `LogEntityIdProviderProto` structurally by exposing an `identity` property:

```python
from mvx.common.logger import LogContextProto, log_invocation


class Client:
    def __init__(self, log_context: LogContextProto, client_id: str) -> None:
        self._log_context = log_context
        self._client_id = client_id

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context

    @property
    def identity(self) -> str:
        return self._client_id

    @log_invocation("connect")
    async def connect(self) -> None:
        ...
```

Or the decorator can receive an explicit zero-argument getter:

```python
from mvx.common.logger import log_invocation, configure_log_context

log_context = configure_log_context("mvx.connector")

@log_invocation(
    "connect",
    ctx=log_context,
    entity_id_getter=lambda: "client-1",
)
async def connect() -> None:
    ...
```

The explicit getter wins when supplied.

## Invoke payload

The `invoke` outcome can include:

```text
closures
context fields
selected arguments
```

By default, function arguments are not logged.

Argument logging is opt-in through `log_kwargs_on_invoke`:

```python
from mvx.common.logger import LogContextProto, log_invocation


class Client:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context

    @log_invocation(
        "send_request",
        log_kwargs_on_invoke=("request_id", "method", "payload.len()"),
    )
    def send_request(self, request_id: str, method: str, payload: bytes) -> None:
        ...
```

The selected values are placed under the `kwargs` key of the `invoke` payload.

This keeps accidental sensitive or large argument values out of logs unless the developer explicitly selects them.

## Closure values

`log_closures_on_invoke` allows values captured by a decorated function closure to be included in the `invoke` payload.

This is useful when `log_invocation` is applied to a function rather than a method, and some useful diagnostic values are not passed as function arguments but are available from the surrounding scope.

For example:

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
In this example, target is not an argument of connect(). It belongs to the outer make_connector() scope.

log_closures_on_invoke makes that value visible in the invoke payload under the closures key.

The values are normalized before being written to the payload. If a closure value cannot be normalized, the decorator stores "<unknown>" for that key.


## Context fields

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
The decorator captures the bound function arguments once, when the wrapper is called. However, context_fields are resolved again before each emitted outcome. Attribute paths are evaluated at that moment.

In this example, state is resolved separately for invoke and success.

The invoke outcome is emitted before the method body runs, so it can contain state="closed".

The success outcome is emitted after the method body completes, so it can contain state="opened".

This is useful for values that should appear on invoke, success, failed, and cancelled outcomes alike, but whose current value may change during the operation.

A request id or message id is usually stable context. A connection state is dynamic context.

## Field specs

Several decorator options use field specs:

```text
context_fields
log_kwargs_on_invoke
log_result_on_success
```

A field spec can select a value by name:

```text
request_id
```

It can follow attributes:

```text
request.id
request.user.name
```

It can rename the field:

```text
user_id=request.user.id
```

It can ask for length:

```text
payload.len()
```

It can disable item-count limiting for that selected value:

```text
headers!
```

And it can be gated by the active plain verbosity level:

```text
MAXIMUM:request.debug_info
NORMAL,MAXIMUM:request.id
```

Unresolvable field specs are skipped.

Field specs use attribute access for nested paths. They do not use dictionary-key lookup for dotted segments.

## Context formatter

`context_formatter` is an optional callable that can build custom payload data from resolved context fields.

It receives:

```text
ctx
event_outcome
event name
resolved raw fields
```

and may return a dictionary to merge into the event payload.

```python
from typing import Any

from mvx.common.logger import LogContextProto, log_invocation


def format_context(
    ctx: LogContextProto,
    event_outcome: object,
    event: str,
    fields: dict[str, Any],
) -> dict[str, Any]:
    return {
        "event": event,
        "outcome": str(event_outcome),
        "fields": fields,
    }
```

This is useful when selected fields need custom shaping before they become part of the log payload.

If the formatter raises or does not return a dictionary, the decorator falls back to normal field handling.

If the formatter returns keys that conflict with system keys, the produced payload is placed under the `context` key instead of being merged directly into the top-level payload.

System keys are:

```text
error
kwargs
result
cancelled
closures
```

## Success payload

When the wrapped operation completes successfully, `log_invocation` emits the `success` outcome if the event was enabled at invocation time.

By default, the success payload contains only context fields produced by `context_fields` or `context_formatter`.

Result logging is opt-in through `log_result_on_success`.

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

If `log_result_on_success` is `None`, the result is not logged.

If it is an empty tuple, the whole result is normalized and placed under the `result` key.

If it contains field specs, only selected result fields are logged.

## Result logging behavior

Result logging depends on the result shape.

For primitive values, the whole result is normalized.

For lists and tuples, field specs may select indexes:

```text
0
user_id=1.id
ttl_ms=2.ttl_ms
```

For dictionaries, the whole dictionary is normalized.

For composite objects, field specs select attributes:

```text
session_id
user_id=user.id
token.len()
```

If no selected field can be resolved for a composite object, the result falls back to the normal value normalization behavior.

## Failure payload

When the wrapped operation raises an exception, `log_invocation` emits the `failed` outcome and then re-raises the original exception.

The decorator does not turn domain errors into logger errors.

The normal shape is:

```text
operation raises exception
   |
   v
emit failed outcome
   |
   v
re-raise the same exception
```

The failed payload can include an `error` field produced by the context:

```text
effective_ctx.build_error_payload(err)
```

The context decides how to represent the exception. Exceptions may provide their own structured payload through `to_log_payload()`, or the context can fall back to generic error fields.

## Error policy

`log_error_policy` controls whether specific exception types should be logged with full error payload or suppressed error payload.

The type is:

```python
LogErrorPolicyRule = tuple[type[BaseException], bool]
LogErrorPolicy = tuple[LogErrorPolicyRule, ...]
```

Each rule contains:

```text
exception type
force_log flag
```

If `force_log` is `True`, the failed outcome includes the full `error` payload.

If `force_log` is `False`, the failed outcome is emitted at `error_level_suppressed` without the detailed `error` payload.

This is useful when some exception classes are expected, already logged elsewhere, or too noisy for full repeated logging.

## Repeated error suppression

`log_invocation` uses context error marker helpers:

```text
is_error_logged(err)
mark_error_logged(err)
```

If the same exception instance was already logged with full error details, a later failed outcome can be emitted at `error_level_suppressed` without the detailed `error` payload.

This avoids repeating the same error body multiple times while still preserving the fact that the operation failed.

The default suppressed level is `LogLevel.DEBUG`.

The original exception is still re-raised.

## Cancellation payload

For `asyncio.CancelledError`, `log_invocation` emits the `cancelled` outcome and re-raises the same cancellation.

Cancellation is not treated as an ordinary failure.

The payload includes:

```text
cancelled = True
error = build_error_payload(cancelled_error)
```

The event level defaults to `LogLevel.INFO`.

This preserves asyncio cancellation semantics. The decorator records the outcome, but it does not swallow or convert cancellation.

If the same cancellation exception instance is already marked as logged, the decorator does not emit another `cancelled` outcome.

## Sync, async, and awaitable results

`log_invocation` supports:

```text
regular synchronous functions
async functions
synchronous functions that return an awaitable
```

For an `async def`, the decorator awaits the function and logs the final outcome.

For a regular function, the decorator logs the outcome after the function returns.

If a regular function returns an awaitable, the decorator wraps that awaitable so that success, failure, and cancellation are logged after the awaitable completes.

This is useful for APIs that are synchronous at the call boundary but produce asynchronous work.

## Levels

Each outcome has its own level option:

```python
from mvx.common.logger import LogLevel, log_invocation, configure_log_context

log_context = configure_log_context("mvx.connector")

@log_invocation(
    "send_request",
    ctx=log_context,
    invoke_level=LogLevel.DEBUG,
    success_level=LogLevel.DEBUG,
    error_level=LogLevel.ERROR,
    error_level_suppressed=LogLevel.DEBUG,
    cancel_level=LogLevel.INFO,
)
def send_request() -> None:
    ...
```

The defaults are:

```text
invoke              DEBUG
success             DEBUG
failed              ERROR
failed suppressed   DEBUG
cancelled           INFO
```

This makes normal operation tracing quiet by default and failures louder by default.

## What log_invocation does not do

`log_invocation` does not configure sinks.

It does not create a `LogContext` for you.

It does not decide how payload values are normalized.

It does not replace event policy.

It does not swallow operation exceptions.

It does not automatically collect source file, source line, or source function information.

It does not log every argument or result by default.

It does not make private helper methods part of the public diagnostic vocabulary unless you explicitly decorate them.

Those responsibilities stay with the core logger infrastructure or with explicit decorator options.

## Typical public API pattern

A common class-level pattern is to decorate public API methods:

```python
from mvx.common.logger import LogContextProto, log_invocation


class LdapClient:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context

    @property
    def identity(self) -> str:
        return "ldap-client"

    @log_invocation(
        "bind",
        context_fields=("dn",),
    )
    async def bind(self, dn: str, password: str) -> None:
        ...
```

This logs the public `bind` operation lifecycle without logging the password.

The method chooses exactly which input fields are safe and useful as shared context for all emitted outcomes.

The context still controls event policy, payload normalization, sink delivery, and logging infrastructure error handling.

## Design summary

`log_invocation` is a user-facing logging component built on top of the core logger infrastructure.

It is intended primarily for public API methods: operations that have independent, atomic value from the user's point of view.

The decorated operation is the event.

`invoke`, `success`, `failed`, and `cancelled` are event outcomes.

The decorator records operation outcomes without changing the operation's return value, exception, or cancellation semantics.

It keeps argument logging, result logging, context data, closure data, and error-detail behavior explicit.


```{toctree}
:caption: What to read next
:maxdepth: 1

lifecycle
context_resolution
payload_fields
result_logging
error_and_cancellation
verbosity
context_formatter
examples/index
architecture
api
```
