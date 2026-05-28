# Embedding in libraries

```{contents} Contents:
:depth: 1
:local:
```

## Overview

The logger can be embedded in reusable libraries without forcing the application to accept global logger configuration.

A library should usually keep two boundaries explicit:

```text
logging boundary
    how the library emits structured diagnostics

error boundary
    which exceptions are allowed to leave the public API
```

`log_invocation` is useful for the logging boundary.

`api_error_processor` is useful for the public error boundary.

Together they create a predictable public API surface:

```text
public API method
   |
   v
internal implementation may raise internal exceptions
   |
   v
api_error_processor maps unexpected errors to public errors
   |
   v
log_invocation records the public operation outcome
   |
   v
caller receives the declared public error shape
```

This pattern gives library users stable errors and gives maintainers structured logs from the same public boundary.

## Ownership rule

A reusable library should normally not own package-level logger state unless the application explicitly asks for that.

The application should own:

```text
sink configuration
context configuration
event policy
payload processor
logger reset and shutdown behavior
```

The library should own:

```text
public operation names
safe payload field choices
public error normalization rules
```

This separation keeps the library observable without letting it take over the application's logging environment.

## Accept a context-like object

The preferred library pattern is to accept a `LogContextProto`-compatible object.

```python
from mvx.common.logger import LogContextProto


class Client:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto:
        return self._log_context
```

The library does not need to know whether the context is package-managed or directly created.

It only needs the behavior required by `LogContextProto`.

This keeps the concrete ownership model in the application.

## Use log_invocation on public API methods

A library can use `log_invocation` on public API methods.

```python
from mvx.common.logger import LogContextProto, log_invocation


class Client:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto:
        return self._log_context

    @log_invocation("connect")
    async def connect(self) -> None:
        ...
```

The decorator resolves the logging context through `get_log_context()`.

The application still controls:

```text
sink delivery
event policy
payload processor
logging infrastructure error handling
context namespace
```

The library controls the operation name and selected payload fields.

## Combine log_invocation with api_error_processor

For public API methods, `log_invocation` is often best used outside `api_error_processor`.

The two decorators solve different problems.

```text
api_error_processor
    normalizes exceptions at the public API boundary

log_invocation
    records the operation lifecycle and error outcome
```

Use `api_error_processor` closest to the method body and `log_invocation` outside it:

```python
from mvx.common.errors import RuntimeExtendedError, RuntimeUnexpectedError
from mvx.common.helpers import api_error_processor
from mvx.common.logger import LogContextProto, log_invocation


class ClientError(RuntimeExtendedError):
    pass


class ClientUnexpectedError(ClientError, RuntimeUnexpectedError):
    pass


class ClientInputError(ClientError):
    pass


public_api = api_error_processor(
    passthrough_error_types=(ClientError,),
    raise_error_type=ClientUnexpectedError,
)


class Client:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto:
        return self._log_context

    @log_invocation("connect")
    @public_api
    async def connect(self) -> None:
        ...
```

Decorator order matters.

Python applies the inner decorator first, so this is conceptually:

```text
connect = log_invocation("connect")(public_api(connect))
```

With this order, unexpected internal exceptions are normalized before they reach `log_invocation`.

That means `log_invocation` logs the same public error shape that the caller receives.

```text
internal exception
   |
   v
api_error_processor
   |
   v
ClientUnexpectedError
   |
   v
log_invocation failed outcome
   |
   v
same ClientUnexpectedError re-raised to caller
```

Declared public errors still pass through unchanged.

`asyncio.CancelledError` also passes through unchanged because cancellation is control flow, not a public API failure.

## Why this order is useful

A public API method should not leak accidental implementation errors such as:

```text
AssertionError
KeyError
TypeError
ValueError
ZeroDivisionError
```

Those errors may be useful inside the implementation, but they are usually poor public API contracts.

`api_error_processor` maps unexpected internal errors to a configured public unexpected-error type.

`log_invocation` then logs the failed operation with that normalized public error.

This gives one clean boundary:

```text
public logs show public errors
callers receive public errors
internal exceptions remain causes
```

The original exception is still preserved as the cause, so diagnostic information is not lost.

## When not to combine them

Do not add `api_error_processor` when the method is not a public API boundary.

Internal helper methods should usually raise natural internal exceptions and let the public boundary normalize them.

Also do not reverse the decorator order unless you intentionally want `log_invocation` to observe raw internal exceptions before API error normalization.

Reversed order:

```python
@public_api
@log_invocation("connect")
async def connect(self) -> None:
    ...
```

This makes `log_invocation` closer to the method body, so it may log raw internal exceptions before they are mapped into the public error type.

That is usually not what you want at a library boundary.

## Public API vocabulary

A library should log public operations, not internal helper calls.

Good decorated methods:

```text
connect
close
send_request
read_message
write_message
start
stop
```

Poor decorated methods:

```text
_parse_one_header
_increment_counter
_build_temp_state
_resolve_internal_flag
```

The log should help the library user understand behavior from the outside.

Internal details can still be logged manually when they are diagnostically meaningful, but they should not become the main operation vocabulary.

## Stable namespaces

A library should define stable namespaces for its components.

For example:

```text
my_lib.client
my_lib.transport
my_lib.protocol
my_lib.cache
```

The namespace should be stable enough for event policies, dashboards, tests, and diagnostics.

Avoid temporary or implementation-specific namespaces:

```text
my_lib.tmp
my_lib.new_impl
my_lib.step2
```

If the application provides a context, it may choose the namespace through that context.

If the library creates a local context internally, it should use a documented namespace.

## Avoid hidden global logger setup

A reusable library should normally not do these things implicitly:

```text
configure package-level sinks
reset package-level logger state
close sinks it did not create
change application-wide context configuration
install global logging handlers unexpectedly
```

These actions affect process-wide logger behavior.

They are appropriate in application setup code, not hidden inside a reusable component constructor.

## Do not reset application logger state

A library should not call:

```python
reset_logger()
```

or:

```python
reset_log_contexts()
```

from normal library code.

These functions belong to application-level lifecycle management and tests that intentionally reset global state.

Calling them inside a reusable library can destroy contexts and sinks configured by the application.

## Do not close sinks you do not own

A library should not close package-managed sinks that it did not configure explicitly.

For example, avoid this inside normal library code:

```python
close_log_sink("stderr")
```

The application may still be using that sink elsewhere.

If the library creates and owns a direct sink, then the library may close that sink as part of its own lifecycle.

Ownership should be explicit.

## Direct context owned by the library

Sometimes a library may create its own direct context.

This is reasonable when the context is local and does not modify package-level state.

```python
from mvx.common.logger import LogContext, LogPayloadProcessor


class LocalComponent:
    def __init__(self, sink) -> None:
        self._log_context = LogContext(
            namespace="my_lib.local_component",
            log_sink=sink,
            payload_processor=LogPayloadProcessor(),
        )
```

The library owns this context object.

If it also owns the sink, it owns the sink lifecycle too.

This pattern is useful for embedded systems, tests, or components that need isolated diagnostics.

## Application-owned context

The preferred library pattern is often application-owned context injection.

Application setup:

```python
sink = configure_log_sink(
    name="app_stderr",
    sink_cls=StreamLogSink,
)

ctx = configure_log_context(
    "my_app.client",
    log_sink=sink,
)

client = Client(log_context=ctx)
```

Library code:

```python
class Client:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto:
        return self._log_context
```

The application controls configuration.

The library emits through the provided context.

## Optional logging

A library may want logging to be optional.

There are two clean ways to do that.

First, require the application to pass a context when logging is desired.

Second, provide an explicit quiet or test context in application code.

Avoid hidden global setup just to make logging optional.

A constructor that silently configures package-level sinks can surprise applications.

Explicit context injection is clearer.

## Payload design inside libraries

Library events should expose useful diagnostic fields without leaking sensitive data.

Good payload fields:

```text
request_id
connection_id
operation state
protocol version
message size
retry count
backend name
```

Risky payload fields:

```text
password
token
raw request body
private key
full protocol frame
large binary payload
```

Use `context_fields`, `log_kwargs_on_invoke`, `log_result_on_success`, and `LogPayloadProvider` deliberately.

The library should decide what is safe to expose at its public API boundary.

## Error behavior

For public API methods, prefer a clear error boundary.

`api_error_processor` defines which errors are part of the public API and which internal failures are mapped to a public unexpected-error type.

`log_invocation` records the resulting operation outcome.

With the recommended decorator order, logger output and caller-visible errors describe the same public boundary.

Logging infrastructure failures remain separate. If the logger cannot deliver an event, `LogContext` applies its effective `LogErrorHandlingPolicy`.

That policy is controlled by the context owner.

## Testing library logging

A library can test logging behavior with a direct in-memory context.

```python
from threading import RLock

from mvx.common.logger import LogContext, LogEvent, LogPayloadProcessor


class ListSink:
    def __init__(self) -> None:
        self._lock = RLock()
        self.events: list[LogEvent] = []

    def log(self, event: LogEvent) -> None:
        with self._lock:
            self.events.append(event)


sink = ListSink()
ctx = LogContext(
    namespace="test.client",
    log_sink=sink,
    payload_processor=LogPayloadProcessor(),
)

client = Client(log_context=ctx)
```

Tests can assert against emitted `LogEvent` objects without touching package-level registries.

This keeps tests isolated and avoids global logger state leaks between test cases.

When the public method also uses `api_error_processor`, test both boundaries:

```text
unexpected internal exception is mapped to public unexpected error
failed outcome logs the mapped public error
original internal exception is preserved as cause
```

## Recommended pattern

For reusable libraries, prefer this pattern:

```text
accept LogContextProto-compatible object
expose get_log_context() on objects that use log_invocation
apply log_invocation to public API methods
apply api_error_processor inside log_invocation at public boundaries
keep payload fields safe and stable
do not configure package-level sinks implicitly
do not reset global logger state
do not close sinks you do not own
```

This keeps the library observable without making it own the application's logging environment.

It also keeps the public error surface predictable.

## Design summary

Embedding the logger in a library is mostly an ownership and boundary question.

The library should own its public operation vocabulary, safe payload choices, and public error normalization rules.

The application should own logger configuration, sink lifecycle, policies, and global reset behavior.

`LogContextProto`, `log_invocation`, and `api_error_processor` make that separation possible: the library can emit structured logs and expose stable public errors without requiring a concrete global logger setup.
