# Architecture

```{contents} Contents:
:depth: 2
:local:
```

## Overview

`log_invocation` is implemented as a small operation-logging runtime around a decorated callable.

It is not a sink, not a context, not an event policy, and not a payload processor.

Its job is narrower:

```text
wrap a public API operation
   |
   v
observe its lifecycle
   |
   v
build operation-specific LogEvent records
   |
   v
emit them through the resolved LogContext
```

Internally, the component has three main phases:

```text
decoration time
    validate static configuration and build the wrapper

call time
    resolve arguments and context; when logging is enabled, resolve entity id and metadata

outcome time
    build outcome payloads and emit LogEvent records
```

This article describes how those phases are organized inside the component.

## High-level structure

The public entry point is the decorator factory:

```python
log_invocation(...)
```

It returns a decorator:

```text
log_invocation(...) -> decorate(func)
```

The decorator returns a wrapper around the original callable:

```text
decorate(func) -> wrapped_async or wrapped_sync
```

The full shape is:

```text
log_invocation(...)
   |
   v
decorate(func)
   |
   +-- validate event name
   |
   +-- capture inspect.Signature
   |
   +-- choose async wrapper if func is coroutine function
   |
   +-- choose sync wrapper otherwise
   |
   v
wrapped callable
```

The wrapper is the runtime part. It resolves the logging context, calls the original function, and preserves the original return/exception/cancellation semantics. When decorator-driven logging is enabled for the call, it also builds metadata and emits lifecycle outcomes.

## Static configuration captured by the decorator

The outer `log_invocation(...)` call captures configuration that is reused by every call to the decorated function.

Important captured values include:

```text
event
invoke_level
success_level
error_level
error_level_suppressed
cancel_level
log_closures_on_invoke
context_fields
context_formatter
log_kwargs_on_invoke
log_result_on_success
log_error_policy
ctx
entity_id_getter
```

These values are not recomputed for every emitted outcome.

They define the logging behavior of the decorated operation.

For example:

```python
@log_invocation(
    "send_request",
    log_kwargs_on_invoke=("request_id",),
    log_result_on_success=("status",),
)
def send_request(...):
    ...
```

At decoration time, the component captures:

```text
event name: send_request
invoke kwargs specs: request_id
result specs: status
```

At call time, it applies those captured specs to the actual call arguments and actual result.

## Event name validation

During decoration, the event name is validated with a regular expression.

The event name must match:

```text
^[A-Za-z_.]+$
```

If the event name is invalid, decoration fails immediately.

This means invalid event names are detected when the function is decorated, not later when the operation is called.

This is an integration-time failure, not an operation failure.

## Signature capture

During decoration, the component captures the function signature:

```python
sig = inspect.signature(func)
```

The signature is later used at call time to build `effective_kwargs` from `args` and `kwargs`.

This allows field specs to refer to arguments by their parameter names.

For example:

```python
@log_invocation(
    "send_request",
    log_kwargs_on_invoke=("request_id",),
)
def send_request(self, request_id: str) -> None:
    ...
```

The runtime call may be positional:

```python
client.send_request("req-1")
```

but the decorator can still expose the argument as:

```text
request_id
```

because it bound the call through the function signature.

## Wrapper selection

The decorator builds one of two wrapper shapes.

If the decorated function is an `async def`, it builds an async wrapper.

```text
inspect.iscoroutinefunction(func) == True
   |
   v
wrapped_async
```

Otherwise, it builds a sync wrapper.

```text
inspect.iscoroutinefunction(func) == False
   |
   v
wrapped_sync
```

The sync wrapper has an additional branch for functions that return an awaitable.

So the runtime supports three execution shapes:

```text
async function
synchronous function
synchronous function returning an awaitable
```

## Runtime setup flow

Both wrappers start with the same setup flow.

```text
extract effective kwargs
   |
   v
resolve context
   |
   +--> context is None
   |       |
   |       v
   |   call original operation without decorator-driven logging
   |
   v
resolve entity id
   |
   v
build LogEventMeta
   |
   v
check event policy
   |
   v
optionally emit invoke
```

If setup fails, the operation body is not called.

Returning None from get_log_context() is not a setup failure. It disables decorator-driven logging for the current call, and the original operation is still called.

That is intentional. Setup failures indicate incorrect integration of the decorator, not failure of the decorated operation itself.

## Argument extraction

At call time, the wrapper extracts bound arguments.

Conceptually:

```text
func signature + args + kwargs -> effective_kwargs
```

The implementation tries to bind the call using the captured signature and then applies defaults.

If binding fails, it falls back to a dictionary made from `kwargs`.

The resulting dictionary is used by:

```text
context_fields
log_kwargs_on_invoke
```

`log_result_on_success` does not use `effective_kwargs`. It works from the actual returned result.

## Context resolution

After arguments are extracted, the wrapper resolves the effective logging context.

There are two paths.

If `ctx` was supplied to the decorator, that context is used directly:

```text
ctx argument -> effective_ctx
```

Otherwise, the wrapper looks at the first positional argument:

```text
args[0] -> get_log_context() -> effective_ctx
```

This is the method-based path, where `args[0]` is normally `self`.

If `ctx` is not supplied and the first positional argument does not provide `LogContextProviderProto`, setup fails.

If the method-based provider returns `None`, setup stops without failure and the original operation is called without decorator-driven logging.

The context is required because the decorator delegates important work to it:

```text
event policy check
value normalization
error payload building
error marker helpers
LogEvent delivery
```

`log_invocation` does not create or configure a context.

## Entity id resolution

After the context is resolved, the wrapper resolves the optional entity id.

There are two paths.

If `entity_id_getter` was supplied, it is called:

```text
entity_id_getter() -> entity_id
```

Otherwise, the wrapper looks at the first positional argument:

```text
args[0].identity -> entity_id
```

If neither path is available, the entity id is `None`.

If `entity_id_getter` or `identity` access raises, setup fails and the operation body is not called.

This is intentional. Entity id resolution is part of decorator integration. A broken entity provider should surface immediately instead of being logged as an operation failure.

## Metadata construction

Once context and entity id are available, the wrapper builds `LogEventMeta`.

The metadata is built once per decorated call:

```python
LogEventMeta(
    event_namespace=effective_ctx.namespace,
    event_name=event,
    entity_id=entity_id,
    source_path=None,
    source_line=None,
    source_func=None,
)
```

The event namespace comes from the resolved context.

The event name comes from the decorator argument.

The entity id comes from the entity resolution step.

Source fields are intentionally set to `None`. `log_invocation` does not collect source file, source line, or source function metadata.

## Event policy check

After metadata is created, the wrapper asks the context whether the event is enabled.

```python
event_enabled = effective_ctx.is_event_enabled(event_meta)
```

This flag is used for normal tracing outcomes:

```text
invoke
success
```

If `event_enabled` is false, `invoke` is not emitted and `success` is not emitted.

Failure and cancellation branches still emit their dedicated outcomes when the operation body raises.

This makes the policy boundary explicit:

```text
event policy controls normal operation tracing
failure/cancellation paths still report exceptional outcomes
```

## Outcome emitters

The component has four internal outcome emitters:

```text
_emit_invoke
_emit_success
_emit_failed
_emit_cancelled
```

Each emitter builds a payload for one outcome and emits a `LogEvent` through the resolved context.

They share the same metadata object created during setup.

They differ in payload sources and level selection.

```text
_emit_invoke
    closures
    context fields
    invoke kwargs
    invoke_level

_emit_success
    context fields
    result payload if enabled
    success_level

_emit_failed
    context fields
    error payload or suppressed error
    error_level or error_level_suppressed

_emit_cancelled
    cancelled flag
    error payload
    context fields
    cancel_level
```

The outcome emitters are the main internal boundary between lifecycle observation and `LogEvent` construction.

## LogEvent construction

Each outcome emitter creates a `LogEvent`.

The shape is:

```python
LogEvent(
    level=...,
    meta=event_meta,
    event_outcome=..., 
    timestamp=time.time(),
    payload=payload,
)
```

Then it emits the event through the context:

```python
effective_ctx.emit_log_event(log_event)
```

This is important: the decorator does not call the sink directly.

It always goes through `LogContext`.

That keeps sink delivery, logging infrastructure error handling, and downstream behavior owned by the core logger infrastructure.

## Invoke emitter

`_emit_invoke` builds the `invoke` payload before the operation body runs.

It can add three groups of data:

```text
closures
context fields
selected invoke kwargs
```

The order is:

```text
start with empty payload
   |
   v
add normalized closure values under closures
   |
   v
inject context payload
   |
   v
add selected kwargs under kwargs
   |
   v
emit LogEvent(event_outcome="invoke")
```

Closure values are normalized one by one through the context.

If closure value normalization fails, the value for that closure key becomes `"<unknown>"`.

Selected invoke kwargs are resolved from `effective_kwargs` and normalized through the context.

## Success emitter

`_emit_success` builds the `success` payload after the operation completes successfully.

It can add:

```text
context fields
result payload
```

The order is:

```text
start with empty payload
   |
   v
inject context payload
   |
   v
if result logging is enabled, add payload["result"]
   |
   v
emit LogEvent(event_outcome="success")
```

If `log_result_on_success` is `None`, result logging is skipped.

If it is an empty tuple, the whole result is normalized.

If it contains field specs, the result payload builder tries to extract selected result values.

## Failed emitter

`_emit_failed` builds the `failed` payload when the operation raises an ordinary exception.

It first injects context payload.

Then it applies error logging logic.

The order is:

```text
start with empty payload
   |
   v
inject context payload
   |
   v
if log_error_policy has matching rule:
       apply rule
   else:
       use repeated-error marker behavior
   |
   v
emit LogEvent(event_outcome="failed")
```

If full error logging is used, the payload receives:

```text
payload["error"] = effective_ctx.build_error_payload(err)
```

If suppressed logging is used, no detailed `error` payload is added.

The original exception is re-raised by the wrapper after `_emit_failed` returns.

## Cancelled emitter

`_emit_cancelled` handles `asyncio.CancelledError`.

It first checks whether the same cancellation exception instance is already marked as logged.

If it is already logged, the emitter returns without emitting another cancelled outcome.

Otherwise, it builds a payload with:

```python
{
    "cancelled": True,
    "error": effective_ctx.build_error_payload(err),
}
```

Then it injects context payload and emits:

```text
event_outcome = "cancelled"
```

After successful emission, it marks the cancellation exception as logged.

The wrapper then re-raises the same `CancelledError`.

## Context payload injection

Context payload injection is handled by a shared helper.

Conceptually:

```text
field specs + effective kwargs
   |
   v
resolve raw fields
   |
   v
optional formatter
   |
   v
inject into target payload
```

This helper is used by all four outcome emitters.

It is why `context_fields` behave consistently across `invoke`, `success`, `failed`, and `cancelled` outcomes.

## Field resolution pipeline

Field resolution turns field spec strings into resolved raw values.

For each field spec, the resolver performs these steps:

```text
strip whitespace
   |
   v
apply verbosity gate
   |
   v
parse ! marker
   |
   v
parse alias
   |
   v
split path by dots
   |
   v
find first segment in effective kwargs
   |
   v
walk remaining path with getattr() or len()
   |
   v
return alias, value, unbounded flag
```

Failures are skipped.

Examples of skipped fields:

```text
empty spec
verbosity gate does not match
missing top-level argument
missing attribute
attribute access raises
len() raises
```

A failed field does not fail the decorated operation.

This is a diagnostic extraction failure, not an operation failure.

## Verbosity gate pipeline

Verbosity filtering is applied before the rest of field parsing.

A field spec without `:` is used as-is.

A field spec with `:` is split into:

```text
left side   -> allowed verbosity names
right side  -> effective field spec
```

The resolver asks the context for plain verbosity level.

If the level is not available or does not match the left side, the field is skipped.

If the left side is empty, the right side is treated as an ungated spec.

The decorator does not validate verbosity names against a fixed enum at this stage. It compares strings.

## Context formatter pipeline

After raw context fields are resolved, the context formatter may run.

The formatter receives:

```text
ctx
event_outcome
event name
raw resolved fields
```

It is called whenever `context_formatter` is provided, even if no fields were resolved.

If the formatter returns a dictionary, that dictionary is injected into the target payload.

If the formatter raises or returns a non-dictionary value, the helper falls back to normal context field handling for resolved fields.

Formatter output is also checked for system-key collisions.

System keys are:

```text
error
kwargs
result
cancelled
closures
```

If formatter output contains any system key, it is placed under `payload["context"]` instead of being merged into the top-level payload.

This prevents formatter output from overwriting lifecycle-owned payload sections.

## Default context field injection

If no formatter output is used, resolved context fields are normalized one by one.

For each resolved field:

```text
ctx.normalize_value_for_log(value, unbounded=field.unbounded_items)
```

The normalized values are injected into the target payload.

If the normalized payload contains no fields, nothing is added.

This is the simple path used by ordinary `context_fields`.

## Invoke kwargs pipeline

Invoke kwargs use the same field resolver as context fields.

The difference is the target location and lifecycle timing.

```text
log_kwargs_on_invoke
   |
   v
resolve fields from effective kwargs
   |
   v
normalize selected values
   |
   v
payload["kwargs"] = normalized mapping
```

This pipeline runs only in `_emit_invoke`.

The selected kwargs are not repeated on `success`, `failed`, or `cancelled` outcomes.

## Result payload pipeline

Result logging uses a separate helper because it works from the returned result object instead of `effective_kwargs`.

The result helper branches by result type.

```text
primitive
    normalize whole result

list / tuple
    select items by index if specs are provided
    otherwise normalize whole result

dict
    normalize whole dict

composite object
    select attributes if specs are provided
    otherwise normalize whole object
```

For list and tuple results, the first path segment must be an integer index.

For composite objects, path segments are resolved with attribute access.

For dictionaries, field specs are ignored and the whole dictionary is normalized.

If selected fields cannot be resolved for a list, tuple, or composite object, the helper falls back to whole-result normalization.

## Error policy pipeline

The failed emitter applies error policy before the default repeated-error behavior.

The pipeline is:

```text
if log_error_policy is not empty:
    for each (exception_type, force_log):
        if isinstance(err, exception_type):
            apply this rule
            stop

if no rule matched:
    use repeated-error marker behavior
```

If `force_log` is true:

```text
add full error payload
emit with error_level
mark error as logged
```

If `force_log` is false:

```text
emit without detailed error payload
emit with error_level_suppressed
mark error as logged
```

If no policy rule applies, the emitter checks whether the exception instance was already logged.

If not logged, it emits a full failed outcome and marks the exception.

If already logged, it emits a suppressed failed outcome.

## Wrapper control flow

The flows below describe calls where decorator-driven logging is enabled.

The async wrapper has this control flow:

```text
setup
   |
   v
if enabled: emit invoke
   |
   v
try:
    result = await func(...)
except CancelledError:
    emit cancelled
    raise
except Exception:
    emit failed
    raise
else:
    if enabled: emit success
    return result
```

The sync wrapper has a similar shape, but first calls the function directly.

If the direct result is awaitable, the wrapper returns a new awaitable that observes the final awaited outcome.

```text
setup
   |
   v
if enabled: emit invoke
   |
   v
try:
    result = func(...)
except CancelledError:
    emit cancelled
    raise
except Exception:
    emit failed
    raise

if result is awaitable:
    return await-and-log wrapper

if enabled: emit success
return result
```

The await-and-log wrapper then handles success, failure, and cancellation around the original awaitable.

## What the component owns

`log_invocation` owns the operation observation logic.

It owns:

```text
decorator factory
function wrapping
setup ordering
context and entity id resolution
metadata construction
outcome selection
payload assembly
result extraction
error/cancellation observation
LogEvent construction for operation outcomes
```

It does not own:

```text
context hierarchy
sink configuration
sink delivery implementation
event policy implementation
payload processor implementation
logger package bootstrap
logging infrastructure error policy
```

That separation is why the component belongs under `log_components`.

It builds on the logger infrastructure instead of becoming part of the infrastructure itself.

## Internal design summary

At decoration time, `log_invocation` validates and captures static configuration.

At call time, the wrapper resolves bound arguments and the effective context. When decorator-driven logging is enabled for the call, it also resolves entity id, metadata, and the event-policy decision.

At outcome time, internal emitters build payloads, construct `LogEvent` records, and emit them through the resolved context.

The component is deliberately organized around operation outcomes:

```text
invoke
success
failed
cancelled
```

Each outcome has its own emitter, payload sources, level, and control-flow position.

The result is a focused operation-logging runtime layered above `LogContext`, not a replacement for the core logger pipeline.
