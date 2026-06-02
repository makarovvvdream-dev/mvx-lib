# Context resolution

```{contents} Contents:
:depth: 1
:local:
```

## Overview

`log_invocation` does not create a logging context.

It needs an existing `LogContext`-compatible object and uses it to:

* check event policy;
* normalize selected values;
* build error payloads;
* emit `LogEvent` records;
* mark repeated errors as already logged.

The decorator can get this context in two ways:

```text
explicit ctx argument
first positional argument with get_log_context()
```

A method-based provider may also return `None`. This is an explicit per-call opt-out from decorator-driven logging.

When a context is available, the same setup step also resolves `entity_id` for `LogEventMeta`.

## Resolution order

When the decorated function is called, the wrapper prepares the logging frame before running the operation body.

The logging-enabled order is:

```text
extract bound function arguments
   |
   v
resolve logging context
   |
   v
resolve entity id
   |
   v
build LogEventMeta
   |
   v
check event policy
```

Context resolution happens before event policy is checked and before the `invoke` outcome can be emitted.

If `ctx` is not supplied and the first positional argument does not provide `LogContextProviderProto`, the wrapped operation body is not called.

If a method-based provider returns `None`, the logging-enabled setup stops there. The wrapped operation body is called normally, and the decorator emits no lifecycle events for that call.

## Explicit context

The simplest form for standalone functions is to pass `ctx` explicitly.

```python
from mvx.common.logger import LogContextProto, log_invocation


def make_loader(log_context: LogContextProto):
    @log_invocation("load_config", ctx=log_context)
    def load_config(path: str) -> dict[str, object]:
        return {"path": path}

    return load_config
```

When `ctx` is supplied, the decorator uses it directly.

It does not inspect the first positional argument for `get_log_context()`.

Use this form for functions that are not instance methods or when the context is known outside the decorated function.

## Method-based context

The common method-based form relies on the first positional argument.

For instance methods, the first positional argument is usually `self`.

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

Here the decorator resolves the context as:

```text
args[0] -> get_log_context() -> effective context
```

The object does not need to inherit from a special base class. It only needs to satisfy the context-provider protocol structurally.

`get_log_context()` may return either a context or `None`.

Returning a context enables normal decorator-driven lifecycle logging.

Returning `None` disables decorator-driven lifecycle logging for the current call. The operation still runs normally, but the decorator does not build metadata, does not check event policy, and does not emit lifecycle outcomes.

## Missing context

If `ctx` is not supplied and the decorated function receives no first positional argument that provides `LogContextProviderProto`, the decorator raises a runtime error.

For example, this is not enough:

```python
from mvx.common.logger import log_invocation


@log_invocation("load_config")
def load_config(path: str) -> dict[str, object]:
    return {"path": path}
```

There is no explicit `ctx`, and `path` is not a context provider.

Use an explicit context instead:

```python
@log_invocation("load_config", ctx=log_context)
def load_config(path: str) -> dict[str, object]:
    return {"path": path}
```

## Disabled method-based logging

A method-based context provider may return `None`.

This is not treated as a missing context. It is an explicit request to disable decorator-driven logging for the current call.

```python
from mvx.common.logger import LogContextProto, log_invocation


class Client:
    def __init__(self, log_context: LogContextProto | None) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context

    @log_invocation("connect")
    async def connect(self) -> None:
        ...
```

If `get_log_context()` returns a context, `connect()` is logged normally.

If `get_log_context()` returns `None`, `connect()` is executed normally, but the decorator emits no `invoke`, `success`, `failed`, or `cancelled` outcomes.

In the disabled case, the decorator also does not resolve `entity_id`, does not build `LogEventMeta`, and does not call event policy.


## Factory functions and closures

A useful pattern for standalone functions is a small factory that closes over the logging context.

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

Here `connect()` has no `self` and no context argument.

The context is supplied explicitly through `ctx=log_context`.

The `target` value is not a function argument either. It belongs to the outer function scope and is added to the `invoke` payload through `log_closures_on_invoke`.

## Entity id resolution

`entity_id` is optional metadata attached to `LogEventMeta`.

It identifies the runtime or domain entity involved in the decorated operation.

Examples:

```text
connection id
client id
request id
message id
worker id
```

The decorator resolves it in two ways:

```text
explicit entity_id_getter
first positional argument with identity property
```

## Explicit entity_id_getter

The explicit getter wins when supplied.

```python
from mvx.common.logger import LogContextProto, log_invocation


def make_connector(log_context: LogContextProto, connection_id: str):
    @log_invocation(
        "connect",
        ctx=log_context,
        entity_id_getter=lambda: connection_id,
    )
    async def connect() -> None:
        ...

    return connect
```

The getter takes no arguments.

It is called when the wrapper prepares the logging frame, before `LogEventMeta` is built.

If the getter raises, the wrapped operation body is not called, and the exception is not logged as a `failed` outcome of the decorated operation.

This is intentional. `entity_id_getter` is part of decorator integration, not part of the decorated operation body. A failure here usually means that the decorator was embedded incorrectly. It is better for such errors to surface immediately than to be silently swallowed or reported as an operation failure.

## Method-based entity id

For methods, the first positional argument may provide an `identity` property.

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

The decorator resolves:

```text
args[0] -> identity -> entity_id
```

This value is placed into `LogEventMeta.entity_id`.

It is metadata, not payload.

## No entity id

If neither `entity_id_getter` nor an `identity` provider is available, `entity_id` is `None`.

That is valid.

Not every event belongs to a stable runtime entity.

For example, a standalone configuration loader may not need entity metadata:

```python
@log_invocation("load_config", ctx=log_context)
def load_config(path: str) -> dict[str, object]:
    return {"path": path}
```

## Context namespace

The resolved context supplies the event namespace.

The decorator builds metadata with:

```python
event_namespace=effective_ctx.namespace
```

That means the namespace is not passed directly to `log_invocation`.

To change the namespace, use a different context.

For example, two clients using different contexts can emit the same event name into different namespaces:

```text
mvx.ldap.client      connect invoke
mvx.http.client      connect invoke
```

The event name is the same. The context namespace tells where the event belongs.

## Source fields

The decorator builds `LogEventMeta` with empty source fields:

```python
source_path=None
source_line=None
source_func=None
```

`log_invocation` does not automatically populate source file, source line, or source function information.

If source metadata is needed, it belongs to manual logging calls or another component that explicitly supplies those fields.

## Bound arguments

Before resolving fields for payloads, the decorator extracts effective function arguments.

It uses the decorated function signature:

```text
inspect.signature(func).bind(*args, **kwargs)
```

Then it applies defaults and stores the bound arguments as a dictionary.

If binding fails, the decorator falls back to a dictionary made from `kwargs`.

The resulting `effective_kwargs` dictionary is used later by:

```text
context_fields
log_kwargs_on_invoke
```

The bound arguments are captured once when the wrapper is called.

Field paths based on those arguments may still read current object attributes later, because `context_fields` are resolved separately for each emitted outcome.

## Public API method pattern

The usual class pattern is:

```python
from mvx.common.logger import LogContextProto, log_invocation


class Repository:
    def __init__(self, log_context: LogContextProto, repo_id: str) -> None:
        self._log_context = log_context
        self._repo_id = repo_id

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context

    @property
    def identity(self) -> str:
        return self._repo_id

    @log_invocation(
        "save",
        log_kwargs_on_invoke=("item_id",),
    )
    async def save(self, item_id: str, payload: bytes) -> None:
        ...
```

This method provides both:

```text
context       -> get_log_context()
entity_id     -> identity
```

The decorated operation name is `save`.

The effective namespace comes from the resolved context.

The effective entity id comes from `identity`.

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

## Resolution failure summary

The wrapped operation body is not called if the decorator fails during setup.

Setup can fail if:

```text
ctx is not supplied and the first positional argument does not provide LogContextProviderProto
entity_id_getter raises
get_log_context() raises
identity property access raises
```

Returning `None` from `get_log_context()` is not a setup failure.

These failures happen before `LogEventMeta` is fully prepared and before the `invoke` outcome can be emitted.

They are configuration or integration errors around the decorator, not failures of the decorated operation body.

This behavior is intentional. Setup failures usually indicate incorrect decorator integration: missing context provider, broken `get_log_context()`, broken `entity_id_getter`, or broken `identity` provider. Such errors should surface immediately instead of being swallowed or logged as ordinary operation failures.

## Design summary

`log_invocation` needs a context before it can log anything.

Use explicit `ctx` for standalone functions and closure-based factories.

Use `get_log_context()` on the first positional argument for public API methods.

Return `None` from `get_log_context()` to run a call without decorator-driven logging.

Use `entity_id_getter` or an `identity` property when the event should be attached to a stable runtime entity.

The context provides the namespace and logging pipeline. The entity id provides optional metadata about the object involved in the event.
