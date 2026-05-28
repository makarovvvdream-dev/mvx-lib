# Package bootstrap

```{contents} Contents:
:depth: 1
:local:
```

## Overview

`MVX Logger` is usable immediately after import.

The package creates a default logging setup during module bootstrap:

```text
import mvx.common.logger
        |
        v
create sink registry
        |
        v
register default stderr sink
        |
        v
create root LogContext
        |
        v
create context registry
```

This gives the package a ready root context and a ready default sink without requiring application code to configure anything first.

At the same time, the default setup is not a closed box. Applications can register additional sinks, configure namespace contexts, reset non-root contexts, or reset the whole package-level logger state.

## Why it exists

The logger needs to work in two different modes.

First, it should be immediately useful:

```python
from mvx.common.logger import get_root_log_context

ctx = get_root_log_context()
ctx.log_info_event(event="app.started", payload={})
```

Second, it should remain configurable for larger applications:

```python
configure_log_sink(name="file", sink_cls=FileLogSink, ...)
configure_log_context("my_app.db", log_sink=file_sink)
```

Bootstrap provides the first mode. Registries and public configuration functions provide the second.

The design avoids forcing every consumer to write setup code before the first event, while still keeping explicit configuration available.

## Bootstrap sequence

Package bootstrap is implemented by `_bootstrap()`.

It creates two registries:

```text
_LogSinkRegistry
_LogContextRegistry
```

The sequence is:

```text
1. Create an empty sink registry.
2. Register the default root sink under the name "stderr".
3. Create the root LogContext.
4. Create the context registry with that root context.
5. Store both registries as package-level state.
```

In code shape:

```python
log_sink_registry = _LogSinkRegistry()

log_sink = log_sink_registry.register(
    name=DEFAULT_ROOT_LOG_SINK_NAME,
    sink_cls=StreamLogSink,
)

root_ctx = LogContext(
    namespace=ROOT_LOG_CONTEXT_NAMESPACE,
    log_sink=log_sink,
    payload_processor=LogPayloadProcessor(),
)

log_context_registry = _LogContextRegistry(root_ctx)
```

If bootstrap fails, the package writes a last-resort internal error to `stderr` and re-raises the exception.

## Default constants

The default root namespace is the empty string:

```python
ROOT_LOG_CONTEXT_NAMESPACE = ""
```

The default sink name is:

```python
DEFAULT_STDERR_LOG_SINK_NAME = "stderr"
DEFAULT_ROOT_LOG_SINK_NAME = DEFAULT_STDERR_LOG_SINK_NAME
```

So the default root context is backed by a package-registered sink named `"stderr"`.

The empty root namespace is internal to the context tree. Public namespace validation for user-created contexts requires a non-empty dotted namespace.

## Default root context

The default root context is created with:

```text
namespace          -> ""
log_sink           -> default StreamLogSink registered as "stderr"
payload_processor  -> LogPayloadProcessor()
parent             -> None
```

The root context is the mandatory fallback for the package-level context tree.

It must always have a sink and a payload processor.

Child contexts may inherit these components from the root unless they define local overrides.

## Default sink

The default sink is created through the same registry mechanism as any other package-managed sink.

Bootstrap calls:

```python
log_sink_registry.register(
    name="stderr",
    sink_cls=StreamLogSink,
)
```

This means the default sink is not a special case after creation. It is stored in the sink registry together with its descriptor and terminator.

It can be retrieved by name:

```python
sink = get_log_sink("stderr")
```

It is also protected by the same in-use rules as other registered sinks.

Because the root context locally owns the default sink, `close_log_sink("stderr")` cannot close it while it is assigned to the root context.

## Sink registry

The sink registry stores package-managed sinks by name.

Each registered sink entry contains:

```text
sink
terminator
descriptor
```

The registry uses two locks internally:

```text
lifecycle lock
registry lock
```

The lifecycle lock protects create/reset/unregister flows.

The registry lock protects access to the dictionary of registered sinks.

The registry supports:

```text
register
get
get_sinks_names
is_empty
unregister
reset
```

Public functions expose only the package-level API, not the registry object itself.

## Sink registration identity

`configure_log_sink()` delegates to the sink registry.

The registry first asks the sink class to build a descriptor:

```python
descriptor = sink_cls.build_descriptor(**sink_kwargs)
```

Then it checks whether the name is already registered.

The behavior is:

```text
name not registered
    -> create and store a new sink

name registered with the same descriptor
    -> return the existing sink

name registered with a different descriptor
    -> raise LogSinkConfigurationConflictError
```

This makes sink configuration idempotent when the request is identical and explicit when a name conflict exists.

The registry does not silently replace existing sinks.

## Context registry

The context registry stores package-managed `LogContext` instances by namespace.

It is initialized with the root context:

```python
self._contexts = {ROOT_LOG_CONTEXT_NAMESPACE: root_log_context}
```

The registry supports:

```text
get_root_log_context
get
put
contains
list_namespaces
clear
get_contexts_by_log_sink
create_log_context_chain
```

Like the sink registry, it is internal. Public functions expose package-level access.

## Context namespace chain creation

When `configure_log_context()` receives a namespace that does not exist, the context registry creates the missing namespace chain.

For example:

```python
configure_log_context("my_app.db.pool")
```

creates:

```text
<root>
   |
   v
my_app
   |
   v
my_app.db
   |
   v
my_app.db.pool
```

Only the leaf context receives the explicit components passed to `configure_log_context()`.

Intermediate contexts are created with no local sink, event policy, payload processor, or error handling policy.

That means they inherit runtime infrastructure from their parent and have no local event policy.

## Updating existing contexts

`configure_log_context()` is an upsert operation.

If the namespace already exists, the function updates only explicitly supplied components:

```python
configure_log_context(
    "my_app.db",
    event_policy=new_policy,
)
```

This changes the local event policy for `my_app.db` and leaves the existing local sink, payload processor, and error handling policy unchanged.

Passing `None` does not reset a component through `configure_log_context()`.

To remove a local component, use the context reset method directly:

```python
ctx.reset_event_policy()
ctx.reset_log_sink()
ctx.reset_payload_processor()
ctx.reset_log_error_handling_policy()
```

## Wiring lock

Package-level sink and context operations are coordinated by a single wiring lock:

```python
_log_context_wiring_lock = threading.RLock()
```

Public functions that read or change package-level registries use this lock.

This keeps operations such as sink registration, context configuration, sink closing, context reset, and full logger reset from interleaving in unsafe ways.

The individual registries also have their own locks. The package-level wiring lock coordinates operations that involve both registries.

## Public facade

The package exposes a small facade over the registries.

Sink functions:

```python
configure_log_sink(...)
get_log_sink(name)
get_configured_log_sink_names()
has_configured_log_sinks()
close_log_sink(name)
```

Context functions:

```python
get_root_log_context()
get_log_context(namespace)
configure_log_context(namespace, ...)
get_log_context_namespaces()
has_log_context(namespace)
reset_log_contexts()
```

Full reset:

```python
reset_logger()
```

These functions are the package-level configuration layer. They are convenient, but they are not the only way to use the logger. Direct `LogContext` construction remains possible.

## Name validation

Package-level sink names and context namespaces are validated before registry access.

Sink names must match:

```text
^[A-Za-z][A-Za-z0-9_-]*$
```

Context namespaces must match:

```text
^[A-Za-z][A-Za-z0-9_-]*(?:\.[A-Za-z][A-Za-z0-9_-]*)*$
```

This gives registered objects stable, predictable names.

The root context namespace is an internal exception. It is `""`, but user-supplied namespaces passed to public namespace functions must be non-empty valid dotted names.

## Closing sinks

`close_log_sink(name)` removes a package-managed sink and calls its terminator.

If the sink name is not registered, it returns `False`.

If the sink is registered, the function first checks whether any registered context has that sink as its local sink.

If such contexts exist, it raises `LogSinkIsInUseError`.

This check protects package-managed contexts from retaining local references to closed sinks.

If the sink is not locally assigned to any registered context, the registry unregisters it and calls its terminator.

On successful close, the function returns `True`.

## Resetting contexts

`reset_log_contexts()` clears all package-managed non-root contexts.

After it runs, the context registry contains only the root context:

```text
<root>
```

It does not reset the sink registry.

It does not close sinks.

It does not affect `LogContext` instances created manually outside the package-level registry.

This operation is useful when application code wants to discard namespace configuration while keeping registered sinks available.

## Resetting the whole logger

`reset_logger()` resets package-level logger state.

It runs under the wiring lock and performs two steps:

```text
1. Reset the sink registry.
2. Run bootstrap again.
```

In code shape:

```python
_log_sink_registry.reset()
_log_sink_registry, _log_context_registry = _bootstrap()
```

Resetting the sink registry clears registered sinks and calls their terminators in reverse registration order.

Then bootstrap creates a fresh sink registry, a fresh default `"stderr"` sink, a fresh root context, and a fresh context registry.

If closing any previous sink fails, `LogSinkCloseError` is raised and bootstrap is not completed.

## Last-resort internal errors

Bootstrap has a minimal fallback error path.

If the default logger setup fails, `_bootstrap()` calls:

```python
_log_internal_error("logger bootstrap failed", exc)
```

This helper writes directly to `stderr` using `print()`.

It does not depend on the logger itself because the logger may not exist yet.

The same principle appears in other internal failure paths where the logging infrastructure cannot rely on itself to report its own failure.

## Direct usage and bootstrap

Bootstrap affects package-level registries only.

It does not prevent direct usage:

```python
ctx = LogContext(
    namespace="local.component",
    log_sink=sink,
    payload_processor=LogPayloadProcessor(),
)
```

Such a context is outside the package-level context registry unless it is created through `configure_log_context()`.

It is not listed by `get_log_context_namespaces()`.

It is not removed by `reset_log_contexts()`.

Its sink cleanup is owned by the code that created the sink, unless that sink is separately package-managed.

## Design summary

Package bootstrap gives the logger an immediately usable default setup.

The default setup is:

```text
registered sink "stderr" -> StreamLogSink
root context ""          -> default sink + LogPayloadProcessor()
```

Package-level registries provide named sink and context management.

The wiring lock coordinates operations that involve registry state.

`reset_log_contexts()` clears namespace configuration but keeps sinks.

`reset_logger()` closes registered sinks and recreates the default root setup.

Direct component usage remains possible outside the package-level bootstrap machinery.
