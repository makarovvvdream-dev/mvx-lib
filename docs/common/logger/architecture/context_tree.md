# Context tree

```{contents} Contents:
:depth: 1
:local:
```

## Overview

`LogContext` is the coordinator of the logging pipeline.

It is not just a name holder and it is not only a facade over a sink. A context decides how a logging call moves from raw input to a delivered `LogEvent`:

```text
LogContext.log_event(...)
   |
   v
create LogEventMeta
   |
   v
check local event policy
   |
   v
resolve payload processor
   |
   v
normalize payload
   |
   v
resolve sink
   |
   v
deliver LogEvent
```

Contexts may be organized into a namespace tree. The tree is used to share common logging infrastructure while allowing selected namespaces to override local behavior.

## Why it exists

A project usually needs one common logging setup, but not one identical behavior for every subsystem.

For example:

* most namespaces can write to the same sink;
* one namespace can use a stricter event policy;
* another namespace can use a different payload processor;
* a diagnostic namespace can use another sink;
* a library can use a local context without owning application-wide logger configuration.

The context tree solves this without forcing every context to duplicate the full setup.

A child context can define only the pieces it wants to override. Missing infrastructure is resolved from its parent.

## Root context

The root context is the top of the tree.

It must have:

* a `LogSinkProto`;
* a `LogPayloadProcessorProto`.

These dependencies are mandatory because every accepted event eventually needs payload processing and sink delivery.

A root context is created with `parent=None`:

```python
root_ctx = LogContext(
    namespace="",
    log_sink=sink,
    payload_processor=payload_processor,
)
```

The root context may also have:

* a local event policy;
* a local log error handling policy.

If no error handling policy is supplied for the root context, it uses `LogErrorHandlingPolicy.PRINT_STDERR`.

The root context cannot reset its sink or payload processor, because there is no parent to fall back to.

## Child contexts

A child context has a parent context.

```python
ctx = LogContext(
    namespace="my_app.db",
    parent=root_ctx,
)
```

A child context may define local overrides:

```python
ctx = LogContext(
    namespace="my_app.db",
    parent=root_ctx,
    log_sink=db_sink,
    event_policy=db_policy,
    payload_processor=db_payload_processor,
    log_error_handling_policy=LogErrorHandlingPolicy.RAISE,
)
```

A child context does not need to define all dependencies. If a dependency is absent, the context resolves it from its parent where inheritance is supported.

## Inherited and local properties

`LogContext` has four configurable runtime components:

```text
log sink
payload processor
log error handling policy
event policy
```

They do not all behave the same way.

### log sink

The sink is inherited.

If a context has a local sink, `context.log_sink` returns it. Otherwise, the lookup continues through the parent chain.

```text
my_app.db local sink? yes -> use it
my_app.db local sink? no  -> ask parent
```

This allows many contexts to share one root sink while specific namespaces route events elsewhere.

A non-root context may reset its local sink. After reset, it uses the parent sink again.

The root context cannot reset its sink.

### payload processor

The payload processor is inherited.

If a context has a local processor, `context.payload_processor` returns it. Otherwise, the lookup continues through the parent chain.

This allows a project to keep one default processor and override payload normalization only for selected namespaces.

A non-root context may reset its local payload processor. After reset, it uses the parent processor again.

The root context cannot reset its payload processor.

### log error handling policy

The log error handling policy is inherited.

If a context has a local policy, `context.log_error_handling_policy` returns it. Otherwise, the lookup continues through the parent chain.

This policy controls what happens when logging infrastructure itself fails while delivering an already accepted event.

A non-root context may reset its local error handling policy. After reset, it uses the parent policy again.

The root context cannot reset its error handling policy.

### event policy

The event policy is local.

`context.event_policy` returns the policy stored directly on that context. If no policy is configured on that context, the value is `None`.

`LogContext.is_event_enabled()` uses only the local event policy:

```text
local event policy is None -> event is enabled
local event policy exists  -> call policy.is_event_enabled(meta)
```

It does not walk the parent chain.

This behavior is intentional.

A logging namespace usually represents a specific application area or layer. Higher-level layers should not need to know the internal event structure of lower-level layers.

If event policy were inherited automatically, policy rules from a parent layer could accidentally depend on event names, entities, or source locations that belong to a lower-level layer. That would make logging configuration leak implementation details across layer boundaries.

Keeping event policy local protects this boundary. A parent context can define policy for its own area, and a child context can define policy for its own area. They do not need to share the same event-selection rules.

This is an important distinction. Sink, payload processor, and error handling policy are inherited runtime infrastructure. Event policy is a local filter attached to the context where the event is emitted.

## Namespace chain

Package-level context configuration uses dotted namespaces.

When a context is configured for:

```text
my_app.db.pool
```

and intermediate contexts do not exist yet, the package-level registry creates the chain:

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

Only the requested leaf context receives the explicit configuration passed to `configure_log_context()`.

Intermediate contexts are structural parents unless they already exist.

For example:

```python
ctx = configure_log_context(
    "my_app.db.pool",
    event_policy=pool_policy,
)
```

creates or updates the `my_app.db.pool` context with `pool_policy`. It does not automatically attach that policy to `my_app` or `my_app.db`.

## Package-level registry

The public facade stores package-managed contexts in an internal registry.

Common operations are:

```python
root = get_root_log_context()
ctx = configure_log_context("my_app.db")
same_ctx = get_log_context("my_app.db")
namespaces = get_log_context_namespaces()
```

`configure_log_context()` behaves as an upsert operation.

If the context already exists, only explicitly supplied components are updated:

```python
configure_log_context(
    "my_app.db",
    event_policy=new_policy,
)
```

This updates the existing context's event policy and leaves its other local components unchanged.

If the context does not exist, the registry creates the missing namespace chain and returns the leaf context.

`reset_log_contexts()` removes all non-root contexts from the package-level registry. It does not destroy manually created contexts that were never registered there.

`reset_logger()` resets both package-level sink and context registries, then recreates the default root setup.

## Default package setup

Importing the package creates a default root logging setup.

The default root context uses:

* namespace `""`;
* default sink name `"stderr"`;
* `StreamLogSink` as the default sink implementation;
* `LogPayloadProcessor()` as the default payload processor.

This means user code can obtain the root context immediately:

```python
ctx = get_root_log_context()
```

and emit structured events without building a logger environment first.

## Direct usage without registry

`LogContext` can be used directly.

This is useful when code wants a local logging pipeline without package-level registration.

```python
ctx = LogContext(
    namespace="local.component",
    log_sink=sink,
    payload_processor=payload_processor,
)
```

A direct context still follows the same runtime rules:

* it creates `LogEventMeta`;
* it checks its local event policy;
* it resolves its payload processor;
* it normalizes payload;
* it emits a final `LogEvent` to its sink.

The difference is only ownership. Package-level functions do not know about this context unless the context was created through the package-level facade.

## Context as coordinator

`LogContext` coordinates the logging pipeline, but it does not replace the surrounding components.

It does not format text lines. That belongs to a sink or sink helper.

It does not decide how arbitrary objects should be represented. That belongs to the payload processor and log adapters.

It does not own backend lifecycle in general. That belongs to sinks and their terminators.

It does not provide a global mutable policy for every namespace. Event policy is local to the context that emits the event.

Its job is to assemble these decisions in the correct order.

## Manual event flow through a context

For a manual event:

```python
ctx.log_info_event(
    event="connection.opened",
    entity_id="conn-1",
    payload={"remote": "127.0.0.1"},
)
```

`LogContext` performs the following steps:

```text
1. Build LogEventMeta.
2. Check ctx.is_event_enabled(meta).
3. Resolve payload processor.
4. Normalize payload unless skip_payload_normalization=True.
5. Create LogEvent.
6. Resolve sink.
7. Call sink.log(event).
8. Apply log error handling policy if sink delivery fails.
```

The context keeps the order stable. This makes behavior predictable for manual logging, decorators, and direct component usage.

## Reset behavior

Resetting a local component removes the local override and makes the context use inherited behavior where inheritance exists.

```python
ctx.reset_log_sink()
ctx.reset_payload_processor()
ctx.reset_log_error_handling_policy()
ctx.reset_event_policy()
```

There is one important difference:

* resetting sink, payload processor, or error handling policy on the root context is invalid;
* resetting event policy is valid because `None` means that all events are enabled for that context.

The root context must always keep a sink and payload processor.

## Design summary

The context tree gives the logger a controlled configuration hierarchy.

Root context provides mandatory infrastructure.

Child contexts can override selected components.

Sink, payload processor, and log error handling policy are inherited.

Event policy is local.

Package-level registries provide convenient named access, but `LogContext` remains usable directly when a local logging pipeline is preferable.
