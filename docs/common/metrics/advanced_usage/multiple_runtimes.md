# Multiple runtimes

```{contents} Contents:
:depth: 1
:local:
```

The usual `MVX Metrics` application model is:

```text
one application or large subsystem
   |
   v
one MetricsRuntime
   |
   v
many recorders
```

A runtime owns a dedicated thread and an `asyncio` event loop.

Because of that, creating many runtimes is not a neutral operation. Each runtime adds another thread, another event
loop, another lifecycle, and another shutdown boundary.

For most applications, one runtime is enough.

## Default model: one runtime, many recorders

Use one `MetricsRuntime` for the application or for a large subsystem.

Create recorders inside that runtime:

```text
MetricsRuntime(namespace="application.metrics")
   |
   +--> recorder: document_storage
   |
   +--> recorder: connection-001
   |
   +--> recorder: connection-002
   |
   +--> recorder: worker-3
```

Recorders are the normal unit of separation.

If the application needs separate metric state for several measured entities, create several recorders, not several
runtimes.

```text
one measured entity
   |
   v
one recorder
```

For example, a connection pool should usually use:

```text
one MetricsRuntime
   |
   +--> recorder: connection-001
   +--> recorder: connection-002
   +--> recorder: connection-003
```

not:

```text
runtime for connection-001
runtime for connection-002
runtime for connection-003
```

## Why not one runtime per entity

A runtime is heavier than a recorder.

A recorder represents a measured scope.

A runtime represents an execution environment for recorders.

Creating one runtime per entity means creating one thread and one event loop per entity.

That can waste resources and make shutdown harder.

```text
many measured entities
   |
   v
many runtimes
   |
   v
many threads and event loops
```

This is usually the wrong scale model.

Use recorder ids and `entity_id` to separate measured scopes.

Use runtimes to separate execution environments.

## When multiple runtimes may be justified

Multiple runtimes can be useful when the application needs deliberate isolation.

Typical reasons include:

* a very heavy metrics workload that should not share one runtime loop;
* a subsystem that must be started and stopped independently;
* a plugin or integration that must be isolated from the main metrics runtime;
* experiments or diagnostics that should not affect the main recorder environment;
* different ownership boundaries inside a large process.

In these cases, multiple runtimes are an architectural decision.

They should not appear accidentally just because several components need metrics.

## Heavy metrics workload

A single runtime can own many recorders.

However, all runtime-created recorders share the same runtime thread and event loop.

If metric processing becomes too heavy for one runtime environment, an application may split work across several
runtimes.

For example:

```text
MetricsRuntime(namespace="networking.metrics")
   |
   +--> recorders for network connections

MetricsRuntime(namespace="storage.metrics")
   |
   +--> recorders for storage components
```

This can be useful if the two subsystems have different load profiles or must be isolated operationally.

It should be done because there is a real processing need, not as a default pattern.

## Independent lifecycle

Another reason for multiple runtimes is independent lifecycle.

For example, a large application may have a subsystem that can be loaded and unloaded separately.

In that case, the subsystem may own its own runtime:

```text
application runtime
   |
   v
main MetricsRuntime

plugin subsystem
   |
   v
plugin MetricsRuntime
```

When the subsystem is unloaded, its runtime can be shut down without affecting the main metrics runtime.

This is a lifecycle isolation decision.

## Integration isolation

A custom integration may also need isolation.

For example, an experimental adapter may use a separate runtime so failures or high load in that integration do not
affect the main metrics processing environment.

```text
main metrics runtime
   |
   v
normal application recorders

experimental integration runtime
   |
   v
integration-specific recorders
```

This pattern should be used carefully.

It adds operational complexity.

## What multiple runtimes do not solve

Multiple runtimes do not replace recorder scoping.

If the problem is:

```text
I need separate metrics for each connection
```

the answer is usually:

```text
one recorder per connection
```

not:

```text
one runtime per connection
```

Multiple runtimes also do not remove the cost of metric processing.

They only move work into separate runtime environments.

If the metrics themselves are too heavy, the application may also need to simplify metrics, reduce event volume, change
overflow policy, or introduce a proper external adapter.

## Naming runtimes

When using more than one runtime, use clear namespaces.

```python
networking_runtime = MetricsRuntime(namespace="networking.metrics")
storage_runtime = MetricsRuntime(namespace="storage.metrics")
```

The namespace should describe the runtime scope.

It should not duplicate individual recorder ids.

Good:

```text
networking.metrics
storage.metrics
plugin.metrics
```

Poor:

```text
connection-001.runtime
connection-002.runtime
connection-003.runtime
```

The latter usually indicates that recorders are being modeled as runtimes.

## Shutdown responsibility

Each runtime has its own lifecycle.

If an application creates several runtimes, it must shut all of them down.

```text
application shutdown
   |
   +--> shutdown networking runtime
   |
   +--> shutdown storage runtime
   |
   +--> shutdown plugin runtime
```

This is another reason to avoid unnecessary runtimes.

One runtime means one shutdown boundary.

Many runtimes mean many shutdown boundaries.

## Decision rule

Use this rule:

```text
need separate metric state
   |
   v
create another recorder

need separate execution environment
   |
   v
consider another runtime
```

Most cases need separate metric state, not a separate execution environment.

## Summary

The normal `MVX Metrics` model is one `MetricsRuntime` with many recorders.

Recorders separate measured scopes.

Runtimes separate execution environments.

Create multiple runtimes only when there is a deliberate need for processing isolation, independent lifecycle, or heavy
workload separation.

Do not create a runtime per component or per measured entity by default.
