# MetricsRuntime

```{contents} Contents:
:depth: 1
:local:
```

`MetricsRuntime` is an isolated execution environment for metrics recorders.

It owns:

* a dedicated thread;
* an `asyncio` event loop inside that thread;
* a registry of recorders created inside the runtime;
* the lifecycle of this processing environment.

Runtime created recorders live inside `MetricsRuntime`.

Production components do not use the runtime directly. They receive ready-to-use recorders from the application code and
emit metric events through them.

Application code owns the runtime: it starts the runtime, creates recorders inside it, passes those recorders to
production components, and shuts the runtime down when metrics processing is no longer needed.

## Why MetricsRuntime exists

`MetricsRuntime` exists to move recorder processing into a separate managed environment.

It also gives application code a simple thread-safe API over that environment.

Without the runtime, application code would have to manage the processing thread, the event loop, recorder startup,
recorder registry, shutdown, and lifecycle coordination directly.

With the runtime, application code works with a simpler shape:

```text
application
    -> start runtime
    -> create recorders
    -> pass recorders to production components
    -> shut runtime down when metrics processing is no longer needed
```

The internal machinery remains behind the runtime API.

## Runtime environment

After `start()`, the runtime has its execution environment ready:

```python
runtime = MetricsRuntime(namespace="example.metrics")
runtime.start()
```

The runtime thread and its `asyncio` event loop are used by recorders created inside this runtime.

The namespace identifies the runtime scope. For simple applications, it can be treated as the name of the
application-level or subsystem-level metrics runtime.

## Creating recorders

Application code creates recorders through the runtime:

```python
recorder = runtime.create_recorder("document_storage")
```

The recorder belongs to this runtime and uses its processing environment.

It can then be passed to a production component:

```python
storage = DocumentStorage(metrics_recorder=recorder)
```

The component sees only the recorder. It does not know which thread or event loop is behind it.

## Runtime as recorder registry

`MetricsRuntime` keeps recorders created inside it.

This lets application code manage recorders in one place.

For example:

```text
MetricsRuntime(namespace="networking.metrics")
    recorder: connection-001
    recorder: connection-002
    recorder: connection-003
```

This is the intended scale model:

```text
one runtime
    many recorders
```

not:

```text
many components
    many runtimes
```

```{warning}
The usual application model is to create and keep **one** `MetricsRuntime` running for the application or for a large subsystem, and then create multiple recorders inside that runtime.

Creating a separate runtime for every component or every measured entity is not the intended usage. Each runtime owns its own thread and event loop, so creating too many runtimes can waste threads and eventually exhaust process resources.

Multiple runtimes should be reserved for cases where metrics processing is intentionally separated, for example when one runtime is not enough for a very heavy metrics workload or when subsystems must be isolated deliberately.
```

## Event processing path

When a production component emits an event through a runtime-created recorder, the event crosses from production code
into the runtime-managed processing environment:

```text
production method
    -> recorder.register_event(event)
    -> recorder internal buffer
    -> runtime thread / asyncio event loop
    -> recorder processing
```

The recorder is the boundary seen by production code.

`MetricsRuntime` is the environment behind that boundary.

## Typical wiring

A typical application wiring looks like this:

```python
runtime = MetricsRuntime(namespace="example.metrics")
runtime.start()

try:
    recorder = runtime.create_recorder("document_storage")

    storage = DocumentStorage(metrics_recorder=recorder)

    storage.save_document("doc-001", "First document")

finally:
    runtime.shutdown()
```

The important ownership model is:

```text
application code
    owns MetricsRuntime

MetricsRuntime
    owns the processing environment and recorder registry

production component
    receives a recorder
```

## Runtime lifecycle

The runtime should be started before creating and using recorders:

```python
runtime.start()
```

It should be shut down when metrics processing is no longer needed:

```python
runtime.shutdown()
```

The usual pattern is:

```python
runtime = MetricsRuntime(namespace="example.metrics")
runtime.start()

try:
    ...
finally:
    runtime.shutdown()
```

Application code owns this lifecycle.

Production components should not create or stop the runtime themselves.

## Thread-safe API

`MetricsRuntime` hides a relatively complex internal model behind a simple API.

Application code can create and access recorders without directly coordinating the runtime thread or event loop.

That is one of the main reasons for the runtime: it provides a manageable API while the actual metrics processing
environment remains isolated.

## When MetricsRuntime is useful

Use `MetricsRuntime` when recorders need a managed processing environment.

Typical cases:

* application-level metrics processing;
* subsystem-level metrics processing;
* reusable libraries that should not own an application event loop;
* connection pools or similar components that create many recorders;
* tests or diagnostics that need real recorder processing without custom infrastructure.

## Summary

`MetricsRuntime` is the isolated execution environment for metrics recorders.

It owns a dedicated thread, an `asyncio` event loop, and a recorder registry.

The normal application model is one runtime with many recorders.

Production components receive recorders, not the runtime itself.
