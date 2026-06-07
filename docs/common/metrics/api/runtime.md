# Runtime

This page documents the `MetricsRuntime` API.

`MetricsRuntime` is the synchronous management layer for runtime-owned recorders. It owns the runtime thread, the
runtime `asyncio` event loop, and the recorder registry.

Production components normally do not depend on `MetricsRuntime` directly. Application code creates the runtime, creates
recorders inside it, and passes those recorders to production components.

## Public API

```{eval-rst}
.. autoenum:: mvx.common.metrics.MetricsRuntimeState

.. autoclass:: mvx.common.metrics.MetricsRuntime
   :members:
   :member-order: bysource
   :class-doc-from: both
```

## Runtime lifecycle

A runtime is created in `VIRGIN` state.

```python
runtime = MetricsRuntime(namespace="example.metrics")
```

The constructor does not start the runtime thread or create the runtime event loop.

Start the runtime before creating recorders:

```python
runtime.start()
```

Shut it down when metrics processing is no longer needed:

```python
runtime.shutdown()
```

A typical shape is:

```python
runtime = MetricsRuntime(namespace="example.metrics")
runtime.start()

try:
    recorder = runtime.create_recorder("document_storage")
    ...
finally:
    runtime.shutdown()
```

## Recorder creation

Use `create_recorder()` to create a recorder inside the runtime-managed event loop:

```python
recorder = runtime.create_recorder("document_storage")
```

The method creates an `AsyncioMetricsRecorder`, starts it, stores it in the runtime registry, and returns the
ready-to-use recorder.

Recorder options can be provided per recorder:

```python
recorder = runtime.create_recorder(
    "document_storage",
    entity_id="document-storage-main",
    namespace="example.metrics.document_storage",
    queue_max_size=10_000,
    queue_overflow_policy=AsyncioMetricsRecorderQueueOverflowPolicy.DROP,
)
```

If no explicit `entity_id` is provided, the recorder id is used as the recorder entity id.

## Recorder registry

`MetricsRuntime` keeps recorders created inside it.

The registry API includes:

```python
recorder = runtime.get_recorder("document_storage")
```

```python
recorder = runtime.try_get_recorder("document_storage")
```

```python
recorder_ids = runtime.list_recorder_ids()
```

`get_recorder()` raises if the recorder does not exist.

`try_get_recorder()` returns `None` when the recorder is not registered.

## Recorder stopping and removal

A recorder can be stopped without being removed:

```python
runtime.stop_recorder("document_storage")
```

A recorder can also be stopped and removed from the registry:

```python
recorder = runtime.stop_and_remove_recorder("document_storage")
```

Use `stop_recorder()` when the recorder should remain registered.

Use `stop_and_remove_recorder()` when the measured scope is gone and the recorder should no longer be managed by the
runtime.

## Runtime state

`MetricsRuntimeState` describes the runtime lifecycle.

The usual path is:

```text
VIRGIN
   |
   v
STARTING
   |
   v
RUNNING
   |
   v
STOPPING
   |
   v
CLOSED
```

If startup or shutdown fails, the runtime can move to `FAILURE`.

The current state can be read with:

```python
state = runtime.get_status()
```

## Logging

`MetricsRuntime` can use `MVX Logger` when a `log_context` is provided.

Runtime public operations are instrumented with `log_invocation`, including:

```text
metrics_runtime.start
metrics_runtime.shutdown
metrics_runtime.create_recorder
metrics_runtime.get_recorder
metrics_runtime.try_get_recorder
metrics_runtime.list_recorder_ids
metrics_runtime.stop_recorder
metrics_runtime.stop_and_remove_recorder
```

Ready-to-use runtime logging policies are documented in the logging policies API page.


