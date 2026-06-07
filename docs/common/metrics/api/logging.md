# Logging

`MVX Metrics` can log recorder and runtime diagnostic events through `MVX Logger`.

Logging is enabled when a metrics component receives a `log_context`.

For example, a runtime can be created with a log context:

```python
runtime = MetricsRuntime(
    namespace="example.metrics",
    log_context=log_context,
)
```

Recorders created by this runtime use the runtime log context by default, unless a recorder-specific context is
provided.

A recorder can also receive a log context directly:

```python
recorder = AsyncioMetricsRecorder(
    "document_storage",
    log_context=log_context,
)
```

If no `log_context` is provided, recorder/runtime diagnostic logging is disabled.

## Recorder logging

`AsyncioMetricsRecorder` can log public recorder operations and internal recorder infrastructure failures.

Recorder logging is controlled by ready-to-use policy helpers based on `AsyncioMetricsRecorderLogPolicyMode`.

### Recorder log events

`AsyncioMetricsRecorder` has two kinds of log events:

* public operation events produced by `log_invocation`;
* internal infrastructure error events emitted directly by the recorder.

### Recorder operation events

These events describe public recorder operations:

| Event                                           | When it is emitted                    |
|-------------------------------------------------|---------------------------------------|
| `asyncio_metrics_recorder.start`                | around recorder startup               |
| `asyncio_metrics_recorder.stop`                 | around recorder shutdown              |
| `asyncio_metrics_recorder.register_metric`      | when a metric is registered           |
| `asyncio_metrics_recorder.get_metric_snapshots` | when metric snapshots are requested   |
| `asyncio_metrics_recorder.iter_metrics`         | when registered metrics are requested |

`register_event()` is intentionally not logged as an operation event.

```python
recorder.register_event(event=event)
```

It is the production hot-path handoff for metric events. Logging every submitted metric event would make recorder
diagnostics noisy and would duplicate the metrics pipeline itself.

### Recorder infrastructure error events

These events are emitted directly by the recorder when internal processing fails:

| Event                                          | When it is emitted                                          |
|------------------------------------------------|-------------------------------------------------------------|
| `metrics_recorder.dispatch_error`              | event dispatching fails inside the recorder dispatcher loop |
| `metrics_recorder.cleanup.task_creation_error` | cleanup task creation fails after dispatcher failure        |
| `metrics_recorder.cleanup.failed`              | recorder cleanup fails                                      |

These are recorder infrastructure diagnostics.

They describe failures in recorder processing, not business metric events.

### Recorder policy modes

Recorder policies use `AsyncioMetricsRecorderLogPolicyMode`.

```text
SILENT
NORMAL
INSPECTION
```

`SILENT`

: No recorder events are enabled by default.

`NORMAL`

: Enables ordinary recorder lifecycle, metric registration, and infrastructure-failure diagnostics.

```text
asyncio_metrics_recorder.start
asyncio_metrics_recorder.stop
asyncio_metrics_recorder.register_metric
metrics_recorder.dispatch_error
metrics_recorder.cleanup.task_creation_error
metrics_recorder.cleanup.failed
```

`INSPECTION`

: Enables all `NORMAL` events and additionally enables recorder inspection events.

```text
asyncio_metrics_recorder.get_metric_snapshots
asyncio_metrics_recorder.iter_metrics
```

### Recorder policy helper API

Use `asyncio_metrics_recorder_event_policy_config()` when you need a `PatternLogEventPolicyConfig`.

Use `asyncio_metrics_recorder_event_policy()` when you need a ready-to-use `PatternLogEventPolicy`.

```{eval-rst}
.. autofunction:: mvx.common.metrics.asyncio_metrics_recorder_event_policy_config

.. autofunction:: mvx.common.metrics.asyncio_metrics_recorder_event_policy
```

## Runtime logging

`MetricsRuntime` can log runtime lifecycle, recorder management, and runtime inspection calls.

Runtime logging is controlled by ready-to-use policy helpers based on `MetricsRuntimeLogPolicyMode`.

### Runtime log events

`MetricsRuntime` logs public runtime operations through `log_invocation`.

These events describe runtime lifecycle, recorder management, and runtime inspection calls.

| Event                                      | When it is emitted                                               |
|--------------------------------------------|------------------------------------------------------------------|
| `metrics_runtime.start`                    | around runtime startup                                           |
| `metrics_runtime.shutdown`                 | around runtime shutdown                                          |
| `metrics_runtime.create_recorder`          | when a recorder is created by the runtime                        |
| `metrics_runtime.stop_recorder`            | when a recorder is stopped without being removed                 |
| `metrics_runtime.stop_and_remove_recorder` | when a recorder is stopped and removed from the runtime registry |
| `metrics_runtime.get_recorder`             | when a recorder is requested by id and absence is an error       |
| `metrics_runtime.try_get_recorder`         | when a recorder is requested by id and absence returns `None`    |
| `metrics_runtime.list_recorder_ids`        | when runtime recorder ids are requested                          |

### Runtime policy modes

Runtime policies use `MetricsRuntimeLogPolicyMode`.

```text
SILENT
NORMAL
INSPECTION
```

`SILENT`

: No runtime events are enabled by default.

`NORMAL`

: Enables runtime lifecycle and recorder-management events.

```text
metrics_runtime.start
metrics_runtime.shutdown
metrics_runtime.create_recorder
metrics_runtime.stop_recorder
metrics_runtime.stop_and_remove_recorder
```

`INSPECTION`

: Enables all `NORMAL` events and additionally enables runtime inspection events.

```text
metrics_runtime.get_recorder
metrics_runtime.try_get_recorder
metrics_runtime.list_recorder_ids
```

### Runtime policy helper API

Use `metrics_runtime_event_policy_config()` when you need a `PatternLogEventPolicyConfig`.

Use `metrics_runtime_event_policy()` when you need a ready-to-use `PatternLogEventPolicy`.

```{eval-rst}
.. autofunction:: mvx.common.metrics.metrics_runtime_event_policy_config

.. autofunction:: mvx.common.metrics.metrics_runtime_event_policy
```

## Summary

Metrics recorder and runtime logging is enabled through `log_context`.

Recorder logging and runtime logging have separate event sets and separate policy helpers.

Ready-to-use policies group events into `SILENT`, `NORMAL`, and `INSPECTION` modes.

This lets applications enable useful diagnostics without writing custom event-policy rules immediately.
