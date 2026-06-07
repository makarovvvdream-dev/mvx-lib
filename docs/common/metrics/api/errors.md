# Errors

This page documents metrics-related errors.

`MVX Metrics` has two error families:

* recorder errors;
* runtime errors.

Recorder errors belong to `AsyncioMetricsRecorder`.

Runtime errors belong to `MetricsRuntime`.

## Error hierarchy

```text
AsyncioMetricsRecorderError
|
+-- AsyncioMetricsRecorderLoopUnavailableError
+-- AsyncioMetricsRecorderInvalidStateError
+-- AsyncioMetricsRecorderOnStartingHookFailedError
+-- AsyncioMetricsRecorderOnStoppedHookFailedError
+-- AsyncioMetricsRecorderQueueOverflowError
+-- AsyncioMetricsRecorderDispatcherCancelledError
+-- AsyncioMetricsRecorderUnexpectedError


MetricsRuntimeError
|
+-- MetricsRuntimeInvalidStateError
+-- MetricsRuntimeStartupError
+-- MetricsRuntimeShutdownError
+-- MetricsRuntimeLoopUnavailableError
+-- MetricsRuntimeRecorderError
|   |
|   +-- MetricsRuntimeRecorderStartupError
|   +-- MetricsRuntimeRecorderAlreadyExistsError
|   +-- MetricsRuntimeRecorderNotFoundError
|   +-- MetricsRuntimeRecorderStopError
|
+-- MetricsRuntimeUnexpectedError
```

Both base error classes inherit from `ReasonedError`.

Unexpected errors also inherit from `RuntimeUnexpectedError`.

## Recorder errors

Recorder errors describe failures in `AsyncioMetricsRecorder` construction, lifecycle, queue handling, dispatcher
execution, and recorder hooks.

```{eval-rst}
.. autoclass:: mvx.common.metrics.AsyncioMetricsRecorderError
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.AsyncioMetricsRecorderLoopUnavailableError
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.AsyncioMetricsRecorderInvalidStateError
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.AsyncioMetricsRecorderOnStartingHookFailedError
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.AsyncioMetricsRecorderOnStoppedHookFailedError
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.AsyncioMetricsRecorderQueueOverflowError
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.AsyncioMetricsRecorderDispatcherCancelledError
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.AsyncioMetricsRecorderUnexpectedError
   :members:
   :member-order: bysource
   :class-doc-from: class
```

## Runtime errors

Runtime errors describe failures in `MetricsRuntime` lifecycle, runtime loop access, recorder registry operations, and
recorder management.

```{eval-rst}
.. autoclass:: mvx.common.metrics.MetricsRuntimeError
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.MetricsRuntimeInvalidStateError
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.MetricsRuntimeStartupError
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.MetricsRuntimeShutdownError
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.MetricsRuntimeLoopUnavailableError
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.MetricsRuntimeRecorderError
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.MetricsRuntimeRecorderStartupError
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.MetricsRuntimeRecorderAlreadyExistsError
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.MetricsRuntimeRecorderNotFoundError
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.MetricsRuntimeRecorderStopError
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.MetricsRuntimeUnexpectedError
   :members:
   :member-order: bysource
   :class-doc-from: class
```

## Boundary with production errors

Metrics errors are infrastructure errors.

They are separate from production-domain errors.

For example, this is a production error:

```python
raise ValueError("document_id must not be empty")
```

This is a recorder infrastructure error:

```python
raise AsyncioMetricsRecorderQueueOverflowError()
```

A production component may choose to suppress recorder errors when metrics are optional:

```python
try:
    self._metrics_recorder.register_event(event=event)
except Exception:
    pass
```

Runtime management code usually should not hide runtime errors silently.

Runtime errors describe problems with runtime startup, shutdown, event-loop access, recorder registry operations, or
recorder lifecycle management.

## Summary

Use recorder errors for `AsyncioMetricsRecorder` construction, lifecycle, queue, hook, and dispatcher failures.

Use runtime errors for `MetricsRuntime` startup, shutdown, loop, registry, and recorder-management failures.

Production-domain errors remain separate from metrics infrastructure errors.
