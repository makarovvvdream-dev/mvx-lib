# Failure model

```{contents} Contents:
:depth: 1
:local:
```

`MVX Metrics` separates production failures from metrics-infrastructure failures.

A business operation may fail because of domain logic.

A recorder may fail because its lifecycle, queue, dispatcher, or hooks failed.

A runtime may fail because its thread, event loop, recorder registry, startup, or shutdown failed.

These are different failure areas and they should not be mixed.

## Main failure boundaries

The main boundaries are:

```text
production component
   |
   v
recorder handoff
   |
   v
recorder processing
   |
   v
runtime environment
```

Each area owns its own failure model.

```text
production component
   |
   v
business errors

recorder
   |
   v
event-processing and metric-dispatch errors

runtime
   |
   v
thread, event loop, recorder registry, startup, shutdown errors
```

This separation lets application code decide how strongly metrics failures should affect business operations.

## Production-side failures

Production code owns business behavior.

For example:

```python
def save_document(self, document_id: str, content: str) -> None:
    if not document_id:
        raise ValueError("document_id must not be empty")
```

That error belongs to the production component.

It is not a metrics failure.

The metric event may record that the operation failed, but the source of the failure is still business logic.

```text
business method fails
   |
   v
method emits FAILURE metric event
   |
   v
original business error is raised
```

Metrics observe the failure. They do not own it.

## Recorder handoff failures

Production code emits metric events through the recorder:

```python
self._metrics_recorder.register_event(event=event)
```

This call can fail if the recorder cannot accept the event.

Typical reasons include:

* invalid recorder state;
* queue overflow when overflow policy is configured to raise;
* recorder already failed earlier;
* invalid event object.

A production component may decide to suppress recorder errors:

```python
def _send_metric_event(self, event: MetricEvent) -> None:
    if self._metrics_recorder is None:
        return

    try:
        self._metrics_recorder.register_event(event=event)
    except Exception:
        pass
```

This pattern treats metrics as optional observability.

If metrics fail, the business operation does not fail only because metrics emission failed.

This is a production integration decision.

A different application may choose to let recorder errors propagate.

## Recorder lifecycle failures

`AsyncioMetricsRecorder` has its own lifecycle.

At a high level, recorder operations are valid only in specific states.

For example:

```text
start
   |
   v
valid from VIRGIN or STOPPED

stop
   |
   v
valid from RUNNING
```

If an operation is requested in the wrong state, the recorder reports an invalid-state error.

This prevents lifecycle misuse from being silently ignored.

## Event loop availability

`AsyncioMetricsRecorder` is an asyncio-based component.

When used directly, it must be created in an environment where a running `asyncio` event loop is available.

If no running loop exists, recorder construction fails with a loop-unavailable error.

This is one of the reasons `MetricsRuntime` exists.

Runtime-created recorders are created inside the runtime-owned event loop, so regular application code does not need to
provide that loop manually.

## Queue overflow

The recorder has an internal processing buffer and a pending-event limit.

If events arrive faster than they can be processed, the pending-event limit can be reached.

At that point, overflow behavior depends on recorder configuration:

```text
pending-event limit reached
   |
   +--> RAISE_ERROR
   |       |
   |       v
   |    raise queue overflow error
   |
   +--> DROP
           |
           v
        drop new event
```

This is an important architectural choice.

`RAISE_ERROR` makes pressure visible to the caller.

`DROP` keeps the production handoff non-blocking and lossy under pressure.

The choice belongs to recorder configuration and application policy.

## Hook failures

Recorder hooks are extension points.

The recorder has startup and shutdown hooks:

```text
_on_starting()
_on_stopped()
```

If `_on_starting()` fails, recorder startup fails.

If `_on_stopped()` fails during normal shutdown, recorder shutdown fails.

This keeps extension failures visible.

A subclass that opens a backend connection during startup, for example, should not silently enter running state if that
backend setup fails.

## Dispatcher failures

The dispatcher is the recorder-side task that processes accepted events and sends them to registered metrics.

The simplified path is:

```text
event from queue
   |
   v
dispatch to registered metrics
   |
   v
metric.handle_event(event)
```

If dispatching raises an error, the recorder treats it as a metrics-infrastructure failure.

The recorder can move into failure state and store the error.

If logging is configured, dispatcher failures are logged as recorder infrastructure diagnostics.

The business operation that originally emitted the event has already crossed the recorder boundary.

## Unexpected dispatcher cancellation

Dispatcher cancellation is expected during normal recorder shutdown.

Unexpected dispatcher cancellation is different.

If the dispatcher is cancelled while the recorder is not stopping, the recorder treats that as an error condition.

This protects the recorder from silently losing its processing task.

## Cleanup failures

If dispatcher failure requires cleanup, cleanup itself may also fail.

Cleanup failures are not business failures.

They are recorder infrastructure failures.

When a log context is available, cleanup-related failures are logged as diagnostic events.

The goal is to make recorder-side problems visible without confusing them with domain operation errors.

## Runtime startup failures

`MetricsRuntime` owns a dedicated thread and an `asyncio` event loop inside that thread.

Startup can fail if the runtime cannot reach a running state.

At a high level:

```text
runtime.start()
   |
   v
create thread
   |
   v
create event loop
   |
   v
signal loop readiness
   |
   v
RUNNING
```

If that sequence fails, the runtime reports a startup error.

After startup failure, the runtime enters failure state.

## Runtime invalid state

Runtime operations are valid only in specific states.

For example, recorder creation requires a running runtime.

If application code tries to create a recorder before the runtime is started, after it is closed, or while it is in
failure state, the runtime reports an invalid-state error.

This keeps runtime lifecycle explicit.

## Runtime loop unavailable

Runtime public methods often need access to the runtime-owned event loop.

If the runtime is expected to be running but the loop is missing, the runtime reports a loop-unavailable error.

This is a runtime infrastructure failure.

It means the runtime cannot schedule recorder work into its processing environment.

## Recorder registry failures

`MetricsRuntime` owns a recorder registry.

Registry operations have their own failures.

For example:

* creating a recorder with an id that already exists;
* getting a recorder that is not registered;
* stopping a recorder that cannot be stopped;
* recorder startup failure during `create_recorder()`.

These are runtime-recorder management failures.

They are different from metric interpretation failures and different from business operation failures.

## Runtime shutdown failures

Runtime shutdown is responsible for stopping and removing runtime-owned recorders, stopping the event loop, joining the
runtime thread, and clearing runtime references.

The simplified flow is:

```text
runtime.shutdown()
   |
   v
stop/remove all recorders
   |
   v
stop event loop
   |
   v
join runtime thread
   |
   v
clear runtime references
```

If recorder shutdown fails, the runtime collects those failures and reports shutdown failure.

This keeps shutdown failure visible at the runtime boundary.

## Logging failure diagnostics

Recorder and runtime failures can be logged through `MVX Logger` when a log context is configured.

This logging is diagnostic.

It does not turn metric events into log events.

It reports infrastructure problems such as:

```text
recorder dispatch error
recorder cleanup failure
runtime lifecycle operation failure
recorder management failure
```

Logging is therefore part of the failure visibility model, not part of metric aggregation.

## Failure ownership

The ownership model is:

```text
business operation failed
   |
   v
production component owns the error

metric event handoff failed
   |
   v
recorder-facing integration owns the decision to suppress or propagate

recorder lifecycle or dispatcher failed
   |
   v
recorder owns the error

runtime startup, registry, or shutdown failed
   |
   v
runtime owns the error
```

This keeps failure handling localized.

## Why this matters

Metrics should improve observability without making the business path fragile by default.

At the same time, metrics infrastructure should not hide its own failures.

The architecture therefore separates two decisions:

```text
Should a production operation fail because metrics failed?
   |
   v
production integration decision

Should recorder/runtime failures be visible and diagnosable?
   |
   v
yes, through domain errors, states, and logging
```

This gives applications control.

A small script may suppress recorder errors.

A strict service may surface them.

A test may assert them.

The metrics core provides explicit failure boundaries for all of these choices.

## Summary

`MVX Metrics` has separate failure boundaries for production code, recorders, and runtime.

Production code owns business errors.

Recorders own event-processing, lifecycle, queue, dispatcher, and hook errors.

`MetricsRuntime` owns thread, event loop, recorder registry, startup, shutdown, and recorder-management errors.

This separation keeps production behavior, metrics processing, and runtime management understandable and testable as
separate parts of the system.
