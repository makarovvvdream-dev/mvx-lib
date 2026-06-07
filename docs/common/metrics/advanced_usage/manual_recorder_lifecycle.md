# Manual recorder lifecycle

```{contents} Contents:
:depth: 1
:local:
```

Usually, recorders are created through `MetricsRuntime`:

```python
recorder = runtime.create_recorder("document_storage")
```

In that mode, the runtime creates the recorder in its own event loop, starts it, keeps it in the runtime registry, and
stops it during shutdown.

Manual lifecycle means using `AsyncioMetricsRecorder` directly.

This is useful only when the application deliberately wants the recorder to run in an application-owned execution
environment.

## When direct recorder usage is useful

Direct recorder usage is useful when the application intentionally wants the recorder to live outside `MetricsRuntime`.

For example, an application may want recorder processing to run:

* in the main application thread;
* in an already existing worker thread;
* inside an application-owned asyncio subsystem;
* inside a custom integration runtime.

In this mode, `MVX Metrics` does not create the thread or event loop for you.

`AsyncioMetricsRecorder` binds itself to the running `asyncio` event loop at construction time, so the recorder must be
created in a thread where an event loop is already running.

```python
recorder = AsyncioMetricsRecorder("document_storage")
```

If no running event loop is available, direct recorder construction fails with
`AsyncioMetricsRecorderLoopUnavailableError`.

The rule is:

```text
custom recorder environment is allowed
   |
   v
but that environment must already provide a running asyncio event loop
```

If the application does not want to own this execution environment, use `MetricsRuntime`.

## Basic manual lifecycle

The manual lifecycle is:

```text
create recorder inside a running asyncio loop
   |
   v
start recorder
   |
   v
register metrics
   |
   v
emit events
   |
   v
inspect snapshots if needed
   |
   v
stop recorder
```

The important difference from `MetricsRuntime` is ownership.

With `MetricsRuntime`, the runtime owns recorder creation, startup, loop ownership, and shutdown.

With manual lifecycle, the application owns those responsibilities.

## Starting the recorder

`start()` schedules recorder startup and returns an operation result through a wait handle.

In async code, await it:

```python
start_result = await recorder.start()
```

Then check the result:

```python
if not start_result.success:
    raise start_result.error
```

A successful start means the recorder is ready to accept metrics and process events.

## Stopping the recorder

Manual lifecycle code should stop the recorder explicitly.

```python
stop_result = await recorder.stop()
```

Then check the result:

```python
if not stop_result.success:
    raise stop_result.error
```

Stopping is part of the ownership contract.

If the application creates the recorder manually, it should also stop it manually.

## Keep stop in finally

Manual lifecycle should normally use `try` / `finally`.

```python
recorder = AsyncioMetricsRecorder("document_storage")

start_result = await recorder.start()
if not start_result.success:
    raise start_result.error

try:
    ...
finally:
    stop_result = await recorder.stop()
    if not stop_result.success:
        raise stop_result.error
```

This keeps recorder shutdown explicit even if the code using the recorder fails.

## Complete runnable example

The full runnable version of this example is available as a source file:

{download}`Download manual_recorder_lifecycle.py <../../../../common/examples/metrics/manual_recorder_lifecycle.py>`

The same file is covered by tests, so the documented example is checked as real Python code.

```python
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pprint import pprint
from typing import Any, Mapping

from mvx.common.metrics import (
    AsyncioMetricsRecorder,
    AsyncioMetricsRecorderLoopUnavailableError,
    Metric,
    MetricEvent,
)


class DocumentSaveAttemptOutcome(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


@dataclass(frozen=True, slots=True)
class DocumentSaveAttemptMetricEvent(MetricEvent):
    outcome: DocumentSaveAttemptOutcome

    @property
    def event_type(self) -> str:
        return "document_storage.save.attempt"


class DocumentSaveAttemptsMetric(Metric):
    def __init__(self) -> None:
        self._total = 0
        self._success_total = 0
        self._failure_total = 0

    @property
    def metric_name(self) -> str:
        return "document_storage.save.attempts"

    def handle_event(self, event: MetricEvent) -> bool:
        if not isinstance(event, DocumentSaveAttemptMetricEvent):
            return False

        self._total += 1

        if event.outcome is DocumentSaveAttemptOutcome.SUCCESS:
            self._success_total += 1

        elif event.outcome is DocumentSaveAttemptOutcome.FAILURE:
            self._failure_total += 1

        return True

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "total": self._total,
                "success_total": self._success_total,
                "failure_total": self._failure_total,
            },
        }


def create_recorder_without_running_loop() -> None:
    try:
        AsyncioMetricsRecorder("document_storage")
    except AsyncioMetricsRecorderLoopUnavailableError:
        print("recorder creation failed: no running asyncio event loop")


async def run_manual_recorder_example() -> Mapping[str, Mapping[str, Any]]:
    recorder = AsyncioMetricsRecorder("document_storage")

    start_result = await recorder.start()
    if not start_result.success:
        assert start_result.error is not None
        raise start_result.error

    try:
        recorder.register_metric(metric=DocumentSaveAttemptsMetric())

        recorder.register_event(
            event=DocumentSaveAttemptMetricEvent(
                outcome=DocumentSaveAttemptOutcome.SUCCESS,
            )
        )
        recorder.register_event(
            event=DocumentSaveAttemptMetricEvent(
                outcome=DocumentSaveAttemptOutcome.SUCCESS,
            )
        )
        recorder.register_event(
            event=DocumentSaveAttemptMetricEvent(
                outcome=DocumentSaveAttemptOutcome.FAILURE,
            )
        )

        stop_result = await recorder.stop()
        if not stop_result.success:
            assert stop_result.error is not None
            raise stop_result.error

        return recorder.get_metric_snapshots()

    except Exception:
        stop_result = await recorder.stop()
        if not stop_result.success:
            assert stop_result.error is not None
            raise stop_result.error
        raise


async def main() -> None:
    snapshots = await run_manual_recorder_example()
    pprint(snapshots)


if __name__ == "__main__":
    import asyncio

    create_recorder_without_running_loop()
    asyncio.run(main())
```

## What the example shows

The example demonstrates two things.

First, direct recorder construction requires a running event loop.

```python
create_recorder_without_running_loop()
```

This function intentionally tries to create `AsyncioMetricsRecorder` without a running loop and catches
`AsyncioMetricsRecorderLoopUnavailableError`.

Second, the actual manual lifecycle happens inside `asyncio.run(main())`.

Inside that running event loop, the example:

1. creates `AsyncioMetricsRecorder`;
2. starts it;
3. checks `start_result`;
4. registers a metric;
5. emits three events;
6. stops the recorder;
7. checks `stop_result`;
8. reads snapshots.

## Running the example

From the `common` package directory, run:

```bash
python examples/metrics/manual_recorder_lifecycle.py
```

The output should show that recorder creation without a running loop fails first:

```text
recorder creation failed: no running asyncio event loop
```

Then it should print the collected snapshot:

```text
{
    "document_storage.save.attempts": {
        "name": "document_storage.save.attempts",
        "dimensions": {
            "total": 3,
            "success_total": 2,
            "failure_total": 1,
        },
    },
}
```

## Manual lifecycle responsibilities

When using `AsyncioMetricsRecorder` directly, the application is responsible for:

* providing the running event loop;
* creating the recorder inside that loop;
* starting the recorder;
* checking startup result;
* stopping the recorder;
* checking stop result.

This is why manual lifecycle belongs to advanced usage.

For regular application wiring, `MetricsRuntime` remains the simpler path.

## When not to use manual lifecycle

Do not use manual recorder lifecycle only to avoid `MetricsRuntime`.

Use `MetricsRuntime` when:

* the application does not want to own a recorder execution environment;
* the application does not already control a suitable running event loop;
* recorders should be created and managed centrally;
* recorder startup and shutdown should be handled by application-level runtime wiring;
* the application needs one runtime with many recorders.

Manual lifecycle is for cases where the application deliberately owns the recorder's event-loop environment.

## Summary

Manual recorder lifecycle gives direct control over `AsyncioMetricsRecorder`.

That control comes with responsibility.

The application must provide a running event loop, create the recorder inside that loop, start it, check lifecycle
results, and stop it explicitly.

For normal application usage, prefer `MetricsRuntime`.
