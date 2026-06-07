# tests/test_metrics/asyncio_metrics_recorder/test_metrics_recorder.py
from __future__ import annotations

from typing import Any, cast
from collections.abc import Callable, Mapping
from dataclasses import dataclass

import asyncio
import concurrent.futures
import threading
import time

import pytest

from mvx.common.logger.models import LogEvent
from mvx.common.logger.log_context import LogContext
from mvx.common.logger.log_payload_processor import LogPayloadProcessor

from mvx.common.metrics import (
    Metric,
    MetricEvent,
    AsyncioMetricsRecorderState,
    AsyncioMetricsRecorderDispatcherCancelledError,
    AsyncioMetricsRecorderError,
    AsyncioMetricsRecorderInvalidStateError,
    AsyncioMetricsRecorderLoopUnavailableError,
    AsyncioMetricsRecorderOnStartingHookFailedError,
    AsyncioMetricsRecorderQueueOverflowError,
    AsyncioMetricsRecorderOnStoppedHookFailedError,
    AsyncioMetricsRecorderUnexpectedError,
)


from mvx.common.metrics.asyncio_metrics_recorder import metrics_recorder as recorder_pack

TIMEOUT = 2.0


@dataclass(frozen=True, slots=True)
class UnrelatedMetricEvent(MetricEvent):
    value: int = 1

    @property
    def event_type(self) -> str:
        return "test.metric.event"


@dataclass(frozen=True, slots=True)
class OtherMetricEvent(MetricEvent):
    @property
    def event_type(self) -> str:
        return "test.other.event"


EVENT = UnrelatedMetricEvent()


class CountingMetric(Metric):
    def __init__(
        self,
        *,
        name: str = "test.metric",
        fail_on_handle: bool = False,
    ) -> None:
        self._name = name
        self._fail_on_handle = fail_on_handle
        self.total = 0
        self.handled_events: list[MetricEvent] = []

    @property
    def metric_name(self) -> str:
        return self._name

    def handle_event(self, event: MetricEvent) -> bool:
        if self._fail_on_handle:
            raise RuntimeError("metric handle failed")

        if not isinstance(event, UnrelatedMetricEvent):
            return False

        self.total += event.value
        self.handled_events.append(event)
        return True

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "total": self.total,
            },
        }


class RecordingMetricsRecorder(recorder_pack.AsyncioMetricsRecorder):
    def __init__(self, *, marker: str = "default", **kwargs: Any) -> None:
        super().__init__(**kwargs)

        self.marker = marker
        self.created_thread_id = threading.get_ident()
        self.created_loop = asyncio.get_running_loop()

        self.starting_entered = threading.Event()
        self.starting_finished = threading.Event()
        self.starting_release = asyncio.Event()
        self.block_starting = False
        self.starting_exception: Exception | None = None
        self.on_starting_count = 0

        self.stopped_entered = threading.Event()
        self.stopped_finished = threading.Event()
        self.stopped_release = asyncio.Event()
        self.block_stopped = False
        self.stopped_exception: Exception | None = None
        self.on_stopped_count = 0

        self.metric_changed_entered = threading.Event()
        self.metric_changed_finished = threading.Event()
        self.metric_changed_release = asyncio.Event()
        self.block_metric_changed = False
        self.metric_changed_exception: Exception | None = None
        self.metric_changed_count = 0
        self.changed_metrics: list[Metric] = []
        self.changed_events: list[MetricEvent] = []
        self.metric_changed_thread_ids: list[int] = []
        self.metric_changed_loops: list[asyncio.AbstractEventLoop] = []

    async def _on_starting(self) -> None:
        self.on_starting_count += 1
        self.starting_entered.set()

        if self.block_starting:
            await self.starting_release.wait()

        if self.starting_exception is not None:
            raise self.starting_exception

        self.starting_finished.set()

    async def _on_stopped(self) -> None:
        self.on_stopped_count += 1
        self.stopped_entered.set()

        if self.block_stopped:
            await self.stopped_release.wait()

        if self.stopped_exception is not None:
            raise self.stopped_exception

        self.stopped_finished.set()

    async def _on_metric_changed(
        self,
        *,
        metric: Metric,
        event: MetricEvent,
    ) -> None:
        self.metric_changed_count += 1
        self.metric_changed_entered.set()

        if self.block_metric_changed:
            await self.metric_changed_release.wait()

        if self.metric_changed_exception is not None:
            raise self.metric_changed_exception

        self.changed_metrics.append(metric)
        self.changed_events.append(event)
        self.metric_changed_thread_ids.append(threading.get_ident())
        self.metric_changed_loops.append(asyncio.get_running_loop())
        self.metric_changed_finished.set()


class RecordingLogContext:
    def __init__(self) -> None:
        self.error_events: list[dict[str, Any]] = []
        self.payload_errors: list[BaseException] = []

    def build_error_payload(self, exc: BaseException) -> Mapping[str, Any]:
        self.payload_errors.append(exc)
        return {
            "error_type": type(exc).__name__,
            "message": str(exc),
        }

    def log_error_event(
        self,
        *,
        event: str,
        payload: Mapping[str, Any],
        entity_id: str,
        skip_payload_normalization: bool,
    ) -> None:
        self.error_events.append(
            {
                "event": event,
                "payload": payload,
                "entity_id": entity_id,
                "skip_payload_normalization": skip_payload_normalization,
            }
        )


def wait_thread_event(event: threading.Event, timeout: float = TIMEOUT) -> None:
    assert event.wait(timeout), "threading.Event was not set in time"


async def wait_thread_event_async(
    event: threading.Event,
    timeout: float = TIMEOUT,
) -> None:
    await asyncio.wait_for(
        asyncio.to_thread(event.wait, timeout),
        timeout=timeout + 0.1,
    )

    assert event.is_set(), "threading.Event was not set in time"


async def wait_until(
    predicate: Callable[[], bool],
    *,
    timeout: float = TIMEOUT,
    interval: float = 0.005,
) -> None:
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if predicate():
            return

        await asyncio.sleep(interval)

    raise AssertionError("condition was not satisfied in time")


async def run_in_thread_async(
    func: Callable[[], Any],
    *,
    timeout: float = TIMEOUT,
) -> Any:
    return await asyncio.wait_for(
        asyncio.to_thread(func),
        timeout=timeout,
    )


def run_many_threads(
    count: int,
    func: Callable[[int], Any],
    *,
    timeout: float = TIMEOUT,
) -> list[Any]:
    start = threading.Barrier(count + 1)

    def wrapped(index: int) -> Any:
        start.wait(timeout=timeout)
        return func(index)

    with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
        futures = [pool.submit(wrapped, index) for index in range(count)]
        start.wait(timeout=timeout)
        return [future.result(timeout=timeout) for future in futures]


async def run_many_threads_async(
    count: int,
    func: Callable[[int], Any],
    *,
    timeout: float = TIMEOUT,
) -> list[Any]:
    return await asyncio.wait_for(
        asyncio.to_thread(run_many_threads, count, func, timeout=timeout),
        timeout=timeout + 0.5,
    )


def make_domain_error() -> AsyncioMetricsRecorderError:
    return AsyncioMetricsRecorderInvalidStateError(
        recorder_state=AsyncioMetricsRecorderState.RUNNING,
        expected_states=(AsyncioMetricsRecorderState.VIRGIN,),
    )


# -------------------------
# Group a: constructor and basic invariants
# -------------------------


@pytest.mark.asyncio
async def test_a01_constructor_inside_running_loop_creates_virgin_recorder() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    assert recorder.entity_id == "recorder-1"
    assert recorder.get_status() is AsyncioMetricsRecorderState.VIRGIN
    assert recorder._dispatcher is None
    assert recorder._pending_counter == 0
    assert recorder._last_error is None
    assert recorder._metrics_by_name == {}


def test_a02_constructor_outside_running_loop_fails() -> None:
    with pytest.raises(AsyncioMetricsRecorderLoopUnavailableError):
        RecordingMetricsRecorder(entity_id="recorder-1")


@pytest.mark.asyncio
async def test_a03_constructor_strips_entity_id() -> None:
    recorder = RecordingMetricsRecorder(entity_id="  recorder-1  ")

    assert recorder.entity_id == "recorder-1"


@pytest.mark.asyncio
async def test_a04_constructor_generates_entity_id_when_blank() -> None:
    recorder = RecordingMetricsRecorder(entity_id="   ")

    assert recorder.entity_id


@pytest.mark.asyncio
async def test_a05_constructor_stores_creation_loop() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    assert recorder.created_loop is asyncio.get_running_loop()


@pytest.mark.asyncio
async def test_a06_constructor_stores_creation_thread() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    assert recorder.created_thread_id == threading.get_ident()


@pytest.mark.asyncio
async def test_a07_default_queue_limit_is_used_when_queue_max_size_is_none() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    assert recorder._max_pending_counter == recorder_pack.DEFAULT_QUEUE_MAX_SIZE


@pytest.mark.asyncio
async def test_a08_custom_queue_limit_is_used() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1", queue_max_size=3)

    assert recorder._max_pending_counter == 3


@pytest.mark.asyncio
async def test_a09_default_overflow_policy_is_raise_error() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    assert (
        recorder._queue_overflow_policy
        is recorder_pack.AsyncioMetricsRecorderQueueOverflowPolicy.RAISE_ERROR
    )


@pytest.mark.asyncio
async def test_a10_custom_overflow_policy_is_stored() -> None:
    recorder = RecordingMetricsRecorder(
        entity_id="recorder-1",
        queue_overflow_policy=recorder_pack.AsyncioMetricsRecorderQueueOverflowPolicy.DROP,
    )

    assert (
        recorder._queue_overflow_policy
        is recorder_pack.AsyncioMetricsRecorderQueueOverflowPolicy.DROP
    )


@pytest.mark.asyncio
async def test_a11_namespace_default_is_used() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    assert recorder._namespace == recorder_pack.DEFAULT_NAMESPACE


@pytest.mark.asyncio
async def test_a12_custom_namespace_is_used() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1", namespace="test.metrics")

    assert recorder._namespace == "test.metrics"


@pytest.mark.asyncio
async def test_a13_constructor_rejects_none_entity_id() -> None:
    with pytest.raises(ValueError, match="entity_id"):
        RecordingMetricsRecorder(entity_id=cast(Any, None))


@pytest.mark.asyncio
async def test_a14_constructor_rejects_non_string_entity_id() -> None:
    with pytest.raises(TypeError, match="entity_id"):
        RecordingMetricsRecorder(entity_id=cast(Any, 123))


@pytest.mark.asyncio
async def test_a15_constructor_rejects_non_string_namespace() -> None:
    with pytest.raises(TypeError, match="namespace"):
        RecordingMetricsRecorder(
            entity_id="recorder-1",
            namespace=cast(Any, 123),
        )


@pytest.mark.asyncio
async def test_a16_constructor_rejects_bool_queue_max_size() -> None:
    with pytest.raises(TypeError, match="queue_max_size"):
        RecordingMetricsRecorder(
            entity_id="recorder-1",
            queue_max_size=cast(Any, True),
        )


@pytest.mark.asyncio
async def test_a17_constructor_rejects_non_integer_queue_max_size() -> None:
    with pytest.raises(TypeError, match="queue_max_size"):
        RecordingMetricsRecorder(
            entity_id="recorder-1",
            queue_max_size=cast(Any, 1.5),
        )


@pytest.mark.asyncio
async def test_a18_constructor_rejects_zero_queue_max_size() -> None:
    with pytest.raises(ValueError, match="queue_max_size"):
        RecordingMetricsRecorder(
            entity_id="recorder-1",
            queue_max_size=0,
        )


@pytest.mark.asyncio
async def test_a19_constructor_rejects_negative_queue_max_size() -> None:
    with pytest.raises(ValueError, match="queue_max_size"):
        RecordingMetricsRecorder(
            entity_id="recorder-1",
            queue_max_size=-1,
        )


@pytest.mark.asyncio
async def test_a20_constructor_rejects_invalid_queue_overflow_policy() -> None:
    with pytest.raises(TypeError, match="queue_overflow_policy"):
        RecordingMetricsRecorder(
            entity_id="recorder-1",
            queue_overflow_policy=cast(Any, "DROP"),
        )


@pytest.mark.asyncio
async def test_a21_constructor_rejects_invalid_log_context() -> None:
    with pytest.raises(TypeError, match="log_context"):
        RecordingMetricsRecorder(
            entity_id="recorder-1",
            log_context=cast(Any, object()),
        )


# -------------------------
# Group b: wait handle
# -------------------------


def test_b01_wait_returns_success_result_after_done_none() -> None:
    handle = recorder_pack._WaitHandleInternal(recorder_pack.AsyncioMetricsRecorderOp.START)

    handle.done(None)

    result = handle.wait()

    assert result.success is True
    assert result.error is None
    assert result.op_name is recorder_pack.AsyncioMetricsRecorderOp.START


def test_b02_wait_returns_domain_error_as_is() -> None:
    handle = recorder_pack._WaitHandleInternal(recorder_pack.AsyncioMetricsRecorderOp.START)
    error = make_domain_error()

    handle.done(error)

    result = handle.wait()

    assert result.success is False
    assert result.error is error


def test_b03_wait_wraps_ordinary_exception() -> None:
    handle = recorder_pack._WaitHandleInternal(recorder_pack.AsyncioMetricsRecorderOp.START)

    handle.done(RuntimeError("boom"))

    result = handle.wait()

    assert result.success is False
    assert isinstance(result.error, AsyncioMetricsRecorderUnexpectedError)


@pytest.mark.asyncio
async def test_b04_await_handle_returns_success_result() -> None:
    handle = recorder_pack._WaitHandleInternal(recorder_pack.AsyncioMetricsRecorderOp.STOP)

    handle.done(None)

    result = await handle

    assert result.success is True
    assert result.error is None
    assert result.op_name is recorder_pack.AsyncioMetricsRecorderOp.STOP


@pytest.mark.asyncio
async def test_b05_await_handle_cancellation_does_not_cancel_internal_future() -> None:
    handle = recorder_pack._WaitHandleInternal(recorder_pack.AsyncioMetricsRecorderOp.START)

    async def waiter() -> None:
        await handle

    task = asyncio.create_task(waiter())

    await asyncio.sleep(0)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert not handle._future.cancelled()

    handle.done(None)

    assert handle.wait().success is True


def test_b06_done_ignores_repeated_completion() -> None:
    handle = recorder_pack._WaitHandleInternal(recorder_pack.AsyncioMetricsRecorderOp.START)

    handle.done(None)
    handle.done(RuntimeError("late error"))

    result = handle.wait()

    assert result.success is True
    assert result.error is None


# -------------------------
# Group c: start lifecycle
# -------------------------


@pytest.mark.asyncio
async def test_c01_start_from_virgin_succeeds() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    outcome = await recorder.start()

    assert outcome.success is True
    assert recorder.get_status() is AsyncioMetricsRecorderState.RUNNING
    assert recorder.on_starting_count == 1
    assert recorder._dispatcher is not None
    assert not recorder._dispatcher.done()

    await recorder.stop()


@pytest.mark.asyncio
async def test_c02_start_returns_handle_immediately_while_starting() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    recorder.block_starting = True

    handle = recorder.start()

    await wait_thread_event_async(recorder.starting_entered)

    assert recorder.get_status() is AsyncioMetricsRecorderState.STARTING
    assert not handle._future.done()

    recorder.starting_release.set()

    assert (await handle).success is True

    await recorder.stop()


@pytest.mark.asyncio
async def test_c03_start_from_starting_joins_existing_start() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    recorder.block_starting = True

    h1 = recorder.start()

    await wait_thread_event_async(recorder.starting_entered)

    h2 = recorder.start()

    recorder.starting_release.set()

    assert (await h1).success is True
    assert (await h2).success is True
    assert recorder.on_starting_count == 1
    assert recorder.get_status() is AsyncioMetricsRecorderState.RUNNING

    await recorder.stop()


@pytest.mark.asyncio
async def test_c04_start_from_running_fails() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    assert (await recorder.start()).success is True

    outcome = await recorder.start()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioMetricsRecorderInvalidStateError)
    assert recorder.get_status() is AsyncioMetricsRecorderState.RUNNING

    await recorder.stop()


@pytest.mark.asyncio
async def test_c05_start_hook_ordinary_exception_becomes_on_starting_hook_failed() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    recorder.starting_exception = RuntimeError("boom")

    outcome = await recorder.start()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioMetricsRecorderOnStartingHookFailedError)
    assert recorder.get_status() is AsyncioMetricsRecorderState.FAILURE
    assert recorder._last_error is outcome.error
    assert recorder._dispatcher is None


@pytest.mark.asyncio
async def test_c06_start_future_is_cleared_after_successful_start() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    assert (await recorder.start()).success is True
    assert recorder._start_future is None

    await recorder.stop()


@pytest.mark.asyncio
async def test_c07_dispatcher_task_name_uses_namespace() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1", namespace="test.metrics")

    await recorder.start()

    assert recorder._dispatcher is not None
    assert recorder._dispatcher.get_name() == "test.metrics.dispatching_loop"

    await recorder.stop()


@pytest.mark.asyncio
async def test_c08_base_on_starting_hook_is_noop() -> None:
    recorder = recorder_pack.AsyncioMetricsRecorder(entity_id="recorder-1")

    outcome = await recorder.start()

    assert outcome.success is True
    assert recorder.get_status() is AsyncioMetricsRecorderState.RUNNING

    await recorder.stop()


# -------------------------
# Group d: stop lifecycle
# -------------------------


@pytest.mark.asyncio
async def test_d01_stop_from_running_succeeds() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    await recorder.start()

    outcome = await recorder.stop()

    assert outcome.success is True
    assert recorder.get_status() is AsyncioMetricsRecorderState.STOPPED
    assert recorder._dispatcher is None
    assert recorder.on_stopped_count == 1
    assert recorder._stop_future is None
    assert recorder._last_error is None


@pytest.mark.asyncio
async def test_d02_stop_from_virgin_fails() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    outcome = await recorder.stop()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioMetricsRecorderInvalidStateError)
    assert recorder.get_status() is AsyncioMetricsRecorderState.VIRGIN
    assert recorder._stop_future is None


@pytest.mark.asyncio
async def test_d03_stop_from_stopping_joins_existing_stop() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    await recorder.start()

    recorder.block_stopped = True

    h1 = recorder.stop()

    await wait_thread_event_async(recorder.stopped_entered)

    h2 = recorder.stop()

    assert recorder.get_status() is AsyncioMetricsRecorderState.STOPPING
    assert h1 is not h2

    recorder.stopped_release.set()

    outcome1 = await h1
    outcome2 = await h2

    assert outcome1.success is True
    assert outcome2.success is True
    assert recorder.on_stopped_count == 1
    assert recorder.get_status() is AsyncioMetricsRecorderState.STOPPED
    assert recorder._stop_future is None


@pytest.mark.asyncio
async def test_d04_on_stopped_ordinary_exception_becomes_stopped_hook_failed() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    await recorder.start()

    recorder.stopped_exception = RuntimeError("boom")

    outcome = await recorder.stop()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioMetricsRecorderOnStoppedHookFailedError)
    assert recorder.get_status() is AsyncioMetricsRecorderState.FAILURE
    assert recorder._last_error is outcome.error


@pytest.mark.asyncio
async def test_d05_normal_stop_cancellation_of_dispatcher_does_not_mark_cancelled() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    await recorder.start()

    outcome = await recorder.stop()

    assert outcome.success is True
    assert recorder.get_status() is AsyncioMetricsRecorderState.STOPPED
    assert not isinstance(recorder._last_error, AsyncioMetricsRecorderDispatcherCancelledError)


@pytest.mark.asyncio
async def test_d06_stop_succeeds_when_dispatcher_is_already_done() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    recorder.register_metric(CountingMetric(fail_on_handle=True))

    await recorder.start()

    recorder.register_event(EVENT)

    await wait_until(lambda: recorder._dispatcher is not None and recorder._dispatcher.done())

    with recorder._thread_lock:
        recorder._state = AsyncioMetricsRecorderState.RUNNING

    outcome = await recorder.stop()

    assert outcome.success is True
    assert recorder._dispatcher is None


# -------------------------
# Group e: metric registration and snapshots
# -------------------------


@pytest.mark.asyncio
async def test_e01_register_metric_in_virgin_registers_metric() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()

    recorder.register_metric(metric)

    assert list(recorder.iter_metrics()) == [metric]
    assert recorder.get_metric_snapshots() == {
        "test.metric": {
            "name": "test.metric",
            "dimensions": {
                "total": 0,
            },
        }
    }


@pytest.mark.asyncio
async def test_e02_register_metric_replaces_metric_with_same_name() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    # noinspection PyArgumentEqualDefault
    first = CountingMetric(name="test.metric")
    # noinspection PyArgumentEqualDefault
    second = CountingMetric(name="test.metric")

    recorder.register_metric(first)
    recorder.register_metric(second)

    assert list(recorder.iter_metrics()) == [second]


@pytest.mark.asyncio
async def test_e03_register_metric_rejects_none() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    with pytest.raises(ValueError, match="metric"):
        recorder.register_metric(cast(Any, None))


@pytest.mark.asyncio
async def test_e04_register_metric_rejects_non_metric() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    with pytest.raises(TypeError, match="metric"):
        recorder.register_metric(cast(Any, object()))


@pytest.mark.asyncio
async def test_e05_register_metric_from_another_thread_registers_metric() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()

    await run_in_thread_async(lambda: recorder.register_metric(metric))

    assert list(recorder.iter_metrics()) == [metric]


@pytest.mark.asyncio
async def test_e06_register_metric_in_stopped_state_fails() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    await recorder.start()
    await recorder.stop()

    with pytest.raises(AsyncioMetricsRecorderInvalidStateError):
        recorder.register_metric(CountingMetric())


@pytest.mark.asyncio
async def test_e07_register_metric_in_starting_state_succeeds() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    recorder.block_starting = True
    metric = CountingMetric()

    start_handle = recorder.start()

    await wait_thread_event_async(recorder.starting_entered)

    recorder.register_metric(metric)

    assert list(recorder.iter_metrics()) == [metric]

    recorder.starting_release.set()

    assert (await start_handle).success is True

    await recorder.stop()


@pytest.mark.asyncio
async def test_e08_register_metric_in_running_state_succeeds() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()

    await recorder.start()

    recorder.register_metric(metric)

    assert list(recorder.iter_metrics()) == [metric]

    await recorder.stop()


@pytest.mark.asyncio
async def test_e09_register_metric_in_failure_state_fails() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    recorder.starting_exception = RuntimeError("boom")

    await recorder.start()

    assert recorder.get_status() is AsyncioMetricsRecorderState.FAILURE

    with pytest.raises(AsyncioMetricsRecorderInvalidStateError):
        recorder.register_metric(CountingMetric())


@pytest.mark.asyncio
async def test_e10_register_metric_in_cancelled_state_fails() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    await recorder.start()

    assert recorder._dispatcher is not None

    recorder._dispatcher.cancel()

    await wait_until(lambda: recorder.get_status() is AsyncioMetricsRecorderState.CANCELLED)

    with pytest.raises(AsyncioMetricsRecorderInvalidStateError):
        recorder.register_metric(CountingMetric())


@pytest.mark.asyncio
async def test_e11_get_metric_snapshots_reflects_dispatched_events() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()
    recorder.register_metric(metric)

    await recorder.start()

    recorder.register_event(EVENT)

    await wait_until(lambda: metric.total == 1)

    assert recorder.get_metric_snapshots() == {
        "test.metric": {
            "name": "test.metric",
            "dimensions": {
                "total": 1,
            },
        }
    }

    await recorder.stop()


@pytest.mark.asyncio
async def test_e12_get_metric_snapshots_from_another_thread() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()
    recorder.register_metric(metric)

    await recorder.start()

    recorder.register_event(EVENT)

    await wait_until(lambda: metric.total == 1)

    snapshots = await run_in_thread_async(recorder.get_metric_snapshots)

    assert snapshots == {
        "test.metric": {
            "name": "test.metric",
            "dimensions": {
                "total": 1,
            },
        }
    }

    await recorder.stop()


@pytest.mark.asyncio
async def test_e13_iter_metrics_from_another_thread() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()

    recorder.register_metric(metric)

    metrics = await run_in_thread_async(lambda: tuple(recorder.iter_metrics()))

    assert metrics == (metric,)


# -------------------------
# Group f: event acceptance and dispatch
# -------------------------


@pytest.mark.asyncio
async def test_f01_register_event_in_virgin_accepts_event_and_triggers_start() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()

    recorder.register_metric(metric)
    recorder.register_event(EVENT)

    await wait_until(lambda: recorder.get_status() is AsyncioMetricsRecorderState.RUNNING)
    await wait_until(lambda: metric.total == 1)

    assert recorder.on_starting_count == 1
    assert recorder._pending_counter == 0

    await recorder.stop()


@pytest.mark.asyncio
async def test_f02_register_event_in_starting_accepts_and_buffers_until_dispatcher_starts() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()
    recorder.register_metric(metric)
    recorder.block_starting = True

    start_handle = recorder.start()

    await wait_thread_event_async(recorder.starting_entered)

    recorder.register_event(EVENT)

    assert recorder.get_status() is AsyncioMetricsRecorderState.STARTING
    assert recorder._pending_counter == 1
    assert metric.total == 0

    recorder.starting_release.set()

    assert (await start_handle).success is True

    await wait_until(lambda: metric.total == 1)

    assert recorder._pending_counter == 0
    assert recorder.get_status() is AsyncioMetricsRecorderState.RUNNING

    await recorder.stop()


@pytest.mark.asyncio
async def test_f03_register_event_in_running_accepts_event() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()
    recorder.register_metric(metric)

    await recorder.start()

    recorder.register_event(EVENT)

    await wait_until(lambda: metric.total == 1)

    assert recorder.get_status() is AsyncioMetricsRecorderState.RUNNING
    assert recorder._pending_counter == 0

    await recorder.stop()


@pytest.mark.asyncio
async def test_f04_register_event_rejects_none() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    with pytest.raises(ValueError, match="event"):
        recorder.register_event(cast(Any, None))


@pytest.mark.asyncio
async def test_f05_register_event_rejects_non_metric_event() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    with pytest.raises(TypeError, match="event"):
        recorder.register_event(cast(Any, object()))


@pytest.mark.asyncio
async def test_f06_register_event_in_stopped_fails() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    await recorder.start()
    await recorder.stop()

    with pytest.raises(AsyncioMetricsRecorderInvalidStateError):
        recorder.register_event(EVENT)


@pytest.mark.asyncio
async def test_f07_unrelated_event_does_not_trigger_metric_changed_hook() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()
    recorder.register_metric(metric)

    await recorder.start()

    recorder.register_event(OtherMetricEvent())

    await wait_until(lambda: recorder._pending_counter == 0)

    assert metric.total == 0
    assert recorder.metric_changed_count == 0

    await recorder.stop()


@pytest.mark.asyncio
async def test_f08_metric_changed_hook_receives_metric_and_event() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()
    recorder.register_metric(metric)

    await recorder.start()

    recorder.register_event(EVENT)

    await wait_thread_event_async(recorder.metric_changed_finished)

    assert recorder.changed_metrics == [metric]
    assert recorder.changed_events == [EVENT]
    assert recorder.metric_changed_thread_ids == [threading.get_ident()]
    assert recorder.metric_changed_loops == [asyncio.get_running_loop()]

    await recorder.stop()


@pytest.mark.asyncio
async def test_f09_base_on_metric_changed_hook_is_noop() -> None:
    recorder = recorder_pack.AsyncioMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()

    recorder.register_metric(metric)

    await recorder.start()

    recorder.register_event(EVENT)

    await wait_until(lambda: metric.total == 1)

    assert recorder.get_metric_snapshots() == {
        "test.metric": {
            "name": "test.metric",
            "dimensions": {
                "total": 1,
            },
        }
    }

    await recorder.stop()


# -------------------------
# Group g: queue / pending counter / overflow
# -------------------------


@pytest.mark.asyncio
async def test_g01_pending_counter_increments_before_dispatch_finishes() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()
    recorder.register_metric(metric)
    recorder.block_metric_changed = True

    await recorder.start()

    recorder.register_event(EVENT)

    await wait_thread_event_async(recorder.metric_changed_entered)

    assert recorder._pending_counter == 1

    recorder.metric_changed_release.set()

    await wait_until(lambda: recorder._pending_counter == 0)

    await recorder.stop()


@pytest.mark.asyncio
async def test_g02_pending_counter_decrements_after_successful_dispatch() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()
    recorder.register_metric(metric)

    await recorder.start()

    recorder.register_event(EVENT)

    await wait_until(lambda: metric.total == 1)

    assert recorder._pending_counter == 0

    await recorder.stop()


@pytest.mark.asyncio
async def test_g03_pending_counter_decrements_after_dispatch_core_raises() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    recorder.register_metric(CountingMetric(fail_on_handle=True))

    await recorder.start()

    recorder.register_event(EVENT)

    await wait_until(lambda: recorder.get_status() is AsyncioMetricsRecorderState.FAILURE)

    assert recorder._pending_counter == 0


@pytest.mark.asyncio
async def test_g04_overflow_raise_error_raises_when_pending_limit_reached() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1", queue_max_size=1)
    metric = CountingMetric()
    recorder.register_metric(metric)
    recorder.block_metric_changed = True

    await recorder.start()

    recorder.register_event(EVENT)

    await wait_thread_event_async(recorder.metric_changed_entered)

    with pytest.raises(AsyncioMetricsRecorderQueueOverflowError):
        recorder.register_event(EVENT)

    recorder.metric_changed_release.set()

    await wait_until(lambda: recorder._pending_counter == 0)

    await recorder.stop()


@pytest.mark.asyncio
async def test_g05_overflow_drop_silently_returns() -> None:
    recorder = RecordingMetricsRecorder(
        entity_id="recorder-1",
        queue_max_size=1,
        queue_overflow_policy=recorder_pack.AsyncioMetricsRecorderQueueOverflowPolicy.DROP,
    )
    metric = CountingMetric()
    recorder.register_metric(metric)
    recorder.block_metric_changed = True

    await recorder.start()

    recorder.register_event(EVENT)

    await wait_thread_event_async(recorder.metric_changed_entered)

    recorder.register_event(EVENT)

    assert recorder._pending_counter == 1

    recorder.metric_changed_release.set()

    await wait_until(lambda: recorder._pending_counter == 0)
    assert metric.total == 1

    await recorder.stop()


@pytest.mark.asyncio
async def test_g06_register_event_rolls_back_pending_counter_when_scheduling_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    recorder.register_metric(CountingMetric())

    await recorder.start()

    original_call_soon_threadsafe = recorder._loop.call_soon_threadsafe

    def raise_runtime_error(
        callback: Callable[..., Any],
        *args: Any,
        context: Any = None,
    ) -> None:
        _ = callback, args, context
        raise RuntimeError("loop rejected scheduling")

    monkeypatch.setattr(
        recorder._loop,
        "call_soon_threadsafe",
        raise_runtime_error,
    )

    with pytest.raises(RuntimeError, match="loop rejected scheduling"):
        recorder.register_event(EVENT)

    assert recorder._pending_counter == 0

    monkeypatch.setattr(
        recorder._loop,
        "call_soon_threadsafe",
        original_call_soon_threadsafe,
    )

    await recorder.stop()


# -------------------------
# Group h: dispatcher failure and cleanup
# -------------------------


@pytest.mark.asyncio
async def test_h01_dispatch_core_exception_stops_dispatcher() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    recorder.register_metric(CountingMetric(fail_on_handle=True))

    await recorder.start()

    recorder.register_event(EVENT)

    await wait_until(lambda: recorder.get_status() is AsyncioMetricsRecorderState.FAILURE)

    assert recorder._dispatcher is not None
    assert recorder._dispatcher.done()


@pytest.mark.asyncio
async def test_h02_dispatch_core_exception_is_mapped_to_unexpected_error() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    recorder.register_metric(CountingMetric(fail_on_handle=True))

    await recorder.start()

    recorder.register_event(EVENT)

    await wait_until(lambda: recorder.get_status() is AsyncioMetricsRecorderState.FAILURE)

    assert isinstance(recorder._last_error, AsyncioMetricsRecorderUnexpectedError)


@pytest.mark.asyncio
async def test_h03_dispatcher_external_cancel_outside_stopping_sets_cancelled() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    await recorder.start()

    assert recorder._dispatcher is not None

    recorder._dispatcher.cancel()

    await wait_until(lambda: recorder.get_status() is AsyncioMetricsRecorderState.CANCELLED)

    assert isinstance(recorder._last_error, AsyncioMetricsRecorderDispatcherCancelledError)


@pytest.mark.asyncio
async def test_h04_cleanup_runs_after_dispatch_failure() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    recorder.register_metric(CountingMetric(fail_on_handle=True))

    await recorder.start()

    recorder.register_event(EVENT)

    await wait_until(lambda: recorder.get_status() is AsyncioMetricsRecorderState.FAILURE)
    await wait_until(lambda: recorder.on_stopped_count == 1)

    assert isinstance(recorder._last_error, AsyncioMetricsRecorderUnexpectedError)


@pytest.mark.asyncio
async def test_h05_dispatching_task_done_ignores_successful_task() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    async def successful_task() -> None:
        return None

    task = asyncio.create_task(successful_task())
    await task

    recorder._on_dispatching_task_done(task)

    assert recorder.get_status() is AsyncioMetricsRecorderState.VIRGIN


@pytest.mark.asyncio
async def test_h06_dispatching_task_done_ignores_exception_while_stopping() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    async def failed_task() -> None:
        raise RuntimeError("dispatcher failed")

    task = asyncio.create_task(failed_task())
    await asyncio.wait({task})

    with recorder._thread_lock:
        recorder._state = AsyncioMetricsRecorderState.STOPPING

    recorder._on_dispatching_task_done(task)

    assert recorder.get_status() is AsyncioMetricsRecorderState.STOPPING
    assert recorder._last_error is None


@pytest.mark.asyncio
async def test_h07_metric_changed_hook_exception_stops_dispatcher() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    recorder.metric_changed_exception = RuntimeError("metric changed failed")
    recorder.register_metric(CountingMetric())

    await recorder.start()

    recorder.register_event(EVENT)

    await wait_until(lambda: recorder.get_status() is AsyncioMetricsRecorderState.FAILURE)

    assert recorder._dispatcher is not None
    assert recorder._dispatcher.done()
    assert isinstance(recorder._last_error, AsyncioMetricsRecorderUnexpectedError)
    assert recorder._pending_counter == 0


# -------------------------
# Group i: flush semantics
# -------------------------


@pytest.mark.asyncio
async def test_i01_stop_flushes_already_queued_events() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()
    recorder.register_metric(metric)

    await recorder.start()

    for _ in range(10):
        recorder.register_event(EVENT)

    outcome = await recorder.stop()

    assert outcome.success is True
    assert metric.total == 10
    assert recorder._pending_counter == 0
    assert recorder.get_status() is AsyncioMetricsRecorderState.STOPPED


@pytest.mark.asyncio
async def test_i02_stop_waits_for_currently_running_metric_changed_hook() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()
    recorder.register_metric(metric)
    recorder.block_metric_changed = True

    await recorder.start()

    recorder.register_event(EVENT)

    await wait_thread_event_async(recorder.metric_changed_entered)

    stop_handle = recorder.stop()

    await asyncio.sleep(0.05)

    assert not stop_handle._future.done()
    assert recorder.get_status() is AsyncioMetricsRecorderState.STOPPING

    recorder.metric_changed_release.set()

    outcome = await stop_handle

    assert outcome.success is True
    assert metric.total == 1
    assert recorder._pending_counter == 0
    assert recorder.get_status() is AsyncioMetricsRecorderState.STOPPED


@pytest.mark.asyncio
async def test_i03_flush_returns_when_dispatcher_is_done() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    recorder.register_metric(CountingMetric(fail_on_handle=True))

    await recorder.start()

    recorder.register_event(EVENT)

    await wait_until(lambda: recorder._dispatcher is not None and recorder._dispatcher.done())

    await recorder._flush_core()


# -------------------------
# Group j: restart semantics
# -------------------------


@pytest.mark.asyncio
async def test_j01_start_after_stopped_succeeds() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    await recorder.start()
    await recorder.stop()

    outcome = await recorder.start()

    assert outcome.success is True
    assert recorder.get_status() is AsyncioMetricsRecorderState.RUNNING
    assert recorder.on_starting_count == 2
    assert recorder._dispatcher is not None
    assert not recorder._dispatcher.done()

    await recorder.stop()


@pytest.mark.asyncio
async def test_j02_metric_registry_survives_restart() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()
    recorder.register_metric(metric)

    await recorder.start()
    await recorder.stop()
    await recorder.start()

    recorder.register_event(EVENT)

    await wait_until(lambda: metric.total == 1)

    await recorder.stop()


@pytest.mark.asyncio
async def test_i04_stop_flushes_events_waiting_behind_blocked_dispatch() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()
    recorder.register_metric(metric)
    recorder.block_metric_changed = True

    await recorder.start()

    recorder.register_event(EVENT)
    recorder.register_event(EVENT)
    recorder.register_event(EVENT)

    await wait_thread_event_async(recorder.metric_changed_entered)

    stop_handle = recorder.stop()

    await asyncio.sleep(0)

    assert not stop_handle._future.done()
    assert recorder.get_status() is AsyncioMetricsRecorderState.STOPPING

    recorder.metric_changed_release.set()

    outcome = await stop_handle

    assert outcome.success is True
    assert metric.total == 3
    assert recorder._pending_counter == 0
    assert recorder.get_status() is AsyncioMetricsRecorderState.STOPPED


# -------------------------
# Group k: cross-thread usage
# -------------------------


@pytest.mark.asyncio
async def test_k01_start_can_be_called_from_another_thread() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    result = await run_in_thread_async(lambda: recorder.start().wait())

    assert result.success is True
    assert recorder.get_status() is AsyncioMetricsRecorderState.RUNNING

    await recorder.stop()


@pytest.mark.asyncio
async def test_k02_stop_can_be_called_from_another_thread() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    await recorder.start()

    result = await run_in_thread_async(lambda: recorder.stop().wait())

    assert result.success is True
    assert recorder.get_status() is AsyncioMetricsRecorderState.STOPPED


@pytest.mark.asyncio
async def test_k03_register_metric_can_be_called_from_another_thread() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()

    await run_in_thread_async(lambda: recorder.register_metric(metric))

    assert list(recorder.iter_metrics()) == [metric]


@pytest.mark.asyncio
async def test_k04_register_event_can_be_called_from_another_thread_when_running() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()
    recorder.register_metric(metric)

    await recorder.start()

    await run_in_thread_async(lambda: recorder.register_event(EVENT))

    await wait_until(lambda: metric.total == 1)

    assert recorder._pending_counter == 0

    await recorder.stop()


@pytest.mark.asyncio
async def test_k05_many_threads_call_register_event_concurrently() -> None:
    thread_count = 10
    events_per_thread = 20

    recorder = RecordingMetricsRecorder(
        entity_id="recorder-1",
        queue_max_size=thread_count * events_per_thread,
    )
    metric = CountingMetric()
    recorder.register_metric(metric)

    await recorder.start()

    def worker(_: int) -> None:
        for _event_index in range(events_per_thread):
            recorder.register_event(EVENT)

    await run_many_threads_async(thread_count, worker)

    await wait_until(
        lambda: metric.total == thread_count * events_per_thread,
        timeout=5.0,
    )

    assert recorder._pending_counter == 0

    await recorder.stop()


@pytest.mark.asyncio
async def test_k06_register_event_from_another_thread_while_starting_is_buffered() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    metric = CountingMetric()
    recorder.register_metric(metric)
    recorder.block_starting = True

    start_handle = recorder.start()

    await wait_thread_event_async(recorder.starting_entered)

    await run_in_thread_async(lambda: recorder.register_event(EVENT))

    assert recorder.get_status() is AsyncioMetricsRecorderState.STARTING
    assert recorder._pending_counter == 1
    assert metric.total == 0

    recorder.starting_release.set()

    assert (await start_handle).success is True

    await wait_until(lambda: metric.total == 1)

    assert recorder._pending_counter == 0

    await recorder.stop()


@pytest.mark.asyncio
async def test_k07_many_threads_call_start_concurrently_join_single_start() -> None:
    thread_count = 10

    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    recorder.block_starting = True

    async def run_starters() -> list[Any]:
        return await run_many_threads_async(
            thread_count,
            lambda _index: recorder.start().wait(),
        )

    starters_task = asyncio.create_task(run_starters())

    await wait_thread_event_async(recorder.starting_entered)

    recorder.starting_release.set()

    results = await starters_task

    assert all(result.success is True for result in results)
    assert recorder.on_starting_count == 1
    assert recorder.get_status() is AsyncioMetricsRecorderState.RUNNING

    await recorder.stop()


@pytest.mark.asyncio
async def test_k08_many_threads_call_stop_concurrently_join_single_stop() -> None:
    thread_count = 10

    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    await recorder.start()

    recorder.block_stopped = True

    async def run_stoppers() -> list[Any]:
        return await run_many_threads_async(
            thread_count,
            lambda _index: recorder.stop().wait(),
        )

    stoppers_task = asyncio.create_task(run_stoppers())

    await wait_thread_event_async(recorder.stopped_entered)

    recorder.stopped_release.set()

    results = await stoppers_task

    assert all(result.success is True for result in results)
    assert recorder.on_stopped_count == 1
    assert recorder.get_status() is AsyncioMetricsRecorderState.STOPPED


@pytest.mark.asyncio
async def test_k09_register_metric_from_another_thread_while_starting_succeeds() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    recorder.block_starting = True
    metric = CountingMetric()

    start_handle = recorder.start()

    await wait_thread_event_async(recorder.starting_entered)

    await run_in_thread_async(lambda: recorder.register_metric(metric))

    assert list(recorder.iter_metrics()) == [metric]

    recorder.starting_release.set()

    assert (await start_handle).success is True

    await recorder.stop()


# -------------------------
# Group l: state / error consistency
# -------------------------


@pytest.mark.asyncio
async def test_l01_successful_start_clears_last_error() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    outcome = await recorder.start()

    assert outcome.success is True
    assert recorder.get_status() is AsyncioMetricsRecorderState.RUNNING
    assert recorder._last_error is None

    await recorder.stop()


@pytest.mark.asyncio
async def test_l02_successful_stop_does_not_set_last_error() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    await recorder.start()

    outcome = await recorder.stop()

    assert outcome.success is True
    assert recorder.get_status() is AsyncioMetricsRecorderState.STOPPED
    assert recorder._last_error is None


@pytest.mark.asyncio
async def test_l03_failure_state_is_sticky_for_start_stop_and_register_event() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")
    recorder.starting_exception = RuntimeError("boom")

    await recorder.start()

    assert recorder.get_status() is AsyncioMetricsRecorderState.FAILURE

    start_outcome = await recorder.start()
    stop_outcome = await recorder.stop()

    assert start_outcome.success is False
    assert stop_outcome.success is False

    with pytest.raises(AsyncioMetricsRecorderInvalidStateError):
        recorder.register_event(EVENT)

    assert recorder.get_status() is AsyncioMetricsRecorderState.FAILURE


@pytest.mark.asyncio
async def test_l04_cancelled_state_is_sticky_for_start_stop_and_register_event() -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-1")

    await recorder.start()

    dispatcher = recorder._dispatcher

    assert dispatcher is not None

    dispatcher.cancel()

    await wait_until(lambda: recorder.get_status() is AsyncioMetricsRecorderState.CANCELLED)

    start_outcome = await recorder.start()
    stop_outcome = await recorder.stop()

    assert start_outcome.success is False
    assert stop_outcome.success is False

    with pytest.raises(AsyncioMetricsRecorderInvalidStateError):
        recorder.register_event(EVENT)

    assert recorder.get_status() is AsyncioMetricsRecorderState.CANCELLED


# -------------------------
# Group m: error logging
# -------------------------
# -------------------------
# Group m: logging integration
# -------------------------


class _MemoryLogSink:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.events: list[LogEvent] = []

    def log(self, event: LogEvent) -> None:
        with self._lock:
            self.events.append(event)


@pytest.fixture()
def memory_log_sink() -> _MemoryLogSink:
    return _MemoryLogSink()


@pytest.fixture()
def memory_log_context(memory_log_sink: _MemoryLogSink) -> LogContext:
    return LogContext(
        namespace="metrics-recorder-tests",
        log_sink=memory_log_sink,
        payload_processor=LogPayloadProcessor(),
    )


def _log_pairs(sink: _MemoryLogSink) -> list[tuple[str, str | None]]:
    return [(event.meta.event_name, event.event_outcome) for event in sink.events]


def _single_log_event(
    sink: _MemoryLogSink,
    *,
    event_name: str,
    outcome: str | None,
) -> LogEvent:
    matches = [
        event
        for event in sink.events
        if event.meta.event_name == event_name and event.event_outcome == outcome
    ]

    assert len(matches) == 1
    return matches[0]


@pytest.mark.asyncio
async def test_m01_start_success_logs_invoke_and_success(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    recorder = RecordingMetricsRecorder(
        entity_id="recorder-log-1",
        log_context=memory_log_context,
    )

    result = await recorder.start()

    assert result.success is True

    assert _log_pairs(memory_log_sink) == [
        ("asyncio_metrics_recorder.start", "invoke"),
        ("asyncio_metrics_recorder.start", "success"),
    ]

    invoke = _single_log_event(
        memory_log_sink,
        event_name="asyncio_metrics_recorder.start",
        outcome="invoke",
    )
    success = _single_log_event(
        memory_log_sink,
        event_name="asyncio_metrics_recorder.start",
        outcome="success",
    )

    assert invoke.meta.entity_id == "recorder-log-1"
    assert success.meta.entity_id == "recorder-log-1"

    assert invoke.payload["state"] == AsyncioMetricsRecorderState.VIRGIN.value
    assert success.payload["state"] == AsyncioMetricsRecorderState.RUNNING.value

    await recorder.stop()


@pytest.mark.asyncio
async def test_m02_start_without_log_context_emits_no_log_events(
    memory_log_sink: _MemoryLogSink,
) -> None:
    recorder = RecordingMetricsRecorder(entity_id="recorder-log-2")

    result = await recorder.start()

    assert result.success is True
    assert memory_log_sink.events == []

    await recorder.stop()


@pytest.mark.asyncio
async def test_m03_stop_success_logs_invoke_and_success(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    recorder = RecordingMetricsRecorder(
        entity_id="recorder-log-3",
        log_context=memory_log_context,
    )

    await recorder.start()
    memory_log_sink.events.clear()

    result = await recorder.stop()

    assert result.success is True

    assert _log_pairs(memory_log_sink) == [
        ("asyncio_metrics_recorder.stop", "invoke"),
        ("asyncio_metrics_recorder.stop", "success"),
    ]

    invoke = _single_log_event(
        memory_log_sink,
        event_name="asyncio_metrics_recorder.stop",
        outcome="invoke",
    )
    success = _single_log_event(
        memory_log_sink,
        event_name="asyncio_metrics_recorder.stop",
        outcome="success",
    )

    assert invoke.meta.entity_id == "recorder-log-3"
    assert success.meta.entity_id == "recorder-log-3"

    assert invoke.payload["state"] == AsyncioMetricsRecorderState.RUNNING.value
    assert success.payload["state"] == AsyncioMetricsRecorderState.STOPPED.value


@pytest.mark.asyncio
async def test_m04_register_metric_success_logs_metric_name(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    recorder = RecordingMetricsRecorder(
        entity_id="recorder-log-4",
        log_context=memory_log_context,
    )
    metric = CountingMetric(name="test.metric.logged")

    recorder.register_metric(metric)

    assert _log_pairs(memory_log_sink) == [
        ("asyncio_metrics_recorder.register_metric", "invoke"),
        ("asyncio_metrics_recorder.register_metric", "success"),
    ]

    invoke = _single_log_event(
        memory_log_sink,
        event_name="asyncio_metrics_recorder.register_metric",
        outcome="invoke",
    )
    success = _single_log_event(
        memory_log_sink,
        event_name="asyncio_metrics_recorder.register_metric",
        outcome="success",
    )

    assert invoke.meta.entity_id == "recorder-log-4"
    assert success.meta.entity_id == "recorder-log-4"

    assert invoke.payload["state"] == AsyncioMetricsRecorderState.VIRGIN.value
    assert invoke.payload["kwargs"] == {
        "metric_name": "test.metric.logged",
    }

    assert success.payload["state"] == AsyncioMetricsRecorderState.VIRGIN.value
    assert "kwargs" not in success.payload
    assert "result" not in success.payload


@pytest.mark.asyncio
async def test_m05_register_metric_failed_logs_failed_event(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    recorder = RecordingMetricsRecorder(
        entity_id="recorder-log-5",
        log_context=memory_log_context,
    )

    with pytest.raises(TypeError, match="metric"):
        recorder.register_metric(cast(Any, object()))

    assert _log_pairs(memory_log_sink) == [
        ("asyncio_metrics_recorder.register_metric", "invoke"),
        ("asyncio_metrics_recorder.register_metric", "failed"),
    ]

    failed = _single_log_event(
        memory_log_sink,
        event_name="asyncio_metrics_recorder.register_metric",
        outcome="failed",
    )

    assert failed.meta.entity_id == "recorder-log-5"
    assert failed.payload["state"] == AsyncioMetricsRecorderState.VIRGIN.value
    assert "error" in failed.payload


@pytest.mark.asyncio
async def test_m06_get_metric_snapshots_logs_invoke_and_success(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    recorder = RecordingMetricsRecorder(
        entity_id="recorder-log-6",
        log_context=memory_log_context,
    )
    recorder.register_metric(CountingMetric(name="test.metric.snapshot"))
    memory_log_sink.events.clear()

    snapshots = recorder.get_metric_snapshots()

    assert snapshots == {
        "test.metric.snapshot": {
            "name": "test.metric.snapshot",
            "dimensions": {
                "total": 0,
            },
        }
    }

    assert _log_pairs(memory_log_sink) == [
        ("asyncio_metrics_recorder.get_metric_snapshots", "invoke"),
        ("asyncio_metrics_recorder.get_metric_snapshots", "success"),
    ]

    invoke = _single_log_event(
        memory_log_sink,
        event_name="asyncio_metrics_recorder.get_metric_snapshots",
        outcome="invoke",
    )
    success = _single_log_event(
        memory_log_sink,
        event_name="asyncio_metrics_recorder.get_metric_snapshots",
        outcome="success",
    )

    assert invoke.meta.entity_id == "recorder-log-6"
    assert success.meta.entity_id == "recorder-log-6"

    assert invoke.payload["state"] == AsyncioMetricsRecorderState.VIRGIN.value
    assert success.payload["state"] == AsyncioMetricsRecorderState.VIRGIN.value
    assert "result" not in success.payload


@pytest.mark.asyncio
async def test_m07_iter_metrics_logs_invoke_and_success(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    recorder = RecordingMetricsRecorder(
        entity_id="recorder-log-7",
        log_context=memory_log_context,
    )
    metric = CountingMetric(name="test.metric.iter")

    recorder.register_metric(metric)
    memory_log_sink.events.clear()

    metrics = tuple(recorder.iter_metrics())

    assert metrics == (metric,)

    assert _log_pairs(memory_log_sink) == [
        ("asyncio_metrics_recorder.iter_metrics", "invoke"),
        ("asyncio_metrics_recorder.iter_metrics", "success"),
    ]

    invoke = _single_log_event(
        memory_log_sink,
        event_name="asyncio_metrics_recorder.iter_metrics",
        outcome="invoke",
    )
    success = _single_log_event(
        memory_log_sink,
        event_name="asyncio_metrics_recorder.iter_metrics",
        outcome="success",
    )

    assert invoke.meta.entity_id == "recorder-log-7"
    assert success.meta.entity_id == "recorder-log-7"

    assert invoke.payload["state"] == AsyncioMetricsRecorderState.VIRGIN.value
    assert success.payload["state"] == AsyncioMetricsRecorderState.VIRGIN.value
    assert "result" not in success.payload


@pytest.mark.asyncio
async def test_m08_dispatch_error_is_logged_with_log_error_event(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    recorder = RecordingMetricsRecorder(
        entity_id="recorder-log-8",
        log_context=memory_log_context,
    )
    recorder.register_metric(CountingMetric(fail_on_handle=True))
    memory_log_sink.events.clear()

    await recorder.start()
    memory_log_sink.events.clear()

    recorder.register_event(EVENT)

    await wait_until(lambda: recorder.get_status() is AsyncioMetricsRecorderState.FAILURE)

    assert _log_pairs(memory_log_sink) == [
        ("metrics_recorder.dispatch_error", None),
    ]

    event = _single_log_event(
        memory_log_sink,
        event_name="metrics_recorder.dispatch_error",
        outcome=None,
    )

    assert event.meta.entity_id == "recorder-log-8"
    assert event.payload["kind"] == "RuntimeError"
    assert event.payload["message"] == "metric handle failed"


@pytest.mark.asyncio
async def test_m09_cleanup_failure_is_logged_with_log_error_event(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    recorder = RecordingMetricsRecorder(
        entity_id="recorder-log-9",
        log_context=memory_log_context,
    )
    recorder.register_metric(CountingMetric(fail_on_handle=True))
    recorder.stopped_exception = RuntimeError("cleanup failed")
    memory_log_sink.events.clear()

    await recorder.start()
    memory_log_sink.events.clear()

    recorder.register_event(EVENT)

    await wait_until(lambda: recorder.get_status() is AsyncioMetricsRecorderState.FAILURE)
    await wait_until(
        lambda: (
            (
                "metrics_recorder.cleanup.failed",
                None,
            )
            in _log_pairs(memory_log_sink)
        )
    )

    cleanup = _single_log_event(
        memory_log_sink,
        event_name="metrics_recorder.cleanup.failed",
        outcome=None,
    )

    assert cleanup.meta.entity_id == "recorder-log-9"
    assert cleanup.payload["kind"] == "RuntimeError"
    assert cleanup.payload["message"] == "cleanup failed"
