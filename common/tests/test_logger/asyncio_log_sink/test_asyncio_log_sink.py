from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import time
from collections.abc import Callable
from typing import Any, cast

import pytest

from mvx.common.logger.models import LogEvent, LogSinkTerminator
from mvx.common.logger.asyncio_log_sink import log_sink as log_sink_pack
from mvx.common.logger.asyncio_log_sink.common import AsyncioLogSinkState
from mvx.common.logger.asyncio_log_sink.errors import (
    AsyncioLogSinkDispatcherCancelledError,
    AsyncioLogSinkError,
    AsyncioLogSinkEventLoopUnavailableError,
    AsyncioLogSinkInvalidStateError,
    AsyncioLogSinkOnStartingHookFailedError,
    AsyncioLogSinkOnStoppedHookFailedError,
    AsyncioLogSinkQueueOverflowError,
    AsyncioLogSinkUnexpectedError,
)

EVENT = cast(LogEvent, object())
TIMEOUT = 2.0


def wait_thread_event(event: threading.Event, timeout: float = TIMEOUT) -> None:
    assert event.wait(timeout), "threading.Event was not set in time"


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


def run_in_thread(func: Callable[[], Any], *, timeout: float = TIMEOUT) -> Any:
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(func)
        return future.result(timeout=timeout)


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


def make_domain_error() -> AsyncioLogSinkError:
    return AsyncioLogSinkInvalidStateError(
        sink_state=AsyncioLogSinkState.RUNNING,
        expected_states=(AsyncioLogSinkState.VIRGIN,),
    )


async def wait_thread_event_async(
    event: threading.Event,
    timeout: float = TIMEOUT,
) -> None:
    await asyncio.wait_for(
        asyncio.to_thread(event.wait, timeout),
        timeout=timeout + 0.1,
    )

    assert event.is_set(), "threading.Event was not set in time"


async def run_in_thread_async(
    func: Callable[[], Any],
    *,
    timeout: float = TIMEOUT,
) -> Any:
    return await asyncio.wait_for(
        asyncio.to_thread(func),
        timeout=timeout,
    )


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


def create_recording_sink(
    **kwargs: Any,
) -> tuple[RecordingSink, LogSinkTerminator]:
    sink, terminator = RecordingSink.create(**kwargs)

    return cast(RecordingSink, sink), terminator


class RecordingSink(log_sink_pack.AsyncioLogSink):
    def __init__(self, *, marker: str = "default", **kwargs: Any) -> None:
        super().__init__(**kwargs)

        self.marker = marker
        self.created_thread_id = threading.get_ident()
        self.created_loop = asyncio.get_running_loop()
        self.created_event_loop = asyncio.get_event_loop()

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

        self.dispatch_entered = threading.Event()
        self.dispatch_finished = threading.Event()
        self.dispatch_release = asyncio.Event()
        self.block_dispatch = False
        self.dispatch_exception: Exception | None = None
        self.fail_on_dispatch_numbers: set[int] = set()
        self.dispatch_call_count = 0

        self.dispatched: list[LogEvent] = []
        self.dispatch_thread_ids: list[int] = []
        self.dispatch_loops: list[asyncio.AbstractEventLoop] = []

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

    async def _dispatch_core(self, event: LogEvent) -> None:
        self.dispatch_call_count += 1
        call_number = self.dispatch_call_count

        self.dispatch_entered.set()

        if self.block_dispatch:
            await self.dispatch_release.wait()

        if call_number in self.fail_on_dispatch_numbers:
            raise self.dispatch_exception or RuntimeError("dispatch failed")

        if self.dispatch_exception is not None:
            raise self.dispatch_exception

        self.dispatched.append(event)
        self.dispatch_thread_ids.append(threading.get_ident())
        self.dispatch_loops.append(asyncio.get_running_loop())
        self.dispatch_finished.set()


class BootstrapFailingSink(RecordingSink):
    def __new__(cls, **kwargs: Any) -> BootstrapFailingSink:
        raise RuntimeError("bootstrap failed")


class BootstrapDomainFailingSink(RecordingSink):
    def __new__(cls, **kwargs: Any) -> BootstrapDomainFailingSink:
        raise AsyncioLogSinkInvalidStateError(
            sink_state=AsyncioLogSinkState.VIRGIN,
            expected_states=(AsyncioLogSinkState.RUNNING,),
        )


class StartingFailingFactorySink(RecordingSink):
    async def _on_starting(self) -> None:
        await super()._on_starting()
        raise RuntimeError("starting failed")


class StoppedFailingFactorySink(RecordingSink):
    async def _on_stopped(self) -> None:
        await super()._on_stopped()
        raise RuntimeError("stopped failed")


@pytest.mark.asyncio
async def test_a01_constructor_inside_running_loop_creates_virgin_sink() -> None:
    sink = RecordingSink()

    assert sink.get_status() is AsyncioLogSinkState.VIRGIN
    assert sink._dispatcher is None
    assert sink._pending_counter == 0
    assert sink._last_error is None


def test_a02_constructor_outside_running_loop_fails() -> None:
    with pytest.raises(AsyncioLogSinkEventLoopUnavailableError):
        RecordingSink()


@pytest.mark.asyncio
async def test_a03_constructor_stores_creation_loop() -> None:
    sink = RecordingSink()

    assert sink.created_loop is asyncio.get_running_loop()


@pytest.mark.asyncio
async def test_a04_constructor_stores_creation_thread() -> None:
    sink = RecordingSink()

    assert sink.created_thread_id == threading.get_ident()


@pytest.mark.asyncio
async def test_a05_default_queue_limit_is_used_when_queue_max_size_is_none() -> None:
    sink = RecordingSink()

    assert sink._max_pending_counter == log_sink_pack.DEFAULT_QUEUE_MAX_SIZE


@pytest.mark.asyncio
async def test_a06_custom_queue_limit_is_used() -> None:
    sink = RecordingSink(queue_max_size=3)

    assert sink._max_pending_counter == 3


@pytest.mark.asyncio
async def test_a07_default_overflow_policy_is_raise_error() -> None:
    sink = RecordingSink()

    assert (
        sink._queue_overflow_policy is log_sink_pack.AsyncioLogSinkQueueOverflowPolicy.RAISE_ERROR
    )


@pytest.mark.asyncio
async def test_a08_custom_overflow_policy_is_stored() -> None:
    sink = RecordingSink(
        queue_overflow_policy=log_sink_pack.AsyncioLogSinkQueueOverflowPolicy.DROP,
    )

    assert sink._queue_overflow_policy is log_sink_pack.AsyncioLogSinkQueueOverflowPolicy.DROP


@pytest.mark.asyncio
async def test_a09_namespace_default_is_used() -> None:
    sink = RecordingSink()

    assert sink._namespace == log_sink_pack.DEFAULT_NAMESPACE


@pytest.mark.asyncio
async def test_a10_custom_namespace_is_used() -> None:
    sink = RecordingSink(namespace="test.sink")

    assert sink._namespace == "test.sink"


def test_b01_wait_returns_success_result_after_done_none() -> None:
    handle = log_sink_pack._WaitHandleInternal(log_sink_pack.AsyncioLogSinkOp.START)

    handle.done(None)

    result = handle.wait()

    assert result.success is True
    assert result.error is None
    assert result.op_name is log_sink_pack.AsyncioLogSinkOp.START


def test_b02_wait_returns_domain_error_as_is() -> None:
    handle = log_sink_pack._WaitHandleInternal(log_sink_pack.AsyncioLogSinkOp.START)
    error = AsyncioLogSinkInvalidStateError(
        sink_state=AsyncioLogSinkState.RUNNING,
        expected_states=(AsyncioLogSinkState.VIRGIN,),
    )

    handle.done(error)

    result = handle.wait()

    assert result.success is False
    assert result.error is error


def test_b03_wait_wraps_ordinary_exception() -> None:
    handle = log_sink_pack._WaitHandleInternal(log_sink_pack.AsyncioLogSinkOp.START)

    handle.done(RuntimeError("boom"))

    result = handle.wait()

    assert result.success is False
    assert isinstance(result.error, AsyncioLogSinkUnexpectedError)


@pytest.mark.asyncio
async def test_b04_await_handle_returns_success_result() -> None:
    handle = log_sink_pack._WaitHandleInternal(log_sink_pack.AsyncioLogSinkOp.STOP)

    handle.done(None)

    result = await handle

    assert result.success is True
    assert result.error is None
    assert result.op_name is log_sink_pack.AsyncioLogSinkOp.STOP


@pytest.mark.asyncio
async def test_b05_await_handle_returns_domain_error_as_is() -> None:
    handle = log_sink_pack._WaitHandleInternal(log_sink_pack.AsyncioLogSinkOp.STOP)
    error = make_domain_error()

    handle.done(error)

    result = await handle

    assert result.success is False
    assert result.error is error


@pytest.mark.asyncio
async def test_b06_await_handle_wraps_ordinary_exception() -> None:
    handle = log_sink_pack._WaitHandleInternal(log_sink_pack.AsyncioLogSinkOp.STOP)

    handle.done(RuntimeError("boom"))

    result = await handle

    assert result.success is False
    assert isinstance(result.error, AsyncioLogSinkUnexpectedError)


def test_b07_repeated_done_keeps_first_success() -> None:
    handle = log_sink_pack._WaitHandleInternal(log_sink_pack.AsyncioLogSinkOp.START)

    handle.done(None)
    handle.done(RuntimeError("late"))

    assert handle.wait().success is True


def test_b08_repeated_done_keeps_first_error() -> None:
    handle = log_sink_pack._WaitHandleInternal(log_sink_pack.AsyncioLogSinkOp.START)
    first = make_domain_error()

    handle.done(first)
    handle.done(None)

    result = handle.wait()

    assert result.success is False
    assert result.error is first


def test_b09_done_from_future_propagates_successful_future() -> None:
    cf_future: concurrent.futures.Future[None] = concurrent.futures.Future()
    handle = log_sink_pack._WaitHandleInternal(log_sink_pack.AsyncioLogSinkOp.START)

    cf_future.set_result(None)
    handle.done_from_future(cf_future)

    assert handle.wait().success is True


def test_b10_done_from_future_propagates_failed_future() -> None:
    cf_future: concurrent.futures.Future[None] = concurrent.futures.Future()
    handle = log_sink_pack._WaitHandleInternal(log_sink_pack.AsyncioLogSinkOp.START)

    cf_future.set_exception(RuntimeError("boom"))
    handle.done_from_future(cf_future)

    result = handle.wait()

    assert result.success is False
    assert isinstance(result.error, AsyncioLogSinkUnexpectedError)


def test_b11_wait_can_be_called_from_non_loop_thread() -> None:
    handle = log_sink_pack._WaitHandleInternal(log_sink_pack.AsyncioLogSinkOp.START)

    def waiter() -> log_sink_pack.AsyncioLogSinkOpResult:
        return handle.wait()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(waiter)

        time.sleep(0.02)
        handle.done(None)

        result = future.result(timeout=TIMEOUT)

    assert result.success is True


@pytest.mark.asyncio
async def test_b12_await_handle_cancellation_does_not_cancel_internal_future() -> None:
    handle = log_sink_pack._WaitHandleInternal(log_sink_pack.AsyncioLogSinkOp.START)

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


@pytest.mark.asyncio
async def test_c01_start_from_virgin_succeeds() -> None:
    sink = RecordingSink()

    outcome = await sink.start()

    assert outcome.success is True
    assert sink.get_status() is AsyncioLogSinkState.RUNNING
    assert sink.on_starting_count == 1
    assert sink._dispatcher is not None
    assert not sink._dispatcher.done()

    await sink.stop()


@pytest.mark.asyncio
async def test_c02_start_returns_handle_immediately_while_starting() -> None:
    sink = RecordingSink()
    sink.block_starting = True

    handle = sink.start()

    await wait_thread_event_async(sink.starting_entered)

    assert sink.get_status() is AsyncioLogSinkState.STARTING
    assert not handle._future.done()

    sink.starting_release.set()

    assert (await handle).success is True

    await sink.stop()


@pytest.mark.asyncio
async def test_c03_start_from_starting_joins_existing_start() -> None:
    sink = RecordingSink()
    sink.block_starting = True

    h1 = sink.start()

    await wait_thread_event_async(sink.starting_entered)

    h2 = sink.start()

    sink.starting_release.set()

    assert (await h1).success is True
    assert (await h2).success is True
    assert sink.on_starting_count == 1
    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    await sink.stop()


@pytest.mark.asyncio
async def test_c04_many_concurrent_start_calls_while_starting_join_same_start() -> None:
    sink = RecordingSink()
    sink.block_starting = True

    first = sink.start()

    await wait_thread_event_async(sink.starting_entered)

    handles = [sink.start() for _ in range(20)]

    sink.starting_release.set()

    outcomes = [await handle for handle in [first, *handles]]

    assert all(outcome.success for outcome in outcomes)
    assert sink.on_starting_count == 1
    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    await sink.stop()


@pytest.mark.asyncio
async def test_c05_start_from_running_fails() -> None:
    sink = RecordingSink()

    assert (await sink.start()).success is True

    outcome = await sink.start()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioLogSinkInvalidStateError)
    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    await sink.stop()


@pytest.mark.asyncio
async def test_c06_start_from_stopping_fails() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.block_stopped = True

    stop_handle = sink.stop()

    await wait_thread_event_async(sink.stopped_entered)

    outcome = await sink.start()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioLogSinkInvalidStateError)
    assert sink.get_status() is AsyncioLogSinkState.STOPPING

    sink.stopped_release.set()

    await stop_handle


@pytest.mark.asyncio
async def test_c07_start_from_failure_fails() -> None:
    sink = RecordingSink()
    sink.starting_exception = RuntimeError("boom")

    assert (await sink.start()).success is False

    outcome = await sink.start()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioLogSinkInvalidStateError)


@pytest.mark.asyncio
async def test_c08_start_from_cancelled_fails() -> None:
    sink = RecordingSink()

    await sink.start()

    assert sink._dispatcher is not None

    sink._dispatcher.cancel()

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.CANCELLED)

    outcome = await sink.start()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioLogSinkInvalidStateError)


@pytest.mark.asyncio
async def test_c09_start_hook_ordinary_exception_becomes_on_starting_hook_failed() -> None:
    sink = RecordingSink()
    sink.starting_exception = RuntimeError("boom")

    outcome = await sink.start()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioLogSinkOnStartingHookFailedError)
    assert sink.get_status() is AsyncioLogSinkState.FAILURE
    assert sink._last_error is outcome.error
    assert sink._dispatcher is None


@pytest.mark.asyncio
async def test_c10_start_hook_domain_error_is_wrapped_by_on_starting_hook_failed() -> None:
    sink = RecordingSink()
    original = make_domain_error()
    sink.starting_exception = original

    outcome = await sink.start()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioLogSinkOnStartingHookFailedError)
    assert getattr(outcome.error, "cause", None) is original


@pytest.mark.asyncio
async def test_c11_dispatcher_is_created_only_after_successful_on_starting() -> None:
    sink = RecordingSink()
    sink.block_starting = True

    handle = sink.start()

    await wait_thread_event_async(sink.starting_entered)

    assert sink._dispatcher is None

    sink.starting_release.set()

    assert (await handle).success is True
    assert sink._dispatcher is not None

    await sink.stop()


@pytest.mark.asyncio
async def test_c12_start_future_is_cleared_after_successful_start() -> None:
    sink = RecordingSink()

    assert (await sink.start()).success is True
    assert sink._start_future is None

    await sink.stop()


@pytest.mark.asyncio
async def test_c13_start_future_is_cleared_after_failed_start() -> None:
    sink = RecordingSink()
    sink.starting_exception = RuntimeError("boom")

    assert (await sink.start()).success is False
    assert sink._start_future is None


# D. Manual stop lifecycle


@pytest.mark.asyncio
async def test_d01_stop_from_running_succeeds() -> None:
    sink = RecordingSink()

    await sink.start()

    outcome = await sink.stop()

    assert outcome.success is True
    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert sink._dispatcher is None
    assert sink.on_stopped_count == 1
    assert sink._stop_future is None
    assert sink._last_error is None


@pytest.mark.asyncio
async def test_d02_stop_from_virgin_fails() -> None:
    sink = RecordingSink()

    outcome = await sink.stop()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioLogSinkInvalidStateError)
    assert sink.get_status() is AsyncioLogSinkState.VIRGIN
    assert sink._stop_future is None


@pytest.mark.asyncio
async def test_d03_stop_from_starting_fails() -> None:
    sink = RecordingSink()
    sink.block_starting = True

    start_handle = sink.start()

    await wait_thread_event_async(sink.starting_entered)

    outcome = await sink.stop()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioLogSinkInvalidStateError)
    assert sink.get_status() is AsyncioLogSinkState.STARTING
    assert sink._stop_future is None

    sink.starting_release.set()

    assert (await start_handle).success is True

    await sink.stop()


@pytest.mark.asyncio
async def test_d04_stop_from_stopping_joins_existing_stop() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.block_stopped = True

    h1 = sink.stop()

    await wait_thread_event_async(sink.stopped_entered)

    h2 = sink.stop()

    assert sink.get_status() is AsyncioLogSinkState.STOPPING
    assert h1 is not h2

    sink.stopped_release.set()

    outcome1 = await h1
    outcome2 = await h2

    assert outcome1.success is True
    assert outcome2.success is True
    assert sink.on_stopped_count == 1
    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert sink._stop_future is None


@pytest.mark.asyncio
async def test_d05_many_concurrent_stop_calls_while_stopping_join_same_stop() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.block_stopped = True

    first = sink.stop()

    await wait_thread_event_async(sink.stopped_entered)

    handles = [sink.stop() for _ in range(20)]

    sink.stopped_release.set()

    outcomes = [await handle for handle in [first, *handles]]

    assert all(outcome.success for outcome in outcomes)
    assert sink.on_stopped_count == 1
    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert sink._stop_future is None


@pytest.mark.asyncio
async def test_d06_stop_from_stopped_fails() -> None:
    sink = RecordingSink()

    await sink.start()
    await sink.stop()

    outcome = await sink.stop()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioLogSinkInvalidStateError)
    assert sink.get_status() is AsyncioLogSinkState.STOPPED


@pytest.mark.asyncio
async def test_d07_stop_from_failure_fails() -> None:
    sink = RecordingSink()
    sink.starting_exception = RuntimeError("boom")

    start_outcome = await sink.start()

    assert start_outcome.success is False
    assert sink.get_status() is AsyncioLogSinkState.FAILURE

    outcome = await sink.stop()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioLogSinkInvalidStateError)
    assert sink.get_status() is AsyncioLogSinkState.FAILURE


@pytest.mark.asyncio
async def test_d08_stop_from_cancelled_fails() -> None:
    sink = RecordingSink()

    await sink.start()

    assert sink._dispatcher is not None

    sink._dispatcher.cancel()

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.CANCELLED)

    outcome = await sink.stop()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioLogSinkInvalidStateError)
    assert sink.get_status() is AsyncioLogSinkState.CANCELLED


@pytest.mark.asyncio
async def test_d09_stop_sets_stopping_before_stop_core_finishes() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.block_stopped = True

    handle = sink.stop()

    await wait_thread_event_async(sink.stopped_entered)

    assert sink.get_status() is AsyncioLogSinkState.STOPPING
    assert sink._stop_future is not None
    assert not handle._future.done()

    sink.stopped_release.set()

    assert (await handle).success is True


@pytest.mark.asyncio
async def test_d10_stop_future_is_cleared_after_successful_stop() -> None:
    sink = RecordingSink()

    await sink.start()

    outcome = await sink.stop()

    assert outcome.success is True
    assert sink._stop_future is None


@pytest.mark.asyncio
async def test_d11_stop_future_is_cleared_after_failed_stop() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.stopped_exception = RuntimeError("boom")

    outcome = await sink.stop()

    assert outcome.success is False
    assert sink._stop_future is None
    assert sink.get_status() is AsyncioLogSinkState.FAILURE


@pytest.mark.asyncio
async def test_d12_on_stopped_ordinary_exception_becomes_on_stopped_hook_failed() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.stopped_exception = RuntimeError("boom")

    outcome = await sink.stop()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioLogSinkOnStoppedHookFailedError)
    assert sink.get_status() is AsyncioLogSinkState.FAILURE
    assert sink._last_error is outcome.error


@pytest.mark.asyncio
async def test_d13_on_stopped_domain_error_is_wrapped_by_on_stopped_hook_failed() -> None:
    sink = RecordingSink()

    await sink.start()

    original = make_domain_error()
    sink.stopped_exception = original

    outcome = await sink.stop()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioLogSinkOnStoppedHookFailedError)
    assert getattr(outcome.error, "cause", None) is original
    assert sink.get_status() is AsyncioLogSinkState.FAILURE


@pytest.mark.asyncio
async def test_d14_normal_stop_cancellation_of_dispatcher_does_not_mark_cancelled() -> None:
    sink = RecordingSink()

    await sink.start()

    outcome = await sink.stop()

    assert outcome.success is True
    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert sink._last_error is None


# E. Log acceptance by state


@pytest.mark.asyncio
async def test_e01_log_in_virgin_accepts_event_and_triggers_start() -> None:
    sink = RecordingSink()

    sink.log(EVENT)

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.RUNNING)
    await wait_until(lambda: sink.dispatched == [EVENT])

    assert sink.on_starting_count == 1
    assert sink._pending_counter == 0

    await sink.stop()


@pytest.mark.asyncio
async def test_e02_log_in_starting_accepts_and_buffers_event_until_dispatcher_starts() -> None:
    sink = RecordingSink()
    sink.block_starting = True

    start_handle = sink.start()

    await wait_thread_event_async(sink.starting_entered)

    sink.log(EVENT)

    assert sink.get_status() is AsyncioLogSinkState.STARTING
    assert sink._pending_counter == 1
    assert sink.dispatched == []

    sink.starting_release.set()

    assert (await start_handle).success is True

    await wait_until(lambda: sink.dispatched == [EVENT])

    assert sink._pending_counter == 0
    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    await sink.stop()


@pytest.mark.asyncio
async def test_e03_log_in_running_accepts_event() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.dispatched == [EVENT])

    assert sink.get_status() is AsyncioLogSinkState.RUNNING
    assert sink._pending_counter == 0

    await sink.stop()


@pytest.mark.asyncio
async def test_e04_log_in_stopping_fails() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.block_stopped = True

    stop_handle = sink.stop()

    await wait_thread_event_async(sink.stopped_entered)

    with pytest.raises(AsyncioLogSinkInvalidStateError) as exc_info:
        sink.log(EVENT)

    assert sink.get_status() is AsyncioLogSinkState.STOPPING
    assert sink._pending_counter == 0

    assert exc_info.value.details["sink_state"] == AsyncioLogSinkState.STOPPING
    assert exc_info.value.details["expected_states"] == (
        AsyncioLogSinkState.VIRGIN,
        AsyncioLogSinkState.STARTING,
        AsyncioLogSinkState.RUNNING,
    )

    sink.stopped_release.set()

    assert (await stop_handle).success is True


@pytest.mark.asyncio
async def test_e05_log_in_stopped_fails() -> None:
    sink = RecordingSink()

    await sink.start()
    await sink.stop()

    with pytest.raises(AsyncioLogSinkInvalidStateError) as exc_info:
        sink.log(EVENT)

    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert sink._pending_counter == 0

    assert exc_info.value.details["sink_state"] == AsyncioLogSinkState.STOPPED
    assert exc_info.value.details["expected_states"] == (
        AsyncioLogSinkState.VIRGIN,
        AsyncioLogSinkState.STARTING,
        AsyncioLogSinkState.RUNNING,
    )


@pytest.mark.asyncio
async def test_e06_log_in_failure_fails_and_carries_last_error() -> None:
    sink = RecordingSink()
    sink.starting_exception = RuntimeError("boom")

    start_outcome = await sink.start()

    assert start_outcome.success is False
    assert sink.get_status() is AsyncioLogSinkState.FAILURE
    assert sink._last_error is start_outcome.error

    with pytest.raises(AsyncioLogSinkInvalidStateError) as exc_info:
        sink.log(EVENT)

    assert sink._pending_counter == 0

    assert exc_info.value.details["sink_state"] == AsyncioLogSinkState.FAILURE
    assert exc_info.value.details["expected_states"] == (
        AsyncioLogSinkState.VIRGIN,
        AsyncioLogSinkState.STARTING,
        AsyncioLogSinkState.RUNNING,
    )

    assert getattr(exc_info.value, "cause", None) is sink._last_error


@pytest.mark.asyncio
async def test_e07_log_in_cancelled_fails() -> None:
    sink = RecordingSink()

    await sink.start()

    assert sink._dispatcher is not None

    sink._dispatcher.cancel()

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.CANCELLED)

    with pytest.raises(AsyncioLogSinkInvalidStateError) as exc_info:
        sink.log(EVENT)

    assert sink._pending_counter == 0

    assert exc_info.value.details["sink_state"] == AsyncioLogSinkState.CANCELLED
    assert exc_info.value.details["expected_states"] == (
        AsyncioLogSinkState.VIRGIN,
        AsyncioLogSinkState.STARTING,
        AsyncioLogSinkState.RUNNING,
    )


@pytest.mark.asyncio
async def test_e08_log_in_virgin_starts_only_once_for_first_lazy_log() -> None:
    event_1 = cast(LogEvent, object())
    event_2 = cast(LogEvent, object())

    sink = RecordingSink()
    sink.block_starting = True

    sink.log(event_1)

    await wait_thread_event_async(sink.starting_entered)

    sink.log(event_2)

    assert sink.get_status() is AsyncioLogSinkState.STARTING
    assert sink.on_starting_count == 1
    assert sink._pending_counter == 2
    assert sink.dispatched == []

    sink.starting_release.set()

    await wait_until(lambda: sink.dispatched == [event_1, event_2])

    assert sink._pending_counter == 0
    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    await sink.stop()


@pytest.mark.asyncio
async def test_e09_log_in_starting_does_not_create_second_start_future() -> None:
    sink = RecordingSink()
    sink.block_starting = True

    first_start_handle = sink.start()

    await wait_thread_event_async(sink.starting_entered)

    original_start_future = sink._start_future

    sink.log(EVENT)

    assert sink._start_future is original_start_future
    assert sink.on_starting_count == 1
    assert sink._pending_counter == 1

    sink.starting_release.set()

    assert (await first_start_handle).success is True

    await wait_until(lambda: sink.dispatched == [EVENT])

    assert sink._pending_counter == 0

    await sink.stop()


# F. Queue / pending counter / overflow


@pytest.mark.asyncio
async def test_f01_pending_counter_increments_before_scheduled_enqueue_is_processed() -> None:
    sink = RecordingSink()
    sink.block_dispatch = True

    await sink.start()

    sink.log(EVENT)

    assert sink._pending_counter == 1

    sink.dispatch_release.set()

    await wait_until(lambda: sink._pending_counter == 0)

    await sink.stop()


@pytest.mark.asyncio
async def test_f02_pending_counter_decrements_after_successful_dispatch() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.dispatched == [EVENT])

    assert sink._pending_counter == 0

    await sink.stop()


@pytest.mark.asyncio
async def test_f03_pending_counter_decrements_after_dispatch_core_raises() -> None:
    sink = RecordingSink()
    sink.dispatch_exception = RuntimeError("boom")

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.FAILURE)

    assert sink._pending_counter == 0


@pytest.mark.asyncio
async def test_f04_overflow_raise_error_raises_when_pending_limit_reached() -> None:
    sink = RecordingSink(queue_max_size=1)
    sink.block_dispatch = True

    await sink.start()

    sink.log(EVENT)

    await wait_thread_event_async(sink.dispatch_entered)

    with pytest.raises(AsyncioLogSinkQueueOverflowError):
        sink.log(EVENT)

    sink.dispatch_release.set()

    await wait_until(lambda: sink._pending_counter == 0)

    await sink.stop()


@pytest.mark.asyncio
async def test_f05_overflow_raise_error_does_not_increment_counter_for_rejected_event() -> None:
    sink = RecordingSink(queue_max_size=1)
    sink.block_dispatch = True

    await sink.start()

    sink.log(EVENT)

    await wait_thread_event_async(sink.dispatch_entered)

    with pytest.raises(AsyncioLogSinkQueueOverflowError):
        sink.log(EVENT)

    assert sink._pending_counter == 1

    sink.dispatch_release.set()

    await wait_until(lambda: sink._pending_counter == 0)

    await sink.stop()


@pytest.mark.asyncio
async def test_f06_overflow_drop_silently_returns() -> None:
    sink = RecordingSink(
        queue_max_size=1,
        queue_overflow_policy=log_sink_pack.AsyncioLogSinkQueueOverflowPolicy.DROP,
    )
    sink.block_dispatch = True

    await sink.start()

    sink.log(EVENT)

    await wait_thread_event_async(sink.dispatch_entered)

    sink.log(EVENT)

    assert sink._pending_counter == 1

    sink.dispatch_release.set()

    await wait_until(lambda: sink._pending_counter == 0)

    await sink.stop()


@pytest.mark.asyncio
async def test_f07_overflow_drop_does_not_enqueue_dropped_event() -> None:
    event_1 = cast(LogEvent, object())
    event_2 = cast(LogEvent, object())

    sink = RecordingSink(
        queue_max_size=1,
        queue_overflow_policy=log_sink_pack.AsyncioLogSinkQueueOverflowPolicy.DROP,
    )
    sink.block_dispatch = True

    await sink.start()

    sink.log(event_1)

    await wait_thread_event_async(sink.dispatch_entered)

    sink.log(event_2)

    sink.dispatch_release.set()

    await wait_until(lambda: sink.dispatched == [event_1])

    assert event_2 not in sink.dispatched
    assert sink._pending_counter == 0

    await sink.stop()


@pytest.mark.asyncio
async def test_f08_overflow_drop_does_not_increment_counter_for_dropped_event() -> None:
    sink = RecordingSink(
        queue_max_size=1,
        queue_overflow_policy=log_sink_pack.AsyncioLogSinkQueueOverflowPolicy.DROP,
    )
    sink.block_dispatch = True

    await sink.start()

    sink.log(EVENT)

    await wait_thread_event_async(sink.dispatch_entered)

    sink.log(EVENT)

    assert sink._pending_counter == 1

    sink.dispatch_release.set()

    await wait_until(lambda: sink._pending_counter == 0)

    await sink.stop()


@pytest.mark.asyncio
async def test_f09_queue_max_size_zero_currently_falls_back_to_default() -> None:
    sink = RecordingSink(queue_max_size=0)

    assert sink._max_pending_counter == log_sink_pack.DEFAULT_QUEUE_MAX_SIZE


@pytest.mark.asyncio
async def test_f10_queue_max_size_negative_creates_immediate_overflow_state() -> None:
    sink = RecordingSink(queue_max_size=-1)

    await sink.start()

    with pytest.raises(AsyncioLogSinkQueueOverflowError):
        sink.log(EVENT)

    assert sink._pending_counter == 0

    await sink.stop()


@pytest.mark.asyncio
async def test_f11_many_events_below_limit_are_all_accepted() -> None:
    events = [cast(LogEvent, object()) for _ in range(100)]

    sink = RecordingSink(queue_max_size=100)

    await sink.start()

    for event in events:
        sink.log(event)

    await wait_until(lambda: len(sink.dispatched) == len(events))

    assert sink.dispatched == events
    assert sink._pending_counter == 0

    await sink.stop()


@pytest.mark.asyncio
async def test_f12_many_events_at_limit_then_one_more_overflows() -> None:
    events = [cast(LogEvent, object()) for _ in range(3)]

    sink = RecordingSink(queue_max_size=3)
    sink.block_dispatch = True

    await sink.start()

    for event in events:
        sink.log(event)

    await wait_thread_event_async(sink.dispatch_entered)

    assert sink._pending_counter == 3

    with pytest.raises(AsyncioLogSinkQueueOverflowError):
        sink.log(cast(LogEvent, object()))

    assert sink._pending_counter == 3

    sink.dispatch_release.set()

    await wait_until(lambda: sink._pending_counter == 0)
    await wait_until(lambda: len(sink.dispatched) == 3)

    await sink.stop()


# G. Dispatching loop


@pytest.mark.asyncio
async def test_g01_events_are_dispatched_in_fifo_order() -> None:
    events = [cast(LogEvent, object()) for _ in range(3)]

    sink = RecordingSink()

    await sink.start()

    for event in events:
        sink.log(event)

    await wait_until(lambda: len(sink.dispatched) == 3)

    assert sink.dispatched == events

    await sink.stop()


@pytest.mark.asyncio
async def test_g02_dispatch_core_receives_exact_event_object() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: len(sink.dispatched) == 1)

    assert sink.dispatched[0] is EVENT

    await sink.stop()


@pytest.mark.asyncio
async def test_g03_dispatch_loop_continues_after_successful_event() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.dispatched == [EVENT])

    assert sink._dispatcher is not None
    assert not sink._dispatcher.done()
    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    await sink.stop()


@pytest.mark.asyncio
async def test_g04_dispatch_core_ordinary_exception_stops_dispatcher() -> None:
    sink = RecordingSink()
    sink.dispatch_exception = RuntimeError("boom")

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.FAILURE)

    assert sink._dispatcher is not None
    assert sink._dispatcher.done()


@pytest.mark.asyncio
async def test_g05_dispatch_core_exception_is_mapped_to_unexpected_error() -> None:
    sink = RecordingSink()
    original = RuntimeError("boom")
    sink.dispatch_exception = original

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.FAILURE)

    assert isinstance(sink._last_error, AsyncioLogSinkUnexpectedError)
    assert getattr(sink._last_error, "cause", None) is original


@pytest.mark.asyncio
async def test_g06_dispatch_core_asyncio_log_sink_error_is_stored_as_is() -> None:
    sink = RecordingSink()
    original = make_domain_error()
    sink.dispatch_exception = original

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.FAILURE)

    assert sink._last_error is original


@pytest.mark.asyncio
async def test_g07_dispatch_failure_still_calls_queue_task_done() -> None:
    sink = RecordingSink()
    sink.dispatch_exception = RuntimeError("boom")

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.FAILURE)

    assert sink._pending_counter == 0

    await asyncio.wait_for(sink._queue.join(), timeout=TIMEOUT)


@pytest.mark.asyncio
async def test_g08_dispatcher_external_cancel_outside_stopping_sets_cancelled() -> None:
    sink = RecordingSink()

    await sink.start()

    assert sink._dispatcher is not None

    sink._dispatcher.cancel()

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.CANCELLED)

    assert isinstance(sink._last_error, AsyncioLogSinkDispatcherCancelledError)


@pytest.mark.asyncio
async def test_g09_dispatcher_normal_cancel_during_stopping_does_not_set_cancelled() -> None:
    sink = RecordingSink()

    await sink.start()

    outcome = await sink.stop()

    assert outcome.success is True
    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert not isinstance(sink._last_error, AsyncioLogSinkDispatcherCancelledError)


@pytest.mark.asyncio
async def test_g10_dispatcher_task_name_uses_namespace() -> None:
    sink = RecordingSink(namespace="test.ns")

    await sink.start()

    assert sink._dispatcher is not None
    assert sink._dispatcher.get_name() == "test.ns.dispatching_loop"

    await sink.stop()


# H. Flush semantics


@pytest.mark.asyncio
async def test_h01_stop_flushes_already_queued_events() -> None:
    events = [cast(LogEvent, object()) for _ in range(10)]

    sink = RecordingSink()

    await sink.start()

    for event in events:
        sink.log(event)

    outcome = await sink.stop()

    assert outcome.success is True
    assert sink.dispatched == events
    assert sink._pending_counter == 0
    assert sink.get_status() is AsyncioLogSinkState.STOPPED


@pytest.mark.asyncio
async def test_h02_stop_flushes_events_scheduled_via_call_soon_threadsafe_before_barrier() -> None:
    sink = RecordingSink()

    await sink.start()

    run_in_thread(lambda: sink.log(EVENT))

    outcome = await sink.stop()

    assert outcome.success is True
    assert sink.dispatched == [EVENT]
    assert sink._pending_counter == 0
    assert sink.get_status() is AsyncioLogSinkState.STOPPED


@pytest.mark.asyncio
async def test_h03_stop_waits_for_currently_running_dispatch_core() -> None:
    sink = RecordingSink()
    sink.block_dispatch = True

    await sink.start()

    sink.log(EVENT)

    await wait_thread_event_async(sink.dispatch_entered)

    stop_handle = sink.stop()

    await asyncio.sleep(0.05)

    assert not stop_handle._future.done()
    assert sink.get_status() is AsyncioLogSinkState.STOPPING

    sink.dispatch_release.set()

    outcome = await stop_handle

    assert outcome.success is True
    assert sink.dispatched == [EVENT]
    assert sink._pending_counter == 0
    assert sink.get_status() is AsyncioLogSinkState.STOPPED


@pytest.mark.asyncio
async def test_h04_flush_returns_if_dispatcher_is_none() -> None:
    sink = RecordingSink()

    await asyncio.wait_for(sink._flush_core(), timeout=TIMEOUT)


@pytest.mark.asyncio
async def test_h05_flush_returns_if_dispatcher_already_done() -> None:
    sink = RecordingSink()
    sink.dispatch_exception = RuntimeError("boom")

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.FAILURE)

    assert sink._dispatcher is not None
    assert sink._dispatcher.done()

    await asyncio.wait_for(sink._flush_core(), timeout=TIMEOUT)


@pytest.mark.asyncio
async def test_h06_flush_does_not_hang_if_dispatcher_finishes_before_queue_join() -> None:
    events = [cast(LogEvent, object()) for _ in range(3)]

    sink = RecordingSink()
    sink.fail_on_dispatch_numbers = {1}
    sink.dispatch_exception = RuntimeError("boom")

    await sink.start()

    for event in events:
        sink.log(event)

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.FAILURE)

    await asyncio.wait_for(sink._flush_core(), timeout=TIMEOUT)

    assert sink._dispatcher is not None
    assert sink._dispatcher.done()


@pytest.mark.asyncio
async def test_h07_flush_cancels_queue_join_task_when_dispatcher_wins_race() -> None:
    sink = RecordingSink()
    sink.dispatch_exception = RuntimeError("boom")

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.FAILURE)

    await sink._flush_core()

    queue_join_tasks = [
        task for task in asyncio.all_tasks() if task.get_name() == f"{sink._namespace}.queue_join"
    ]

    assert queue_join_tasks == []


@pytest.mark.asyncio
async def test_h08_stop_flushes_multiple_events_when_dispatch_is_slow_but_successful() -> None:
    events = [cast(LogEvent, object()) for _ in range(5)]

    sink = RecordingSink()
    sink.block_dispatch = True

    await sink.start()

    for event in events:
        sink.log(event)

    await wait_thread_event_async(sink.dispatch_entered)

    stop_handle = sink.stop()

    await asyncio.sleep(0.05)

    assert not stop_handle._future.done()
    assert sink.get_status() is AsyncioLogSinkState.STOPPING

    sink.dispatch_release.set()

    outcome = await stop_handle

    assert outcome.success is True
    assert sink.dispatched == events
    assert sink._pending_counter == 0


@pytest.mark.asyncio
async def test_h09_stop_does_not_call_on_stopped_before_flush_finishes() -> None:
    sink = RecordingSink()
    sink.block_dispatch = True

    await sink.start()

    sink.log(EVENT)

    await wait_thread_event_async(sink.dispatch_entered)

    stop_handle = sink.stop()

    await asyncio.sleep(0.05)

    assert sink.on_stopped_count == 0

    sink.dispatch_release.set()

    outcome = await stop_handle

    assert outcome.success is True
    assert sink.on_stopped_count == 1
    assert sink.dispatched == [EVENT]


@pytest.mark.asyncio
async def test_h10_stop_with_empty_queue_stops_dispatcher_and_calls_on_stopped() -> None:
    sink = RecordingSink()

    await sink.start()

    outcome = await sink.stop()

    assert outcome.success is True
    assert sink.dispatched == []
    assert sink.on_stopped_count == 1
    assert sink._dispatcher is None
    assert sink.get_status() is AsyncioLogSinkState.STOPPED


# I. Cleanup after abnormal dispatcher termination


@pytest.mark.asyncio
async def test_i01_cleanup_runs_after_dispatch_failure() -> None:
    sink = RecordingSink()
    sink.dispatch_exception = RuntimeError("boom")

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.FAILURE)
    await wait_until(lambda: sink.on_stopped_count == 1)

    assert isinstance(sink._last_error, AsyncioLogSinkUnexpectedError)
    assert sink._dispatcher is not None
    assert sink._dispatcher.done()


@pytest.mark.asyncio
async def test_i02_cleanup_runs_after_dispatcher_external_cancellation() -> None:
    sink = RecordingSink()

    await sink.start()

    assert sink._dispatcher is not None

    sink._dispatcher.cancel()

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.CANCELLED)
    await wait_until(lambda: sink.on_stopped_count == 1)

    assert isinstance(sink._last_error, AsyncioLogSinkDispatcherCancelledError)
    assert sink._dispatcher.done()


@pytest.mark.asyncio
async def test_i03_cleanup_is_not_run_after_normal_stop_cancellation() -> None:
    sink = RecordingSink()

    await sink.start()

    outcome = await sink.stop()

    assert outcome.success is True

    await asyncio.sleep(0.05)

    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert sink.on_stopped_count == 1
    assert sink._last_error is None


@pytest.mark.asyncio
async def test_i04_cleanup_suppresses_on_stopped_ordinary_exception() -> None:
    sink = RecordingSink()
    sink.dispatch_exception = RuntimeError("dispatch failed")
    sink.stopped_exception = RuntimeError("cleanup failed")

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.FAILURE)
    await wait_until(lambda: sink.on_stopped_count == 1)

    assert sink.get_status() is AsyncioLogSinkState.FAILURE
    assert isinstance(sink._last_error, AsyncioLogSinkUnexpectedError)


@pytest.mark.asyncio
async def test_i05_cleanup_suppresses_on_stopped_domain_exception() -> None:
    sink = RecordingSink()
    sink.dispatch_exception = RuntimeError("dispatch failed")
    sink.stopped_exception = make_domain_error()

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.FAILURE)
    await wait_until(lambda: sink.on_stopped_count == 1)

    assert sink.get_status() is AsyncioLogSinkState.FAILURE
    assert isinstance(sink._last_error, AsyncioLogSinkUnexpectedError)


@pytest.mark.asyncio
async def test_i06_cleanup_task_name_uses_namespace_after_dispatch_failure() -> None:
    sink = RecordingSink(namespace="test.cleanup")
    sink.dispatch_exception = RuntimeError("dispatch failed")
    sink.block_stopped = True

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.FAILURE)

    await wait_until(
        lambda: any(task.get_name() == "test.cleanup.cleanup" for task in asyncio.all_tasks())
    )

    sink.stopped_release.set()

    await wait_until(lambda: sink.on_stopped_count == 1)


@pytest.mark.asyncio
async def test_i07_cleanup_task_name_uses_namespace_after_dispatcher_cancellation() -> None:
    sink = RecordingSink(namespace="test.cancel.cleanup")
    sink.block_stopped = True

    await sink.start()

    assert sink._dispatcher is not None

    sink._dispatcher.cancel()

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.CANCELLED)

    await wait_until(
        lambda: any(
            task.get_name() == "test.cancel.cleanup.cleanup" for task in asyncio.all_tasks()
        )
    )

    sink.stopped_release.set()

    await wait_until(lambda: sink.on_stopped_count == 1)


@pytest.mark.asyncio
async def test_i08_cleanup_after_dispatch_failure_does_not_clear_failure_state() -> None:
    sink = RecordingSink()
    sink.dispatch_exception = RuntimeError("dispatch failed")

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.FAILURE)
    await wait_until(lambda: sink.on_stopped_count == 1)

    assert sink.get_status() is AsyncioLogSinkState.FAILURE


@pytest.mark.asyncio
async def test_i09_cleanup_after_dispatcher_cancellation_does_not_clear_cancelled_state() -> None:
    sink = RecordingSink()

    await sink.start()

    assert sink._dispatcher is not None

    sink._dispatcher.cancel()

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.CANCELLED)
    await wait_until(lambda: sink.on_stopped_count == 1)

    assert sink.get_status() is AsyncioLogSinkState.CANCELLED


@pytest.mark.asyncio
async def test_i10_cleanup_is_started_only_once_for_single_dispatcher_failure() -> None:
    sink = RecordingSink()
    sink.dispatch_exception = RuntimeError("dispatch failed")

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.FAILURE)
    await wait_until(lambda: sink.on_stopped_count == 1)

    await asyncio.sleep(0.05)

    assert sink.on_stopped_count == 1


# J. Restart semantics


@pytest.mark.asyncio
async def test_j01_start_after_stopped_succeeds() -> None:
    sink = RecordingSink()

    await sink.start()
    await sink.stop()

    outcome = await sink.start()

    assert outcome.success is True
    assert sink.get_status() is AsyncioLogSinkState.RUNNING
    assert sink.on_starting_count == 2
    assert sink._dispatcher is not None
    assert not sink._dispatcher.done()

    await sink.stop()


@pytest.mark.asyncio
async def test_j02_dispatcher_after_restart_is_new_task() -> None:
    sink = RecordingSink()

    await sink.start()

    first_dispatcher = sink._dispatcher

    assert first_dispatcher is not None

    await sink.stop()
    await sink.start()

    second_dispatcher = sink._dispatcher

    assert second_dispatcher is not first_dispatcher
    assert second_dispatcher is not None
    assert not second_dispatcher.done()

    await sink.stop()


@pytest.mark.asyncio
async def test_j03_log_after_restart_works() -> None:
    sink = RecordingSink()

    await sink.start()
    await sink.stop()

    sink.dispatched.clear()

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.dispatched == [EVENT])

    assert sink._pending_counter == 0
    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    await sink.stop()


@pytest.mark.asyncio
async def test_j04_stop_after_restart_works() -> None:
    sink = RecordingSink()

    await sink.start()
    await sink.stop()
    await sink.start()

    outcome = await sink.stop()

    assert outcome.success is True
    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert sink.on_stopped_count == 2
    assert sink._dispatcher is None


@pytest.mark.asyncio
async def test_j05_failure_sink_cannot_restart() -> None:
    sink = RecordingSink()
    sink.starting_exception = RuntimeError("boom")

    first_outcome = await sink.start()

    assert first_outcome.success is False
    assert sink.get_status() is AsyncioLogSinkState.FAILURE

    second_outcome = await sink.start()

    assert second_outcome.success is False
    assert isinstance(second_outcome.error, AsyncioLogSinkInvalidStateError)
    assert sink.get_status() is AsyncioLogSinkState.FAILURE


@pytest.mark.asyncio
async def test_j06_cancelled_sink_cannot_restart() -> None:
    sink = RecordingSink()

    await sink.start()

    assert sink._dispatcher is not None

    sink._dispatcher.cancel()

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.CANCELLED)

    outcome = await sink.start()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioLogSinkInvalidStateError)
    assert sink.get_status() is AsyncioLogSinkState.CANCELLED


@pytest.mark.asyncio
async def test_j07_restart_after_stop_calls_on_starting_again() -> None:
    sink = RecordingSink()

    await sink.start()

    assert sink.on_starting_count == 1

    await sink.stop()
    await sink.start()

    assert sink.on_starting_count == 2

    await sink.stop()


@pytest.mark.asyncio
async def test_j08_restart_after_stop_calls_on_stopped_again_on_second_stop() -> None:
    sink = RecordingSink()

    await sink.start()
    await sink.stop()

    assert sink.on_stopped_count == 1

    await sink.start()
    await sink.stop()

    assert sink.on_stopped_count == 2


@pytest.mark.asyncio
async def test_j09_restart_after_stop_preserves_queue_and_allows_new_events() -> None:
    event_1 = cast(LogEvent, object())
    event_2 = cast(LogEvent, object())

    sink = RecordingSink()

    await sink.start()

    sink.log(event_1)

    await wait_until(lambda: sink.dispatched == [event_1])

    await sink.stop()
    await sink.start()

    sink.log(event_2)

    await wait_until(lambda: sink.dispatched == [event_1, event_2])

    assert sink._pending_counter == 0

    await sink.stop()


@pytest.mark.asyncio
async def test_j10_restart_after_stop_does_not_reuse_old_stop_future() -> None:
    sink = RecordingSink()

    await sink.start()
    await sink.stop()

    assert sink._stop_future is None

    await sink.start()

    assert sink._stop_future is None
    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    await sink.stop()


@pytest.mark.asyncio
async def test_j11_restart_after_stop_does_not_reuse_old_start_future() -> None:
    sink = RecordingSink()

    await sink.start()

    assert sink._start_future is None

    await sink.stop()
    await sink.start()

    assert sink._start_future is None
    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    await sink.stop()


# K. Cross-thread manual usage

# K. Cross-thread manual usage


@pytest.mark.asyncio
async def test_k01_start_can_be_called_from_another_thread() -> None:
    sink = RecordingSink()

    result = await run_in_thread_async(lambda: sink.start().wait())

    assert result.success is True
    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    await sink.stop()


@pytest.mark.asyncio
async def test_k02_stop_can_be_called_from_another_thread() -> None:
    sink = RecordingSink()

    await sink.start()

    result = await run_in_thread_async(lambda: sink.stop().wait())

    assert result.success is True
    assert sink.get_status() is AsyncioLogSinkState.STOPPED


@pytest.mark.asyncio
async def test_k03_log_can_be_called_from_another_thread_when_running() -> None:
    sink = RecordingSink()

    await sink.start()

    await run_in_thread_async(lambda: sink.log(EVENT))

    await wait_until(lambda: sink.dispatched == [EVENT])

    assert sink._pending_counter == 0

    await sink.stop()


@pytest.mark.asyncio
async def test_k04_many_threads_call_log_concurrently() -> None:
    thread_count = 10
    events_per_thread = 20
    events = [cast(LogEvent, object()) for _ in range(thread_count * events_per_thread)]

    sink = RecordingSink(queue_max_size=len(events))

    await sink.start()

    def worker(index: int) -> None:
        base = index * events_per_thread

        for offset in range(events_per_thread):
            sink.log(events[base + offset])

    await run_many_threads_async(thread_count, worker)

    await wait_until(lambda: len(sink.dispatched) == len(events))

    assert set(map(id, sink.dispatched)) == set(map(id, events))
    assert sink._pending_counter == 0

    await sink.stop()


@pytest.mark.asyncio
async def test_k05_many_threads_call_start_concurrently_on_virgin() -> None:
    sink = RecordingSink()
    sink.block_starting = True

    def worker(_: int) -> log_sink_pack.AsyncioLogSinkOpResult:
        return sink.start().wait()

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(worker, index) for index in range(10)]

        await wait_thread_event_async(sink.starting_entered)

        sink.starting_release.set()

        results = await asyncio.gather(
            *[asyncio.to_thread(future.result, TIMEOUT) for future in futures]
        )

    assert all(result.success for result in results)
    assert sink.on_starting_count == 1
    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    await sink.stop()


@pytest.mark.asyncio
async def test_k06_many_threads_call_stop_concurrently_on_running() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.block_stopped = True

    def worker(_: int) -> log_sink_pack.AsyncioLogSinkOpResult:
        return sink.stop().wait()

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(worker, index) for index in range(10)]

        await wait_thread_event_async(sink.stopped_entered)

        sink.stopped_release.set()

        results = await asyncio.gather(
            *[asyncio.to_thread(future.result, TIMEOUT) for future in futures]
        )

    assert all(result.success for result in results)
    assert sink.on_stopped_count == 1
    assert sink.get_status() is AsyncioLogSinkState.STOPPED


@pytest.mark.asyncio
async def test_k07_get_status_is_safe_from_another_thread() -> None:
    sink = RecordingSink()

    stop_reading = threading.Event()
    seen: list[AsyncioLogSinkState] = []

    def reader() -> None:
        while not stop_reading.is_set():
            seen.append(sink.get_status())

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(reader)

        await sink.start()
        await sink.stop()

        stop_reading.set()

        await asyncio.to_thread(future.result, TIMEOUT)

    assert seen
    assert all(isinstance(state, AsyncioLogSinkState) for state in seen)


@pytest.mark.asyncio
async def test_k08_log_from_another_thread_during_stopping_fails_cleanly() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.block_stopped = True

    stop_handle = sink.stop()

    await wait_thread_event_async(sink.stopped_entered)

    def worker() -> type[BaseException] | None:
        try:
            sink.log(EVENT)
        except BaseException as exc:
            return type(exc)

        return None

    result = await run_in_thread_async(worker)

    assert result is AsyncioLogSinkInvalidStateError

    sink.stopped_release.set()

    assert (await stop_handle).success is True


@pytest.mark.asyncio
async def test_k09_log_from_another_thread_after_stopped_fails_cleanly() -> None:
    sink = RecordingSink()

    await sink.start()
    await sink.stop()

    def worker() -> type[BaseException] | None:
        try:
            sink.log(EVENT)
        except BaseException as exc:
            return type(exc)

        return None

    result = await run_in_thread_async(worker)

    assert result is AsyncioLogSinkInvalidStateError


@pytest.mark.asyncio
async def test_k10_wait_handle_returned_in_worker_thread_can_be_awaited_in_loop_thread() -> None:
    sink = RecordingSink()

    handle = await run_in_thread_async(lambda: sink.start())

    result = await handle

    assert result.success is True
    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    await sink.stop()


@pytest.mark.asyncio
async def test_k11_wait_handle_returned_in_loop_thread_can_be_waited_in_worker_thread() -> None:
    sink = RecordingSink()

    handle = sink.start()

    result = await run_in_thread_async(lambda: handle.wait())

    assert result.success is True
    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    await sink.stop()


@pytest.mark.asyncio
async def test_k12_start_from_another_thread_while_starting_joins_existing_start() -> None:
    sink = RecordingSink()
    sink.block_starting = True

    first_handle = sink.start()

    await wait_thread_event_async(sink.starting_entered)

    second_handle = await run_in_thread_async(lambda: sink.start())

    assert sink.on_starting_count == 1
    assert sink.get_status() is AsyncioLogSinkState.STARTING

    sink.starting_release.set()

    first_result = await first_handle
    second_result = await second_handle

    assert first_result.success is True
    assert second_result.success is True
    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    await sink.stop()


@pytest.mark.asyncio
async def test_k13_stop_from_another_thread_while_stopping_joins_existing_stop() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.block_stopped = True

    first_handle = sink.stop()

    await wait_thread_event_async(sink.stopped_entered)

    second_handle = await run_in_thread_async(lambda: sink.stop())

    assert sink.on_stopped_count == 1
    assert sink.get_status() is AsyncioLogSinkState.STOPPING

    sink.stopped_release.set()

    first_result = await first_handle
    second_result = await second_handle

    assert first_result.success is True
    assert second_result.success is True
    assert sink.get_status() is AsyncioLogSinkState.STOPPED


@pytest.mark.asyncio
async def test_k14_log_from_another_thread_in_virgin_triggers_lazy_start() -> None:
    sink = RecordingSink()

    await run_in_thread_async(lambda: sink.log(EVENT))

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.RUNNING)
    await wait_until(lambda: sink.dispatched == [EVENT])

    assert sink.on_starting_count == 1
    assert sink._pending_counter == 0

    await sink.stop()


@pytest.mark.asyncio
async def test_k15_log_from_another_thread_in_starting_is_buffered() -> None:
    sink = RecordingSink()
    sink.block_starting = True

    start_handle = sink.start()

    await wait_thread_event_async(sink.starting_entered)

    await run_in_thread_async(lambda: sink.log(EVENT))

    assert sink.get_status() is AsyncioLogSinkState.STARTING
    assert sink._pending_counter == 1
    assert sink.dispatched == []

    sink.starting_release.set()

    assert (await start_handle).success is True

    await wait_until(lambda: sink.dispatched == [EVENT])

    assert sink._pending_counter == 0

    await sink.stop()


# L. Factory create()


def test_l01_create_returns_sink_and_terminator() -> None:
    sink, terminator = create_recording_sink()

    try:
        assert isinstance(sink, RecordingSink)
        assert callable(terminator)

    finally:
        terminator()


def test_l02_create_returns_already_running_sink() -> None:
    sink, terminator = create_recording_sink()

    try:
        assert sink.get_status() is AsyncioLogSinkState.RUNNING

    finally:
        terminator()


def test_l03_create_calls_on_starting_exactly_once() -> None:
    sink, terminator = create_recording_sink()

    try:
        assert sink.on_starting_count == 1

    finally:
        terminator()


def test_l04_create_creates_sink_in_dedicated_thread() -> None:
    test_thread_id = threading.get_ident()

    sink, terminator = create_recording_sink()

    try:
        assert sink.created_thread_id != test_thread_id

    finally:
        terminator()


def test_l05_create_creates_sink_in_dedicated_loop() -> None:
    sink, terminator = create_recording_sink()

    try:
        assert sink.created_loop is sink.created_event_loop

    finally:
        terminator()


def test_l06_factory_created_dispatcher_runs_on_dedicated_loop() -> None:
    sink, terminator = create_recording_sink()

    try:
        sink.log(EVENT)

        wait_thread_event(sink.dispatch_finished)

        assert sink.dispatch_thread_ids == [sink.created_thread_id]
        assert sink.dispatch_loops == [sink.created_loop]

    finally:
        terminator()


def test_l07_create_bootstrap_ordinary_exception_raises_runtime_error() -> None:
    with pytest.raises(RuntimeError) as exc_info:
        BootstrapFailingSink.create()

    assert "runtime creation failed: sink bootstrap failed" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, RuntimeError)


def test_l08_create_bootstrap_domain_error_raises_runtime_error() -> None:
    with pytest.raises(RuntimeError) as exc_info:
        BootstrapDomainFailingSink.create()

    assert "runtime creation failed: sink bootstrap failed" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, AsyncioLogSinkInvalidStateError)


def test_l09_create_startup_failure_raises_runtime_error() -> None:
    with pytest.raises(RuntimeError) as exc_info:
        StartingFailingFactorySink.create()

    assert "runtime creation failed: sink startup failed" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, AsyncioLogSinkOnStartingHookFailedError)


def test_l10_create_startup_failure_returns_control_without_hanging() -> None:
    with pytest.raises(RuntimeError):
        StartingFailingFactorySink.create()


def test_l11_create_with_custom_kwargs_passes_kwargs_to_sink() -> None:
    sink, terminator = create_recording_sink(marker="x", queue_max_size=7)

    try:
        assert sink.marker == "x"
        assert sink._max_pending_counter == 7

    finally:
        terminator()


def test_l12_factory_sink_uses_custom_namespace() -> None:
    sink, terminator = create_recording_sink(namespace="factory.ns")

    try:
        assert sink._namespace == "factory.ns"

        dispatcher = sink._dispatcher

        assert dispatcher is not None
        assert dispatcher.get_name() == "factory.ns.dispatching_loop"

    finally:
        terminator()


def test_l13_factory_created_sink_can_log_from_creator_thread() -> None:
    sink, terminator = create_recording_sink()

    try:
        sink.log(EVENT)

        wait_thread_event(sink.dispatch_finished)

        assert sink.dispatched == [EVENT]
        assert sink._pending_counter == 0

    finally:
        terminator()


def test_l14_factory_created_sink_can_be_stopped_directly() -> None:
    sink, terminator = create_recording_sink()

    try:
        outcome = sink.stop().wait()

        assert outcome.success is True
        assert sink.get_status() is AsyncioLogSinkState.STOPPED
        assert sink.on_stopped_count == 1

    finally:
        terminator()


def test_l15_factory_created_sink_start_after_direct_stop_restarts_inside_factory_loop() -> None:
    sink, terminator = create_recording_sink()

    try:
        assert sink.stop().wait().success is True

        outcome = sink.start().wait()

        assert outcome.success is True
        assert sink.get_status() is AsyncioLogSinkState.RUNNING
        assert sink.on_starting_count == 2

    finally:
        terminator()


def test_l16_factory_create_with_small_queue_limit_preserves_limit() -> None:
    sink, terminator = create_recording_sink(queue_max_size=1)

    try:
        assert sink._max_pending_counter == 1

    finally:
        terminator()


# M. Factory terminator


def test_m01_terminator_stops_running_sink() -> None:
    sink, terminator = create_recording_sink()

    terminator()

    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert sink.on_stopped_count == 1
    assert sink._dispatcher is None


def test_m02_terminator_is_idempotent() -> None:
    sink, terminator = create_recording_sink()

    terminator()
    terminator()
    terminator()

    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert sink.on_stopped_count == 1


def test_m03_concurrent_terminator_calls_are_idempotent() -> None:
    sink, terminator = create_recording_sink()

    results = run_many_threads(
        10,
        lambda _: terminator(),
    )

    assert results == [None] * 10
    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert sink.on_stopped_count == 1


def test_m04_terminator_after_external_stop_does_not_stop_twice() -> None:
    sink, terminator = create_recording_sink()

    stop_outcome = sink.stop().wait()

    assert stop_outcome.success is True
    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert sink.on_stopped_count == 1

    terminator()

    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert sink.on_stopped_count == 1


def test_m05_terminator_joins_existing_stopping_operation() -> None:
    sink, terminator = create_recording_sink()

    sink.block_stopped = True

    stop_handle = sink.stop()

    wait_thread_event(sink.stopped_entered)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(terminator)

        time.sleep(0.05)

        assert not future.done()
        assert sink.get_status() is AsyncioLogSinkState.STOPPING
        assert sink.on_stopped_count == 1

        # noinspection PyTypeChecker
        sink.created_loop.call_soon_threadsafe(sink.stopped_release.set)

        assert future.result(timeout=TIMEOUT) is None

    stop_outcome = stop_handle.wait()

    assert stop_outcome.success is True
    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert sink.on_stopped_count == 1


def test_m06_terminator_after_failure_stops_runtime_without_calling_stop_again() -> None:
    sink, terminator = create_recording_sink()

    sink.dispatch_exception = RuntimeError("dispatch failed")

    sink.log(EVENT)

    wait_thread_event(sink.dispatch_entered)

    deadline = time.monotonic() + TIMEOUT

    while sink.get_status() is not AsyncioLogSinkState.FAILURE:
        if time.monotonic() >= deadline:
            raise AssertionError("sink did not enter FAILURE in time")

        time.sleep(0.005)

    terminator()

    assert sink.get_status() is AsyncioLogSinkState.FAILURE


def test_m07_terminator_after_cancelled_stops_runtime_without_calling_stop_again() -> None:
    sink, terminator = create_recording_sink()

    dispatcher = sink._dispatcher

    assert dispatcher is not None

    # noinspection PyTypeChecker
    sink.created_loop.call_soon_threadsafe(dispatcher.cancel)

    deadline = time.monotonic() + TIMEOUT

    while sink.get_status() is not AsyncioLogSinkState.CANCELLED:
        if time.monotonic() >= deadline:
            raise AssertionError("sink did not enter CANCELLED in time")

        time.sleep(0.005)

    terminator()

    assert sink.get_status() is AsyncioLogSinkState.CANCELLED


def test_m08_terminator_propagates_stop_failure() -> None:
    sink, terminator = cast(
        tuple[StoppedFailingFactorySink, LogSinkTerminator],
        StoppedFailingFactorySink.create(),
    )

    with pytest.raises(AsyncioLogSinkOnStoppedHookFailedError):
        terminator()

    assert sink.get_status() is AsyncioLogSinkState.FAILURE
    assert sink.on_stopped_count == 1


def test_m09_terminator_is_marked_terminated_even_if_stop_fails() -> None:
    sink, terminator = cast(
        tuple[StoppedFailingFactorySink, LogSinkTerminator],
        StoppedFailingFactorySink.create(),
    )

    with pytest.raises(AsyncioLogSinkOnStoppedHookFailedError):
        terminator()

    terminator()

    assert sink.get_status() is AsyncioLogSinkState.FAILURE
    assert sink.on_stopped_count == 1


def test_m10_terminator_cannot_be_called_from_own_event_loop_thread() -> None:
    sink, terminator = create_recording_sink()

    result_future: concurrent.futures.Future[type[BaseException] | None] = (
        concurrent.futures.Future()
    )

    def call_terminator() -> None:
        try:
            terminator()
        except BaseException as exc:
            result_future.set_result(type(exc))
            return

        result_future.set_result(None)

    # noinspection PyTypeChecker
    sink.created_loop.call_soon_threadsafe(call_terminator)

    assert result_future.result(timeout=TIMEOUT) is RuntimeError

    terminator()

    assert sink.get_status() is AsyncioLogSinkState.STOPPED


def test_m11_terminator_after_direct_stop_then_restart_stops_running_sink() -> None:
    sink, terminator = create_recording_sink()

    assert sink.stop().wait().success is True
    assert sink.start().wait().success is True

    terminator()

    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert sink.on_stopped_count == 2


def test_m12_terminator_after_log_flushes_accepted_event_before_shutdown() -> None:
    sink, terminator = create_recording_sink()

    sink.log(EVENT)

    terminator()

    assert sink.dispatched == [EVENT]
    assert sink._pending_counter == 0
    assert sink.get_status() is AsyncioLogSinkState.STOPPED


def test_m13_terminator_waits_for_slow_dispatch_before_shutdown() -> None:
    sink, terminator = create_recording_sink()

    sink.block_dispatch = True

    sink.log(EVENT)

    wait_thread_event(sink.dispatch_entered)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(terminator)

        time.sleep(0.05)

        assert not future.done()
        assert sink.get_status() is AsyncioLogSinkState.STOPPING
        assert sink.dispatched == []

        # noinspection PyTypeChecker
        sink.created_loop.call_soon_threadsafe(sink.dispatch_release.set)

        assert future.result(timeout=TIMEOUT) is None

    assert sink.dispatched == [EVENT]
    assert sink._pending_counter == 0
    assert sink.get_status() is AsyncioLogSinkState.STOPPED


# N. Factory-created sink logging


def test_n01_log_immediately_after_create_succeeds() -> None:
    sink, terminator = create_recording_sink()

    try:
        sink.log(EVENT)

        wait_thread_event(sink.dispatch_finished)

        assert sink.dispatched == [EVENT]
        assert sink._pending_counter == 0
        assert sink.get_status() is AsyncioLogSinkState.RUNNING

    finally:
        terminator()


def test_n02_log_from_creator_thread_goes_to_dedicated_loop() -> None:
    sink, terminator = create_recording_sink()

    try:
        sink.log(EVENT)

        wait_thread_event(sink.dispatch_finished)

        assert sink.dispatch_thread_ids == [sink.created_thread_id]
        assert sink.dispatch_loops == [sink.created_loop]

    finally:
        terminator()


def test_n03_many_creator_thread_logs_after_create_are_delivered() -> None:
    events = [cast(LogEvent, object()) for _ in range(50)]

    sink, terminator = create_recording_sink(queue_max_size=len(events))

    try:
        for event in events:
            sink.log(event)

        deadline = time.monotonic() + TIMEOUT

        while len(sink.dispatched) != len(events):
            if time.monotonic() >= deadline:
                raise AssertionError("events were not dispatched in time")

            time.sleep(0.005)

        assert sink.dispatched == events
        assert sink._pending_counter == 0

    finally:
        terminator()


def test_n04_many_worker_thread_logs_after_create_are_delivered() -> None:
    thread_count = 10
    events_per_thread = 10
    events = [cast(LogEvent, object()) for _ in range(thread_count * events_per_thread)]

    sink, terminator = create_recording_sink(queue_max_size=len(events))

    try:

        def worker(index: int) -> None:
            base = index * events_per_thread

            for offset in range(events_per_thread):
                sink.log(events[base + offset])

        run_many_threads(thread_count, worker)

        deadline = time.monotonic() + TIMEOUT

        while len(sink.dispatched) != len(events):
            if time.monotonic() >= deadline:
                raise AssertionError("events were not dispatched in time")

            time.sleep(0.005)

        assert set(map(id, sink.dispatched)) == set(map(id, events))
        assert sink._pending_counter == 0

    finally:
        terminator()


def test_n05_log_after_terminator_fails() -> None:
    sink, terminator = create_recording_sink()

    terminator()

    with pytest.raises(AsyncioLogSinkInvalidStateError) as exc_info:
        sink.log(EVENT)

    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert exc_info.value.details["sink_state"] == AsyncioLogSinkState.STOPPED


def test_n06_log_during_terminator_stopping_fails() -> None:
    sink, terminator = create_recording_sink()

    sink.block_stopped = True

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(terminator)

        wait_thread_event(sink.stopped_entered)

        with pytest.raises(AsyncioLogSinkInvalidStateError) as exc_info:
            sink.log(EVENT)

        assert sink.get_status() is AsyncioLogSinkState.STOPPING
        assert exc_info.value.details["sink_state"] == AsyncioLogSinkState.STOPPING

        # noinspection PyTypeChecker
        sink.created_loop.call_soon_threadsafe(sink.stopped_release.set)

        assert future.result(timeout=TIMEOUT) is None

    assert sink.get_status() is AsyncioLogSinkState.STOPPED


def test_n07_overflow_raise_error_works_in_factory_created_sink() -> None:
    sink, terminator = create_recording_sink(queue_max_size=1)

    try:
        sink.block_dispatch = True

        sink.log(EVENT)

        wait_thread_event(sink.dispatch_entered)

        with pytest.raises(AsyncioLogSinkQueueOverflowError):
            sink.log(EVENT)

        assert sink._pending_counter == 1

        # noinspection PyTypeChecker
        sink.created_loop.call_soon_threadsafe(sink.dispatch_release.set)

        deadline = time.monotonic() + TIMEOUT

        while sink._pending_counter != 0:
            if time.monotonic() >= deadline:
                raise AssertionError("pending counter did not return to zero in time")

            time.sleep(0.005)

    finally:
        terminator()


def test_n08_overflow_drop_works_in_factory_created_sink() -> None:
    event_1 = cast(LogEvent, object())
    event_2 = cast(LogEvent, object())

    sink, terminator = create_recording_sink(
        queue_max_size=1,
        queue_overflow_policy=log_sink_pack.AsyncioLogSinkQueueOverflowPolicy.DROP,
    )

    try:
        sink.block_dispatch = True

        sink.log(event_1)

        wait_thread_event(sink.dispatch_entered)

        sink.log(event_2)

        assert sink._pending_counter == 1

        # noinspection PyTypeChecker
        sink.created_loop.call_soon_threadsafe(sink.dispatch_release.set)

        deadline = time.monotonic() + TIMEOUT

        while sink._pending_counter != 0:
            if time.monotonic() >= deadline:
                raise AssertionError("pending counter did not return to zero in time")

            time.sleep(0.005)

        assert sink.dispatched == [event_1]
        assert event_2 not in sink.dispatched

    finally:
        terminator()


def test_n09_log_storm_then_terminator_flushes_accepted_events() -> None:
    events = [cast(LogEvent, object()) for _ in range(50)]

    sink, terminator = create_recording_sink(queue_max_size=len(events))

    for event in events:
        sink.log(event)

    terminator()

    assert sink.dispatched == events
    assert sink._pending_counter == 0
    assert sink.get_status() is AsyncioLogSinkState.STOPPED


def test_n10_log_from_worker_thread_during_stopping_fails_cleanly() -> None:
    sink, terminator = create_recording_sink()

    sink.block_stopped = True

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        terminator_future = pool.submit(terminator)

        wait_thread_event(sink.stopped_entered)

        def worker() -> type[BaseException] | None:
            try:
                sink.log(EVENT)
            except BaseException as exc:
                return type(exc)

            return None

        log_future = pool.submit(worker)

        assert log_future.result(timeout=TIMEOUT) is AsyncioLogSinkInvalidStateError

        # noinspection PyTypeChecker
        sink.created_loop.call_soon_threadsafe(sink.stopped_release.set)

        assert terminator_future.result(timeout=TIMEOUT) is None

    assert sink.get_status() is AsyncioLogSinkState.STOPPED


def test_n11_log_from_worker_thread_after_terminator_fails_cleanly() -> None:
    sink, terminator = create_recording_sink()

    terminator()

    def worker() -> type[BaseException] | None:
        try:
            sink.log(EVENT)
        except BaseException as exc:
            return type(exc)

        return None

    assert run_in_thread(worker) is AsyncioLogSinkInvalidStateError


# O. Races


@pytest.mark.asyncio
async def test_o01_race_many_concurrent_start_calls() -> None:
    sink = RecordingSink()
    sink.block_starting = True

    def worker(_: int) -> log_sink_pack.AsyncioLogSinkOpResult:
        return sink.start().wait()

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        futures = [pool.submit(worker, index) for index in range(20)]

        await wait_thread_event_async(sink.starting_entered)

        assert sink.get_status() is AsyncioLogSinkState.STARTING
        assert sink.on_starting_count == 1

        sink.starting_release.set()

        results = [await asyncio.to_thread(future.result, TIMEOUT) for future in futures]

    assert all(result.success for result in results)
    assert sink.get_status() is AsyncioLogSinkState.RUNNING
    assert sink.on_starting_count == 1

    await sink.stop()


@pytest.mark.asyncio
async def test_o02_race_many_concurrent_stop_calls() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.block_stopped = True

    def worker(_: int) -> log_sink_pack.AsyncioLogSinkOpResult:
        return sink.stop().wait()

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        futures = [pool.submit(worker, index) for index in range(20)]

        await wait_thread_event_async(sink.stopped_entered)

        assert sink.get_status() is AsyncioLogSinkState.STOPPING
        assert sink.on_stopped_count == 1

        sink.stopped_release.set()

        results = [await asyncio.to_thread(future.result, TIMEOUT) for future in futures]

    assert all(result.success for result in results)
    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert sink.on_stopped_count == 1


@pytest.mark.asyncio
async def test_o03_race_start_vs_start_while_on_starting_blocked() -> None:
    sink = RecordingSink()
    sink.block_starting = True

    first_handle = sink.start()

    await wait_thread_event_async(sink.starting_entered)

    second_handle = sink.start()
    third_handle = await run_in_thread_async(lambda: sink.start())

    assert sink.get_status() is AsyncioLogSinkState.STARTING
    assert sink.on_starting_count == 1

    sink.starting_release.set()

    first_result = await first_handle
    second_result = await second_handle
    third_result = await third_handle

    assert first_result.success is True
    assert second_result.success is True
    assert third_result.success is True
    assert sink.get_status() is AsyncioLogSinkState.RUNNING
    assert sink.on_starting_count == 1

    await sink.stop()


@pytest.mark.asyncio
async def test_o04_race_stop_vs_stop_while_on_stopped_blocked() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.block_stopped = True

    first_handle = sink.stop()

    await wait_thread_event_async(sink.stopped_entered)

    second_handle = sink.stop()
    third_handle = await run_in_thread_async(lambda: sink.stop())

    assert sink.get_status() is AsyncioLogSinkState.STOPPING
    assert sink.on_stopped_count == 1

    sink.stopped_release.set()

    first_result = await first_handle
    second_result = await second_handle
    third_result = await third_handle

    assert first_result.success is True
    assert second_result.success is True
    assert third_result.success is True
    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert sink.on_stopped_count == 1


@pytest.mark.asyncio
async def test_o05_race_log_storm_while_running() -> None:
    thread_count = 10
    events_per_thread = 50
    events = [cast(LogEvent, object()) for _ in range(thread_count * events_per_thread)]

    sink = RecordingSink(queue_max_size=len(events))

    await sink.start()

    def worker(index: int) -> None:
        base = index * events_per_thread

        for offset in range(events_per_thread):
            sink.log(events[base + offset])

    await run_many_threads_async(thread_count, worker, timeout=5.0)

    await wait_until(lambda: len(sink.dispatched) == len(events), timeout=5.0)

    assert set(map(id, sink.dispatched)) == set(map(id, events))
    assert sink._pending_counter == 0
    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    await sink.stop()


@pytest.mark.asyncio
async def test_o06_race_log_storm_while_stop_begins() -> None:
    thread_count = 10
    events_per_thread = 50

    sink = RecordingSink(queue_max_size=thread_count * events_per_thread)
    sink.block_dispatch = True

    await sink.start()

    accepted_count = 0
    invalid_state_count = 0
    lock = threading.Lock()

    def logger_worker(_: int) -> None:
        nonlocal accepted_count
        nonlocal invalid_state_count

        for _event_index in range(events_per_thread):
            try:
                sink.log(cast(LogEvent, object()))
            except AsyncioLogSinkInvalidStateError:
                with lock:
                    invalid_state_count += 1
            else:
                with lock:
                    accepted_count += 1

    with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count + 1) as pool:
        log_futures = [pool.submit(logger_worker, index) for index in range(thread_count)]

        await wait_thread_event_async(sink.dispatch_entered)

        stop_future = pool.submit(lambda: sink.stop().wait())

        for future in log_futures:
            await asyncio.to_thread(future.result, 5.0)

        sink.dispatch_release.set()

        stop_result = await asyncio.to_thread(stop_future.result, 5.0)

    assert stop_result.success is True
    assert accepted_count + invalid_state_count == thread_count * events_per_thread
    assert invalid_state_count >= 0
    assert sink._pending_counter == 0
    assert len(sink.dispatched) == accepted_count
    assert sink.get_status() is AsyncioLogSinkState.STOPPED


@pytest.mark.asyncio
async def test_o07_race_log_exactly_around_stopping_transition_has_consistent_accounting() -> None:
    sink = RecordingSink(queue_max_size=100)
    sink.block_stopped = True

    await sink.start()

    stop_handle = sink.stop()

    await wait_thread_event_async(sink.stopped_entered)

    errors: list[type[BaseException]] = []

    def worker() -> None:
        try:
            sink.log(EVENT)
        except BaseException as exc:
            errors.append(type(exc))

    await run_in_thread_async(worker)

    assert errors == [AsyncioLogSinkInvalidStateError]
    assert sink._pending_counter == 0
    assert sink.get_status() is AsyncioLogSinkState.STOPPING

    sink.stopped_release.set()

    assert (await stop_handle).success is True


@pytest.mark.asyncio
async def test_o08_race_stop_while_dispatch_core_is_blocked_and_new_logs_arrive() -> None:
    sink = RecordingSink(queue_max_size=100)
    sink.block_dispatch = True

    await sink.start()

    first_event = cast(LogEvent, object())
    sink.log(first_event)

    await wait_thread_event_async(sink.dispatch_entered)

    stop_handle = sink.stop()

    await asyncio.sleep(0.05)

    assert sink.get_status() is AsyncioLogSinkState.STOPPING

    errors: list[type[BaseException]] = []

    def logger() -> None:
        try:
            sink.log(cast(LogEvent, object()))
        except BaseException as exc:
            errors.append(type(exc))

    await run_in_thread_async(logger)

    assert errors == [AsyncioLogSinkInvalidStateError]

    sink.dispatch_release.set()

    assert (await stop_handle).success is True
    assert sink.dispatched == [first_event]
    assert sink._pending_counter == 0
    assert sink.get_status() is AsyncioLogSinkState.STOPPED


@pytest.mark.asyncio
async def test_o09_race_dispatch_failure_while_logs_are_being_submitted() -> None:
    sink = RecordingSink(queue_max_size=1000)
    sink.fail_on_dispatch_numbers = {1}
    sink.dispatch_exception = RuntimeError("dispatch failed")

    await sink.start()

    accepted = 0
    invalid_state = 0
    lock = threading.Lock()

    def logger_worker(_: int) -> None:
        nonlocal accepted
        nonlocal invalid_state

        for _index in range(50):
            try:
                sink.log(cast(LogEvent, object()))
            except AsyncioLogSinkInvalidStateError:
                with lock:
                    invalid_state += 1
            else:
                with lock:
                    accepted += 1

    await run_many_threads_async(10, logger_worker, timeout=5.0)

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.FAILURE, timeout=5.0)

    assert accepted + invalid_state == 500
    assert accepted >= 1
    assert isinstance(sink._last_error, AsyncioLogSinkUnexpectedError)
    assert sink.get_status() is AsyncioLogSinkState.FAILURE


@pytest.mark.asyncio
async def test_o10_race_dispatcher_external_cancellation_while_logs_are_being_submitted() -> None:
    sink = RecordingSink(queue_max_size=1000)
    sink.block_dispatch = True

    await sink.start()

    sink.log(cast(LogEvent, object()))

    await wait_thread_event_async(sink.dispatch_entered)

    accepted = 0
    invalid_state = 0
    lock = threading.Lock()

    def logger_worker(_: int) -> None:
        nonlocal accepted
        nonlocal invalid_state

        for _index in range(50):
            try:
                sink.log(cast(LogEvent, object()))
            except AsyncioLogSinkInvalidStateError:
                with lock:
                    invalid_state += 1
            else:
                with lock:
                    accepted += 1

    with concurrent.futures.ThreadPoolExecutor(max_workers=11) as pool:
        futures = [pool.submit(logger_worker, index) for index in range(10)]

        assert sink._dispatcher is not None
        sink._dispatcher.cancel()

        for future in futures:
            await asyncio.to_thread(future.result, 5.0)

    sink.dispatch_release.set()

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.CANCELLED, timeout=5.0)

    assert accepted + invalid_state == 500
    assert isinstance(sink._last_error, AsyncioLogSinkDispatcherCancelledError)
    assert sink.get_status() is AsyncioLogSinkState.CANCELLED


@pytest.mark.asyncio
async def test_o11_race_start_during_stopping_fails() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.block_stopped = True

    stop_handle = sink.stop()

    await wait_thread_event_async(sink.stopped_entered)

    start_outcome = await sink.start()

    assert start_outcome.success is False
    assert isinstance(start_outcome.error, AsyncioLogSinkInvalidStateError)
    assert sink.get_status() is AsyncioLogSinkState.STOPPING

    sink.stopped_release.set()

    assert (await stop_handle).success is True


@pytest.mark.asyncio
async def test_o12_race_start_after_stopped_succeeds() -> None:
    sink = RecordingSink()

    await sink.start()
    await sink.stop()

    start_outcome = await sink.start()

    assert start_outcome.success is True
    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    await sink.stop()


@pytest.mark.asyncio
async def test_o13_race_get_status_during_lifecycle_transitions() -> None:
    sink = RecordingSink()

    stop_reading = threading.Event()
    seen: list[AsyncioLogSinkState] = []

    def reader() -> None:
        while not stop_reading.is_set():
            seen.append(sink.get_status())

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(reader)

        for _ in range(10):
            await sink.start()
            await sink.stop()

        stop_reading.set()

        await asyncio.to_thread(future.result, TIMEOUT)

    assert seen
    assert all(isinstance(state, AsyncioLogSinkState) for state in seen)


@pytest.mark.asyncio
async def test_o14_race_overflow_raise_error_under_multi_thread_log_storm() -> None:
    limit = 10
    thread_count = 20

    sink = RecordingSink(queue_max_size=limit)
    sink.block_dispatch = True

    await sink.start()

    accepted = 0
    overflow = 0
    lock = threading.Lock()

    def worker(_: int) -> None:
        nonlocal accepted
        nonlocal overflow

        try:
            sink.log(cast(LogEvent, object()))
        except AsyncioLogSinkQueueOverflowError:
            with lock:
                overflow += 1
        else:
            with lock:
                accepted += 1

    await run_many_threads_async(thread_count, worker)

    await wait_thread_event_async(sink.dispatch_entered)

    assert accepted == limit
    assert overflow == thread_count - limit
    assert sink._pending_counter == limit

    sink.dispatch_release.set()

    await wait_until(lambda: sink._pending_counter == 0)

    await sink.stop()


@pytest.mark.asyncio
async def test_o15_race_overflow_drop_under_multi_thread_log_storm() -> None:
    limit = 10
    thread_count = 20

    sink = RecordingSink(
        queue_max_size=limit,
        queue_overflow_policy=log_sink_pack.AsyncioLogSinkQueueOverflowPolicy.DROP,
    )
    sink.block_dispatch = True

    await sink.start()

    def worker(_: int) -> None:
        sink.log(cast(LogEvent, object()))

    await run_many_threads_async(thread_count, worker)

    await wait_thread_event_async(sink.dispatch_entered)

    assert sink._pending_counter == limit

    sink.dispatch_release.set()

    await wait_until(lambda: sink._pending_counter == 0)
    await wait_until(lambda: len(sink.dispatched) == limit)

    await sink.stop()


# P. State / error consistency


@pytest.mark.asyncio
async def test_p01_successful_start_clears_last_error() -> None:
    sink = RecordingSink()

    outcome = await sink.start()

    assert outcome.success is True
    assert sink.get_status() is AsyncioLogSinkState.RUNNING
    assert sink._last_error is None

    await sink.stop()


@pytest.mark.asyncio
async def test_p02_successful_stop_does_not_set_last_error() -> None:
    sink = RecordingSink()

    await sink.start()

    outcome = await sink.stop()

    assert outcome.success is True
    assert sink.get_status() is AsyncioLogSinkState.STOPPED
    assert sink._last_error is None


@pytest.mark.asyncio
async def test_p03_startup_failure_stores_last_error() -> None:
    sink = RecordingSink()
    sink.starting_exception = RuntimeError("boom")

    outcome = await sink.start()

    assert outcome.success is False
    assert sink.get_status() is AsyncioLogSinkState.FAILURE
    assert sink._last_error is outcome.error
    assert isinstance(sink._last_error, AsyncioLogSinkOnStartingHookFailedError)


@pytest.mark.asyncio
async def test_p04_stop_failure_stores_last_error() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.stopped_exception = RuntimeError("boom")

    outcome = await sink.stop()

    assert outcome.success is False
    assert sink.get_status() is AsyncioLogSinkState.FAILURE
    assert sink._last_error is outcome.error
    assert isinstance(sink._last_error, AsyncioLogSinkOnStoppedHookFailedError)


@pytest.mark.asyncio
async def test_p05_dispatch_failure_stores_last_error() -> None:
    sink = RecordingSink()
    sink.dispatch_exception = RuntimeError("boom")

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.FAILURE)

    assert isinstance(sink._last_error, AsyncioLogSinkUnexpectedError)


@pytest.mark.asyncio
async def test_p06_external_dispatcher_cancellation_stores_dispatcher_cancelled_error() -> None:
    sink = RecordingSink()

    await sink.start()

    dispatcher = sink._dispatcher

    assert dispatcher is not None

    dispatcher.cancel()

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.CANCELLED)

    assert isinstance(sink._last_error, AsyncioLogSinkDispatcherCancelledError)


@pytest.mark.asyncio
async def test_p07_failure_state_is_sticky_for_start_stop_and_log() -> None:
    sink = RecordingSink()
    sink.starting_exception = RuntimeError("boom")

    await sink.start()

    assert sink.get_status() is AsyncioLogSinkState.FAILURE

    start_outcome = await sink.start()
    stop_outcome = await sink.stop()

    assert start_outcome.success is False
    assert stop_outcome.success is False

    with pytest.raises(AsyncioLogSinkInvalidStateError):
        sink.log(EVENT)

    assert sink.get_status() is AsyncioLogSinkState.FAILURE


@pytest.mark.asyncio
async def test_p08_cancelled_state_is_sticky_for_start_stop_and_log() -> None:
    sink = RecordingSink()

    await sink.start()

    dispatcher = sink._dispatcher

    assert dispatcher is not None

    dispatcher.cancel()

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.CANCELLED)

    start_outcome = await sink.start()
    stop_outcome = await sink.stop()

    assert start_outcome.success is False
    assert stop_outcome.success is False

    with pytest.raises(AsyncioLogSinkInvalidStateError):
        sink.log(EVENT)

    assert sink.get_status() is AsyncioLogSinkState.CANCELLED


@pytest.mark.asyncio
async def test_p09_stopped_state_allows_start_but_rejects_log_before_restart() -> None:
    sink = RecordingSink()

    await sink.start()
    await sink.stop()

    with pytest.raises(AsyncioLogSinkInvalidStateError):
        sink.log(EVENT)

    start_outcome = await sink.start()

    assert start_outcome.success is True
    assert sink.get_status() is AsyncioLogSinkState.RUNNING

    await sink.stop()


@pytest.mark.asyncio
async def test_p10_start_invalid_state_expected_states_are_correct() -> None:
    sink = RecordingSink()

    await sink.start()

    outcome = await sink.start()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioLogSinkInvalidStateError)
    assert outcome.error.details["expected_states"] == (
        AsyncioLogSinkState.VIRGIN,
        AsyncioLogSinkState.STOPPED,
    )

    await sink.stop()


@pytest.mark.asyncio
async def test_p11_stop_invalid_state_expected_states_are_correct() -> None:
    sink = RecordingSink()

    outcome = await sink.stop()

    assert outcome.success is False
    assert isinstance(outcome.error, AsyncioLogSinkInvalidStateError)
    assert outcome.error.details["expected_states"] == (AsyncioLogSinkState.RUNNING,)


@pytest.mark.asyncio
async def test_p12_log_invalid_state_expected_states_are_correct() -> None:
    sink = RecordingSink()

    await sink.start()
    await sink.stop()

    with pytest.raises(AsyncioLogSinkInvalidStateError) as exc_info:
        sink.log(EVENT)

    assert exc_info.value.details["expected_states"] == (
        AsyncioLogSinkState.VIRGIN,
        AsyncioLogSinkState.STARTING,
        AsyncioLogSinkState.RUNNING,
    )


@pytest.mark.asyncio
async def test_p13_failure_log_error_carries_last_error_as_cause() -> None:
    sink = RecordingSink()
    sink.starting_exception = RuntimeError("boom")

    await sink.start()

    assert sink.get_status() is AsyncioLogSinkState.FAILURE
    assert sink._last_error is not None

    with pytest.raises(AsyncioLogSinkInvalidStateError) as exc_info:
        sink.log(EVENT)

    assert getattr(exc_info.value, "cause", None) is sink._last_error


@pytest.mark.asyncio
async def test_p14_non_failure_log_invalid_state_does_not_carry_last_error() -> None:
    sink = RecordingSink()

    await sink.start()
    await sink.stop()

    with pytest.raises(AsyncioLogSinkInvalidStateError) as exc_info:
        sink.log(EVENT)

    assert getattr(exc_info.value, "cause", None) is None


@pytest.mark.asyncio
async def test_p15_start_failure_does_not_leave_start_future() -> None:
    sink = RecordingSink()
    sink.starting_exception = RuntimeError("boom")

    outcome = await sink.start()

    assert outcome.success is False
    assert sink._start_future is None
    assert sink.get_status() is AsyncioLogSinkState.FAILURE


@pytest.mark.asyncio
async def test_p16_stop_failure_does_not_leave_stop_future() -> None:
    sink = RecordingSink()

    await sink.start()

    sink.stopped_exception = RuntimeError("boom")

    outcome = await sink.stop()

    assert outcome.success is False
    assert sink._stop_future is None
    assert sink.get_status() is AsyncioLogSinkState.FAILURE


@pytest.mark.asyncio
async def test_p17_successful_stop_clears_dispatcher_reference() -> None:
    sink = RecordingSink()

    await sink.start()

    assert sink._dispatcher is not None

    await sink.stop()

    assert sink._dispatcher is None


@pytest.mark.asyncio
async def test_p18_dispatch_failure_keeps_dispatcher_reference_done() -> None:
    sink = RecordingSink()
    sink.dispatch_exception = RuntimeError("boom")

    await sink.start()

    sink.log(EVENT)

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.FAILURE)

    dispatcher = sink._dispatcher

    assert dispatcher is not None
    assert dispatcher.done()


@pytest.mark.asyncio
async def test_p19_cancelled_dispatcher_keeps_dispatcher_reference_done() -> None:
    sink = RecordingSink()

    await sink.start()

    dispatcher = sink._dispatcher

    assert dispatcher is not None

    dispatcher.cancel()

    await wait_until(lambda: sink.get_status() is AsyncioLogSinkState.CANCELLED)

    dispatcher_after = sink._dispatcher

    assert dispatcher_after is not None
    assert dispatcher_after.done()


@pytest.mark.asyncio
async def test_p20_normal_restart_after_stopped_clears_last_error_still_none() -> None:
    sink = RecordingSink()

    await sink.start()
    await sink.stop()
    await sink.start()

    assert sink.get_status() is AsyncioLogSinkState.RUNNING
    assert sink._last_error is None

    await sink.stop()
