# src/mvx/networking/metrics/asyncio_metrics_recorder/metrics_recorder.py
from __future__ import annotations
from mvx.common.helpers.document_enum import document_enum

from typing import Any, Iterable
from collections.abc import Generator, Mapping
from dataclasses import dataclass
from enum import StrEnum

import threading
import asyncio
import concurrent.futures
import contextlib
from uuid import uuid4

from mvx.common.logger import LogContext

from ..metric import Metric
from ..metric_event import MetricEvent

from .common import AsyncioMetricsRecorderState

from .errors import (
    AsyncioMetricsRecorderError,
    AsyncioMetricsRecorderLoopUnavailableError,
    AsyncioMetricsRecorderInvalidStateError,
    AsyncioMetricsRecorderOnStartingHookFailedError,
    AsyncioMetricsRecorderStoppedHookFailedError,
    AsyncioMetricsRecorderQueueOverflowError,
    AsyncioMetricsRecorderDispatcherCancelledError,
    AsyncioMetricsRecorderUnexpectedError,
)

__all__ = (
    "AsyncioMetricsRecorderQueueOverflowPolicy",
    "AsyncioMetricsRecorderOp",
    "AsyncioMetricsRecorderOpResult",
    "AsyncioMetricsRecorderWaitHandle",
    "AsyncioMetricsRecorder",
)

DEFAULT_NAMESPACE = "mvx.common.metrics.asyncio_metrics_recorder"


@document_enum
class AsyncioMetricsRecorderQueueOverflowPolicy(StrEnum):
    """
    Queue overflow behavior for `AsyncioMetricsRecorder`.
    """

    #: Drop an event when the pending-event limit is reached.
    DROP = "DROP"

    #: Raise `AsyncioMetricsRecorderQueueOverflowError` when the pending-event limit is reached.
    RAISE_ERROR = "RAISE_ERROR"


DEFAULT_QUEUE_MAX_SIZE = 10_000


@document_enum
class AsyncioMetricsRecorderOp(StrEnum):
    """
    Lifecycle operation names reported by `AsyncioMetricsRecorderOpResult`.
    """

    #: Start operation.
    START = "START"

    #: Stop operation.
    STOP = "STOP"


@dataclass(frozen=True, slots=True)
class AsyncioMetricsRecorderOpResult:
    """
    Result of an `AsyncioMetricsRecorder` lifecycle operation.

    :param op_name: lifecycle operation name.
    :param success: whether the operation completed successfully.
    :param error: operation error, or None if the operation succeeded.
    """

    op_name: AsyncioMetricsRecorderOp
    success: bool
    error: AsyncioMetricsRecorderError | None = None


class AsyncioMetricsRecorderWaitHandle:
    """
    Wait handle returned by `AsyncioMetricsRecorder.start()` and `AsyncioMetricsRecorder.stop()`.

    The handle can be used synchronously through `wait()` or awaited from async
    code. Both forms return `AsyncioMetricsRecorderOpResult`.
    """

    def __init__(self, operation: AsyncioMetricsRecorderOp) -> None:
        """
        Create a wait handle for a lifecycle operation.

        :param operation: lifecycle operation represented by this handle.
        """
        self._future: concurrent.futures.Future[None] = concurrent.futures.Future()
        self._operation = operation

    def wait(self) -> AsyncioMetricsRecorderOpResult:
        """
        Wait synchronously for the lifecycle operation to finish.

        :return: lifecycle operation result.
        """
        try:
            self._future.result()
        except Exception as exc:
            return self._error_result(exc)

        return self._success_result()

    def __await__(self) -> Generator[Any, None, AsyncioMetricsRecorderOpResult]:
        return self._wait_async().__await__()

    async def _wait_async(self) -> AsyncioMetricsRecorderOpResult:
        try:
            await asyncio.shield(asyncio.wrap_future(self._future))
        except Exception as exc:
            return self._error_result(exc)

        return self._success_result()

    def _success_result(self) -> AsyncioMetricsRecorderOpResult:
        return AsyncioMetricsRecorderOpResult(
            op_name=self._operation,
            success=True,
        )

    def _error_result(self, exc: Exception) -> AsyncioMetricsRecorderOpResult:
        mapped_exc = (
            exc
            if isinstance(exc, AsyncioMetricsRecorderError)
            else AsyncioMetricsRecorderUnexpectedError(cause=exc)
        )

        return AsyncioMetricsRecorderOpResult(
            op_name=self._operation,
            success=False,
            error=mapped_exc,
        )


class _WaitHandleInternal(AsyncioMetricsRecorderWaitHandle):
    def done(self, exc: Exception | None) -> None:
        try:
            if exc is None:
                self._future.set_result(None)
            else:
                self._future.set_exception(exc)
        except concurrent.futures.InvalidStateError:
            pass

    def done_from_future(
        self,
        cf_future: concurrent.futures.Future[None],
    ) -> None:
        try:
            cf_future.result()

        except Exception as exc:
            self.done(exc)
            return

        self.done(None)


DEFAULT_THREAD_START_TIMEOUT_S = 5.0
DEFAULT_THREAD_JOIN_TIMEOUT_S = 5.0
DEFAULT_PENDING_TASKS_CANCEL_TIMEOUT_S = 5.0


class AsyncioMetricsRecorder:
    __slots__ = (
        "_entity_id",
        "_namespace",
        "_loop",
        "_state",
        "_queue",
        "_queue_overflow_policy",
        "_dispatcher",
        "_metrics_by_name",
        "_pending_counter",
        "_max_pending_counter",
        "_last_error",
        "_start_future",
        "_stop_future",
        "_thread_lock",
        "_log_context",
    )

    _state: AsyncioMetricsRecorderState
    _queue: asyncio.Queue[MetricEvent]
    _queue_overflow_policy: AsyncioMetricsRecorderQueueOverflowPolicy
    _dispatcher: asyncio.Task[None] | None

    def __init__(
        self,
        entity_id: str,
        *,
        namespace: str | None = None,
        queue_max_size: int | None = None,
        queue_overflow_policy: AsyncioMetricsRecorderQueueOverflowPolicy | None = None,
        log_context: LogContext | None = None,
    ) -> None:

        if entity_id is None:
            raise ValueError("argument 'entity_id' must not be None")

        if not isinstance(entity_id, str):
            raise TypeError("argument 'entity_id' must be string when provided")

        if namespace is not None:
            if not isinstance(namespace, str):
                raise TypeError("argument 'namespace' must be string when provided")

        if queue_max_size is not None:
            if isinstance(queue_max_size, bool) or not isinstance(queue_max_size, int):
                raise TypeError("argument 'queue_max_size' must be integer when provided")
            if queue_max_size <= 0:
                raise ValueError("argument 'queue_max_size' must be positive integer when provided")

        if queue_overflow_policy is not None:
            if not isinstance(queue_overflow_policy, AsyncioMetricsRecorderQueueOverflowPolicy):
                raise TypeError(
                    "argument 'queue_overflow_policy' must be an instance of 'AsyncioMetricsRecorderQueueOverflowPolicy' when provided"
                )

        if log_context is not None:
            if not isinstance(log_context, LogContext):
                raise TypeError(
                    "argument 'log_context' must be an instance of 'LogContext' when provided"
                )

        _entity_id = (entity_id or "").strip()
        self._entity_id = _entity_id or uuid4().hex[:8]

        self._namespace = namespace or DEFAULT_NAMESPACE

        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError as exc:
            raise AsyncioMetricsRecorderLoopUnavailableError() from exc

        self._state = AsyncioMetricsRecorderState.VIRGIN

        # Queue itself is intentionally unbounded.
        # Backpressure is controlled by _pending_counter, which includes scheduled
        # but not yet enqueued events.
        self._queue: asyncio.Queue[MetricEvent] = asyncio.Queue()

        self._metrics_by_name: dict[str, Metric] = {}

        self._queue_overflow_policy = (
            queue_overflow_policy or AsyncioMetricsRecorderQueueOverflowPolicy.RAISE_ERROR
        )
        self._pending_counter = 0
        self._max_pending_counter = queue_max_size or DEFAULT_QUEUE_MAX_SIZE

        self._dispatcher: asyncio.Task[None] | None = None

        self._last_error: AsyncioMetricsRecorderError | None = None

        self._start_future: concurrent.futures.Future[None] | None = None
        self._stop_future: concurrent.futures.Future[None] | None = None

        self._thread_lock = threading.Lock()

        self._log_context = log_context

    # ---- Properties ----------------------------------------------------------------------
    @property
    def entity_id(self) -> str:
        return self._entity_id

    # ---- Lifecycle public API ------------------------------------------------------------

    def get_status(self) -> AsyncioMetricsRecorderState:
        """
        Return the current lifecycle state.

        :return: current recorder state.
        """
        with self._thread_lock:
            return self._state

    def start(self) -> AsyncioMetricsRecorderWaitHandle:
        """
        Start the async recorder runtime.

        The method schedules startup on the recorder event loop and returns immediately
        with a wait handle. If startup is already in progress, the returned handle is
        attached to the same startup operation.

        :return: wait handle for the start operation.
        """
        handle = _WaitHandleInternal(operation=AsyncioMetricsRecorderOp.START)

        with self._thread_lock:
            current_state = self._state

            if (
                self._state is AsyncioMetricsRecorderState.STARTING
                and self._start_future is not None
            ):
                self._start_future.add_done_callback(handle.done_from_future)
                return handle

            if current_state not in (
                AsyncioMetricsRecorderState.VIRGIN,
                AsyncioMetricsRecorderState.STOPPED,
            ):
                handle.done(
                    exc=AsyncioMetricsRecorderInvalidStateError(
                        recorder_state=current_state,
                        expected_states=(
                            AsyncioMetricsRecorderState.VIRGIN,
                            AsyncioMetricsRecorderState.STOPPED,
                        ),
                    )
                )
                return handle

            start_future = asyncio.run_coroutine_threadsafe(self._start_core(), self._loop)
            start_future.add_done_callback(handle.done_from_future)
            self._start_future = start_future
            self._state = AsyncioMetricsRecorderState.STARTING

        return handle

    def stop(self) -> AsyncioMetricsRecorderWaitHandle:
        """
        Stop the async recorder runtime.

        The method schedules shutdown on the recorder event loop and returns immediately
        with a wait handle. Stop is valid only when the recorder is running.

        Shutdown flushes accepted events on a best-effort basis, stops the dispatcher,
        and runs the stop hook.

        :return: wait handle for the stop operation.
        """
        handle = _WaitHandleInternal(operation=AsyncioMetricsRecorderOp.STOP)

        with self._thread_lock:
            current_state = self._state

            if (
                current_state is AsyncioMetricsRecorderState.STOPPING
                and self._stop_future is not None
            ):
                self._stop_future.add_done_callback(handle.done_from_future)
                return handle

            if current_state is not AsyncioMetricsRecorderState.RUNNING:
                handle.done(
                    exc=AsyncioMetricsRecorderInvalidStateError(
                        recorder_state=current_state,
                        expected_states=(AsyncioMetricsRecorderState.RUNNING,),
                    )
                )
                return handle

            stop_future = asyncio.run_coroutine_threadsafe(self._stop_core(), self._loop)
            stop_future.add_done_callback(handle.done_from_future)
            self._stop_future = stop_future
            self._state = AsyncioMetricsRecorderState.STOPPING

            return handle

    # ---- Lifecycle internal realization --------------------------------------------------

    def _is_running_in_owning_loop(self) -> bool:
        try:
            return asyncio.get_running_loop() is self._loop
        except RuntimeError:
            return False

    async def _start_core(self) -> None:
        try:
            try:
                await self._on_starting()
            except Exception as exc:
                raise AsyncioMetricsRecorderOnStartingHookFailedError(
                    cause=exc,
                ) from exc

            dispatcher = self._loop.create_task(
                self._dispatching_loop(self._queue),
                name=f"{self._namespace}.dispatching_loop",
            )
            dispatcher.add_done_callback(self._on_dispatching_task_done)
            self._dispatcher = dispatcher

        except Exception as exc:
            mapped_exc = (
                exc
                if isinstance(exc, AsyncioMetricsRecorderError)
                else AsyncioMetricsRecorderUnexpectedError(cause=exc)
            )
            with self._thread_lock:
                self._last_error = mapped_exc
                self._start_future = None
                self._state = AsyncioMetricsRecorderState.FAILURE

            raise mapped_exc from exc
        else:
            with self._thread_lock:
                self._start_future = None

                if self._state in (
                    AsyncioMetricsRecorderState.FAILURE,
                    AsyncioMetricsRecorderState.CANCELLED,
                ):
                    assert self._last_error is not None
                    raise self._last_error

                self._last_error = None
                self._state = AsyncioMetricsRecorderState.RUNNING

    # ---- Startup and stopping hooks ------------------------------------------------------

    async def _on_starting(self) -> None:
        """
        Run backend-specific startup logic.

        This hook is called before the dispatcher is started. Subclasses may override
        it to open connections, create clients, or prepare backend resources.

        :return: None.
        """
        pass

    async def _flush_core(self) -> None:
        # Barrier: let already scheduled call_soon_threadsafe(queue.put_nowait, event)
        # callbacks run before we start waiting for queue.join().
        barrier = self._loop.create_future()
        self._loop.call_soon(barrier.set_result, None)
        await barrier

        # Flush is best-effort and must never block forever.
        #
        # queue.join() is safe to wait for only while the dispatcher is alive,
        # because the dispatcher is the only consumer that can call task_done().
        # Therefore we wait until either:
        #   1. all currently accepted events are processed, or
        #   2. the dispatcher finishes first.
        #
        # In the second case flushing cannot make further progress, so we stop
        # waiting and let the normal stopping/failure path handle the dispatcher
        # outcome.

        dispatcher = self._dispatcher
        if dispatcher is None or dispatcher.done():
            return

        queue_join_task = self._loop.create_task(
            self._queue.join(),
            name=f"{self._namespace}.queue_join",
        )

        try:
            _, _ = await asyncio.wait(
                {queue_join_task, dispatcher},
                return_when=asyncio.FIRST_COMPLETED,
            )

        finally:
            if not queue_join_task.done():
                queue_join_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await queue_join_task

    async def _stop_core(self) -> None:

        try:
            # Best effort to deliver events either already in the queue or scheduled via
            # call_soon_threadsafe(queue.put_nowait, event)
            await self._flush_core()

            # Stopping the dispatcher.
            dispatcher = self._dispatcher
            if dispatcher is not None and not dispatcher.done():
                dispatcher.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await dispatcher

            self._dispatcher = None

            # Calling on_stopped hook.
            try:
                await self._on_stopped()
            except Exception as exc:
                raise AsyncioMetricsRecorderStoppedHookFailedError(cause=exc) from exc

        except Exception as exc:
            mapped_exc = (
                exc
                if isinstance(exc, AsyncioMetricsRecorderError)
                else AsyncioMetricsRecorderUnexpectedError(cause=exc)
            )

            with self._thread_lock:
                self._stop_future = None
                self._last_error = mapped_exc
                self._state = AsyncioMetricsRecorderState.FAILURE

            raise mapped_exc from exc

        else:
            with self._thread_lock:
                self._stop_future = None
                if self._state not in (
                    AsyncioMetricsRecorderState.CANCELLED,
                    AsyncioMetricsRecorderState.FAILURE,
                ):
                    self._state = AsyncioMetricsRecorderState.STOPPED
                    return

                assert self._last_error is not None
                raise self._last_error

    async def _on_stopped(self) -> None:
        """
        Run backend-specific shutdown logic.

        This hook is called after the dispatcher has stopped during normal shutdown.
        Subclasses may override it to close connections, clients, handlers, or other
        backend resources.

        :return: None.
        """
        pass

    # ---- Dispatching loop ----------------------------------------------------------------

    async def _dispatch_core(self, event: MetricEvent) -> None:
        for metric in self._metrics_by_name.values():
            changed = metric.handle_event(event)
            if changed:
                await self._on_metric_changed(metric=metric, event=event)

    async def _dispatching_loop(self, queue: asyncio.Queue[MetricEvent]) -> None:
        while True:
            event = await queue.get()
            try:
                await self._dispatch_core(event)
            except Exception as exc:
                log_context = self._log_context
                if log_context is not None:
                    log_context.log_error_event(
                        event="metrics_recorder.dispatch_error",
                        payload=log_context.build_error_payload(exc),
                        entity_id=self._entity_id,
                        skip_payload_normalization=True,
                    )
                raise
            finally:
                queue.task_done()
                with self._thread_lock:
                    self._pending_counter -= 1

    def _on_dispatching_task_done(self, task: asyncio.Task[None]) -> None:
        start_clean_up = False

        try:
            task.result()
            return
        except asyncio.CancelledError:
            with self._thread_lock:
                if self._state is not AsyncioMetricsRecorderState.STOPPING:
                    self._last_error = AsyncioMetricsRecorderDispatcherCancelledError()
                    self._state = AsyncioMetricsRecorderState.CANCELLED
                    start_clean_up = True

        except Exception as exc:
            mapped_exc = (
                exc
                if isinstance(exc, AsyncioMetricsRecorderError)
                else AsyncioMetricsRecorderUnexpectedError(
                    cause=exc,
                )
            )
            with self._thread_lock:
                if self._state is not AsyncioMetricsRecorderState.STOPPING:
                    self._last_error = mapped_exc
                    self._state = AsyncioMetricsRecorderState.FAILURE
                    start_clean_up = True

        finally:
            if not start_clean_up:
                return

            try:
                self._loop.create_task(self._cleanup(), name=f"{self._namespace}.cleanup")
            except RuntimeError as exc:
                log_context = self._log_context
                if log_context is not None:
                    log_context.log_error_event(
                        event="metrics_recorder.cleanup.task_creation_error",
                        payload=log_context.build_error_payload(exc),
                        entity_id=self._entity_id,
                        skip_payload_normalization=True,
                    )

    async def _cleanup(self) -> None:
        try:
            await self._on_stopped()
        except Exception as exc:
            log_context = self._log_context
            if log_context is not None:
                log_context.log_error_event(
                    event="metrics_recorder.cleanup.failed",
                    payload=log_context.build_error_payload(exc),
                    entity_id=self._entity_id,
                    skip_payload_normalization=True,
                )

    # ---- MetricsRecorderProto implementation ---------------------------------------------

    def register_metric(self, metric: Metric) -> None:
        """
        Register a metric in the recorder.
        """

        async def _register_metric_core(_metric: Metric) -> None:
            self._metrics_by_name[_metric.metric_name] = _metric

        if metric is None:
            raise ValueError("argument 'metric' must not be None")

        if not isinstance(metric, Metric):
            raise TypeError("argument 'metric' must be an instance of 'Metric'")

        with self._thread_lock:
            state = self._state
            last_error = self._last_error

            if state is AsyncioMetricsRecorderState.FAILURE:
                raise AsyncioMetricsRecorderInvalidStateError(
                    recorder_state=state,
                    expected_states=(
                        AsyncioMetricsRecorderState.VIRGIN,
                        AsyncioMetricsRecorderState.STARTING,
                        AsyncioMetricsRecorderState.RUNNING,
                    ),
                    cause=last_error,
                )

            if state not in (
                AsyncioMetricsRecorderState.VIRGIN,
                AsyncioMetricsRecorderState.STARTING,
                AsyncioMetricsRecorderState.RUNNING,
            ):
                raise AsyncioMetricsRecorderInvalidStateError(
                    recorder_state=state,
                    expected_states=(
                        AsyncioMetricsRecorderState.VIRGIN,
                        AsyncioMetricsRecorderState.STARTING,
                        AsyncioMetricsRecorderState.RUNNING,
                    ),
                )

        if self._is_running_in_owning_loop():
            self._metrics_by_name[metric.metric_name] = metric
            return

        future = asyncio.run_coroutine_threadsafe(
            _register_metric_core(metric),
            self._loop,
        )
        future.result()

    def register_event(self, event: MetricEvent) -> None:
        """
        Register a metric event in the recorder.
        """

        if event is None:
            raise ValueError("argument 'event' must not be None")

        if not isinstance(event, MetricEvent):
            raise TypeError("argument 'event' must be an instance of 'MetricEvent'")

        with self._thread_lock:
            state = self._state
            last_error = self._last_error

            if state is AsyncioMetricsRecorderState.FAILURE:
                raise AsyncioMetricsRecorderInvalidStateError(
                    recorder_state=state,
                    expected_states=(
                        AsyncioMetricsRecorderState.VIRGIN,
                        AsyncioMetricsRecorderState.STARTING,
                        AsyncioMetricsRecorderState.RUNNING,
                    ),
                    cause=last_error,
                )

            if state not in (
                AsyncioMetricsRecorderState.VIRGIN,
                AsyncioMetricsRecorderState.STARTING,
                AsyncioMetricsRecorderState.RUNNING,
            ):
                raise AsyncioMetricsRecorderInvalidStateError(
                    recorder_state=state,
                    expected_states=(
                        AsyncioMetricsRecorderState.VIRGIN,
                        AsyncioMetricsRecorderState.STARTING,
                        AsyncioMetricsRecorderState.RUNNING,
                    ),
                )

            if self._pending_counter >= self._max_pending_counter:
                overflow = True
            else:
                self._pending_counter += 1
                overflow = False

        if overflow:
            if self._queue_overflow_policy is AsyncioMetricsRecorderQueueOverflowPolicy.RAISE_ERROR:
                raise AsyncioMetricsRecorderQueueOverflowError()
            return  # DROP

        if state is AsyncioMetricsRecorderState.VIRGIN:
            self.start()

        try:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, event)
        except RuntimeError:
            with self._thread_lock:
                self._pending_counter -= 1
            raise

    async def _on_metric_changed(
        self,
        *,
        metric: Metric,
        event: MetricEvent,
    ) -> None:
        pass

    def get_metric_snapshots(self) -> Mapping[str, Mapping[str, Any]]:
        """
        Return snapshots of registered metrics.
        """

        async def _get_metric_snapshots_core() -> Mapping[str, Mapping[str, Any]]:
            return {
                metric_name: metric.snapshot()
                for metric_name, metric in self._metrics_by_name.items()
            }

        if self._is_running_in_owning_loop():
            return {
                metric_name: metric.snapshot()
                for metric_name, metric in self._metrics_by_name.items()
            }

        future = asyncio.run_coroutine_threadsafe(
            _get_metric_snapshots_core(),
            self._loop,
        )
        return future.result()

    def iter_metrics(self) -> Iterable[Metric]:
        """
        Iterate over registered metrics.
        """

        async def _iter_metrics_core() -> tuple[Metric, ...]:
            return tuple(self._metrics_by_name.values())

        if self._is_running_in_owning_loop():
            return tuple(self._metrics_by_name.values())

        future = asyncio.run_coroutine_threadsafe(
            _iter_metrics_core(),
            self._loop,
        )
        return future.result()
