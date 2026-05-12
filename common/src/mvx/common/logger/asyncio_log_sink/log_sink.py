# src/mvx/common/logger/asyncio_log_sink/log_sink.py
from __future__ import annotations

from typing import Any
from collections.abc import Generator
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum

import threading
import asyncio
import concurrent.futures
import contextlib

from ..models import LogEvent, LogSinkProto, LogSinkTerminator
from ..helpers import log_internal_error as _log_internal_error

from .common import AsyncioLogSinkState

from .errors import (
    AsyncioLogSinkError,
    AsyncioLogSinkEventLoopUnavailableError,
    AsyncioLogSinkInvalidStateError,
    AsyncioLogSinkOnStartingHookFailedError,
    AsyncioLogSinkOnStoppedHookFailedError,
    AsyncioLogSinkQueueOverflowError,
    AsyncioLogSinkDispatcherCancelledError,
    AsyncioLogSinkUnexpectedError,
)

__all__ = (
    "AsyncioLogSinkQueueOverflowPolicy",
    "AsyncioLogSinkOp",
    "AsyncioLogSinkOpResult",
    "AsyncioLogSinkWaitHandle",
    "AsyncioLogSink",
)

DEFAULT_NAMESPACE = "mvx.common.logger.asyncio_log_sink"


class AsyncioLogSinkQueueOverflowPolicy(StrEnum):
    DROP = "DROP"
    RAISE_ERROR = "RAISE_ERROR"


DEFAULT_QUEUE_MAX_SIZE = 10_000


class AsyncioLogSinkOp(StrEnum):
    START = "START"
    STOP = "STOP"


@dataclass(frozen=True, slots=True)
class AsyncioLogSinkOpResult:
    op_name: AsyncioLogSinkOp
    success: bool
    error: AsyncioLogSinkError | None = None


class AsyncioLogSinkWaitHandle:
    def __init__(self, operation: AsyncioLogSinkOp) -> None:
        self._future: concurrent.futures.Future[None] = concurrent.futures.Future()
        self._operation = operation

    def wait(self) -> AsyncioLogSinkOpResult:
        try:
            self._future.result()
        except Exception as exc:
            return self._error_result(exc)

        return self._success_result()

    def __await__(self) -> Generator[Any, None, AsyncioLogSinkOpResult]:
        return self._wait_async().__await__()

    async def _wait_async(self) -> AsyncioLogSinkOpResult:
        try:
            await asyncio.shield(asyncio.wrap_future(self._future))
        except Exception as exc:
            return self._error_result(exc)

        return self._success_result()

    def _success_result(self) -> AsyncioLogSinkOpResult:
        return AsyncioLogSinkOpResult(
            op_name=self._operation,
            success=True,
        )

    def _error_result(self, exc: Exception) -> AsyncioLogSinkOpResult:
        mapped_exc = (
            exc
            if isinstance(exc, AsyncioLogSinkError)
            else AsyncioLogSinkUnexpectedError(cause=exc)
        )

        return AsyncioLogSinkOpResult(
            op_name=self._operation,
            success=False,
            error=mapped_exc,
        )


class _WaitHandleInternal(AsyncioLogSinkWaitHandle):
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


class AsyncioLogSink(ABC):

    _state: AsyncioLogSinkState

    _queue: asyncio.Queue[LogEvent]
    _queue_overflow_policy: AsyncioLogSinkQueueOverflowPolicy
    _dispatcher: asyncio.Task[None] | None

    def __init__(
        self,
        *,
        namespace: str | None = None,
        queue_max_size: int | None = None,
        queue_overflow_policy: AsyncioLogSinkQueueOverflowPolicy = AsyncioLogSinkQueueOverflowPolicy.RAISE_ERROR,
    ) -> None:

        self._namespace = namespace or DEFAULT_NAMESPACE

        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError as exc:
            raise AsyncioLogSinkEventLoopUnavailableError() from exc

        self._state = AsyncioLogSinkState.VIRGIN

        # Queue itself is intentionally unbounded.
        # Backpressure is controlled by _pending_counter, which includes scheduled
        # but not yet enqueued events.
        self._queue: asyncio.Queue[LogEvent] = asyncio.Queue()

        self._queue_overflow_policy = queue_overflow_policy
        self._pending_counter = 0
        self._max_pending_counter = queue_max_size or DEFAULT_QUEUE_MAX_SIZE

        self._dispatcher: asyncio.Task[None] | None = None

        self._last_error: AsyncioLogSinkError | None = None

        self._start_future: concurrent.futures.Future[None] | None = None
        self._stop_future: concurrent.futures.Future[None] | None = None

        self._thread_lock = threading.Lock()

    # ---- Lifecycle public API ------------------------------------------------------------

    def get_status(self) -> AsyncioLogSinkState:
        with self._thread_lock:
            return self._state

    def start(self) -> AsyncioLogSinkWaitHandle:
        handle = _WaitHandleInternal(operation=AsyncioLogSinkOp.START)

        with self._thread_lock:
            current_state = self._state

            if self._state is AsyncioLogSinkState.STARTING and self._start_future is not None:
                self._start_future.add_done_callback(handle.done_from_future)
                return handle

            if current_state not in (AsyncioLogSinkState.VIRGIN, AsyncioLogSinkState.STOPPED):
                handle.done(
                    exc=AsyncioLogSinkInvalidStateError(
                        sink_state=current_state,
                        expected_states=(AsyncioLogSinkState.VIRGIN, AsyncioLogSinkState.STOPPED),
                    )
                )
                return handle

            start_future = asyncio.run_coroutine_threadsafe(self._start_core(), self._loop)
            start_future.add_done_callback(handle.done_from_future)
            self._start_future = start_future
            self._state = AsyncioLogSinkState.STARTING

        return handle

    def stop(self) -> AsyncioLogSinkWaitHandle:
        handle = _WaitHandleInternal(operation=AsyncioLogSinkOp.STOP)

        with self._thread_lock:
            current_state = self._state

            if current_state is AsyncioLogSinkState.STOPPING and self._stop_future is not None:
                self._stop_future.add_done_callback(handle.done_from_future)
                return handle

            if current_state is not AsyncioLogSinkState.RUNNING:
                handle.done(
                    exc=AsyncioLogSinkInvalidStateError(
                        sink_state=current_state,
                        expected_states=(AsyncioLogSinkState.RUNNING,),
                    )
                )
                return handle

            stop_future = asyncio.run_coroutine_threadsafe(self._stop_core(), self._loop)
            stop_future.add_done_callback(handle.done_from_future)
            self._stop_future = stop_future
            self._state = AsyncioLogSinkState.STOPPING

            return handle

    # ---- Lifecycle internal realization --------------------------------------------------

    async def _start_core(self) -> None:

        try:
            try:
                await self._on_starting()
            except Exception as exc:
                raise AsyncioLogSinkOnStartingHookFailedError(
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
                if isinstance(exc, AsyncioLogSinkError)
                else AsyncioLogSinkUnexpectedError(cause=exc)
            )
            with self._thread_lock:
                self._last_error = mapped_exc
                self._start_future = None
                self._state = AsyncioLogSinkState.FAILURE

            raise mapped_exc from exc
        else:
            with self._thread_lock:
                self._start_future = None

                if self._state in (AsyncioLogSinkState.FAILURE, AsyncioLogSinkState.CANCELLED):
                    assert self._last_error is not None
                    raise self._last_error

                self._last_error = None
                self._state = AsyncioLogSinkState.RUNNING

    # ---- Startup and stopping hooks ------------------------------------------------------

    async def _on_starting(self) -> None:
        """
        This method is an internal hook that is invoked asynchronously during the
        startup process before dispatcher is started. It is designed as an extension point for
        fulfilling any startup requirements, such as starting connection for external log
        collection system. This method does not accept any parameters and does not return
        any value.

        :return: None
        :rtype: None
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
                raise AsyncioLogSinkOnStoppedHookFailedError(cause=exc) from exc

        except Exception as exc:
            mapped_exc = (
                exc
                if isinstance(exc, AsyncioLogSinkError)
                else AsyncioLogSinkUnexpectedError(cause=exc)
            )

            with self._thread_lock:
                self._stop_future = None
                self._last_error = mapped_exc
                self._state = AsyncioLogSinkState.FAILURE

            raise mapped_exc from exc

        else:
            with self._thread_lock:
                self._stop_future = None
                if self._state not in (AsyncioLogSinkState.CANCELLED, AsyncioLogSinkState.FAILURE):
                    self._state = AsyncioLogSinkState.STOPPED
                    return

                assert self._last_error is not None
                raise self._last_error

    async def _on_stopped(self) -> None:
        """
        This method is an internal hook that is invoked asynchronously during the
        stopping process after dispatcher has been stopped. It is designed as an extension
        fulfilling any stopping requirements, such as closing connection for external log
        collection system. This method does not accept any parameters and does not return
        any value.

        :return: None
        :rtype: None
        """
        pass

    # ---- Dispatching loop ----------------------------------------------------------------

    @abstractmethod
    async def _dispatch_core(self, event: LogEvent) -> None:
        raise NotImplementedError()

    async def _dispatching_loop(self, queue: asyncio.Queue[LogEvent]) -> None:
        while True:
            event = await queue.get()
            try:
                await self._dispatch_core(event)
            except Exception as exc:
                _log_internal_error(f"{self.__class__.__name__} dispatch error", exc)
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
                if self._state is not AsyncioLogSinkState.STOPPING:
                    self._last_error = AsyncioLogSinkDispatcherCancelledError()
                    self._state = AsyncioLogSinkState.CANCELLED
                    start_clean_up = True

        except Exception as exc:
            mapped_exc = (
                exc
                if isinstance(exc, AsyncioLogSinkError)
                else AsyncioLogSinkUnexpectedError(
                    cause=exc,
                )
            )
            with self._thread_lock:
                if self._state is not AsyncioLogSinkState.STOPPING:
                    self._last_error = mapped_exc
                    self._state = AsyncioLogSinkState.FAILURE
                    start_clean_up = True

        finally:
            if not start_clean_up:
                return

            try:
                self._loop.create_task(self._cleanup(), name=f"{self._namespace}.cleanup")
            except RuntimeError as exc:
                _log_internal_error(f"{self.__class__.__name__} cleanup task creation failed", exc)

    async def _cleanup(self) -> None:
        try:
            await self._on_stopped()
        except Exception as exc:
            _log_internal_error(f"{self.__class__.__name__} cleanup failed", exc)

    # ---- Logging -------------------------------------------------------------------------

    def log(self, event: LogEvent) -> None:
        """
        Accept a log event without waiting for sink startup or delivery.

        This method is intentionally non-blocking. If the sink is still VIRGIN,
        it triggers lazy startup on a best-effort basis and enqueues the event
        anyway. Events accepted during STARTING remain buffered and will be
        dispatched once the dispatcher is created.

        The method only rejects events when the sink is stopping, stopped,
        cancelled, failed, or when the local pending limit is exceeded.
        """

        with self._thread_lock:
            state = self._state
            last_error = self._last_error

            if state is AsyncioLogSinkState.FAILURE:
                raise AsyncioLogSinkInvalidStateError(
                    sink_state=state,
                    expected_states=(
                        AsyncioLogSinkState.VIRGIN,
                        AsyncioLogSinkState.STARTING,
                        AsyncioLogSinkState.RUNNING,
                    ),
                    cause=last_error,
                )

            if state not in (
                AsyncioLogSinkState.VIRGIN,
                AsyncioLogSinkState.STARTING,
                AsyncioLogSinkState.RUNNING,
            ):
                raise AsyncioLogSinkInvalidStateError(
                    sink_state=state,
                    expected_states=(
                        AsyncioLogSinkState.VIRGIN,
                        AsyncioLogSinkState.STARTING,
                        AsyncioLogSinkState.RUNNING,
                    ),
                )

            if self._pending_counter >= self._max_pending_counter:
                overflow = True
            else:
                self._pending_counter += 1
                overflow = False

        if overflow:
            if self._queue_overflow_policy is AsyncioLogSinkQueueOverflowPolicy.RAISE_ERROR:
                raise AsyncioLogSinkQueueOverflowError()
            return  # DROP

        if state is AsyncioLogSinkState.VIRGIN:
            self.start()

        try:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, event)
        except RuntimeError:
            with self._thread_lock:
                self._pending_counter -= 1
            raise

    @classmethod
    def create(cls, **kwargs: Any) -> tuple[LogSinkProto, LogSinkTerminator]:

        # 1. Creating and starting a new thread with an event loop inside to host the sink.

        loop: asyncio.AbstractEventLoop | None = None
        loop_ready = threading.Event()

        def thread_target() -> None:
            nonlocal loop

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop_ready.set()

            assert loop is not None

            try:
                loop.run_forever()

            finally:
                pending_tasks = asyncio.all_tasks(loop)

                for task in pending_tasks:
                    task.cancel()

                if pending_tasks:
                    done, still_pending = loop.run_until_complete(
                        asyncio.wait(
                            pending_tasks,
                            timeout=DEFAULT_PENDING_TASKS_CANCEL_TIMEOUT_S,
                        )
                    )

                    if still_pending:
                        _log_internal_error(
                            f"{cls.__name__} runtime shutdown warning: pending task cancellation "
                            f"timed out after {DEFAULT_PENDING_TASKS_CANCEL_TIMEOUT_S:.1f} seconds; "
                            f"{len(still_pending)} task(s) still pending",
                            None,
                        )

                    for task in done:
                        try:
                            task.result()
                        except asyncio.CancelledError:
                            pass
                        except Exception as _exc:
                            _log_internal_error(
                                f"{cls.__name__} runtime shutdown warning: pending task finished "
                                f"with error during cancellation",
                                _exc,
                            )

                loop.close()

        thread = threading.Thread(
            target=thread_target,
            name=f"{cls.__name__}.threaded-log-sink",
            daemon=False,
        )
        thread.start()

        if not loop_ready.wait(timeout=DEFAULT_THREAD_START_TIMEOUT_S):
            if loop is not None:
                with contextlib.suppress(RuntimeError):
                    # noinspection PyTypeChecker
                    loop.call_soon_threadsafe(loop.stop)

            thread.join(timeout=DEFAULT_THREAD_JOIN_TIMEOUT_S)

            raise RuntimeError(
                f"{cls.__name__} runtime creation failed: event loop thread did not become ready "
                f"within {DEFAULT_THREAD_START_TIMEOUT_S:.1f} seconds"
            )

        assert loop is not None

        # 2. Creating a sink instance in the thread and waiting for it to be ready.

        async def bootstrap() -> AsyncioLogSink:
            return cls(**kwargs)

        bootstrap_future = asyncio.run_coroutine_threadsafe(bootstrap(), loop)

        try:
            sink = bootstrap_future.result(timeout=DEFAULT_THREAD_START_TIMEOUT_S)

        except Exception as exc:
            bootstrap_future.cancel()

            with contextlib.suppress(RuntimeError):
                # noinspection PyTypeChecker
                loop.call_soon_threadsafe(loop.stop)

            thread.join(timeout=DEFAULT_THREAD_JOIN_TIMEOUT_S)

            if thread.is_alive():
                raise RuntimeError(
                    f"{cls.__name__} runtime creation failed: sink bootstrap failed and event loop "
                    f"thread did not stop within {DEFAULT_THREAD_JOIN_TIMEOUT_S:.1f} seconds -> "
                    f"{type(exc).__name__}: {exc}"
                ) from exc

            raise RuntimeError(
                f"{cls.__name__} runtime creation failed: sink bootstrap failed -> "
                f"{type(exc).__name__}: {exc}"
            ) from exc

        start_outcome = sink.start().wait()

        if not start_outcome.success:
            with contextlib.suppress(RuntimeError):
                # noinspection PyTypeChecker
                loop.call_soon_threadsafe(loop.stop)

            thread.join(timeout=DEFAULT_THREAD_JOIN_TIMEOUT_S)

            if start_outcome.error is not None:
                raise RuntimeError(
                    f"{cls.__name__} runtime creation failed: sink startup failed -> "
                    f"{type(start_outcome.error).__name__}: {start_outcome.error}"
                ) from start_outcome.error

            raise RuntimeError(
                f"{cls.__name__} runtime creation failed: sink startup failed without error details"
            )

        # 3. Creating an idempotent terminator for the sink runtime.

        terminator_lock = threading.Lock()
        terminated = False

        def terminator() -> None:
            nonlocal terminated

            if threading.current_thread() is thread:
                raise RuntimeError(
                    f"{cls.__name__} runtime termination failed: terminator cannot be called "
                    f"from its own event loop thread"
                )

            with terminator_lock:
                if terminated:
                    return

                terminated = True

            termination_error: Exception | None = None

            try:
                status = sink.get_status()

                if status in (AsyncioLogSinkState.RUNNING, AsyncioLogSinkState.STOPPING):
                    outcome = sink.stop().wait()

                    if not outcome.success:
                        if outcome.error is not None:
                            termination_error = outcome.error
                        else:
                            termination_error = RuntimeError(
                                f"{cls.__name__} runtime termination failed: sink stop operation failed "
                                f"without error details"
                            )

            except Exception as _exc:
                termination_error = _exc

            finally:
                try:
                    assert loop is not None
                    # noinspection PyTypeChecker
                    loop.call_soon_threadsafe(loop.stop)

                except RuntimeError as _exc:
                    if termination_error is None:
                        termination_error = RuntimeError(
                            f"{cls.__name__} runtime termination failed: unable to schedule event loop stop -> "
                            f"{type(_exc).__name__}: {_exc}"
                        )

                thread.join(timeout=DEFAULT_THREAD_JOIN_TIMEOUT_S)

                if thread.is_alive() and termination_error is None:
                    termination_error = RuntimeError(
                        f"{cls.__name__} runtime termination failed: event loop thread did not stop "
                        f"within {DEFAULT_THREAD_JOIN_TIMEOUT_S:.1f} seconds"
                    )

            if termination_error is not None:
                raise termination_error

        return sink, terminator
