# common/src/mvx/common/logger/core/log_sinks/asyncio_base_sink.py
"""
Asynchronous buffered log sink base.

This module defines :class:`AsyncioLogSink`, a base class for log sinks that
accept log events synchronously and deliver them asynchronously through an
``asyncio`` dispatcher task.

The sink is designed for application code that must not block on log delivery.
Calling :meth:`AsyncioLogSink.log` only validates the current sink state,
accounts the event against the local pending limit, and schedules the event to
be appended to an internal queue owned by the sink event loop. Actual delivery is
performed later by the dispatcher task through the subclass-provided
:meth:`AsyncioLogSink._dispatch_core` method.

Threading and event-loop ownership
----------------------------------
The public API is thread-safe by design. Public methods such as ``log()``,
``start()``, ``stop()`` and ``get_status()`` may be called from any thread.

The sink itself, however, is owned by one specific ``asyncio`` event loop. The
instance must be created in a thread where that event loop is already running;
initialization captures the current running loop and creates loop-owned
asyncio primitives for that loop.

The sink lifetime is bound to the lifetime of the owning event loop. As long as
that loop is alive, public methods may schedule work into it from other threads.
If the owning loop is stopped or closed, the sink is no longer usable; handling
a dead loop is outside the normal sink lifecycle contract.

Lifecycle model
---------------
The sink has an explicit lifecycle:

* ``VIRGIN``:
  The sink has not been started yet. Calling ``log()`` triggers lazy startup
  and accepts the event.

* ``STARTING``:
  Startup is in progress. Events are accepted and buffered. They will be
  dispatched after startup completes and the dispatcher task is created.

* ``STARTED``:
  The dispatcher task is running and consumes accepted events from the queue.

* ``STOPPING``:
  Graceful stop is in progress. New events are rejected. The sink first performs
  a best-effort flush of already accepted events, then cancels the dispatcher and
  invokes ``_on_stopped()``.

* ``STOPPED``:
  The sink has been gracefully stopped. New events are rejected. Starting the
  sink again is allowed.

* ``FAILURE``:
  The sink entered a terminal failure state. New events, start and stop requests
  are rejected. Graceful stop is not available from this state.

* ``CANCELLED``:
  The dispatcher was cancelled outside the normal stopping path. This is a
  terminal state. New events, start and stop requests are rejected.

Queue and backpressure
----------------------
The internal ``asyncio.Queue`` is intentionally unbounded. Memory pressure is
controlled separately by ``_pending_counter`` and ``_max_pending_counter``.

The pending counter tracks accepted events that have not been fully processed
yet. This includes events that were accepted by ``log()`` and scheduled with
``call_soon_threadsafe()``, but have not yet been physically appended to the
queue.

When the pending limit is reached, behavior is controlled by
:class:`QueueOverflowPolicy`:

* ``RAISE_ERROR`` raises :class:`LogSinkError`;
* ``DROP`` silently drops the new event.

Stopping and flushing
---------------------
Graceful stop is best-effort. Before waiting for ``queue.join()``, the sink
places a loop-local barrier into the event loop. This allows already scheduled
``queue.put_nowait(event)`` callbacks to run before the stop path starts waiting
for the queue to drain.

Flush waits until either all currently accepted events are processed or the
dispatcher task finishes first. If the dispatcher finishes first, flushing cannot
make further progress and the normal stopping or failure path handles the final
state.

Wait handles
------------
``start()`` and ``stop()`` return a :class:`WaitHandle`.

The handle intentionally has two delivery paths:

* synchronous callers use ``wait()``, backed by ``concurrent.futures.Future.result()``;
* asynchronous callers use ``await handle``, backed by ``asyncio.wrap_future()``
  around the same ``concurrent.futures.Future``.

This allows ordinary threads to wait for lifecycle completion without owning an
event loop, while async callers can await the same operation from any running
event loop. Cancelling an async wait does not cancel the underlying lifecycle
operation.

Subclass contract
-----------------
Subclasses implement only destination-specific behavior:

* ``_on_starting()`` may open external resources before dispatching starts;
* ``_dispatch_core(event)`` delivers one log event;
* ``_on_stopped()`` closes external resources during graceful stop.

``_on_stopped()`` is a graceful-stop hook. It is not guaranteed to run after a
terminal dispatcher failure.
"""

from __future__ import annotations
from typing import Any
from collections.abc import Generator
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum


import sys
import threading
import asyncio
import concurrent.futures

import contextlib

from common.errors import ReasonedError
from ..protocols import LogEvent

__all__ = ("AsyncioLogSink",)


class LogSinkStatus(StrEnum):
    VIRGIN = "VIRGIN"
    STARTING = "STARTING"
    STARTED = "STARTED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILURE = "FAILURE"
    CANCELLED = "CANCELLED"


class QueueOverflowPolicy(StrEnum):
    DROP = "DROP"
    RAISE_ERROR = "RAISE_ERROR"


DEFAULT_QUEUE_MAX_SIZE = 10_000
DEFAULT_NAMESPACE = "mvx.asyncio_base_sink"


class LogSinkError(ReasonedError):
    pass


class LogSinkErrorReason(StrEnum):
    LOG_SINK_IS_STARTING = "LOG_SINK_IS_STARTING"
    LOG_SINK_ALREADY_STARTED = "LOG_SINK_ALREADY_STARTED"
    LOG_SINK_IS_STOPPING = "LOG_SINK_IS_STOPPING"
    LOG_SINK_ALREADY_STOPPED = "LOG_SINK_ALREADY_STOPPED"
    LOG_SINK_IS_IN_FAILURE = "LOG_SINK_IS_IN_FAILURE"
    LOG_SINK_ON_STARTING_ERROR = "LOG_SINK_ON_STARTING_ERROR"
    LOG_SINK_ON_STOPPED_ERROR = "LOG_SINK_ON_STOPPED_ERROR"
    LOG_SINK_IS_CANCELLED = "LOG_SINK_IS_CANCELLED"
    LOG_SINK_UNEXPECTED_ERROR = "LOG_SINK_UNEXPECTED_ERROR"

    def to_message(self, *, cls: type, msg: str = "") -> str:
        if self is LogSinkErrorReason.LOG_SINK_IS_STARTING:
            return f"{cls.__name__} is starting"
        if self is LogSinkErrorReason.LOG_SINK_ALREADY_STARTED:
            return f"{cls.__name__} is already started"
        if self is LogSinkErrorReason.LOG_SINK_IS_STOPPING:
            return f"{cls.__name__} is stopping"
        if self is LogSinkErrorReason.LOG_SINK_ALREADY_STOPPED:
            return f"{cls.__name__} is already stopped"
        if self is LogSinkErrorReason.LOG_SINK_IS_IN_FAILURE:
            return f"{cls.__name__} is in failure state"
        if self is LogSinkErrorReason.LOG_SINK_ON_STARTING_ERROR:
            return f"Error during {cls.__name__}._on_starting() -> {msg}"
        if self is LogSinkErrorReason.LOG_SINK_ON_STOPPED_ERROR:
            return f"Error during {cls.__name__}._on_stopped() -> {msg}"
        if self is LogSinkErrorReason.LOG_SINK_IS_CANCELLED:
            return f"{cls.__name__} is cancelled"
        if self is LogSinkErrorReason.LOG_SINK_UNEXPECTED_ERROR:
            return f"{cls.__name__} unexpected error -> {msg}"

        return f"Unknown error reason: {self.value}"


@dataclass(frozen=True, slots=True)
class LogSinkOperationOutcome:
    success: bool
    error: LogSinkError | None = None


class WaitHandle:
    def __init__(self):
        self._future: concurrent.futures.Future[LogSinkOperationOutcome] = (
            concurrent.futures.Future()
        )

    def wait(self) -> LogSinkOperationOutcome:
        return self._future.result()

    def __await__(self) -> Generator[Any, None, LogSinkOperationOutcome]:
        return self._wait_async().__await__()

    async def _wait_async(self) -> LogSinkOperationOutcome:
        return await asyncio.shield(asyncio.wrap_future(self._future))


class _WaitHandleInternal(WaitHandle):
    def done(self, result: LogSinkOperationOutcome) -> None:
        try:
            self._future.set_result(result)
        except concurrent.futures.InvalidStateError:
            return

    def done_from_future(self, cf_future: concurrent.futures.Future) -> None:
        try:
            result = cf_future.result()
        except Exception as exc:
            result = LogSinkOperationOutcome(
                success=False,
                error=LogSinkError(
                    message=str(exc),
                    reason=LogSinkErrorReason.LOG_SINK_UNEXPECTED_ERROR.value,
                    cause=exc,
                ),
            )

        self.done(result)


class AsyncioLogSink(ABC):

    _state: LogSinkStatus

    _queue: asyncio.Queue[LogEvent]
    _queue_overflow_policy: QueueOverflowPolicy
    _dispatcher: asyncio.Task[None] | None

    @staticmethod
    def _log_internal_error(message: str) -> None:
        print(message, file=sys.stderr)

    def __init__(
        self,
        *,
        namespace: str | None = None,
        queue_max_size: int | None = None,
        queue_overflow_policy: QueueOverflowPolicy = QueueOverflowPolicy.RAISE_ERROR,
    ) -> None:

        self._namespace = namespace or DEFAULT_NAMESPACE

        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError as exc:
            self._log_internal_error(
                f"{self.__class__.__name__} initialization error -> {str(exc)}"
            )
            raise LogSinkError(
                message=f"{self.__class__.__name__} initialization error -> {exc}",
                reason=LogSinkErrorReason.LOG_SINK_UNEXPECTED_ERROR.value,
                cause=exc,
            ) from exc

        self._state = LogSinkStatus.VIRGIN

        # Queue itself is intentionally unbounded.
        # Backpressure is controlled by _pending_counter, which includes scheduled
        # but not yet enqueued events.
        self._queue: asyncio.Queue[LogEvent] = asyncio.Queue()

        self._queue_overflow_policy = queue_overflow_policy
        self._pending_counter = 0
        self._max_pending_counter = queue_max_size or DEFAULT_QUEUE_MAX_SIZE

        self._dispatcher: asyncio.Task[None] | None = None

        self._last_error: LogSinkError | None = None

        self._start_future: concurrent.futures.Future | None = None
        self._stop_future: concurrent.futures.Future | None = None

        self._thread_lock = threading.Lock()

    # ---- Helpers -------------------------------------------------------------------------
    def _set_handle_with_error(
        self, handle: _WaitHandleInternal, error: LogSinkErrorReason
    ) -> None:
        handle.done(
            LogSinkOperationOutcome(
                success=False,
                error=LogSinkError(
                    message=error.to_message(cls=self.__class__),
                    reason=error.value,
                ),
            )
        )

    # ---- Lifecycle public API ------------------------------------------------------------

    def get_status(self) -> LogSinkStatus:
        with self._thread_lock:
            return self._state

    def start(self) -> WaitHandle:
        handle = _WaitHandleInternal()

        with self._thread_lock:
            if self._state is LogSinkStatus.CANCELLED:
                self._set_handle_with_error(handle, LogSinkErrorReason.LOG_SINK_IS_CANCELLED)
                return handle

            if self._state is LogSinkStatus.FAILURE:
                self._set_handle_with_error(handle, LogSinkErrorReason.LOG_SINK_IS_IN_FAILURE)
                return handle

            if self._state is LogSinkStatus.STARTED:
                self._set_handle_with_error(handle, LogSinkErrorReason.LOG_SINK_ALREADY_STARTED)
                return handle

            if self._state is LogSinkStatus.STOPPING:
                self._set_handle_with_error(handle, LogSinkErrorReason.LOG_SINK_IS_STOPPING)
                return handle

            if self._state is LogSinkStatus.STARTING and self._start_future is not None:
                self._start_future.add_done_callback(handle.done_from_future)
                return handle

            start_future = asyncio.run_coroutine_threadsafe(self._start_core(), self._loop)
            start_future.add_done_callback(handle.done_from_future)
            self._start_future = start_future
            self._state = LogSinkStatus.STARTING

        return handle

    def stop(self) -> WaitHandle:
        handle = _WaitHandleInternal()

        with self._thread_lock:

            if self._state is LogSinkStatus.CANCELLED:
                self._set_handle_with_error(handle, LogSinkErrorReason.LOG_SINK_IS_CANCELLED)
                return handle

            if self._state in (LogSinkStatus.STOPPED, LogSinkStatus.VIRGIN):
                self._set_handle_with_error(
                    handle,
                    LogSinkErrorReason.LOG_SINK_ALREADY_STOPPED,
                )
                return handle

            if self._state is LogSinkStatus.STARTING:
                self._set_handle_with_error(handle, LogSinkErrorReason.LOG_SINK_IS_STARTING)
                return handle

            if self._state is LogSinkStatus.FAILURE:
                self._set_handle_with_error(handle, LogSinkErrorReason.LOG_SINK_IS_IN_FAILURE)
                return handle

            if self._state is LogSinkStatus.STOPPING and self._stop_future is not None:
                self._stop_future.add_done_callback(handle.done_from_future)
                return handle

            stop_future = asyncio.run_coroutine_threadsafe(self._stop_core(), self._loop)
            stop_future.add_done_callback(handle.done_from_future)
            self._stop_future = stop_future
            self._state = LogSinkStatus.STOPPING

            return handle

    # ---- Lifecycle internal realization --------------------------------------------------

    async def _start_core(self) -> LogSinkOperationOutcome:

        try:
            try:
                await self._on_starting()
            except Exception as exc:
                raise LogSinkError(
                    message=LogSinkErrorReason.LOG_SINK_ON_STARTING_ERROR.to_message(
                        cls=self.__class__, msg=str(exc)
                    ),
                    reason=LogSinkErrorReason.LOG_SINK_ON_STARTING_ERROR.value,
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
                if isinstance(exc, LogSinkError)
                else LogSinkError(
                    message=LogSinkErrorReason.LOG_SINK_UNEXPECTED_ERROR.to_message(
                        cls=self.__class__, msg=str(exc)
                    ),
                    reason=LogSinkErrorReason.LOG_SINK_UNEXPECTED_ERROR.value,
                    cause=exc,
                )
            )
            with self._thread_lock:
                self._last_error = mapped_exc
                self._start_future = None
                self._state = LogSinkStatus.FAILURE

            return LogSinkOperationOutcome(
                success=False,
                error=mapped_exc,
            )
        else:
            with self._thread_lock:
                self._start_future = None

                if self._state in (LogSinkStatus.FAILURE, LogSinkStatus.CANCELLED):
                    return LogSinkOperationOutcome(False, self._last_error)

                self._last_error = None
                self._state = LogSinkStatus.STARTED

            return LogSinkOperationOutcome(True)

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

    async def _stop_core(self) -> LogSinkOperationOutcome:

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
                raise LogSinkError(
                    message=LogSinkErrorReason.LOG_SINK_ON_STOPPED_ERROR.to_message(
                        cls=self.__class__, msg=str(exc)
                    ),
                    reason=LogSinkErrorReason.LOG_SINK_ON_STOPPED_ERROR.value,
                    cause=exc,
                ) from exc

        except Exception as exc:
            mapped_exc = (
                exc
                if isinstance(exc, LogSinkError)
                else LogSinkError(
                    message=LogSinkErrorReason.LOG_SINK_UNEXPECTED_ERROR.to_message(
                        cls=self.__class__, msg=str(exc)
                    ),
                    reason=LogSinkErrorReason.LOG_SINK_UNEXPECTED_ERROR.value,
                    cause=exc,
                )
            )
            with self._thread_lock:
                self._stop_future = None
                self._last_error = mapped_exc
                self._state = LogSinkStatus.FAILURE

            return LogSinkOperationOutcome(
                success=False,
                error=mapped_exc,
            )

        else:
            with self._thread_lock:
                self._stop_future = None
                if self._state not in (LogSinkStatus.CANCELLED, LogSinkStatus.FAILURE):
                    self._state = LogSinkStatus.STOPPED
                    return LogSinkOperationOutcome(True)

                return LogSinkOperationOutcome(False, self._last_error)

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
    async def _dispatch_core(self, event: LogEvent):
        raise NotImplementedError()

    async def _dispatching_loop(self, queue: asyncio.Queue[LogEvent]) -> None:
        while True:
            event = await queue.get()
            try:
                await self._dispatch_core(event)
            except Exception as exc:
                self._log_internal_error(f"{self.__class__.__name__} dispatch error -> {exc}")
                raise
            finally:
                queue.task_done()
                with self._thread_lock:
                    self._pending_counter -= 1

    def _on_dispatching_task_done(self, task: asyncio.Task[None]) -> None:
        try:
            task.result()
            return
        except asyncio.CancelledError:
            handle_cancellation = False
            with self._thread_lock:
                if self._state is not LogSinkStatus.STOPPING:
                    self._state = LogSinkStatus.CANCELLED
                    handle_cancellation = True

            if handle_cancellation:
                try:
                    self._loop.create_task(self._cleanup(), name=f"{self._namespace}.cleanup")
                except RuntimeError as exc:
                    self._log_internal_error(
                        f"{self.__class__.__name__} cleanup task creation failed -> {exc}"
                    )

        except Exception as exc:
            mapped_exc = (
                exc
                if isinstance(exc, LogSinkError)
                else LogSinkError(
                    message=LogSinkErrorReason.LOG_SINK_UNEXPECTED_ERROR.to_message(
                        cls=self.__class__, msg=str(exc)
                    ),
                    reason=LogSinkErrorReason.LOG_SINK_UNEXPECTED_ERROR.value,
                    cause=exc,
                )
            )
            with self._thread_lock:
                self._last_error = mapped_exc
                self._state = LogSinkStatus.FAILURE

    async def _cleanup(self) -> None:
        try:
            await self._on_stopped()
        except Exception as exc:
            self._log_internal_error(f"{self.__class__.__name__} cleanup error -> {exc}")

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

            if state is LogSinkStatus.FAILURE:
                raise LogSinkError(
                    message=LogSinkErrorReason.LOG_SINK_IS_IN_FAILURE.to_message(
                        cls=self.__class__
                    ),
                    reason=LogSinkErrorReason.LOG_SINK_IS_IN_FAILURE.value,
                    cause=last_error,
                )

            if state is LogSinkStatus.CANCELLED:
                raise LogSinkError(
                    message=LogSinkErrorReason.LOG_SINK_IS_CANCELLED.to_message(cls=self.__class__),
                    reason=LogSinkErrorReason.LOG_SINK_IS_CANCELLED.value,
                )

            if state is LogSinkStatus.STOPPING:
                raise LogSinkError(
                    message=LogSinkErrorReason.LOG_SINK_IS_STOPPING.to_message(cls=self.__class__),
                    reason=LogSinkErrorReason.LOG_SINK_IS_STOPPING.value,
                )

            if state is LogSinkStatus.STOPPED:
                raise LogSinkError(
                    message=LogSinkErrorReason.LOG_SINK_ALREADY_STOPPED.to_message(
                        cls=self.__class__
                    ),
                    reason=LogSinkErrorReason.LOG_SINK_ALREADY_STOPPED.value,
                )

            if self._pending_counter >= self._max_pending_counter:
                overflow = True
            else:
                self._pending_counter += 1
                overflow = False

        if overflow:
            if self._queue_overflow_policy is QueueOverflowPolicy.RAISE_ERROR:
                raise LogSinkError(
                    message=f"{self.__class__.__name__} queue is full",
                    reason="LOG_SINK_QUEUE_FULL",
                )
            return  # DROP

        if state is LogSinkStatus.VIRGIN:
            self.start()

        try:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, event)
        except RuntimeError:
            with self._thread_lock:
                self._pending_counter -= 1
            raise
