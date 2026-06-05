# src/mvx/common/metrics/metrics_runtime/metrics_runtime.py
from __future__ import annotations

import threading
import asyncio
import contextlib

from mvx.common.logger.log_context import LogContext
from mvx.common.logger.log_components import log_invocation, LogContextProto

from ..asyncio_metrics_recorder import (
    AsyncioMetricsRecorder,
    AsyncioMetricsRecorderQueueOverflowPolicy,
)

from .common import MetricsRuntimeState

from .errors import (
    MetricsRuntimeError,
    MetricsRuntimeInvalidStateError,
    MetricsRuntimeStartupError,
    MetricsRuntimeShutdownError,
    MetricsRuntimeLoopUnavailableError,
    MetricsRuntimeRecorderAlreadyExistsError,
    MetricsRuntimeRecorderNotFoundError,
    MetricsRuntimeRecorderStartupError,
    MetricsRuntimeRecorderStopError,
)

__all__ = ("MetricsRuntime",)

DEFAULT_RUNTIME_THREAD_START_TIMEOUT_S = 5.0
DEFAULT_RUNTIME_THREAD_JOIN_TIMEOUT_S = 5.0
DEFAULT_RUNTIME_SHUTDOWN_TIMEOUT_S = 5.0


class MetricsRuntime:
    __slots__ = (
        "_namespace",
        "_default_recorder_queue_max_size",
        "_default_recorder_queue_overflow_policy",
        "_log_context",
        "_state",
        "_recorders",
        "_creating_recorders",
        "_removing_recorders",
        "_thread",
        "_thread_lock",
        "_loop",
        "_loop_ready_event",
        "_last_error",
    )

    def __init__(
        self,
        *,
        namespace: str,
        default_recorder_queue_max_size: int | None = None,
        default_recorder_queue_overflow_policy: AsyncioMetricsRecorderQueueOverflowPolicy = AsyncioMetricsRecorderQueueOverflowPolicy.DROP,
        log_context: LogContext | None = None,
    ) -> None:

        if namespace is None:
            raise ValueError("argument 'namespace' must not be None")

        if not isinstance(namespace, str):
            raise TypeError("argument 'namespace' must be string when provided")

        if default_recorder_queue_max_size is not None:
            if isinstance(default_recorder_queue_max_size, bool) or not isinstance(
                default_recorder_queue_max_size, int
            ):
                raise TypeError(
                    "argument 'default_recorder_queue_max_size' must be integer when provided"
                )
            if default_recorder_queue_max_size <= 0:
                raise ValueError(
                    "argument 'default_recorder_queue_max_size' must be positive integer when provided"
                )

        if default_recorder_queue_overflow_policy is not None:
            if not isinstance(
                default_recorder_queue_overflow_policy, AsyncioMetricsRecorderQueueOverflowPolicy
            ):
                raise TypeError(
                    "argument 'default_recorder_queue_overflow_policy' must be an instance of 'AsyncioMetricsRecorderQueueOverflowPolicy' when provided"
                )

        if log_context is not None:
            if not isinstance(log_context, LogContext):
                raise TypeError(
                    "argument 'log_context' must be an instance of 'LogContext' when provided"
                )

        self._namespace = namespace
        self._default_recorder_queue_max_size = default_recorder_queue_max_size
        self._default_recorder_queue_overflow_policy = default_recorder_queue_overflow_policy
        self._log_context = log_context

        self._state = MetricsRuntimeState.VIRGIN

        self._recorders: dict[str, AsyncioMetricsRecorder] = {}
        self._creating_recorders: set[str] = set()
        self._removing_recorders: set[str] = set()

        self._thread: threading.Thread | None = None
        self._thread_lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_ready_event = threading.Event()
        self._last_error: MetricsRuntimeError | None = None

    # ---- Logging infrastructure ----------------------------------------------------------

    def get_log_context(self) -> LogContextProto | None:
        """
        Return the log context associated with this metrics runtime.

        :return: runtime log context, or None when runtime logging is disabled.
        """
        return self._log_context

    @property
    def identity(self) -> str:
        """
        Return the stable runtime identity used by logging.

        :return: runtime namespace.
        """
        return self._namespace

    # ---- Runtime Lifecycle public API ------------------------------------------------------------

    def get_status(self) -> MetricsRuntimeState:
        with self._thread_lock:
            return self._state

    @log_invocation(
        "metrics_runtime.start",
        context_fields=("state=self._state",),
    )
    def start(self) -> None:

        with self._thread_lock:
            state = self._state

            if state is MetricsRuntimeState.RUNNING:
                return

            if state is not MetricsRuntimeState.VIRGIN:
                raise MetricsRuntimeInvalidStateError(
                    runtime_state=state,
                    expected_states=(MetricsRuntimeState.VIRGIN,),
                )

            self._state = MetricsRuntimeState.STARTING
            self._last_error = None
            self._loop_ready_event.clear()

            thread = threading.Thread(
                target=self._thread_main_function,
                name=f"{self._namespace}.metrics_runtime",
                daemon=False,
            )
            self._thread = thread
            thread.start()

        if not self._loop_ready_event.wait(DEFAULT_RUNTIME_THREAD_START_TIMEOUT_S):
            with self._thread_lock:
                self._state = MetricsRuntimeState.FAILURE
                self._last_error = MetricsRuntimeStartupError()

            raise MetricsRuntimeStartupError()

        with self._thread_lock:
            if self._last_error is not None:
                raise self._last_error

            if self._loop is None:
                self._state = MetricsRuntimeState.FAILURE
                self._last_error = MetricsRuntimeLoopUnavailableError()
                raise self._last_error

            self._state = MetricsRuntimeState.RUNNING

    @log_invocation(
        "metrics_runtime.shutdown",
        context_fields=("state=self._state",),
    )
    def shutdown(self) -> None:
        loop: asyncio.AbstractEventLoop | None = None
        thread: threading.Thread | None = None
        should_schedule_shutdown = False

        with self._thread_lock:
            state = self._state

            if state in (
                MetricsRuntimeState.VIRGIN,
                MetricsRuntimeState.CLOSED,
            ):
                self._state = MetricsRuntimeState.CLOSED
                return

            if state is MetricsRuntimeState.RUNNING:
                loop = self._loop
                thread = self._thread

                if loop is None:
                    error = MetricsRuntimeLoopUnavailableError()
                    self._state = MetricsRuntimeState.FAILURE
                    self._last_error = error
                    raise error

                self._state = MetricsRuntimeState.STOPPING
                should_schedule_shutdown = True

            elif state is MetricsRuntimeState.STOPPING:
                loop = self._loop
                thread = self._thread

            else:
                raise MetricsRuntimeInvalidStateError(
                    runtime_state=state,
                    expected_states=(
                        MetricsRuntimeState.RUNNING,
                        MetricsRuntimeState.STOPPING,
                    ),
                )

        shutdown_error: MetricsRuntimeError | None = None

        if should_schedule_shutdown:
            assert loop is not None

            try:
                future = asyncio.run_coroutine_threadsafe(
                    self._shutdown_loop_core(),
                    loop,
                )
                future.result(timeout=DEFAULT_RUNTIME_SHUTDOWN_TIMEOUT_S)

            except Exception as exc:
                shutdown_error = (
                    exc
                    if isinstance(exc, MetricsRuntimeError)
                    else MetricsRuntimeShutdownError(cause=exc)
                )

            finally:
                with contextlib.suppress(RuntimeError):
                    # noinspection PyTypeChecker
                    loop.call_soon_threadsafe(loop.stop)

        if thread is not None:
            thread.join(DEFAULT_RUNTIME_THREAD_JOIN_TIMEOUT_S)

            if thread.is_alive() and shutdown_error is None:
                shutdown_error = MetricsRuntimeShutdownError()

        with self._thread_lock:
            self._thread = None
            self._loop = None

            if shutdown_error is not None:
                self._state = MetricsRuntimeState.FAILURE
                self._last_error = shutdown_error
                raise shutdown_error

            self._state = MetricsRuntimeState.CLOSED
            self._last_error = None

    # ---- Recorders management public API -------------------------------------------------

    @log_invocation(
        "metrics_runtime.create_recorder",
        context_fields=("state=self._state",),
        log_kwargs_on_invoke=(
            "recorder_id",
            "entity_id",
            "namespace",
            "queue_max_size",
            "queue_overflow_policy",
        ),
    )
    def create_recorder(
        self,
        recorder_id: str,
        *,
        entity_id: str | None = None,
        namespace: str | None = None,
        queue_max_size: int | None = None,
        queue_overflow_policy: AsyncioMetricsRecorderQueueOverflowPolicy | None = None,
        log_context: LogContext | None = None,
    ) -> AsyncioMetricsRecorder:

        normalized_recorder_id = self._ensure_recorder_id(recorder_id)

        if entity_id is not None:
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
                    "argument 'queue_overflow_policy' must be an instance of "
                    "'AsyncioMetricsRecorderQueueOverflowPolicy' when provided"
                )

        if log_context is not None:
            if not isinstance(log_context, LogContext):
                raise TypeError(
                    "argument 'log_context' must be an instance of 'LogContext' when provided"
                )

        loop = self._get_running_loop_or_raise()

        try:
            future = asyncio.run_coroutine_threadsafe(
                self._create_recorder_core(
                    recorder_id=normalized_recorder_id,
                    entity_id=entity_id,
                    namespace=namespace,
                    queue_max_size=queue_max_size,
                    queue_overflow_policy=queue_overflow_policy,
                    log_context=log_context,
                ),
                loop,
            )
            return future.result()

        except MetricsRuntimeError:
            raise

        except Exception as exc:
            raise MetricsRuntimeRecorderStartupError(recorder_id=recorder_id, cause=exc) from exc

    @log_invocation(
        "metrics_runtime.get_recorder",
        context_fields=("state=self._state",),
        log_kwargs_on_invoke=("recorder_id",),
    )
    def get_recorder(
        self,
        recorder_id: str,
    ) -> AsyncioMetricsRecorder:

        normalized_recorder_id = self._ensure_recorder_id(recorder_id)
        loop = self._get_running_loop_or_raise()

        future = asyncio.run_coroutine_threadsafe(
            self._get_recorder_core(normalized_recorder_id),
            loop,
        )

        return future.result()

    @log_invocation(
        "metrics_runtime.try_get_recorder",
        context_fields=("state=self._state",),
        log_kwargs_on_invoke=("recorder_id",),
    )
    def try_get_recorder(
        self,
        recorder_id: str,
    ) -> AsyncioMetricsRecorder | None:

        normalized_recorder_id = self._ensure_recorder_id(recorder_id)
        loop = self._get_running_loop_or_raise()

        future = asyncio.run_coroutine_threadsafe(
            self._try_get_recorder_core(normalized_recorder_id),
            loop,
        )

        return future.result()

    @log_invocation(
        "metrics_runtime.list_recorder_ids",
        context_fields=("state=self._state",),
    )
    def list_recorder_ids(self) -> tuple[str, ...]:

        loop = self._get_running_loop_or_raise()

        future = asyncio.run_coroutine_threadsafe(
            self._list_recorder_ids_core(),
            loop,
        )

        return future.result()

    @log_invocation(
        "metrics_runtime.stop_recorder",
        context_fields=("state=self._state",),
        log_kwargs_on_invoke=("recorder_id",),
    )
    def stop_recorder(
        self,
        recorder_id: str,
    ) -> None:

        normalized_recorder_id = self._ensure_recorder_id(recorder_id)
        loop = self._get_running_loop_or_raise()

        future = asyncio.run_coroutine_threadsafe(
            self._stop_recorder_core(normalized_recorder_id),
            loop,
        )

        future.result()

    @log_invocation(
        "metrics_runtime.stop_and_remove_recorder",
        context_fields=("state=self._state",),
        log_kwargs_on_invoke=("recorder_id",),
    )
    def stop_and_remove_recorder(
        self,
        recorder_id: str,
    ) -> AsyncioMetricsRecorder:

        normalized_recorder_id = self._ensure_recorder_id(recorder_id)
        loop = self._get_running_loop_or_raise()

        future = asyncio.run_coroutine_threadsafe(
            self._stop_and_remove_recorder_core(normalized_recorder_id),
            loop,
        )

        return future.result()

    # ---- Internal functions --------------------------------------------------------------

    def _thread_main_function(self) -> None:
        loop: asyncio.AbstractEventLoop | None = None

        try:
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)

            with self._thread_lock:
                self._loop = new_loop

            self._loop_ready_event.set()
            loop = new_loop
            new_loop.run_forever()

        except Exception as exc:
            mapped_exc = MetricsRuntimeStartupError(cause=exc)

            with self._thread_lock:
                self._state = MetricsRuntimeState.FAILURE
                self._last_error = mapped_exc

            self._loop_ready_event.set()

        finally:
            if loop is not None:
                with contextlib.suppress(Exception):
                    pending_tasks = asyncio.all_tasks(loop)

                    for task in pending_tasks:
                        task.cancel()

                    if pending_tasks:
                        loop.run_until_complete(
                            asyncio.gather(
                                *pending_tasks,
                                return_exceptions=True,
                            )
                        )

                    loop.run_until_complete(loop.shutdown_asyncgens())
                    loop.run_until_complete(loop.shutdown_default_executor())

                asyncio.set_event_loop(None)
                loop.close()

    async def _shutdown_loop_core(self) -> None:

        def _build_shutdown_error_payload(
            errors: dict[str, Exception],
        ) -> dict[str, object]:
            return {
                "failed_recorder_ids": tuple(errors),
                "recorder_errors": {
                    _recorder_id: {
                        "error_type": type(_exc).__name__,
                        "error_message": str(_exc),
                    }
                    for _recorder_id, _exc in errors.items()
                },
            }

        assert self._is_running_in_runtime_loop()

        recorder_ids = tuple(self._recorders)
        shutdown_errors: dict[str, Exception] = {}

        for recorder_id in recorder_ids:
            try:
                await self._stop_and_remove_recorder_core(recorder_id)

            except MetricsRuntimeRecorderNotFoundError:
                # The recorder may already be in removing state or removed by another
                # runtime-loop operation. During global shutdown this is not fatal.
                pass

            except Exception as exc:
                shutdown_errors[recorder_id] = exc

        self._recorders.clear()
        self._creating_recorders.clear()
        self._removing_recorders.clear()

        if shutdown_errors:
            first_error = next(iter(shutdown_errors.values()))

            raise MetricsRuntimeShutdownError(
                cause=first_error,
                details=_build_shutdown_error_payload(shutdown_errors),
            )

    def _is_running_in_runtime_loop(self) -> bool:
        try:
            return asyncio.get_running_loop() is self._loop
        except RuntimeError:
            return False

    @staticmethod
    def _ensure_recorder_id(recorder_id: str) -> str:
        if recorder_id is None:
            raise ValueError("argument 'recorder_id' must not be None")

        if not isinstance(recorder_id, str):
            raise TypeError("argument 'recorder_id' must be string when provided")

        normalized_recorder_id = recorder_id.strip()

        if not normalized_recorder_id:
            raise ValueError("argument 'recorder_id' must not be empty")

        return normalized_recorder_id

    def _get_running_loop_or_raise(self) -> asyncio.AbstractEventLoop:
        with self._thread_lock:
            state = self._state
            loop = self._loop

            if state is not MetricsRuntimeState.RUNNING:
                raise MetricsRuntimeInvalidStateError(
                    runtime_state=state,
                    expected_states=(MetricsRuntimeState.RUNNING,),
                )

            if loop is None:
                raise MetricsRuntimeLoopUnavailableError()

            return loop

    async def _create_recorder_core(
        self,
        *,
        recorder_id: str,
        entity_id: str | None,
        namespace: str | None,
        queue_max_size: int | None,
        queue_overflow_policy: AsyncioMetricsRecorderQueueOverflowPolicy | None,
        log_context: LogContext | None,
    ) -> AsyncioMetricsRecorder:

        assert self._is_running_in_runtime_loop()

        if recorder_id in self._recorders or recorder_id in self._creating_recorders:
            raise MetricsRuntimeRecorderAlreadyExistsError(
                recorder_id=recorder_id,
            )

        self._creating_recorders.add(recorder_id)

        try:
            recorder = AsyncioMetricsRecorder(
                entity_id=entity_id or recorder_id,
                namespace=namespace or self._namespace,
                queue_max_size=(
                    queue_max_size
                    if queue_max_size is not None
                    else self._default_recorder_queue_max_size
                ),
                queue_overflow_policy=(
                    queue_overflow_policy
                    if queue_overflow_policy is not None
                    else self._default_recorder_queue_overflow_policy
                ),
                log_context=log_context if log_context is not None else self._log_context,
            )

            start_result = await recorder.start()

            if not start_result.success:
                raise MetricsRuntimeRecorderStartupError(
                    recorder_id=recorder_id,
                    cause=start_result.error,
                )

            self._recorders[recorder_id] = recorder

            return recorder

        finally:
            self._creating_recorders.discard(recorder_id)

    async def _get_recorder_core(
        self,
        recorder_id: str,
    ) -> AsyncioMetricsRecorder:

        assert self._is_running_in_runtime_loop()

        try:
            return self._recorders[recorder_id]
        except KeyError:
            raise MetricsRuntimeRecorderNotFoundError(
                recorder_id=recorder_id,
            )

    async def _try_get_recorder_core(
        self,
        recorder_id: str,
    ) -> AsyncioMetricsRecorder | None:

        assert self._is_running_in_runtime_loop()

        return self._recorders.get(recorder_id)

    async def _list_recorder_ids_core(self) -> tuple[str, ...]:

        assert self._is_running_in_runtime_loop()

        return tuple(self._recorders)

    async def _stop_recorder_core(
        self,
        recorder_id: str,
    ) -> None:

        assert self._is_running_in_runtime_loop()

        if recorder_id in self._removing_recorders:
            raise MetricsRuntimeRecorderNotFoundError(
                recorder_id=recorder_id,
            )

        try:
            recorder = self._recorders[recorder_id]
        except KeyError:
            raise MetricsRuntimeRecorderNotFoundError(
                recorder_id=recorder_id,
            ) from None

        recorder_state = recorder.get_status()

        if recorder_state.name == "STOPPED":
            return

        if recorder_state.name not in (
            "RUNNING",
            "STOPPING",
        ):
            raise MetricsRuntimeRecorderStopError(
                recorder_id=recorder_id,
            )

        stop_result = await recorder.stop()

        if not stop_result.success:
            raise MetricsRuntimeRecorderStopError(
                recorder_id=recorder_id,
                cause=stop_result.error,
            )

    async def _stop_and_remove_recorder_core(
        self,
        recorder_id: str,
    ) -> AsyncioMetricsRecorder:

        assert self._is_running_in_runtime_loop()

        if recorder_id in self._removing_recorders:
            raise MetricsRuntimeRecorderNotFoundError(
                recorder_id=recorder_id,
            )

        try:
            recorder = self._recorders[recorder_id]
        except KeyError:
            raise MetricsRuntimeRecorderNotFoundError(
                recorder_id=recorder_id,
            ) from None

        self._removing_recorders.add(recorder_id)

        stop_error: MetricsRuntimeRecorderStopError | None = None

        try:
            recorder_state = recorder.get_status()

            if recorder_state.name in (
                "RUNNING",
                "STOPPING",
            ):
                stop_result = await recorder.stop()

                if not stop_result.success:
                    stop_error = MetricsRuntimeRecorderStopError(
                        recorder_id=recorder_id,
                        cause=stop_result.error,
                    )

            elif recorder_state.name == "STOPPED":
                pass

            else:
                stop_error = MetricsRuntimeRecorderStopError(
                    recorder_id=recorder_id,
                )

        finally:
            self._recorders.pop(recorder_id, None)
            self._removing_recorders.discard(recorder_id)

        if stop_error is not None:
            raise stop_error

        return recorder
