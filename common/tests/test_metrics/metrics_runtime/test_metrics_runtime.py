# tests/test_metrics/metrics_runtime/test_metrics_runtime.py

from __future__ import annotations

from typing import Any, cast
from collections.abc import Callable


import asyncio
import concurrent.futures
import contextlib
import threading
import time

import pytest

from mvx.common.logger.log_context import LogContext
from mvx.common.logger.models import LogEvent
from mvx.common.logger.log_payload_processor import LogPayloadProcessor


import mvx.common.metrics.metrics_runtime.metrics_runtime as runtime_pack

from mvx.common.metrics import (
    MetricsRuntime,
    MetricsRuntimeState,
    MetricsRuntimeInvalidStateError,
    MetricsRuntimeLoopUnavailableError,
    MetricsRuntimeShutdownError,
    MetricsRuntimeStartupError,
    MetricsRuntimeRecorderAlreadyExistsError,
    MetricsRuntimeRecorderStartupError,
    MetricsRuntimeRecorderNotFoundError,
    MetricsRuntimeRecorderStopError,
    AsyncioMetricsRecorder,
    AsyncioMetricsRecorderQueueOverflowPolicy,
)

TIMEOUT = 2.0


class _MemoryLogSink:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.events: list[LogEvent] = []

    def log(self, event: LogEvent) -> None:
        with self._lock:
            self.events.append(event)


def make_log_context() -> LogContext:
    return LogContext(
        namespace="metrics-runtime-tests",
        log_sink=_MemoryLogSink(),
        payload_processor=LogPayloadProcessor(),
    )


@pytest.fixture()
def memory_log_sink() -> _MemoryLogSink:
    return _MemoryLogSink()


@pytest.fixture()
def memory_log_context(memory_log_sink: _MemoryLogSink) -> LogContext:
    return LogContext(
        namespace="metrics-runtime-tests",
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


def make_runtime(namespace: str = "test") -> MetricsRuntime:
    return MetricsRuntime(namespace=namespace)


def shutdown_safely(runtime: MetricsRuntime) -> None:
    with contextlib.suppress(Exception):
        runtime.shutdown()


def get_runtime_thread(runtime: MetricsRuntime) -> threading.Thread | None:
    # noinspection PyProtectedMember
    return runtime._thread


def get_runtime_loop(runtime: MetricsRuntime) -> asyncio.AbstractEventLoop | None:
    # noinspection PyProtectedMember
    return runtime._loop


def wait_thread_event(event: threading.Event, timeout: float = TIMEOUT) -> None:
    assert event.wait(timeout), "threading.Event was not set in time"


def wait_until_sync(
    predicate: Callable[[], bool],
    *,
    timeout: float = TIMEOUT,
    interval: float = 0.005,
) -> None:
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if predicate():
            return

        time.sleep(interval)

    raise AssertionError("condition was not satisfied in time")


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


# -------------------------
# Group a: constructor and basic invariants
# -------------------------


def test_a01_constructor_creates_virgin_runtime() -> None:
    runtime = make_runtime()

    assert runtime.get_status() is MetricsRuntimeState.VIRGIN
    assert runtime._namespace == "test"
    assert runtime._default_recorder_queue_max_size is None
    assert (
        runtime._default_recorder_queue_overflow_policy
        is AsyncioMetricsRecorderQueueOverflowPolicy.DROP
    )
    assert runtime._log_context is None
    assert runtime._recorders == {}
    assert runtime._creating_recorders == set()
    assert runtime._removing_recorders == set()
    assert runtime._thread is None
    assert runtime._loop is None
    assert runtime._last_error is None


def test_a02_constructor_accepts_valid_queue_max_size() -> None:
    runtime = MetricsRuntime(
        namespace="test",
        default_recorder_queue_max_size=16,
    )

    assert runtime.get_status() is MetricsRuntimeState.VIRGIN
    assert runtime._default_recorder_queue_max_size == 16


def test_a03_constructor_accepts_valid_queue_overflow_policy() -> None:
    # noinspection PyArgumentEqualDefault
    runtime = MetricsRuntime(
        namespace="test",
        default_recorder_queue_overflow_policy=AsyncioMetricsRecorderQueueOverflowPolicy.DROP,
    )

    assert runtime.get_status() is MetricsRuntimeState.VIRGIN
    assert (
        runtime._default_recorder_queue_overflow_policy
        is AsyncioMetricsRecorderQueueOverflowPolicy.DROP
    )


def test_a04_constructor_rejects_none_namespace() -> None:
    with pytest.raises(ValueError, match="namespace"):
        MetricsRuntime(namespace=cast(Any, None))


def test_a05_constructor_rejects_non_string_namespace() -> None:
    with pytest.raises(TypeError, match="namespace"):
        MetricsRuntime(namespace=cast(Any, 123))


def test_a06_constructor_rejects_bool_queue_max_size() -> None:
    with pytest.raises(TypeError, match="default_recorder_queue_max_size"):
        MetricsRuntime(
            namespace="test",
            default_recorder_queue_max_size=cast(Any, True),
        )


def test_a07_constructor_rejects_non_integer_queue_max_size() -> None:
    with pytest.raises(TypeError, match="default_recorder_queue_max_size"):
        MetricsRuntime(
            namespace="test",
            default_recorder_queue_max_size=cast(Any, 1.5),
        )


def test_a08_constructor_rejects_zero_queue_max_size() -> None:
    with pytest.raises(ValueError, match="default_recorder_queue_max_size"):
        MetricsRuntime(
            namespace="test",
            default_recorder_queue_max_size=0,
        )


def test_a09_constructor_rejects_negative_queue_max_size() -> None:
    with pytest.raises(ValueError, match="default_recorder_queue_max_size"):
        MetricsRuntime(
            namespace="test",
            default_recorder_queue_max_size=-1,
        )


def test_a10_constructor_rejects_invalid_queue_overflow_policy() -> None:
    with pytest.raises(TypeError, match="default_recorder_queue_overflow_policy"):
        MetricsRuntime(
            namespace="test",
            default_recorder_queue_overflow_policy=cast(Any, "DROP"),
        )


def test_a11_constructor_rejects_invalid_log_context() -> None:
    with pytest.raises(TypeError, match="log_context"):
        MetricsRuntime(
            namespace="test",
            log_context=cast(Any, object()),
        )


# -------------------------
# Group b: start lifecycle
# -------------------------


def test_b01_start_from_virgin_moves_runtime_to_running() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        thread = get_runtime_thread(runtime)
        loop = get_runtime_loop(runtime)

        assert runtime.get_status() is MetricsRuntimeState.RUNNING
        assert thread is not None
        assert thread.is_alive()
        assert thread.name == "test.metrics_runtime"
        assert loop is not None
        assert not loop.is_closed()

    finally:
        shutdown_safely(runtime)


def test_b02_start_from_running_is_idempotent() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        first_thread = get_runtime_thread(runtime)
        first_loop = get_runtime_loop(runtime)

        runtime.start()

        assert runtime.get_status() is MetricsRuntimeState.RUNNING
        assert get_runtime_thread(runtime) is first_thread
        assert get_runtime_loop(runtime) is first_loop

    finally:
        shutdown_safely(runtime)


def test_b03_start_after_closed_fails() -> None:
    runtime = make_runtime()

    runtime.shutdown()

    assert runtime.get_status() is MetricsRuntimeState.CLOSED

    with pytest.raises(MetricsRuntimeInvalidStateError):
        runtime.start()

    assert runtime.get_status() is MetricsRuntimeState.CLOSED


def test_b04_start_failure_from_thread_main_is_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()

    def new_event_loop_failed() -> asyncio.AbstractEventLoop:
        raise RuntimeError("boom")

    monkeypatch.setattr(
        runtime_pack.asyncio,
        "new_event_loop",
        new_event_loop_failed,
    )

    with pytest.raises(MetricsRuntimeStartupError):
        runtime.start()

    assert runtime.get_status() is MetricsRuntimeState.FAILURE

    thread = get_runtime_thread(runtime)

    assert thread is not None

    thread.join(timeout=TIMEOUT)

    assert not thread.is_alive()


def test_b05_start_failure_when_loop_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()

    def thread_main_without_loop(self: MetricsRuntime) -> None:
        self._loop_ready_event.set()

    monkeypatch.setattr(
        MetricsRuntime,
        "_thread_main_function",
        thread_main_without_loop,
    )

    with pytest.raises(MetricsRuntimeLoopUnavailableError):
        runtime.start()

    assert runtime.get_status() is MetricsRuntimeState.FAILURE

    thread = get_runtime_thread(runtime)

    assert thread is not None

    thread.join(timeout=TIMEOUT)

    assert not thread.is_alive()


def test_b06_start_timeout_moves_runtime_to_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()
    release_thread = threading.Event()

    monkeypatch.setattr(
        runtime_pack,
        "DEFAULT_RUNTIME_THREAD_START_TIMEOUT_S",
        0.02,
    )

    def blocked_thread_main(self: MetricsRuntime) -> None:
        _ = self
        release_thread.wait(timeout=TIMEOUT)

    monkeypatch.setattr(
        MetricsRuntime,
        "_thread_main_function",
        blocked_thread_main,
    )

    try:
        with pytest.raises(MetricsRuntimeStartupError):
            runtime.start()

        assert runtime.get_status() is MetricsRuntimeState.FAILURE

    finally:
        release_thread.set()

        thread = get_runtime_thread(runtime)

        if thread is not None:
            thread.join(timeout=TIMEOUT)
            assert not thread.is_alive()


# -------------------------
# Group c: shutdown lifecycle
# -------------------------


def test_c01_shutdown_from_virgin_closes_runtime() -> None:
    runtime = make_runtime()

    runtime.shutdown()

    assert runtime.get_status() is MetricsRuntimeState.CLOSED
    assert runtime._thread is None
    assert runtime._loop is None
    assert runtime._last_error is None


def test_c02_shutdown_from_closed_is_idempotent() -> None:
    runtime = make_runtime()

    runtime.shutdown()
    runtime.shutdown()

    assert runtime.get_status() is MetricsRuntimeState.CLOSED
    assert runtime._thread is None
    assert runtime._loop is None
    assert runtime._last_error is None


def test_c03_shutdown_from_running_closes_thread_and_loop() -> None:
    runtime = make_runtime()

    runtime.start()

    thread = get_runtime_thread(runtime)
    loop = get_runtime_loop(runtime)

    assert thread is not None
    assert loop is not None
    assert thread.is_alive()
    assert not loop.is_closed()

    runtime.shutdown()

    assert runtime.get_status() is MetricsRuntimeState.CLOSED
    assert runtime._thread is None
    assert runtime._loop is None
    assert runtime._last_error is None
    assert not thread.is_alive()
    assert loop.is_closed()


def test_c04_shutdown_clears_empty_recorder_registry() -> None:
    runtime = make_runtime()

    runtime.start()
    runtime.shutdown()

    assert runtime.get_status() is MetricsRuntimeState.CLOSED
    assert runtime._recorders == {}


def test_c05_shutdown_from_failure_state_fails() -> None:
    runtime = make_runtime()

    runtime._state = MetricsRuntimeState.FAILURE

    with pytest.raises(MetricsRuntimeInvalidStateError):
        runtime.shutdown()

    assert runtime.get_status() is MetricsRuntimeState.FAILURE


def test_c06_shutdown_from_starting_state_fails() -> None:
    runtime = make_runtime()

    runtime._state = MetricsRuntimeState.STARTING

    with pytest.raises(MetricsRuntimeInvalidStateError):
        runtime.shutdown()

    assert runtime.get_status() is MetricsRuntimeState.STARTING


def test_c07_shutdown_reports_loop_unavailable_from_running_state() -> None:
    runtime = make_runtime()

    runtime._state = MetricsRuntimeState.RUNNING
    runtime._loop = None

    with pytest.raises(MetricsRuntimeLoopUnavailableError):
        runtime.shutdown()

    assert runtime.get_status() is MetricsRuntimeState.FAILURE
    assert isinstance(runtime._last_error, MetricsRuntimeLoopUnavailableError)


# -------------------------
# Group d: runtime loop thread access
# -------------------------


def test_d01_get_status_can_be_called_from_runtime_loop_thread() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        loop = get_runtime_loop(runtime)

        assert loop is not None

        async def read_status() -> MetricsRuntimeState:
            return runtime.get_status()

        future = asyncio.run_coroutine_threadsafe(read_status(), loop)

        assert future.result(timeout=TIMEOUT) is MetricsRuntimeState.RUNNING

    finally:
        shutdown_safely(runtime)


def test_d02_runtime_loop_executes_coroutines_on_runtime_thread() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        loop = get_runtime_loop(runtime)
        thread = get_runtime_thread(runtime)

        assert loop is not None
        assert thread is not None

        async def read_thread_identity() -> int:
            return threading.get_ident()

        future = asyncio.run_coroutine_threadsafe(read_thread_identity(), loop)

        assert future.result(timeout=TIMEOUT) == thread.ident

    finally:
        shutdown_safely(runtime)


# -------------------------
# Group d2: runtime loop detection
# -------------------------


def test_d2_01_is_running_in_runtime_loop_returns_false_without_running_loop() -> None:
    runtime = make_runtime()

    assert runtime._is_running_in_runtime_loop() is False


def test_d2_02_is_running_in_runtime_loop_returns_true_inside_runtime_loop() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        loop = get_runtime_loop(runtime)

        assert loop is not None

        async def check_runtime_loop() -> bool:
            return runtime._is_running_in_runtime_loop()

        future = asyncio.run_coroutine_threadsafe(check_runtime_loop(), loop)

        assert future.result(timeout=TIMEOUT) is True

    finally:
        shutdown_safely(runtime)


def test_d2_03_is_running_in_runtime_loop_returns_false_inside_foreign_loop() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        async def check_foreign_loop() -> bool:
            return runtime._is_running_in_runtime_loop()

        assert asyncio.run(check_foreign_loop()) is False

    finally:
        shutdown_safely(runtime)


# -------------------------
# Group e: concurrent start requests
# -------------------------


def test_e01_start_from_many_threads_after_running_is_idempotent() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        results = run_many_threads(
            8,
            lambda _index: runtime.start(),
        )

        assert results == [None] * 8
        assert runtime.get_status() is MetricsRuntimeState.RUNNING

    finally:
        shutdown_safely(runtime)


def test_e02_concurrent_start_during_starting_allows_only_one_starter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()
    release_startup = threading.Event()
    entered_thread_main = threading.Event()

    def delayed_thread_main(self: MetricsRuntime) -> None:
        entered_thread_main.set()
        release_startup.wait(timeout=TIMEOUT)
        self._loop_ready_event.set()

    monkeypatch.setattr(
        MetricsRuntime,
        "_thread_main_function",
        delayed_thread_main,
    )
    monkeypatch.setattr(
        runtime_pack,
        "DEFAULT_RUNTIME_THREAD_START_TIMEOUT_S",
        0.5,
    )

    first_error: list[BaseException] = []
    second_error: list[BaseException] = []

    def first_start() -> None:
        try:
            runtime.start()
        except BaseException as exc:
            first_error.append(exc)

    def second_start() -> None:
        try:
            runtime.start()
        except BaseException as exc:
            second_error.append(exc)

    first_thread = threading.Thread(target=first_start)
    first_thread.start()

    wait_thread_event(entered_thread_main)

    assert runtime.get_status() is MetricsRuntimeState.STARTING

    second_thread = threading.Thread(target=second_start)
    second_thread.start()
    second_thread.join(timeout=TIMEOUT)

    release_startup.set()

    first_thread.join(timeout=TIMEOUT)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert len(second_error) == 1
    assert isinstance(second_error[0], MetricsRuntimeInvalidStateError)
    assert len(first_error) == 1
    assert isinstance(first_error[0], MetricsRuntimeLoopUnavailableError)
    assert runtime.get_status() is MetricsRuntimeState.FAILURE


# -------------------------
# Group f: concurrent shutdown requests
# -------------------------


def test_f01_shutdown_from_many_threads_closes_runtime_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()

    async def slow_shutdown_core(self: MetricsRuntime) -> None:
        await asyncio.sleep(0.05)
        self._recorders.clear()

    monkeypatch.setattr(
        MetricsRuntime,
        "_shutdown_loop_core",
        slow_shutdown_core,
    )

    runtime.start()

    results = run_many_threads(
        8,
        lambda _index: runtime.shutdown(),
    )

    assert results == [None] * 8
    assert runtime.get_status() is MetricsRuntimeState.CLOSED
    assert runtime._thread is None
    assert runtime._loop is None


def test_f02_shutdown_waits_for_existing_stopping_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()
    shutdown_started = threading.Event()
    release_shutdown = threading.Event()

    async def controlled_shutdown_core(self: MetricsRuntime) -> None:
        shutdown_started.set()

        while not release_shutdown.is_set():
            await asyncio.sleep(0.005)

        self._recorders.clear()

    monkeypatch.setattr(
        MetricsRuntime,
        "_shutdown_loop_core",
        controlled_shutdown_core,
    )

    runtime.start()

    first_error: list[BaseException] = []
    second_error: list[BaseException] = []

    def first_shutdown() -> None:
        try:
            runtime.shutdown()
        except BaseException as exc:
            first_error.append(exc)

    def second_shutdown() -> None:
        try:
            runtime.shutdown()
        except BaseException as exc:
            second_error.append(exc)

    first_thread = threading.Thread(target=first_shutdown)
    first_thread.start()

    wait_thread_event(shutdown_started)

    assert runtime.get_status() is MetricsRuntimeState.STOPPING

    second_thread = threading.Thread(target=second_shutdown)
    second_thread.start()

    release_shutdown.set()

    first_thread.join(timeout=TIMEOUT)
    second_thread.join(timeout=TIMEOUT)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert first_error == []
    assert second_error == []
    assert runtime.get_status() is MetricsRuntimeState.CLOSED


# -------------------------
# Group g: shutdown failures and cleanup
# -------------------------


def test_g01_shutdown_core_generic_error_becomes_shutdown_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()

    async def failing_shutdown_core(self: MetricsRuntime) -> None:
        _ = self
        raise RuntimeError("shutdown failed")

    monkeypatch.setattr(
        MetricsRuntime,
        "_shutdown_loop_core",
        failing_shutdown_core,
    )

    runtime.start()

    with pytest.raises(MetricsRuntimeShutdownError):
        runtime.shutdown()

    assert runtime.get_status() is MetricsRuntimeState.FAILURE
    assert runtime._thread is None
    assert runtime._loop is None
    assert isinstance(runtime._last_error, MetricsRuntimeShutdownError)


def test_g02_shutdown_core_metrics_error_is_preserved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()
    expected_error = MetricsRuntimeShutdownError()

    async def failing_shutdown_core(self: MetricsRuntime) -> None:
        _ = self
        raise expected_error

    monkeypatch.setattr(
        MetricsRuntime,
        "_shutdown_loop_core",
        failing_shutdown_core,
    )

    runtime.start()

    with pytest.raises(MetricsRuntimeShutdownError) as exc_info:
        runtime.shutdown()

    assert exc_info.value is expected_error
    assert runtime.get_status() is MetricsRuntimeState.FAILURE
    assert runtime._thread is None
    assert runtime._loop is None
    assert runtime._last_error is expected_error


def test_g03_shutdown_join_timeout_is_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()
    blocking_callback_finished = threading.Event()

    monkeypatch.setattr(
        runtime_pack,
        "DEFAULT_RUNTIME_SHUTDOWN_TIMEOUT_S",
        0.02,
    )
    monkeypatch.setattr(
        runtime_pack,
        "DEFAULT_RUNTIME_THREAD_JOIN_TIMEOUT_S",
        0.02,
    )

    runtime.start()

    loop = get_runtime_loop(runtime)

    assert loop is not None

    def blocking_callback() -> None:
        try:
            time.sleep(0.2)
        finally:
            blocking_callback_finished.set()

    # noinspection PyTypeChecker
    loop.call_soon_threadsafe(blocking_callback)

    with pytest.raises(MetricsRuntimeShutdownError):
        runtime.shutdown()

    assert runtime.get_status() is MetricsRuntimeState.FAILURE
    assert isinstance(runtime._last_error, MetricsRuntimeShutdownError)

    wait_thread_event(blocking_callback_finished)

    thread = get_runtime_thread(runtime)

    if thread is not None:
        thread.join(timeout=TIMEOUT)
        assert not thread.is_alive()


def test_g04_pending_tasks_are_cancelled_when_runtime_loop_stops() -> None:
    runtime = make_runtime()
    task_started = threading.Event()
    task_cancelled = threading.Event()

    runtime.start()

    loop = get_runtime_loop(runtime)

    assert loop is not None

    async def never_finishes() -> None:
        task_started.set()

        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            task_cancelled.set()
            raise

    future = asyncio.run_coroutine_threadsafe(never_finishes(), loop)

    wait_thread_event(task_started)

    runtime.shutdown()

    assert runtime.get_status() is MetricsRuntimeState.CLOSED
    assert task_cancelled.wait(TIMEOUT)
    assert future.done()


def test_g05_shutdown_waits_for_shutdown_core_before_stopping_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()
    core_completed = threading.Event()

    async def controlled_shutdown_core(self: MetricsRuntime) -> None:
        _ = self
        await asyncio.sleep(0.01)
        core_completed.set()

    monkeypatch.setattr(
        MetricsRuntime,
        "_shutdown_loop_core",
        controlled_shutdown_core,
    )

    runtime.start()
    runtime.shutdown()

    assert core_completed.is_set()
    assert runtime.get_status() is MetricsRuntimeState.CLOSED


# -------------------------
# Group h: recorder API placeholders
# -------------------------


# -------------------------
# Group i: create recorder
# -------------------------


def test_i01_create_recorder_before_start_fails() -> None:
    runtime = make_runtime()

    with pytest.raises(MetricsRuntimeInvalidStateError):
        runtime.create_recorder("recorder")

    assert runtime.get_status() is MetricsRuntimeState.VIRGIN
    assert runtime._recorders == {}


def test_i02_create_recorder_after_shutdown_fails() -> None:
    runtime = make_runtime()

    runtime.shutdown()

    with pytest.raises(MetricsRuntimeInvalidStateError):
        runtime.create_recorder("recorder")

    assert runtime.get_status() is MetricsRuntimeState.CLOSED
    assert runtime._recorders == {}


def test_i03_create_recorder_rejects_none_recorder_id() -> None:
    runtime = make_runtime()

    with pytest.raises(ValueError, match="recorder_id"):
        runtime.create_recorder(cast(Any, None))


def test_i04_create_recorder_rejects_non_string_recorder_id() -> None:
    runtime = make_runtime()

    with pytest.raises(TypeError, match="recorder_id"):
        runtime.create_recorder(cast(Any, 123))


def test_i05_create_recorder_rejects_empty_recorder_id() -> None:
    runtime = make_runtime()

    with pytest.raises(ValueError, match="recorder_id"):
        runtime.create_recorder("   ")


def test_i06_create_recorder_rejects_invalid_entity_id() -> None:
    runtime = make_runtime()

    with pytest.raises(TypeError, match="entity_id"):
        runtime.create_recorder(
            "recorder",
            entity_id=cast(Any, 123),
        )


def test_i07_create_recorder_rejects_invalid_namespace() -> None:
    runtime = make_runtime()

    with pytest.raises(TypeError, match="namespace"):
        runtime.create_recorder(
            "recorder",
            namespace=cast(Any, 123),
        )


def test_i08_create_recorder_rejects_bool_queue_max_size() -> None:
    runtime = make_runtime()

    with pytest.raises(TypeError, match="queue_max_size"):
        runtime.create_recorder(
            "recorder",
            queue_max_size=cast(Any, True),
        )


def test_i09_create_recorder_rejects_non_integer_queue_max_size() -> None:
    runtime = make_runtime()

    with pytest.raises(TypeError, match="queue_max_size"):
        runtime.create_recorder(
            "recorder",
            queue_max_size=cast(Any, 1.5),
        )


def test_i10_create_recorder_rejects_zero_queue_max_size() -> None:
    runtime = make_runtime()

    with pytest.raises(ValueError, match="queue_max_size"):
        runtime.create_recorder(
            "recorder",
            queue_max_size=0,
        )


def test_i11_create_recorder_rejects_negative_queue_max_size() -> None:
    runtime = make_runtime()

    with pytest.raises(ValueError, match="queue_max_size"):
        runtime.create_recorder(
            "recorder",
            queue_max_size=-1,
        )


def test_i12_create_recorder_rejects_invalid_queue_overflow_policy() -> None:
    runtime = make_runtime()

    with pytest.raises(TypeError, match="queue_overflow_policy"):
        runtime.create_recorder(
            "recorder",
            queue_overflow_policy=cast(Any, "DROP"),
        )


def test_i13_create_recorder_rejects_invalid_log_context() -> None:
    runtime = make_runtime()

    with pytest.raises(TypeError, match="log_context"):
        runtime.create_recorder(
            "recorder",
            log_context=cast(Any, object()),
        )


def test_i14_create_recorder_creates_running_recorder() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        assert isinstance(recorder, AsyncioMetricsRecorder)
        assert recorder.get_status().name == "RUNNING"
        assert runtime._recorders == {"recorder": recorder}

    finally:
        shutdown_safely(runtime)


def test_i15_create_recorder_normalizes_recorder_id() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        recorder = runtime.create_recorder("  recorder  ")

        assert runtime._recorders == {"recorder": recorder}
        assert recorder.identity == "recorder"

    finally:
        shutdown_safely(runtime)


def test_i16_create_recorder_uses_recorder_id_as_default_entity_id() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        assert recorder.identity == "recorder"

    finally:
        shutdown_safely(runtime)


def test_i17_create_recorder_accepts_custom_entity_id() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        recorder = runtime.create_recorder(
            "recorder",
            entity_id="entity",
        )

        assert recorder.identity == "entity"
        assert runtime._recorders["recorder"] is recorder

    finally:
        shutdown_safely(runtime)


def test_i18_create_recorder_uses_runtime_default_queue_options() -> None:
    # noinspection PyArgumentEqualDefault
    runtime = MetricsRuntime(
        namespace="test",
        default_recorder_queue_max_size=7,
        default_recorder_queue_overflow_policy=AsyncioMetricsRecorderQueueOverflowPolicy.DROP,
    )

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        assert recorder._max_pending_counter == 7
        assert recorder._queue_overflow_policy is AsyncioMetricsRecorderQueueOverflowPolicy.DROP

    finally:
        shutdown_safely(runtime)


def test_i19_create_recorder_accepts_custom_queue_options() -> None:
    # noinspection PyArgumentEqualDefault
    runtime = MetricsRuntime(
        namespace="test",
        default_recorder_queue_max_size=7,
        default_recorder_queue_overflow_policy=AsyncioMetricsRecorderQueueOverflowPolicy.DROP,
    )

    try:
        runtime.start()

        recorder = runtime.create_recorder(
            "recorder",
            queue_max_size=3,
            queue_overflow_policy=AsyncioMetricsRecorderQueueOverflowPolicy.RAISE_ERROR,
        )

        assert recorder._max_pending_counter == 3
        assert (
            recorder._queue_overflow_policy is AsyncioMetricsRecorderQueueOverflowPolicy.RAISE_ERROR
        )

    finally:
        shutdown_safely(runtime)


def test_i20_create_recorder_creates_recorder_in_runtime_loop() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        runtime_loop = get_runtime_loop(runtime)

        assert runtime_loop is not None

        recorder = runtime.create_recorder("recorder")

        assert recorder._loop is runtime_loop

    finally:
        shutdown_safely(runtime)


def test_i21_create_recorder_core_runs_in_runtime_loop_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()
    core_thread_id: list[int] = []

    original_create_recorder_core = MetricsRuntime._create_recorder_core

    async def tracking_create_recorder_core(
        self: MetricsRuntime,
        *,
        recorder_id: str,
        entity_id: str | None,
        namespace: str | None,
        queue_max_size: int | None,
        queue_overflow_policy: AsyncioMetricsRecorderQueueOverflowPolicy | None,
        log_context: LogContext | None,
    ) -> AsyncioMetricsRecorder:
        core_thread_id.append(threading.get_ident())

        # noinspection PyTypeChecker
        return await original_create_recorder_core(
            self,
            recorder_id=recorder_id,
            entity_id=entity_id,
            namespace=namespace,
            queue_max_size=queue_max_size,
            queue_overflow_policy=queue_overflow_policy,
            log_context=log_context,
        )

    monkeypatch.setattr(
        MetricsRuntime,
        "_create_recorder_core",
        tracking_create_recorder_core,
    )

    try:
        runtime.start()

        thread = get_runtime_thread(runtime)

        assert thread is not None

        runtime.create_recorder("recorder")

        assert core_thread_id == [thread.ident]

    finally:
        shutdown_safely(runtime)


def test_i22_create_recorder_duplicate_fails() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        first_recorder = runtime.create_recorder("recorder")

        with pytest.raises(MetricsRuntimeRecorderAlreadyExistsError):
            runtime.create_recorder("recorder")

        assert runtime._recorders == {"recorder": first_recorder}

    finally:
        shutdown_safely(runtime)


def test_i23_create_recorder_duplicate_with_normalized_id_fails() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        first_recorder = runtime.create_recorder("recorder")

        with pytest.raises(MetricsRuntimeRecorderAlreadyExistsError):
            runtime.create_recorder("  recorder  ")

        assert runtime._recorders == {"recorder": first_recorder}

    finally:
        shutdown_safely(runtime)


def test_i24_create_recorder_start_failure_is_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()

    class FailingRecorder(AsyncioMetricsRecorder):
        def start(self) -> Any:
            handle = super().start()
            handle._future.set_exception(RuntimeError("start failed"))
            return handle

    monkeypatch.setattr(
        runtime_pack,
        "AsyncioMetricsRecorder",
        FailingRecorder,
    )

    try:
        runtime.start()

        with pytest.raises(MetricsRuntimeRecorderStartupError):
            runtime.create_recorder("recorder")

        assert runtime._recorders == {}

    finally:
        shutdown_safely(runtime)


def test_i25_create_recorder_constructor_failure_is_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()

    class FailingRecorder:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise RuntimeError("constructor failed")

    monkeypatch.setattr(
        runtime_pack,
        "AsyncioMetricsRecorder",
        FailingRecorder,
    )

    try:
        runtime.start()

        with pytest.raises(MetricsRuntimeRecorderStartupError):
            runtime.create_recorder("recorder")

        assert runtime._recorders == {}

    finally:
        shutdown_safely(runtime)


def test_i26_concurrent_create_recorder_with_different_ids_succeeds() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        recorders = run_many_threads(
            8,
            lambda index: runtime.create_recorder(f"recorder-{index}"),
        )

        assert len(recorders) == 8
        assert len(set(map(id, recorders))) == 8
        assert set(runtime._recorders) == {f"recorder-{index}" for index in range(8)}

    finally:
        shutdown_safely(runtime)


def test_i27_concurrent_create_recorder_with_same_id_allows_only_one() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        def create_or_return_error(_index: int) -> object:
            try:
                return runtime.create_recorder("recorder")
            except MetricsRuntimeRecorderAlreadyExistsError as exc:
                return exc

        results = run_many_threads(
            8,
            create_or_return_error,
        )

        created = [result for result in results if isinstance(result, AsyncioMetricsRecorder)]
        errors = [
            result
            for result in results
            if isinstance(result, MetricsRuntimeRecorderAlreadyExistsError)
        ]

        assert len(created) == 1
        assert len(errors) == 7
        assert runtime._recorders == {"recorder": created[0]}

    finally:
        shutdown_safely(runtime)


def test_i28_failed_create_releases_recorder_id_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()

    class FailingRecorder(AsyncioMetricsRecorder):
        def start(self) -> Any:
            handle = super().start()
            handle._future.set_exception(RuntimeError("start failed"))
            return handle

    monkeypatch.setattr(
        runtime_pack,
        "AsyncioMetricsRecorder",
        FailingRecorder,
    )

    try:
        runtime.start()

        with pytest.raises(MetricsRuntimeRecorderStartupError):
            runtime.create_recorder("recorder")

        assert runtime._recorders == {}
        assert runtime._creating_recorders == set()

    finally:
        shutdown_safely(runtime)


def test_i28_create_recorder_accepts_custom_namespace() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        recorder = runtime.create_recorder(
            "recorder",
            namespace="custom.namespace",
        )

        assert recorder._namespace == "custom.namespace"
        assert runtime._recorders == {"recorder": recorder}

    finally:
        shutdown_safely(runtime)


def test_i29_create_recorder_accepts_valid_log_context() -> None:
    runtime = make_runtime()
    log_context = make_log_context()

    try:
        runtime.start()

        recorder = runtime.create_recorder(
            "recorder",
            log_context=log_context,
        )

        assert recorder.get_log_context() is log_context

    finally:
        runtime.shutdown()


# -------------------------
# Group j: recorder registry read API
# -------------------------


def test_j01_get_recorder_before_start_fails() -> None:
    runtime = make_runtime()

    with pytest.raises(MetricsRuntimeInvalidStateError):
        runtime.get_recorder("recorder")


def test_j02_try_get_recorder_before_start_fails() -> None:
    runtime = make_runtime()

    with pytest.raises(MetricsRuntimeInvalidStateError):
        runtime.try_get_recorder("recorder")


def test_j03_list_recorder_ids_before_start_fails() -> None:
    runtime = make_runtime()

    with pytest.raises(MetricsRuntimeInvalidStateError):
        runtime.list_recorder_ids()


def test_j04_get_recorder_after_shutdown_fails() -> None:
    runtime = make_runtime()

    runtime.shutdown()

    with pytest.raises(MetricsRuntimeInvalidStateError):
        runtime.get_recorder("recorder")


def test_j05_try_get_recorder_after_shutdown_fails() -> None:
    runtime = make_runtime()

    runtime.shutdown()

    with pytest.raises(MetricsRuntimeInvalidStateError):
        runtime.try_get_recorder("recorder")


def test_j06_list_recorder_ids_after_shutdown_fails() -> None:
    runtime = make_runtime()

    runtime.shutdown()

    with pytest.raises(MetricsRuntimeInvalidStateError):
        runtime.list_recorder_ids()


def test_j07_get_recorder_rejects_none_recorder_id() -> None:
    runtime = make_runtime()

    with pytest.raises(ValueError, match="recorder_id"):
        runtime.get_recorder(cast(Any, None))


def test_j08_get_recorder_rejects_non_string_recorder_id() -> None:
    runtime = make_runtime()

    with pytest.raises(TypeError, match="recorder_id"):
        runtime.get_recorder(cast(Any, 123))


def test_j09_get_recorder_rejects_empty_recorder_id() -> None:
    runtime = make_runtime()

    with pytest.raises(ValueError, match="recorder_id"):
        runtime.get_recorder("   ")


def test_j10_try_get_recorder_rejects_none_recorder_id() -> None:
    runtime = make_runtime()

    with pytest.raises(ValueError, match="recorder_id"):
        runtime.try_get_recorder(cast(Any, None))


def test_j11_try_get_recorder_rejects_non_string_recorder_id() -> None:
    runtime = make_runtime()

    with pytest.raises(TypeError, match="recorder_id"):
        runtime.try_get_recorder(cast(Any, 123))


def test_j12_try_get_recorder_rejects_empty_recorder_id() -> None:
    runtime = make_runtime()

    with pytest.raises(ValueError, match="recorder_id"):
        runtime.try_get_recorder("   ")


def test_j13_get_recorder_returns_registered_recorder() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        assert runtime.get_recorder("recorder") is recorder

    finally:
        shutdown_safely(runtime)


def test_j14_get_recorder_normalizes_recorder_id() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        assert runtime.get_recorder("  recorder  ") is recorder

    finally:
        shutdown_safely(runtime)


def test_j15_get_recorder_missing_id_fails() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        with pytest.raises(MetricsRuntimeRecorderNotFoundError):
            runtime.get_recorder("missing")

    finally:
        shutdown_safely(runtime)


def test_j16_try_get_recorder_returns_registered_recorder() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        assert runtime.try_get_recorder("recorder") is recorder

    finally:
        shutdown_safely(runtime)


def test_j17_try_get_recorder_normalizes_recorder_id() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        assert runtime.try_get_recorder("  recorder  ") is recorder

    finally:
        shutdown_safely(runtime)


def test_j18_try_get_recorder_missing_id_returns_none() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        assert runtime.try_get_recorder("missing") is None

    finally:
        shutdown_safely(runtime)


def test_j19_list_recorder_ids_returns_empty_tuple_when_registry_is_empty() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        assert runtime.list_recorder_ids() == ()

    finally:
        shutdown_safely(runtime)


def test_j20_list_recorder_ids_returns_registered_ids() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        runtime.create_recorder("one")
        runtime.create_recorder("two")
        runtime.create_recorder("three")

        assert runtime.list_recorder_ids() == ("one", "two", "three")

    finally:
        shutdown_safely(runtime)


def test_j21_get_recorder_core_runs_in_runtime_loop_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()
    core_thread_id: list[int] = []

    original_get_recorder_core = MetricsRuntime._get_recorder_core

    async def tracking_get_recorder_core(
        self: MetricsRuntime,
        recorder_id: str,
    ) -> AsyncioMetricsRecorder:
        core_thread_id.append(threading.get_ident())
        # noinspection PyTypeChecker
        return await original_get_recorder_core(
            self,
            recorder_id,
        )

    monkeypatch.setattr(
        MetricsRuntime,
        "_get_recorder_core",
        tracking_get_recorder_core,
    )

    try:
        runtime.start()

        thread = get_runtime_thread(runtime)

        assert thread is not None

        runtime.create_recorder("recorder")
        runtime.get_recorder("recorder")

        assert core_thread_id == [thread.ident]

    finally:
        shutdown_safely(runtime)


def test_j22_try_get_recorder_core_runs_in_runtime_loop_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()
    core_thread_id: list[int] = []

    original_try_get_recorder_core = MetricsRuntime._try_get_recorder_core

    async def tracking_try_get_recorder_core(
        self: MetricsRuntime,
        recorder_id: str,
    ) -> AsyncioMetricsRecorder | None:
        core_thread_id.append(threading.get_ident())

        # noinspection PyTypeChecker
        return await original_try_get_recorder_core(
            self,
            recorder_id,
        )

    monkeypatch.setattr(
        MetricsRuntime,
        "_try_get_recorder_core",
        tracking_try_get_recorder_core,
    )

    try:
        runtime.start()

        thread = get_runtime_thread(runtime)

        assert thread is not None

        runtime.create_recorder("recorder")
        runtime.try_get_recorder("recorder")

        assert core_thread_id == [thread.ident]

    finally:
        shutdown_safely(runtime)


def test_j23_list_recorder_ids_core_runs_in_runtime_loop_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()
    core_thread_id: list[int] = []

    original_list_recorder_ids_core = MetricsRuntime._list_recorder_ids_core

    # noinspection PyTypeChecker
    async def tracking_list_recorder_ids_core(
        self: MetricsRuntime,
    ) -> tuple[str, ...]:
        core_thread_id.append(threading.get_ident())

        return await original_list_recorder_ids_core(self)

    monkeypatch.setattr(
        MetricsRuntime,
        "_list_recorder_ids_core",
        tracking_list_recorder_ids_core,
    )

    try:
        runtime.start()

        thread = get_runtime_thread(runtime)

        assert thread is not None

        runtime.create_recorder("recorder")
        runtime.list_recorder_ids()

        assert core_thread_id == [thread.ident]

    finally:
        shutdown_safely(runtime)


# -------------------------
# Group k: stop recorder
# -------------------------


def test_k01_stop_recorder_before_start_fails() -> None:
    runtime = make_runtime()

    with pytest.raises(MetricsRuntimeInvalidStateError):
        runtime.stop_recorder("recorder")


def test_k02_stop_recorder_after_shutdown_fails() -> None:
    runtime = make_runtime()

    runtime.shutdown()

    with pytest.raises(MetricsRuntimeInvalidStateError):
        runtime.stop_recorder("recorder")


def test_k03_stop_recorder_rejects_none_recorder_id() -> None:
    runtime = make_runtime()

    with pytest.raises(ValueError, match="recorder_id"):
        runtime.stop_recorder(cast(Any, None))


def test_k04_stop_recorder_rejects_non_string_recorder_id() -> None:
    runtime = make_runtime()

    with pytest.raises(TypeError, match="recorder_id"):
        runtime.stop_recorder(cast(Any, 123))


def test_k05_stop_recorder_rejects_empty_recorder_id() -> None:
    runtime = make_runtime()

    with pytest.raises(ValueError, match="recorder_id"):
        runtime.stop_recorder("   ")


def test_k06_stop_recorder_missing_id_fails() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        with pytest.raises(MetricsRuntimeRecorderNotFoundError):
            runtime.stop_recorder("missing")

    finally:
        shutdown_safely(runtime)


def test_k07_stop_recorder_stops_running_recorder() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        assert recorder.get_status().name == "RUNNING"

        runtime.stop_recorder("recorder")

        assert recorder.get_status().name == "STOPPED"
        assert runtime._recorders == {"recorder": recorder}

    finally:
        shutdown_safely(runtime)


def test_k08_stop_recorder_normalizes_recorder_id() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        runtime.stop_recorder("  recorder  ")

        assert recorder.get_status().name == "STOPPED"
        assert runtime._recorders == {"recorder": recorder}

    finally:
        shutdown_safely(runtime)


def test_k09_stop_recorder_stopped_recorder_is_noop() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        runtime.stop_recorder("recorder")
        runtime.stop_recorder("recorder")

        assert recorder.get_status().name == "STOPPED"
        assert runtime._recorders == {"recorder": recorder}

    finally:
        shutdown_safely(runtime)


def test_k10_stop_recorder_core_runs_in_runtime_loop_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()
    core_thread_id: list[int] = []

    original_stop_recorder_core = MetricsRuntime._stop_recorder_core

    async def tracking_stop_recorder_core(
        self: MetricsRuntime,
        recorder_id: str,
    ) -> None:
        core_thread_id.append(threading.get_ident())

        # noinspection PyTypeChecker
        await original_stop_recorder_core(
            self,
            recorder_id,
        )

    monkeypatch.setattr(
        MetricsRuntime,
        "_stop_recorder_core",
        tracking_stop_recorder_core,
    )

    try:
        runtime.start()

        thread = get_runtime_thread(runtime)

        assert thread is not None

        runtime.create_recorder("recorder")
        runtime.stop_recorder("recorder")

        assert core_thread_id == [thread.ident]

    finally:
        shutdown_safely(runtime)


def test_k11_stop_recorder_failure_is_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()

    class BrokenStopRecorder(AsyncioMetricsRecorder):
        def stop(self) -> Any:
            handle = super().stop()
            handle._future.set_exception(RuntimeError("stop failed"))
            return handle

    monkeypatch.setattr(
        runtime_pack,
        "AsyncioMetricsRecorder",
        BrokenStopRecorder,
    )

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        with pytest.raises(MetricsRuntimeRecorderStopError):
            runtime.stop_recorder("recorder")

        assert runtime._recorders == {"recorder": recorder}

    finally:
        shutdown_safely(runtime)


def test_k12_concurrent_stop_recorder_same_id_is_safe() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        results = run_many_threads(
            8,
            lambda _index: runtime.stop_recorder("recorder"),
        )

        assert results == [None] * 8
        assert recorder.get_status().name == "STOPPED"
        assert runtime._recorders == {"recorder": recorder}

    finally:
        shutdown_safely(runtime)


# -------------------------
# Group l: stop and remove recorder
# -------------------------


def test_l01_stop_and_remove_recorder_before_start_fails() -> None:
    runtime = make_runtime()

    with pytest.raises(MetricsRuntimeInvalidStateError):
        runtime.stop_and_remove_recorder("recorder")


def test_l02_stop_and_remove_recorder_after_shutdown_fails() -> None:
    runtime = make_runtime()

    runtime.shutdown()

    with pytest.raises(MetricsRuntimeInvalidStateError):
        runtime.stop_and_remove_recorder("recorder")


def test_l03_stop_and_remove_recorder_rejects_none_recorder_id() -> None:
    runtime = make_runtime()

    with pytest.raises(ValueError, match="recorder_id"):
        runtime.stop_and_remove_recorder(cast(Any, None))


def test_l04_stop_and_remove_recorder_rejects_non_string_recorder_id() -> None:
    runtime = make_runtime()

    with pytest.raises(TypeError, match="recorder_id"):
        runtime.stop_and_remove_recorder(cast(Any, 123))


def test_l05_stop_and_remove_recorder_rejects_empty_recorder_id() -> None:
    runtime = make_runtime()

    with pytest.raises(ValueError, match="recorder_id"):
        runtime.stop_and_remove_recorder("   ")


def test_l06_stop_and_remove_recorder_missing_id_fails() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        with pytest.raises(MetricsRuntimeRecorderNotFoundError):
            runtime.stop_and_remove_recorder("missing")

    finally:
        shutdown_safely(runtime)


def test_l07_stop_and_remove_recorder_stops_and_removes_running_recorder() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        removed_recorder = runtime.stop_and_remove_recorder("recorder")

        assert removed_recorder is recorder
        assert recorder.get_status().name == "STOPPED"
        assert runtime._recorders == {}
        assert runtime.try_get_recorder("recorder") is None

    finally:
        shutdown_safely(runtime)


def test_l08_stop_and_remove_recorder_normalizes_recorder_id() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        removed_recorder = runtime.stop_and_remove_recorder("  recorder  ")

        assert removed_recorder is recorder
        assert recorder.get_status().name == "STOPPED"
        assert runtime._recorders == {}

    finally:
        shutdown_safely(runtime)


def test_l09_stop_and_remove_recorder_removes_stopped_recorder() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        runtime.stop_recorder("recorder")

        assert recorder.get_status().name == "STOPPED"

        removed_recorder = runtime.stop_and_remove_recorder("recorder")

        assert removed_recorder is recorder
        assert recorder.get_status().name == "STOPPED"
        assert runtime._recorders == {}

    finally:
        shutdown_safely(runtime)


def test_l10_stop_and_remove_recorder_core_runs_in_runtime_loop_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()
    core_thread_id: list[int] = []

    original_stop_and_remove_recorder_core = MetricsRuntime._stop_and_remove_recorder_core

    async def tracking_stop_and_remove_recorder_core(
        self: MetricsRuntime,
        recorder_id: str,
    ) -> AsyncioMetricsRecorder:
        core_thread_id.append(threading.get_ident())
        # noinspection PyTypeChecker
        return await original_stop_and_remove_recorder_core(
            self,
            recorder_id,
        )

    monkeypatch.setattr(
        MetricsRuntime,
        "_stop_and_remove_recorder_core",
        tracking_stop_and_remove_recorder_core,
    )

    try:
        runtime.start()

        thread = get_runtime_thread(runtime)

        assert thread is not None

        runtime.create_recorder("recorder")
        runtime.stop_and_remove_recorder("recorder")

        assert core_thread_id == [thread.ident]

    finally:
        shutdown_safely(runtime)


def test_l11_stop_and_remove_recorder_removes_even_when_stop_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()

    class BrokenStopRecorder(AsyncioMetricsRecorder):
        def stop(self) -> Any:
            handle = super().stop()
            handle._future.set_exception(RuntimeError("stop failed"))
            return handle

    monkeypatch.setattr(
        runtime_pack,
        "AsyncioMetricsRecorder",
        BrokenStopRecorder,
    )

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        with pytest.raises(MetricsRuntimeRecorderStopError):
            runtime.stop_and_remove_recorder("recorder")

        assert runtime._recorders == {}
        assert runtime.try_get_recorder("recorder") is None
        assert recorder.get_status().name != "RUNNING"

    finally:
        shutdown_safely(runtime)


def test_l12_concurrent_stop_and_remove_recorder_same_id_allows_only_one() -> None:
    runtime = make_runtime()

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        def remove_or_return_error(_index: int) -> object:
            try:
                return runtime.stop_and_remove_recorder("recorder")
            except MetricsRuntimeRecorderNotFoundError as exc:
                return exc

        results = run_many_threads(
            8,
            remove_or_return_error,
        )

        removed = [result for result in results if isinstance(result, AsyncioMetricsRecorder)]
        errors = [
            result for result in results if isinstance(result, MetricsRuntimeRecorderNotFoundError)
        ]

        assert removed == [recorder]
        assert len(errors) == 7
        assert runtime._recorders == {}
        assert recorder.get_status().name == "STOPPED"

    finally:
        shutdown_safely(runtime)


def test_l13_stop_then_stop_and_remove_same_recorder_is_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()
    stop_started = threading.Event()
    release_stop = threading.Event()

    class SlowStoppingRecorder(AsyncioMetricsRecorder):
        async def _on_stopped(self) -> None:
            stop_started.set()

            while not release_stop.is_set():
                await asyncio.sleep(0.005)

    monkeypatch.setattr(
        runtime_pack,
        "AsyncioMetricsRecorder",
        SlowStoppingRecorder,
    )

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        stop_result: list[object] = []
        remove_result: list[object] = []

        def stop_worker() -> None:
            try:
                runtime.stop_recorder("recorder")
                stop_result.append(None)
            except BaseException as exc:
                stop_result.append(exc)

        def remove_worker() -> None:
            try:
                remove_result.append(runtime.stop_and_remove_recorder("recorder"))
            except BaseException as exc:
                remove_result.append(exc)

        stop_thread = threading.Thread(target=stop_worker)
        stop_thread.start()

        wait_thread_event(stop_started)

        remove_thread = threading.Thread(target=remove_worker)
        remove_thread.start()

        release_stop.set()

        stop_thread.join(timeout=TIMEOUT)
        remove_thread.join(timeout=TIMEOUT)

        assert not stop_thread.is_alive()
        assert not remove_thread.is_alive()

        assert stop_result == [None]
        assert remove_result == [recorder]
        assert runtime._recorders == {}
        assert runtime._removing_recorders == set()
        assert recorder.get_status().name == "STOPPED"

    finally:
        release_stop.set()
        shutdown_safely(runtime)


def test_l14_stop_and_remove_then_stop_same_recorder_reports_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()
    stop_started = threading.Event()
    release_stop = threading.Event()

    class SlowStoppingRecorder(AsyncioMetricsRecorder):
        async def _on_stopped(self) -> None:
            stop_started.set()

            while not release_stop.is_set():
                await asyncio.sleep(0.005)

    monkeypatch.setattr(
        runtime_pack,
        "AsyncioMetricsRecorder",
        SlowStoppingRecorder,
    )

    try:
        runtime.start()

        recorder = runtime.create_recorder("recorder")

        remove_result: list[object] = []
        stop_result: list[object] = []

        def remove_worker() -> None:
            try:
                remove_result.append(runtime.stop_and_remove_recorder("recorder"))
            except BaseException as exc:
                remove_result.append(exc)

        def stop_worker() -> None:
            try:
                runtime.stop_recorder("recorder")
                stop_result.append(None)
            except BaseException as exc:
                stop_result.append(exc)

        remove_thread = threading.Thread(target=remove_worker)
        remove_thread.start()

        wait_thread_event(stop_started)

        stop_thread = threading.Thread(target=stop_worker)
        stop_thread.start()
        stop_thread.join(timeout=TIMEOUT)

        release_stop.set()
        remove_thread.join(timeout=TIMEOUT)

        assert not stop_thread.is_alive()
        assert not remove_thread.is_alive()

        assert remove_result == [recorder]
        assert len(stop_result) == 1
        assert isinstance(stop_result[0], MetricsRuntimeRecorderNotFoundError)
        assert runtime._recorders == {}
        assert runtime._removing_recorders == set()
        assert recorder.get_status().name == "STOPPED"

    finally:
        release_stop.set()
        shutdown_safely(runtime)


# -------------------------
# Group m: shutdown registered recorders
# -------------------------


def test_m01_shutdown_stops_registered_running_recorder() -> None:
    runtime = make_runtime()

    runtime.start()

    recorder = runtime.create_recorder("recorder")

    assert recorder.get_status().name == "RUNNING"

    runtime.shutdown()

    assert runtime.get_status() is MetricsRuntimeState.CLOSED
    assert recorder.get_status().name == "STOPPED"
    assert runtime._recorders == {}
    assert runtime._creating_recorders == set()
    assert runtime._removing_recorders == set()


def test_m02_shutdown_stops_multiple_registered_recorders() -> None:
    runtime = make_runtime()

    runtime.start()

    recorder_one = runtime.create_recorder("one")
    recorder_two = runtime.create_recorder("two")
    recorder_three = runtime.create_recorder("three")

    runtime.shutdown()

    assert runtime.get_status() is MetricsRuntimeState.CLOSED
    assert recorder_one.get_status().name == "STOPPED"
    assert recorder_two.get_status().name == "STOPPED"
    assert recorder_three.get_status().name == "STOPPED"
    assert runtime._recorders == {}
    assert runtime._creating_recorders == set()
    assert runtime._removing_recorders == set()


def test_m03_shutdown_handles_already_stopped_recorder() -> None:
    runtime = make_runtime()

    runtime.start()

    recorder = runtime.create_recorder("recorder")
    runtime.stop_recorder("recorder")

    assert recorder.get_status().name == "STOPPED"

    runtime.shutdown()

    assert runtime.get_status() is MetricsRuntimeState.CLOSED
    assert recorder.get_status().name == "STOPPED"
    assert runtime._recorders == {}
    assert runtime._creating_recorders == set()
    assert runtime._removing_recorders == set()


def test_m04_shutdown_clears_recorder_reservations() -> None:
    runtime = make_runtime()

    runtime.start()

    runtime._creating_recorders.add("creating")
    runtime._removing_recorders.add("removing")

    runtime.shutdown()

    assert runtime.get_status() is MetricsRuntimeState.CLOSED
    assert runtime._recorders == {}
    assert runtime._creating_recorders == set()
    assert runtime._removing_recorders == set()


def test_m05_shutdown_loop_core_runs_in_runtime_loop_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()
    core_thread_id: list[int] = []

    original_shutdown_loop_core = MetricsRuntime._shutdown_loop_core

    async def tracking_shutdown_loop_core(self: MetricsRuntime) -> None:
        core_thread_id.append(threading.get_ident())

        # noinspection PyTypeChecker
        await original_shutdown_loop_core(self)

    monkeypatch.setattr(
        MetricsRuntime,
        "_shutdown_loop_core",
        tracking_shutdown_loop_core,
    )

    runtime.start()

    thread = get_runtime_thread(runtime)

    assert thread is not None

    runtime.create_recorder("recorder")
    runtime.shutdown()

    assert core_thread_id == [thread.ident]
    assert runtime.get_status() is MetricsRuntimeState.CLOSED


def test_m06_shutdown_continues_after_one_recorder_stop_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()

    class BrokenStopRecorder(AsyncioMetricsRecorder):
        async def _on_stopped(self) -> None:
            if self.identity == "broken":
                raise RuntimeError("stop failed")

    monkeypatch.setattr(
        runtime_pack,
        "AsyncioMetricsRecorder",
        BrokenStopRecorder,
    )

    runtime.start()

    broken_recorder = runtime.create_recorder("broken")
    healthy_recorder = runtime.create_recorder("healthy")

    with pytest.raises(MetricsRuntimeShutdownError):
        runtime.shutdown()

    assert runtime.get_status() is MetricsRuntimeState.FAILURE
    assert healthy_recorder.get_status().name == "STOPPED"
    assert broken_recorder.get_status().name == "FAILURE"
    assert runtime._recorders == {}
    assert runtime._creating_recorders == set()
    assert runtime._removing_recorders == set()


def test_m07_shutdown_error_contains_failed_recorder_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()

    class BrokenStopRecorder(AsyncioMetricsRecorder):
        async def _on_stopped(self) -> None:
            raise RuntimeError("stop failed")

    monkeypatch.setattr(
        runtime_pack,
        "AsyncioMetricsRecorder",
        BrokenStopRecorder,
    )

    runtime.start()
    runtime.create_recorder("broken")

    with pytest.raises(MetricsRuntimeShutdownError) as exc_info:
        runtime.shutdown()

    assert exc_info.value.details["failed_recorder_ids"] == ("broken",)
    assert "broken" in exc_info.value.details["recorder_errors"]
    assert (
        exc_info.value.details["recorder_errors"]["broken"]["error_type"]
        == "MetricsRuntimeRecorderStopError"
    )
    assert runtime.get_status() is MetricsRuntimeState.FAILURE
    assert runtime._recorders == {}
    assert runtime._creating_recorders == set()
    assert runtime._removing_recorders == set()


def test_m08_shutdown_removes_recorder_even_when_stop_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = make_runtime()

    class BrokenStopRecorder(AsyncioMetricsRecorder):
        async def _on_stopped(self) -> None:
            raise RuntimeError("stop failed")

    monkeypatch.setattr(
        runtime_pack,
        "AsyncioMetricsRecorder",
        BrokenStopRecorder,
    )

    runtime.start()

    recorder = runtime.create_recorder("broken")

    with pytest.raises(MetricsRuntimeShutdownError):
        runtime.shutdown()

    assert runtime.get_status() is MetricsRuntimeState.FAILURE
    assert recorder.get_status().name == "FAILURE"
    assert runtime._recorders == {}
    assert runtime._creating_recorders == set()
    assert runtime._removing_recorders == set()


# -------------------------
# Group n: logging integration
# -------------------------


def test_n01_start_success_logs_invoke_and_success(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    runtime = MetricsRuntime(
        namespace="runtime-log-1",
        log_context=memory_log_context,
    )

    try:
        runtime.start()

        assert _log_pairs(memory_log_sink) == [
            ("metrics_runtime.start", "invoke"),
            ("metrics_runtime.start", "success"),
        ]

        invoke = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.start",
            outcome="invoke",
        )
        success = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.start",
            outcome="success",
        )

        assert invoke.meta.entity_id == "runtime-log-1"
        assert success.meta.entity_id == "runtime-log-1"

        assert invoke.payload["state"] == MetricsRuntimeState.VIRGIN.value
        assert success.payload["state"] == MetricsRuntimeState.RUNNING.value

    finally:
        shutdown_safely(runtime)


def test_n02_start_without_log_context_emits_no_log_events(
    memory_log_sink: _MemoryLogSink,
) -> None:
    runtime = make_runtime(namespace="runtime-log-2")

    try:
        runtime.start()

        assert memory_log_sink.events == []

    finally:
        shutdown_safely(runtime)


def test_n03_start_failed_logs_failed_event(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    runtime = MetricsRuntime(
        namespace="runtime-log-3",
        log_context=memory_log_context,
    )

    runtime.shutdown()
    memory_log_sink.events.clear()

    with pytest.raises(MetricsRuntimeInvalidStateError):
        runtime.start()

    assert _log_pairs(memory_log_sink) == [
        ("metrics_runtime.start", "invoke"),
        ("metrics_runtime.start", "failed"),
    ]

    failed = _single_log_event(
        memory_log_sink,
        event_name="metrics_runtime.start",
        outcome="failed",
    )

    assert failed.meta.entity_id == "runtime-log-3"
    assert failed.payload["state"] == MetricsRuntimeState.CLOSED.value
    assert "error" in failed.payload


def test_n04_shutdown_success_logs_invoke_and_success(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    runtime = MetricsRuntime(
        namespace="runtime-log-4",
        log_context=memory_log_context,
    )

    runtime.start()
    memory_log_sink.events.clear()

    runtime.shutdown()

    assert _log_pairs(memory_log_sink) == [
        ("metrics_runtime.shutdown", "invoke"),
        ("metrics_runtime.shutdown", "success"),
    ]

    invoke = _single_log_event(
        memory_log_sink,
        event_name="metrics_runtime.shutdown",
        outcome="invoke",
    )
    success = _single_log_event(
        memory_log_sink,
        event_name="metrics_runtime.shutdown",
        outcome="success",
    )

    assert invoke.meta.entity_id == "runtime-log-4"
    assert success.meta.entity_id == "runtime-log-4"

    assert invoke.payload["state"] == MetricsRuntimeState.RUNNING.value
    assert success.payload["state"] == MetricsRuntimeState.CLOSED.value


def test_n05_create_recorder_success_logs_runtime_and_nested_recorder_start(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    runtime = MetricsRuntime(
        namespace="runtime-log-5",
        log_context=memory_log_context,
    )

    try:
        runtime.start()
        memory_log_sink.events.clear()

        recorder = runtime.create_recorder(
            "recorder",
            entity_id="recorder-entity",
            namespace="recorder.namespace",
            queue_max_size=3,
            queue_overflow_policy=AsyncioMetricsRecorderQueueOverflowPolicy.RAISE_ERROR,
        )

        assert recorder.identity == "recorder-entity"

        assert _log_pairs(memory_log_sink) == [
            ("metrics_runtime.create_recorder", "invoke"),
            ("asyncio_metrics_recorder.start", "invoke"),
            ("asyncio_metrics_recorder.start", "success"),
            ("metrics_runtime.create_recorder", "success"),
        ]

        invoke = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.create_recorder",
            outcome="invoke",
        )
        success = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.create_recorder",
            outcome="success",
        )

        assert invoke.meta.entity_id == "runtime-log-5"
        assert success.meta.entity_id == "runtime-log-5"

        assert invoke.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert invoke.payload["kwargs"]["recorder_id"] == "recorder"
        assert invoke.payload["kwargs"]["entity_id"] == "recorder-entity"
        assert invoke.payload["kwargs"]["namespace"] == "recorder.namespace"
        assert invoke.payload["kwargs"]["queue_max_size"] == 3
        assert (
            invoke.payload["kwargs"]["queue_overflow_policy"]
            == AsyncioMetricsRecorderQueueOverflowPolicy.RAISE_ERROR.value
        )

        assert success.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert "kwargs" not in success.payload
        assert "result" not in success.payload

        recorder_start = _single_log_event(
            memory_log_sink,
            event_name="asyncio_metrics_recorder.start",
            outcome="invoke",
        )

        assert recorder_start.meta.entity_id == "recorder-entity"

    finally:
        shutdown_safely(runtime)


def test_n06_create_recorder_failed_logs_failed_event(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    runtime = MetricsRuntime(
        namespace="runtime-log-6",
        log_context=memory_log_context,
    )

    try:
        runtime.start()
        runtime.create_recorder("recorder")
        memory_log_sink.events.clear()

        with pytest.raises(MetricsRuntimeRecorderAlreadyExistsError):
            runtime.create_recorder("recorder")

        assert _log_pairs(memory_log_sink) == [
            ("metrics_runtime.create_recorder", "invoke"),
            ("metrics_runtime.create_recorder", "failed"),
        ]

        invoke = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.create_recorder",
            outcome="invoke",
        )
        failed = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.create_recorder",
            outcome="failed",
        )

        assert invoke.meta.entity_id == "runtime-log-6"
        assert failed.meta.entity_id == "runtime-log-6"

        assert invoke.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert invoke.payload["kwargs"]["recorder_id"] == "recorder"

        assert failed.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert "kwargs" not in failed.payload
        assert "error" in failed.payload

    finally:
        shutdown_safely(runtime)


def test_n07_get_recorder_success_logs_invoke_and_success(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    runtime = MetricsRuntime(
        namespace="runtime-log-7",
        log_context=memory_log_context,
    )

    try:
        runtime.start()
        recorder = runtime.create_recorder("recorder")
        memory_log_sink.events.clear()

        result = runtime.get_recorder("recorder")

        assert result is recorder

        assert _log_pairs(memory_log_sink) == [
            ("metrics_runtime.get_recorder", "invoke"),
            ("metrics_runtime.get_recorder", "success"),
        ]

        invoke = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.get_recorder",
            outcome="invoke",
        )
        success = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.get_recorder",
            outcome="success",
        )

        assert invoke.meta.entity_id == "runtime-log-7"
        assert success.meta.entity_id == "runtime-log-7"

        assert invoke.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert invoke.payload["kwargs"]["recorder_id"] == "recorder"
        assert success.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert "result" not in success.payload

    finally:
        shutdown_safely(runtime)


def test_n08_get_recorder_failed_logs_failed_event(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    runtime = MetricsRuntime(
        namespace="runtime-log-8",
        log_context=memory_log_context,
    )

    try:
        runtime.start()
        memory_log_sink.events.clear()

        with pytest.raises(MetricsRuntimeRecorderNotFoundError):
            runtime.get_recorder("missing")

        assert _log_pairs(memory_log_sink) == [
            ("metrics_runtime.get_recorder", "invoke"),
            ("metrics_runtime.get_recorder", "failed"),
        ]

        invoke = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.get_recorder",
            outcome="invoke",
        )
        failed = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.get_recorder",
            outcome="failed",
        )

        assert invoke.meta.entity_id == "runtime-log-8"
        assert failed.meta.entity_id == "runtime-log-8"

        assert invoke.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert invoke.payload["kwargs"]["recorder_id"] == "missing"

        assert failed.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert "kwargs" not in failed.payload
        assert "error" in failed.payload

    finally:
        shutdown_safely(runtime)


def test_n09_try_get_recorder_success_logs_invoke_and_success(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    runtime = MetricsRuntime(
        namespace="runtime-log-9",
        log_context=memory_log_context,
    )

    try:
        runtime.start()
        recorder = runtime.create_recorder("recorder")
        memory_log_sink.events.clear()

        result = runtime.try_get_recorder("recorder")

        assert result is recorder

        assert _log_pairs(memory_log_sink) == [
            ("metrics_runtime.try_get_recorder", "invoke"),
            ("metrics_runtime.try_get_recorder", "success"),
        ]

        invoke = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.try_get_recorder",
            outcome="invoke",
        )
        success = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.try_get_recorder",
            outcome="success",
        )

        assert invoke.meta.entity_id == "runtime-log-9"
        assert success.meta.entity_id == "runtime-log-9"

        assert invoke.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert invoke.payload["kwargs"]["recorder_id"] == "recorder"
        assert success.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert "result" not in success.payload

    finally:
        shutdown_safely(runtime)


def test_n10_try_get_recorder_missing_logs_success_not_failed(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    runtime = MetricsRuntime(
        namespace="runtime-log-10",
        log_context=memory_log_context,
    )

    try:
        runtime.start()
        memory_log_sink.events.clear()

        result = runtime.try_get_recorder("missing")

        assert result is None

        assert _log_pairs(memory_log_sink) == [
            ("metrics_runtime.try_get_recorder", "invoke"),
            ("metrics_runtime.try_get_recorder", "success"),
        ]

        invoke = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.try_get_recorder",
            outcome="invoke",
        )

        assert invoke.meta.entity_id == "runtime-log-10"
        assert invoke.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert invoke.payload["kwargs"]["recorder_id"] == "missing"

    finally:
        shutdown_safely(runtime)


def test_n11_list_recorder_ids_success_logs_invoke_and_success(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    runtime = MetricsRuntime(
        namespace="runtime-log-11",
        log_context=memory_log_context,
    )

    try:
        runtime.start()
        runtime.create_recorder("one")
        runtime.create_recorder("two")
        memory_log_sink.events.clear()

        result = runtime.list_recorder_ids()

        assert result == ("one", "two")

        assert _log_pairs(memory_log_sink) == [
            ("metrics_runtime.list_recorder_ids", "invoke"),
            ("metrics_runtime.list_recorder_ids", "success"),
        ]

        invoke = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.list_recorder_ids",
            outcome="invoke",
        )
        success = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.list_recorder_ids",
            outcome="success",
        )

        assert invoke.meta.entity_id == "runtime-log-11"
        assert success.meta.entity_id == "runtime-log-11"

        assert invoke.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert success.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert "result" not in success.payload

    finally:
        shutdown_safely(runtime)


def test_n12_stop_recorder_success_logs_runtime_and_nested_recorder_stop(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    runtime = MetricsRuntime(
        namespace="runtime-log-12",
        log_context=memory_log_context,
    )

    try:
        runtime.start()
        recorder = runtime.create_recorder("recorder")
        memory_log_sink.events.clear()

        runtime.stop_recorder("recorder")

        assert recorder.get_status().name == "STOPPED"

        assert _log_pairs(memory_log_sink) == [
            ("metrics_runtime.stop_recorder", "invoke"),
            ("asyncio_metrics_recorder.stop", "invoke"),
            ("asyncio_metrics_recorder.stop", "success"),
            ("metrics_runtime.stop_recorder", "success"),
        ]

        invoke = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.stop_recorder",
            outcome="invoke",
        )
        success = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.stop_recorder",
            outcome="success",
        )

        assert invoke.meta.entity_id == "runtime-log-12"
        assert success.meta.entity_id == "runtime-log-12"

        assert invoke.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert invoke.payload["kwargs"]["recorder_id"] == "recorder"
        assert success.payload["state"] == MetricsRuntimeState.RUNNING.value

        nested_stop = _single_log_event(
            memory_log_sink,
            event_name="asyncio_metrics_recorder.stop",
            outcome="success",
        )

        assert nested_stop.meta.entity_id == "recorder"

    finally:
        shutdown_safely(runtime)


def test_n13_stop_recorder_failed_logs_failed_event(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    runtime = MetricsRuntime(
        namespace="runtime-log-13",
        log_context=memory_log_context,
    )

    try:
        runtime.start()
        memory_log_sink.events.clear()

        with pytest.raises(MetricsRuntimeRecorderNotFoundError):
            runtime.stop_recorder("missing")

        assert _log_pairs(memory_log_sink) == [
            ("metrics_runtime.stop_recorder", "invoke"),
            ("metrics_runtime.stop_recorder", "failed"),
        ]

        invoke = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.stop_recorder",
            outcome="invoke",
        )
        failed = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.stop_recorder",
            outcome="failed",
        )

        assert invoke.meta.entity_id == "runtime-log-13"
        assert failed.meta.entity_id == "runtime-log-13"

        assert invoke.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert invoke.payload["kwargs"]["recorder_id"] == "missing"

        assert failed.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert "kwargs" not in failed.payload
        assert "error" in failed.payload

    finally:
        shutdown_safely(runtime)


def test_n14_stop_and_remove_recorder_success_logs_runtime_and_nested_recorder_stop(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    runtime = MetricsRuntime(
        namespace="runtime-log-14",
        log_context=memory_log_context,
    )

    try:
        runtime.start()
        recorder = runtime.create_recorder("recorder")
        memory_log_sink.events.clear()

        removed = runtime.stop_and_remove_recorder("recorder")

        assert removed is recorder
        assert recorder.get_status().name == "STOPPED"

        assert _log_pairs(memory_log_sink) == [
            ("metrics_runtime.stop_and_remove_recorder", "invoke"),
            ("asyncio_metrics_recorder.stop", "invoke"),
            ("asyncio_metrics_recorder.stop", "success"),
            ("metrics_runtime.stop_and_remove_recorder", "success"),
        ]

        invoke = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.stop_and_remove_recorder",
            outcome="invoke",
        )
        success = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.stop_and_remove_recorder",
            outcome="success",
        )

        assert invoke.meta.entity_id == "runtime-log-14"
        assert success.meta.entity_id == "runtime-log-14"

        assert invoke.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert invoke.payload["kwargs"]["recorder_id"] == "recorder"
        assert success.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert "result" not in success.payload

    finally:
        shutdown_safely(runtime)


def test_n15_stop_and_remove_recorder_failed_logs_failed_event(
    memory_log_context: LogContext,
    memory_log_sink: _MemoryLogSink,
) -> None:
    runtime = MetricsRuntime(
        namespace="runtime-log-15",
        log_context=memory_log_context,
    )

    try:
        runtime.start()
        memory_log_sink.events.clear()

        with pytest.raises(MetricsRuntimeRecorderNotFoundError):
            runtime.stop_and_remove_recorder("missing")

        assert _log_pairs(memory_log_sink) == [
            ("metrics_runtime.stop_and_remove_recorder", "invoke"),
            ("metrics_runtime.stop_and_remove_recorder", "failed"),
        ]

        invoke = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.stop_and_remove_recorder",
            outcome="invoke",
        )
        failed = _single_log_event(
            memory_log_sink,
            event_name="metrics_runtime.stop_and_remove_recorder",
            outcome="failed",
        )

        assert invoke.meta.entity_id == "runtime-log-15"
        assert failed.meta.entity_id == "runtime-log-15"

        assert invoke.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert invoke.payload["kwargs"]["recorder_id"] == "missing"

        assert failed.payload["state"] == MetricsRuntimeState.RUNNING.value
        assert "kwargs" not in failed.payload
        assert "error" in failed.payload

    finally:
        shutdown_safely(runtime)
