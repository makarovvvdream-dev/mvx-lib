# common/tests/test_logger/core/test_log_sink_registry.py

from __future__ import annotations

from collections.abc import Callable
from typing import Any
import threading
import time

import pytest

from mvx.common.logger.core.log_sink_registry import (
    LogSinkRegistry,
    LogSinkRegistryState,
)
from mvx.common.logger.core.models import (
    LogEvent,
    LogSinkDescriptor,
    LogSinkProto,
)
from mvx.common.logger.errors import (
    LogSinkRegistryError,
    LogSinkRegistryErrorReason,
)


class _FakeSink:
    _class_lock = threading.RLock()

    create_call_count: int = 0
    terminator_call_count: int = 0

    create_thread_ids: list[int] = []
    terminator_thread_ids: list[int] = []

    created_resources: list[str] = []
    terminated_resources: list[str] = []

    create_delay_s: float = 0.0
    create_started_event: threading.Event | None = None
    create_continue_event: threading.Event | None = None

    terminator_started_event: threading.Event | None = None
    terminator_continue_event: threading.Event | None = None

    should_fail_create: bool = False

    def __init__(self, *, resource: str, config: str = "default") -> None:
        self.resource = resource
        self.config = config
        self.events: list[LogEvent] = []

    @classmethod
    def reset(cls) -> None:
        with cls._class_lock:
            cls.create_call_count = 0
            cls.terminator_call_count = 0

            cls.create_thread_ids = []
            cls.terminator_thread_ids = []

            cls.created_resources = []
            cls.terminated_resources = []

            cls.create_delay_s = 0.0
            cls.create_started_event = None
            cls.create_continue_event = None

            cls.terminator_started_event = None
            cls.terminator_continue_event = None

            cls.should_fail_create = False

    @classmethod
    def build_descriptor(cls, **kwargs: Any) -> LogSinkDescriptor:
        return LogSinkDescriptor(
            sink_type="fake",
            resource_key=("fake-resource", kwargs["resource"]),
            config_key=("fake-config", kwargs.get("config", "default")),
        )

    @classmethod
    def create(cls, **kwargs: Any) -> tuple[LogSinkProto, Callable[[], None]]:
        with cls._class_lock:
            cls.create_call_count += 1
            cls.create_thread_ids.append(threading.get_ident())
            cls.created_resources.append(kwargs["resource"])

        if cls.create_started_event is not None:
            cls.create_started_event.set()

        if cls.create_continue_event is not None:
            if not cls.create_continue_event.wait(timeout=2.0):
                raise TimeoutError("create_continue_event was not set")

        if cls.create_delay_s > 0:
            time.sleep(cls.create_delay_s)

        if cls.should_fail_create:
            raise RuntimeError("create failed intentionally")

        sink = cls(
            resource=kwargs["resource"],
            config=kwargs.get("config", "default"),
        )

        def terminator() -> None:
            with cls._class_lock:
                cls.terminator_call_count += 1
                cls.terminator_thread_ids.append(threading.get_ident())
                cls.terminated_resources.append(sink.resource)

            if cls.terminator_started_event is not None:
                cls.terminator_started_event.set()

            if cls.terminator_continue_event is not None:
                if not cls.terminator_continue_event.wait(timeout=2.0):
                    raise TimeoutError("terminator_continue_event was not set")

        return sink, terminator

    def log(self, event: LogEvent) -> None:
        self.events.append(event)


def _assert_registry_error_reason(
    exc_info: pytest.ExceptionInfo[LogSinkRegistryError],
    reason: LogSinkRegistryErrorReason,
) -> None:
    assert exc_info.value.reason_code == reason.value


def test_register_creates_sink_and_get_returns_same_instance() -> None:
    _FakeSink.reset()
    registry = LogSinkRegistry()

    sink = registry.register(
        name="root",
        sink_cls=_FakeSink,
        resource="main",
        config="default",
    )

    assert registry.get("root") is sink
    assert isinstance(sink, _FakeSink)

    assert _FakeSink.create_call_count == 1
    assert _FakeSink.created_resources == ["main"]
    assert _FakeSink.terminator_call_count == 0


def test_register_same_name_with_same_descriptor_returns_existing_sink() -> None:
    _FakeSink.reset()
    registry = LogSinkRegistry()

    first = registry.register(
        name="root",
        sink_cls=_FakeSink,
        resource="main",
        config="default",
    )

    second = registry.register(
        name="root",
        sink_cls=_FakeSink,
        resource="main",
        config="default",
    )

    assert second is first
    assert _FakeSink.create_call_count == 1
    assert _FakeSink.terminator_call_count == 0


def test_register_same_name_with_different_descriptor_raises() -> None:
    _FakeSink.reset()
    registry = LogSinkRegistry()

    registry.register(
        name="root",
        sink_cls=_FakeSink,
        resource="main",
        config="default",
    )

    with pytest.raises(LogSinkRegistryError) as exc_info:
        registry.register(
            name="root",
            sink_cls=_FakeSink,
            resource="other",
            config="default",
        )

    _assert_registry_error_reason(
        exc_info,
        LogSinkRegistryErrorReason.LOG_SINK_ALREADY_REGISTERED_WITH_DIFFERENT_DESCRIPTOR,
    )

    assert _FakeSink.create_call_count == 1
    assert _FakeSink.created_resources == ["main"]


def test_get_unknown_sink_raises() -> None:
    _FakeSink.reset()
    registry = LogSinkRegistry()

    with pytest.raises(LogSinkRegistryError) as exc_info:
        registry.get("missing")

    _assert_registry_error_reason(
        exc_info,
        LogSinkRegistryErrorReason.LOG_SINK_NOT_FOUND,
    )


def test_register_create_failure_is_mapped_to_registry_error() -> None:
    _FakeSink.reset()
    _FakeSink.should_fail_create = True

    registry = LogSinkRegistry()

    with pytest.raises(LogSinkRegistryError) as exc_info:
        registry.register(
            name="root",
            sink_cls=_FakeSink,
            resource="main",
        )

    _assert_registry_error_reason(
        exc_info,
        LogSinkRegistryErrorReason.LOG_SINK_CREATE_FAILED,
    )

    assert _FakeSink.create_call_count == 1
    assert _FakeSink.terminator_call_count == 0


def test_shutdown_calls_terminators_in_reverse_registration_order() -> None:
    _FakeSink.reset()
    registry = LogSinkRegistry()

    registry.register(name="a", sink_cls=_FakeSink, resource="a")
    registry.register(name="b", sink_cls=_FakeSink, resource="b")
    registry.register(name="c", sink_cls=_FakeSink, resource="c")

    registry.shutdown()

    assert registry.get_state() is LogSinkRegistryState.SHUT_DOWN

    assert _FakeSink.create_call_count == 3
    assert _FakeSink.terminator_call_count == 3
    assert _FakeSink.terminated_resources == ["c", "b", "a"]


def test_shutdown_is_idempotent() -> None:
    _FakeSink.reset()
    registry = LogSinkRegistry()

    registry.register(name="root", sink_cls=_FakeSink, resource="main")

    registry.shutdown()
    registry.shutdown()

    assert registry.get_state() is LogSinkRegistryState.SHUT_DOWN
    assert _FakeSink.terminator_call_count == 1
    assert _FakeSink.terminated_resources == ["main"]


def test_register_after_shutdown_raises_registry_is_not_active() -> None:
    _FakeSink.reset()
    registry = LogSinkRegistry()

    registry.shutdown()

    with pytest.raises(LogSinkRegistryError) as exc_info:
        registry.register(
            name="root",
            sink_cls=_FakeSink,
            resource="main",
        )

    _assert_registry_error_reason(
        exc_info,
        LogSinkRegistryErrorReason.LOG_SINK_REGISTRY_IS_NOT_ACTIVE,
    )

    assert _FakeSink.create_call_count == 0


def test_get_after_shutdown_raises_registry_is_not_active() -> None:
    _FakeSink.reset()
    registry = LogSinkRegistry()

    registry.register(name="root", sink_cls=_FakeSink, resource="main")
    registry.shutdown()

    with pytest.raises(LogSinkRegistryError) as exc_info:
        registry.get("root")

    _assert_registry_error_reason(
        exc_info,
        LogSinkRegistryErrorReason.LOG_SINK_REGISTRY_IS_NOT_ACTIVE,
    )


def test_concurrent_register_same_name_creates_sink_only_once() -> None:
    _FakeSink.reset()
    _FakeSink.create_delay_s = 0.1

    registry = LogSinkRegistry()

    workers_count = 12
    start_barrier = threading.Barrier(workers_count)

    worker_thread_ids: list[int] = []
    result_holder: list[LogSinkProto] = []
    error_holder: list[BaseException] = []

    result_lock = threading.Lock()

    def worker() -> None:
        try:
            thread_id = threading.get_ident()

            with result_lock:
                worker_thread_ids.append(thread_id)

            start_barrier.wait(timeout=2.0)

            sink = registry.register(
                name="root",
                sink_cls=_FakeSink,
                resource="main",
                config="default",
            )

            with result_lock:
                result_holder.append(sink)

        except BaseException as exc:
            with result_lock:
                error_holder.append(exc)

    threads = [
        threading.Thread(
            target=worker,
            name=f"register-worker-{index}",
        )
        for index in range(workers_count)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join(timeout=3.0)

    assert all(not thread.is_alive() for thread in threads)

    assert error_holder == []
    assert len(result_holder) == workers_count

    assert len(worker_thread_ids) == workers_count
    assert len(set(worker_thread_ids)) == workers_count

    assert _FakeSink.create_call_count == 1
    assert len(_FakeSink.create_thread_ids) == 1
    assert _FakeSink.create_thread_ids[0] in set(worker_thread_ids)

    assert len({id(sink) for sink in result_holder}) == 1
    assert registry.get("root") is result_holder[0]

    assert _FakeSink.terminator_call_count == 0


def test_shutdown_waits_for_in_flight_register_create_before_terminating() -> None:
    _FakeSink.reset()

    create_started = threading.Event()
    create_continue = threading.Event()

    _FakeSink.create_started_event = create_started
    _FakeSink.create_continue_event = create_continue

    registry = LogSinkRegistry()

    register_started = threading.Event()
    register_finished = threading.Event()

    shutdown_started = threading.Event()
    shutdown_finished = threading.Event()

    registered_sink_holder: list[LogSinkProto] = []
    error_holder: list[BaseException] = []

    def register_target() -> None:
        try:
            register_started.set()

            sink = registry.register(
                name="root",
                sink_cls=_FakeSink,
                resource="main",
            )

            registered_sink_holder.append(sink)

        except BaseException as exc:
            error_holder.append(exc)

        finally:
            register_finished.set()

    def shutdown_target() -> None:
        try:
            shutdown_started.set()
            registry.shutdown()
        except BaseException as exc:
            error_holder.append(exc)
        finally:
            shutdown_finished.set()

    register_thread = threading.Thread(
        target=register_target,
        name="registry-register-thread",
    )

    shutdown_thread = threading.Thread(
        target=shutdown_target,
        name="registry-shutdown-thread",
    )

    register_thread.start()

    assert register_started.wait(timeout=1.0)
    assert create_started.wait(timeout=1.0)

    shutdown_thread.start()

    assert shutdown_started.wait(timeout=1.0)

    time.sleep(0.05)

    assert not register_finished.is_set()
    assert not shutdown_finished.is_set()

    create_continue.set()

    register_thread.join(timeout=2.0)
    shutdown_thread.join(timeout=2.0)

    assert not register_thread.is_alive()
    assert not shutdown_thread.is_alive()

    assert error_holder == []
    assert register_finished.is_set()
    assert shutdown_finished.is_set()

    assert len(registered_sink_holder) == 1

    assert _FakeSink.create_call_count == 1
    assert _FakeSink.terminator_call_count == 1
    assert _FakeSink.terminated_resources == ["main"]

    assert registry.get_state() is LogSinkRegistryState.SHUT_DOWN


def test_get_during_shutdown_raises_registry_is_not_active() -> None:
    _FakeSink.reset()

    terminator_started = threading.Event()
    terminator_continue = threading.Event()

    _FakeSink.terminator_started_event = terminator_started
    _FakeSink.terminator_continue_event = terminator_continue

    registry = LogSinkRegistry()

    registry.register(
        name="root",
        sink_cls=_FakeSink,
        resource="main",
    )

    shutdown_error_holder: list[BaseException] = []

    def shutdown_target() -> None:
        try:
            registry.shutdown()
        except BaseException as exc:
            shutdown_error_holder.append(exc)

    shutdown_thread = threading.Thread(
        target=shutdown_target,
        name="registry-shutdown-thread",
    )

    shutdown_thread.start()

    assert terminator_started.wait(timeout=1.0)
    assert registry.get_state() is LogSinkRegistryState.SHUTTING_DOWN

    with pytest.raises(LogSinkRegistryError) as exc_info:
        registry.get("root")

    _assert_registry_error_reason(
        exc_info,
        LogSinkRegistryErrorReason.LOG_SINK_REGISTRY_IS_NOT_ACTIVE,
    )

    terminator_continue.set()

    shutdown_thread.join(timeout=2.0)

    assert not shutdown_thread.is_alive()
    assert shutdown_error_holder == []

    assert registry.get_state() is LogSinkRegistryState.SHUT_DOWN
    assert _FakeSink.terminator_call_count == 1


def test_concurrent_register_same_name_with_different_descriptors_creates_only_one_sink() -> None:
    _FakeSink.reset()
    _FakeSink.create_delay_s = 0.1

    registry = LogSinkRegistry()

    workers_count = 12
    start_barrier = threading.Barrier(workers_count)

    result_holder: list[LogSinkProto] = []
    error_holder: list[BaseException] = []

    result_lock = threading.Lock()

    def worker(index: int) -> None:
        try:
            start_barrier.wait(timeout=2.0)

            resource = "main" if index % 2 == 0 else "other"

            sink = registry.register(
                name="root",
                sink_cls=_FakeSink,
                resource=resource,
                config="default",
            )

            with result_lock:
                result_holder.append(sink)

        except BaseException as exc:
            with result_lock:
                error_holder.append(exc)

    threads = [
        threading.Thread(
            target=worker,
            args=(index,),
            name=f"register-conflict-worker-{index}",
        )
        for index in range(workers_count)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join(timeout=3.0)

    assert all(not thread.is_alive() for thread in threads)

    assert _FakeSink.create_call_count == 1
    assert _FakeSink.terminator_call_count == 0

    assert len(result_holder) + len(error_holder) == workers_count
    assert len(result_holder) in (workers_count // 2, workers_count // 2 + 1)
    assert len(error_holder) in (workers_count // 2, workers_count // 2 - 1)

    assert len({id(sink) for sink in result_holder}) == 1

    for error in error_holder:
        assert isinstance(error, LogSinkRegistryError)
        assert (
            error.reason_code
            == LogSinkRegistryErrorReason.LOG_SINK_ALREADY_REGISTERED_WITH_DIFFERENT_DESCRIPTOR.value
        )

    registered_sink = registry.get("root")
    assert registered_sink is result_holder[0]

    registered_fake_sink = registered_sink
    assert isinstance(registered_fake_sink, _FakeSink)
    assert registered_fake_sink.resource in {"main", "other"}

    for sink in result_holder:
        assert sink is registered_sink


def test_concurrent_shutdown_calls_each_terminator_only_once() -> None:
    _FakeSink.reset()

    registry = LogSinkRegistry()

    registry.register(name="a", sink_cls=_FakeSink, resource="a")
    registry.register(name="b", sink_cls=_FakeSink, resource="b")
    registry.register(name="c", sink_cls=_FakeSink, resource="c")

    workers_count = 10
    start_barrier = threading.Barrier(workers_count)

    error_holder: list[BaseException] = []
    worker_thread_ids: list[int] = []

    result_lock = threading.Lock()

    def worker() -> None:
        try:
            with result_lock:
                worker_thread_ids.append(threading.get_ident())

            start_barrier.wait(timeout=2.0)
            registry.shutdown()

        except BaseException as exc:
            with result_lock:
                error_holder.append(exc)

    threads = [
        threading.Thread(
            target=worker,
            name=f"shutdown-worker-{index}",
        )
        for index in range(workers_count)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join(timeout=3.0)

    assert all(not thread.is_alive() for thread in threads)

    assert error_holder == []

    assert len(worker_thread_ids) == workers_count
    assert len(set(worker_thread_ids)) == workers_count

    assert registry.get_state() is LogSinkRegistryState.SHUT_DOWN

    assert _FakeSink.terminator_call_count == 3
    assert sorted(_FakeSink.terminated_resources) == ["a", "b", "c"]


def test_register_during_shutdown_raises_registry_is_not_active_and_does_not_create_sink() -> None:
    _FakeSink.reset()

    terminator_started = threading.Event()
    terminator_continue = threading.Event()

    _FakeSink.terminator_started_event = terminator_started
    _FakeSink.terminator_continue_event = terminator_continue

    registry = LogSinkRegistry()

    registry.register(
        name="root",
        sink_cls=_FakeSink,
        resource="main",
    )

    shutdown_error_holder: list[BaseException] = []

    def shutdown_target() -> None:
        try:
            registry.shutdown()
        except BaseException as exc:
            shutdown_error_holder.append(exc)

    shutdown_thread = threading.Thread(
        target=shutdown_target,
        name="registry-shutdown-thread",
    )

    shutdown_thread.start()

    assert terminator_started.wait(timeout=1.0)
    assert registry.get_state() is LogSinkRegistryState.SHUTTING_DOWN

    create_call_count_before_register = _FakeSink.create_call_count

    with pytest.raises(LogSinkRegistryError) as exc_info:
        registry.register(
            name="late",
            sink_cls=_FakeSink,
            resource="late",
        )

    _assert_registry_error_reason(
        exc_info,
        LogSinkRegistryErrorReason.LOG_SINK_REGISTRY_IS_NOT_ACTIVE,
    )

    assert _FakeSink.create_call_count == create_call_count_before_register

    terminator_continue.set()

    shutdown_thread.join(timeout=2.0)

    assert not shutdown_thread.is_alive()
    assert shutdown_error_holder == []

    assert registry.get_state() is LogSinkRegistryState.SHUT_DOWN
    assert _FakeSink.terminator_call_count == 1
    assert _FakeSink.terminated_resources == ["main"]
