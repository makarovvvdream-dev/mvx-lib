"""
Tests for mvx.asyncio.helpers.run_with_cancellation_policy.run_with_cancellation_policy().

Assumptions:
- Python 3.13+ (Task.uncancel() and Task.cancelling()).
- pytest + pytest-asyncio.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Optional, TypeVar

import pytest

from mvx.common.helpers import (
    CancellationPolicy,
    run_with_cancellation_policy,
)

T = TypeVar("T")


# -----------------------------
# Helpers
# -----------------------------


@dataclass(slots=True)
class CoreProbe:
    """
    Observe core execution and cancellation behavior.
    """

    started: asyncio.Event
    release: asyncio.Event
    finished: asyncio.Event
    cancelled_seen: asyncio.Event
    steps: list[str]


def make_core_probe() -> CoreProbe:
    return CoreProbe(
        started=asyncio.Event(),
        release=asyncio.Event(),
        finished=asyncio.Event(),
        cancelled_seen=asyncio.Event(),
        steps=[],
    )


def make_waiting_core(
    probe: CoreProbe, *, result: object = "ok"
) -> Callable[[], Awaitable[object]]:
    """
    Core coroutine factory:
    - signals 'started'
    - waits for 'release'
    - returns result
    - records CancelledError if received
    """

    async def _core() -> object:
        probe.steps.append("core:enter")
        probe.started.set()
        try:
            probe.steps.append("core:wait_release")
            await probe.release.wait()
            probe.steps.append("core:released")
            return result
        except asyncio.CancelledError:
            probe.steps.append("core:cancelled")
            probe.cancelled_seen.set()
            raise
        finally:
            probe.steps.append("core:finally")
            probe.finished.set()

    def _factory() -> Awaitable[object]:
        return _core()

    return _factory


def make_raising_core(
    probe: CoreProbe,
    exc: BaseException,
    *,
    wait_release: bool = True,
) -> Callable[[], Awaitable[object]]:
    """
    Core coroutine factory:
    - signals 'started'
    - optionally waits for 'release'
    - raises exc
    - records CancelledError if received
    """

    async def _core() -> object:
        probe.steps.append("core:enter")
        probe.started.set()
        try:
            if wait_release:
                probe.steps.append("core:wait_release")
                await probe.release.wait()
                probe.steps.append("core:released")
            raise exc
        except asyncio.CancelledError:
            probe.steps.append("core:cancelled")
            probe.cancelled_seen.set()
            raise
        finally:
            probe.steps.append("core:finally")
            probe.finished.set()

    def _factory() -> Awaitable[object]:
        return _core()

    return _factory


async def cancel_repeatedly(task: asyncio.Task[object], *, times: int, delay_s: float) -> None:
    """
    Cancel the given task multiple times with a small delay between cancels.
    """
    for _ in range(times):
        task.cancel()
        if delay_s > 0:
            await asyncio.sleep(delay_s)


@dataclass(slots=True)
class TaskCaptureProbe:
    """
    Capture the task running the core coroutine (the internally created core_task).
    """

    started: asyncio.Event
    release: asyncio.Event
    core_task: Optional[asyncio.Task[object]]
    steps: list[str]

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.core_task = None
        self.steps = []


def make_capturing_core(probe: TaskCaptureProbe) -> Callable[[], Awaitable[object]]:
    """
    Core coroutine factory that captures asyncio.current_task() (the core_task created by the helper),
    then waits for release.
    """

    async def _core() -> object:
        probe.steps.append("core:enter")
        probe.core_task = asyncio.current_task()
        probe.started.set()
        probe.steps.append("core:wait_release")
        await probe.release.wait()
        probe.steps.append("core:released")
        return "ok"

    def _factory() -> Awaitable[object]:
        return _core()

    return _factory


class CustomAwaitable:
    """
    A minimal awaitable object (not a coroutine object) to validate the _as_coro() wrapper.

    It forwards __await__ to an inner awaitable.
    """

    __slots__ = ("_inner",)

    def __init__(self, inner: Awaitable[object]) -> None:
        self._inner = inner

    def __await__(self):
        return self._inner.__await__()


# -----------------------------
# PLAIN policy tests
# -----------------------------


@pytest.mark.asyncio
async def test_a01_plain_success_returns_result() -> None:
    probe = make_core_probe()
    core_func = make_waiting_core(probe, result=123)

    async def runner() -> object:
        return await run_with_cancellation_policy(core_func, policy=CancellationPolicy.PLAIN)

    task = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)
    probe.release.set()

    result = await asyncio.wait_for(task, timeout=1.0)

    assert result == 123
    assert probe.cancelled_seen.is_set() is False
    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_a02_plain_propagates_wrapper_cancellation_during_wait() -> None:
    probe = make_core_probe()
    core_func = make_waiting_core(probe, result="x")

    async def runner() -> object:
        return await run_with_cancellation_policy(core_func, policy=CancellationPolicy.PLAIN)

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)

    wrapper.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(wrapper, timeout=1.0)

    assert probe.cancelled_seen.is_set() is True
    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_a03_plain_pending_cancellation_before_entry_cancels_at_first_await_in_core() -> None:
    """
    In asyncio, Task.cancel() schedules cancellation which is delivered at the next await.
    Therefore in PLAIN mode (`await core_func()`), the core coroutine MAY start executing
    up to its first await, and then receive CancelledError.
    """
    probe = make_core_probe()

    async def core() -> object:
        probe.steps.append("core:enter")
        probe.started.set()
        try:
            probe.steps.append("core:before_first_await")
            await asyncio.sleep(0)  # cancellation is injected here
            probe.steps.append("core:after_first_await")  # should not happen
            return "ok"
        except asyncio.CancelledError:
            probe.steps.append("core:cancelled")
            probe.cancelled_seen.set()
            raise
        finally:
            probe.steps.append("core:finally")
            probe.finished.set()

    def core_func() -> Awaitable[object]:
        return core()

    t = asyncio.current_task()
    assert t is not None
    t.cancel()

    with pytest.raises(asyncio.CancelledError):
        await run_with_cancellation_policy(core_func, policy=CancellationPolicy.PLAIN)

    assert probe.started.is_set() is True
    assert probe.cancelled_seen.is_set() is True
    assert probe.finished.is_set() is True
    assert "core:after_first_await" not in probe.steps


@pytest.mark.asyncio
async def test_a04_plain_core_exception_propagated() -> None:
    probe = make_core_probe()
    core_func = make_raising_core(probe, RuntimeError("boom"), wait_release=True)

    async def runner() -> object:
        return await run_with_cancellation_policy(core_func, policy=CancellationPolicy.PLAIN)

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)
    probe.release.set()

    with pytest.raises(RuntimeError) as ei:
        await asyncio.wait_for(wrapper, timeout=1.0)

    assert "boom" in str(ei.value)
    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_a05_plain_core_cancelled_error_propagated() -> None:
    probe = make_core_probe()
    core_func = make_raising_core(probe, asyncio.CancelledError(), wait_release=False)

    async def runner() -> object:
        return await run_with_cancellation_policy(core_func, policy=CancellationPolicy.PLAIN)

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(wrapper, timeout=1.0)

    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_a06_plain_core_func_sync_exception_propagated() -> None:
    calls: list[int] = []

    def core_func() -> Awaitable[object]:
        calls.append(1)
        raise RuntimeError("sync boom")

    async def runner() -> object:
        return await run_with_cancellation_policy(core_func, policy=CancellationPolicy.PLAIN)

    wrapper = asyncio.create_task(runner())

    with pytest.raises(RuntimeError) as ei:
        await asyncio.wait_for(wrapper, timeout=1.0)

    assert "sync boom" in str(ei.value)
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_a07_plain_runs_core_in_same_task_no_internal_task_created() -> None:
    wrapper_task: list[asyncio.Task[object]] = []
    core_task_seen: list[asyncio.Task[object]] = []

    async def core() -> object:
        t = asyncio.current_task()
        assert t is not None
        core_task_seen.append(t)
        return "ok"

    def core_func() -> Awaitable[object]:
        return core()

    async def runner() -> object:
        t = asyncio.current_task()
        assert t is not None
        wrapper_task.append(t)
        return await run_with_cancellation_policy(
            core_func, policy=CancellationPolicy.PLAIN, op_name="ignored"
        )

    result = await asyncio.create_task(runner())
    assert result == "ok"
    assert core_task_seen[0] is wrapper_task[0]


# -----------------------------
# DEFER_FLAG policy tests
# -----------------------------


@pytest.mark.asyncio
async def test_b1_defer_flag_success_no_cancellation_returns_false_and_result() -> None:
    probe = make_core_probe()
    core_func = make_waiting_core(probe, result=123)

    async def runner() -> tuple[bool, object]:
        return await run_with_cancellation_policy(core_func, policy=CancellationPolicy.DEFER_FLAG)

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)
    probe.release.set()

    cancel_requested, result = await asyncio.wait_for(wrapper, timeout=1.0)

    assert cancel_requested is False
    assert result == 123
    assert probe.cancelled_seen.is_set() is False
    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_b2_defer_flag_single_wrapper_cancel_during_wait_sets_flag_and_core_completes() -> (
    None
):
    probe = make_core_probe()
    core_func = make_waiting_core(probe, result="done")

    async def runner() -> tuple[bool, object]:
        return await run_with_cancellation_policy(core_func, policy=CancellationPolicy.DEFER_FLAG)

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)

    wrapper.cancel()
    await asyncio.sleep(0)

    assert probe.cancelled_seen.is_set() is False

    probe.release.set()
    cancel_requested, result = await asyncio.wait_for(wrapper, timeout=1.0)

    assert cancel_requested is True
    assert result == "done"
    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_b3_defer_flag_multiple_wrapper_cancels_are_swallowed_until_core_finishes() -> None:
    probe = make_core_probe()
    core_func = make_waiting_core(probe, result=999)

    async def runner() -> tuple[bool, object]:
        return await run_with_cancellation_policy(core_func, policy=CancellationPolicy.DEFER_FLAG)

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)

    canceller = asyncio.create_task(cancel_repeatedly(wrapper, times=7, delay_s=0.01))

    await asyncio.sleep(0.05)
    assert probe.cancelled_seen.is_set() is False

    probe.release.set()

    cancel_requested, result = await asyncio.wait_for(wrapper, timeout=1.0)
    await asyncio.wait_for(canceller, timeout=1.0)

    assert cancel_requested is True
    assert result == 999
    assert probe.cancelled_seen.is_set() is False
    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_b4_defer_flag_pending_cancellation_before_entry_is_deferred_and_flag_set() -> None:
    probe = make_core_probe()
    core_func = make_waiting_core(probe, result="pre-cancel-ok")

    async def runner() -> tuple[bool, object]:
        # Make cancellation pending in the currently running wrapper task
        # before awaiting the primitive.
        t = asyncio.current_task()
        assert t is not None
        t.cancel()

        return await run_with_cancellation_policy(core_func, policy=CancellationPolicy.DEFER_FLAG)

    wrapper = asyncio.create_task(runner())

    # Core must still start (the primitive creates the core task before its first await).
    await asyncio.wait_for(probe.started.wait(), timeout=1.0)

    probe.release.set()
    cancel_requested, result = await asyncio.wait_for(wrapper, timeout=1.0)

    assert cancel_requested is True
    assert result == "pre-cancel-ok"
    assert probe.cancelled_seen.is_set() is False
    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_b5_defer_flag_cancel_close_to_completion_sets_flag_and_returns_result() -> None:
    probe = make_core_probe()
    core_func = make_waiting_core(probe, result="late")

    async def runner() -> tuple[bool, object]:
        return await run_with_cancellation_policy(core_func, policy=CancellationPolicy.DEFER_FLAG)

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)

    async def release_then_cancel() -> None:
        probe.release.set()
        await asyncio.sleep(0)
        wrapper.cancel()

    await asyncio.create_task(release_then_cancel())

    cancel_requested, result = await asyncio.wait_for(wrapper, timeout=1.0)

    assert cancel_requested is True
    assert result == "late"
    assert probe.cancelled_seen.is_set() is False
    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_b6_defer_flag_clears_pending_cancellation_so_post_return_await_does_not_fail() -> (
    None
):
    probe = make_core_probe()
    core_func = make_waiting_core(probe, result="post-await-ok")

    reached_after = asyncio.Event()

    async def runner() -> tuple[bool, object]:
        cancel_requested, result = await run_with_cancellation_policy(
            core_func,
            policy=CancellationPolicy.DEFER_FLAG,
        )
        await asyncio.sleep(0)
        reached_after.set()
        return cancel_requested, result

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)

    await cancel_repeatedly(wrapper, times=5, delay_s=0.0)

    probe.release.set()

    cancel_requested, result = await asyncio.wait_for(wrapper, timeout=1.0)

    assert cancel_requested is True
    assert result == "post-await-ok"
    assert reached_after.is_set() is True
    assert probe.cancelled_seen.is_set() is False
    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_b7_defer_flag_cancelling_counter_is_zero_after_return() -> None:
    probe = make_core_probe()
    core_func = make_waiting_core(probe, result="ok")

    cancelling_after: list[int] = []

    async def runner() -> tuple[bool, object]:
        cancel_requested, result = await run_with_cancellation_policy(
            core_func,
            policy=CancellationPolicy.DEFER_FLAG,
        )
        t = asyncio.current_task()
        assert t is not None
        cancelling_after.append(t.cancelling())
        return cancel_requested, result

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)
    await cancel_repeatedly(wrapper, times=3, delay_s=0.0)

    probe.release.set()

    cancel_requested, result = await asyncio.wait_for(wrapper, timeout=1.0)

    assert cancel_requested is True
    assert result == "ok"
    assert cancelling_after == [0]
    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_b8_defer_flag_core_exception_is_propagated_even_if_wrapper_cancelled() -> None:
    probe = make_core_probe()
    core_func = make_raising_core(probe, RuntimeError("boom"), wait_release=True)

    async def runner() -> tuple[bool, object]:
        return await run_with_cancellation_policy(core_func, policy=CancellationPolicy.DEFER_FLAG)

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)

    await cancel_repeatedly(wrapper, times=3, delay_s=0.01)

    probe.release.set()

    with pytest.raises(RuntimeError) as ei:
        await asyncio.wait_for(wrapper, timeout=1.0)

    assert "boom" in str(ei.value)
    assert probe.cancelled_seen.is_set() is False
    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_b9_defer_flag_core_cancelled_error_is_not_suppressed() -> None:
    probe = make_core_probe()
    core_func = make_raising_core(probe, asyncio.CancelledError(), wait_release=True)

    async def runner() -> tuple[bool, object]:
        return await run_with_cancellation_policy(core_func, policy=CancellationPolicy.DEFER_FLAG)

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)

    wrapper.cancel()
    await asyncio.sleep(0)

    probe.release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(wrapper, timeout=1.0)

    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_b10_defer_flag_core_task_cancel_via_cancel_is_not_masked_even_if_wrapper_cancelled() -> (
    None
):
    probe = TaskCaptureProbe()
    core_func = make_capturing_core(probe)

    async def runner() -> tuple[bool, object]:
        return await run_with_cancellation_policy(core_func, policy=CancellationPolicy.DEFER_FLAG)

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)
    assert probe.core_task is not None

    wrapper.cancel()
    await asyncio.sleep(0)

    probe.core_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(wrapper, timeout=1.0)


@pytest.mark.asyncio
async def test_b11_defer_flag_core_func_called_exactly_once_under_cancellation_storm() -> None:
    probe = make_core_probe()
    calls: list[int] = []

    base_core = make_waiting_core(probe, result="x")

    async def core_impl() -> object:
        calls.append(1)
        return await base_core()

    def core_func() -> Awaitable[object]:
        return core_impl()

    async def runner() -> tuple[bool, object]:
        return await run_with_cancellation_policy(core_func, policy=CancellationPolicy.DEFER_FLAG)

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)

    await cancel_repeatedly(wrapper, times=10, delay_s=0.0)

    probe.release.set()
    cancel_requested, result = await asyncio.wait_for(wrapper, timeout=1.0)

    assert len(calls) == 1
    assert cancel_requested is True
    assert result == "x"
    assert probe.cancelled_seen.is_set() is False


@pytest.mark.asyncio
async def test_b12_defer_flag_fast_core_completion_with_wrapper_cancel_returns_true_and_result() -> (
    None
):
    started = asyncio.Event()

    async def core() -> str:
        started.set()
        await asyncio.sleep(0)
        return "fast"

    def core_func() -> Awaitable[str]:
        return core()

    async def runner() -> tuple[bool, str]:
        return await run_with_cancellation_policy(core_func, policy=CancellationPolicy.DEFER_FLAG)

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(started.wait(), timeout=1.0)

    wrapper.cancel()
    await asyncio.sleep(0)

    cancel_requested, result = await asyncio.wait_for(wrapper, timeout=1.0)

    assert cancel_requested is True
    assert result == "fast"


@pytest.mark.asyncio
async def test_b13_defer_flag_no_unretrieved_task_exception_on_core_failure() -> None:
    loop = asyncio.get_running_loop()
    captured: list[dict[str, object]] = []
    prev_handler = loop.get_exception_handler()

    def handler(_loop: asyncio.AbstractEventLoop, context: dict[str, object]) -> None:
        captured.append(context)

    loop.set_exception_handler(handler)
    try:
        probe = make_core_probe()
        core_func = make_raising_core(probe, RuntimeError("boom"), wait_release=True)

        async def runner() -> tuple[bool, object]:
            return await run_with_cancellation_policy(
                core_func, policy=CancellationPolicy.DEFER_FLAG
            )

        wrapper = asyncio.create_task(runner())

        await asyncio.wait_for(probe.started.wait(), timeout=1.0)
        probe.release.set()

        with pytest.raises(RuntimeError):
            await asyncio.wait_for(wrapper, timeout=1.0)

        await asyncio.sleep(0)

        msgs = [str(ctx.get("message", "")) for ctx in captured]
        assert not any("Task exception was never retrieved" in m for m in msgs)

    finally:
        loop.set_exception_handler(prev_handler)


@pytest.mark.asyncio
async def test_b14_defer_flag_core_func_sync_exception_propagated_and_core_not_started() -> None:
    probe = make_core_probe()

    def core_func() -> Awaitable[object]:
        raise RuntimeError("sync boom")

    async def runner() -> tuple[bool, object]:
        return await run_with_cancellation_policy(core_func, policy=CancellationPolicy.DEFER_FLAG)

    wrapper = asyncio.create_task(runner())

    with pytest.raises(RuntimeError) as ei:
        await asyncio.wait_for(wrapper, timeout=1.0)

    assert "sync boom" in str(ei.value)
    assert probe.started.is_set() is False


@pytest.mark.asyncio
async def test_b15_defer_flag_accepts_non_coroutine_awaitable_via_as_coro() -> None:
    probe = make_core_probe()

    async def core() -> object:
        probe.started.set()
        await probe.release.wait()
        return "ok"

    def core_func() -> Awaitable[object]:
        return CustomAwaitable(core())

    async def runner() -> tuple[bool, object]:
        return await run_with_cancellation_policy(core_func, policy=CancellationPolicy.DEFER_FLAG)

    wrapper = asyncio.create_task(runner())

    try:
        await asyncio.wait_for(probe.started.wait(), timeout=1.0)
        probe.release.set()
        cancel_requested, result = await asyncio.wait_for(wrapper, timeout=1.0)
    finally:
        if wrapper.done():
            # Ensure exception is retrieved to avoid "Task exception was never retrieved"
            try:
                wrapper.result()
            except BaseException:
                pass
        else:
            wrapper.cancel()
            with pytest.raises(asyncio.CancelledError):
                await wrapper

    assert cancel_requested is False
    assert result == "ok"


@pytest.mark.asyncio
async def test_b16_defer_flag_wrapper_cancel_does_not_cancel_core_task() -> None:
    probe = TaskCaptureProbe()
    core_func = make_capturing_core(probe)

    async def runner() -> tuple[bool, object]:
        return await run_with_cancellation_policy(core_func, policy=CancellationPolicy.DEFER_FLAG)

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)
    assert probe.core_task is not None

    wrapper.cancel()
    await asyncio.sleep(0)

    assert probe.core_task.cancelled() is False
    assert probe.core_task.done() is False

    probe.release.set()
    cancel_requested, result = await asyncio.wait_for(wrapper, timeout=1.0)
    assert cancel_requested is True
    assert result == "ok"


# -----------------------------
# DEFER_RERAISE policy tests
# -----------------------------


@pytest.mark.asyncio
async def test_c1_defer_reraise_success_no_cancellation_returns_result() -> None:
    probe = make_core_probe()
    core_func = make_waiting_core(probe, result="ok")

    async def runner() -> object:
        return await run_with_cancellation_policy(
            core_func, policy=CancellationPolicy.DEFER_RERAISE
        )

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)
    probe.release.set()

    result = await asyncio.wait_for(wrapper, timeout=1.0)

    assert result == "ok"
    assert probe.cancelled_seen.is_set() is False
    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_c2_defer_reraise_single_wrapper_cancel_during_wait_reraises_after_completion() -> (
    None
):
    probe = make_core_probe()
    core_func = make_waiting_core(probe, result="done")

    async def runner() -> object:
        return await run_with_cancellation_policy(
            core_func, policy=CancellationPolicy.DEFER_RERAISE
        )

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)

    wrapper.cancel()
    await asyncio.sleep(0)

    probe.release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(wrapper, timeout=1.0)

    assert probe.cancelled_seen.is_set() is False
    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_c3_defer_reraise_multiple_wrapper_cancels_reraises_after_completion() -> None:
    probe = make_core_probe()
    core_func = make_waiting_core(probe, result=999)

    async def runner() -> object:
        return await run_with_cancellation_policy(
            core_func, policy=CancellationPolicy.DEFER_RERAISE
        )

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)

    canceller = asyncio.create_task(cancel_repeatedly(wrapper, times=7, delay_s=0.01))

    await asyncio.sleep(0.05)

    probe.release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(wrapper, timeout=1.0)

    await asyncio.wait_for(canceller, timeout=1.0)

    assert probe.cancelled_seen.is_set() is False
    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_c4_defer_reraise_pending_cancellation_before_entry_is_deferred_but_reraised_after_completion() -> (
    None
):
    probe = make_core_probe()
    core_func = make_waiting_core(probe, result="pre-cancel-ok")

    async def runner() -> object:
        # Make cancellation pending in the running wrapper task before entering the primitive.
        t = asyncio.current_task()
        assert t is not None
        t.cancel()

        return await run_with_cancellation_policy(
            core_func,
            policy=CancellationPolicy.DEFER_RERAISE,
        )

    wrapper = asyncio.create_task(runner())

    # Core must still start (deferred modes start core before the first await).
    await asyncio.wait_for(probe.started.wait(), timeout=1.0)

    probe.release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(wrapper, timeout=1.0)

    assert probe.cancelled_seen.is_set() is False
    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_c5_defer_reraise_cancel_close_to_completion_reraises_after_success() -> None:
    probe = make_core_probe()
    core_func = make_waiting_core(probe, result="late")

    async def runner() -> object:
        return await run_with_cancellation_policy(
            core_func, policy=CancellationPolicy.DEFER_RERAISE
        )

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)

    async def release_then_cancel() -> None:
        probe.release.set()
        await asyncio.sleep(0)
        wrapper.cancel()

    await asyncio.create_task(release_then_cancel())

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(wrapper, timeout=1.0)

    assert probe.cancelled_seen.is_set() is False
    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_c6_defer_reraise_core_exception_propagated_not_masked_by_wrapper_cancel() -> None:
    probe = make_core_probe()
    core_func = make_raising_core(probe, RuntimeError("boom"), wait_release=True)

    async def runner() -> object:
        return await run_with_cancellation_policy(
            core_func, policy=CancellationPolicy.DEFER_RERAISE
        )

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)

    await cancel_repeatedly(wrapper, times=3, delay_s=0.01)

    probe.release.set()

    with pytest.raises(RuntimeError) as ei:
        await asyncio.wait_for(wrapper, timeout=1.0)

    assert "boom" in str(ei.value)
    assert probe.cancelled_seen.is_set() is False
    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_c7_defer_reraise_core_cancelled_error_propagated() -> None:
    probe = TaskCaptureProbe()
    core_func = make_capturing_core(probe)

    async def runner() -> object:
        return await run_with_cancellation_policy(
            core_func, policy=CancellationPolicy.DEFER_RERAISE
        )

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)
    assert probe.core_task is not None

    # Cancel core: must be propagated, not converted into "deferred wrapper cancellation".
    probe.core_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(wrapper, timeout=1.0)


@pytest.mark.asyncio
async def test_c8_defer_reraise_core_func_called_exactly_once_under_cancellation_storm() -> None:
    probe = make_core_probe()
    calls: list[int] = []

    base_core = make_waiting_core(probe, result="x")

    async def core_impl() -> object:
        calls.append(1)
        return await base_core()

    def core_func() -> Awaitable[object]:
        return core_impl()

    async def runner() -> object:
        return await run_with_cancellation_policy(
            core_func, policy=CancellationPolicy.DEFER_RERAISE
        )

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)

    await cancel_repeatedly(wrapper, times=10, delay_s=0.0)

    probe.release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(wrapper, timeout=1.0)

    assert len(calls) == 1
    assert probe.cancelled_seen.is_set() is False
    assert probe.finished.is_set() is True


@pytest.mark.asyncio
async def test_c9_defer_reraise_core_func_sync_exception_propagated() -> None:
    def core_func() -> Awaitable[object]:
        raise RuntimeError("sync boom")

    async def runner() -> object:
        return await run_with_cancellation_policy(
            core_func, policy=CancellationPolicy.DEFER_RERAISE
        )

    wrapper = asyncio.create_task(runner())

    with pytest.raises(RuntimeError) as ei:
        await asyncio.wait_for(wrapper, timeout=1.0)

    assert "sync boom" in str(ei.value)


@pytest.mark.asyncio
async def test_c10_defer_reraise_core_func_sync_exception_propagated() -> None:
    def core_func() -> Awaitable[object]:
        raise RuntimeError("sync boom")

    async def runner() -> object:
        return await run_with_cancellation_policy(
            core_func,
            policy=CancellationPolicy.DEFER_RERAISE,
        )

    with pytest.raises(RuntimeError) as ei:
        await runner()

    assert "sync boom" in str(ei.value)


# -----------------------------
# op_name
# -----------------------------


@pytest.mark.asyncio
async def test_d1_core_task_name_uses_op_name() -> None:
    """
    The helper must name the internally created core task using the provided op_name.
    """
    probe = TaskCaptureProbe()
    core_func = make_capturing_core(probe)

    async def runner() -> tuple[bool, object]:
        return await run_with_cancellation_policy(
            core_func,
            policy=CancellationPolicy.DEFER_FLAG,
            op_name="mvx.test.core_op",
        )

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)
    assert probe.core_task is not None

    assert probe.core_task.get_name() == "mvx.test.core_op"

    probe.release.set()
    cancel_requested, result = await asyncio.wait_for(wrapper, timeout=1.0)

    assert cancel_requested is False
    assert result == "ok"


@pytest.mark.asyncio
async def test_d2_core_task_name_uses_op_name_in_defer_reraise() -> None:
    probe = TaskCaptureProbe()
    core_func = make_capturing_core(probe)

    async def runner() -> object:
        return await run_with_cancellation_policy(
            core_func,
            policy=CancellationPolicy.DEFER_RERAISE,
            op_name="mvx.test.core_op.reraise",
        )

    wrapper = asyncio.create_task(runner())

    await asyncio.wait_for(probe.started.wait(), timeout=1.0)
    assert probe.core_task is not None
    assert probe.core_task.get_name() == "mvx.test.core_op.reraise"

    probe.release.set()
    result = await asyncio.wait_for(wrapper, timeout=1.0)
    assert result == "ok"


@pytest.mark.asyncio
async def test_e1_base_exception_not_intercepted() -> None:
    class CustomBaseException(BaseException):
        pass

    async def core() -> object:
        raise CustomBaseException("boom")

    def core_func() -> Awaitable[object]:
        return core()

    with pytest.raises(CustomBaseException):
        await run_with_cancellation_policy(
            core_func,
            policy=CancellationPolicy.DEFER_FLAG,
        )
