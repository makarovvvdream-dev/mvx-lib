# common/src/mvx/common/helpers/run_with_cancellation_policy.py
"""
Utilities for running awaitables under explicit cancellation policies.

The module provides a small primitive for cases where cancellation of the
caller task must either be handled normally, deferred and reported as a flag,
or deferred and re-raised after the protected operation completes.
"""

from __future__ import annotations

from typing import Any, Coroutine, Literal, TypeVar, Tuple, overload, Union
from collections.abc import Awaitable, Callable

from enum import StrEnum
import asyncio

__all__ = (
    "CancellationPolicy",
    "run_with_cancellation_policy",
)

T = TypeVar("T")


class CancellationPolicy(StrEnum):
    """
    Cancellation handling policy for run_with_cancellation_policy().
    """

    PLAIN = "PLAIN"
    DEFER_RERAISE = "DEFER_RERAISE"
    DEFER_FLAG = "DEFER_FLAG"


async def _as_coro(a: Awaitable[T]) -> T:
    """
    Wrap an arbitrary awaitable into a coroutine.

    Args:
        a: Awaitable to execute.

    Returns:
        Result produced by the awaitable.
    """
    return await a


def _start_core_task(core_func: Callable[[], Awaitable[T]], op_name: str) -> asyncio.Task[T]:
    """
    Start the core awaitable as a named asyncio task.

    Args:
        core_func: Zero-argument callable returning the awaitable to run.
        op_name: Name assigned to the created task.

    Returns:
        Started asyncio task.
    """
    a = core_func()
    coro: Coroutine[Any, Any, T] = _as_coro(a)
    return asyncio.create_task(coro, name=op_name)


@overload
async def run_with_cancellation_policy(
    core_func: Callable[[], Awaitable[T]],
    *,
    policy: Literal[CancellationPolicy.PLAIN],
) -> T: ...


@overload
async def run_with_cancellation_policy(
    core_func: Callable[[], Awaitable[T]],
    *,
    policy: Literal[CancellationPolicy.DEFER_RERAISE],
) -> T: ...


@overload
async def run_with_cancellation_policy(
    core_func: Callable[[], Awaitable[T]],
    *,
    policy: Literal[CancellationPolicy.DEFER_FLAG] = CancellationPolicy.DEFER_FLAG,
) -> Tuple[bool, T]: ...


@overload
async def run_with_cancellation_policy(
    core_func: Callable[[], Awaitable[T]],
    *,
    policy: CancellationPolicy,
    op_name: str = "unknown",
) -> Union[T, Tuple[bool, T]]: ...


async def run_with_cancellation_policy(
    core_func: Callable[[], Awaitable[T]],
    *,
    policy: CancellationPolicy = CancellationPolicy.DEFER_FLAG,
    op_name: str = "unknown",
) -> Union[T, Tuple[bool, T]]:
    """
    Run one awaitable under the selected cancellation policy.

    Args:
        core_func: Zero-argument callable returning the awaitable to run.
        policy: Cancellation policy applied while awaiting the operation.
        op_name: Name assigned to the internal task in deferred policies.

    Returns:
        The operation result, or ``(cancel_requested, result)`` when using
        ``CancellationPolicy.DEFER_FLAG``.

    Raises:
        asyncio.CancelledError: Raised according to the selected cancellation
            policy or when the core task itself is cancelled.
        Exception: Propagates exceptions raised by the core awaitable.
    """
    if policy is CancellationPolicy.PLAIN:
        return await core_func()

    core_task: asyncio.Task[T] = _start_core_task(core_func, op_name)
    cancel_requested = False

    while True:
        try:
            result = await asyncio.shield(core_task)
            break
        except asyncio.CancelledError:
            # If the core task itself was cancelled, propagate core cancellation.
            if core_task.cancelled():
                raise

            # Otherwise this is caller-task cancellation while core is still running (deferrable).
            cancel_requested = True

            current = asyncio.current_task()
            if current is not None:
                # Drop all pending cancellation requests for this caller task so that
                # the next await does not immediately raise again.
                while current.uncancel() > 0:
                    pass
            # Keep waiting for core_task to finish.

        # Drain any pending cancellation that might have arrived in a tight race window
        # after the await resumed but before we return/raise.
    current = asyncio.current_task()
    if current is not None and current.cancelling() > 0:
        cancel_requested = True
        while current.uncancel() > 0:
            pass

    if policy is CancellationPolicy.DEFER_FLAG:
        return cancel_requested, result

    # policy is DEFER_RERAISE
    if cancel_requested:
        raise asyncio.CancelledError()
    return result
