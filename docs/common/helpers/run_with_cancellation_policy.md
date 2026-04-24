# run_with_cancellation_policy

`run_with_cancellation_policy` runs one async operation under an explicit
cancellation policy.

It is intended for lifecycle and cleanup code where caller cancellation should
not always abort the protected operation immediately.

## Why it exists

In `asyncio`, cancellation is delivered by injecting `asyncio.CancelledError`
into the awaiting task.

For ordinary operations, this behavior is usually correct:

```text
result = await operation()
```

If the caller task is cancelled, the await is interrupted.

But some operations are different. Cleanup, graceful disconnect, drain-to-idle,
or teardown sequences may need to finish even if the caller task receives
cancellation while waiting.

Examples:

```text
disconnect
cleanup
drain pending work
close transport
wait until internal queues become idle
```

In those cases, the code often needs one of three behaviors:

- propagate cancellation normally;
- defer cancellation and return whether it happened;
- defer cancellation and re-raise it after the protected operation completes.

`run_with_cancellation_policy` makes that choice explicit.

## Cancellation policies

The behavior is controlled by `CancellationPolicy`.

```python
from mvx.common.helpers import CancellationPolicy
```

## PLAIN

`CancellationPolicy.PLAIN` behaves like a normal await.

```python

from mvx.common.helpers import run_with_cancellation_policy, CancellationPolicy

async def operation() -> None:
    ...

async def main():
    result = await run_with_cancellation_policy(
        core_func=operation,
        policy=CancellationPolicy.PLAIN,
    )
```

This is equivalent to:

```text
result = await operation()
```

No separate task is created. No shielding is applied.

If the caller task is cancelled, cancellation propagates normally and the await
is interrupted.

Use this policy when no special cancellation handling is needed.

## DEFER_FLAG

`CancellationPolicy.DEFER_FLAG` protects the core operation from caller-task
cancellation and reports whether cancellation was requested.

```python
from mvx.common.helpers import run_with_cancellation_policy, CancellationPolicy

async def operation() -> None:
    ...
async def main():
    cancel_requested, result = await run_with_cancellation_policy(
        core_func=operation,
        policy=CancellationPolicy.DEFER_FLAG,
        op_name="disconnect",
    )
```

The core operation runs in its own task. The caller awaits it through
`asyncio.shield()`.

If the caller task is cancelled while waiting:

- the core task is not cancelled;
- the cancellation request is recorded;
- the caller task cancellation state is cleared;
- waiting continues until the core task finishes.

On success, the function returns:

```text
cancel_requested, result
```

This policy is useful when higher-level code needs to know that cancellation was
requested, but the protected operation still had to finish.

## DEFER_RERAISE

`CancellationPolicy.DEFER_RERAISE` also protects the core operation from
caller-task cancellation.

The difference is the final outcome.

```python
from mvx.common.helpers import run_with_cancellation_policy, CancellationPolicy

async def operation() -> None:
    ...
async def main():
    result = await run_with_cancellation_policy(
        core_func=operation,
        policy=CancellationPolicy.DEFER_RERAISE,
        op_name="cleanup",
    )
```

If cancellation was requested while waiting, the function waits for the core
operation to complete successfully and then raises `asyncio.CancelledError`.

This gives deferred cancellation semantics:

```text
finish the protected operation first
then restore cancellation to the caller
```

Use this policy when cleanup must finish, but the caller should still observe
cancellation after cleanup is complete.

## Core task cancellation

Deferred policies protect the core task from caller-task cancellation.

They do not hide cancellation of the core task itself.

If the core task is cancelled directly, `asyncio.CancelledError` is propagated.

This distinction matters:

```text
caller cancelled while waiting  → can be deferred
core task cancelled             → propagated
```

## Core exceptions

Exceptions raised by the core operation are never masked.

```python
async def operation() -> int:
    raise RuntimeError("operation failed")
```

All policies propagate that exception:

```python
from mvx.common.helpers import run_with_cancellation_policy, CancellationPolicy

async def operation() -> None:
    ...
async def main():
    try:
        await run_with_cancellation_policy(
            core_func=operation,
            policy=CancellationPolicy.DEFER_FLAG,
        )
    except RuntimeError:
        handled = True
```

Deferred policies only affect caller-task cancellation while waiting. They do not
turn core failures into successful results.

## Operation invocation

The `core_func` callable is invoked exactly once.

```python
from mvx.common.helpers import run_with_cancellation_policy, CancellationPolicy

async def operation() -> str:
    return "done"

async def main():
    result = await run_with_cancellation_policy(
        core_func=operation,
        policy=CancellationPolicy.PLAIN,
    )
```

In deferred policies, the awaitable returned by `core_func` is wrapped into a
single internal task.

The operation is not retried, restarted, or supervised.

## Passing arguments to the operation

`core_func` is a zero-argument callable. If the coroutine function needs
arguments, wrap the call with `lambda`.

```python
from mvx.common.helpers import run_with_cancellation_policy, CancellationPolicy

async def disconnect(reason: str, force: bool) -> None:
    ...
async def main():
    await run_with_cancellation_policy(
        core_func=lambda: disconnect(reason="shutdown", force=True),
        policy=CancellationPolicy.DEFER_RERAISE,
        op_name="disconnect",
    )
```

The lambda is not used for laziness as a trick. It is the explicit boundary where
the helper receives a no-argument callable and remains responsible only for
cancellation policy, not for knowing the operation signature.

## Task name

Deferred policies create an internal task.

The `op_name` argument is used as that task's name:

```python
from mvx.common.helpers import run_with_cancellation_policy, CancellationPolicy

async def operation() -> str:
    return "done"

async def main():
    cancel_requested, result = await run_with_cancellation_policy(
        core_func=operation,
        policy=CancellationPolicy.DEFER_FLAG,
        op_name="transport-cleanup",
    )
```

This is useful for debugging, logs, and asyncio task introspection.

`op_name` is ignored by `CancellationPolicy.PLAIN`.

## Cancellation accounting

Deferred policies use `Task.cancelling()` and `Task.uncancel()`.

Multiple calls to `Task.cancel()` may leave multiple pending cancellation
requests on the caller task. If those pending requests are not cleared, the next
`await` may immediately raise `CancelledError` again.

The helper drains pending caller-task cancellations while it is deliberately
deferring cancellation.

This is the mechanism that allows the helper to continue waiting for the core
task after caller cancellation was requested.

## Typical usage

The helper is most useful around lifecycle code:

```python
from mvx.common.helpers import run_with_cancellation_policy, CancellationPolicy

async def disconnect() -> bool:
    return True

async def main():
    cancel_requested, _ = await run_with_cancellation_policy(
        core_func=disconnect,
        policy=CancellationPolicy.DEFER_FLAG,
        op_name="transport-disconnect",
    )

    if cancel_requested:
        cleanup_info = "disconnect completed after cancellation request"
```

For cleanup that must finish but should preserve cancellation semantics:

```python
from mvx.common.helpers import run_with_cancellation_policy, CancellationPolicy

async def cleanup() -> bool:
    return True

async def main():
    await run_with_cancellation_policy(
        core_func=cleanup,
        policy=CancellationPolicy.DEFER_RERAISE,
        op_name="processor-cleanup",
    )
```

## Design rule

Use `run_with_cancellation_policy` when cancellation behavior is part of the
operation contract.

Do not use it as a generic wrapper around every async call. Most awaits should
remain plain awaits.

This helper is for places where the code must explicitly choose between normal
cancellation, deferred cancellation with a flag, and deferred cancellation with
re-raise.

## API

```{eval-rst}
.. autoclass:: mvx.common.helpers.CancellationPolicy
   :members:

.. autofunction:: mvx.common.helpers.run_with_cancellation_policy
```