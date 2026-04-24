# common/src/mvx/common/helpers/api_error_processor.py
"""
Decorator for normalizing public API errors.

The decorator lets declared API errors pass through unchanged and wraps
unexpected exceptions into a configured RuntimeExtendedError subclass.
Cancellation is always propagated unchanged.
"""

from __future__ import annotations

from typing import Callable, TypeVar, Any, cast

import asyncio
from functools import wraps
import inspect

from ..errors import RuntimeExtendedError

__all__ = ("api_error_processor",)

F = TypeVar("F", bound=Callable[..., Any])


def api_error_processor(
    *,
    passthrough_error_types: tuple[type[Exception], ...],
    raise_error_type: type[RuntimeExtendedError],
) -> Callable[[F], F]:
    """
    Build a decorator for public API exception normalization.

    Args:
        passthrough_error_types: Exception types that must pass through unchanged.
        raise_error_type: RuntimeExtendedError subclass used to wrap unexpected
            exceptions.

    Returns:
        Decorator that applies the public API error policy to sync or async
        callables.
    """

    def decorate(func: F) -> F:
        module = getattr(func, "__module__", "<unknown>")
        qualname = getattr(func, "__qualname__", getattr(func, "__name__", "<unknown>"))

        target = inspect.unwrap(func)

        if inspect.iscoroutinefunction(target):

            @wraps(func)
            async def wrapped_async(*args: Any, **kwargs: Any) -> Any:
                try:
                    return await func(*args, **kwargs)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if isinstance(exc, passthrough_error_types):
                        raise

                    if isinstance(exc, RuntimeExtendedError):
                        if exc.module is None:
                            exc.module = module
                        if exc.qualname is None:
                            exc.qualname = qualname
                        raise

                    # noinspection PyBroadException
                    try:
                        err = raise_error_type(
                            message=f"runtime unexpected error: {str(exc)}",
                            module=module,
                            qualname=qualname,
                            cause=exc,
                        )
                    except Exception:
                        err = raise_error_type(message=str(exc))
                        err.cause = exc
                        err.module = module
                        err.qualname = qualname

                    raise err from exc

            # noinspection PyUnnecessaryCast
            return cast(F, wrapped_async)

        @wraps(func)
        def wrapped_sync(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if isinstance(exc, passthrough_error_types):
                    raise
                if isinstance(exc, RuntimeExtendedError):
                    if exc.module is None:
                        exc.module = module
                    if exc.qualname is None:
                        exc.qualname = qualname
                    raise

                # noinspection PyBroadException
                try:
                    err = raise_error_type(
                        message=f"runtime unexpected error: {str(exc)}",
                        module=module,
                        qualname=qualname,
                        cause=exc,
                    )
                except Exception:
                    err = raise_error_type(message=str(exc))
                    err.cause = exc
                    err.module = module
                    err.qualname = qualname

                raise err from exc

        # noinspection PyUnnecessaryCast
        return cast(F, wrapped_sync)

    return decorate
