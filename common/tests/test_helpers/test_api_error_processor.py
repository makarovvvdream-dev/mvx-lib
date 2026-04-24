from __future__ import annotations

import asyncio
import inspect
from functools import wraps
from typing import Any

import pytest

from mvx.common.helpers import api_error_processor
from mvx.common.errors import RuntimeExtendedError


class ApiInputError(ValueError):
    """Declared API error that must pass through unchanged."""


class RuntimeUnexpectedError(RuntimeExtendedError):
    pass


class MessageOnlyUnexpectedError(RuntimeError):
    def __init__(self, message: str) -> None:
        super().__init__(message)


def _make_decorator(
    raise_error_type: type[Exception] = RuntimeUnexpectedError,
    passthrough_error_types: tuple[type[Exception], ...] = (ApiInputError,),
):
    return api_error_processor(
        passthrough_error_types=passthrough_error_types,
        raise_error_type=raise_error_type,  # type: ignore[arg-type]
    )


# ---------- Group A: passthrough and cancellation ----------


def test_a1_sync_passthrough_is_not_wrapped() -> None:
    public_api = _make_decorator()

    @public_api
    def f() -> None:
        raise ApiInputError("bad input")

    with pytest.raises(ApiInputError) as ei:
        f()

    assert ei.value.args == ("bad input",)


@pytest.mark.asyncio
async def test_a2_async_cancelled_error_is_not_wrapped() -> None:
    public_api = _make_decorator()

    @public_api
    async def f() -> None:
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await f()


def test_a3_sync_cancelled_error_is_not_wrapped() -> None:
    public_api = _make_decorator()

    @public_api
    def f() -> None:
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        f()


def test_a4_passthrough_multiple_types() -> None:
    class ApiAlreadyWrappedError(RuntimeError):
        pass

    public_api = _make_decorator(
        passthrough_error_types=(ApiInputError, ApiAlreadyWrappedError),
    )

    @public_api
    def f1() -> None:
        raise ApiInputError("a")

    @public_api
    def f2() -> None:
        raise ApiAlreadyWrappedError("b")

    with pytest.raises(ApiInputError):
        f1()

    with pytest.raises(ApiAlreadyWrappedError):
        f2()


# ---------- Group B: unexpected error wrapping ----------


def test_b1_sync_unexpected_is_wrapped_with_metadata_and_chaining() -> None:
    public_api = _make_decorator()

    @public_api
    def f() -> None:
        raise ValueError("boom")

    with pytest.raises(RuntimeUnexpectedError) as ei:
        f()

    err = ei.value
    assert err.module == f.__module__
    assert err.qualname == f.__qualname__
    assert isinstance(err.cause, ValueError)
    assert str(err.cause) == "boom"
    assert err.message == "runtime unexpected error: boom"
    assert isinstance(err.__cause__, ValueError)


@pytest.mark.asyncio
async def test_b2_async_unexpected_is_wrapped_with_metadata_and_chaining() -> None:
    public_api = _make_decorator()

    @public_api
    async def f() -> None:
        raise TypeError("kaboom")

    with pytest.raises(RuntimeUnexpectedError) as ei:
        await f()

    err = ei.value
    assert err.module == f.__module__
    assert err.qualname == f.__qualname__
    assert isinstance(err.cause, TypeError)
    assert str(err.cause) == "kaboom"
    assert err.message == "runtime unexpected error: kaboom"
    assert isinstance(err.__cause__, TypeError)


def test_b3_fallback_path_used_when_wrapper_ctor_rejects_kwargs() -> None:
    public_api = _make_decorator(raise_error_type=MessageOnlyUnexpectedError)

    @public_api
    def f() -> None:
        raise AssertionError("invariant failed")

    with pytest.raises(MessageOnlyUnexpectedError) as ei:
        f()

    err = ei.value
    assert str(err) == "invariant failed"
    assert getattr(err, "module") == f.__module__
    assert getattr(err, "qualname") == f.__qualname__
    assert isinstance(getattr(err, "cause"), AssertionError)
    assert isinstance(err.__cause__, AssertionError)


# ---------- Group C: RuntimeExtendedError passthrough ----------


def test_c1_runtime_extended_error_is_not_wrapped_but_enriched() -> None:
    public_api = _make_decorator()

    @public_api
    def f() -> None:
        raise RuntimeUnexpectedError(message="already wrapped")

    with pytest.raises(RuntimeUnexpectedError) as ei:
        f()

    err = ei.value
    assert err.message == "already wrapped"
    assert err.module == f.__module__
    assert err.qualname == f.__qualname__


def test_c2_runtime_extended_error_metadata_not_overwritten() -> None:
    public_api = _make_decorator()

    @public_api
    def f() -> None:
        raise RuntimeUnexpectedError(
            message="already wrapped",
            module="pre.module",
            qualname="pre.qualname",
        )

    with pytest.raises(RuntimeUnexpectedError) as ei:
        f()

    err = ei.value
    assert err.module == "pre.module"
    assert err.qualname == "pre.qualname"


# ---------- Group D: function metadata detection ----------


def test_d1_qualname_for_methods_is_captured() -> None:
    public_api = _make_decorator()

    class Svc:
        @public_api
        def do(self) -> None:
            raise ValueError("x")

    svc = Svc()

    with pytest.raises(RuntimeUnexpectedError) as ei:
        svc.do()

    err = ei.value
    assert err.qualname.endswith("Svc.do")
    assert err.module == svc.do.__module__


@pytest.mark.asyncio
async def test_d2_unwrap_detects_coroutine_function_under_other_decorators() -> None:
    def other_decorator(func):
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await func(*args, **kwargs)

        return wrapper

    @other_decorator
    async def inner() -> None:
        raise ValueError("inner boom")

    public_api = _make_decorator()
    wrapped = public_api(inner)

    assert inspect.iscoroutinefunction(wrapped)

    with pytest.raises(RuntimeUnexpectedError) as ei:
        await wrapped()

    err = ei.value
    assert err.module == inner.__module__
    assert err.qualname == inner.__qualname__
    assert isinstance(err.__cause__, ValueError)


# ---------- Group E: RuntimeExtendedError normalization ----------


def test_e1_runtime_extended_error_metadata_normalization() -> None:
    err1 = RuntimeUnexpectedError(message="x", module="   ", qualname="\n\t  ")
    assert err1.module is None
    assert err1.qualname is None

    err2 = RuntimeUnexpectedError(message="x", module="  mod.name  ", qualname="  C.m  ")
    assert err2.module == "mod.name"
    assert err2.qualname == "C.m"


# ---------- Group F: edge cases and failure modes ----------


def test_f1_base_exception_is_not_wrapped() -> None:
    public_api = _make_decorator()

    @public_api
    def f() -> None:
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        f()


def test_f2_fallback_constructor_failure_propagates() -> None:
    class BrokenError(RuntimeError):
        def __init__(self, *args, **kwargs):
            raise RuntimeError("broken ctor")

    public_api = _make_decorator(raise_error_type=BrokenError)

    @public_api
    def f() -> None:
        raise ValueError("boom")

    with pytest.raises(RuntimeError) as ei:
        f()

    assert str(ei.value) == "broken ctor"
