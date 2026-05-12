# tests/test_logger/test_package_log_invocation_integration.py
from __future__ import annotations

import asyncio
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path


import pytest

import mvx.common.logger as logger_pack

from mvx.common.logger import (
    LogContext,
    LogLevel,
)


@dataclass(frozen=True, slots=True)
class OperationResult:
    status: str
    items: list[int]


class InvocationOwner:
    def __init__(self, ctx: LogContext, identity: str = "owner-1") -> None:
        self._ctx = ctx
        self._identity = identity

    def get_log_context(self) -> LogContext:
        return self._ctx

    @property
    def identity(self) -> str:
        return self._identity


class CustomOperationError(RuntimeError):
    pass


@pytest.fixture(autouse=True)
def reset_logger_environment() -> Iterator[None]:
    logger_pack.reset_logger()

    yield

    logger_pack.reset_logger()


def configure_file_logging(tmp_path: Path) -> tuple[Path, LogContext]:
    log_file = tmp_path / "invocation.log"

    file_sink = logger_pack.configure_log_sink(
        name="file",
        sink_cls=logger_pack.FileLogSink,
        config=logger_pack.LoggingFileConfig(
            file_path=log_file,
            level=LogLevel.DEBUG,
            log_format="%(levelname)s: %(message)s %(payload)s",
        ),
    )

    root = logger_pack.get_root_log_context()
    root.set_log_sink(file_sink)

    ctx = logger_pack.configure_log_context("mvx.integration")

    return log_file, ctx


# ==== A. Sync invocation through package logger ===========================================


def test_a01_sync_invocation_writes_invoke_and_success_events_to_file(tmp_path: Path) -> None:
    log_file, ctx = configure_file_logging(tmp_path)
    owner = InvocationOwner(ctx, identity="sync-owner")

    @logger_pack.log_invocation(
        "operation.transfer",
        invoke_level=LogLevel.INFO,
        success_level=LogLevel.INFO,
        context_fields=("operation_id",),
        log_kwargs_on_invoke=("amount", "tags!"),
        log_result_on_success=("status", "item_count=items.len()"),
    )
    def transfer(
        self: InvocationOwner,
        operation_id: str,
        amount: int,
        tags: list[str],
    ) -> OperationResult:
        _ = self
        _ = operation_id
        _ = amount
        _ = tags

        return OperationResult(
            status="accepted",
            items=[1, 2, 3],
        )

    result = transfer(
        owner,
        operation_id="op-001",
        amount=150,
        tags=["urgent", "vip", "manual"],
    )

    assert result == OperationResult(status="accepted", items=[1, 2, 3])

    logger_pack.reset_logger()

    content = log_file.read_text()

    assert "INFO: mvx.integration.sync-owner.operation.transfer [invoke]" in content
    assert "INFO: mvx.integration.sync-owner.operation.transfer [success]" in content

    assert "'operation_id': 'op-001'" in content
    assert "'kwargs': {'amount': 150, 'tags': ['urgent', 'vip', 'manual']}" in content
    assert "'result': {'status': 'accepted', 'item_count': 3}" in content


def test_a02_sync_invocation_writes_failed_event_to_file_and_reraises(tmp_path: Path) -> None:
    log_file, ctx = configure_file_logging(tmp_path)
    owner = InvocationOwner(ctx, identity="sync-owner")

    @logger_pack.log_invocation(
        "operation.fail",
        invoke_level=LogLevel.INFO,
        context_fields=("operation_id",),
        log_kwargs_on_invoke=("amount",),
    )
    def failing_operation(
        self: InvocationOwner,
        operation_id: str,
        amount: int,
    ) -> None:
        _ = self
        _ = operation_id
        _ = amount

        raise CustomOperationError("operation failed")

    with pytest.raises(CustomOperationError, match="operation failed"):
        failing_operation(
            owner,
            operation_id="op-err",
            amount=999,
        )

    logger_pack.reset_logger()

    content = log_file.read_text()

    assert "INFO: mvx.integration.sync-owner.operation.fail [invoke]" in content
    assert "ERROR: mvx.integration.sync-owner.operation.fail [failed]" in content

    assert "'operation_id': 'op-err'" in content
    assert "'kwargs': {'amount': 999}" in content
    assert "'error': {'kind': 'CustomOperationError', 'message': 'operation failed'}" in content


def test_a03_nested_sync_invocation_logs_error_payload_only_once(tmp_path: Path) -> None:
    log_file, ctx = configure_file_logging(tmp_path)
    owner = InvocationOwner(ctx, identity="sync-owner")

    @logger_pack.log_invocation(
        "operation.inner",
        invoke_level=LogLevel.INFO,
    )
    def inner(self: InvocationOwner) -> None:
        _ = self
        raise CustomOperationError("inner failed")

    @logger_pack.log_invocation(
        "operation.outer",
        invoke_level=LogLevel.INFO,
    )
    def outer(self: InvocationOwner) -> None:
        inner(self)

    with pytest.raises(CustomOperationError, match="inner failed"):
        outer(owner)

    logger_pack.reset_logger()

    content = log_file.read_text()

    assert "INFO: mvx.integration.sync-owner.operation.outer [invoke]" in content
    assert "INFO: mvx.integration.sync-owner.operation.inner [invoke]" in content

    assert "ERROR: mvx.integration.sync-owner.operation.inner [failed]" in content
    assert "DEBUG: mvx.integration.sync-owner.operation.outer [failed]" in content

    assert content.count("'kind': 'CustomOperationError'") == 1
    assert content.count("'message': 'inner failed'") == 1


# ==== B. Async invocation through package logger ==========================================


@pytest.mark.asyncio
async def test_b01_async_invocation_writes_invoke_and_success_events_to_file(
    tmp_path: Path,
) -> None:
    log_file, ctx = configure_file_logging(tmp_path)
    owner = InvocationOwner(ctx, identity="async-owner")

    @logger_pack.log_invocation(
        "operation.async",
        invoke_level=LogLevel.INFO,
        success_level=LogLevel.INFO,
        context_fields=("operation_id",),
        log_kwargs_on_invoke=("amount",),
        log_result_on_success=("status",),
    )
    async def async_operation(
        self: InvocationOwner,
        operation_id: str,
        amount: int,
    ) -> OperationResult:
        _ = self
        _ = operation_id
        _ = amount

        await asyncio.sleep(0)

        return OperationResult(
            status="done",
            items=[1, 2],
        )

    result = await async_operation(
        owner,
        operation_id="async-001",
        amount=10,
    )

    assert result == OperationResult(status="done", items=[1, 2])

    logger_pack.reset_logger()

    content = log_file.read_text()

    assert "INFO: mvx.integration.async-owner.operation.async [invoke]" in content
    assert "INFO: mvx.integration.async-owner.operation.async [success]" in content

    assert "'operation_id': 'async-001'" in content
    assert "'kwargs': {'amount': 10}" in content
    assert "'result': {'status': 'done'}" in content


@pytest.mark.asyncio
async def test_b02_async_invocation_writes_failed_event_to_file_and_reraises(
    tmp_path: Path,
) -> None:
    log_file, ctx = configure_file_logging(tmp_path)
    owner = InvocationOwner(ctx, identity="async-owner")

    @logger_pack.log_invocation(
        "operation.async.fail",
        invoke_level=LogLevel.INFO,
        context_fields=("operation_id",),
    )
    async def async_failing_operation(
        self: InvocationOwner,
        operation_id: str,
    ) -> None:
        _ = self
        _ = operation_id

        await asyncio.sleep(0)

        raise CustomOperationError("async failed")

    with pytest.raises(CustomOperationError, match="async failed"):
        await async_failing_operation(
            owner,
            operation_id="async-err",
        )

    logger_pack.reset_logger()

    content = log_file.read_text()

    assert "INFO: mvx.integration.async-owner.operation.async.fail [invoke]" in content
    assert "ERROR: mvx.integration.async-owner.operation.async.fail [failed]" in content

    assert "'operation_id': 'async-err'" in content
    assert "'error': {'kind': 'CustomOperationError', 'message': 'async failed'}" in content


@pytest.mark.asyncio
async def test_b03_async_invocation_writes_cancelled_event_to_file_and_reraises(
    tmp_path: Path,
) -> None:
    log_file, ctx = configure_file_logging(tmp_path)
    owner = InvocationOwner(ctx, identity="async-owner")

    @logger_pack.log_invocation(
        "operation.async.cancel",
        invoke_level=LogLevel.INFO,
        context_fields=("operation_id",),
    )
    async def async_cancelled_operation(
        self: InvocationOwner,
        operation_id: str,
    ) -> None:
        _ = self
        _ = operation_id

        await asyncio.sleep(0)

        raise asyncio.CancelledError("async cancelled")

    with pytest.raises(asyncio.CancelledError):
        await async_cancelled_operation(
            owner,
            operation_id="async-cancel",
        )

    logger_pack.reset_logger()

    content = log_file.read_text()

    assert "INFO: mvx.integration.async-owner.operation.async.cancel [invoke]" in content
    assert "INFO: mvx.integration.async-owner.operation.async.cancel [cancelled]" in content

    assert "'operation_id': 'async-cancel'" in content
    assert "'cancelled': True" in content
    assert "'error': {'kind': 'CancelledError', 'message': 'async cancelled'}" in content


# ==== C. Explicit context through package logger ==========================================


def test_c01_log_invocation_uses_explicit_real_context_without_owner_provider(
    tmp_path: Path,
) -> None:
    log_file, ctx = configure_file_logging(tmp_path)

    @logger_pack.log_invocation(
        "operation.explicit",
        ctx=ctx,
        invoke_level=LogLevel.INFO,
        success_level=LogLevel.INFO,
        log_kwargs_on_invoke=("value",),
        log_result_on_success=(),
    )
    def explicit_context_operation(value: int) -> str:
        _ = value
        return "ok"

    assert explicit_context_operation(42) == "ok"

    logger_pack.reset_logger()

    content = log_file.read_text()

    assert "INFO: mvx.integration.operation.explicit [invoke]" in content
    assert "INFO: mvx.integration.operation.explicit [success]" in content

    assert "'kwargs': {'value': 42}" in content
    assert "'result': 'ok'" in content
