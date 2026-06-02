from __future__ import annotations

import asyncio

import pytest

from mvx.common.logger import LogContextProto, LogEvent, LogLevel, log_invocation

# ---------- Example code ----------


class Worker:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context

    @log_invocation("run")
    async def run(self) -> None:
        raise asyncio.CancelledError()


# ---------- Tests ----------


@pytest.mark.asyncio
async def test_cancelled_operation_emits_cancelled_outcome(
    log_context: LogContextProto,
    log_events: list[LogEvent],
) -> None:
    worker = Worker(log_context)

    with pytest.raises(asyncio.CancelledError):
        await worker.run()

    assert len(log_events) == 2

    invoke_event = log_events[0]
    assert invoke_event.level == LogLevel.DEBUG
    assert invoke_event.meta.event_namespace == "example"
    assert invoke_event.meta.event_name == "run"
    assert invoke_event.meta.entity_id is None
    assert invoke_event.event_outcome == "invoke"
    assert dict(invoke_event.payload) == {}

    cancelled_event = log_events[1]
    assert cancelled_event.level == LogLevel.INFO
    assert cancelled_event.meta.event_namespace == "example"
    assert cancelled_event.meta.event_name == "run"
    assert cancelled_event.meta.entity_id is None
    assert cancelled_event.event_outcome == "cancelled"

    cancelled_payload = dict(cancelled_event.payload)
    assert cancelled_payload["cancelled"] is True
    assert "error" in cancelled_payload
