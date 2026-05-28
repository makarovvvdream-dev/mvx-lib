from __future__ import annotations

from collections.abc import Awaitable

import pytest

from mvx.common.logger import LogContextProto, LogEvent, LogLevel, log_invocation

# ---------- Example code ----------


class Client:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto:
        return self._log_context

    @log_invocation(
        "send",
        log_result_on_success=(),
    )
    def send(self) -> Awaitable[str]:
        async def run() -> str:
            return "ok"

        return run()


# ---------- Tests ----------


@pytest.mark.asyncio
async def test_sync_function_returning_awaitable_logs_final_awaited_outcome(
    log_context: LogContextProto,
    log_events: list[LogEvent],
) -> None:
    client = Client(log_context)

    result_awaitable = client.send()

    assert len(log_events) == 1

    invoke_event = log_events[0]
    assert invoke_event.level == LogLevel.DEBUG
    assert invoke_event.meta.event_namespace == "example"
    assert invoke_event.meta.event_name == "send"
    assert invoke_event.meta.entity_id is None
    assert invoke_event.event_outcome == "invoke"
    assert dict(invoke_event.payload) == {}

    result = await result_awaitable

    assert result == "ok"

    assert len(log_events) == 2

    success_event = log_events[1]
    assert success_event.level == LogLevel.DEBUG
    assert success_event.meta.event_namespace == "example"
    assert success_event.meta.event_name == "send"
    assert success_event.meta.entity_id is None
    assert success_event.event_outcome == "success"
    assert dict(success_event.payload) == {
        "result": "ok",
    }
