from __future__ import annotations

import pytest

from mvx.common.logger import LogContextProto, LogEvent, LogLevel, log_invocation

# ---------- Example code ----------


def make_connector(log_context: LogContextProto, target: str):
    @log_invocation(
        "connect",
        ctx=log_context,
        log_closures_on_invoke={"target": target},
    )
    async def connect() -> None: ...

    return connect


# ---------- Tests ----------


@pytest.mark.asyncio
async def test_standalone_function_uses_explicit_context_and_closure_values(
    log_context: LogContextProto,
    log_events: list[LogEvent],
) -> None:
    connect = make_connector(log_context, target="ldap")

    await connect()

    assert len(log_events) == 2

    invoke_event = log_events[0]
    assert invoke_event.level == LogLevel.DEBUG
    assert invoke_event.meta.event_namespace == "example"
    assert invoke_event.meta.event_name == "connect"
    assert invoke_event.meta.entity_id is None
    assert invoke_event.event_outcome == "invoke"
    assert dict(invoke_event.payload) == {
        "closures": {
            "target": "ldap",
        },
    }

    success_event = log_events[1]
    assert success_event.level == LogLevel.DEBUG
    assert success_event.meta.event_namespace == "example"
    assert success_event.meta.event_name == "connect"
    assert success_event.meta.entity_id is None
    assert success_event.event_outcome == "success"
    assert dict(success_event.payload) == {}
