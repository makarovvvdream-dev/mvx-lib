from __future__ import annotations

from typing import Any

import pytest

from mvx.common.logger import LogContextProto, LogEvent, LogLevel, log_invocation

# ---------- Example code ----------


def connection_payload(
    ctx: LogContextProto,
    event_outcome: object,
    event: str,
    fields: dict[str, Any],
) -> dict[str, Any]:
    return {
        "connection": {
            "event": event,
            "outcome": str(event_outcome),
            "state": ctx.normalize_value_for_log(fields.get("state")),
            "peer": ctx.normalize_value_for_log(fields.get("peer")),
        }
    }


class Connection:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context
        self.state = "closed"
        self.peer = "ldap.example.local"

    def get_log_context(self) -> LogContextProto:
        return self._log_context

    @log_invocation(
        "open",
        context_fields=("state=self.state", "peer=self.peer"),
        context_formatter=connection_payload,
    )
    async def open(self) -> None:
        self.state = "opened"


# ---------- Tests ----------


@pytest.mark.asyncio
async def test_context_formatter_controls_context_payload_shape(
    log_context: LogContextProto,
    log_events: list[LogEvent],
) -> None:
    connection = Connection(log_context)

    await connection.open()

    assert len(log_events) == 2

    invoke_event = log_events[0]
    assert invoke_event.level == LogLevel.DEBUG
    assert invoke_event.meta.event_namespace == "example"
    assert invoke_event.meta.event_name == "open"
    assert invoke_event.meta.entity_id is None
    assert invoke_event.event_outcome == "invoke"
    assert dict(invoke_event.payload) == {
        "connection": {
            "event": "open",
            "outcome": "invoke",
            "state": "closed",
            "peer": "ldap.example.local",
        }
    }

    success_event = log_events[1]
    assert success_event.level == LogLevel.DEBUG
    assert success_event.meta.event_namespace == "example"
    assert success_event.meta.event_name == "open"
    assert success_event.meta.entity_id is None
    assert success_event.event_outcome == "success"
    assert dict(success_event.payload) == {
        "connection": {
            "event": "open",
            "outcome": "success",
            "state": "opened",
            "peer": "ldap.example.local",
        }
    }
