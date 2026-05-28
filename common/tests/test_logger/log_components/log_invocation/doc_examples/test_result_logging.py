from __future__ import annotations

from dataclasses import dataclass

from mvx.common.logger import LogContextProto, LogEvent, LogLevel, log_invocation

# ---------- Example code ----------


@dataclass(frozen=True, slots=True)
class Session:
    session_id: str
    ttl_ms: int


class SessionService:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto:
        return self._log_context

    @log_invocation(
        "create_session",
        log_result_on_success=("session_id", "ttl_ms"),
    )
    def create_session(self, user_id: str) -> Session:
        return Session(session_id=f"session-{user_id}", ttl_ms=30_000)


# ---------- Tests ----------


def test_selected_result_fields_are_logged_on_success(
    log_context: LogContextProto,
    log_events: list[LogEvent],
) -> None:
    service = SessionService(log_context)

    result = service.create_session("u1")

    assert result == Session(session_id="session-u1", ttl_ms=30_000)

    assert len(log_events) == 2

    invoke_event = log_events[0]
    assert invoke_event.level == LogLevel.DEBUG
    assert invoke_event.meta.event_namespace == "example"
    assert invoke_event.meta.event_name == "create_session"
    assert invoke_event.meta.entity_id is None
    assert invoke_event.event_outcome == "invoke"
    assert dict(invoke_event.payload) == {}

    success_event = log_events[1]
    assert success_event.level == LogLevel.DEBUG
    assert success_event.meta.event_namespace == "example"
    assert success_event.meta.event_name == "create_session"
    assert success_event.meta.entity_id is None
    assert success_event.event_outcome == "success"
    assert dict(success_event.payload) == {
        "result": {
            "session_id": "session-u1",
            "ttl_ms": 30_000,
        },
    }
