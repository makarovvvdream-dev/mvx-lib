from __future__ import annotations

from mvx.common.logger import LogContextProto, LogEvent, LogLevel, log_invocation

# ---------- Example code ----------


class Client:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto:
        return self._log_context

    @log_invocation(
        "send_request",
        log_kwargs_on_invoke=(
            "request_id",
            "method",
            "payload_size=payload.len()",
        ),
    )
    def send_request(self, request_id: str, method: str, payload: bytes) -> None: ...


# ---------- Tests ----------


def test_invoke_kwargs_are_logged_only_for_invoke_outcome(
    log_context: LogContextProto,
    log_events: list[LogEvent],
) -> None:
    client = Client(log_context)

    client.send_request(
        request_id="req-1",
        method="GET",
        payload=b"abc",
    )

    assert len(log_events) == 2

    invoke_event = log_events[0]
    assert invoke_event.level == LogLevel.DEBUG
    assert invoke_event.meta.event_namespace == "example"
    assert invoke_event.meta.event_name == "send_request"
    assert invoke_event.meta.entity_id is None
    assert invoke_event.event_outcome == "invoke"
    assert dict(invoke_event.payload) == {
        "kwargs": {
            "request_id": "req-1",
            "method": "GET",
            "payload_size": 3,
        },
    }

    success_event = log_events[1]
    assert success_event.level == LogLevel.DEBUG
    assert success_event.meta.event_namespace == "example"
    assert success_event.meta.event_name == "send_request"
    assert success_event.meta.entity_id is None
    assert success_event.event_outcome == "success"
    assert dict(success_event.payload) == {}
