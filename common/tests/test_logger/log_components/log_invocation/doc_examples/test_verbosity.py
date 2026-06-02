from __future__ import annotations

from mvx.common.logger import (
    LogContext,
    LogContextProto,
    LogEvent,
    LogLevel,
    LogPayloadProcessor,
    LogVerbosityLevel,
    log_invocation,
)

# ---------- Example code ----------


class Client:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context

    @log_invocation(
        "send_request",
        log_kwargs_on_invoke=(
            "request_id",
            "NORMAL,MAXIMUM:method",
            "MAXIMUM:payload_size=payload.len()",
        ),
    )
    def send_request(self, request_id: str, method: str, payload: bytes) -> None: ...


# ---------- Tests ----------


class ListLogSink:
    def __init__(self, events: list[LogEvent]) -> None:
        self._events = events

    def log(self, event: LogEvent) -> None:
        self._events.append(event)


def make_context_with_verbosity(
    verbosity_level: LogVerbosityLevel,
) -> tuple[LogContext, list[LogEvent]]:
    events: list[LogEvent] = []
    sink = ListLogSink(events)

    ctx = LogContext(
        namespace="example",
        log_sink=sink,
        payload_processor=LogPayloadProcessor(
            verbosity_level=verbosity_level,
        ),
    )

    return ctx, events


def test_verbosity_gates_select_minimal_fields() -> None:
    log_context, log_events = make_context_with_verbosity(LogVerbosityLevel.MINIMAL)

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
        },
    }

    success_event = log_events[1]
    assert success_event.event_outcome == "success"
    assert dict(success_event.payload) == {}


def test_verbosity_gates_select_normal_fields() -> None:
    log_context, log_events = make_context_with_verbosity(LogVerbosityLevel.NORMAL)

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
        },
    }

    success_event = log_events[1]
    assert success_event.event_outcome == "success"
    assert dict(success_event.payload) == {}


def test_verbosity_gates_select_maximum_fields() -> None:
    log_context, log_events = make_context_with_verbosity(LogVerbosityLevel.MAXIMUM)

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
    assert success_event.event_outcome == "success"
    assert dict(success_event.payload) == {}
