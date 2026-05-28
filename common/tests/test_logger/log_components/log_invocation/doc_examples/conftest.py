from __future__ import annotations

import pytest

from mvx.common.logger import LogContext, LogEvent, LogPayloadProcessor


class ListLogSink:
    def __init__(self, events: list[LogEvent]) -> None:
        self._events = events

    def log(self, event: LogEvent) -> None:
        self._events.append(event)


@pytest.fixture
def log_events() -> list[LogEvent]:
    return []


@pytest.fixture
def log_sink(log_events: list[LogEvent]) -> ListLogSink:
    return ListLogSink(log_events)


@pytest.fixture
def log_context(log_sink: ListLogSink) -> LogContext:
    return LogContext(
        namespace="example",
        log_sink=log_sink,
        payload_processor=LogPayloadProcessor(),
    )
