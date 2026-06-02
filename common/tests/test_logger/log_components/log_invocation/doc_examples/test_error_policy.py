from __future__ import annotations

import pytest

from mvx.common.logger import LogContextProto, LogEvent, LogLevel, log_invocation

# ---------- Example code ----------


class ConfigLoader:
    def __init__(self, log_context: LogContextProto) -> None:
        self._log_context = log_context

    def get_log_context(self) -> LogContextProto | None:
        return self._log_context

    @log_invocation(
        "load_optional_config",
        error_level_suppressed=LogLevel.INFO,
        log_error_policy=((FileNotFoundError, False),),
    )
    def load_optional_config(self, path: str) -> dict[str, object]:
        raise FileNotFoundError(path)


# ---------- Tests ----------


def test_error_policy_can_emit_suppressed_failed_outcome(
    log_context: LogContextProto,
    log_events: list[LogEvent],
) -> None:
    loader = ConfigLoader(log_context)

    with pytest.raises(FileNotFoundError):
        loader.load_optional_config("/tmp/missing.json")

    assert len(log_events) == 2

    invoke_event = log_events[0]
    assert invoke_event.level == LogLevel.DEBUG
    assert invoke_event.meta.event_namespace == "example"
    assert invoke_event.meta.event_name == "load_optional_config"
    assert invoke_event.meta.entity_id is None
    assert invoke_event.event_outcome == "invoke"
    assert dict(invoke_event.payload) == {}

    failed_event = log_events[1]
    assert failed_event.level == LogLevel.INFO
    assert failed_event.meta.event_namespace == "example"
    assert failed_event.meta.event_name == "load_optional_config"
    assert failed_event.meta.entity_id is None
    assert failed_event.event_outcome == "failed"
    assert dict(failed_event.payload) == {}
