# tests/test_logger/adapter_logging/test_log_record_factory.py
from __future__ import annotations

from typing import Any
from collections.abc import Iterator, Mapping

import logging

import pytest

from mvx.common.logger.models import LogEvent

from mvx.common.logger.adapter_logging.log_record_factory import (
    make_log_record_from_event,
)

# ---- Test helpers ------------------------------------------------------------------------


def make_event(
    *,
    level: int = logging.INFO,
    event_namespace: str = "mvx.ldap",
    event_name: str = "bind.success",
    event_type: str = "operation",
    timestamp: float = 1_700_000_000.123,
    entity_id: str = "conn-1",
    payload: Mapping[str, Any] | None = None,
    source_path: str = "/tmp/source.py",
    source_line: int = 42,
    source_func: str = "run_operation",
) -> LogEvent:
    return LogEvent(
        level=level,
        event_namespace=event_namespace,
        event_name=event_name,
        event_type=event_type,
        timestamp=timestamp,
        entity_id=entity_id,
        payload=payload if payload is not None else {"result": "ok"},
        source_path=source_path,
        source_line=source_line,
        source_func=source_func,
    )


def get_record_extra(record: logging.LogRecord, key: str) -> Any:
    return record.__dict__[key]


# ---- A. Standard LogRecord fields --------------------------------------------------------


def test_a01_record_name_is_logger_name() -> None:
    event = make_event()

    record = make_log_record_from_event("mvx.test.logger", event)

    assert record.name == "mvx.test.logger"


def test_a02_record_level_is_event_level() -> None:
    event = make_event(level=logging.ERROR)

    record = make_log_record_from_event("mvx.test.logger", event)

    assert record.levelno == logging.ERROR
    assert record.levelname == "ERROR"


def test_a03_record_pathname_is_event_source_path() -> None:
    event = make_event(source_path="/project/app.py")

    record = make_log_record_from_event("mvx.test.logger", event)

    assert record.pathname == "/project/app.py"


def test_a04_record_lineno_is_event_source_line() -> None:
    event = make_event(source_line=777)

    record = make_log_record_from_event("mvx.test.logger", event)

    assert record.lineno == 777


def test_a05_record_func_name_is_event_source_func() -> None:
    event = make_event(source_func="some_func")

    record = make_log_record_from_event("mvx.test.logger", event)

    assert record.funcName == "some_func"


def test_a06_record_args_is_empty_tuple() -> None:
    event = make_event()

    record = make_log_record_from_event("mvx.test.logger", event)

    assert record.args == ()


def test_a07_record_exc_info_is_none() -> None:
    event = make_event()

    record = make_log_record_from_event("mvx.test.logger", event)

    assert record.exc_info is None


def test_a08_record_stack_info_is_none() -> None:
    event = make_event()

    record = make_log_record_from_event("mvx.test.logger", event)

    assert record.stack_info is None


# ---- B. Message construction -------------------------------------------------------------


def test_b01_message_contains_namespace_entity_event_and_type() -> None:
    event = make_event()

    record = make_log_record_from_event("mvx.test.logger", event)

    assert record.msg == "mvx.ldap.conn-1.bind.success [operation]"
    assert record.getMessage() == "mvx.ldap.conn-1.bind.success [operation]"


def test_b02_message_uses_custom_entity_id() -> None:
    event = make_event(entity_id="entity-42")

    record = make_log_record_from_event("mvx.test.logger", event)

    assert record.msg == "mvx.ldap.entity-42.bind.success [operation]"


def test_b03_message_uses_custom_event_type() -> None:
    event = make_event(event_type="lifecycle")

    record = make_log_record_from_event("mvx.test.logger", event)

    assert record.msg == "mvx.ldap.conn-1.bind.success [lifecycle]"


def test_b04_message_uses_custom_event_name() -> None:
    event = make_event(event_name="search.done")

    record = make_log_record_from_event("mvx.test.logger", event)

    assert record.msg == "mvx.ldap.conn-1.search.done [operation]"


def test_b05_message_uses_event_namespace_not_logger_name() -> None:
    event = make_event(event_namespace="event.ns")

    record = make_log_record_from_event("logger.name", event)

    assert record.name == "logger.name"
    assert record.msg.startswith("event.ns.")


# ---- C. Timestamp mapping ----------------------------------------------------------------


def test_c01_record_created_is_event_timestamp() -> None:
    event = make_event(timestamp=1_700_000_123.456)

    record = make_log_record_from_event("mvx.test.logger", event)

    assert record.created == 1_700_000_123.456


def test_c02_record_msecs_is_fractional_milliseconds() -> None:
    event = make_event(timestamp=1000.125)

    record = make_log_record_from_event("mvx.test.logger", event)

    assert record.msecs == 125.0


def test_c03_record_msecs_is_zero_for_integer_timestamp() -> None:
    event = make_event(timestamp=1000.0)

    record = make_log_record_from_event("mvx.test.logger", event)

    assert record.msecs == 0.0


def test_c04_asctime_uses_event_timestamp() -> None:
    event = make_event(timestamp=1_700_000_000.0)
    formatter = logging.Formatter("%(asctime)s", datefmt="%Y")

    record = make_log_record_from_event("mvx.test.logger", event)

    assert formatter.format(record) == "2023"


# ---- D. Custom event fields --------------------------------------------------------------


def test_d01_record_namespace_custom_field_is_event_namespace() -> None:
    event = make_event(event_namespace="mvx.custom")

    record = make_log_record_from_event("mvx.test.logger", event)

    assert get_record_extra(record, "event_namespace") == "mvx.custom"


def test_d02_record_event_name_custom_field_is_event_name() -> None:
    event = make_event(event_name="search.done")

    record = make_log_record_from_event("mvx.test.logger", event)

    assert get_record_extra(record, "event_name") == "search.done"


def test_d03_record_event_type_custom_field_is_event_type() -> None:
    event = make_event(event_type="lifecycle")

    record = make_log_record_from_event("mvx.test.logger", event)

    assert get_record_extra(record, "event_type") == "lifecycle"


def test_d04_record_entity_id_custom_field_is_entity_id() -> None:
    event = make_event(entity_id="entity-42")

    record = make_log_record_from_event("mvx.test.logger", event)

    assert get_record_extra(record, "entity_id") == "entity-42"


def test_d05_record_payload_is_copied_to_plain_dict() -> None:
    payload = {"a": 1, "b": 2}
    event = make_event(payload=payload)

    record = make_log_record_from_event("mvx.test.logger", event)
    record_payload = get_record_extra(record, "payload")

    assert record_payload == payload
    assert record_payload is not payload


def test_d06_record_payload_accepts_custom_mapping() -> None:
    class CustomMapping(Mapping[str, Any]):
        def __init__(self) -> None:
            self._data = {"x": 1}

        def __getitem__(self, key: str) -> Any:
            return self._data[key]

        def __iter__(self) -> Iterator[str]:
            return iter(self._data)

        def __len__(self) -> int:
            return len(self._data)

    event = make_event(payload=CustomMapping())

    record = make_log_record_from_event("mvx.test.logger", event)
    record_payload = get_record_extra(record, "payload")

    assert record_payload == {"x": 1}
    assert isinstance(record_payload, dict)


# ---- E. Formatter compatibility ----------------------------------------------------------


def test_e01_default_format_can_format_record() -> None:
    event = make_event(payload={"result": "ok"})
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s: " "%(event_name)s [%(event_type)s:%(entity_id)s] %(payload)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    record = make_log_record_from_event("mvx.test.logger", event)

    formatted = formatter.format(record)

    assert "INFO" in formatted
    assert "bind.success" in formatted
    assert "[operation:conn-1]" in formatted
    assert "{'result': 'ok'}" in formatted


def test_e02_format_can_use_standard_logger_name_field() -> None:
    event = make_event()
    formatter = logging.Formatter("%(name)s:%(message)s")

    record = make_log_record_from_event("logger.name", event)

    formatted = formatter.format(record)

    assert formatted.startswith("logger.name:")


def test_e03_format_can_use_custom_namespace_field() -> None:
    event = make_event(event_namespace="event.namespace")
    formatter = logging.Formatter("%(event_namespace)s:%(message)s")

    record = make_log_record_from_event("logger.name", event)

    formatted = formatter.format(record)

    assert formatted.startswith("event.namespace:")


def test_e04_format_can_use_source_fields() -> None:
    event = make_event(
        source_line=123,
        source_func="func_name",
    )
    formatter = logging.Formatter("%(pathname)s:%(lineno)d:%(funcName)s:%(message)s")

    record = make_log_record_from_event("logger.name", event)

    formatted = formatter.format(record)

    assert formatted.startswith("/tmp/source.py:123:func_name:")


# ---- F. Multiple records independence ----------------------------------------------------


def test_f01_two_records_have_independent_payload_dicts() -> None:
    payload = {"x": 1}
    event = make_event(payload=payload)

    first = make_log_record_from_event("logger.name", event)
    second = make_log_record_from_event("logger.name", event)

    first_payload = get_record_extra(first, "payload")
    second_payload = get_record_extra(second, "payload")

    assert first_payload == second_payload == {"x": 1}
    assert first_payload is not second_payload


def test_f02_mutating_record_payload_does_not_mutate_event_payload() -> None:
    payload = {"x": 1}
    event = make_event(payload=payload)

    record = make_log_record_from_event("logger.name", event)
    record_payload = get_record_extra(record, "payload")

    record_payload["x"] = 2

    assert payload == {"x": 1}


def test_f03_records_from_different_events_have_different_messages() -> None:
    first_event = make_event(event_name="first")
    second_event = make_event(event_name="second")

    first = make_log_record_from_event("logger.name", first_event)
    second = make_log_record_from_event("logger.name", second_event)

    assert first.getMessage().endswith(".first [operation]")
    assert second.getMessage().endswith(".second [operation]")


# ---- G. Level handling -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("level", "levelname"),
    [
        (logging.DEBUG, "DEBUG"),
        (logging.INFO, "INFO"),
        (logging.WARNING, "WARNING"),
        (logging.ERROR, "ERROR"),
        (logging.CRITICAL, "CRITICAL"),
    ],
)
def test_g01_record_level_fields_for_standard_levels(level: int, levelname: str) -> None:
    event = make_event(level=level)

    record = make_log_record_from_event("logger.name", event)

    assert record.levelno == level
    assert record.levelname == levelname


def test_g02_record_accepts_custom_numeric_level() -> None:
    event = make_event(level=35)

    record = make_log_record_from_event("logger.name", event)

    assert record.levelno == 35
    assert record.levelname == "Level 35"
