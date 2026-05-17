# src/mvx/common/logger/adapter_logging/log_record_factory.py
"""
Helpers for adapting MVX log events to standard ``logging.LogRecord`` objects.

The module maps an already-built ``LogEvent`` to the record shape expected by
Python's standard logging handlers and formatters.

Message format
--------------
The standard ``LogRecord.message`` value is built from MVX event identity:

    event_namespace[.entity_id].event_name[ [event_type]]

Examples:

    mvx.ldap.conn-1.bind.success [operation]
    mvx.ldap.bind.success
    mvx.ldap.bind.success [operation]
    conn-1.bind.success [operation]
    bind.success

``event_namespace``, ``entity_id``, and ``event_type`` are included in the
message only when they are present on the source event. ``event_name`` is always
included.

Formatter fields
----------------
The resulting record supports all standard ``logging.LogRecord`` fields, plus
these MVX-specific fields:

    event_namespace
        Event namespace from ``LogEvent.meta.event_namespace`` or
        ``"<not defined>"``.

    event_name
        Event name from ``LogEvent.meta.event_name``.

    event_type
        Event type from ``LogEvent.event_type`` or ``"<not defined>"``.

    entity_id
        Entity identifier from ``LogEvent.meta.entity_id`` or ``"<not defined>"``.

    payload
        Plain ``dict`` copy of ``LogEvent.payload``.

Source mapping
--------------
Source location fields are mapped to standard ``logging.LogRecord`` fields:

    pathname
        Source path from ``LogEvent.meta.source_path`` or ``"<not defined>"``.

    lineno
        Source line from ``LogEvent.meta.source_line`` or ``-1``.

    funcName
        Source function from ``LogEvent.meta.source_func`` or
        ``"<not defined>"``.

Timestamp mapping
-----------------
``LogRecord.created`` and ``LogRecord.msecs`` are derived from
``LogEvent.timestamp``.
"""

from __future__ import annotations

import logging

from ..models import LogEvent

__all__ = ("make_log_record_from_event",)


def make_log_record_from_event(logger_name: str, event: LogEvent) -> logging.LogRecord:
    """
    Build a standard ``logging.LogRecord`` from an already-built MVX ``LogEvent``.

    The event is expected to be fully prepared by the caller. This function does
    not apply event policy and does not normalize the payload; it only adapts the
    event to the shape used by Python's standard logging package.

    Args:
        logger_name: Name assigned to the standard logging record.
        event: MVX log event to adapt.

    Returns:
        A ``logging.LogRecord`` carrying standard logging fields plus MVX-specific
        fields such as ``event_namespace``, ``event_name``, ``event_type``,
        ``entity_id``, and ``payload``.
    """
    msg_parts: list[str] = []

    if event.meta.event_namespace:
        msg_parts.append(event.meta.event_namespace)

    if event.meta.entity_id:
        msg_parts.append(event.meta.entity_id)

    msg_parts.append(event.meta.event_name)

    msg = ".".join(msg_parts)

    if event.event_type:
        msg += f" [{event.event_type}]"

    event_namespace = event.meta.event_namespace or "<not defined>"
    event_type = event.event_type or "<not defined>"
    entity_id = event.meta.entity_id or "<not defined>"
    source_path = event.meta.source_path or "<not defined>"
    source_line = event.meta.source_line or -1
    source_func = event.meta.source_func or "<not defined>"

    # noinspection PyArgumentEqualDefault
    record = logging.LogRecord(
        name=logger_name,
        level=int(event.level),
        pathname=source_path,
        lineno=source_line,
        func=source_func,
        msg=msg,
        args=(),
        exc_info=None,
        sinfo=None,
    )

    record.created = event.timestamp
    record.msecs = (event.timestamp - int(event.timestamp)) * 1000

    record.event_namespace = event_namespace
    record.event_name = event.meta.event_name
    record.event_type = event_type
    record.entity_id = entity_id
    record.payload = dict(event.payload)

    return record
