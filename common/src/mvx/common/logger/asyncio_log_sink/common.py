# src/mvx/common/logger/asyncio_log_sink/common.py
from __future__ import annotations
from mvx.common.helpers.document_enum import document_enum

from enum import StrEnum

__all__ = ("AsyncioLogSinkState",)


@document_enum
class AsyncioLogSinkState(StrEnum):
    """
    Lifecycle state of an `AsyncioLogSink` instance.

    The state describes whether the sink has not been started yet, is starting,
    is running, is stopping, has stopped normally, or has reached a terminal
    error state.
    """

    #: The sink has been created but has not started yet.
    VIRGIN = "VIRGIN"

    #: Startup is in progress.
    STARTING = "STARTING"

    #: The sink is running and can accept events.
    RUNNING = "RUNNING"

    #: Shutdown is in progress.
    STOPPING = "STOPPING"

    #: The sink has stopped normally.
    STOPPED = "STOPPED"

    #: The sink has entered a terminal failure state.
    FAILURE = "FAILURE"

    #: The dispatching task was cancelled unexpectedly.
    CANCELLED = "CANCELLED"
