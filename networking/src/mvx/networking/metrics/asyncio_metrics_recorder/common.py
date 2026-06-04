# src/mvx/networking/metrics/asyncio_metrics_recorder/common.py
from __future__ import annotations
from mvx.common.helpers.document_enum import document_enum

from enum import StrEnum

__all__ = ("AsyncioMetricsRecorderState",)


@document_enum
class AsyncioMetricsRecorderState(StrEnum):
    """
    Lifecycle state of an `AsyncioMetricsRecorder` instance.

    The state describes whether the recorder has not been started yet, is starting,
    is running, is stopping, has stopped normally, or has reached a terminal
    error state.
    """

    #: The recorder has been created but has not started yet.
    VIRGIN = "VIRGIN"

    #: Startup is in progress.
    STARTING = "STARTING"

    #: The recorder is running and can accept events.
    RUNNING = "RUNNING"

    #: Shutdown is in progress.
    STOPPING = "STOPPING"

    #: The recorder has stopped normally.
    STOPPED = "STOPPED"

    #: The recorder has entered a terminal failure state.
    FAILURE = "FAILURE"

    #: The dispatching task was cancelled unexpectedly.
    CANCELLED = "CANCELLED"
