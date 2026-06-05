# src/mvx/common/metrics/metrics_runtime/common.py
from __future__ import annotations
from mvx.common.helpers.document_enum import document_enum

from enum import StrEnum

__all__ = ("MetricsRuntimeState",)


@document_enum
class MetricsRuntimeState(StrEnum):
    """
    Lifecycle states of the metrics runtime.

    The runtime owns the dedicated metrics thread, event loop, recorder registry,
    and lifecycle coordination for package-managed metrics recorders.
    """

    #: Runtime has been created but has not started yet.
    VIRGIN = "VIRGIN"

    #: Runtime startup is in progress.
    STARTING = "STARTING"

    #: Runtime is running and can create or manage recorders.
    RUNNING = "RUNNING"

    #: Runtime shutdown is in progress.
    STOPPING = "STOPPING"

    #: Runtime has been shut down and cannot be restarted.
    CLOSED = "CLOSED"

    #: Runtime entered a terminal failure state.
    FAILURE = "FAILURE"
