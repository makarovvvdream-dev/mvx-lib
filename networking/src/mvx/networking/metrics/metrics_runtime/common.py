# src/mvx/networking/metrics/metrics_runtime/common.py
from __future__ import annotations
from mvx.common.helpers.document_enum import document_enum

from enum import StrEnum

__all__ = ("MetricsRuntimeState",)


@document_enum
class MetricsRuntimeState(StrEnum):
    VIRGIN = "VIRGIN"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    CLOSED = "CLOSED"
    FAILURE = "FAILURE"
