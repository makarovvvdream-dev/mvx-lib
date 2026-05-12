# src/mvx/common/logger/asyncio_log_sink/common.py
from __future__ import annotations
from enum import StrEnum

__all__ = ("AsyncioLogSinkState",)


class AsyncioLogSinkState(StrEnum):
    VIRGIN = "VIRGIN"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILURE = "FAILURE"
    CANCELLED = "CANCELLED"
