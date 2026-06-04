# src/mvx/common/metrics/metric_event.py

from __future__ import annotations

from abc import ABC, abstractmethod

__all__ = ("MetricEvent",)


class MetricEvent(ABC):
    """
    Base class for metric events.

    A metric event describes something that happened and may affect one or more
    metric values.
    """

    @property
    @abstractmethod
    def event_type(self) -> str:
        """
        Stable event type.
        """
        raise NotImplementedError
