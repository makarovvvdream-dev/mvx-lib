# src/mvx/common/metrics/metric.py

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping

from .metric_event import MetricEvent

__all__ = ("Metric",)


class Metric(ABC):
    """
    Base class for metric values.

    A metric owns its value and knows how metric events affect it.
    """

    @property
    @abstractmethod
    def metric_name(self) -> str:
        """
        Stable metric name.
        """
        raise NotImplementedError

    @abstractmethod
    def handle_event(self, event: MetricEvent) -> bool:
        """
        Apply a metric event.

        Returns True when this event changed the metric value.
        """
        raise NotImplementedError

    @abstractmethod
    def snapshot(self) -> Mapping[str, Any]:
        """
        Return a serializable metric snapshot.
        """
        raise NotImplementedError
