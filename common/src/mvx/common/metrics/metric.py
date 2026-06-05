# src/mvx/common/metrics/metric.py

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping

from .metric_event import MetricEvent

__all__ = ("Metric",)


class Metric(ABC):
    """
    Base class for metric value objects.

    A metric owns one measured value or a related group of measured values. It
    receives `MetricEvent` objects from a recorder and decides whether each event
    affects its internal state.

    Implementations are responsible for keeping their own state consistent and
    for returning a snapshot that can be consumed by monitoring, logging, tests,
    or diagnostics.
    """

    @property
    @abstractmethod
    def metric_name(self) -> str:
        """
        Return the stable metric name.

        The name identifies this metric inside a recorder and should not depend
        on the current metric value.

        :return: stable metric name.
        """
        raise NotImplementedError

    @abstractmethod
    def handle_event(self, event: MetricEvent) -> bool:
        """
        Apply a metric event to this metric.

        The recorder calls this method for registered metrics when a new
        `MetricEvent` is dispatched. The method should update internal state only
        when the event is relevant to this metric.

        :param event: metric event to apply.
        :return: True if the metric value changed, False otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def snapshot(self) -> Mapping[str, Any]:
        """
        Return a serializable snapshot of the current metric value.

        The returned mapping should contain log- and diagnostics-friendly values
        that describe the current metric state.

        :return: current metric snapshot.
        """
        raise NotImplementedError
