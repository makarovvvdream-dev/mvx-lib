# src/mvx/common/metrics/metric_event.py

from __future__ import annotations

from abc import ABC, abstractmethod

__all__ = ("MetricEvent",)


class MetricEvent(ABC):
    """
    Base class for metric event objects.

    A metric event describes something that happened in runtime code and may
    affect one or more registered metrics. Recorders dispatch these events to
    metrics, while metrics decide whether a particular event is relevant to their
    own state.
    """

    @property
    @abstractmethod
    def event_type(self) -> str:
        """
        Return the stable event type.

        The event type identifies the kind of runtime occurrence represented by
        this event. Metrics use it to decide whether and how the event should
        affect their values.

        :return: stable event type.
        """
        raise NotImplementedError
