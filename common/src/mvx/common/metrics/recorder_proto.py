# src/mvx/common/metrics/recorder_proto.py

from __future__ import annotations

from typing import Protocol, runtime_checkable, Iterable, Mapping, Any

from .metric import Metric
from .metric_event import MetricEvent

__all__ = ("MetricsRecorderProto",)


@runtime_checkable
class MetricsRecorderProto(Protocol):
    """
    Protocol for metrics recorders.

    This protocol is the narrow integration contract used by runtime components
    that publish metric events. Components depend on this protocol instead of a
    concrete recorder implementation.

    A recorder owns registered metrics, accepts `MetricEvent` objects, dispatches
    events to metrics, and exposes metric snapshots for diagnostics, tests, or
    monitoring integrations.
    """

    def register_metric(self, metric: Metric) -> None:
        """
        Register a metric in the recorder.

        Registered metrics receive subsequently dispatched metric events. Metric
        names are expected to be stable within one recorder instance.

        :param metric: metric instance to register.
        :return: None.
        """
        ...

    def register_event(self, event: MetricEvent) -> None:
        """
        Register a metric event in the recorder.

        The recorder accepts the event for asynchronous or synchronous dispatch,
        depending on the concrete implementation.

        :param event: metric event to register.
        :return: None.
        """
        ...

    def get_metric_snapshots(self) -> Mapping[str, Mapping[str, Any]]:
        """
        Return snapshots of registered metrics.

        The outer mapping is keyed by metric name. Each inner mapping is the
        current snapshot returned by the corresponding metric.

        :return: mapping of metric names to metric snapshots.
        """
        ...

    def iter_metrics(self) -> Iterable[Metric]:
        """
        Iterate over registered metrics.

        :return: iterable of registered metric instances.
        """
        ...
