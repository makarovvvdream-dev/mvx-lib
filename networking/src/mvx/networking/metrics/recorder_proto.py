# src/mvx/common/metrics/recorder_proto.py

from __future__ import annotations

from typing import Protocol, runtime_checkable, Iterable, Mapping, Any

from .metric import Metric
from .metric_event import MetricEvent

__all__ = ("MetricsRecorderProto",)


@runtime_checkable
class MetricsRecorderProto(Protocol):
    """
    Protocol for metric recorders.

    This is the narrow integration contract used by components that publish
    metrics.
    """

    def register_metric(self, metric: Metric) -> None:
        """
        Register a metric in the recorder.
        """
        ...

    def register_event(self, event: MetricEvent) -> None:
        """
        Register a metric event in the recorder.
        """
        ...

    def get_metric_snapshots(self) -> Mapping[str, Mapping[str, Any]]:
        """
        Return snapshots of registered metrics.
        """
        ...

    def iter_metrics(self) -> Iterable[Metric]:
        """
        Iterate over registered metrics.
        """
        ...
