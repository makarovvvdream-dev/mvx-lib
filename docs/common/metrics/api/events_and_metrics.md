# Events and metrics

This page documents the base API for defining metric events and metrics.

## Public API

```{eval-rst}
.. autoclass:: mvx.common.metrics.MetricEvent
   :members:
   :member-order: bysource
   :class-doc-from: class

.. autoclass:: mvx.common.metrics.Metric
   :members:
   :member-order: bysource
   :class-doc-from: class
```

## Minimal event example

A metric event is usually a small immutable object with an `event_type` and domain payload.

```python
from dataclasses import dataclass
from enum import StrEnum

from mvx.common.metrics import MetricEvent


class DocumentSaveAttemptOutcome(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


@dataclass(frozen=True, slots=True)
class DocumentSaveAttemptMetricEvent(MetricEvent):
    outcome: DocumentSaveAttemptOutcome

    @property
    def event_type(self) -> str:
        return "document_storage.save.attempt"
```

## Minimal metric example

A metric accepts relevant events, updates its own state, and exposes a snapshot.

```python
from typing import Any, Mapping

from mvx.common.metrics import Metric, MetricEvent


class DocumentSaveAttemptsMetric(Metric):
    def __init__(self) -> None:
        self._total = 0
        self._success_total = 0
        self._failure_total = 0

    @property
    def metric_name(self) -> str:
        return "document_storage.save.attempts"

    def handle_event(self, event: MetricEvent) -> bool:
        if not isinstance(event, DocumentSaveAttemptMetricEvent):
            return False

        self._total += 1

        if event.outcome is DocumentSaveAttemptOutcome.SUCCESS:
            self._success_total += 1

        elif event.outcome is DocumentSaveAttemptOutcome.FAILURE:
            self._failure_total += 1

        return True

    def snapshot(self) -> Mapping[str, Any]:
        return {
            "name": self.metric_name,
            "dimensions": {
                "total": self._total,
                "success_total": self._success_total,
                "failure_total": self._failure_total,
            },
        }
```

## Contract notes

`MetricEvent.event_type` should be stable.

`Metric.metric_name` should be stable because recorder snapshots use it as the metric key.

`Metric.handle_event()` returns `True` only when the metric accepted the event and changed its state. Recorders use this
return value for post-change processing.

`Metric.snapshot()` is the public read surface of the metric state. External code should use snapshots instead of metric
internals.
