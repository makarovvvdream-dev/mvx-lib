from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pprint import pprint
from typing import Any, Mapping

from mvx.common.metrics import AsyncioMetricsRecorder, Metric, MetricEvent


class DocumentSaveAttemptOutcome(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


@dataclass(frozen=True, slots=True)
class DocumentSaveAttemptMetricEvent(MetricEvent):
    outcome: DocumentSaveAttemptOutcome

    @property
    def event_type(self) -> str:
        return "document_storage.save.attempt"


@dataclass(frozen=True, slots=True)
class UnknownMetricEvent(MetricEvent):
    @property
    def event_type(self) -> str:
        return "unknown.event"


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


class ObservingMetricsRecorder(AsyncioMetricsRecorder):
    def __init__(self, entity_id: str) -> None:
        super().__init__(entity_id)
        self.metric_changes: list[Mapping[str, Any]] = []

    async def _on_metric_changed(
        self,
        *,
        metric: Metric,
        event: MetricEvent,
    ) -> None:
        self.metric_changes.append(
            {
                "metric_name": metric.metric_name,
                "event_type": event.event_type,
                "snapshot": metric.snapshot(),
            }
        )


async def run_metric_changed_hook_example() -> Mapping[str, Any]:
    recorder = ObservingMetricsRecorder("document_storage")

    start_result = await recorder.start()
    if not start_result.success:
        assert start_result.error is not None
        raise start_result.error

    try:
        recorder.register_metric(metric=DocumentSaveAttemptsMetric())

        recorder.register_event(
            event=DocumentSaveAttemptMetricEvent(
                outcome=DocumentSaveAttemptOutcome.SUCCESS,
            )
        )
        recorder.register_event(event=UnknownMetricEvent())
        recorder.register_event(
            event=DocumentSaveAttemptMetricEvent(
                outcome=DocumentSaveAttemptOutcome.SUCCESS,
            )
        )
        recorder.register_event(
            event=DocumentSaveAttemptMetricEvent(
                outcome=DocumentSaveAttemptOutcome.FAILURE,
            )
        )

        stop_result = await recorder.stop()
        if not stop_result.success:
            assert stop_result.error is not None
            raise stop_result.error

        return {
            "snapshots": recorder.get_metric_snapshots(),
            "metric_changes": recorder.metric_changes,
        }

    except Exception:
        stop_result = await recorder.stop()
        if not stop_result.success:
            assert stop_result.error is not None
            raise stop_result.error
        raise


async def main() -> None:
    result = await run_metric_changed_hook_example()
    pprint(result)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
