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


class InMemoryMetricChangePublisher:
    def __init__(self) -> None:
        self._is_open = False
        self._records: list[Mapping[str, Any]] = []

    @property
    def is_open(self) -> bool:
        return self._is_open

    @property
    def records(self) -> list[Mapping[str, Any]]:
        return list(self._records)

    async def open(self) -> None:
        self._is_open = True

    async def close(self) -> None:
        self._is_open = False

    async def publish(self, record: Mapping[str, Any]) -> None:
        if not self._is_open:
            raise RuntimeError("publisher is not open")

        self._records.append(record)


class PublishingMetricsRecorder(AsyncioMetricsRecorder):
    def __init__(
        self,
        entity_id: str,
        *,
        publisher: InMemoryMetricChangePublisher,
    ) -> None:
        super().__init__(entity_id)
        self._publisher = publisher

    async def _on_starting(self) -> None:
        await self._publisher.open()

    async def _on_stopped(self) -> None:
        await self._publisher.close()

    async def _on_metric_changed(
        self,
        *,
        metric: Metric,
        event: MetricEvent,
    ) -> None:
        await self._publisher.publish(
            {
                "entity_id": self.entity_id,
                "metric_name": metric.metric_name,
                "event_type": event.event_type,
                "snapshot": metric.snapshot(),
            }
        )


async def run_custom_recorder_example() -> Mapping[str, Any]:
    publisher = InMemoryMetricChangePublisher()
    recorder = PublishingMetricsRecorder(
        "document_storage",
        publisher=publisher,
    )

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
            "publisher_is_open": publisher.is_open,
            "published_records": publisher.records,
            "snapshots": recorder.get_metric_snapshots(),
        }

    except Exception:
        stop_result = await recorder.stop()
        if not stop_result.success:
            assert stop_result.error is not None
            raise stop_result.error
        raise


async def main() -> None:
    result = await run_custom_recorder_example()
    pprint(result)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
