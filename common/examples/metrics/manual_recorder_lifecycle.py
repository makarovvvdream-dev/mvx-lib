from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pprint import pprint
from typing import Any, Mapping

from mvx.common.metrics import (
    AsyncioMetricsRecorder,
    AsyncioMetricsRecorderLoopUnavailableError,
    Metric,
    MetricEvent,
)


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


def create_recorder_without_running_loop() -> None:
    try:
        AsyncioMetricsRecorder("document_storage")
    except AsyncioMetricsRecorderLoopUnavailableError:
        print("recorder creation failed: no running asyncio event loop")


async def run_manual_recorder_example() -> Mapping[str, Mapping[str, Any]]:
    recorder = AsyncioMetricsRecorder("document_storage")

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

        return recorder.get_metric_snapshots()

    except Exception:
        stop_result = await recorder.stop()
        if not stop_result.success:
            assert stop_result.error is not None
            raise stop_result.error
        raise


async def main() -> None:
    snapshots = await run_manual_recorder_example()
    pprint(snapshots)


if __name__ == "__main__":
    import asyncio

    create_recorder_without_running_loop()
    asyncio.run(main())
