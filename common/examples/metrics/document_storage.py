from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Mapping

from pprint import pprint

from mvx.common.metrics import (
    Metric,
    MetricEvent,
    MetricsRecorderProto,
    MetricsRuntime,
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


class DocumentStorage:
    def __init__(
        self,
        *,
        metrics_recorder: MetricsRecorderProto | None = None,
    ) -> None:
        self._metrics_recorder = metrics_recorder
        self._register_metrics()

    def _register_metrics(self) -> None:
        if self._metrics_recorder is None:
            return

        metrics = (DocumentSaveAttemptsMetric(),)

        for metric in metrics:
            try:
                self._metrics_recorder.register_metric(metric=metric)
            except Exception:
                pass

    def _send_metric_event(self, event: MetricEvent) -> None:
        if self._metrics_recorder is None:
            return

        try:
            self._metrics_recorder.register_event(event=event)
        except Exception:
            pass

    def save_document(self, document_id: str, content: str) -> None:
        try:
            if not document_id:
                raise ValueError("document_id must not be empty")

            if not content:
                raise ValueError("content must not be empty")

        except Exception:
            self._send_metric_event(
                DocumentSaveAttemptMetricEvent(
                    outcome=DocumentSaveAttemptOutcome.FAILURE,
                )
            )
            raise

        self._send_metric_event(
            DocumentSaveAttemptMetricEvent(
                outcome=DocumentSaveAttemptOutcome.SUCCESS,
            )
        )


def main() -> None:
    runtime = MetricsRuntime(namespace="example.metrics")
    runtime.start()

    try:
        recorder = runtime.create_recorder("document_storage")

        storage = DocumentStorage(metrics_recorder=recorder)

        storage.save_document("doc-001", "First document")
        storage.save_document("doc-002", "Second document")

        try:
            storage.save_document("", "Broken document")
        except ValueError:
            pass

        snapshots = recorder.get_metric_snapshots()
        pprint(snapshots)

    finally:
        runtime.shutdown()


if __name__ == "__main__":
    main()
