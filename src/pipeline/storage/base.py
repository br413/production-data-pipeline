"""Storage interfaces for ingestion pipelines."""

from __future__ import annotations

from typing import Protocol

from src.pipeline.models import Checkpoint, EventRecord


class CheckpointStore(Protocol):
    def load(self) -> Checkpoint: ...

    def save(self, checkpoint: Checkpoint) -> None: ...


class LandingStore(Protocol):
    def persist(self, records: list[EventRecord]) -> int: ...


class QuarantineStore(Protocol):
    def persist(
        self,
        record: EventRecord,
        *,
        failed_rule: str,
        failure_message: str,
        pipeline_name: str,
        run_id: str,
    ) -> None: ...
