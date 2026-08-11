"""JSONL quarantine store for file-based local ingestion."""

from __future__ import annotations

import json
from pathlib import Path

from src.pipeline.models import EventRecord


class FileQuarantineStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def persist(
        self,
        record: EventRecord,
        *,
        failed_rule: str,
        failure_message: str,
        pipeline_name: str,
        run_id: str,
    ) -> None:
        payload = {
            "event_id": record.event_id,
            "occurred_at": record.occurred_at,
            "payload": record.payload,
            "failed_rule": failed_rule,
            "failure_message": failure_message,
            "pipeline_name": pipeline_name,
            "run_id": run_id,
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload) + "\n")
