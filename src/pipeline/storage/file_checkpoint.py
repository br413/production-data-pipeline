"""File-based checkpoint store for local development."""

from __future__ import annotations

import json
from pathlib import Path

from src.pipeline.models import Checkpoint


class FileCheckpointStore:
    def __init__(self, path: Path) -> None:
        self._path = path

    def load(self) -> Checkpoint:
        if not self._path.exists():
            return Checkpoint(cursor="", processed_ids=set())
        with self._path.open(encoding="utf-8") as handle:
            data = json.load(handle)
        return Checkpoint(
            cursor=data.get("cursor", ""),
            processed_ids=set(data.get("processed_ids", [])),
        )

    def save(self, checkpoint: Checkpoint) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "cursor": checkpoint.cursor,
            "processed_ids": sorted(checkpoint.processed_ids),
        }
        with self._path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
