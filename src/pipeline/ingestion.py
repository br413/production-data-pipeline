"""Incremental, idempotent API-style ingestion with checkpoint persistence."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class EventRecord:
    """Normalized event from an external source."""

    event_id: str
    occurred_at: str
    payload: dict


@dataclass
class Checkpoint:
    """Tracks the last successfully processed cursor."""

    cursor: str
    processed_ids: set[str]

    def to_dict(self) -> dict:
        return {"cursor": self.cursor, "processed_ids": sorted(self.processed_ids)}

    @classmethod
    def from_dict(cls, data: dict) -> "Checkpoint":
        return cls(cursor=data.get("cursor", ""), processed_ids=set(data.get("processed_ids", [])))


def load_checkpoint(path: Path) -> Checkpoint:
    if not path.exists():
        return Checkpoint(cursor="", processed_ids=set())
    with path.open(encoding="utf-8") as handle:
        return Checkpoint.from_dict(json.load(handle))


def save_checkpoint(path: Path, checkpoint: Checkpoint) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(checkpoint.to_dict(), handle, indent=2)


def fetch_sample_events(cursor: str) -> tuple[list[EventRecord], str]:
    """Synthetic paginated source for local development and tests."""
    pages = {
        "": [
            EventRecord("evt-001", "2026-07-01T10:00:00Z", {"value": 10}),
            EventRecord("evt-002", "2026-07-01T10:05:00Z", {"value": 20}),
        ],
        "page-2": [
            EventRecord("evt-003", "2026-07-01T10:10:00Z", {"value": 30}),
        ],
    }
    next_cursor = {"": "page-2", "page-2": ""}.get(cursor, "")
    return pages.get(cursor, []), next_cursor


def ingest_incremental(
    fetch_page,
    checkpoint_path: Path,
    *,
    max_pages: int | None = None,
) -> list[EventRecord]:
    """Load new records while preserving idempotency across runs."""
    checkpoint = load_checkpoint(checkpoint_path)
    ingested: list[EventRecord] = []
    cursor = checkpoint.cursor
    pages_read = 0

    while True:
        if max_pages is not None and pages_read >= max_pages:
            break

        records, next_cursor = fetch_page(cursor)
        pages_read += 1

        for record in records:
            if record.event_id in checkpoint.processed_ids:
                continue
            ingested.append(record)
            checkpoint.processed_ids.add(record.event_id)

        if not next_cursor or next_cursor == cursor:
            checkpoint.cursor = cursor if records else next_cursor
            break

        cursor = next_cursor
        checkpoint.cursor = cursor

    save_checkpoint(checkpoint_path, checkpoint)
    return ingested


def main() -> None:
    parser = argparse.ArgumentParser(description="Run sample incremental ingestion.")
    parser.add_argument("--source", default="sample", choices=["sample"])
    parser.add_argument("--checkpoint", type=Path, required=True)
    args = parser.parse_args()

    if args.source != "sample":
        raise SystemExit(f"Unsupported source: {args.source}")

    records = ingest_incremental(fetch_sample_events, args.checkpoint)
    print(json.dumps([record.__dict__ for record in records], indent=2))


if __name__ == "__main__":
    main()
