"""Incremental, idempotent API-style ingestion with checkpoint persistence."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from src.pipeline.models import Checkpoint, EventRecord
from src.pipeline.quality.validators import assert_quality_suite
from src.pipeline.storage.base import CheckpointStore, LandingStore
from src.pipeline.storage.file_checkpoint import FileCheckpointStore
from src.pipeline.storage.postgres import (
    DEFAULT_DATABASE_URL,
    PostgresCheckpointStore,
    PostgresLandingStore,
    init_schema,
)


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
    checkpoint_store: CheckpointStore,
    *,
    landing_store: LandingStore | None = None,
    max_pages: int | None = None,
    validate: bool = True,
) -> list[EventRecord]:
    """Load new records while preserving idempotency across runs."""
    checkpoint = checkpoint_store.load()
    ingested: list[EventRecord] = []
    cursor = checkpoint.cursor
    pages_read = 0

    while True:
        if max_pages is not None and pages_read >= max_pages:
            break

        records, next_cursor = fetch_page(cursor)
        pages_read += 1
        page_batch: list[EventRecord] = []

        for record in records:
            if record.event_id in checkpoint.processed_ids:
                continue
            page_batch.append(record)

        if page_batch:
            if validate:
                assert_quality_suite(page_batch)
            if landing_store is not None:
                landing_store.persist(page_batch)
            ingested.extend(page_batch)
            checkpoint.processed_ids.update(record.event_id for record in page_batch)

        if not next_cursor or next_cursor == cursor:
            checkpoint.cursor = cursor if records else next_cursor
            break

        cursor = next_cursor
        checkpoint.cursor = cursor

    checkpoint_store.save(checkpoint)
    return ingested


def build_stores(
    *,
    storage: str,
    checkpoint: Path | None,
    database_url: str,
    pipeline_name: str,
) -> tuple[CheckpointStore, LandingStore | None]:
    if storage == "file":
        if checkpoint is None:
            raise ValueError("--checkpoint is required when --storage file")
        return FileCheckpointStore(checkpoint), None

    if storage == "postgres":
        init_schema(database_url)
        return (
            PostgresCheckpointStore(database_url, pipeline_name),
            PostgresLandingStore(database_url),
        )

    raise ValueError(f"Unsupported storage backend: {storage}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run sample incremental ingestion.")
    parser.add_argument("--source", default="sample", choices=["sample"])
    parser.add_argument("--storage", default="file", choices=["file", "postgres"])
    parser.add_argument("--checkpoint", type=Path, help="Checkpoint file for file storage")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL))
    parser.add_argument("--pipeline-name", default="sample-ingestion")
    args = parser.parse_args()

    if args.source != "sample":
        raise SystemExit(f"Unsupported source: {args.source}")

    checkpoint_store, landing_store = build_stores(
        storage=args.storage,
        checkpoint=args.checkpoint,
        database_url=args.database_url,
        pipeline_name=args.pipeline_name,
    )
    records = ingest_incremental(
        fetch_sample_events,
        checkpoint_store,
        landing_store=landing_store,
    )
    print(json.dumps([record.__dict__ for record in records], indent=2))


if __name__ == "__main__":
    main()
