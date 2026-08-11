"""Incremental, idempotent API-style ingestion with checkpoint persistence."""

from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from src.pipeline.alerts import IngestionAlert, WebhookAlerter
from src.pipeline.models import Checkpoint, EventRecord, IngestionResult, IngestionSummary
from src.pipeline.quality.validators import assert_quality_suite, record_quality_failures
from src.pipeline.storage.base import CheckpointStore, LandingStore, QuarantineStore
from src.pipeline.storage.file_checkpoint import FileCheckpointStore
from src.pipeline.storage.file_quarantine import FileQuarantineStore
from src.pipeline.storage.postgres import (
    DEFAULT_DATABASE_URL,
    PostgresCheckpointStore,
    PostgresLandingStore,
    PostgresQuarantineStore,
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


def _quarantine_record(
    record: EventRecord,
    *,
    failed_rule: str,
    failure_message: str,
    quarantine_store: QuarantineStore,
    pipeline_name: str,
    run_id: str,
    checkpoint: Checkpoint,
) -> None:
    quarantine_store.persist(
        record,
        failed_rule=failed_rule,
        failure_message=failure_message,
        pipeline_name=pipeline_name,
        run_id=run_id,
    )
    checkpoint.processed_ids.add(record.event_id)


def _process_page_with_quarantine(
    records: list[EventRecord],
    checkpoint: Checkpoint,
    *,
    quarantine_store: QuarantineStore,
    pipeline_name: str,
    run_id: str,
    validate: bool,
    quality_now: datetime | None,
) -> tuple[list[EventRecord], int, int]:
    page_batch: list[EventRecord] = []
    duplicates_skipped = 0
    records_quarantined = 0
    page_seen_ids: set[str] = set()

    for record in records:
        if record.event_id in checkpoint.processed_ids:
            duplicates_skipped += 1
            continue

        if record.event_id in page_seen_ids:
            _quarantine_record(
                record,
                failed_rule="uniqueness",
                failure_message=f"duplicate event_id in page: {record.event_id}",
                quarantine_store=quarantine_store,
                pipeline_name=pipeline_name,
                run_id=run_id,
                checkpoint=checkpoint,
            )
            records_quarantined += 1
            continue

        page_seen_ids.add(record.event_id)

        if validate:
            failures = record_quality_failures(record, now=quality_now)
            if failures:
                first = failures[0]
                _quarantine_record(
                    record,
                    failed_rule=first.rule,
                    failure_message=first.message,
                    quarantine_store=quarantine_store,
                    pipeline_name=pipeline_name,
                    run_id=run_id,
                    checkpoint=checkpoint,
                )
                records_quarantined += 1
                continue

        page_batch.append(record)

    return page_batch, duplicates_skipped, records_quarantined


def _process_page_batch(
    records: list[EventRecord],
    checkpoint: Checkpoint,
) -> tuple[list[EventRecord], int]:
    page_batch: list[EventRecord] = []
    duplicates_skipped = 0

    for record in records:
        if record.event_id in checkpoint.processed_ids:
            duplicates_skipped += 1
            continue
        page_batch.append(record)

    return page_batch, duplicates_skipped


def ingest_incremental(
    fetch_page,
    checkpoint_store: CheckpointStore,
    *,
    landing_store: LandingStore | None = None,
    quarantine_store: QuarantineStore | None = None,
    pipeline_name: str = "default",
    run_id: str | None = None,
    max_pages: int | None = None,
    validate: bool = True,
    quality_now: datetime | None = None,
) -> IngestionResult:
    """Load new records while preserving idempotency across runs."""
    checkpoint = checkpoint_store.load()
    ingested: list[EventRecord] = []
    cursor = checkpoint.cursor
    pages_read = 0
    duplicates_skipped = 0
    records_quarantined = 0
    effective_run_id = run_id or str(uuid.uuid4())

    while True:
        if max_pages is not None and pages_read >= max_pages:
            break

        records, next_cursor = fetch_page(cursor)
        pages_read += 1

        if quarantine_store is not None:
            page_batch, page_duplicates, page_quarantined = _process_page_with_quarantine(
                records,
                checkpoint,
                quarantine_store=quarantine_store,
                pipeline_name=pipeline_name,
                run_id=effective_run_id,
                validate=validate,
                quality_now=quality_now,
            )
            duplicates_skipped += page_duplicates
            records_quarantined += page_quarantined
        else:
            page_batch, page_duplicates = _process_page_batch(records, checkpoint)
            duplicates_skipped += page_duplicates
            if page_batch and validate:
                assert_quality_suite(page_batch, now=quality_now)

        if page_batch:
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
    summary = IngestionSummary(
        pages_read=pages_read,
        records_ingested=len(ingested),
        duplicates_skipped=duplicates_skipped,
        final_cursor=checkpoint.cursor,
        records_quarantined=records_quarantined,
    )
    return IngestionResult(records=tuple(ingested), summary=summary)


def build_stores(
    *,
    storage: str,
    checkpoint: Path | None,
    database_url: str,
    pipeline_name: str,
    enable_quarantine: bool = False,
) -> tuple[CheckpointStore, LandingStore | None, QuarantineStore | None]:
    if storage == "file":
        if checkpoint is None:
            raise ValueError("--checkpoint is required when --storage file")
        quarantine_store = None
        if enable_quarantine:
            quarantine_path = checkpoint.with_suffix(".quarantine.jsonl")
            quarantine_store = FileQuarantineStore(quarantine_path)
        return FileCheckpointStore(checkpoint), None, quarantine_store

    if storage == "postgres":
        init_schema(database_url)
        quarantine_store = PostgresQuarantineStore(database_url) if enable_quarantine else None
        return (
            PostgresCheckpointStore(database_url, pipeline_name),
            PostgresLandingStore(database_url),
            quarantine_store,
        )

    raise ValueError(f"Unsupported storage backend: {storage}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run sample incremental ingestion.")
    parser.add_argument("--source", default="sample", choices=["sample"])
    parser.add_argument("--storage", default="file", choices=["file", "postgres"])
    parser.add_argument("--checkpoint", type=Path, help="Checkpoint file for file storage")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL))
    parser.add_argument("--pipeline-name", default="sample-ingestion")
    parser.add_argument(
        "--webhook-url",
        default=os.getenv("PIPELINE_WEBHOOK_URL"),
        help="Optional webhook for ingestion alerts",
    )
    parser.add_argument(
        "--alert-on-zero-records",
        action="store_true",
        help="POST to webhook when no new records are ingested",
    )
    parser.add_argument(
        "--enable-quarantine",
        action="store_true",
        help="Route validation failures to quarantine instead of failing the run",
    )
    parser.add_argument(
        "--alert-on-quarantine",
        action="store_true",
        help="POST to webhook when records are quarantined",
    )
    args = parser.parse_args()

    if args.source != "sample":
        raise SystemExit(f"Unsupported source: {args.source}")

    checkpoint_store, landing_store, quarantine_store = build_stores(
        storage=args.storage,
        checkpoint=args.checkpoint,
        database_url=args.database_url,
        pipeline_name=args.pipeline_name,
        enable_quarantine=args.enable_quarantine,
    )
    result = ingest_incremental(
        fetch_sample_events,
        checkpoint_store,
        landing_store=landing_store,
        quarantine_store=quarantine_store,
        pipeline_name=args.pipeline_name,
    )
    print(json.dumps([record.__dict__ for record in result.records], indent=2))
    print(json.dumps({"ingestion_summary": result.summary.__dict__}, indent=2))

    if args.webhook_url:
        if args.alert_on_zero_records and result.summary.records_ingested == 0:
            WebhookAlerter(args.webhook_url).send(
                IngestionAlert(
                    event_type="zero_record_ingestion",
                    pipeline_name=args.pipeline_name,
                    message="Ingestion completed with zero new records",
                    summary=result.summary.__dict__,
                )
            )
        if args.alert_on_quarantine and result.summary.records_quarantined > 0:
            WebhookAlerter(args.webhook_url).send(
                IngestionAlert(
                    event_type="ingestion_quarantine",
                    pipeline_name=args.pipeline_name,
                    message=(
                        f"Ingestion quarantined {result.summary.records_quarantined} "
                        "record(s) after validation failures"
                    ),
                    summary=result.summary.__dict__,
                )
            )


if __name__ == "__main__":
    main()
