from pathlib import Path

import pytest

from src.pipeline.ingestion import ingest_incremental
from src.pipeline.models import EventRecord
from src.pipeline.quality.validators import record_quality_failures
from src.pipeline.storage.file_checkpoint import FileCheckpointStore
from src.pipeline.storage.file_quarantine import FileQuarantineStore
from src.pipeline.storage.postgres import (
    PostgresCheckpointStore,
    PostgresLandingStore,
    PostgresQuarantineStore,
)


def fetch_mixed_quality(cursor: str):
    pages = {
        "": (
            [
                EventRecord("good-1", "2026-07-01T10:00:00Z", {"value": 1}),
                EventRecord("bad-1", "2026-07-01T10:01:00Z", {}),
                EventRecord("good-2", "2026-07-01T10:02:00Z", {"value": 2}),
            ],
            "",
        ),
    }
    return pages.get(cursor, ([], ""))


def test_record_quality_failures_detects_missing_payload() -> None:
    record = EventRecord("bad", "2026-07-01T10:00:00Z", {})
    failures = record_quality_failures(record)
    assert failures
    assert failures[0].rule == "required_fields"


def test_quarantine_routes_invalid_records_without_blocking_valid(
    tmp_path: Path,
    quality_reference_time,
) -> None:
    checkpoint_store = FileCheckpointStore(tmp_path / "checkpoint.json")
    quarantine_store = FileQuarantineStore(tmp_path / "checkpoint.quarantine.jsonl")

    result = ingest_incremental(
        fetch_mixed_quality,
        checkpoint_store,
        quarantine_store=quarantine_store,
        pipeline_name="test-quarantine",
        run_id="run-test-1",
        quality_now=quality_reference_time,
    )

    assert [record.event_id for record in result.records] == ["good-1", "good-2"]
    assert result.summary.records_ingested == 2
    assert result.summary.records_quarantined == 1

    quarantine_lines = (tmp_path / "checkpoint.quarantine.jsonl").read_text(encoding="utf-8")
    assert "bad-1" in quarantine_lines
    assert "required_fields" in quarantine_lines

    saved = checkpoint_store.load()
    assert saved.processed_ids == {"good-1", "good-2", "bad-1"}


def test_quarantine_without_store_still_fails_fast(tmp_path: Path, quality_reference_time) -> None:
    checkpoint_store = FileCheckpointStore(tmp_path / "checkpoint.json")

    with pytest.raises(ValueError, match="required_fields"):
        ingest_incremental(
            fetch_mixed_quality,
            checkpoint_store,
            quality_now=quality_reference_time,
        )


@pytest.mark.integration
def test_postgres_quarantine_persists_failed_records(
    database_url: str,
    quality_reference_time,
) -> None:
    checkpoint_store = PostgresCheckpointStore(database_url, "test-quarantine-pg")
    landing_store = PostgresLandingStore(database_url)
    quarantine_store = PostgresQuarantineStore(database_url)

    result = ingest_incremental(
        fetch_mixed_quality,
        checkpoint_store,
        landing_store=landing_store,
        quarantine_store=quarantine_store,
        pipeline_name="test-quarantine-pg",
        run_id="run-pg-1",
        quality_now=quality_reference_time,
    )

    assert result.summary.records_ingested == 2
    assert result.summary.records_quarantined == 1
    assert quarantine_store.count_for_pipeline("test-quarantine-pg") == 1

    landed = landing_store.fetch_all()
    assert {record.event_id for record in landed} == {"good-1", "good-2"}
