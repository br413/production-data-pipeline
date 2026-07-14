from datetime import datetime, timedelta, timezone

import pytest

from src.pipeline.ingestion import ingest_incremental
from src.pipeline.models import EventRecord
from src.pipeline.quality.validators import (
    assert_quality_suite,
    run_quality_suite,
    validate_freshness,
    validate_required_payload_fields,
    validate_unique_event_ids,
)
from src.pipeline.storage.postgres import PostgresCheckpointStore, PostgresLandingStore


def test_required_payload_fields_fail_when_missing() -> None:
    records = [EventRecord("evt-1", "2026-07-01T10:00:00Z", {})]
    result = validate_required_payload_fields(records)
    assert result.passed is False


def test_unique_event_ids_detects_duplicates() -> None:
    records = [
        EventRecord("dup", "2026-07-01T10:00:00Z", {"value": 1}),
        EventRecord("dup", "2026-07-01T10:01:00Z", {"value": 2}),
    ]
    result = validate_unique_event_ids(records)
    assert result.passed is False


def test_freshness_detects_stale_events() -> None:
    records = [EventRecord("old", "2020-01-01T00:00:00Z", {"value": 1})]
    result = validate_freshness(
        records,
        max_age=timedelta(days=7),
        now=datetime(2026, 7, 1, tzinfo=timezone.utc),
    )
    assert result.passed is False


def test_quality_suite_passes_for_valid_records() -> None:
    records = [
        EventRecord("evt-1", "2026-07-01T10:00:00Z", {"value": 1}),
        EventRecord("evt-2", "2026-07-01T10:05:00Z", {"value": 2}),
    ]
    results = run_quality_suite(records)
    assert all(result.passed for result in results)
    assert_quality_suite(records)


def fetch_fixture(cursor: str):
    pages = {
        "": (
            [
                EventRecord("pg-a", "2026-07-01T10:00:00Z", {"value": 1}),
                EventRecord("pg-b", "2026-07-01T10:05:00Z", {"value": 2}),
            ],
            "p2",
        ),
        "p2": ([EventRecord("pg-c", "2026-07-01T10:10:00Z", {"value": 3})], ""),
    }
    return pages.get(cursor, ([], ""))


@pytest.mark.integration
def test_postgres_landing_and_checkpoint_persistence(database_url: str) -> None:
    checkpoint_store = PostgresCheckpointStore(database_url, "test-postgres-flow")
    landing_store = PostgresLandingStore(database_url)

    first_run = ingest_incremental(
        fetch_fixture,
        checkpoint_store,
        landing_store=landing_store,
        max_pages=2,
    )
    second_run = ingest_incremental(
        fetch_fixture,
        checkpoint_store,
        landing_store=landing_store,
        max_pages=2,
    )

    assert [record.event_id for record in first_run] == ["pg-a", "pg-b", "pg-c"]
    assert second_run == []

    landed = landing_store.fetch_all()
    assert [record.event_id for record in landed] == ["pg-a", "pg-b", "pg-c"]

    saved = checkpoint_store.load()
    assert saved.processed_ids == {"pg-a", "pg-b", "pg-c"}
