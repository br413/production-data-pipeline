from pathlib import Path

import pytest

from src.pipeline.quarantine_metrics import (
    format_summary,
    summarize_quarantine_file,
    summarize_quarantine_postgres,
)
from src.pipeline.storage.file_quarantine import FileQuarantineStore
from src.pipeline.models import EventRecord


def test_summarize_quarantine_file_empty_path(tmp_path: Path) -> None:
    summary = summarize_quarantine_file(tmp_path / "missing.jsonl")
    assert summary.total_records == 0
    assert summary.by_failed_rule == ()
    assert format_summary(summary) == "total_records: 0"


def test_summarize_quarantine_file_groups_by_rule_and_pipeline(tmp_path: Path) -> None:
    store = FileQuarantineStore(tmp_path / "checkpoint.quarantine.jsonl")
    record = EventRecord("bad-1", "2026-07-01T10:00:00Z", {})
    store.persist(
        record,
        failed_rule="required_fields",
        failure_message="missing value",
        pipeline_name="demo-pipeline",
        run_id="run-1",
    )
    store.persist(
        record,
        failed_rule="schema",
        failure_message="invalid type",
        pipeline_name="demo-pipeline",
        run_id="run-2",
    )

    summary = summarize_quarantine_file(tmp_path / "checkpoint.quarantine.jsonl")
    assert summary.total_records == 2
    assert dict(summary.by_failed_rule) == {"required_fields": 1, "schema": 1}
    assert dict(summary.by_pipeline) == {"demo-pipeline": 2}
    assert summary.recent_run_ids == ("run-2", "run-1")

    rendered = format_summary(summary)
    assert "required_fields: 1" in rendered
    assert "demo-pipeline: 2" in rendered


@pytest.mark.integration
def test_summarize_quarantine_postgres(database_url: str) -> None:
    from src.pipeline.storage.postgres import PostgresQuarantineStore

    quarantine_store = PostgresQuarantineStore(database_url)
    record = EventRecord("metrics-bad", "2026-07-01T10:00:00Z", {})
    quarantine_store.persist(
        record,
        failed_rule="required_fields",
        failure_message="missing value",
        pipeline_name="metrics-test",
        run_id="run-metrics-1",
    )

    summary = summarize_quarantine_postgres(
        database_url=database_url,
        pipeline_name="metrics-test",
    )
    assert summary.total_records >= 1
    assert dict(summary.by_failed_rule).get("required_fields", 0) >= 1
    assert "run-metrics-1" in summary.recent_run_ids
