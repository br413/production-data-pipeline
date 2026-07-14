from pathlib import Path

from src.pipeline.ingestion import ingest_incremental
from src.pipeline.models import EventRecord
from src.pipeline.storage.file_checkpoint import FileCheckpointStore


def fetch_fixture(cursor: str):
    pages = {
        "": (
            [
                EventRecord("a", "2026-07-01T10:00:00Z", {"value": 1}),
                EventRecord("b", "2026-07-01T10:05:00Z", {"value": 2}),
            ],
            "p2",
        ),
        "p2": ([EventRecord("c", "2026-07-01T10:10:00Z", {"value": 3})], ""),
    }
    return pages.get(cursor, ([], ""))


def test_incremental_ingestion_is_idempotent(tmp_path: Path) -> None:
    checkpoint_store = FileCheckpointStore(tmp_path / "checkpoint.json")

    first_run = ingest_incremental(fetch_fixture, checkpoint_store, max_pages=2)
    second_run = ingest_incremental(fetch_fixture, checkpoint_store, max_pages=2)

    assert [record.event_id for record in first_run] == ["a", "b", "c"]
    assert second_run == []

    saved = checkpoint_store.load()
    assert saved.processed_ids == {"a", "b", "c"}


def test_checkpoint_survives_partial_page(tmp_path: Path) -> None:
    checkpoint_store = FileCheckpointStore(tmp_path / "checkpoint.json")

    ingest_incremental(fetch_fixture, checkpoint_store, max_pages=1)
    saved = checkpoint_store.load()

    assert saved.cursor == "p2"
    assert saved.processed_ids == {"a", "b"}
