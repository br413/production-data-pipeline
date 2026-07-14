from pathlib import Path

from src.pipeline.ingestion import EventRecord, ingest_incremental, load_checkpoint


def fetch_fixture(cursor: str):
    pages = {
        "": ([EventRecord("a", "t1", {}), EventRecord("b", "t2", {})], "p2"),
        "p2": ([EventRecord("c", "t3", {})], ""),
    }
    return pages.get(cursor, ([], ""))


def test_incremental_ingestion_is_idempotent(tmp_path: Path) -> None:
    checkpoint = tmp_path / "checkpoint.json"

    first_run = ingest_incremental(fetch_fixture, checkpoint, max_pages=2)
    second_run = ingest_incremental(fetch_fixture, checkpoint, max_pages=2)

    assert [record.event_id for record in first_run] == ["a", "b", "c"]
    assert second_run == []

    saved = load_checkpoint(checkpoint)
    assert saved.processed_ids == {"a", "b", "c"}


def test_checkpoint_survives_partial_page(tmp_path: Path) -> None:
    checkpoint = tmp_path / "checkpoint.json"

    ingest_incremental(fetch_fixture, checkpoint, max_pages=1)
    saved = load_checkpoint(checkpoint)

    assert saved.cursor == "p2"
    assert saved.processed_ids == {"a", "b"}
