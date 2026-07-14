import os
from pathlib import Path

import pytest

from src.pipeline.ingestion import ingest_incremental
from src.pipeline.models import EventRecord
from src.pipeline.run_dbt import run_transforms
from src.pipeline.storage.postgres import PostgresCheckpointStore, PostgresLandingStore


def fetch_fixture(cursor: str):
    pages = {
        "": (
            [
                EventRecord("dbt-1", "2026-07-01T10:00:00Z", {"value": 10}),
                EventRecord("dbt-2", "2026-07-01T11:00:00Z", {"value": 20}),
            ],
            "",
        ),
    }
    return pages.get(cursor, ([], ""))


@pytest.mark.integration
def test_dbt_models_build_from_landed_events(database_url: str) -> None:
    os.environ["DBT_TARGET"] = "ci"
    os.environ["DBT_PROFILES_DIR"] = str(Path(__file__).resolve().parents[1] / "dbt")

    checkpoint_store = PostgresCheckpointStore(database_url, "test-dbt-flow")
    landing_store = PostgresLandingStore(database_url)

    ingested = ingest_incremental(
        fetch_fixture,
        checkpoint_store,
        landing_store=landing_store,
        max_pages=1,
    )
    assert len(ingested) == 2

    run_transforms(target="ci")

    import psycopg

    with psycopg.connect(database_url) as conn:
        silver_count = conn.execute("SELECT COUNT(*) FROM silver.stg_events").fetchone()[0]
        gold_rows = conn.execute(
            "SELECT event_count, total_value FROM gold.fct_daily_event_metrics"
        ).fetchall()

    assert silver_count == 2
    assert len(gold_rows) == 1
    assert gold_rows[0][0] == 2
    assert float(gold_rows[0][1]) == 30.0
