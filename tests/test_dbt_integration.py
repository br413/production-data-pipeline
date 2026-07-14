import os
import subprocess
from pathlib import Path

import psycopg
import pytest

from src.pipeline.ingestion import ingest_incremental
from src.pipeline.models import EventRecord
from src.pipeline.storage.postgres import PostgresCheckpointStore, PostgresLandingStore

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_ROOT / "dbt"


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


def _dbt_env(target: str = "ci") -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "DBT_TARGET": target,
            "DBT_PROFILES_DIR": str(DBT_DIR),
        }
    )
    return env


def _list_relations(database_url: str) -> list[tuple[str, str, str]]:
    with psycopg.connect(database_url) as conn:
        return conn.execute(
            """
            SELECT n.nspname, c.relname, c.relkind
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname IN ('silver', 'gold', 'bronze')
              AND c.relkind IN ('r', 'v', 'm')
            ORDER BY 1, 2
            """
        ).fetchall()


@pytest.mark.integration
def test_dbt_models_build_from_landed_events(database_url: str) -> None:
    checkpoint_store = PostgresCheckpointStore(database_url, "test-dbt-flow")
    landing_store = PostgresLandingStore(database_url)

    ingested = ingest_incremental(
        fetch_fixture,
        checkpoint_store,
        landing_store=landing_store,
        max_pages=1,
    )
    assert len(ingested) == 2

    ls = subprocess.run(
        [
            "dbt",
            "ls",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
            "--target",
            "ci",
        ],
        env=_dbt_env("ci"),
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )
    assert ls.returncode == 0, ls.stderr or ls.stdout
    assert "stg_events" in ls.stdout, ls.stdout

    clean = subprocess.run(
        [
            "dbt",
            "clean",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
        ],
        env=_dbt_env("ci"),
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )
    assert clean.returncode == 0, clean.stderr or clean.stdout

    run_result = subprocess.run(
        [
            "dbt",
            "run",
            "--full-refresh",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
            "--target",
            "ci",
        ],
        env=_dbt_env("ci"),
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )
    assert run_result.returncode == 0, f"dbt run failed:\n{run_result.stdout}\n{run_result.stderr}"

    test_result = subprocess.run(
        [
            "dbt",
            "test",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
            "--target",
            "ci",
        ],
        env=_dbt_env("ci"),
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )
    assert test_result.returncode == 0, f"dbt test failed:\n{test_result.stdout}\n{test_result.stderr}"

    relations = _list_relations(database_url)
    relation_names = {(schema, name) for schema, name, _ in relations}
    assert ("silver", "stg_events") in relation_names, f"relations: {relations}"
    assert ("gold", "fct_daily_event_metrics") in relation_names, f"relations: {relations}"

    with psycopg.connect(database_url) as conn:
        silver_count = conn.execute("SELECT COUNT(*) FROM silver.stg_events").fetchone()[0]
        gold_rows = conn.execute(
            "SELECT event_count, total_value FROM gold.fct_daily_event_metrics"
        ).fetchall()

    assert silver_count == 2
    assert len(gold_rows) == 1
    assert gold_rows[0][0] == 2
    assert float(gold_rows[0][1]) == 30.0
