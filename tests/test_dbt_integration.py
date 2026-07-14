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


def _run_dbt_command(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(args),
        env=_dbt_env("ci"),
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )


def _list_relations(database_url: str) -> list[tuple[str, str, str]]:
    with psycopg.connect(database_url) as conn:
        views = conn.execute(
            """
            SELECT schemaname, viewname, 'v'
            FROM pg_views
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            """
        ).fetchall()
        tables = conn.execute(
            """
            SELECT schemaname, tablename, 'r'
            FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            """
        ).fetchall()
    return list(views) + list(tables)


def _relation_map(relations: list[tuple[str, str, str]]) -> dict[tuple[str, str], str]:
    return {(schema, name): kind for schema, name, kind in relations}


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

    clean = _run_dbt_command(
        "dbt",
        "clean",
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(DBT_DIR),
    )
    assert clean.returncode == 0, clean.stderr or clean.stdout

    run_result = _run_dbt_command(
        "dbt",
        "run",
        "--full-refresh",
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(DBT_DIR),
        "--target",
        "ci",
    )
    assert run_result.returncode == 0, f"dbt run failed:\n{run_result.stdout}\n{run_result.stderr}"
    assert "ERROR=0" in run_result.stdout, run_result.stdout
    assert "NO-OP=0" in run_result.stdout, run_result.stdout

    test_result = _run_dbt_command(
        "dbt",
        "test",
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(DBT_DIR),
        "--target",
        "ci",
    )
    assert test_result.returncode == 0, f"dbt test failed:\n{test_result.stdout}\n{test_result.stderr}"

    relations = _list_relations(database_url)
    relation_names = _relation_map(relations)
    assert ("silver", "stg_events") in relation_names, (
        f"relations: {relations}\n"
        f"dbt run stdout:\n{run_result.stdout}"
    )
    assert ("gold", "fct_daily_event_metrics") in relation_names, (
        f"relations: {relations}\n"
        f"dbt run stdout:\n{run_result.stdout}"
    )

    with psycopg.connect(database_url) as conn:
        silver_count = conn.execute("SELECT COUNT(*) FROM silver.stg_events").fetchone()[0]
        gold_rows = conn.execute(
            "SELECT event_count, total_value FROM gold.fct_daily_event_metrics"
        ).fetchall()

    assert silver_count == 2
    assert len(gold_rows) == 1
    assert gold_rows[0][0] == 2
    assert float(gold_rows[0][1]) == 30.0
