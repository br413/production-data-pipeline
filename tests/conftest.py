import os
from datetime import datetime, timezone

import pytest

from src.pipeline.storage.postgres import DEFAULT_DATABASE_URL, init_schema


@pytest.fixture(scope="session")
def database_url() -> str:
    import psycopg

    url = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
    try:
        with psycopg.connect(url, connect_timeout=3):
            pass
        init_schema(url)
    except Exception as exc:  # pragma: no cover - environment specific
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    return url


@pytest.fixture
def quality_reference_time() -> datetime:
    """Fixed clock for freshness checks in tests using July 2026 fixture dates."""
    return datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def clean_tables(request) -> None:
    if request.node.get_closest_marker("integration") is None:
        return

    import psycopg

    url = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute("DROP VIEW IF EXISTS silver.stg_events CASCADE")
        conn.execute("DROP TABLE IF EXISTS gold.fct_daily_event_metrics CASCADE")
        conn.execute("DROP SCHEMA IF EXISTS silver CASCADE")
        conn.execute("DROP SCHEMA IF EXISTS gold CASCADE")
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        conn.execute("TRUNCATE bronze.raw_events")
        conn.execute("TRUNCATE bronze.quarantine_events")
        conn.execute(
            "DELETE FROM meta.processed_event_ids WHERE pipeline_name LIKE 'test-%'"
        )
        conn.execute(
            "DELETE FROM meta.pipeline_checkpoints WHERE pipeline_name LIKE 'test-%'"
        )
