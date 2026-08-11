"""PostgreSQL landing and checkpoint persistence."""

from __future__ import annotations

import json
from pathlib import Path

import psycopg

from src.pipeline.models import Checkpoint, EventRecord

DEFAULT_DATABASE_URL = "postgresql://pipeline:pipeline@localhost:5432/landing"
SCHEMA_PATH = Path(__file__).resolve().parents[3] / "sql" / "schema.sql"


def init_schema(database_url: str = DEFAULT_DATABASE_URL) -> None:
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute(schema_sql)


class PostgresLandingStore:
    def __init__(self, database_url: str = DEFAULT_DATABASE_URL) -> None:
        self._database_url = database_url

    def persist(self, records: list[EventRecord]) -> int:
        if not records:
            return 0

        inserted = 0
        with psycopg.connect(self._database_url) as conn:
            for record in records:
                result = conn.execute(
                    """
                    INSERT INTO bronze.raw_events (event_id, occurred_at, payload)
                    VALUES (%s, %s, %s::jsonb)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (record.event_id, record.occurred_at, json.dumps(record.payload)),
                )
                inserted += result.rowcount
            conn.commit()
        return inserted

    def fetch_all(self) -> list[EventRecord]:
        with psycopg.connect(self._database_url) as conn:
            rows = conn.execute(
                """
                SELECT event_id, occurred_at, payload
                FROM bronze.raw_events
                ORDER BY occurred_at
                """
            ).fetchall()

        return [
            EventRecord(
                event_id=row[0],
                occurred_at=row[1].isoformat().replace("+00:00", "Z"),
                payload=row[2],
            )
            for row in rows
        ]


class PostgresQuarantineStore:
    def __init__(
        self,
        database_url: str = DEFAULT_DATABASE_URL,
    ) -> None:
        self._database_url = database_url

    def persist(
        self,
        record: EventRecord,
        *,
        failed_rule: str,
        failure_message: str,
        pipeline_name: str,
        run_id: str,
    ) -> None:
        with psycopg.connect(self._database_url) as conn:
            conn.execute(
                """
                INSERT INTO bronze.quarantine_events (
                    event_id,
                    occurred_at,
                    payload,
                    failed_rule,
                    failure_message,
                    pipeline_name,
                    run_id
                )
                VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s)
                ON CONFLICT (pipeline_name, event_id, run_id) DO NOTHING
                """,
                (
                    record.event_id,
                    record.occurred_at,
                    json.dumps(record.payload),
                    failed_rule,
                    failure_message,
                    pipeline_name,
                    run_id,
                ),
            )
            conn.commit()

    def count_for_pipeline(self, pipeline_name: str) -> int:
        with psycopg.connect(self._database_url) as conn:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM bronze.quarantine_events
                WHERE pipeline_name = %s
                """,
                (pipeline_name,),
            ).fetchone()
        return int(row[0]) if row else 0


class PostgresCheckpointStore:
    def __init__(
        self,
        database_url: str = DEFAULT_DATABASE_URL,
        pipeline_name: str = "default",
    ) -> None:
        self._database_url = database_url
        self._pipeline_name = pipeline_name

    def load(self) -> Checkpoint:
        with psycopg.connect(self._database_url) as conn:
            row = conn.execute(
                """
                SELECT cursor
                FROM meta.pipeline_checkpoints
                WHERE pipeline_name = %s
                """,
                (self._pipeline_name,),
            ).fetchone()

            if row is None:
                return Checkpoint(cursor="", processed_ids=set())

            processed_rows = conn.execute(
                """
                SELECT event_id
                FROM meta.processed_event_ids
                WHERE pipeline_name = %s
                """,
                (self._pipeline_name,),
            ).fetchall()

        return Checkpoint(cursor=row[0], processed_ids={item[0] for item in processed_rows})

    def save(self, checkpoint: Checkpoint) -> None:
        with psycopg.connect(self._database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO meta.pipeline_checkpoints (pipeline_name, cursor, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (pipeline_name)
                    DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = NOW()
                    """,
                    (self._pipeline_name, checkpoint.cursor),
                )
                if checkpoint.processed_ids:
                    cur.executemany(
                        """
                        INSERT INTO meta.processed_event_ids (pipeline_name, event_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        [
                            (self._pipeline_name, event_id)
                            for event_id in sorted(checkpoint.processed_ids)
                        ],
                    )
            conn.commit()
