"""Airflow DAG for ingestion and dbt transformation."""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "br413",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

PROJECT_ROOT = os.environ.get("PIPELINE_PROJECT_ROOT", os.getcwd())
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://pipeline:pipeline@postgres:5432/landing",
)
WEBHOOK_URL = os.environ.get("PIPELINE_WEBHOOK_URL", "")

# Import callback after PROJECT_ROOT is on path when Airflow loads DAGs.
import sys

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pipeline_callbacks import webhook_task_failure_callback  # noqa: E402

default_args = dict(DEFAULT_ARGS)
if WEBHOOK_URL:
    default_args["on_failure_callback"] = webhook_task_failure_callback

with DAG(
    dag_id="production_sample_ingestion",
    default_args=default_args,
    description="Ingest sample events, validate quality, and run dbt transforms",
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["ingestion", "dbt", "portfolio"],
    doc_md="""
    ## production_sample_ingestion

    1. Incrementally ingest sample API events into PostgreSQL bronze layer
    2. Run dbt silver and gold models with tests

    Set `PIPELINE_PROJECT_ROOT` to the repository root when deploying.
    """,
) as dag:
    ingest_sample_events = BashOperator(
        task_id="ingest_sample_events",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m src.pipeline.ingestion "
            "--source sample --storage postgres --pipeline-name airflow-ingestion "
            "--alert-on-zero-records"
            + (f" --webhook-url {WEBHOOK_URL}" if WEBHOOK_URL else "")
        ),
        env={"DATABASE_URL": DATABASE_URL, "PIPELINE_WEBHOOK_URL": WEBHOOK_URL},
    )

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m src.pipeline.run_dbt --target dev"
        ),
        env={
            "DATABASE_URL": DATABASE_URL,
            "DBT_PROFILES_DIR": f"{PROJECT_ROOT}/dbt",
            "DBT_HOST": os.environ.get("DBT_HOST", "postgres"),
            "DBT_TARGET": os.environ.get("DBT_TARGET", "dev"),
        },
    )

    ingest_sample_events >> run_dbt_models
