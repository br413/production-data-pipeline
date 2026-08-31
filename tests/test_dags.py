"""Validate Airflow DAG integrity."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAGS_FOLDER = str(PROJECT_ROOT / "dags")

# Airflow 2.x expects DAGs folder on sys.path for module imports.
sys.path.insert(0, DAGS_FOLDER)
os.environ.setdefault("AIRFLOW_HOME", str(PROJECT_ROOT / ".airflow"))


def test_dag_loads_without_import_errors() -> None:
    from airflow.models import DagBag

    dag_bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    assert dag_bag.import_errors == {}, f"DAG import errors: {dag_bag.import_errors}"

    dag = dag_bag.get_dag("production_sample_ingestion")
    assert dag is not None
    assert len(dag.tasks) == 3


def test_dag_task_dependencies() -> None:
    from airflow.models import DagBag

    dag_bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    dag = dag_bag.get_dag("production_sample_ingestion")
    assert dag is not None

    ingest_task = dag.get_task("ingest_sample_events")
    dbt_task = dag.get_task("run_dbt_models")
    quality_task = dag.get_task("run_quality_contracts")
    assert dbt_task in ingest_task.downstream_list
    assert quality_task in dbt_task.downstream_list
