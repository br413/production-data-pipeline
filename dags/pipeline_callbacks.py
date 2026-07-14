"""Airflow callbacks for pipeline alerting."""

from __future__ import annotations

import os
from typing import Any

from src.pipeline.alerts import IngestionAlert, WebhookAlerter


def webhook_task_failure_callback(context: dict[str, Any]) -> None:
    """Notify webhook when an Airflow task fails after retries."""
    url = os.environ.get("PIPELINE_WEBHOOK_URL")
    if not url:
        return

    task_instance = context["task_instance"]
    dag_run = context.get("dag_run")
    run_id = str(dag_run.run_id) if dag_run is not None else None

    WebhookAlerter(url).send(
        IngestionAlert(
            event_type="airflow_task_failure",
            pipeline_name=os.environ.get("PIPELINE_NAME", "airflow-ingestion"),
            message=(
                f"Task {task_instance.task_id} failed in DAG "
                f"{task_instance.dag_id} after retries"
            ),
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=run_id,
        )
    )
