from unittest.mock import patch

from src.pipeline.notify import main


def test_notify_cli_sends_webhook() -> None:
    with patch("src.pipeline.notify.WebhookAlerter.send") as send:
        exit_code = main(
            [
                "--webhook-url",
                "https://example.test/hook",
                "--event-type",
                "airflow_task_failure",
                "--message",
                "ingest task failed",
                "--dag-id",
                "production_sample_ingestion",
                "--task-id",
                "ingest_sample_events",
            ]
        )

    assert exit_code == 0
    send.assert_called_once()
    alert = send.call_args.args[0]
    assert alert.event_type == "airflow_task_failure"
    assert alert.dag_id == "production_sample_ingestion"
