from urllib.error import URLError
from unittest.mock import MagicMock, patch

import pytest

from src.pipeline.alerts import IngestionAlert, WebhookAlerter


def test_webhook_alerter_posts_json_payload() -> None:
    alert = IngestionAlert(
        event_type="zero_record_ingestion",
        pipeline_name="sample-ingestion",
        message="Ingestion completed with zero new records",
        summary={"records_ingested": 0},
    )
    alerter = WebhookAlerter("https://example.test/hook")

    with patch("src.pipeline.alerts.request.urlopen") as urlopen:
        urlopen.return_value.__enter__ = MagicMock(return_value=MagicMock())
        urlopen.return_value.__exit__ = MagicMock(return_value=False)
        alerter.send(alert)

    request_obj = urlopen.call_args.args[0]
    assert request_obj.full_url == "https://example.test/hook"
    assert b"zero_record_ingestion" in request_obj.data


def test_webhook_alerter_raises_on_delivery_failure() -> None:
    alerter = WebhookAlerter("https://example.test/hook")
    alert = IngestionAlert(
        event_type="airflow_task_failure",
        pipeline_name="airflow-ingestion",
        message="task failed",
    )

    with patch("src.pipeline.alerts.request.urlopen", side_effect=URLError("network down")):
        with pytest.raises(RuntimeError, match="webhook delivery failed"):
            alerter.send(alert)
