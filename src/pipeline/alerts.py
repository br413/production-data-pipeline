"""Webhook alerting for ingestion and orchestration failures."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any
from urllib import error, request


@dataclass(frozen=True)
class IngestionAlert:
    event_type: str
    pipeline_name: str
    message: str
    summary: dict[str, Any] | None = None
    dag_id: str | None = None
    task_id: str | None = None
    run_id: str | None = None


class WebhookAlerter:
    def __init__(self, url: str, *, timeout_seconds: float = 5.0) -> None:
        self.url = url
        self.timeout_seconds = timeout_seconds

    def send(self, alert: IngestionAlert) -> None:
        payload = json.dumps(asdict(alert)).encode("utf-8")
        http_request = request.Request(
            self.url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(http_request, timeout=self.timeout_seconds):
                return
        except error.URLError as exc:
            raise RuntimeError(f"webhook delivery failed: {exc}") from exc
