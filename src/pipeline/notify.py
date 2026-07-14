"""CLI for orchestration failure notifications."""

from __future__ import annotations

import argparse
import os
import sys

from src.pipeline.alerts import IngestionAlert, WebhookAlerter


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send pipeline alerts to a webhook")
    parser.add_argument(
        "--webhook-url",
        default=os.getenv("PIPELINE_WEBHOOK_URL"),
        help="Webhook endpoint (or set PIPELINE_WEBHOOK_URL)",
    )
    parser.add_argument("--event-type", required=True)
    parser.add_argument("--pipeline-name", default="airflow-ingestion")
    parser.add_argument("--message", required=True)
    parser.add_argument("--dag-id", default=None)
    parser.add_argument("--task-id", default=None)
    parser.add_argument("--run-id", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.webhook_url:
        parser.error("--webhook-url or PIPELINE_WEBHOOK_URL is required")

    WebhookAlerter(args.webhook_url).send(
        IngestionAlert(
            event_type=args.event_type,
            pipeline_name=args.pipeline_name,
            message=args.message,
            dag_id=args.dag_id,
            task_id=args.task_id,
            run_id=args.run_id,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
