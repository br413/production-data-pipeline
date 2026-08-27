"""Summarize quarantine volume for operators."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import psycopg

from src.pipeline.storage.postgres import DEFAULT_DATABASE_URL


@dataclass(frozen=True)
class QuarantineMetricsSummary:
    total_records: int
    by_failed_rule: tuple[tuple[str, int], ...]
    by_pipeline: tuple[tuple[str, int], ...]
    recent_run_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "total_records": self.total_records,
            "by_failed_rule": dict(self.by_failed_rule),
            "by_pipeline": dict(self.by_pipeline),
            "recent_run_ids": list(self.recent_run_ids),
        }


def summarize_quarantine_file(path: Path, *, recent_limit: int = 5) -> QuarantineMetricsSummary:
    if not path.is_file():
        return QuarantineMetricsSummary(0, (), (), ())

    failed_rules: Counter[str] = Counter()
    pipelines: Counter[str] = Counter()
    run_ids: list[str] = []

    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        failed_rules[str(payload.get("failed_rule", "unknown"))] += 1
        pipelines[str(payload.get("pipeline_name", "unknown"))] += 1
        run_id = payload.get("run_id")
        if isinstance(run_id, str):
            run_ids.append(run_id)

    unique_recent = tuple(list(dict.fromkeys(reversed(run_ids)))[:recent_limit])
    return QuarantineMetricsSummary(
        total_records=sum(failed_rules.values()),
        by_failed_rule=tuple(failed_rules.most_common()),
        by_pipeline=tuple(pipelines.most_common()),
        recent_run_ids=unique_recent,
    )


def summarize_quarantine_postgres(
    *,
    database_url: str = DEFAULT_DATABASE_URL,
    pipeline_name: str | None = None,
    recent_limit: int = 5,
) -> QuarantineMetricsSummary:
    params: list[str] = []
    pipeline_filter = ""
    if pipeline_name is not None:
        pipeline_filter = "WHERE pipeline_name = %s"
        params.append(pipeline_name)

    with psycopg.connect(database_url) as conn:
        total_row = conn.execute(
            f"SELECT COUNT(*) FROM bronze.quarantine_events {pipeline_filter}",
            params,
        ).fetchone()
        rule_rows = conn.execute(
            f"""
            SELECT failed_rule, COUNT(*)
            FROM bronze.quarantine_events
            {pipeline_filter}
            GROUP BY failed_rule
            ORDER BY COUNT(*) DESC
            """,
            params,
        ).fetchall()
        pipeline_rows = conn.execute(
            """
            SELECT pipeline_name, COUNT(*)
            FROM bronze.quarantine_events
            GROUP BY pipeline_name
            ORDER BY COUNT(*) DESC
            """
        ).fetchall()
        run_rows = conn.execute(
            f"""
            SELECT run_id
            FROM bronze.quarantine_events
            {pipeline_filter}
            ORDER BY quarantined_at DESC
            LIMIT %s
            """,
            [*params, recent_limit],
        ).fetchall()

    return QuarantineMetricsSummary(
        total_records=int(total_row[0]) if total_row else 0,
        by_failed_rule=tuple((str(rule), int(count)) for rule, count in rule_rows),
        by_pipeline=tuple((str(name), int(count)) for name, count in pipeline_rows),
        recent_run_ids=tuple(list(dict.fromkeys(str(row[0]) for row in run_rows))),
    )


def format_summary(summary: QuarantineMetricsSummary) -> str:
    lines = [f"total_records: {summary.total_records}"]
    if summary.by_failed_rule:
        lines.append("by_failed_rule:")
        for rule, count in summary.by_failed_rule:
            lines.append(f"  {rule}: {count}")
    if summary.by_pipeline:
        lines.append("by_pipeline:")
        for pipeline, count in summary.by_pipeline:
            lines.append(f"  {pipeline}: {count}")
    if summary.recent_run_ids:
        lines.append("recent_run_ids:")
        for run_id in summary.recent_run_ids:
            lines.append(f"  {run_id}")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Summarize quarantine volume by rule and pipeline")
    parser.add_argument(
        "--file",
        type=Path,
        default=None,
        help="Path to JSONL quarantine file (file checkpoint mode)",
    )
    parser.add_argument(
        "--storage",
        choices=("postgres",),
        default=None,
        help="Read metrics from PostgreSQL bronze.quarantine_events",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="PostgreSQL URL when --storage postgres",
    )
    parser.add_argument(
        "--pipeline-name",
        default=None,
        help="Optional pipeline filter for PostgreSQL metrics",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON instead of plain text",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.file is not None:
        summary = summarize_quarantine_file(args.file)
    elif args.storage == "postgres":
        summary = summarize_quarantine_postgres(
            database_url=args.database_url or DEFAULT_DATABASE_URL,
            pipeline_name=args.pipeline_name,
        )
    else:
        parser.error("pass --file or --storage postgres")

    if args.json:
        print(json.dumps(summary.to_dict(), indent=2))
    else:
        print(format_summary(summary))
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
