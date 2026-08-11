"""Data-quality validation rules for landed events."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from src.pipeline.models import EventRecord

REQUIRED_PAYLOAD_FIELDS = ("value",)


@dataclass(frozen=True)
class QualityResult:
    rule: str
    passed: bool
    message: str


def validate_event_schema(record: EventRecord) -> QualityResult:
    if not record.event_id.strip():
        return QualityResult("schema", False, "event_id must be non-empty")
    if not record.occurred_at.strip():
        return QualityResult("schema", False, "occurred_at must be non-empty")
    if not isinstance(record.payload, dict):
        return QualityResult("schema", False, "payload must be an object")
    return QualityResult("schema", True, "record schema is valid")


def validate_required_payload_fields(records: list[EventRecord]) -> QualityResult:
    missing = [
        record.event_id
        for record in records
        if any(field not in record.payload for field in REQUIRED_PAYLOAD_FIELDS)
    ]
    if missing:
        return QualityResult(
            "required_fields",
            False,
            f"missing required payload fields for: {', '.join(missing)}",
        )
    return QualityResult("required_fields", True, "required payload fields present")


def validate_unique_event_ids(records: list[EventRecord]) -> QualityResult:
    ids = [record.event_id for record in records]
    duplicates = {event_id for event_id in ids if ids.count(event_id) > 1}
    if duplicates:
        return QualityResult(
            "uniqueness",
            False,
            f"duplicate event_id values: {', '.join(sorted(duplicates))}",
        )
    return QualityResult("uniqueness", True, "event_id values are unique")


def validate_freshness(
    records: list[EventRecord],
    *,
    max_age: timedelta = timedelta(days=30),
    now: datetime | None = None,
) -> QualityResult:
    reference = now or datetime.now(timezone.utc)
    stale: list[str] = []

    for record in records:
        occurred_at = datetime.fromisoformat(record.occurred_at.replace("Z", "+00:00"))
        if occurred_at.tzinfo is None:
            occurred_at = occurred_at.replace(tzinfo=timezone.utc)
        if reference - occurred_at > max_age:
            stale.append(record.event_id)

    if stale:
        return QualityResult(
            "freshness",
            False,
            f"stale events beyond {max_age.days} days: {', '.join(stale)}",
        )
    return QualityResult("freshness", True, "events are within freshness window")


def record_quality_failures(
    record: EventRecord,
    *,
    now: datetime | None = None,
) -> list[QualityResult]:
    """Return failed quality checks for a single record."""
    results = [
        validate_event_schema(record),
        validate_required_payload_fields([record]),
        validate_freshness([record], now=now),
    ]
    return [result for result in results if not result.passed]


def run_quality_suite(
    records: list[EventRecord],
    *,
    now: datetime | None = None,
) -> list[QualityResult]:
    results = [validate_event_schema(record) for record in records]
    results.extend(
        [
            validate_required_payload_fields(records),
            validate_unique_event_ids(records),
            validate_freshness(records, now=now),
        ]
    )
    return results


def assert_quality_suite(records: list[EventRecord], *, now: datetime | None = None) -> None:
    failures = [result for result in run_quality_suite(records, now=now) if not result.passed]
    if failures:
        messages = "; ".join(f"{result.rule}: {result.message}" for result in failures)
        raise ValueError(messages)
