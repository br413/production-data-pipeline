"""Shared domain models for ingestion and storage."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EventRecord:
    """Normalized event from an external source."""

    event_id: str
    occurred_at: str
    payload: dict


@dataclass
class Checkpoint:
    """Tracks the last successfully processed cursor."""

    cursor: str
    processed_ids: set[str]


@dataclass(frozen=True)
class IngestionSummary:
    """Structured run metrics for logging and monitoring."""

    pages_read: int
    records_ingested: int
    duplicates_skipped: int
    final_cursor: str


@dataclass(frozen=True)
class IngestionResult:
    """Records loaded in a run plus observability metadata."""

    records: tuple[EventRecord, ...]
    summary: IngestionSummary
