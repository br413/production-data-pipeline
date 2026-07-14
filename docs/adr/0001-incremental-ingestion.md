# ADR 0001: Incremental ingestion with file-based checkpoints

## Status

Accepted

## Context

The pipeline must resume after interruption without duplicating events or skipping pages. A durable checkpoint mechanism is required before adding orchestration and database landing layers.

## Decision

Use a JSON checkpoint file containing:

- `cursor` — opaque pagination token from the source API
- `processed_ids` — stable identifiers for idempotent deduplication

Checkpoints are written after each successfully processed page.

## Consequences

**Positive**

- Simple to test and inspect locally
- Clear upgrade path to PostgreSQL or Redis metadata tables
- Supports backfill by resetting cursor with documented procedure

**Negative**

- Not suitable for high-concurrency writers without external locking
- File-based store is a development convenience, not a production final state

## Alternatives considered

1. **Database-only checkpoints** — rejected for initial scaffold due to local setup friction
2. **Timestamp-only watermarks** — rejected because APIs may return out-of-order events with stable IDs
