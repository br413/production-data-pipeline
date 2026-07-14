# ADR 0002: PostgreSQL landing and checkpoint store

## Status

Accepted

## Context

Week 2 requires durable landing storage and transactional checkpoints so ingestion can resume safely across process restarts without duplicating bronze records.

## Decision

- Persist raw events in `bronze.raw_events` with `ON CONFLICT DO NOTHING` on `event_id`
- Store pipeline cursor and processed IDs in `meta.pipeline_checkpoints` and `meta.processed_event_ids`
- Keep file-based checkpoints for lightweight local workflows
- Run schema validation, required-field, uniqueness, and freshness checks before landing writes

## Consequences

**Positive**

- Idempotency enforced in both application logic and database constraints
- Integration tests exercise the full ingestion-to-landing path
- Clear migration path to orchestrated production runs in Week 3

**Negative**

- Local development requires Docker Compose or a reachable PostgreSQL instance for integration tests
- Processed-ID tracking grows with pipeline volume and will need pruning/archival in production

## Alternatives considered

1. **Continue with JSON checkpoints only** — rejected; does not demonstrate production landing patterns
2. **SQLite for portability** — rejected; portfolio targets warehouse/lakehouse PostgreSQL patterns
