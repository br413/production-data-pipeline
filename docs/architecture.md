# Architecture

## Components

### Ingestion connector

Responsible for paginated reads from external APIs, cursor advancement, and idempotent deduplication using stable event identifiers.

### Checkpoint store

PostgreSQL metadata tables (`meta.pipeline_checkpoints`, `meta.processed_event_ids`) record cursor position and processed event IDs. A file-based checkpoint remains available for lightweight local runs.

### Landing layer

PostgreSQL `bronze.raw_events` receives append-only raw events with idempotent inserts on `event_id`. Data-quality checks run before persistence.

### Transformation layer (planned)

dbt models enforce typing, business rules, and dimensional structures. Tests run in CI alongside Python unit tests.

### Orchestration (planned)

Airflow schedules ingestion, transformation, and quality gates with explicit retry and alerting policies.

## Failure modes

| Failure | Mitigation |
|---------|------------|
| Duplicate delivery | `processed_ids` set suppresses re-ingestion |
| Partial page read | Checkpoint advances only after successful page processing |
| API timeout | Retry with backoff (planned) |
| Schema drift | Validation gate blocks silver promotion (planned) |

## Local development

```bash
docker compose up -d
pytest
python -m src.pipeline.ingestion --source sample --checkpoint .checkpoints/dev.json
python -m src.pipeline.ingestion --source sample --storage postgres --pipeline-name dev-ingestion
```
