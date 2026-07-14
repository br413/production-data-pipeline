# Architecture

## Components

### Ingestion connector

Responsible for paginated reads from external APIs, cursor advancement, and idempotent deduplication using stable event identifiers.

### Checkpoint store

A lightweight JSON persistence layer records the last cursor and processed event IDs. Production deployments would migrate this to a transactional metadata store.

### Landing layer (planned)

PostgreSQL receives append-only raw events. Schema validation occurs before promotion to silver models.

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
```
