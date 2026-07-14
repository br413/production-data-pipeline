# Architecture

## Components

### Ingestion connector

Responsible for paginated reads from external APIs, cursor advancement, and idempotent deduplication using stable event identifiers.

### Checkpoint store

PostgreSQL metadata tables (`meta.pipeline_checkpoints`, `meta.processed_event_ids`) record cursor position and processed event IDs. A file-based checkpoint remains available for lightweight local runs.

### Landing layer

PostgreSQL `bronze.raw_events` receives append-only raw events with idempotent inserts on `event_id`. Data-quality checks run before persistence.

### Transformation layer

dbt builds:

- **Silver:** `silver.stg_events` — validated staging view with typed `event_value`
- **Gold:** `gold.fct_daily_event_metrics` — daily aggregates for analytics consumers

dbt tests enforce uniqueness, not-null constraints, and source freshness expectations.

### Orchestration

Airflow DAG `production_sample_ingestion` schedules:

1. `ingest_sample_events` — Python ingestion into bronze
2. `run_dbt_models` — dbt run + test for silver and gold

Default retry policy: 2 retries with 5-minute delay.

## Failure modes

| Failure | Mitigation |
|---------|------------|
| Duplicate delivery | `processed_ids` set suppresses re-ingestion |
| Partial page read | Checkpoint advances only after successful page processing |
| API timeout | Airflow retry; manual rerun documented in operations runbook |
| Schema drift | Python quality gate blocks landing; dbt tests catch transform issues |
| dbt test failure | DAG stops; fix model or source data before downstream use |

## Local development

```bash
docker compose up -d
pip install -r requirements.txt -r requirements-dbt.txt
pytest
python -m src.pipeline.ingestion --source sample --storage postgres --pipeline-name dev-ingestion
python -m src.pipeline.run_dbt --target dev
```

Airflow local test:

```bash
pip install -r requirements-airflow.txt
export AIRFLOW_HOME=./.airflow
airflow db init
airflow dags test production_sample_ingestion 2026-07-14
```

See [`operations.md`](operations.md) for monitoring and recovery procedures.
