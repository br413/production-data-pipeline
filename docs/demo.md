# Demo Script (2–3 minutes)

Use this script to record a short walkthrough of the pipeline for your portfolio site or Dev.to article.

## Setup before recording

```powershell
cd production-data-pipeline
docker compose up -d
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -r requirements-dbt.txt
```

## Scene 1 — Architecture (30 seconds)

Show `docs/architecture.md` or README architecture diagram.

**Say:**

> This project demonstrates a production-style data pipeline: incremental API ingestion into a PostgreSQL bronze layer, data-quality validation, dbt transformations into silver and gold, and Airflow orchestration with retries.

## Scene 2 — Run ingestion (45 seconds)

```powershell
python -m src.pipeline.ingestion --source sample --storage postgres --pipeline-name demo-ingestion
```

**Say:**

> Ingestion is incremental and idempotent. Checkpoints track the cursor and processed event IDs so restarts do not duplicate data.

Optional: show checkpoint table:

```sql
SELECT * FROM meta.pipeline_checkpoints WHERE pipeline_name = 'demo-ingestion';
```

## Scene 3 — Run dbt transforms (45 seconds)

```powershell
python -m src.pipeline.run_dbt --target dev
```

**Say:**

> dbt builds a silver staging view with validated fields, then a gold daily metrics table. Tests enforce uniqueness, not-null constraints, and basic business rules.

Show output tables:

```sql
SELECT * FROM silver.stg_events;
SELECT * FROM gold.fct_daily_event_metrics;
```

## Scene 4 — Tests and CI (30 seconds)

```powershell
pytest -q
```

**Say:**

> Unit tests cover ingestion and quality rules. Integration tests run against PostgreSQL in CI, including dbt model tests.

Show GitHub Actions green check on the repository.

## Scene 5 — Operations (30 seconds)

Open `docs/operations.md`.

**Say:**

> The runbook documents monitoring signals, retry policy, backfill procedures, and incident triage — the operational layer that makes a pipeline production-ready.

## Closing line

> This is v0.2.0 — a focused, inspectable portfolio project showing how I design reliable data platforms with testing, documentation, and clear recovery paths.

## Optional: Airflow UI

If running Airflow locally:

```bash
export AIRFLOW_HOME=./.airflow
airflow db init
airflow dags test production_sample_ingestion 2026-07-14
```

Show the DAG graph with `ingest_sample_events >> run_dbt_models`.
