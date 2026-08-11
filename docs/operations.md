# Operations Runbook

## Pipeline overview

| Stage | Component | Failure signal |
|-------|-----------|----------------|
| Ingestion | `src.pipeline.ingestion` | Task failure, zero new rows unexpectedly |
| Quality | `src.pipeline.quality.validators` | Validation error before landing |
| Transform | dbt silver/gold models | `dbt test` failure |
| Quarantine (planned) | `bronze.quarantine_events` | `records_quarantined` > 0 in ingestion summary |
| Orchestration | Airflow DAG `production_sample_ingestion` | DAG run failed / retry exhausted |

## Monitoring

### What to watch

- **Ingestion volume:** row count in `bronze.raw_events` per run
- **Ingestion summary:** JSON block printed after each CLI run (`pages_read`, `records_ingested`, `duplicates_skipped`, `final_cursor`)
- **Checkpoint freshness:** `meta.pipeline_checkpoints.updated_at`
- **Transform health:** dbt test results in Airflow logs
- **DAG SLA:** daily run completion before business hours

### Log locations

| Environment | Location |
|-------------|----------|
| Local CLI | stdout from `python -m src.pipeline.ingestion` (records + `ingestion_summary`) |
| Airflow | Task logs in Airflow UI → DAG → Task Instance |
| CI | GitHub Actions job output |

### Suggested alerts

1. Airflow DAG failure after retries exhausted
2. `dbt test` failure on `stg_events` or `fct_daily_event_metrics`
3. No checkpoint update within 24 hours for scheduled pipeline
4. Ingestion returns zero records for 3 consecutive runs (possible upstream outage)
5. Quarantine volume spikes (planned — see [ADR 0004](adr/0004-failed-record-quarantine.md))

## Retry policy

| Layer | Retries | Backoff | Owner |
|-------|---------|---------|-------|
| Airflow tasks | 2 | 5 minutes | DAG default args |
| API ingestion | Manual re-run | N/A | On-call engineer |
| dbt transforms | Re-run `dbt run` | Immediate after fix | Data engineer |

## Recovery procedures

### 1. Ingestion task failed mid-run

**Symptoms:** Airflow task `ingest_sample_events` failed; partial data may exist.

**Steps:**

1. Check logs for validation or database connection errors
2. Verify PostgreSQL is reachable: `docker compose ps`
3. Inspect checkpoint state:
   ```sql
   SELECT * FROM meta.pipeline_checkpoints WHERE pipeline_name = 'airflow-ingestion';
   ```
4. Re-run ingestion manually:
   ```bash
   python -m src.pipeline.ingestion --source sample --storage postgres --pipeline-name airflow-ingestion
   ```
5. Confirm idempotency — no duplicate `event_id` in `bronze.raw_events`

### 2. dbt transformation failed

**Symptoms:** `run_dbt_models` task failed; silver/gold may be stale.

**Steps:**

1. Review dbt logs in Airflow task output
2. Run locally:
   ```bash
   python -m src.pipeline.run_dbt --target dev
   ```
3. If schema drift caused failure, inspect `bronze.raw_events` payloads
4. Fix model or source data, then re-run DAG transform task only

### 3. Duplicate events detected

**Symptoms:** Uniqueness test failure on `event_id`.

**Steps:**

1. Identify duplicates:
   ```sql
   SELECT event_id, COUNT(*) FROM bronze.raw_events GROUP BY 1 HAVING COUNT(*) > 1;
   ```
2. Do **not** delete without understanding root cause
3. Check whether checkpoint store was reset incorrectly
4. Re-run ingestion — `ON CONFLICT DO NOTHING` prevents new duplicates

### 4. Backfill from specific date

**Steps:**

1. Document the backfill scope and approval
2. Reset checkpoint for the pipeline:
   ```sql
   DELETE FROM meta.processed_event_ids WHERE pipeline_name = 'airflow-ingestion';
   UPDATE meta.pipeline_checkpoints SET cursor = '' WHERE pipeline_name = 'airflow-ingestion';
   ```
3. Re-run ingestion for the desired window
4. Run `python -m src.pipeline.run_dbt --target dev`
5. Validate gold metrics manually

### 5. Full pipeline reset (local dev only)

```bash
docker compose down -v
docker compose up -d
python -m src.pipeline.ingestion --source sample --storage postgres --pipeline-name airflow-ingestion
python -m src.pipeline.run_dbt --target dev
```

## Incident triage checklist

- [ ] Identify failing stage (ingestion, quality, dbt, orchestration)
- [ ] Capture Airflow run ID and timestamp
- [ ] Check PostgreSQL connectivity and disk space
- [ ] Verify upstream API availability (or sample source for demo)
- [ ] Attempt manual rerun before structural changes
- [ ] Document root cause and preventive action

## Secrets and configuration

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` | PostgreSQL connection for ingestion |
| `DBT_TARGET` | dbt profile target (`dev`, `ci`) |
| `DBT_HOST` | Database host for dbt |
| `PIPELINE_PROJECT_ROOT` | Repo root for Airflow BashOperators |
| `PIPELINE_WEBHOOK_URL` | Optional webhook for zero-record and task-failure alerts |

Never commit credentials. Use environment variables or a secrets manager in production.

### Webhook alerts

When `PIPELINE_WEBHOOK_URL` is set:

- **Zero-record runs:** ingestion posts `zero_record_ingestion` when `--alert-on-zero-records` is enabled and no new rows land
- **Airflow failures:** `on_failure_callback` posts `airflow_task_failure` after retries are exhausted

Manual notification:

```bash
python -m src.pipeline.notify \
  --webhook-url "$PIPELINE_WEBHOOK_URL" \
  --event-type airflow_task_failure \
  --message "ingest task failed" \
  --dag-id production_sample_ingestion \
  --task-id ingest_sample_events
```

## Failed-record quarantine (planned)

Design: [ADR 0004 — failed-record quarantine](adr/0004-failed-record-quarantine.md).

When implemented, invalid rows will land in `bronze.quarantine_events` instead of aborting the entire ingestion page. Until Phase 2 ships, validation failures still fail the task — treat as **High** severity if recurring.

**Planned recovery steps:**

1. Query quarantine rows for the pipeline run:
   ```sql
   SELECT event_id, failed_rule, failure_message, quarantined_at
   FROM bronze.quarantine_events
   WHERE pipeline_name = 'airflow-ingestion'
   ORDER BY quarantined_at DESC;
   ```
2. Fix upstream payload, contract rule, or required-field mapping.
3. Replay: insert corrected row into `bronze.raw_events` or delete quarantine row and re-run ingestion (idempotent `event_id` handling prevents duplicates).
4. Confirm silver/gold models exclude quarantined IDs via dbt source filters.

## Escalation

| Severity | Example | Response |
|----------|---------|----------|
| Low | Single retry succeeded | Log and monitor |
| Medium | DAG failed once, manual rerun fixed | Document in issue |
| High | Data corruption or extended outage | Stop downstream consumers, open incident issue |
