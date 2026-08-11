# ADR 0004: Failed-record quarantine (dead-letter path)

## Status

Proposed

## Context

Today, `assert_quality_suite()` runs on each page batch before landing. Any validation failure raises `ValueError` and **aborts the entire ingestion run** — no partial landing, no durable record of which events failed or why.

That fail-fast behavior is appropriate for small, trusted sources in development. In production API ingestion it creates two problems:

1. **Poison-pill records** — one malformed event blocks an entire page (and sometimes the DAG) even when the rest of the batch is valid.
2. **No triage artifact** — operators must reproduce the failure from logs; failed payloads are not queryable alongside landed bronze rows.

The pipeline already tracks `duplicates_skipped` in `IngestionSummary` but has no counter or store for **rejected** records.

Related work elsewhere in the portfolio: `data-quality-observability` handles contract failures at the dataset layer; this ADR covers **row-level ingestion quarantine** before silver transforms.

## Decision

Introduce a **failed-record quarantine** path during ingestion:

1. **Record-level validation** — evaluate `run_quality_suite()` per record (or per record plus batch-level rules where required).
2. **Route outcomes:**
   - **Pass** → land in `bronze.raw_events` (unchanged).
   - **Fail (recoverable)** → write to `bronze.quarantine_events` with failure reason, rule name, raw payload, and ingestion run metadata.
   - **Fail (batch-level)** — uniqueness violations across a page still fail the page unless split into per-record checks with quarantine; document explicitly in implementation.
3. **Checkpoint behavior** — advance cursor and mark quarantined `event_id` values in `meta.processed_event_ids` so retries do not re-fetch the same poison pill indefinitely.
4. **Summary metrics** — extend `IngestionSummary` with `records_quarantined` and surface in CLI JSON + webhook alerts when quarantine count > 0.
5. **Operator recovery** — document replay procedure: fix payload or rule, move row from quarantine to bronze (or delete + source replay), documented in `docs/operations.md`.

### Proposed schema addition

```sql
CREATE TABLE IF NOT EXISTS bronze.quarantine_events (
    event_id TEXT NOT NULL,
    occurred_at TIMESTAMPTZ,
    payload JSONB NOT NULL,
    failed_rule TEXT NOT NULL,
    failure_message TEXT NOT NULL,
    pipeline_name TEXT NOT NULL,
    run_id TEXT NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_name, event_id, run_id)
);
```

`run_id` allows multiple quarantine attempts if an operator replays after a fix without losing audit history.

### Alerting

- Webhook event type `ingestion_quarantine` when `records_quarantined > 0` (alongside existing zero-record alert).
- Airflow task succeeds with quarantine count logged as warning — DAG does not fail unless **all** records in a run quarantined (configurable threshold).

## Consequences

**Positive**

- Poison-pill records no longer block valid events in the same page.
- Operators can `SELECT` quarantine rows for triage without log archaeology.
- Aligns with production patterns (DLQ / dead-letter tables) expected in senior DE portfolios.
- Clear extension point for a future silver model that excludes quarantined IDs.

**Negative**

- Per-record validation is slower than batch `assert_quality_suite` for large pages.
- Quarantine replay is manual in v1 — needs runbook discipline.
- Batch rules (uniqueness within page) require explicit design to avoid silent duplicates in bronze.

## Alternatives considered

1. **Keep fail-fast** — rejected; does not match production recovery requirements in the roadmap.
2. **Log-only DLQ (no table)** — rejected; not queryable, weak operational story.
3. **S3/jsonl sidecar file for failures** — rejected for postgres-backed deployment; acceptable fallback for `--storage file` mode only.
4. **Push failures to external queue (SQS/Kafka DLQ)** — deferred; out of scope for portfolio postgres focus.

## Implementation phases

| Phase | Scope |
|-------|-------|
| **1 (design)** | This ADR, GitHub issue, operations runbook stub |
| **2** | Schema + `QuarantineStore` + per-record routing in `ingest_incremental` |
| **3** | Summary metrics, webhook alert, Airflow log warnings |
| **4** | pytest coverage + dbt source exclusion for quarantined IDs |

## References

- Issue: [#31](https://github.com/br413/production-data-pipeline/issues/31)
- `src/pipeline/quality/validators.py` — current batch validation
- `docs/operations.md` — recovery procedures (to be extended)
