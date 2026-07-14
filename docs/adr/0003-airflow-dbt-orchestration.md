# ADR 0003: Airflow and dbt orchestration

## Status

Accepted

## Context

Week 3 requires scheduled execution, transformation layers, and operational documentation suitable for a v0.1.0 release.

## Decision

- Orchestrate ingestion and dbt with Airflow DAG `production_sample_ingestion`
- Use BashOperators invoking existing Python modules for portability
- Model silver and gold layers in dbt with tests and `dbt_utils` helpers
- Document monitoring, retries, and recovery in `docs/operations.md`

## Consequences

**Positive**

- Clear separation between ingestion (Python), transformation (dbt), and scheduling (Airflow)
- Runnable end-to-end path for demos and CI
- Versioned release with changelog and runbook

**Negative**

- Airflow full deployment is not bundled in Docker Compose; local demo uses `airflow dags test` or standalone mode
- BashOperators are simple but less observable than custom operators

## Alternatives considered

1. **Prefect or Dagster** — rejected to align with roadmap technology choices
2. **Python-only transforms** — rejected; dbt demonstrates analytics engineering practices
