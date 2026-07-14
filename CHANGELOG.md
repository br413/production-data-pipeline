# Changelog

All notable changes to this project are documented in this file.

## [0.1.0] - 2026-07-14

### Added

- Incremental, idempotent API ingestion with file and PostgreSQL checkpoint stores
- PostgreSQL bronze landing layer (`bronze.raw_events`)
- Data-quality validators: schema, required fields, uniqueness, freshness
- dbt silver model `stg_events` and gold model `fct_daily_event_metrics`
- Airflow DAG `production_sample_ingestion` with retries and daily schedule
- Operations runbook with monitoring, recovery, and backfill procedures
- Demo script for portfolio walkthrough
- CI with PostgreSQL service, pytest, dbt run/test, and DAG validation

### Changed

- Refactored ingestion to use pluggable storage adapters
- Extended schema with `silver` and `gold` layers

[0.1.0]: https://github.com/br413/production-data-pipeline/releases/tag/v0.1.0
