# Changelog

All notable changes to this project are documented in this file.

## [0.3.0] - 2026-08-27

### Added

- Quarantine volume metrics: `python -m src.pipeline.quarantine_metrics` for JSONL and PostgreSQL stores
- Ops runbook section with CLI examples and SQL breakdown by `failed_rule`
- Quality contract pins ([ADR 0005](docs/adr/0005-quality-contract-pins.md)) with `config/quality_contracts.yml`

## [0.2.1] - 2026-08-11

### Added

- Failed-record quarantine: `bronze.quarantine_events`, per-record validation routing, `records_quarantined` metric
- `--enable-quarantine` and `--alert-on-quarantine` CLI flags; webhook event type `ingestion_quarantine`
- dbt `stg_events` excludes quarantined event IDs
- ADR 0004: failed-record quarantine design and implementation

## [0.2.0] - 2026-08-11

### Added

- Structured `IngestionSummary` metrics (`pages_read`, `records_ingested`, `duplicates_skipped`, `final_cursor`) printed after each ingestion run
- Webhook alerts for zero-record ingestion runs and Airflow task failures (`PIPELINE_WEBHOOK_URL`, `python -m src.pipeline.notify`)
- Local validation script `scripts/check.ps1` for pytest, dbt, and DAG import checks
- Mermaid architecture diagram in README showing bronze → silver → gold flow

### Changed

- README reframed around problem, decisions, trade-offs, and operational outcomes
- PowerShell demo/check scripts now resolve repo root reliably on Windows

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

[0.3.0]: https://github.com/br413/production-data-pipeline/releases/tag/v0.3.0
[0.2.1]: https://github.com/br413/production-data-pipeline/releases/tag/v0.2.1
[0.2.0]: https://github.com/br413/production-data-pipeline/releases/tag/v0.2.0
[0.1.0]: https://github.com/br413/production-data-pipeline/releases/tag/v0.1.0
