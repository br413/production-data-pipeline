## [0.2.0] - 2026-08-11

### Added

- Structured `IngestionSummary` metrics (`pages_read`, `records_ingested`, `duplicates_skipped`, `final_cursor`) printed after each ingestion run
- Webhook alerts for zero-record ingestion runs and Airflow task failures (`PIPELINE_WEBHOOK_URL`, `python -m src.pipeline.notify`)
- Local validation script `scripts/check.ps1` for pytest, dbt, and DAG import checks
- Mermaid architecture diagram in README showing bronze → silver → gold flow

### Changed

- README reframed around problem, decisions, trade-offs, and operational outcomes
- PowerShell demo/check scripts now resolve repo root reliably on Windows

**Full changelog:** https://github.com/br413/production-data-pipeline/blob/main/CHANGELOG.md
