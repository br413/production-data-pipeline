# v0.3.0 — Quarantine metrics & contract pins

## Added

- Quarantine volume metrics: `python -m src.pipeline.quarantine_metrics` for JSONL and PostgreSQL stores
- Ops runbook section with CLI examples and SQL breakdown by `failed_rule`
- Quality contract pins ([ADR 0005](docs/adr/0005-quality-contract-pins.md)) with `config/quality_contracts.yml`

## Upgrade notes

No schema changes. Existing quarantine tables and JSONL sidecars work unchanged; run the metrics CLI after ingestion to inspect volume by rule.
