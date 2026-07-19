# Production Data Pipeline

> **Incremental ETL pipeline** for API ingestion with checkpointing, PostgreSQL bronze landing, dbt transformations, and Apache Airflow orchestration — a reference implementation for data engineers building reliable cloud data platforms.

[![CI](https://github.com/br413/production-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/br413/production-data-pipeline/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/br413/production-data-pipeline?label=release&style=flat-square)](https://github.com/br413/production-data-pipeline/releases)
[![Python 3.12](https://img.shields.io/badge/python-3.12-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![dbt](https://img.shields.io/badge/dbt-transformations-FF694B?style=flat-square&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Apache-Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green?style=flat-square)](LICENSE)

A production-style **data engineering** portfolio project demonstrating incremental API ingestion, idempotent loads, medallion-style layering (bronze → silver → gold), and operational monitoring — patterns used in modern **ETL/ELT pipelines** on cloud data platforms.

## Why this project exists

Operational analytics depends on pipelines that survive API rate limits, partial failures, and schema drift without silent data loss. This repository demonstrates a production-style ingestion pattern: idempotent loads, explicit checkpoints, and separable transformation layers that can be tested independently.

**Ideal for:** data engineers evaluating incremental loading patterns, teams prototyping bronze/silver/gold architectures, and hiring managers reviewing pipeline design skills.

## Architecture

```text
External API
    ↓
Ingestion connector (incremental, idempotent)
    ↓
Raw / bronze layer (PostgreSQL landing)
    ↓
Validated / silver layer (dbt `stg_events`)
    ↓
Curated / gold layer (`fct_daily_event_metrics`)
    ↓
Downstream consumers
```

See [`docs/architecture.md`](docs/architecture.md) for component boundaries and failure modes.

## Current capabilities

- [x] Incremental ingestion with JSON-file or PostgreSQL checkpoint store
- [x] Idempotent record handling via stable event IDs
- [x] PostgreSQL bronze landing layer with Docker Compose
- [x] Data-quality checks: schema, required fields, uniqueness, freshness
- [x] dbt silver (`stg_events`) and gold (`fct_daily_event_metrics`) models
- [x] Airflow DAG `production_sample_ingestion` with retries
- [x] Ingestion summary metrics and webhook alerts for zero-record runs
- [x] Operations runbook and demo script
- [x] Unit and integration tests with CI PostgreSQL service
- [x] CI workflow on push and pull request
- [ ] Production Airflow deployment (local `airflow dags test` supported)

## Technology stack

| Area | Selection |
|------|-----------|
| Language | Python 3.12 |
| Storage | PostgreSQL bronze + metadata, optional JSON checkpoints |
| Orchestration | Apache Airflow (`production_sample_ingestion`) |
| Transformation | dbt (silver + gold models) |
| Testing | pytest |
| Deployment | Docker Compose (local), CI via GitHub Actions |

## Quick start

```bash
git clone https://github.com/br413/production-data-pipeline.git
cd production-data-pipeline
python -m venv .venv
```

Windows:

```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pytest
```

Linux/macOS:

```bash
source .venv/bin/activate
pip install -r requirements.txt
pytest
```

Run the sample ingestion (file checkpoints):

```bash
python -m src.pipeline.ingestion --source sample --checkpoint .checkpoints/sample.json
```

Run with PostgreSQL landing and DB checkpoints:

```bash
docker compose up -d
python -m src.pipeline.ingestion --source sample --storage postgres --pipeline-name sample-ingestion
python -m src.pipeline.run_dbt --target dev
```

Run the full demo script (Windows):

```powershell
.\scripts\run_demo.ps1
```

## Project structure

```text
.
├── .github/
│   ├── ISSUE_TEMPLATE/
│   ├── workflows/ci.yml
│   └── pull_request_template.md
├── dags/
│   └── production_pipeline_dag.py
├── dbt/
│   ├── models/
│   └── profiles.yml
├── docs/
│   ├── architecture.md
│   ├── demo.md
│   ├── operations.md
│   └── adr/
├── scripts/
│   └── run_demo.ps1
├── src/
│   └── pipeline/
├── tests/
├── docker-compose.yml
├── README.md
├── CONTRIBUTING.md
├── SECURITY.md
└── requirements.txt
```

## Engineering decisions

Architectural Decision Records are stored in [`docs/adr/`](docs/adr/).

## Testing

```bash
pytest -v
```

Coverage includes checkpoint persistence, duplicate suppression, incremental cursor advancement, PostgreSQL landing writes, data-quality validation, dbt transforms, and Airflow DAG integrity.

## Operations

| Concern | Approach |
|---------|----------|
| Scheduling | Airflow DAG `@daily` with 2 retries |
| Monitoring | Ingestion metrics + webhook alerts — see [`docs/operations.md`](docs/operations.md) |
| Retries | Airflow 5-minute backoff; manual rerun documented |
| Backfills | Checkpoint reset with documented procedure |
| Recovery | Checkpoint replay from last successful cursor |
| Secrets | Environment variables; never committed |
| Demo | [`docs/demo.md`](docs/demo.md) walkthrough script |

## Related projects

| Project | Focus |
|---------|-------|
| [**data-quality-observability**](https://github.com/br413/data-quality-observability) | Contract-driven data quality checks with history and alert routing |
| [**cloud-lakehouse-blueprint**](https://github.com/br413/cloud-lakehouse-blueprint) | Medallion lakehouse architecture with Terraform IaC |
| [**@br413**](https://github.com/br413) | Senior Data Engineer & Data Architect portfolio |

## Roadmap

Tracked via GitHub Issues and milestones. Week 4 career materials:

- [`docs/career/resume-snippets.md`](docs/career/resume-snippets.md)
- [`docs/career/linkedin-setup.md`](docs/career/linkedin-setup.md)
- [`docs/career/outreach-tracker.md`](docs/career/outreach-tracker.md)

## Topics

`data-engineering` · `etl` · `elt` · `data-pipeline` · `incremental-loading` · `airflow` · `dbt` · `python` · `postgresql` · `data-platform` · `bronze-silver-gold`

## Attribution

Built as a public portfolio project by [@br413](https://github.com/br413) — Senior Data Engineer & Data Architect. Sample data is synthetic for demonstration.

## License

MIT — see [LICENSE](LICENSE).
