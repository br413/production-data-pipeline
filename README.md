# production-data-pipeline

> Incremental API ingestion with checkpointing, validated landing storage, and testable transformation boundaries.

## Why this project exists

Operational analytics depends on pipelines that survive API rate limits, partial failures, and schema drift without silent data loss. This repository demonstrates a production-style ingestion pattern: idempotent loads, explicit checkpoints, and separable transformation layers that can be tested independently.

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
- [x] Operations runbook and demo script
- [x] Unit and integration tests with CI PostgreSQL service
- [x] CI workflow on push and pull request
- [ ] Production Airflow deployment (local `airflow dags test` supported)

## Technology

| Area | Selection |
|------|-----------|
| Language | Python 3.12 |
| Storage | PostgreSQL bronze + metadata, optional JSON checkpoints |
| Orchestration | Airflow (`production_sample_ingestion`) |
| Transformation | dbt (silver + gold) |
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
| Monitoring | See [`docs/operations.md`](docs/operations.md) |
| Retries | Airflow 5-minute backoff; manual rerun documented |
| Backfills | Checkpoint reset with documented procedure |
| Recovery | Checkpoint replay from last successful cursor |
| Secrets | Environment variables; never committed |
| Demo | [`docs/demo.md`](docs/demo.md) walkthrough script |

## Roadmap

Tracked via GitHub Issues and milestones:

1. Repository foundation
2. Ingestion connector
3. Idempotency and checkpoints
4. Transformation models
5. Data-quality rules
6. Orchestration
7. CI/CD hardening
8. Monitoring and retry strategy
9. Documentation
10. Versioned release

## Attribution

Built as a public portfolio project by [@br413](https://github.com/br413). Sample data is synthetic for demonstration.

## License

MIT — see [LICENSE](LICENSE).
