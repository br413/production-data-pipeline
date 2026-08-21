# ADR 0005: Quality contract pins (cross-repo)

## Status

Accepted

## Context

[ADR 0004](0004-failed-record-quarantine.md) covers row-level quarantine at ingestion. [data-quality-observability ADR 0002](https://github.com/br413/data-quality-observability/blob/main/docs/adr/0002-schema-registry-and-contract-versioning.md) adds a file-based contract registry with semver pins and run-history attribution.

Operators need a **single config** in this repo that declares which dataset contracts apply after bronze landing — without hard-coding paths to sibling repos.

## Decision

Add `config/quality_contracts.yml`:

```yaml
pins:
  orders: orders@1.0
  customers: customers@1.0

dqo:
  project_root: ../data-quality-observability
```

`src/pipeline/quality_contracts.py` parses `name@version` pins and resolves the dqo project root for full-stack demos.

**Boundary:**

| Layer | Project | Gate |
|-------|---------|------|
| Row-level poison pills | `production-data-pipeline` | Quarantine at ingestion |
| Dataset contract | `data-quality-observability` | Registry + CLI + run history |

Pins are documentation-first for portfolio demos; production deployments would inject `DQO_PROJECT_ROOT` and run `dqo.cli` after export/landing.

## Consequences

**Positive**

- Full-stack story is config-driven, not README-only
- Version pins match registry `current` values — auditable in code review
- Clear handoff between ingestion and quality repos

**Negative**

- Assumes sibling checkout layout for local demos (`../data-quality-observability`)
- Does not auto-run dqo from the Airflow DAG yet (optional follow-on)

## References

- [data-quality-observability registry](https://github.com/br413/data-quality-observability/blob/main/contracts/registry.yml)
- [Data Quality Contracts article](https://dev.to/bobby_ray_581732c715283b2/data-quality-contracts-in-production-pipelines-without-a-separate-platform-team-f3)
