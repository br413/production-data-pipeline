# Contributing

## Workflow

1. Open or select an issue.
2. Discuss significant design changes before implementation.
3. Create a focused branch.
4. Add or update tests.
5. Run all quality checks locally.
6. Open a pull request with technical context and validation evidence.

## Commit guidance

Use concise, meaningful messages:

```text
feat: add incremental API ingestion
fix: preserve checkpoint after retry
test: cover duplicate-event handling
docs: record partitioning decision
```

Preserve accurate authorship. Use co-authorship only when both parties genuinely contributed.

## Pull-request standard

A pull request should explain:

- Problem
- Approach
- Alternatives considered
- Testing performed
- Operational impact
- Rollback strategy, when relevant
