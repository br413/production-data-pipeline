CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS bronze.raw_events (
    event_id TEXT PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS meta.pipeline_checkpoints (
    pipeline_name TEXT PRIMARY KEY,
    cursor TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS meta.processed_event_ids (
    pipeline_name TEXT NOT NULL REFERENCES meta.pipeline_checkpoints (pipeline_name) ON DELETE CASCADE,
    event_id TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_name, event_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_events_occurred_at
    ON bronze.raw_events (occurred_at DESC);

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

CREATE INDEX IF NOT EXISTS idx_quarantine_events_pipeline
    ON bronze.quarantine_events (pipeline_name, quarantined_at DESC);
