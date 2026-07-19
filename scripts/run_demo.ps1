# Run the portfolio demo end-to-end (requires Docker PostgreSQL)
param(
    [string]$PipelineName = "demo-ingestion"
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot '..')

Write-Host "Starting PostgreSQL..."
docker compose up -d

Write-Host "Running ingestion..."
python -m src.pipeline.ingestion `
    --source sample `
    --storage postgres `
    --pipeline-name $PipelineName

Write-Host "Running dbt transforms..."
$env:DBT_TARGET = "dev"
python -m src.pipeline.run_dbt --target dev

Write-Host "Running tests..."
python -m pytest -q

Write-Host "Demo complete. See docs/demo.md for recording script."
