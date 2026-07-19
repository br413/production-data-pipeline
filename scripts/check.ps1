# Run local validation and tests
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot '..')

python -m src.pipeline.ingestion --source sample --checkpoint .checkpoints/check.ps1.json
python -m pytest -q --ignore=tests/test_dbt_integration.py --ignore=tests/test_quality.py
Write-Host "Checks passed."
