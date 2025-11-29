# scripts/accept_r21.ps1
param(
    [string]$DbPath = "data\dev.db"
)

$ErrorActionPreference = "Stop"

Write-Host "=== R21 acceptance: search indexing prep ==="
Write-Host ""

Write-Host "→ Using SQLite at: $DbPath"

# Resolve Python executable (virtualenv preferred)
$PyExe = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $PyExe)) {
    $PyExe = "python"
}

Write-Host ""
Write-Host "→ Applying base schema ..."
& $PyExe scripts\apply_schema.py

Write-Host ""
Write-Host "→ Running R21 search indexing migration ..."
& $PyExe scripts\migrate_r21_search_indexing.py --db $DbPath

Write-Host ""
Write-Host "→ Running pytest for R21 + export pipeline sanity ..."
& $PyExe -m pytest `
    tests/test_r21_search_indexing.py `
    tests/test_r20_export_pipeline.py `
    -q

if ($LASTEXITCODE -ne 0) {
    Write-Error "Pytest failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

Write-Host ""
Write-Host "✓ R21 acceptance checks completed successfully."
exit 0
