param(
    [string]$DbPath = "data\dev.db",
    [string]$PyExe = "python"
)

$ErrorActionPreference = "Stop"

Write-Host "=== R25 acceptance: QA & end-to-end tests ===" -ForegroundColor Cyan

# Resolve DB path and URL
$DbFullPath = Resolve-Path $DbPath
$env:DB_URL = "sqlite:///$($DbFullPath.Path.Replace('\','/'))"

Write-Host "→ Using SQLite at: $DbFullPath"
Write-Host "→ Using Python at: $PyExe"
Write-Host "→ Existing DB_URL: $($env:DB_URL)"

# Ensure a default ADMIN_API_KEY so admin routes/CLI work
if (-not $env:ADMIN_API_KEY) {
    $env:ADMIN_API_KEY = "dev-admin-key"  # pragma: allowlist secret
    Write-Host "→ Using default ADMIN_API_KEY=dev-admin-key"
}

# 1) Apply schema + seed dev data
Write-Host "`n→ Ensuring schema + seed data..." -ForegroundColor Yellow

& $PyExe .\scripts\apply_schema.py
if ($LASTEXITCODE -ne 0) {
    throw "apply_schema.py failed with exit code $LASTEXITCODE"
}

if (Test-Path .\scripts\seed_dev.py) {
    & $PyExe .\scripts\seed_dev.py
    if ($LASTEXITCODE -ne 0) {
        throw "seed_dev.py failed with exit code $LASTEXITCODE"
    }
}
else {
    Write-Host "↪ seed_dev.py not found; skipping dev seed step." -ForegroundColor DarkYellow
}

# 2) Run focused pytest suite including robots, R20–R24, optionals, and R25
Write-Host "`n→ Running focused pytest suite for R25 ..." -ForegroundColor Yellow

& $PyExe -m pytest `
    tests/test_robots_enforcement.py `
    tests/test_r20_export_pipeline.py `
    tests/test_r21_search_indexing.py `
    tests/test_r22_search_backend.py `
    tests/test_r22_api.py `
    tests/test_r23_facets_backend.py `
    tests/test_r24_admin_ui.py `
    tests/test_o14_facets_mv.py `
    tests/test_o15_search_cache.py `
    tests/test_o17_analytics_diagnostics.py `
    tests/test_o20_cli_admin_status.py `
    tests/test_o23_admin_auth.py `
    tests/test_r25_qa_acceptance.py

if ($LASTEXITCODE -ne 0) {
    throw "pytest suite failed with exit code $LASTEXITCODE"
}

# 3) Run CLI ingest + export end-to-end using R25 fixture batch
Write-Host "`n→ Running CLI ingest + export using R25 fixture batch..." -ForegroundColor Yellow

$FixturePath = "tests\fixtures\r25_e2e_batch.csv"
if (-not (Test-Path $FixturePath)) {
    throw "R25 fixture batch not found at $FixturePath"
}

& $PyExe .\scripts\ingest_csv.py $FixturePath
if ($LASTEXITCODE -ne 0) {
    throw "ingest_csv.py failed with exit code $LASTEXITCODE"
}

$ExportPath = "data\r25_export.csv"

& $PyExe .\scripts\export_leads.py --output $ExportPath
if ($LASTEXITCODE -ne 0) {
    throw "export_leads.py failed with exit code $LASTEXITCODE"
}

if (-not (Test-Path $ExportPath)) {
    throw "Expected export file at $ExportPath but it was not created."
}

$exportInfo = Get-Item $ExportPath
if ($exportInfo.Length -le 0) {
    throw "Export file at $ExportPath is empty."
}

Write-Host "→ Export file created at $ExportPath (size: $($exportInfo.Length) bytes)"

# 4) Smoke-test admin CLI status (O20) against the same DB/API
Write-Host "`n→ Running admin status CLI smoke test..." -ForegroundColor Yellow

& $PyExe -m src.cli admin status --json
if ($LASTEXITCODE -ne 0) {
    throw "admin status CLI failed with exit code $LASTEXITCODE"
}

Write-Host "`n✔ R25 acceptance passed." -ForegroundColor Green
