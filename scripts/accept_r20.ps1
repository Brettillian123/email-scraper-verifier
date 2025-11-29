Param(
    [string]$DbPath = "data\dev.db"
)

Write-Host "=== R20 acceptance: export pipeline ==="
Write-Host ""

# Resolve Python executable (virtualenv first, then fallback to system)
$PyExe = ".\.venv\Scripts\python.exe"
if (!(Test-Path $PyExe)) {
    $PyExe = "python"
}

Write-Host "→ Using SQLite at: $DbPath"
Write-Host ""

# 1. Run focused tests for the export stack
Write-Host "→ Running pytest for R20/O10/O11/R19 (export stack) ..."
& $PyExe -m pytest -k "r19_policy or o10_export_policy or o11_crm_suppression or r20_export" -q
if ($LASTEXITCODE -ne 0) {
    Write-Error "pytest failed, aborting R20 acceptance."
    exit 1
}
Write-Host ""

# 2. Inspect a small sample from v_emails_latest
Write-Host "→ Inspecting sample of v_emails_latest ..."
& $PyExe -m sqlite3 $DbPath "SELECT email, verify_status, icp_score FROM v_emails_latest LIMIT 5;"
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to query v_emails_latest."
    exit 1
}
Write-Host ""

# 3. Run export_leads.py against the target DB
$ExportPath = "tmp\r20_export_demo.csv"
if (!(Test-Path "tmp")) {
    New-Item -ItemType Directory -Path "tmp" | Out-Null
}

Write-Host "→ Running export_leads.py against $DbPath ..."
& $PyExe scripts\export_leads.py --db $DbPath --output $ExportPath --format csv
if ($LASTEXITCODE -ne 0) {
    Write-Error "export_leads.py failed."
    exit 1
}
Write-Host ""

Write-Host "→ Showing first 10 lines of export:"
Get-Content $ExportPath | Select-Object -First 10
Write-Host ""

Write-Host "✓ R20 acceptance checks completed successfully." -ForegroundColor Green
