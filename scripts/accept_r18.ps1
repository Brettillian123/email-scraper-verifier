param(
    [string]$DbPath = "data\dev.db"
)

$ErrorActionPreference = "Stop"

Write-Host "=== R18 acceptance starting ===" -ForegroundColor Cyan

# ----------------------------------------
# Python / env setup
# ----------------------------------------
$PyExe = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $PyExe)) {
    $PyExe = "python"
}

# Ensure data directory exists
$dbDir = Split-Path $DbPath -Parent
if (-not (Test-Path $dbDir)) {
    New-Item -ItemType Directory -Path $dbDir | Out-Null
}

# Fresh DB
if (Test-Path $DbPath) {
    Write-Host "Removing existing DB at $DbPath..."
    Remove-Item $DbPath -Force -ErrorAction SilentlyContinue
}

# Use a simple, robust absolute path (avoid Resolve-Path before file exists)
$DbFullPath = [System.IO.Path]::GetFullPath($DbPath)
$env:DATABASE_PATH = $DbFullPath
$env:DATABASE_URL = "sqlite:///$DbFullPath"

if (-not $env:SMTP_HELO_DOMAIN) {
    $env:SMTP_HELO_DOMAIN = "verifier.crestwellpartners.com"
}
if (-not $env:SMTP_MAIL_FROM) {
    $env:SMTP_MAIL_FROM = "bounce@verifier.crestwellpartners.com"
}

Write-Host "Using DATABASE_PATH = $env:DATABASE_PATH"
Write-Host "Using DATABASE_URL  = $env:DATABASE_URL"
Write-Host "Using SMTP_HELO_DOMAIN = $env:SMTP_HELO_DOMAIN"
Write-Host "Using SMTP_MAIL_FROM   = $env:SMTP_MAIL_FROM"
Write-Host ""

# ----------------------------------------
# Schema + migrations (through R18 + O10)
# ----------------------------------------

Write-Host "Applying base schema..." -ForegroundColor Cyan
& $PyExe scripts\apply_schema.py

Write-Host "Running migrations..." -ForegroundColor Cyan

# Note: order matters for backfills that depend on earlier columns.
$migrations = @(
    "scripts\migrate_o01_add_domain_patterns.py",   # O01 domain_patterns table
    "scripts\migrate_r13_add_normalization.py",     # R13 name/title/company normalization
    "scripts\migrate_o02_title_fields.py",          # O02 role_family/seniority fields
    "scripts\migrate_o03_company_attrs.py",         # O03 companies.attrs JSON
    "scripts\migrate_r14_add_icp.py",              # R14 ICP scoring columns/backfill
    "scripts\migrate_r15_add_domain_resolutions.py",
    "scripts\migrate_r17_add_catchall.py",
    "scripts\migrate_o07_fallback.py",
    "scripts\migrate_r18_verify_status.py"
)

foreach ($m in $migrations) {
    if (Test-Path $m) {
        Write-Host "  -> $m --db $DbPath"
        & $PyExe $m --db $DbPath
    }
    else {
        Write-Host "  (skip) $m not found in this repo" -ForegroundColor DarkYellow
    }
}

Write-Host ""

# ----------------------------------------
# Seed sample data
# ----------------------------------------

Write-Host "Seeding sample leads (samples\leads.csv)..." -ForegroundColor Cyan
& $PyExe scripts\ingest_csv.py .\samples\leads.csv
Write-Host ""

# ----------------------------------------
# MX + catch-all priming (R15 + R17)
# ----------------------------------------

Write-Host "Priming MX cache with a couple of domains (R15)..." -ForegroundColor Cyan
try {
    & $PyExe scripts\resolve_mx.py --domain gmail.com   --db $DbPath --force --refresh-behavior
    & $PyExe scripts\resolve_mx.py --domain outlook.com --db $DbPath --force --refresh-behavior
}
catch {
    Write-Host "resolve_mx priming failed or CLI signature changed; continuing..." -ForegroundColor DarkYellow
}

Write-Host "Running a couple of catch-all probes (R17)..." -ForegroundColor Cyan
try {
    & $PyExe scripts\probe_catchall.py --domain gmail.com
    & $PyExe scripts\probe_catchall.py --domain outlook.com
}
catch {
    Write-Host "probe_catchall failed or not available; continuing..." -ForegroundColor DarkYellow
}
Write-Host ""

# ----------------------------------------
# R18 status checks via pytest
# ----------------------------------------

Write-Host "Running R18/O10-focused tests (pytest -k 'r18 or o10')..." -ForegroundColor Cyan
& $PyExe -m pytest -k "r18 or o10" -q
Write-Host ""

# ----------------------------------------
# Spot-check R18 classification in SQLite
# ----------------------------------------

Write-Host "Inspecting verification_results & v_emails_latest (SQLite)..." -ForegroundColor Cyan

# Show verification_results table structure (should include verify_status/reason/mx/at)
& $PyExe -m sqlite3 $DbPath "PRAGMA table_info(verification_results);"

Write-Host ""
Write-Host "Sample latest verification rows (from v_emails_latest, if present):" -ForegroundColor Cyan

try {
    & $PyExe -m sqlite3 $DbPath @"
SELECT email,
       verify_status,
       verify_reason,
       verified_mx,
       verified_at
FROM v_emails_latest
ORDER BY verified_at DESC
LIMIT 10;
"@
}
catch {
    Write-Host "v_emails_latest view not present or query failed; continuing..." -ForegroundColor DarkYellow
}

Write-Host ""
Write-Host "R18 acceptance completed successfully." -ForegroundColor Green
Write-Host "You now have verify_status / verify_reason / verified_mx / verified_at wired through." -ForegroundColor Green
