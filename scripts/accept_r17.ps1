# scripts/accept_r17.ps1
param(
    [string]$Db = "data\dev.db"
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "======================================" -ForegroundColor Cyan
Write-Host " R17 Acceptance – Catch-all Detection " -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""

# Resolve repo root (one level up from scripts/)
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Push-Location $repoRoot
try {
    # -----------------------------
    # Python + DB environment
    # -----------------------------
    $PyExe = ".\.venv\Scripts\python.exe"
    if (-not (Test-Path $PyExe)) {
        $PyExe = "python"
    }

    $dbPath = Resolve-Path $Db
    Write-Host "Using DB: $dbPath"

    $env:DATABASE_PATH = $dbPath.Path
    $env:DATABASE_URL = "sqlite:///$($env:DATABASE_PATH)"
    $env:SMTP_HELO_DOMAIN = "verifier.crestwellpartners.com"
    $env:SMTP_MAIL_FROM = "bounce@verifier.crestwellpartners.com"

    # -----------------------------
    # Schema & migrations
    # -----------------------------
    Write-Host ""
    Write-Host "==> Applying schema & migrations" -ForegroundColor Yellow

    & $PyExe "scripts/apply_schema.py"
    & $PyExe "scripts/migrate_r15_add_domain_resolutions.py" --db $dbPath
    & $PyExe "scripts/migrate_o06_mx_behavior.py"            --db $dbPath
    & $PyExe "scripts/migrate_r17_add_catchall.py"           --db $dbPath

    Write-Host "✔ Migrations applied (R15 + O06 + R17 catch-all columns)." -ForegroundColor Green

    # -----------------------------
    # Seed domains (ingest + MX)
    # -----------------------------
    Write-Host ""
    Write-Host "==> Seeding domains from samples/leads.csv" -ForegroundColor Yellow
    & $PyExe "scripts/ingest_csv.py" "samples/leads.csv" --db $dbPath

    Write-Host ""
    Write-Host "==> Resolving MX for seeded domains (R15)" -ForegroundColor Yellow
    & $PyExe "scripts/resolve_mx.py" --db $dbPath --from-db

    # -----------------------------
    # Catch-all probes (R17 CLI)
    # -----------------------------
    Write-Host ""
    Write-Host "==> Running catch-all probes (scripts/probe_catchall.py)" -ForegroundColor Yellow

    $domains = @("gmail.com", "outlook.com")
    foreach ($d in $domains) {
        Write-Host ""
        Write-Host "---- Domain: $d ----" -ForegroundColor Cyan

        Write-Host "First probe (should be fresh)..." -ForegroundColor DarkGray
        & $PyExe "scripts/probe_catchall.py" --domain $d

        Write-Host "Second probe (should be cached)..." -ForegroundColor DarkGray
        & $PyExe "scripts/probe_catchall.py" --domain $d

        Write-Host "Forced probe (bypass cache)..." -ForegroundColor DarkGray
        & $PyExe "scripts/probe_catchall.py" --domain $d --force
    }

    # Optionally probe a project-specific domain if present
    Write-Host ""
    Write-Host "Optional: probing crestwellpartners.com (if MX resolvable)..." -ForegroundColor DarkGray
    try {
        & $PyExe "scripts/probe_catchall.py" --domain "crestwellpartners.com"
    }
    catch {
        Write-Host "Skipping crestwellpartners.com probe (error: $($_.Exception.Message))" -ForegroundColor DarkYellow
    }

    # -----------------------------
    # Show DB state
    # -----------------------------
    Write-Host ""
    Write-Host "==> Inspecting domain_resolutions catch-all columns" -ForegroundColor Yellow

    $sql = @"
SELECT domain,
       lowest_mx,
       catch_all_status,
       catch_all_checked_at,
       catch_all_smtp_code
FROM domain_resolutions
ORDER BY id DESC
LIMIT 10;
"@

    & $PyExe -m sqlite3 $dbPath $sql

    # -----------------------------
    # Run tests
    # -----------------------------
    Write-Host ""
    Write-Host "==> Running pytest -k 'r17'" -ForegroundColor Yellow
    & $PyExe -m pytest -k "r17" -q

    Write-Host ""
    Write-Host "✔ R17 acceptance checks completed (schema, CLI, caching, tests)" -ForegroundColor Green
}
finally {
    Pop-Location
}
