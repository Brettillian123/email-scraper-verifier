# R19 acceptance: suppression, dedupe, and export policy wiring
param(
    [string]$DbPath = "data\dev.db"
)

$ErrorActionPreference = "Stop"

function Assert-Step {
    param(
        [string]$Step
    )
    if ($LASTEXITCODE -ne 0) {
        throw "Step failed: $Step (exit code $LASTEXITCODE)"
    }
}

# --- choose Python ---------------------------------------------------------
$PyExe = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $PyExe)) {
    $PyExe = "python"
}

Write-Host ""
Write-Host "=== R19 acceptance: suppression, dedupe, export policies ===" -ForegroundColor Cyan

# Resolve DB path (will throw if it doesn't exist)
$ResolvedDb = Resolve-Path $DbPath
Write-Host "→ Using SQLite at: $($ResolvedDb.Path)" -ForegroundColor Cyan

# ---------------------------------------------------------------------------
# 1) Enforce unique email invariant (R19 core dedupe)
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "→ Enforcing unique email invariant on emails.email ..." -ForegroundColor Yellow
& $PyExe scripts\apply_unique_email_index.py --db $DbPath
Assert-Step "scripts/apply_unique_email_index.py"

# ---------------------------------------------------------------------------
# 2) Run focused tests for R19 / O10 / O11
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "→ Running pytest for R19/O10/O11 (export policy + suppression) ..." -ForegroundColor Yellow
& $PyExe -m pytest -k "r19 or o10 or o11" -q
Assert-Step "pytest -k 'r19 or o10 or o11'"

# ---------------------------------------------------------------------------
# 3) Seed demo suppression rows (email + domain) for the export demo
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "→ Seeding demo suppression entries for accept_r19 ..." -ForegroundColor Yellow

# Clean out old demo rows if present (safe if they don't exist).
& $PyExe -m sqlite3 $DbPath "DELETE FROM suppression WHERE email = 'blocked@example.com';"
& $PyExe -m sqlite3 $DbPath "DELETE FROM suppression WHERE domain = 'suppressed-domain.test';"

# Insert fresh demo rows.
& $PyExe -m sqlite3 $DbPath "INSERT INTO suppression (email, reason, source, created_at) VALUES ('blocked@example.com', 'manual_test', 'accept_r19', CURRENT_TIMESTAMP);"
Assert-Step "seed suppression email=blocked@example.com"

& $PyExe -m sqlite3 $DbPath "INSERT INTO suppression (domain, reason, source, created_at) VALUES ('suppressed-domain.test', 'manual_test', 'accept_r19', CURRENT_TIMESTAMP);"
Assert-Step "seed suppression domain=suppressed-domain.test"

# ---------------------------------------------------------------------------
# 4) Run the export policy demo to visually confirm behavior
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "→ Running demo_export_policy.py (R19 export decisions) ..." -ForegroundColor Yellow
& $PyExe scripts\demo_export_policy.py --db $DbPath
Assert-Step "scripts/demo_export_policy.py"

Write-Host ""
Write-Host "✔ R19 acceptance PASSED: suppression, dedupe, and export policies look good." -ForegroundColor Green
Write-Host ""
