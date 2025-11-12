# scripts/accept_r13.ps1
[CmdletBinding()]
param(
    [switch]$FullTests  # run entire pytest suite (requires httpx, respx, bs4)
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Resolve repo root and choose Python (prefer venv)
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$venvPy = Join-Path $repoRoot ".venv\Scripts\python.exe"
$PyExe = if (Test-Path $venvPy) { $venvPy } else { "python" }

function RunPy {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [string[]]$ArgList = @()
    )
    & $PyExe $Path @ArgList
    if ($LASTEXITCODE -ne 0) {
        throw "Python exited $LASTEXITCODE -> $PyExe $Path $($ArgList -join ' ')"
    }
}

# Paths
$dataDir = Join-Path $repoRoot "data"
$dbData = Join-Path $dataDir  "dev.db"  # canonical location
$dbRoot = Join-Path $repoRoot "dev.db"  # compatibility shim for certain scripts

# Env for child processes
New-Item -ItemType Directory -Force -Path $dataDir | Out-Null
$env:DATABASE_URL = "sqlite:///" + ($dbData -replace '\\', '/')

Write-Host "Using DB: $dbData"
Write-Host ""

# Fresh start to avoid ambiguity
if (Test-Path $dbData) { Write-Host "Removing existing DB at $dbData ..." ; Remove-Item $dbData -Force }
if (Test-Path $dbRoot) { Write-Host "Removing stray DB at $dbRoot ..."     ; Remove-Item $dbRoot -Force }

Write-Host "==> Applying schema ..."
RunPy (Join-Path $repoRoot 'scripts\apply_schema.py')  # creates data/dev.db

# --- Compatibility window for migration scripts that insist on .\dev.db ---
Copy-Item $dbData $dbRoot -Force

Write-Host "==> Running migrations (compat mode, absolute --db) ..."
$migrations = @(
    'migrate_r13_add_normalization.py',
    'migrate_o02_title_fields.py',
    'migrate_o03_company_attrs.py'
)
foreach ($m in $migrations) {
    $mPath = Join-Path $repoRoot "scripts\$m"
    if (Test-Path $mPath) {
        Write-Host "· $m ..."
        # Pass absolute path to avoid CWD assumptions in scripts
        RunPy $mPath @('--db', $dbRoot)
    }
    else {
        Write-Host "· Skipping $m (not found)"
    }
}

# Sync back to data/dev.db and clean up the root copy
Copy-Item $dbRoot $dbData -Force
Remove-Item $dbRoot -Force

Write-Host ""
Write-Host "==> Running tests ..."
if ($FullTests) {
    & $PyExe -m pytest -q
}
else {
    # A fast subset that covers normalization + CSV ingest + (optional) O03
    & $PyExe -m pytest -q -k "normalization or ingest_csv_jsonl or o03_company_enrichment"
}
if ($LASTEXITCODE -ne 0) { throw "pytest exited $LASTEXITCODE" }

Write-Host ""
Write-Host "==> Ingesting samples/leads.csv ..."
$sampleCsv = Join-Path $repoRoot 'samples\leads.csv'
if (!(Test-Path $sampleCsv)) { throw "Expected sample CSV not found at: $sampleCsv" }
RunPy (Join-Path $repoRoot 'scripts\ingest_csv.py') @($sampleCsv)

Write-Host ""
Write-Host "==> Backfilling O02 role_family/seniority (if available) ..."
$backfill = Join-Path $repoRoot 'scripts\backfill_o02_roles.py'
if (Test-Path $backfill) {
    RunPy $backfill @()
}
else {
    Write-Host "· Skipping backfill_o02_roles.py (not found)"
}

Write-Host ""
Write-Host "==> Inspecting DB for normalized fields ..."

# Write the inspector script to a temp file to avoid -c quoting issues
$inspector = Join-Path $dataDir 'inspect_r13.py'
$py = @'
import os, sqlite3, sys

db_url = os.environ.get("DATABASE_URL")
if not db_url or not db_url.startswith("sqlite:///"):
    print("DATABASE_URL missing or not sqlite:///...", file=sys.stderr)
    sys.exit(1)

db = db_url.replace("sqlite:///", "")
print("DATABASE_URL =", db_url)

conn = sqlite3.connect(db)
cur = conn.cursor()

def cols(t):
    return {r[1] for r in cur.execute(f"PRAGMA table_info({t})")}

p = cols("people")
c = cols("companies")
print("People columns:", sorted(p))
print("Companies columns:", sorted(c))

sel_p = ["first_name", "last_name"]
for col in ["title", "title_raw", "title_norm", "role_family", "seniority", "source_url"]:
    if col in p:
        sel_p.append(col)

print("\n--- People ---")
for row in cur.execute("SELECT " + ",".join(sel_p) + " FROM people ORDER BY rowid DESC LIMIT 5"):
    print(row)

sel_c = ["name", "domain"]
for col in ["name_norm", "norm_key", "attrs"]:
    if col in c:
        sel_c.append(col)

print("\n--- Companies ---")
for row in cur.execute("SELECT " + ",".join(sel_c) + " FROM companies ORDER BY rowid DESC LIMIT 5"):
    print(row)
'@

Set-Content -Path $inspector -Value $py -Encoding UTF8
& $PyExe $inspector
if ($LASTEXITCODE -ne 0) { throw "Python inspection step failed with $LASTEXITCODE" }
Remove-Item $inspector -Force

Write-Host ""
Write-Host "✔ R13 acceptance complete."
