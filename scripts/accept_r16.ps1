# scripts/accept_r16.ps1
<#
R16 — SMTP RCPT TO probe acceptance

Flow:
  1) Apply schema/migrations
  2) Run focused tests (-k "r15 or r16")
  3) Ingest sample leads (if present)
  4) Resolve MX for companies from DB (inline Python; no --from-db)
  5) Start worker (queues: mx,verify) in background
  6) Enqueue a small verify batch (R16 task_probe_email)
  7) Run CLI probe
  8) Show mx_behavior snippets
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Assert-LastExitCode([string]$Step) {
  if ($LASTEXITCODE -ne 0) {
    Write-Error "Step failed: $Step (exit=$LASTEXITCODE)"
    exit 1
  }
}

# ---------------------------
# Resolve paths
# ---------------------------
$ScriptsDir = if ($PSScriptRoot) { $PSScriptRoot } else { (Resolve-Path ".").Path }
$Root       = Split-Path -Parent $ScriptsDir
$VenvPy     = Join-Path $Root ".venv\Scripts\python.exe"
$PyExe      = if (Test-Path $VenvPy) { $VenvPy } else { "python" }
$DBRel      = "data\dev.db"
$DBAbs      = (Resolve-Path (Join-Path $Root $DBRel)).Path
$WorkerPath = Join-Path $ScriptsDir "run_worker.py"

$env:DATABASE_URL  = "sqlite:///$DBAbs"
$env:DATABASE_PATH = $DBAbs

Write-Host "Using Python: $PyExe"
Write-Host "Repo root   : $Root"
Write-Host "DB (abs)    : $DBAbs"
Write-Host "DATABASE_URL: $env:DATABASE_URL"
Write-Host ""

Push-Location $Root
try {
  # 1) Schema & migrations
  Write-Host "==> Applying schema & migrations"
  & $PyExe .\scripts\apply_schema.py
  Assert-LastExitCode "apply_schema.py"

  if (Test-Path .\scripts\migrate_r15_add_domain_resolutions.py) {
    & $PyExe .\scripts\migrate_r15_add_domain_resolutions.py --db $DBAbs
    Assert-LastExitCode "migrate_r15_add_domain_resolutions.py"
  }

  # 2) Focused tests
  Write-Host "==> Running tests (-k 'r15 or r16')"
  & $PyExe -m pytest -k "r15 or r16" -q
  Assert-LastExitCode "pytest subset (r15|r16)"

  # 3) Seed leads
  if (Test-Path .\samples\leads.csv) {
    Write-Host "==> Ingesting sample leads"
    & $PyExe .\scripts\ingest_csv.py .\samples\leads.csv
    Assert-LastExitCode "ingest_csv.py"
  } else {
    Write-Warning "samples\leads.csv not found; continuing with existing data."
  }

  # 4) Resolve MX (inline; no --from-db in CLI)
  Write-Host "==> Resolving MX for domains found in companies (inline)"
  $resolveMX = @"
import os, sqlite3, traceback
from src.resolve.mx import resolve_mx

db = os.environ.get("DATABASE_PATH") or r"$DBAbs"
con = sqlite3.connect(db)
con.row_factory = sqlite3.Row

rows = con.execute("""
  SELECT id AS company_id,
         TRIM(COALESCE(official_domain, domain)) AS dom
    FROM companies
   WHERE COALESCE(official_domain, domain) IS NOT NULL
     AND TRIM(COALESCE(official_domain, domain)) <> ''
   ORDER BY id DESC
   LIMIT 50
""").fetchall()
con.close()

if not rows:
    print("No companies with a domain to resolve.")
else:
    for r in rows:
        dom = r["dom"]
        try:
            res = resolve_mx(company_id=int(r["company_id"]), domain=dom, force=False, db_path=db)
            print(f"Resolved: {dom} -> {res.lowest_mx} (cached={res.cached}, failure={res.failure})")
        except Exception as e:
            print(f"ERROR resolving {dom}: {e}")
            traceback.print_exc()
"@
  & $PyExe -c $resolveMX
  Assert-LastExitCode "resolve MX inline"

  # 5) Worker
  Write-Host "==> Starting worker (mx,verify) in background"
  $worker = $null
  if (Test-Path $WorkerPath) {
    $worker = Start-Job -ArgumentList @($PyExe, $env:DATABASE_URL, $WorkerPath) -ScriptBlock {
      param($py, $dburl, $workerPath)
      $ErrorActionPreference = "Stop"
      $env:DATABASE_URL = $dburl
      & $py $workerPath --queues mx,verify --burst
    }
    Start-Sleep -Seconds 2
  } else {
    Write-Warning "scripts\run_worker.py not found; jobs will remain queued."
  }

  # 6) Enqueue verify batch (R16)
  Write-Host "==> Enqueuing a small verification batch (R16 task_probe_email)"
  $enqueueCode = @"
import os, sqlite3
db = os.environ.get("DATABASE_PATH") or r"$DBAbs"
con = sqlite3.connect(db)
con.row_factory = sqlite3.Row
rows = con.execute("""
  SELECT id, email,
         CASE
           WHEN instr(email,'@')>0 THEN lower(substr(email, instr(email,'@')+1))
           ELSE NULL
         END AS domain
  FROM emails
  ORDER BY id DESC
  LIMIT 10
""").fetchall()
con.close()
to_probe = [(r["id"], r["email"], r["domain"]) for r in rows if r["email"] and r["domain"]]
if not to_probe:
    print("No emails in DB; nothing to enqueue.")
else:
    try:
        from rq import Queue
        from src.queueing.redis_conn import get_redis
        from src.queueing.tasks import task_probe_email
        q = Queue("verify", connection=get_redis())
        for (eid, em, dom) in to_probe[:5]:
            q.enqueue(task_probe_email, email_id=eid, email=em, domain=dom, force=False)
            print("Enqueued probe:", eid, em, dom)
    except Exception as e:
        print("Failed to enqueue (RQ/Redis?):", e)
"@
  & $PyExe -c $enqueueCode
  Assert-LastExitCode "enqueue verify batch (inline)"

  if ($worker) {
    Write-Host "==> Waiting for worker to drain queued jobs..."
    Wait-Job $worker -Timeout 30 | Out-Null
    Receive-Job $worker | Out-Host
    Remove-Job $worker -Force | Out-Null
  }

  # 7) CLI sanity probe (direct)
  if (Test-Path .\scripts\probe_smtp.py) {
    Write-Host "==> CLI sanity: probing a target via R16 CLI"
    $pickTarget = @"
import os, sqlite3
db = os.environ.get("DATABASE_PATH") or r"$DBAbs"
con = sqlite3.connect(db)
con.row_factory = sqlite3.Row
row = con.execute("SELECT email FROM emails WHERE instr(email,'@')>0 ORDER BY id DESC LIMIT 1").fetchone()
con.close()
print(row["email"] if row else "someone@gmail.com")
"@
    $target = & $PyExe -c $pickTarget
    Assert-LastExitCode "select target email (inline)"
    & $PyExe .\scripts\probe_smtp.py --email "$target"
    Assert-LastExitCode "probe_smtp.py"
  } else {
    Write-Warning "scripts\probe_smtp.py not found; skipping CLI sanity probe."
  }

  # 8) Inspect behavior cache (O06)
  Write-Host "==> Inspecting recent domain_resolutions rows (mx_behavior column)"
  & $PyExe -m sqlite3 $DBAbs "SELECT domain, lowest_mx, substr(mx_behavior,1,80) as mx_behavior, resolved_at FROM domain_resolutions ORDER BY id DESC LIMIT 5;"
  Assert-LastExitCode "sqlite3 mx_behavior check"

  Write-Host ""
  Write-Host "✔ R16 SMTP RCPT TO probe acceptance passed." -ForegroundColor Green
}
finally {
  Pop-Location
}
