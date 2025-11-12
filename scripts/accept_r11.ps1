# scripts/accept_r11.ps1
Param(
  [string]$Domain = "crestwellpartners.com",
  [string]$Db = "dev.db"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "== R11 acceptance for domain '$Domain' with DB '$Db' =="

# 0) Ensure unique index on emails.email (idempotent; safe to re-run)
if (Test-Path .\scripts\apply_unique_email_index.py) {
  Write-Host "-> Ensuring unique index on emails.email..."
  python .\scripts\apply_unique_email_index.py --db "$Db"
} else {
  Write-Host "-> (skip) scripts\apply_unique_email_index.py not found; assuming schema already enforced."
}

# 1) Ensure sources exist (from R10)
Write-Host "-> Crawling sources via R10 (this reads politely and writes into 'sources')..."
python .\scripts\crawl_domain.py "www.$Domain" --db "$Db"

# 2) Run extraction
Write-Host "-> Extracting candidates (R11)..."
python .\scripts\extract_candidates.py --domain "$Domain" --db "$Db"

# 3) Show sample results (SQLite one-liner; avoid heredocs)
Write-Host "-> Showing latest 5 emails with provenance:"
python -c "import sqlite3;con=sqlite3.connect(r'$Db');print('email | source_url');[print(f'{e} | {s or \"\"}') for (e,s) in con.execute('select email, source_url from emails order by rowid desc limit 5')];con.close()"

Write-Host "== Done =="
