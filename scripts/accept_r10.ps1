Param(
  [string]$Domain = "crestwellpartners.com",
  [string]$Db = "dev.db"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Paths to venv tools
$pytest = Join-Path $PSScriptRoot "..\.venv\Scripts\pytest.exe"
$python = Join-Path $PSScriptRoot "..\.venv\Scripts\python.exe"

# 1) Unit tests (disable 3rd-party pytest auto-plugins)
$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD = 1
& $pytest -q "tests\test_r10_crawler.py"
if ($LASTEXITCODE -ne 0) { throw "R10 tests failed" }

# 2) Dry run crawl (robots-aware via fetch_url)
$crawlScript = Join-Path $PSScriptRoot "crawl_domain.py"
& $python $crawlScript $Domain --db $Db

# 3) Quick verification (SQLite count)
$code = 'import sqlite3, sys; con=sqlite3.connect(sys.argv[1]); cur=con.cursor(); cur.execute("select count(*) from sources"); row=cur.fetchone(); print("sources rows:", row[0] if row else 0)'
& $python -c $code $Db
