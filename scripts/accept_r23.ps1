# scripts/accept_r23.ps1
param(
    [string]$DbPath = "data\dev.db"
)

Write-Host "=== R23 acceptance: search facets ===`n"

$PyExe = ".\.venv\Scripts\python.exe"
if (!(Test-Path $PyExe)) {
    $PyExe = "python"
}

Write-Host "→ Using SQLite at: $DbPath`n"

Write-Host "→ Running pytest for R21/R22/R23 facets + O14/O15 cache ..."
& $PyExe -m pytest `
    tests/test_r21_search_indexing.py `
    tests/test_r22_search_backend.py `
    tests/test_r22_api.py `
    tests/test_r23_facets_backend.py `
    tests/test_o14_facets_mv.py `
    tests/test_o15_search_cache.py

if ($LASTEXITCODE -ne 0) {
    Write-Error "Pytest failed; aborting R23 acceptance."
    exit 1
}

Write-Host "`n→ Refreshing O14 materialized view (lead_search_docs) ..."
& $PyExe scripts/backfill_o14_lead_search_docs.py --db $DbPath

if ($LASTEXITCODE -ne 0) {
    Write-Error "Backfill script failed; aborting R23 acceptance."
    exit 1
}

Write-Host "`n→ Demo /leads/search with facets (verify_status, icp_bucket) ..."

# Ensure FastAPI app uses the same DB path as this script
$env:DB_PATH = $DbPath

$demoCode = @"
from fastapi.testclient import TestClient

from src.api.app import app

client = TestClient(app)

# Use a broad query and no verify_status filter so we see whatever exists.
resp = client.get(
    "/leads/search",
    params={
        "q": "a",
        "facets": "verify_status,icp_bucket",
        "limit": "5",
    },
)
resp.raise_for_status()
data = resp.json()

print("results:", len(data.get("results", [])))
print("facets.verify_status:", data.get("facets", {}).get("verify_status"))
print("facets.icp_bucket:", data.get("facets", {}).get("icp_bucket"))
"@

& $PyExe -c $demoCode

if ($LASTEXITCODE -ne 0) {
    Write-Error "Demo /leads/search with facets failed; aborting R23 acceptance."
    exit 1
}

Write-Host "`n✓ R23 acceptance checks completed successfully."
