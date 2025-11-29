param(
    [string]$DbPath = "data\dev.db"
)

Write-Host "=== R22 acceptance: lead search API ===`n"

$PyExe = ".\.venv\Scripts\python.exe"
if (!(Test-Path $PyExe)) {
    $PyExe = "python"
}

Write-Host "→ Using SQLite at: $DbPath`n"

# 1) Ensure schema + views are OK (v_emails_latest, FTS tables, etc.)
Write-Host "→ Applying base schema ..."
& $PyExe scripts\apply_schema.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "`n[ERROR] apply_schema.py failed (exit code $LASTEXITCODE)." -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host "→ Checking views (v_emails_latest, search FTS) ..."
& $PyExe scripts\check_view.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "`n[ERROR] check_view.py failed (exit code $LASTEXITCODE)." -ForegroundColor Red
    exit $LASTEXITCODE
}

# 2) Run focused tests for R22 + O15
Write-Host "`n→ Running pytest for R22/O15 (search backend, API, cache) ..."
& $PyExe -m pytest `
    tests/test_r21_search_indexing.py `
    tests/test_r22_search_backend.py `
    tests/test_r22_api.py `
    tests/test_o15_search_cache.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "`n[ERROR] pytest failed for R22/O15 (exit code $LASTEXITCODE)." -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host "`n→ R22/O15 tests passed."

Write-Host "`n→ Manual API smoke test (optional):"
Write-Host "   1) Run your API server in another terminal, e.g.:"
Write-Host "        uvicorn src.api.app:app --reload"
Write-Host "   2) Hit /leads/search, e.g.:"
Write-Host "        curl 'http://127.0.0.1:8000/leads/search?q=sales&icp_min=80&verify_status=valid'"

Write-Host "`n✓ R22 acceptance checks completed successfully."
