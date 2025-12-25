# Build an absolute path safely (data/ must exist)
$DbPath = Join-Path (Resolve-Path "data").Path "test_companies.db"

# Clear old test DB
Remove-Item -LiteralPath $DbPath -Force -ErrorAction SilentlyContinue

# Force ALL internal get_conn() calls (queueing tasks, etc.) to hit the same DB
$env:DATABASE_URL = "sqlite:///$($DbPath -replace '\\','/')"

Write-Host "DBPath: $DbPath"
Write-Host "DATABASE_URL: $env:DATABASE_URL"

# Company list (name, domain)
$Companies = @(
    @("Chili Piper", "chilipiper.com"),
    @("Paddle", "paddle.com"),
    @("Outreach", "outreach.io"),
    @("Gong", "gong.io"),
    @("Aircall", "aircall.io"),
    @("Lokalise", "lokalise.com"),
    @("Clari", "clari.com"),
    @("Dooly", "dooly.ai"),
    @("Sendoso", "sendoso.com"),
    @("Chargebee", "chargebee.com"),
    @("Crestwell Partners", "crestwellpartners.com")
)

$PyExe = "python"
$FirstRun = $true

foreach ($company in $Companies) {
    $Name = $company[0]
    $Domain = $company[1].Trim()

    Write-Host "`n" -NoNewline
    Write-Host "=" * 70 -ForegroundColor Cyan
    Write-Host "  Testing: $Name ($Domain)" -ForegroundColor Cyan
    Write-Host "=" * 70 -ForegroundColor Cyan

    $Args = @(
        ".\scripts\demo_autodiscovery.py",
        "--db", $DbPath,
        "--company", $Name,
        "--domain", $Domain,
        "--log-level", "INFO"
    )

    # Only init schema on first run
    if ($FirstRun) {
        $Args += "--init-schema"
        $FirstRun = $false
    }

    & $PyExe @Args

    Write-Host "`nCompleted: $Name" -ForegroundColor Green
    Write-Host "Waiting 2 seconds before next company..." -ForegroundColor Yellow
    Start-Sleep -Seconds 2
}

Write-Host "`n" -NoNewline
Write-Host "=" * 70 -ForegroundColor Green
Write-Host "  ALL COMPANIES COMPLETE" -ForegroundColor Green
Write-Host "=" * 70 -ForegroundColor Green
