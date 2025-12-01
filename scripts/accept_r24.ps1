# scripts/accept_r24.ps1
param(
    [string]$DbPath = "data\dev.db",
    [string]$ApiHost = "127.0.0.1",
    [int]$ApiPort = 8000,
    [string]$PythonExe = "python",
    [string]$AdminApiKey = ""
)

Write-Host "=== R24 acceptance: admin UI & status ==="
Write-Host ""
Write-Host "→ Using SQLite at: $DbPath"
Write-Host "→ Using Python at: $PythonExe"
Write-Host "→ API host: $ApiHost"
Write-Host "→ API port: $ApiPort"
if ($Env:DB_URL) {
    Write-Host "→ Existing DB_URL: $($Env:DB_URL)"
}

# Ensure DB_PATH is visible to the app (src/db.get_connection can use it)
$Env:DB_PATH = $DbPath

# Optionally propagate an admin API key to the app and requests
if ($AdminApiKey) {
    $Env:ADMIN_API_KEY = $AdminApiKey
}

# Common headers for admin calls (only set if ADMIN_API_KEY present)
$adminHeaders = @{}
if ($Env:ADMIN_API_KEY) {
    $adminHeaders["x-admin-api-key"] = $Env:ADMIN_API_KEY
}

Write-Host ""
Write-Host "→ Running focused tests for R24 (admin API/UI) ..."
& $PythonExe -m pytest `
    tests/test_r24_admin_ui.py `
    tests/test_o20_cli_admin_status.py
if ($LASTEXITCODE -ne 0) {
    Write-Error "R24/O20 tests failed."
    exit 1
}

$healthUrl = "http://$ApiHost`:$ApiPort/health"
$metricsUrl = "http://$ApiHost`:$ApiPort/admin/metrics"
$analyticsUrl = "http://$ApiHost`:$ApiPort/admin/analytics"

Write-Host ""
Write-Host "→ Starting API server in background ..."
$server = Start-Process -FilePath $PythonExe `
    -ArgumentList "-m", "uvicorn", "src.api.app:app", "--host", $ApiHost, "--port", $ApiPort, "--log-level", "warning" `
    -PassThru

try {
    # Wait for the server to become ready, polling /health
    $ready = $false
    $maxAttempts = 20

    Write-Host "→ Waiting for API server to become ready at $healthUrl ..."

    for ($i = 1; $i -le $maxAttempts; $i++) {
        if ($server.HasExited) {
            Write-Error "API server process exited early with code $($server.ExitCode)."
            throw "API server failed to start."
        }

        try {
            Invoke-RestMethod -Uri $healthUrl -Method GET -Headers $adminHeaders -TimeoutSec 3 | Out-Null
            $ready = $true
            break
        }
        catch {
            Start-Sleep -Milliseconds 750
        }
    }

    if (-not $ready) {
        Write-Error "API server did not become ready at $healthUrl within the expected time."
        throw "API server not ready."
    }

    Write-Host "→ Hitting $metricsUrl ..."
    try {
        $metrics = Invoke-RestMethod -Uri $metricsUrl -Method GET -Headers $adminHeaders -TimeoutSec 20
    }
    catch {
        Write-Error "Failed to call /admin/metrics: $($_.Exception.Message)"
        throw
    }

    Write-Host "Queues:" ($metrics.queues | ConvertTo-Json -Depth 3)
    Write-Host "Workers:" ($metrics.workers | ConvertTo-Json -Depth 3)
    Write-Host "Verification:" ($metrics.verification | ConvertTo-Json -Depth 3)
    Write-Host "Costs:" ($metrics.costs | ConvertTo-Json -Depth 3)

    Write-Host ""
    Write-Host "→ Hitting $analyticsUrl ..."
    try {
        $analytics = Invoke-RestMethod -Uri $analyticsUrl -Method GET -Headers $adminHeaders -TimeoutSec 20
    }
    catch {
        Write-Error "Failed to call /admin/analytics: $($_.Exception.Message)"
        throw
    }

    Write-Host "Verification time series:" ($analytics.verification_time_series | ConvertTo-Json -Depth 3)
    Write-Host "Domain breakdown:" ($analytics.domain_breakdown | ConvertTo-Json -Depth 3)
    Write-Host "Error breakdown:" ($analytics.error_breakdown | ConvertTo-Json -Depth 3)

    Write-Host ""
    Write-Host "✔ R24 admin UI & status checks passed."
}
finally {
    if ($server -and -not $server.HasExited) {
        Write-Host "→ Stopping API server ..."
        $server | Stop-Process -Force
    }
}
