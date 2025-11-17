# scripts/accept_r15.ps1
[CmdletBinding()]
param(
    [string]$Domain = "gmail.com",
    [string]$DbPath,
    [switch]$VerboseOutput,
    [int]$TimeoutSeconds = 45
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---------------------------
# Helpers
# ---------------------------

function Write-Info($msg) {
    if ($VerboseOutput) { Write-Host "[R15] $msg" -ForegroundColor DarkGray }
    else { Write-Host "[R15] $msg" }
}

function Assert-True([bool]$cond, [string]$message) {
    if (-not $cond) {
        throw "ASSERT FAILED: $message"
    }
}

function Join-Args([string[]]$Items) {
    $Items | ForEach-Object {
        if ($_ -match '[\s"]') { '"' + ($_ -replace '"','\"') + '"' } else { $_ }
    } | ForEach-Object { $_ } | Out-String -Stream |
      Where-Object { $_ -ne "" } |
      ForEach-Object { $_.TrimEnd() } |
      ForEach-Object { $_ } |
      Join-String -Separator ' '
}

function Invoke-PyJson([string]$ScriptPath, [string[]]$ArgList, [int]$TimeoutSec = 45) {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $PyExe
    $allArgs = @($ScriptPath) + $ArgList
    $psi.Arguments = (Join-Args $allArgs)
    $psi.WorkingDirectory = $repoRoot
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError  = $true
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.EnvironmentVariables["PYTHONUNBUFFERED"] = "1"
    if ($env:DATABASE_URL) {
        $psi.EnvironmentVariables["DATABASE_URL"] = $env:DATABASE_URL
    }

    $p = New-Object System.Diagnostics.Process
    $p.StartInfo = $psi
    $null = $p.Start()

    if (-not $p.WaitForExit($TimeoutSec * 1000)) {
        try { $p.Kill() } catch {}
        $cmdline = "$($psi.FileName) $($psi.Arguments)"
        throw "Timeout after ${TimeoutSec}s: $cmdline"
    }

    $out = $p.StandardOutput.ReadToEnd()
    $err = $p.StandardError.ReadToEnd()

    if ($p.ExitCode -ne 0) {
        if ($out) { Write-Host $out }
        if ($err) { Write-Warning $err }
        $cmdline = "$($psi.FileName) $($psi.Arguments)"
        throw "Python exited with code $($p.ExitCode): $cmdline"
    }

    try {
        return $out | ConvertFrom-Json
    } catch {
        if ($out) { Write-Host $out }
        if ($err) { Write-Warning $err }
        $cmdline = "$($psi.FileName) $($psi.Arguments)"
        throw "Failed to parse JSON from: $cmdline"
    }
}

# ---------------------------
# Resolve repo & Python
# ---------------------------

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$venvPy = Join-Path $repoRoot ".venv\Scripts\python.exe"
$PyExe = if (Test-Path $venvPy) { $venvPy } else { "python" }

if (-not $DbPath) { $DbPath = Join-Path $repoRoot "data\dev.db" }
$DbPath = (Resolve-Path $DbPath).Path

$env:DATABASE_URL = "sqlite:///$DbPath"

Write-Info "Repo root: $repoRoot"
Write-Info "Python   : $PyExe"
Write-Info "DB       : $DbPath"
Write-Info "Domain   : $Domain"

# ---------------------------
# 1) Apply schema + R15 migration
# ---------------------------

Write-Info "Applying base schema…"
& $PyExe (Join-Path $repoRoot "scripts\apply_schema.py")
Assert-True ($LASTEXITCODE -eq 0) "apply_schema.py failed"

Write-Info "Ensuring R15 domain_resolutions schema…"
& $PyExe (Join-Path $repoRoot "scripts\migrate_r15_add_domain_resolutions.py") --db $DbPath
Assert-True ($LASTEXITCODE -eq 0) "migrate_r15_add_domain_resolutions.py failed"

# ---------------------------
# 2) Resolve MX (forced fresh to avoid prior cache)
# ---------------------------

$cli = (Join-Path $repoRoot "scripts\resolve_mx.py")
Assert-True (Test-Path $cli) "scripts\resolve_mx.py not found"

Write-Info "Resolving MX (forced fresh)…"
$res1 = Invoke-PyJson -ScriptPath $cli -ArgList @("--domain",$Domain,"--db",$DbPath,"--json","--force") -TimeoutSec $TimeoutSeconds
Write-Info ("Result#1 cached: " + $res1.cached)
Write-Info ("Result#1 lowest_mx: " + ($res1.lowest_mx ?? "(none)"))

# Assertions for forced fresh resolution
Assert-True ($res1.domain -ne $null -and $res1.domain -ne "") "domain missing in result"
Assert-True (-not $res1.cached) "first resolution should be fresh (forced)"
Assert-True ($res1.lowest_mx -ne $null -and $res1.lowest_mx -ne "") "lowest_mx should be non-empty"
$mxHosts1 = @($res1.mx_hosts)
Assert-True ($mxHosts1.Count -gt 0) "mx_hosts must contain at least one host"
$prefMap1Count = (@($res1.preference_map.PSObject.Properties).Count)
Assert-True ($prefMap1Count -gt 0) "preference_map must contain entries"

# ---------------------------
# 3) Resolve MX again (cache hit)
# ---------------------------

Write-Info "Resolving MX again (should be cached)…"
$res2 = Invoke-PyJson -ScriptPath $cli -ArgList @("--domain",$Domain,"--db",$DbPath,"--json") -TimeoutSec $TimeoutSeconds
Write-Info ("Result#2 cached: " + $res2.cached)

Assert-True ($res2.cached) "second resolution should be cached"
Assert-True ($res2.lowest_mx -eq $res1.lowest_mx) "lowest_mx should match cached value"
Assert-True ((@($res2.mx_hosts)).Count -ge $mxHosts1.Count) "cached hosts should be present"

# ---------------------------
# 4) Resolve MX with --force (fresh again)
# ---------------------------

Write-Info "Resolving MX with --force (fresh)…"
$res3 = Invoke-PyJson -ScriptPath $cli -ArgList @("--domain",$Domain,"--db",$DbPath,"--json","--force") -TimeoutSec $TimeoutSeconds
Write-Info ("Result#3 cached: " + $res3.cached)

Assert-True (-not $res3.cached) "forced resolution should be fresh"
Assert-True ($res3.lowest_mx -ne $null -and $res3.lowest_mx -ne "") "lowest_mx should be non-empty after force"

# ---------------------------
# 5) Final banner
# ---------------------------

Write-Host ""
Write-Host "==============================================" -ForegroundColor Green
Write-Host "✔ R15 acceptance passed" -ForegroundColor Green
Write-Host "  Domain: $Domain" -ForegroundColor Green
Write-Host "  Lowest MX: $($res3.lowest_mx)" -ForegroundColor Green
Write-Host "  Cached → Fresh path verified." -ForegroundColor Green
Write-Host "==============================================" -ForegroundColor Green
