<#  R09 Acceptance Runner (PowerShell 7.x)

Verifies:
  1) Robots deny -> returns "blocked_by_robots" and page path is NOT fetched
  2) Crawl delay -> sequential fetches respect >= robots crawl-delay
  3) Caching     -> second call served from cache within TTL
  4) WAF handling-> 429 triggers backoff; next request waits >= Retry-After

Usage:
  pwsh -f .\scripts\accept_r09.ps1
#>

param()

$ErrorActionPreference = "Stop"

$PY = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $PY)) {
    Write-Host "❌ venv python not found at $PY" -Foreground Red
    exit 1
}

# ---------- tiny local HTTP test server (Python) ----------
$serverPy = @'
from __future__ import annotations
import argparse, json, socket, threading, time
from http.server import HTTPServer, BaseHTTPRequestHandler

COUNTS = {}
WAF_SEEN = set()
POLICY = "delay2"  # "deny" or "delay2"

def incr(path):
    COUNTS[path] = COUNTS.get(path, 0) + 1

class Handler(BaseHTTPRequestHandler):
    server_version = "R09Test/0.1"
    def log_message(self, fmt, *args):  # quiet
        pass
    def do_GET(self):
        path = self.path.split("?",1)[0]
        incr(path)

        if path == "/_stats":
            body = json.dumps({"counts": COUNTS}).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Cache-Control","no-store")
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/robots.txt":
            self.send_response(200)
            self.send_header("Content-Type","text/plain")
            if POLICY == "deny":
                body = b"User-agent: *\nDisallow: /\n"
                self.send_header("Cache-Control","max-age=60")
            else:
                body = b"User-agent: *\nAllow: /\nCrawl-delay: 2\n"
                self.send_header("Cache-Control","max-age=60")
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/cache-me":
            body = b"<html><body>cached-ok</body></html>"
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("ETag", '"v1"')
            self.send_header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
            self.send_header("Cache-Control", "max-age=30")
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/waf":
            if "waf" not in WAF_SEEN:
                WAF_SEEN.add("waf")
                self.send_response(429)
                self.send_header("Content-Type","text/plain")
                self.send_header("Retry-After","3")
                self.end_headers()
                self.wfile.write(b"rate limited")
                return
            else:
                self.send_response(200)
                self.send_header("Content-Type","text/html")
                self.end_headers()
                self.wfile.write(b"<html>ok after backoff</html>")
                return

        # generic page
        self.send_response(200)
        self.send_header("Content-Type","text/html")
        self.end_headers()
        self.wfile.write(b"<html>ok</html>")

def find_free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port

def run_server(policy:str, portfile:str):
    global POLICY
    POLICY = policy
    port = find_free_port()
    httpd = HTTPServer(("127.0.0.1", port), Handler)
    # write port for the runner to pick up
    with open(portfile, "w", encoding="utf-8") as f:
        f.write(str(port))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--policy", choices=["deny","delay2"], required=True)
    ap.add_argument("--portfile", required=True)
    args = ap.parse_args()
    run_server(args.policy, args.portfile)
'@

# Write server script
$serverPath = Join-Path -Path "scripts" -ChildPath "_tmp_r09_server.py"
Set-Content -Path $serverPath -Value $serverPy -Encoding UTF8

function Start-TestServer {
    param([string]$Policy, [string]$Name)

    $portfile = Join-Path -Path "scripts" -ChildPath ("_tmp_r09_{0}.port" -f $Name)
    if (Test-Path $portfile) { Remove-Item $portfile -Force }

    # Use ProcessStartInfo and ADD to ArgumentList (don't assign)
    $psi = [System.Diagnostics.ProcessStartInfo]::new()
    $psi.FileName = $PY
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true

    # Build the argument list safely
    $null = $psi.ArgumentList.Add($serverPath)
    $null = $psi.ArgumentList.Add("--policy")
    $null = $psi.ArgumentList.Add($Policy)
    $null = $psi.ArgumentList.Add("--portfile")
    $null = $psi.ArgumentList.Add($portfile)

    $proc = [System.Diagnostics.Process]::new()
    $proc.StartInfo = $psi
    $proc.Start() | Out-Null

    # Wait for the server to write its port file
    $tries = 0
    while (-not (Test-Path $portfile)) {
        Start-Sleep -Milliseconds 100
        $tries++
        if ($tries -gt 100) { throw "Server $Name did not write portfile" }
    }
    $port = Get-Content $portfile | Select-Object -First 1
    return @{ "Proc" = $proc; "Port" = [int]$port; "PortFile" = $portfile }
}

function Stop-TestServer {
    param($Server)
    try {
        if ($Server.Proc -and -not $Server.Proc.HasExited) { $Server.Proc.Kill() | Out-Null }
    }
    catch {}
    if (Test-Path $Server.PortFile) { Remove-Item $Server.PortFile -Force }
}

# ---------- Start two servers ----------
$denySrv = Start-TestServer -Policy "deny"   -Name "deny"
$delaySrv = Start-TestServer -Policy "delay2" -Name "delay"

Write-Host "→ deny server on http://127.0.0.1:$($denySrv.Port)"
Write-Host "→ delay server on http://127.0.0.1:$($delaySrv.Port)"

# Helper to run fetch_url.py and parse JSON
function Fetch-URL {
    param([string]$Url)
    $json = & $PY .\scripts\fetch_url.py --url $Url --allow-http-robots
    return $json | ConvertFrom-Json
}
function Get-Stats {
    param([int]$Port)
    $resp = Invoke-WebRequest -Uri ("http://127.0.0.1:{0}/_stats" -f $Port)
    return $resp.Content | ConvertFrom-Json
}

$ok = $true

# 1) Robots deny
Write-Host "=== R09: Robots deny ==="
$denyUrl = "http://127.0.0.1:{0}/blocked" -f $denySrv.Port
$denyRes = Fetch-URL -Url $denyUrl
$denyStats = Get-Stats -Port $denySrv.Port
$blockedReasonOk = ($denyRes.reason -eq "blocked_by_robots")
$pageNotFetchedOk = -not $denyStats.counts.PSObject.Properties.Name.Contains("/blocked")
if ($blockedReasonOk -and $pageNotFetchedOk) {
    Write-Host "✔ Robots deny passed" -ForegroundColor Green
}
else {
    Write-Host "❌ Robots deny failed (reason=$($denyRes.reason); hits=/blocked -> $($denyStats.counts.'/blocked'))" -ForegroundColor Red
    $ok = $false
}

# 2) Crawl delay (expect ≥ 2s between back-to-back fetches)
Write-Host "=== R09: Crawl delay ==="
$delayUrl = "http://127.0.0.1:{0}/page" -f $delaySrv.Port
$first = Fetch-URL -Url $delayUrl
$second = Fetch-URL -Url $delayUrl
$gap = [double]$second.ts_start - [double]$first.ts_end
if ($gap -ge 2.0) {
    Write-Host ("✔ Crawl delay respected (gap={0:N2}s)" -f $gap) -ForegroundColor Green
}
else {
    Write-Host ("❌ Crawl delay too short (gap={0:N2}s, need ≥2.0s)" -f $gap) -ForegroundColor Red
    $ok = $false
}

# 3) Caching (second call served from cache)
Write-Host "=== R09: Caching ==="
$cacheUrl = "http://127.0.0.1:{0}/cache-me" -f $delaySrv.Port
$c1 = Fetch-URL -Url $cacheUrl
$c2 = Fetch-URL -Url $cacheUrl
if ($c2.reason -eq "cached_fresh" -and $c2.from_cache -eq $true) {
    Write-Host "✔ Caching hit (second call served from cache)" -ForegroundColor Green
}
else {
    Write-Host ("❌ Caching failed (c2.reason={0}, from_cache={1})" -f $c2.reason, $c2.from_cache) -ForegroundColor Red
    $ok = $false
}

# 4) WAF handling (429 triggers backoff; next request waits ≥ Retry-After=3s)
Write-Host "=== R09: WAF handling (429) ==="
$wafUrl = "http://127.0.0.1:{0}/waf" -f $delaySrv.Port
$w1 = Fetch-URL -Url $wafUrl
$start2 = Get-Date
$w2 = Fetch-URL -Url $delayUrl   # same host, different path; should honor cool-off
$elapsed2 = (Get-Date) - $start2
if ($w1.reason -eq "throttled" -and $elapsed2.TotalSeconds -ge 3.0) {
    Write-Host ("✔ Backoff respected (waited ~{0:N2}s after 429)" -f $elapsed2.TotalSeconds) -ForegroundColor Green
}
else {
    Write-Host ("❌ Backoff not respected (first.reason={0}; waited={1:N2}s)" -f $w1.reason, $elapsed2.TotalSeconds) -ForegroundColor Red
    $ok = $false
}

# Cleanup servers
Stop-TestServer -Server $denySrv
Stop-TestServer -Server $delaySrv
Remove-Item $serverPath -Force -ErrorAction SilentlyContinue

if ($ok) { Write-Host "=== R09 acceptance: PASSED ===" -ForegroundColor Green; exit 0 }
else { Write-Host "=== R09 acceptance: FAILED ===" -ForegroundColor Red; exit 2 }
