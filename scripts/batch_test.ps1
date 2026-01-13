<#
scripts/batch_test.ps1

Batch-runs demo_autodiscovery.py and writes FULL, untruncated output to:
  - data\batch_logs\ALL_COMPANIES.txt
  - data\batch_logs\<NN>_<company>_<domain>.txt  (per company)

Enhancements (thorough metrics):
  - Per-company + batch-level efficiency metrics parsed from streamed log lines
  - AUTODISCOVERY SUMMARY parsing (pages, candidates, AI, fallback, approved, permutations, upserts)
  - HTTP request metrics from httpx "HTTP Request:" lines (methods, status codes, unique URLs, duplicates)
  - Runtime metrics (per company, avg/min/max/median, top slowest)
  - Zero-candidate metrics:
      * % of completed companies with 0 valid candidates (Final approved people = 0)
      * runtime spent on zero-candidate companies (seconds + % of batch)
      * pages fetched spent on zero-candidate companies (count + % of batch)
      * pages-per-approved ratio (global + excluding zero-approved)
  - Snapshot row counts from v_emails_latest section
  - CSV + JSONL metrics exports for easy diffing across runs:
      * data\batch_logs\BATCH_METRICS.csv
      * data\batch_logs\BATCH_METRICS.jsonl
  - Error classification:
      * error_lines_total (all [ERROR])
      * smtp_expected_error_lines (TCP/25 preflight + MX tried + skipping SMTP step)
      * error_lines_true (non-SMTP errors)

Key guarantees preserved:
  - No buffering Python output into variables
  - Python unbuffered output (PYTHONUNBUFFERED=1)
  - Line-by-line streaming to UTF-8 logs via StreamWriter

Polish / correctness guarantees:
  - Metrics schema is always initialized (no null-valued expression failures)
  - CSV export is real Export-Csv output (no Format-Table/Out-String truncation)
  - JSONL export is line-delimited JSON (one metrics object per line)
  - Write-LineBoth avoids double-write if Per/All logs ever point at the same file
#>

[CmdletBinding()]
param(
    [string]$DbFileName = "test_companies.db",
    [string]$LogLevel = "INFO",
    [int]$SleepSeconds = 2,
    [string]$PyExe = "python"
)

Set-StrictMode -Version Latest
$ProgressPreference = "SilentlyContinue"

# ---------------------------
# Run mode (explicit)
# ---------------------------
$RunMode  = "DISCOVERY_ONLY"
$SMTPMode = "DISABLED_INTENTIONAL"   # informational; Python may still print TCP/25 messages

# ---------------------------
# Helpers
# ---------------------------
function Sanitize-FileName {
    param([Parameter(Mandatory = $true)][string]$Value)
    $s = $Value.Trim()
    $s = $s -replace '[\\/:*?"<>|]+', '_'
    $s = $s -replace '\s+', '_'
    $s = $s.Trim(' ', '.', '_')
    if ([string]::IsNullOrWhiteSpace($s)) { $s = "unknown" }
    return $s
}

function New-Utf8StreamWriter {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter()][bool]$Append = $true
    )
    $enc = New-Object System.Text.UTF8Encoding($false)
    $sw = New-Object System.IO.StreamWriter($Path, $Append, $enc)
    $sw.AutoFlush = $true
    return $sw
}

# These are set per-company so Write-LineBoth can dedupe if paths ever match.
$script:__AllLogPath = ""
$script:__PerLogPath = ""

function Write-LineBoth {
    param(
        [Parameter()][AllowNull()][System.IO.StreamWriter]$Per,
        [Parameter()][AllowNull()][System.IO.StreamWriter]$All,
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [AllowNull()]
        [string]$Line
    )
    if ($null -eq $Line) { $Line = "" }

    # If Per/All are the same underlying writer, write only once.
    if ($null -ne $Per -and $null -ne $All) {
        if ([object]::ReferenceEquals($Per, $All)) {
            $All.WriteLine($Line)
            return
        }
    }

    # If Per/All logs point to the same file, write only once.
    if (-not [string]::IsNullOrWhiteSpace($script:__PerLogPath) -and
        -not [string]::IsNullOrWhiteSpace($script:__AllLogPath) -and
        ($script:__PerLogPath -eq $script:__AllLogPath)) {
        if ($null -ne $All) { $All.WriteLine($Line) }
        return
    }

    if ($null -ne $Per) { $Per.WriteLine($Line) }
    if ($null -ne $All) { $All.WriteLine($Line) }
}

function Median-Of {
    param([double[]]$Values)
    if ($null -eq $Values -or $Values.Count -eq 0) { return 0.0 }
    $sorted = $Values | Sort-Object
    $n = $sorted.Count
    if (($n % 2) -eq 1) {
        return [double]$sorted[[int](($n - 1) / 2)]
    }
    $a = [double]$sorted[($n / 2) - 1]
    $b = [double]$sorted[($n / 2)]
    return ($a + $b) / 2.0
}

function Try-Parse-LogTimestamp {
    param([string]$Line)
    # Handles leading timestamps like: "2026-01-03 19:05:51,745 [INFO] ..."
    if ($Line -match '^(?<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3})') {
        $ts = $Matches['ts']
        try {
            return [DateTime]::ParseExact(
                $ts,
                'yyyy-MM-dd HH:mm:ss,fff',
                [System.Globalization.CultureInfo]::InvariantCulture
            )
        } catch {
            return $null
        }
    }
    return $null
}

function IntOrZero {
    param([AllowNull()][object]$v)
    if ($null -eq $v) { return 0 }
    try { return [int]$v } catch { return 0 }
}

function StrOrEmpty {
    param([AllowNull()][object]$v)
    if ($null -eq $v) { return "" }
    return "$v"
}

function New-CompanyMetricsSchema {
    param(
        [Parameter(Mandatory = $true)][int]$Idx,
        [Parameter(Mandatory = $true)][string]$Company,
        [Parameter(Mandatory = $true)][string]$Domain
    )

    # IMPORTANT: This is the single authoritative schema initializer.
    # No downstream code should assume missing properties or null structures.
    return [ordered]@{
        idx = $Idx
        company = $Company
        domain = $Domain

        exit_code = -1
        exception = ""
        runtime_seconds = 0.0

        pages_fetched = 0
        pages_skipped_robots = 0
        pages_persisted = 0

        candidates_with_email = 0
        candidates_without_email = 0

        ai_enabled = ""
        ai_called = ""
        ai_succeeded = ""
        ai_input_candidates = 0
        ai_returned_people = 0
        ai_seconds = 0.0

        fallback_used = ""

        final_approved_people = 0
        people_upserted = 0
        emails_upserted = 0

        permutations_generated = 0

        http_requests_total = 0
        http_unique_urls = 0
        http_duplicate_requests = 0
        http_get = 0
        http_head = 0
        http_post = 0
        http_other_method = 0
        http_200 = 0
        http_3xx = 0
        http_403 = 0
        http_404 = 0
        http_4xx_other = 0
        http_5xx = 0
        robots_fetches = 0
        robots_403 = 0

        tcp25_preflight_failed = 0

        error_lines_total = 0
        error_lines_true = 0
        smtp_expected_error_lines = 0
        warning_lines = 0

        crawl_origin = ""
        crawl_pages = 0
        crawl_urls_attempted = 0
        crawl_seeds_attempted = 0
        crawl_seed_people_pages = 0
        crawl_tiers_enqueued = 0

        people_cards_extracted_total = 0
        people_cards_candidates_found = 0
        team_fallback_inserted = 0

        found_no_email_people = 0
        limited_to_people = 0

        snapshot_rows = 0
    }
}

$script:__CsvInitialized = $false

function Write-MetricsExports {
    param(
        [Parameter(Mandatory = $true)][pscustomobject]$MetricsObject,
        [Parameter(Mandatory = $true)][string]$CsvPath,
        [Parameter(Mandatory = $true)][string]$JsonlPath
    )

    # CSV (canonical): Export-Csv from real objects (no formatting pipelines).
    try {
        if (-not $script:__CsvInitialized) {
            $MetricsObject | Export-Csv -Path $CsvPath -NoTypeInformation -Encoding UTF8
            $script:__CsvInitialized = $true
        } else {
            $MetricsObject | Export-Csv -Path $CsvPath -NoTypeInformation -Append -Encoding UTF8
        }
    } catch {
        # best-effort; do not stop the batch for export write issues
    }

    # JSONL: one compact JSON object per line.
    try {
        $json = $MetricsObject | ConvertTo-Json -Compress -Depth 6
        Add-Content -Path $JsonlPath -Value $json -Encoding UTF8
    } catch {
        # best-effort; do not stop the batch for export write issues
    }
}

# ---------------------------
# Global counters
# ---------------------------
$BatchStart = Get-Date

[int]$CompaniesAttempted = 0
[int]$CompaniesCompleted = 0
[int]$CompaniesFailedExit = 0
[int]$CompaniesFailedException = 0
[int]$CompaniesZeroValid = 0

[double]$ZeroValidRuntimeSeconds = 0.0
[int]$ZeroValidPagesFetched = 0
[int]$ZeroValidPagesPersisted = 0

# Batch totals parsed from AUTODISCOVERY SUMMARY
[int]$TotalPagesFetched = 0
[int]$TotalPagesSkippedRobots = 0
[int]$TotalCandidatesWithEmail = 0
[int]$TotalCandidatesWithoutEmail = 0
[int]$TotalFinalApprovedPeople = 0
[int]$TotalPeopleUpserted = 0
[int]$TotalEmailsUpserted = 0
[int]$TotalPermutationsGenerated = 0
[int]$TotalAiInputCandidates = 0
[int]$TotalAiReturnedPeople = 0
[int]$CompaniesAiCalled = 0
[int]$CompaniesAiSucceeded = 0
[int]$CompaniesFallbackUsed = 0
[int]$CompaniesWithPublishedEmails = 0

# HTTP totals from httpx request lines
[int]$HttpRequestsTotal = 0
[int]$HttpGet = 0
[int]$HttpHead = 0
[int]$HttpPost = 0
[int]$HttpOtherMethod = 0
[int]$Http200 = 0
[int]$Http3xx = 0
[int]$Http403 = 0
[int]$Http404 = 0
[int]$Http4xxOther = 0
[int]$Http5xx = 0
[int]$RobotsFetches = 0
[int]$Robots403 = 0
[int]$Tcp25PreflightFailed = 0

# Crawl runner totals
[int]$CrawlRunnerPages = 0
[int]$CrawlRunnerUrlsAttempted = 0
[int]$CrawlRunnerSeedsAttempted = 0
[int]$CrawlRunnerSeedPeoplePages = 0
[int]$CrawlRunnerTiersEnqueued = 0

# Snapshot totals
[int]$SnapshotRowsTotal = 0

# Severity totals (with SMTP-off classification)
[int]$ErrorLinesTotal = 0
[int]$ErrorLinesTrue = 0
[int]$SmtpExpectedErrorLines = 0
[int]$WarningLines = 0

$CompanyDurations = New-Object System.Collections.Generic.List[double]
$CompanyPagesFetched = New-Object System.Collections.Generic.List[int]
$CompanyApprovedPeople = New-Object System.Collections.Generic.List[int]

$Failures = New-Object System.Collections.Generic.List[string]
$CompanyMetrics = New-Object System.Collections.Generic.List[psobject]

# ---------------------------
# Ensure data/ and logs dirs exist
# ---------------------------
$DataDir = Join-Path (Get-Location) "data"
if (-not (Test-Path -LiteralPath $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir | Out-Null
}

$LogsDir = Join-Path $DataDir "batch_logs"
if (-not (Test-Path -LiteralPath $LogsDir)) {
    New-Item -ItemType Directory -Path $LogsDir | Out-Null
}

# Aggregate log
$AllLog = Join-Path $LogsDir "ALL_COMPANIES.txt"
Remove-Item -LiteralPath $AllLog -Force -ErrorAction SilentlyContinue

# Metrics exports
$MetricsCsv = Join-Path $LogsDir "BATCH_METRICS.csv"
$MetricsJsonl = Join-Path $LogsDir "BATCH_METRICS.jsonl"
Remove-Item -LiteralPath $MetricsCsv -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $MetricsJsonl -Force -ErrorAction SilentlyContinue
$script:__CsvInitialized = $false

# ---------------------------
# Build an absolute DB path safely
# ---------------------------
$DbPath = Join-Path (Resolve-Path $DataDir).Path $DbFileName
Remove-Item -LiteralPath $DbPath -Force -ErrorAction SilentlyContinue

# Force ALL internal get_conn() calls to hit the same DB
$env:DATABASE_URL = "sqlite:///$($DbPath -replace '\\','/')"

# Force Python to flush output immediately
$env:PYTHONUNBUFFERED = "1"
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

Write-Host "DBPath: $DbPath"
Write-Host "DATABASE_URL: $env:DATABASE_URL"
Write-Host "LogsDir: $LogsDir"
Write-Host "AllLog: $AllLog"
Write-Host "MetricsCsv: $MetricsCsv"
Write-Host "MetricsJsonl: $MetricsJsonl"

# ---------------------------
# Company list (name, domain)
# Updated to your 50-company table
# ---------------------------
$Companies = @(
    @("Sentry", "sentry.io"),
    @("Vercel", "vercel.com"),
    @("Netlify", "netlify.com"),
    @("Render", "render.com"),
    @("Supabase", "supabase.com"),
    @("PlanetScale", "planetscale.com"),
    @("Temporal", "temporal.io"),
    @("Prefect", "prefect.io"),
    @("Pulumi", "pulumi.com"),
    @("Honeycomb", "honeycomb.io"),
    @("PostHog", "posthog.com"),
    @("Metabase", "metabase.com"),
    @("Tailscale", "tailscale.com"),
    @("Chainguard", "chainguard.dev"),
    @("Secureframe", "secureframe.com"),
    @("Bugcrowd", "bugcrowd.com"),
    @("Detectify", "detectify.com"),
    @("Cobalt", "cobalt.io"),
    @("Hightouch", "hightouch.com"),
    @("Census", "getcensus.com"),
    @("Hex", "hex.tech"),
    @("RudderStack", "rudderstack.com"),
    @("MotherDuck", "motherduck.com"),
    @("Chili Piper", "chilipiper.com"),
    @("Mutiny", "mutinyhq.com"),
    @("Customer.io", "customer.io"),
    @("lemlist", "lemlist.com"),
    @("Help Scout", "helpscout.com"),
    @("Gorgias", "gorgias.com"),
    @("Ashby", "ashbyhq.com"),
    @("ChartHop", "charthop.com"),
    @("Mercury", "mercury.com"),
    @("Lithic", "lithic.com"),
    @("Alloy", "alloy.com"),
    @("Unit21", "unit21.ai"),
    @("Synctera", "synctera.com"),
    @("Treasury Prime", "treasuryprime.com"),
    @("Sardine", "sardine.ai"),
    @("Noyo", "noyo.com"),
    @("Ribbon Health", "ribbonhealth.com"),
    @("Particle Health", "particlehealth.com"),
    @("Watershed", "watershed.com"),
    @("Patch", "patch.io"),
    @("Shippo", "goshippo.com"),
    @("ShipHero", "shiphero.com"),
    @("OpenSpace", "openspace.ai"),
    @("BuildOps", "buildops.com"),
    @("Homebound", "homebound.com"),
    @("Flyhomes", "flyhomes.com"),
    @("SpotDraft", "spotdraft.com")
)

# Open ALL_COMPANIES writer once for the entire run (prevents accidental double-open / duplication).
$swAll = $null
try {
    $swAll = New-Utf8StreamWriter -Path $AllLog -Append $false
    $script:__AllLogPath = $AllLog
} catch {
    Write-Host "ERROR: Failed to open ALL_COMPANIES log: $AllLog" -ForegroundColor Red
    throw
}

$FirstRun = $true
$idx = 0

foreach ($company in $Companies) {
    $idx++
    $CompaniesAttempted++

    $Name = $company[0]
    $Domain = "$($company[1])".Trim().ToLowerInvariant()

    $SafeName = Sanitize-FileName $Name
    $SafeDomain = Sanitize-FileName $Domain
    $PerLog = Join-Path $LogsDir ("{0:D2}_{1}_{2}.txt" -f $idx, $SafeName, $SafeDomain)
    Remove-Item -LiteralPath $PerLog -Force -ErrorAction SilentlyContinue

    $swPer = $null
    $CompanyStart = Get-Date
    $metricsRecorded = $false

    # Per-company metrics (schema initialized defensively; no nullable structures)
    $m = New-CompanyMetricsSchema -Idx $idx -Company $Name -Domain $Domain

    $uniqueUrls = New-Object "System.Collections.Generic.HashSet[string]"
    $inAutoSummary = $false
    $inSnapshot = $false
    $snapshotHeaderSeen = $false

    $aiCallStart = $null

    try {
        $swPer = New-Utf8StreamWriter -Path $PerLog -Append $false
        $script:__PerLogPath = $PerLog

        $stamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
        $sep = "=" * 70

        Write-Host ""
        Write-Host $sep -ForegroundColor Cyan
        Write-Host "  Testing: $Name ($Domain)" -ForegroundColor Cyan
        Write-Host $sep -ForegroundColor Cyan

        Write-LineBoth $swPer $swAll ""
        Write-LineBoth $swPer $swAll $sep
        Write-LineBoth $swPer $swAll ("Testing: {0} ({1})" -f $Name, $Domain)
        Write-LineBoth $swPer $swAll ("RunMode: {0} | SMTP: {1}" -f $RunMode, $SMTPMode)
        Write-LineBoth $swPer $swAll ("Timestamp: {0}" -f $stamp)
        Write-LineBoth $swPer $swAll ("DBPath: {0}" -f $DbPath)
        Write-LineBoth $swPer $swAll ("LogLevel: {0}" -f $LogLevel)
        Write-LineBoth $swPer $swAll ("PerLog: {0}" -f $PerLog)
        Write-LineBoth $swPer $swAll $sep
        Write-LineBoth $swPer $swAll ""

        $Args = @(
            ".\scripts\demo_autodiscovery.py",
            "--db", $DbPath,
            "--company", $Name,
            "--domain", $Domain,
            "--log-level", $LogLevel
        )

        if ($FirstRun) {
            $Args += "--init-schema"
            $FirstRun = $false
        }

        # Stream output line-by-line to both logs (NO variable capture; NO truncation)
        & $PyExe @Args 2>&1 | ForEach-Object {
            $line = $_.ToString()

            # Classify SMTP-expected "errors"
            $isSmtpExpected =
                ($line -match 'TCP/25 preflight FAILED') -or
                ($line -match 'Skipping SMTP verification step') -or
                ($line -match 'MX tried:')

            # Severity counters
            if ($line -match '\[WARNING\]') {
                $m.warning_lines++
                $WarningLines++
            }
            if ($line -match '\[ERROR\]') {
                $m.error_lines_total++
                $ErrorLinesTotal++
                if ($isSmtpExpected) {
                    $m.smtp_expected_error_lines++
                    $SmtpExpectedErrorLines++
                } else {
                    $m.error_lines_true++
                    $ErrorLinesTrue++
                }
            }

            # TCP/25 preflight (expected in your mode, but still counted)
            if ($line -match 'TCP/25 preflight FAILED') {
                $m.tcp25_preflight_failed++
                $Tcp25PreflightFailed++
            }

            # Parse timestamps (for AI latency)
            $dt = Try-Parse-LogTimestamp -Line $line

            # AI latency hooks
            if ($line -match 'Calling AI refiner') {
                if ($null -ne $dt) { $aiCallStart = $dt }
            }
            if ($line -match 'AI refinement complete') {
                if ($null -ne $dt -and $null -ne $aiCallStart) {
                    $m.ai_seconds += ($dt - $aiCallStart).TotalSeconds
                    $aiCallStart = $null
                }
                # Also parse: "AI refinement complete: 2 raw … 0 people"
                if ($line -match 'AI refinement complete:\s*(\d+)\s+raw.*\s+(\d+)\s+people') {
                    $m.ai_input_candidates = [int]$Matches[1]
                    $m.ai_returned_people  = [int]$Matches[2]
                }
            }

            # httpx request lines
            if ($line -match 'httpx:\s+HTTP Request:\s+(?<method>[A-Z]+)\s+(?<url>\S+)\s+"HTTP/\d\.\d\s+(?<status>\d{3})') {
                $m.http_requests_total++
                $HttpRequestsTotal++

                $method = $Matches['method']
                $url = $Matches['url']
                $status = [int]$Matches['status']

                if ($null -ne $uniqueUrls) {
                    if (-not $uniqueUrls.Contains($url)) { [void]$uniqueUrls.Add($url) }
                }

                switch ($method) {
                    "GET"  { $m.http_get++;  $HttpGet++ }
                    "HEAD" { $m.http_head++; $HttpHead++ }
                    "POST" { $m.http_post++; $HttpPost++ }
                    default { $m.http_other_method++; $HttpOtherMethod++ }
                }

                if ($url -match '/robots\.txt(\?|$)') {
                    $m.robots_fetches++
                    $RobotsFetches++
                }

                if ($status -ge 200 -and $status -lt 300) { $m.http_200++; $Http200++ }
                elseif ($status -ge 300 -and $status -lt 400) { $m.http_3xx++; $Http3xx++ }
                elseif ($status -eq 403) {
                    $m.http_403++; $Http403++
                    if ($url -match '/robots\.txt(\?|$)') { $m.robots_403++; $Robots403++ }
                }
                elseif ($status -eq 404) { $m.http_404++; $Http404++ }
                elseif ($status -ge 400 -and $status -lt 500) { $m.http_4xx_other++; $Http4xxOther++ }
                elseif ($status -ge 500 -and $status -lt 600) { $m.http_5xx++; $Http5xx++ }
            }

            # Crawl runner summary
            if ($line -match 'crawl\.runner:\s+Crawl complete for\s+(?<domain>\S+):\s+origin=(?<origin>\S+)\s+pages=(?<pages>\d+)\s+urls_attempted=(?<urls>\d+)\s+seeds_attempted=(?<seeds>\d+)\s+seed_people_pages=(?<seedpp>\d+)\s+tiers_enqueued=(?<tiers>\d+)') {
                $m.crawl_origin = $Matches['origin']
                $m.crawl_pages = [int]$Matches['pages']
                $m.crawl_urls_attempted = [int]$Matches['urls']
                $m.crawl_seeds_attempted = [int]$Matches['seeds']
                $m.crawl_seed_people_pages = [int]$Matches['seedpp']
                $m.crawl_tiers_enqueued = [int]$Matches['tiers']

                $CrawlRunnerPages += $m.crawl_pages
                $CrawlRunnerUrlsAttempted += $m.crawl_urls_attempted
                $CrawlRunnerSeedsAttempted += $m.crawl_seeds_attempted
                $CrawlRunnerSeedPeoplePages += $m.crawl_seed_people_pages
                $CrawlRunnerTiersEnqueued += $m.crawl_tiers_enqueued
            }

            # Persisted pages
            if ($line -match 'Persisted\s+(?<n>\d+)\s+pages\s+into\s+sources') {
                $m.pages_persisted = [int]$Matches['n']
            }

            # People cards
            if ($line -match 'src\.extract\.people_cards:\s+Extracted\s+(?<n>\d+)\s+people cards') {
                $m.people_cards_extracted_total += [int]$Matches['n']
            }
            if ($line -match 'People cards extractor found\s+(?<n>\d+)\s+candidates') {
                $m.people_cards_candidates_found = [int]$Matches['n']
            }

            # Team-page fallback inserted
            if ($line -match 'Team-page fallback inserted\s+(?<n>\d+)\s+people') {
                $m.team_fallback_inserted = [int]$Matches['n']
            }

            # "Found X people without non-placeholder emails ... limiting to first Y"
            if ($line -match 'Found\s+(?<x>\d+)\s+people without non-placeholder emails.*limiting to first\s+(?<y>\d+)') {
                $m.found_no_email_people = [int]$Matches['x']
                $m.limited_to_people = [int]$Matches['y']
            }

            # v_emails_latest snapshot row count
            if ($line -match '^=== v_emails_latest snapshot for domain:') {
                $inSnapshot = $true
                $snapshotHeaderSeen = $false
            }
            elseif ($inSnapshot) {
                if (-not $snapshotHeaderSeen) {
                    if ($line -match '^\s*email\s+') { $snapshotHeaderSeen = $true }
                }
                else {
                    if ($line -match '^\S+@\S+\s') {
                        $m.snapshot_rows++
                        $SnapshotRowsTotal++
                    }
                    elseif ($line -match '^\s*$' -or $line -match '^[-=]{10,}') {
                        $inSnapshot = $false
                    }
                }
            }

            # AUTODISCOVERY SUMMARY parsing
            if ($line -match 'AUTODISCOVERY SUMMARY') {
                $inAutoSummary = $true
            }
            elseif ($inAutoSummary) {
                if ($line -match '^-{5,}$') {
                    $inAutoSummary = $false
                } else {
                    if ($line -match '^\s+Pages fetched:\s*(\d+)') { $m.pages_fetched = [int]$Matches[1] }
                    if ($line -match '^\s+Pages skipped \(robots\):\s*(\d+)') { $m.pages_skipped_robots = [int]$Matches[1] }
                    if ($line -match '^\s+Candidates with email:\s*(\d+)') { $m.candidates_with_email = [int]$Matches[1] }
                    if ($line -match '^\s+Candidates without email:\s*(\d+)') { $m.candidates_without_email = [int]$Matches[1] }
                    if ($line -match '^\s+AI enabled:\s*(yes|no)') { $m.ai_enabled = $Matches[1] }
                    if ($line -match '^\s+AI called:\s*(yes|no)') { $m.ai_called = $Matches[1] }
                    if ($line -match '^\s+AI call succeeded:\s*(yes|no)') { $m.ai_succeeded = $Matches[1] }
                    if ($line -match '^\s+AI input candidates:\s*(\d+)') { $m.ai_input_candidates = [int]$Matches[1] }
                    if ($line -match '^\s+AI returned people:\s*(\d+)') { $m.ai_returned_people = [int]$Matches[1] }
                    if ($line -match '^\s+Fallback used:\s*(yes|no)') { $m.fallback_used = $Matches[1] }
                    if ($line -match '^\s+Final approved people:\s*(\d+)') { $m.final_approved_people = [int]$Matches[1] }
                    if ($line -match '^\s+People upserted:\s*(\d+)') { $m.people_upserted = [int]$Matches[1] }
                    if ($line -match '^\s+Emails upserted:\s*(\d+)') { $m.emails_upserted = [int]$Matches[1] }
                    if ($line -match '^\s+Permutations generated:\s*(\d+)') { $m.permutations_generated = [int]$Matches[1] }
                }
            }

            # Always stream to logs + console
            if ($null -ne $swPer) { $swPer.WriteLine($line) }
            if ($null -ne $swAll) { $swAll.WriteLine($line) }
            $line
        }

        $ExitCode = $LASTEXITCODE
        $m.exit_code = [int]$ExitCode

        # If AI is explicitly disabled and called/succeeded are missing, normalize for readability.
        if ($m.ai_enabled -eq "no") {
            if ([string]::IsNullOrWhiteSpace($m.ai_called)) { $m.ai_called = "no" }
            if ([string]::IsNullOrWhiteSpace($m.ai_succeeded)) { $m.ai_succeeded = "no" }
        }

        $CompanyEnd = Get-Date
        $dur = ($CompanyEnd - $CompanyStart).TotalSeconds
        $m.runtime_seconds = [math]::Round($dur, 3)
        $CompanyDurations.Add($dur) | Out-Null

        # finalize URL uniqueness metrics
        $m.http_unique_urls = if ($null -ne $uniqueUrls) { $uniqueUrls.Count } else { 0 }
        $m.http_duplicate_requests = [math]::Max(0, $m.http_requests_total - $m.http_unique_urls)

        # batch aggregates
        $TotalPagesFetched += [int]$m.pages_fetched
        $TotalPagesSkippedRobots += [int]$m.pages_skipped_robots
        $TotalCandidatesWithEmail += [int]$m.candidates_with_email
        $TotalCandidatesWithoutEmail += [int]$m.candidates_without_email
        $TotalFinalApprovedPeople += [int]$m.final_approved_people
        $TotalPeopleUpserted += [int]$m.people_upserted
        $TotalEmailsUpserted += [int]$m.emails_upserted
        $TotalPermutationsGenerated += [int]$m.permutations_generated
        $TotalAiInputCandidates += [int]$m.ai_input_candidates
        $TotalAiReturnedPeople += [int]$m.ai_returned_people

        $CompanyPagesFetched.Add([int]$m.pages_fetched) | Out-Null
        $CompanyApprovedPeople.Add([int]$m.final_approved_people) | Out-Null

        if ([int]$m.candidates_with_email -gt 0) { $CompaniesWithPublishedEmails++ }
        if ($m.ai_called -eq "yes") { $CompaniesAiCalled++ }
        if ($m.ai_succeeded -eq "yes") { $CompaniesAiSucceeded++ }
        if ($m.fallback_used -eq "yes") { $CompaniesFallbackUsed++ }

        # Completed / failed tracking
        if ($ExitCode -ne 0) {
            $CompaniesFailedExit++
            $Failures.Add("$Name ($Domain) [exit=$ExitCode]") | Out-Null
            Write-Host "`nFAILED (exit=$ExitCode): $Name" -ForegroundColor Red
        } else {
            $CompaniesCompleted++
            Write-Host "`nCompleted: $Name" -ForegroundColor Green

            if ([int]$m.final_approved_people -eq 0) {
                $CompaniesZeroValid++
                $ZeroValidRuntimeSeconds += $dur
                $ZeroValidPagesFetched += [int]$m.pages_fetched
                $ZeroValidPagesPersisted += [int]$m.pages_persisted
            }
        }

        # Per-company metrics block
        Write-LineBoth $swPer $swAll ""
        Write-LineBoth $swPer $swAll "=== COMPANY METRICS ==="
        Write-LineBoth $swPer $swAll ("ExitCode: {0}" -f $m.exit_code)
        Write-LineBoth $swPer $swAll ("RuntimeSeconds: {0:N2}" -f $m.runtime_seconds)
        Write-LineBoth $swPer $swAll ("PagesFetched: {0}" -f $m.pages_fetched)
        Write-LineBoth $swPer $swAll ("PagesSkippedRobots: {0}" -f $m.pages_skipped_robots)
        Write-LineBoth $swPer $swAll ("PagesPersisted: {0}" -f $m.pages_persisted)
        Write-LineBoth $swPer $swAll ("CandidatesWithEmail: {0}" -f $m.candidates_with_email)
        Write-LineBoth $swPer $swAll ("CandidatesWithoutEmail: {0}" -f $m.candidates_without_email)
        Write-LineBoth $swPer $swAll ("FinalApprovedPeople: {0}" -f $m.final_approved_people)
        Write-LineBoth $swPer $swAll ("PermutationsGenerated: {0}" -f $m.permutations_generated)
        Write-LineBoth $swPer $swAll ("AIEnabled: {0} | AICalled: {1} | AISucceeded: {2} | AISeconds: {3:N2}" -f $m.ai_enabled, $m.ai_called, $m.ai_succeeded, $m.ai_seconds)
        Write-LineBoth $swPer $swAll ("AIInputCandidates: {0} | AIReturnedPeople: {1} | FallbackUsed: {2}" -f $m.ai_input_candidates, $m.ai_returned_people, $m.fallback_used)
        Write-LineBoth $swPer $swAll ("HTTPRequests: {0} | UniqueURLs: {1} | Duplicates: {2}" -f $m.http_requests_total, $m.http_unique_urls, $m.http_duplicate_requests)
        Write-LineBoth $swPer $swAll ("HTTPStatus: 200={0} 3xx={1} 403={2} 404={3} 4xx_other={4} 5xx={5}" -f $m.http_200, $m.http_3xx, $m.http_403, $m.http_404, $m.http_4xx_other, $m.http_5xx)
        Write-LineBoth $swPer $swAll ("RobotsFetches: {0} | Robots403: {1}" -f $m.robots_fetches, $m.robots_403)
        Write-LineBoth $swPer $swAll ("TCP25PreflightFailed: {0}" -f $m.tcp25_preflight_failed)
        Write-LineBoth $swPer $swAll ("SnapshotRows: {0}" -f $m.snapshot_rows)
        Write-LineBoth $swPer $swAll ("Errors: total={0} true={1} smtp_expected={2} | Warnings: {3}" -f $m.error_lines_total, $m.error_lines_true, $m.smtp_expected_error_lines, $m.warning_lines)
        Write-LineBoth $swPer $swAll ("-" * 70)
        Write-LineBoth $swPer $swAll ""

        # Record metrics exactly once (prevents duplicate rows in CSV/JSONL and summary tables).
        if (-not $metricsRecorded) {
            $mObj = [pscustomobject]$m
            $CompanyMetrics.Add($mObj) | Out-Null
            Write-MetricsExports -MetricsObject $mObj -CsvPath $MetricsCsv -JsonlPath $MetricsJsonl
            $metricsRecorded = $true
        }
    }
    catch {
        $Msg = $_.Exception.Message
        $CompaniesFailedException++
        $Failures.Add("$Name ($Domain) [exception=$Msg]") | Out-Null

        $CompanyEnd = Get-Date
        $dur = ($CompanyEnd - $CompanyStart).TotalSeconds

        $m.exception = $Msg
        $m.runtime_seconds = [math]::Round($dur, 3)
        if ($m.exit_code -eq -1) { $m.exit_code = -1 }

        Write-LineBoth $swPer $swAll ""
        Write-LineBoth $swPer $swAll "EXCEPTION:"
        Write-LineBoth $swPer $swAll $Msg
        Write-LineBoth $swPer $swAll ("-" * 70)
        Write-LineBoth $swPer $swAll ""

        Write-Host "`nFAILED (exception): $Name" -ForegroundColor Red
        Write-Host $Msg -ForegroundColor DarkRed

        # Record/export metrics exactly once (best-effort, even on exception).
        if (-not $metricsRecorded) {
            try {
                $mObj = [pscustomobject]$m
                $CompanyMetrics.Add($mObj) | Out-Null
                Write-MetricsExports -MetricsObject $mObj -CsvPath $MetricsCsv -JsonlPath $MetricsJsonl
                $metricsRecorded = $true
            } catch {
                # best-effort
            }
        }
    }
    finally {
        if ($null -ne $swPer) { $swPer.Dispose() }
        $script:__PerLogPath = ""
    }

    if ($SleepSeconds -gt 0) {
        Write-Host "Waiting $SleepSeconds seconds before next company..." -ForegroundColor Yellow
        Start-Sleep -Seconds $SleepSeconds
    }
}

# ---------------------------
# Batch Summary + Diagnosis
# ---------------------------
$BatchEnd = Get-Date
$BatchSeconds = ($BatchEnd - $BatchStart).TotalSeconds

$durArr = @()
foreach ($d in $CompanyDurations) { $durArr += [double]$d }
$avgDur = if ($durArr.Count -gt 0) { ($durArr | Measure-Object -Average).Average } else { 0.0 }
$minDur = if ($durArr.Count -gt 0) { ($durArr | Measure-Object -Minimum).Minimum } else { 0.0 }
$maxDur = if ($durArr.Count -gt 0) { ($durArr | Measure-Object -Maximum).Maximum } else { 0.0 }
$medDur = Median-Of -Values $durArr

$pagesArr = @()
foreach ($p in $CompanyPagesFetched) { $pagesArr += [double]$p }
$avgPages = if ($pagesArr.Count -gt 0) { ($pagesArr | Measure-Object -Average).Average } else { 0.0 }
$medPages = Median-Of -Values $pagesArr

$approvedArr = @()
foreach ($a in $CompanyApprovedPeople) { $approvedArr += [double]$a }
$avgApproved = if ($approvedArr.Count -gt 0) { ($approvedArr | Measure-Object -Average).Average } else { 0.0 }
$medApproved = Median-Of -Values $approvedArr

$ZeroValidPct = if ($CompaniesCompleted -gt 0) { (100.0 * $CompaniesZeroValid / $CompaniesCompleted) } else { 0.0 }
$ZeroValidRuntimePct = if ($BatchSeconds -gt 0) { (100.0 * $ZeroValidRuntimeSeconds / $BatchSeconds) } else { 0.0 }
$ZeroValidPagesFetchedPct = if ($TotalPagesFetched -gt 0) { (100.0 * $ZeroValidPagesFetched / $TotalPagesFetched) } else { 0.0 }

# Pages-per-approved ratios
$PagesPerApprovedAll = if ($TotalFinalApprovedPeople -gt 0) { ($TotalPagesFetched / [double]$TotalFinalApprovedPeople) } else { 0.0 }
$PagesPerApprovedNonZero = 0.0
if ($null -ne $CompanyMetrics -and $CompanyMetrics.Count -gt 0) {
    $sumPagesNonZero = 0.0
    $sumApprovedNonZero = 0.0
    foreach ($cm in $CompanyMetrics) {
        $p = $(IntOrZero $cm.pages_fetched)
        $ap = $(IntOrZero $cm.final_approved_people)
        if ($ap -gt 0) {
            $sumPagesNonZero += $p
            $sumApprovedNonZero += $ap
        }
    }
    if ($sumApprovedNonZero -gt 0) { $PagesPerApprovedNonZero = $sumPagesNonZero / $sumApprovedNonZero }
}

$CandidateTotal = $TotalCandidatesWithEmail + $TotalCandidatesWithoutEmail
$PublishedEmailCandidatePct = if ($CandidateTotal -gt 0) { (100.0 * $TotalCandidatesWithEmail / $CandidateTotal) } else { 0.0 }
$CompaniesPublishedEmailPct = if ($CompaniesCompleted -gt 0) { (100.0 * $CompaniesWithPublishedEmails / $CompaniesCompleted) } else { 0.0 }
$AiCalledPct = if ($CompaniesCompleted -gt 0) { (100.0 * $CompaniesAiCalled / $CompaniesCompleted) } else { 0.0 }
$AiSucceededPct = if ($CompaniesAiCalled -gt 0) { (100.0 * $CompaniesAiSucceeded / $CompaniesAiCalled) } else { 0.0 }
$AiYieldPct = if ($TotalAiInputCandidates -gt 0) { (100.0 * $TotalAiReturnedPeople / $TotalAiInputCandidates) } else { 0.0 }
$FallbackPct = if ($CompaniesCompleted -gt 0) { (100.0 * $CompaniesFallbackUsed / $CompaniesCompleted) } else { 0.0 }
$Http403Pct = if ($HttpRequestsTotal -gt 0) { (100.0 * $Http403 / $HttpRequestsTotal) } else { 0.0 }
$Http5xxPct = if ($HttpRequestsTotal -gt 0) { (100.0 * $Http5xx / $HttpRequestsTotal) } else { 0.0 }

# Top offenders (guarded; never assume non-empty lists)
$topSlow = @()
$top403 = @()
$topZeroTime = @()

if ($null -ne $CompanyMetrics -and $CompanyMetrics.Count -gt 0) {
    $topSlow = $CompanyMetrics | Sort-Object -Property runtime_seconds -Descending | Select-Object -First 5
    $top403  = $CompanyMetrics | Sort-Object -Property http_403 -Descending | Select-Object -First 5
    $topZeroTime = $CompanyMetrics |
        Where-Object { $_.exit_code -eq 0 -and $(IntOrZero $_.final_approved_people) -eq 0 } |
        Sort-Object -Property runtime_seconds -Descending |
        Select-Object -First 5
}

# Append summary + diagnosis to ALL_COMPANIES.txt (emit exactly once)
try {
    $swAll.WriteLine("")
    $swAll.WriteLine(("=" * 70))
    $swAll.WriteLine("BATCH COMPLETE")
    $swAll.WriteLine(("=" * 70))
    $swAll.WriteLine("RunMode: $RunMode | SMTP: $SMTPMode")
    $swAll.WriteLine("Started: $($BatchStart.ToString('yyyy-MM-dd HH:mm:ss'))")
    $swAll.WriteLine("Ended:   $($BatchEnd.ToString('yyyy-MM-dd HH:mm:ss'))")
    $swAll.WriteLine(("BatchRuntimeSeconds: {0:N2}" -f $BatchSeconds))
    $swAll.WriteLine("")

    $swAll.WriteLine("=== COMPANY OUTCOMES ===")
    $swAll.WriteLine("Attempted: $CompaniesAttempted")
    $swAll.WriteLine("Completed (exit=0): $CompaniesCompleted")
    $swAll.WriteLine("Failed (non-zero exit): $CompaniesFailedExit")
    $swAll.WriteLine("Failed (exception): $CompaniesFailedException")
    $swAll.WriteLine("")

    $swAll.WriteLine("=== ZERO-VALID-CANDIDATE METRICS (Final approved people = 0) ===")
    $swAll.WriteLine("Zero-valid companies: $CompaniesZeroValid")
    $swAll.WriteLine(("Zero-valid rate (% of completed): {0:N2}" -f $ZeroValidPct))
    $swAll.WriteLine(("Zero-valid runtime seconds: {0:N2}" -f $ZeroValidRuntimeSeconds))
    $swAll.WriteLine(("Zero-valid runtime (% of batch): {0:N2}" -f $ZeroValidRuntimePct))
    $swAll.WriteLine("Zero-valid pages fetched: $ZeroValidPagesFetched")
    $swAll.WriteLine(("Zero-valid pages fetched (% of batch pages): {0:N2}" -f $ZeroValidPagesFetchedPct))
    $swAll.WriteLine("Zero-valid pages persisted: $ZeroValidPagesPersisted")
    $swAll.WriteLine("")

    $swAll.WriteLine("=== RUNTIME DISTRIBUTION ===")
    $swAll.WriteLine(("Avg company runtime (s): {0:N2}" -f $avgDur))
    $swAll.WriteLine(("Median company runtime (s): {0:N2}" -f $medDur))
    $swAll.WriteLine(("Min company runtime (s): {0:N2}" -f $minDur))
    $swAll.WriteLine(("Max company runtime (s): {0:N2}" -f $maxDur))
    $swAll.WriteLine("")

    $swAll.WriteLine("=== AUTODISCOVERY SUMMARY TOTALS ===")
    $swAll.WriteLine("Total pages fetched: $TotalPagesFetched")
    $swAll.WriteLine("Total pages skipped (robots): $TotalPagesSkippedRobots")
    $swAll.WriteLine(("Avg pages fetched / company (completed): {0:N2}" -f $avgPages))
    $swAll.WriteLine(("Median pages fetched / company (completed): {0:N2}" -f $medPages))
    $swAll.WriteLine("")
    $swAll.WriteLine("Total candidates with email: $TotalCandidatesWithEmail")
    $swAll.WriteLine("Total candidates without email: $TotalCandidatesWithoutEmail")
    $swAll.WriteLine(("Published-email candidate rate (% of all candidates): {0:N2}" -f $PublishedEmailCandidatePct))
    $swAll.WriteLine(("Companies with any published emails: {0} ({1:N2}%)" -f $CompaniesWithPublishedEmails, $CompaniesPublishedEmailPct))
    $swAll.WriteLine("")
    $swAll.WriteLine("Total AI input candidates: $TotalAiInputCandidates")
    $swAll.WriteLine("Total AI returned people: $TotalAiReturnedPeople")
    $swAll.WriteLine(("AI yield (% returned / input): {0:N2}" -f $AiYieldPct))
    $swAll.WriteLine(("Companies AI called: {0} ({1:N2}%)" -f $CompaniesAiCalled, $AiCalledPct))
    $swAll.WriteLine(("AI success rate (% of called): {0:N2}" -f $AiSucceededPct))
    $swAll.WriteLine(("Companies fallback used: {0} ({1:N2}%)" -f $CompaniesFallbackUsed, $FallbackPct))
    $swAll.WriteLine("")
    $swAll.WriteLine("Total final approved people: $TotalFinalApprovedPeople")
    $swAll.WriteLine(("Avg approved people / company (completed): {0:N2}" -f $avgApproved))
    $swAll.WriteLine(("Median approved people / company (completed): {0:N2}" -f $medApproved))
    $swAll.WriteLine("Total people upserted: $TotalPeopleUpserted")
    $swAll.WriteLine("Total emails upserted: $TotalEmailsUpserted")
    $swAll.WriteLine("Total permutations generated: $TotalPermutationsGenerated")
    $swAll.WriteLine(("Pages per approved person (global, includes zero-approved companies in pages): {0:N2}" -f $PagesPerApprovedAll))
    $swAll.WriteLine(("Pages per approved person (only companies with approved>0): {0:N2}" -f $PagesPerApprovedNonZero))
    $swAll.WriteLine("")
    $swAll.WriteLine("Total v_emails_latest snapshot rows: $SnapshotRowsTotal")
    $swAll.WriteLine("")

    $swAll.WriteLine("=== HTTP (httpx observed) ===")
    $swAll.WriteLine("HTTP requests total: $HttpRequestsTotal")
    $swAll.WriteLine("Methods: GET=$HttpGet HEAD=$HttpHead POST=$HttpPost OTHER=$HttpOtherMethod")
    $swAll.WriteLine("Status: 200=$Http200 3xx=$Http3xx 403=$Http403 404=$Http404 4xx_other=$Http4xxOther 5xx=$Http5xx")
    $swAll.WriteLine(("403 rate (% of requests): {0:N2}" -f $Http403Pct))
    $swAll.WriteLine(("5xx rate (% of requests): {0:N2}" -f $Http5xxPct))
    $swAll.WriteLine("robots.txt fetches: $RobotsFetches (403: $Robots403)")
    $swAll.WriteLine("TCP/25 preflight failed lines (expected in this mode): $Tcp25PreflightFailed")
    $swAll.WriteLine("")

    $swAll.WriteLine("=== CRAWL RUNNER TOTALS (if emitted) ===")
    $swAll.WriteLine("Crawl pages (runner): $CrawlRunnerPages")
    $swAll.WriteLine("URLs attempted (runner): $CrawlRunnerUrlsAttempted")
    $swAll.WriteLine("Seeds attempted (runner): $CrawlRunnerSeedsAttempted")
    $swAll.WriteLine("Seed people pages (runner): $CrawlRunnerSeedPeoplePages")
    $swAll.WriteLine("Tiers enqueued (runner): $CrawlRunnerTiersEnqueued")
    $swAll.WriteLine("")

    $swAll.WriteLine("=== SEVERITY ===")
    $swAll.WriteLine(("ERROR lines: total={0} true={1} smtp_expected={2}" -f $ErrorLinesTotal, $ErrorLinesTrue, $SmtpExpectedErrorLines))
    $swAll.WriteLine("WARNING lines: $WarningLines")
    $swAll.WriteLine("")

    $swAll.WriteLine("=== COMPANY METRICS TABLE (completed) ===")
    $swAll.WriteLine("idx | domain | pages_fetched | cand_email | cand_no_email | ai_in | ai_out | final_approved | perms | http_req | http_403 | runtime_s | snapshot_rows")
    foreach ($cm in ($CompanyMetrics | Where-Object { $_.exit_code -eq 0 } | Sort-Object idx)) {
        $pf = $(IntOrZero $cm.pages_fetched)
        $ce = $(IntOrZero $cm.candidates_with_email)
        $cne = $(IntOrZero $cm.candidates_without_email)
        $aii = $(IntOrZero $cm.ai_input_candidates)
        $aio = $(IntOrZero $cm.ai_returned_people)
        $fa = $(IntOrZero $cm.final_approved_people)
        $pg = $(IntOrZero $cm.permutations_generated)
        $hr = $(IntOrZero $cm.http_requests_total)
        $h403 = $(IntOrZero $cm.http_403)
        $rt = [double]$cm.runtime_seconds
        $sr = $(IntOrZero $cm.snapshot_rows)
        $swAll.WriteLine(("{0:D2} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9} | {10} | {11:N2} | {12}" -f $cm.idx, $cm.domain, $pf, $ce, $cne, $aii, $aio, $fa, $pg, $hr, $h403, $rt, $sr))
    }
    $swAll.WriteLine("")

    $swAll.WriteLine("=== TOP OFFENDERS ===")
    $swAll.WriteLine("Top slowest (runtime_s):")
    foreach ($cm in $topSlow) {
        $swAll.WriteLine(("  - {0:D2} {1} ({2}) runtime_s={3:N2} pages={4} approved={5} http_403={6}" -f $cm.idx, $cm.company, $cm.domain, $cm.runtime_seconds, $(IntOrZero $cm.pages_fetched), $(IntOrZero $cm.final_approved_people), $(IntOrZero $cm.http_403)))
    }
    $swAll.WriteLine("Top 403 (http_403):")
    foreach ($cm in $top403) {
        $swAll.WriteLine(("  - {0:D2} {1} ({2}) http_403={3} http_req={4} pages={5} approved={6}" -f $cm.idx, $cm.company, $cm.domain, $(IntOrZero $cm.http_403), $(IntOrZero $cm.http_requests_total), $(IntOrZero $cm.pages_fetched), $(IntOrZero $cm.final_approved_people)))
    }
    $swAll.WriteLine("Top zero-valid by runtime:")
    foreach ($cm in $topZeroTime) {
        $swAll.WriteLine(("  - {0:D2} {1} ({2}) runtime_s={3:N2} pages={4} http_403={5} ai_in={6} ai_out={7}" -f $cm.idx, $cm.company, $cm.domain, (0.0 + $cm.runtime_seconds), $(IntOrZero $cm.pages_fetched), $(IntOrZero $cm.http_403), $(IntOrZero $cm.ai_input_candidates), $(IntOrZero $cm.ai_returned_people)))
    }
    $swAll.WriteLine("")

    $swAll.WriteLine("=== RUN DIAGNOSIS (HEURISTIC, METRIC-DRIVEN) ===")
    if ($CompaniesFailedExit -gt 0 -or $CompaniesFailedException -gt 0) {
        $swAll.WriteLine("- One or more companies failed → isolate failures per-company and keep batch fault-tolerant.")
    }
    if ($ZeroValidPct -ge 30) {
        $swAll.WriteLine(("- High zero-valid rate ({0:N2}%) → tighten crawl targeting, add early abort for WAF/403-only domains, and gate AI calls." -f $ZeroValidPct))
    }
    if ($ZeroValidRuntimePct -ge 20) {
        $swAll.WriteLine(("- Material runtime spent on zero-valid companies ({0:N2}%) → add hard-block detection (robots 403 + homepage 403) and short-circuit fallbacks." -f $ZeroValidRuntimePct))
    }
    if ($Http403Pct -ge 25) {
        $swAll.WriteLine(("- High 403 rate ({0:N2}%) → implement per-domain 403 ceiling + abort policy; avoid repeatedly hitting blocked endpoints." -f $Http403Pct))
    }
    if ($PagesPerApprovedNonZero -ge 20) {
        $swAll.WriteLine(("- High pages-per-approved (non-zero companies) ({0:N2}) → precision issue; restrict extraction to intent pages (/team,/leadership,/about) and de-prioritize blog/news." -f $PagesPerApprovedNonZero))
    }
    if ($RunMode -eq "DISCOVERY_ONLY" -and $TotalPermutationsGenerated -gt 0) {
        $swAll.WriteLine("- Permutations generated while SMTP is intentionally disabled → consider gating permutations behind a flag, or only generate when exporting for verification.")
    }
    if ($CompaniesAiCalled -gt 0 -and $AiYieldPct -lt 30) {
        $swAll.WriteLine(("- AI yield is low ({0:N2}% people returned / input) → adjust AI acceptance criteria OR only call AI above a raw-candidate threshold." -f $AiYieldPct))
    }
    if ($Tcp25PreflightFailed -gt 0) {
        $swAll.WriteLine("- TCP/25 preflight failures are present; in this run mode treat them as expected so they do not mask real errors.")
    }
    $swAll.WriteLine("")

    if ($Failures.Count -gt 0) {
        $swAll.WriteLine(("Failures ({0}):" -f $Failures.Count))
        foreach ($f in $Failures) { $swAll.WriteLine("  - $f") }
        $swAll.WriteLine("")
    }

    $swAll.WriteLine("Metrics exports:")
    $swAll.WriteLine("  - BATCH_METRICS.csv:  $MetricsCsv")
    $swAll.WriteLine("  - BATCH_METRICS.jsonl: $MetricsJsonl")
    $swAll.WriteLine("")
}
finally {
    if ($null -ne $swAll) { $swAll.Dispose() }
    $script:__AllLogPath = ""
}

if ($Failures.Count -gt 0) {
    Write-Host "`nFailures ($($Failures.Count)):" -ForegroundColor Red
    foreach ($f in $Failures) { Write-Host "  - $f" -ForegroundColor Red }
    exit 1
}

Write-Host "`nAll runs succeeded." -ForegroundColor Green
exit 0
