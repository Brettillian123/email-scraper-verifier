Param(
  [string]$Domain = "crestwellpartners.com",
  [string]$DB = "dev.db"
)

$ErrorActionPreference = "Stop"

Write-Host "== R11 acceptance for domain '$Domain' with DB '$DB' =="

# 0) Safety backup
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$bak = "$DB.$stamp.bak"
Copy-Item -Path $DB -Destination $bak -ErrorAction SilentlyContinue
if (Test-Path $bak) { Write-Host "[INFO] Backup created: $bak" }

# 1) Ensure schema for R11
Write-Host "-> Ensuring R11 tables (people/emails/email_provenance)..."
python .\scripts\migrate_r11_add_extraction_tables.py --db "$DB"

# 2) Crawl a few pages via R10 (polite)
Write-Host "-> Crawling sources via R10 (writes into 'sources')..."
$env:CRAWL_SEED_PATHS = "/,/about,/team,/contact,/news"
$env:CRAWL_MAX_DEPTH = "1"
$env:CRAWL_MAX_PAGES_PER_DOMAIN = "5"
python .\scripts\migrate_r10_add_sources.py --db "$DB" | Out-Null
python .\scripts\crawl_domain.py "www.$Domain" --db "$DB"

# 3) Run the extractor (R11)
Write-Host "-> Extracting candidates (R11)..."
python .\scripts\extract_candidates.py --db "$DB" --domain "$Domain"

# 4) If nothing landed, seed one test page into sources and retry
$got = & sqlite3 $DB "SELECT COUNT(*) FROM email_provenance;"
if ([int]$got -eq 0) {
  Write-Host "[info] No emails found; seeding a small test page in 'sources' and retrying..."

  # See if 'domain' column exists in sources; pick seed SQL accordingly
  $hasDomain = & sqlite3 $DB "SELECT 1 FROM pragma_table_info('sources') WHERE name='domain' LIMIT 1;"
  if ($hasDomain -eq "1") {
    $seedSql = @"
INSERT INTO sources(domain, path, source_url, html, fetched_at)
VALUES (
  '$Domain',
  '/accept-r11',
  'https://$Domain/accept-r11',
  '<h1>Contact</h1>
   <p>Sales lead: <a href="mailto:jane.doe@$Domain">Jane Doe</a></p>
   <p>General inbox: info@$Domain</p>',
  datetime('now')
);
"@
  } else {
    $seedSql = @"
INSERT INTO sources(source_url, html, fetched_at)
VALUES (
  'https://$Domain/accept-r11',
  '<h1>Contact</h1>
   <p>Sales lead: <a href="mailto:jane.doe@$Domain">Jane Doe</a></p>
   <p>General inbox: info@$Domain</p>',
  datetime('now')
);
"@
  }

  & sqlite3 $DB $seedSql
  python .\scripts\extract_candidates.py --db "$DB" --domain "$Domain"
}

# 5) Show latest 5 emails with provenance
Write-Host "-> Showing latest 5 emails with provenance:"
& sqlite3 $DB ".headers on" ".mode column" @"
SELECT e.email,
       COALESCE(p.name,'') AS name,
       ep.source_url,
       ep.discovered_at
FROM email_provenance ep
JOIN emails e ON e.id = ep.email_id
LEFT JOIN people p ON p.id = e.person_id
ORDER BY ep.discovered_at DESC
LIMIT 5;
"@

Write-Host "== Done =="
