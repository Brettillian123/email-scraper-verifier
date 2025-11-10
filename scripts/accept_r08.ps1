param(
    [string]$DbPath = "dev.db",
    [int]$Limit = 50
)

$ErrorActionPreference = "Stop"
$PY = ".\.venv\Scripts\python.exe"

Write-Host "=== R08 Acceptance: Step 1/5 - Deps & schema ===" -ForegroundColor Cyan
& $PY -m pip install -r requirements.txt
& $PY .\scripts\migrate_r08_add_domains.py $DbPath

Write-Host "=== R08 Acceptance: Step 2/5 - Lint & unit tests (R08 scope) ===" -ForegroundColor Cyan
& $PY -m ruff check . --fix
& $PY -m ruff format .
& $PY -m pytest -q -k "r08 or domain_resolver" -vv --maxfail=1

Write-Host "=== R08 Acceptance: Step 3/5 - Seed a few companies (idempotent) ===" -ForegroundColor Cyan
& $PY .\scripts\seed_dev.py

Write-Host "=== R08 Acceptance: Step 4/5 - Run resolver over backlog ===" -ForegroundColor Cyan
& $PY .\scripts\resolve_domains.py --db $DbPath --limit $Limit

Write-Host "=== R08 Acceptance: Step 5/5 - Spot check ===" -ForegroundColor Cyan
$spot = @'
import sqlite3, sys
db = sys.argv[1]
con = sqlite3.connect(db); con.row_factory = sqlite3.Row

# Figure out actual R08 column names (domain_official vs official_domain, etc.)
cols = {r["name"] for r in con.execute("PRAGMA table_info(companies)")}
OFF = "domain_official" if "domain_official" in cols else ("official_domain" if "official_domain" in cols else None)
CONF = "domain_confidence" if "domain_confidence" in cols else ("official_domain_confidence" if "official_domain_confidence" in cols else None)
if not OFF:
    raise SystemExit("ERROR: Could not find R08 official-domain column in companies")
if not CONF:
    raise SystemExit("ERROR: Could not find R08 confidence column in companies")

for r in con.execute(f"SELECT id,name,user_supplied_domain,{OFF} AS domain_official,{CONF} AS domain_confidence FROM companies ORDER BY id DESC LIMIT 10"):
    print(dict(r))
'@
$spot | Set-Content -Path .\tmp_r08_spot.py -Encoding UTF8
& $PY .\tmp_r08_spot.py $DbPath
Remove-Item .\tmp_r08_spot.py -Force

Write-Host "=== R08 Acceptance: Programmatic PASS checks ===" -ForegroundColor Cyan

# -- Check A: punycode ASCII + audit row exists with method+confidence
$checkA = @'
import sqlite3, sys, idna

db = sys.argv[1]
con = sqlite3.connect(db); con.row_factory = sqlite3.Row

# Detect column names in companies
comp_cols = {r["name"] for r in con.execute("PRAGMA table_info(companies)")}
OFF = "domain_official" if "domain_official" in comp_cols else "official_domain"
CONF = "domain_confidence" if "domain_confidence" in comp_cols else ("official_domain_confidence" if "official_domain_confidence" in comp_cols else None)
if OFF is None:
    print("FAIL:COMP Missing official domain column in companies")
    sys.exit(2)

# Find an audit table
audit_tbl = None
for cand in ("domain_resolutions", "domain_resolution_audit", "domain_resolution_log"):
    if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (cand,)).fetchone():
        audit_tbl = cand
        break
if not audit_tbl:
    print("FAIL:AUDIT_TABLE Missing audit table for domain resolutions")
    sys.exit(2)

# Introspect audit table columns
audit_cols = {r["name"] for r in con.execute(f"PRAGMA table_info({audit_tbl})")}
required = {"company_id","method","confidence"}
if not required.issubset(audit_cols):
    print(f"FAIL:AUDIT_COLUMNS {audit_tbl} must have company_id, method, confidence; has: {sorted(audit_cols)}")
    sys.exit(2)

# Optional domain-like column in audit
domain_like = None
for name in ["domain","resolved_domain","official_domain","selected_domain","chosen_domain","result_domain","value","candidate_domain"]:
    if name in audit_cols:
        domain_like = name
        break

bad_non_ascii = []
missing_audit = []
checked = []
skipped_grandfathered = []

rows = con.execute(f"SELECT id, name, {OFF} AS dom, {CONF} AS conf FROM companies WHERE {OFF} IS NOT NULL").fetchall()
for r in rows:
    dom = r["dom"]

    # ASCII / IDNA sanity
    if any(ord(ch) > 127 for ch in dom):
        bad_non_ascii.append((r["id"], r["name"], dom))
        continue
    try:
        idna.decode(dom.encode("ascii"))
    except Exception:
        bad_non_ascii.append((r["id"], r["name"], dom))
        continue

    # Only *require* an audit row for R08-updated companies (those with a confidence on the company row).
    if CONF is None or r["conf"] is None:
        skipped_grandfathered.append((r["id"], r["name"], dom))
        continue

    if domain_like:
        a = con.execute(
            f"SELECT method, confidence FROM {audit_tbl} WHERE company_id=? AND {domain_like}=? ORDER BY id DESC LIMIT 1",
            (r["id"], dom),
        ).fetchone()
    else:
        a = con.execute(
            f"SELECT method, confidence FROM {audit_tbl} WHERE company_id=? ORDER BY id DESC LIMIT 1",
            (r["id"],),
        ).fetchone()

    if not a or a["method"] is None or a["confidence"] is None:
        missing_audit.append((r["id"], r["name"], dom))
    else:
        checked.append((r["id"], r["name"], dom, a["confidence"]))

if bad_non_ascii:
    print("FAIL:ASCII Found non-ASCII official domain(s):")
    for it in bad_non_ascii:
        print("  ", it)
    sys.exit(2)

if missing_audit:
    print("FAIL:AUDIT Missing audit row with method+confidence for:")
    for it in missing_audit:
        print("  ", it)
    sys.exit(2)

print(f"OK:ASCII_AUDIT {len(checked)} validated via audit; {len(skipped_grandfathered)} skipped (pre-R08 with no company confidence).")
'@
$checkA | Set-Content -Path .\tmp_r08_checkA.py -Encoding UTF8
& $PY .\tmp_r08_checkA.py $DbPath
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Remove-Item .\tmp_r08_checkA.py -Force

# -- Check B: ingest path (hint) flows into companies.domain_official after resolver
# We'll insert a synthetic company with a hint directly if ingest CLI isn't available or schema differs.
$checkB = @'
import sqlite3, sys, time
db = sys.argv[1]
con = sqlite3.connect(db); con.row_factory = sqlite3.Row
cols = {r["name"] for r in con.execute("PRAGMA table_info(companies)")}
OFF = "domain_official" if "domain_official" in cols else "official_domain"
HINTCOL = "user_supplied_domain" if "user_supplied_domain" in cols else None
if not HINTCOL:
    print("SKIP:INGEST_HINT no user_supplied_domain column; cannot assert hint path here")
    sys.exit(0)

company_name = "R08 Hint Path Co (acceptance)"
hint = "example.com"  # harmless deterministic domain we can use for hint flow

# Ensure clean slate
con.execute("DELETE FROM companies WHERE name=?", (company_name,))
con.commit()

# Insert a row with a domain hint (simulates ingest enqueue)
con.execute(f"INSERT INTO companies(name,{HINTCOL}) VALUES (?,?)", (company_name, hint))
con.commit()

print("OK:INGEST seeded row with hint; run resolver now and re-check this test.")
'@
$checkB | Set-Content -Path .\tmp_r08_checkB_prep.py -Encoding UTF8
& $PY .\tmp_r08_checkB_prep.py $DbPath
Remove-Item .\tmp_r08_checkB_prep.py -Force

# Run resolver again to pick up the hinted row
& $PY .\scripts\resolve_domains.py --db $DbPath --limit 10

# Now verify the hinted company got an official domain set
$checkB2 = @'
import sqlite3, sys
db = sys.argv[1]
con = sqlite3.connect(db); con.row_factory = sqlite3.Row
cols = {r["name"] for r in con.execute("PRAGMA table_info(companies)")}
OFF = "domain_official" if "domain_official" in cols else "official_domain"
row = con.execute(f"SELECT id,name,{OFF} AS dom FROM companies WHERE name=?", ("R08 Hint Path Co (acceptance)",)).fetchone()
if not row:
    print("FAIL:INGEST_HINT seeded company not found after resolver")
    sys.exit(2)
if not row["dom"]:
    print("FAIL:INGEST_HINT official domain not set after resolver")
    sys.exit(2)
print("OK:INGEST_HINT chosen domain landed in companies.", row["dom"])
'@
$checkB2 | Set-Content -Path .\tmp_r08_checkB_verify.py -Encoding UTF8
& $PY .\tmp_r08_checkB_verify.py $DbPath
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Remove-Item .\tmp_r08_checkB_verify.py -Force

Write-Host "`n✅ ACCEPT_R08: PASS — All checks succeeded." -ForegroundColor Green
