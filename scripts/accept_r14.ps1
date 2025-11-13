# scripts/accept_r14.ps1
param(
  [string]$DbPath = "data\dev.db"
)

$ErrorActionPreference = "Stop"

Write-Host "Using DB: $DbPath" -ForegroundColor Cyan
Write-Host "==> Applying schema & migrations" -ForegroundColor Yellow

# Prep current DB for tests (ALL migrations, incl. R14)
python scripts\apply_schema.py
python scripts\migrate_r13_add_normalization.py --db $DbPath
python scripts\migrate_o02_title_fields.py      --db $DbPath
python scripts\migrate_o03_company_attrs.py     --db $DbPath
python scripts\migrate_r14_add_icp.py           --db $DbPath

# Quick config sanity (prove YAML was loaded & shaped correctly)
Write-Host "==> ICP config summary" -ForegroundColor Yellow
$pyCfg = @"
from src.config import load_icp_config
cfg = load_icp_config() or {}
w = (cfg.get('weights') or cfg.get('signals') or {})
rf = w.get('role_family') or {}
sn = w.get('seniority') or {}
need = cfg.get('min_required') or []
print('version:', cfg.get('version'))
print('cap:', cfg.get('cap', 100))
print('min_required:', need)
print('weights: role_family:', len(rf), 'seniority:', len(sn))
"@
python -c $pyCfg

Write-Host "==> Running R14 unit tests" -ForegroundColor Yellow
pytest -k r14

Write-Host "==> Ingesting samples + scoring" -ForegroundColor Yellow
if (Test-Path $DbPath) {
  Write-Host "Removing existing DB at $DbPath ..."
  Remove-Item $DbPath -Force
}

# Fresh DB + ALL migrations required for R14
python scripts\apply_schema.py
python scripts\migrate_r13_add_normalization.py --db $DbPath
python scripts\migrate_o02_title_fields.py      --db $DbPath
python scripts\migrate_o03_company_attrs.py     --db $DbPath
python scripts\migrate_r14_add_icp.py           --db $DbPath

# Ingest
python scripts\ingest_csv.py samples\leads.csv

# --- Optional polish & reliability ---

# Ensure O02 role_family/seniority are populated after ingest
Write-Host "==> Backfilling O02 role_family/seniority" -ForegroundColor Yellow
if (Test-Path scripts\backfill_o02_roles.py) {
  python scripts\backfill_o02_roles.py --db $DbPath
} else {
  Write-Host "[warn] scripts\backfill_o02_roles.py not found; relying on ingest to populate roles." -ForegroundColor DarkYellow
}

# Quick sanity: show distinct role/seniority
$pyO02 = @"
import sqlite3
con = sqlite3.connect(r'$DbPath'); cur = con.cursor()
cur.execute('SELECT COUNT(*) FROM people WHERE COALESCE(role_family,"")<>"" AND COALESCE(seniority,"")<>""')
filled = cur.fetchone()[0]
cur.execute('SELECT COUNT(*) FROM people')
total = cur.fetchone()[0]
print(f'O02 populated: {filled}/{total} rows')
"@
python -c $pyO02

# Backfill ICP (R14) using migrator (scores & reasons)
Write-Host "==> Backfilling ICP (R14)" -ForegroundColor Yellow
python scripts\migrate_r14_add_icp.py --db $DbPath -v

# Pretty Top-10 with role/seniority + reasons
Write-Host "==> Top leads by icp_score" -ForegroundColor Yellow
$py = @"
import sqlite3, json
db = r'$DbPath'
con = sqlite3.connect(db)
cur = con.cursor()
cur.execute("""
SELECT
  p.id,
  COALESCE(NULLIF(p.full_name,''), TRIM(COALESCE(p.first_name,'')||' '||COALESCE(p.last_name,''))) AS name,
  COALESCE(p.title_norm, COALESCE(p.title,'')) AS title,
  COALESCE(p.role_family,'') AS rf,
  COALESCE(p.seniority,'') AS sr,
  COALESCE(p.icp_score,0) AS score,
  COALESCE(p.icp_reasons,'[]') AS reasons
FROM people p
ORDER BY score DESC, p.id
LIMIT 10
""")
rows = cur.fetchall()

def trunc(s, n):
    s = s if isinstance(s, str) else json.dumps(s, ensure_ascii=False)
    return (s[:n-1] + "…") if len(s) > n else s

print(f"{'#':>3} | {'Name':20} | {'Title':28} | {'Role':12} | {'Seniority':9} | {'Score':5} | Reasons")
print("-"*120)
for idx, (pid, name, title, rf, sr, score, reasons) in enumerate(rows, 1):
    print(f"{idx:3} | {name[:20]:20} | {title[:28]:28} | {rf[:12]:12} | {sr[:9]:9} | {score:5} | {trunc(reasons, 70)}")
"@
python -c $py
