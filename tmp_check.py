import csv
from pathlib import Path
import src.ingest as I

print("USING:", I.__file__)
csv_path = Path("tests/fixtures/leads_small.csv")

def check_rows(rows):
    a=r=0
    for i,row in enumerate(rows,1):
        ok, err = I.ingest_row(row)
        # show domain_raw vs normalized to verify the invalid-domain check
        domain_raw = row.get("domain")
        domain_norm = I.normalize_domain(domain_raw)
        print(f"{i}: ok={ok} err={err} domain_raw={domain_raw!r} domain_norm={domain_norm!r} role={row.get('role')!r} company={row.get('company')!r}")
        a += 1 if ok else 0
        r += 0 if ok else 1
    print("ACCEPTED/REJECTED:", a, r)

with csv_path.open(encoding="utf-8") as f:
    rows = list(csv.DictReader(f))
check_rows(rows)
