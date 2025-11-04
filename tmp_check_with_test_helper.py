import importlib.util
from pathlib import Path
import src.ingest as I

# Load the test module from file so we can use its _rows_from_csv
spec = importlib.util.spec_from_file_location("tmod", "tests/test_ingest_csv_jsonl.py")
tmod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tmod)

rows = tmod._rows_from_csv(Path("tests/fixtures/leads_small.csv"))

a=r=0
print("USING:", I.__file__)
for i,row in enumerate(rows,1):
    ok, err = I.ingest_row(row)
    domain_raw = row.get("domain")
    domain_norm = I.normalize_domain(domain_raw)
    print(f"{i}: ok={ok} err={err} role={row.get('role')!r} company={row.get('company')!r} domain_raw={domain_raw!r} domain_norm={domain_norm!r}")
    a += 1 if ok else 0
    r += 0 if ok else 1
print("ACCEPTED/REJECTED:", a, r)
