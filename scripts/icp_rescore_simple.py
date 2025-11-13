import datetime as dt
import json
import sqlite3
from collections.abc import Iterable
from pathlib import Path

try:
    import yaml
except Exception as e:
    raise SystemExit("PyYAML not installed; run: python -m pip install pyyaml") from e

CFG_PATH = Path("docs/icp-schema.yaml")
cfg = yaml.safe_load(CFG_PATH.read_text(encoding="utf-8")) or {}
cap: int = int(cfg.get("cap", 100))
weights = cfg.get("weights") or cfg.get("signals") or {}
W_RF = weights.get("role_family") or {}
W_SN = weights.get("seniority") or {}


def score_row(rf: str | None, sn: str | None) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []
    if rf and rf in W_RF:
        pts = int(W_RF[rf])
        score += pts
        reasons.append(f"role_family:{rf}+{pts}")
    if sn and sn in W_SN:
        pts = int(W_SN[sn])
        score += pts
        reasons.append(f"seniority:{sn}+{pts}")
    score = max(0, min(cap, score))
    if not reasons:
        return 0, ["missing_min_required"]
    return score, reasons


def main(db_path: str = "data/dev.db") -> None:
    if not Path(db_path).exists():
        raise SystemExit(f"Database not found: {db_path}")

    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute("SELECT id, role_family, seniority FROM people")
        rows: Iterable[tuple[int, str | None, str | None]] = cur.fetchall()

        now = dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

        updated = 0
        for pid, rf, sn in rows:
            s, reasons = score_row(rf, sn)
            cur.execute(
                "UPDATE people SET icp_score=?, icp_reasons=?, last_scored_at=? WHERE id=?",
                (s, json.dumps(reasons, ensure_ascii=False), now, pid),
            )
            updated += cur.rowcount

        con.commit()
        print(f"rescored {updated} rows using {CFG_PATH}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
