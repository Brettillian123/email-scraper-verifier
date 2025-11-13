import argparse
import importlib
import re
import sqlite3


def classify_heuristic(title: str):
    t = (title or "").lower()

    # seniority
    if re.search(r"\bchief\b|^c[eo]o\b|^c[a-z]{1,3}o\b", t) or re.search(
        r"\bceo|cto|cfo|cmo|cso|cio\b", t
    ):
        seniority = "C"
    elif "vice president" in t or re.search(r"\bvp\b", t):
        seniority = "VP"
    elif re.search(r"\bhead\b", t):
        seniority = "Head"
    elif "director" in t:
        seniority = "Director"
    elif "manager" in t:
        seniority = "Manager"
    else:
        seniority = "IC"

    # role family
    if "sales" in t:
        role_family = "Sales"
    elif "marketing" in t:
        role_family = "Marketing"
    elif "engineering" in t or "engineer" in t or "devops" in t:
        role_family = "Engineering"
    elif "customer success" in t or ("customer" in t and "success" in t):
        role_family = "Customer Success"
    elif "operations" in t or re.search(r"\bops\b", t):
        role_family = "Operations"
    else:
        role_family = None

    return role_family, seniority


def classify_with_module(title: str):
    """
    Try to use src.ingest.title_norm if available.
    Supports a few likely function shapes; falls back to heuristics.
    """
    try:
        m = importlib.import_module("src.ingest.title_norm")
    except Exception:
        return None, None

    candidates = (
        "classify",
        "infer",
        "normalize",
        "normalize_title",
        "classify_title",
        "infer_role_seniority",
    )

    for name in candidates:
        f = getattr(m, name, None)
        if callable(f):
            try:
                out = f(title)
                # tuple(shape) -> (role_family, seniority) or (seniority, role) – try both
                if isinstance(out, tuple) and len(out) >= 2:
                    a, b = out[0], out[1]
                    # If we got a known seniority token first, swap
                    if str(a).lower() in {"c", "vp", "head", "director", "manager", "ic"}:
                        return (b, a)
                    return (a, b)
                # dataclass / object with attributes
                role_family = getattr(out, "role_family", None)
                seniority = getattr(out, "seniority", None)
                if role_family or seniority:
                    return role_family, seniority
            except Exception:
                continue
    return None, None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/dev.db")
    args = ap.parse_args()

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    cur.execute("""
        SELECT id,
               COALESCE(NULLIF(title_norm, ''), NULLIF(title, '')) AS t,
               COALESCE(NULLIF(role_family, ''), NULL) AS rf,
               COALESCE(NULLIF(seniority, ''), NULL)   AS sn
        FROM people
    """)
    rows = cur.fetchall()

    updated = 0
    for pid, t, rf, sn in rows:
        if not t:
            continue

        new_rf, new_sn = rf, sn
        if not (rf and sn):
            mod_rf, mod_sn = classify_with_module(t)
            if not mod_rf and not mod_sn:
                mod_rf, mod_sn = classify_heuristic(t)

            new_rf = new_rf or mod_rf
            new_sn = new_sn or mod_sn

        if (new_rf != rf) or (new_sn != sn):
            cur.execute(
                "UPDATE people SET role_family = ?, seniority = ? WHERE id = ?",
                (new_rf, new_sn, pid),
            )
            updated += cur.rowcount

    con.commit()
    print(f"Backfill complete. Rows updated: {updated}")


if __name__ == "__main__":
    main()
