# test/test_domain_resolver.py
import sqlite3

from src.queueing.tasks import resolve_company_domain


def test_resolver_writes_company_and_audit(tmp_path, monkeypatch):
    # Create an isolated DB and load schema
    db_path = tmp_path / "t.db"
    con = sqlite3.connect(str(db_path))
    with open("db/schema.sql", encoding="utf-8") as f:
        con.executescript(f.read())
    con.close()

    # CRITICAL: Patch _conn in the tasks module (where it's used)
    import src.queueing.tasks as tasks_mod

    def _test_conn():
        return sqlite3.connect(str(db_path))

    # Patch the _conn function that resolve_company_domain actually calls
    monkeypatch.setattr(tasks_mod, "_conn", _test_conn)

    # Also patch get_conn as backup
    import src.db as db_mod

    monkeypatch.setattr(db_mod, "get_conn", _test_conn)

    # And set env var
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    # Seed company row
    with sqlite3.connect(str(db_path)) as con:
        cur = con.execute(
            "INSERT INTO companies(name, domain) VALUES (?, ?)",
            ("Bücher GmbH", "buecher.de"),
        )
        company_id = cur.lastrowid

    # Fake the network
    import src.resolve.domain as mod

    monkeypatch.setattr(mod, "_dns_any", lambda h: h == "xn--bcher-kva.de")
    monkeypatch.setattr(mod, "_http_head_ok", lambda h: (h == "xn--bcher-kva.de", None))

    # Execute job
    res = resolve_company_domain(company_id, "Bücher GmbH", "bücher.de")
    assert res["chosen"] == "xn--bcher-kva.de"
    assert res["confidence"] >= 80

    # Verify results
    with sqlite3.connect(str(db_path)) as con:
        row = con.execute(
            "SELECT official_domain, official_domain_confidence FROM companies WHERE id = ?",
            (company_id,),
        ).fetchone()
        assert row is not None
        assert row[0] == "xn--bcher-kva.de"
        assert row[1] >= 80

        audit = con.execute(
            "SELECT chosen_domain, method, confidence, resolver_version "
            "FROM domain_resolutions WHERE company_id = ?",
            (company_id,),
        ).fetchall()
        assert len(audit) == 1
        chosen_domain, method, confidence, resolver_version = audit[0]
        assert chosen_domain == "xn--bcher-kva.de"
        assert confidence >= 80
        assert isinstance(method, str) and method
        assert isinstance(resolver_version, str) and resolver_version
