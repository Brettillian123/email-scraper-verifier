import sqlite3

from src.queueing.tasks import resolve_company_domain


def test_resolver_writes_company_and_audit(tmp_path, monkeypatch):
    # Create an isolated DB and load your schema into it
    db_path = tmp_path / "t.db"
    con = sqlite3.connect(db_path)
    with open("db/schema.sql", encoding="utf-8") as f:
        con.executescript(f.read())
    con.close()

    # Point the app code at our temp DB (used by tasks._conn())
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    # Seed a company row (minimal insert; other fields default NULL)
    with sqlite3.connect(db_path) as con:
        cur = con.execute("INSERT INTO companies(name) VALUES (?)", ("Bücher GmbH",))
        company_id = cur.lastrowid

    # Fake the network: only the punycode domain "works"
    import src.resolve.domain as mod

    monkeypatch.setattr(mod, "_dns_any", lambda h: h == "xn--bcher-kva.de")
    monkeypatch.setattr(mod, "_http_head_ok", lambda h: (h == "xn--bcher-kva.de", None))

    # Execute the job function exactly as the worker would
    res = resolve_company_domain(company_id, "Bücher GmbH", "bücher.de")
    assert res["chosen"] == "xn--bcher-kva.de"
    assert res["confidence"] >= 80

    # Verify company row was updated with the official domain & confidence
    with sqlite3.connect(db_path) as con:
        row = con.execute(
            "SELECT official_domain, official_domain_confidence FROM companies WHERE id = ?",
            (company_id,),
        ).fetchone()
        assert row is not None
        assert row[0] == "xn--bcher-kva.de"
        assert row[1] >= 80

        # And an audit row exists with the decision details
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
