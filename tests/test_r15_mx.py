# tests/test_r15_mx.py
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from src.resolve.mx import norm_domain, resolve_mx

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_path(tmp_path: Path) -> str:
    """
    Create a temporary SQLite DB with the R15 domain_resolutions table.
    We do not depend on the migration script/package layout in tests.
    """
    dbfile = tmp_path / "r15_test.db"
    con = sqlite3.connect(str(dbfile))
    try:
        con.executescript(
            """
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS domain_resolutions (
                id INTEGER PRIMARY KEY,
                company_id INTEGER,
                domain TEXT,
                mx_hosts TEXT,
                preference_map TEXT,
                lowest_mx TEXT,
                resolved_at TEXT,
                ttl INTEGER DEFAULT 86400,
                failure TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_domain_resolutions_company_id
                ON domain_resolutions(company_id);
            CREATE INDEX IF NOT EXISTS idx_domain_resolutions_domain
                ON domain_resolutions(domain);
            """
        )
        con.commit()
    finally:
        con.close()
    return str(dbfile)


def _set_row_resolved_at(db_path: str, company_id: int, domain: str, iso_ts: str) -> None:
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            "UPDATE domain_resolutions SET resolved_at=? WHERE company_id=? AND domain=?",
            (iso_ts, int(company_id), domain),
        )
        con.commit()
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Helpers to patch DNS/A fallback paths inside src.resolve.mx
# ---------------------------------------------------------------------------


def _patch_mx_pairs(monkeypatch, pairs: list[tuple[int, str]]) -> dict:
    """
    Patch the dnspython path inside src.resolve.mx to return `pairs`
    (list of (preference, host)). Returns a counter dict to observe calls.
    """
    import src.resolve.mx as mxmod  # local import to ensure patching correct module

    calls = {"count": 0}

    def fake_lookup_with_dnspython(domain: str):
        calls["count"] += 1
        return list(pairs)

    monkeypatch.setattr(mxmod, "_DNSPY_AVAILABLE", True)
    monkeypatch.setattr(mxmod, "_mx_lookup_with_dnspython", fake_lookup_with_dnspython)
    # Ensure A/AAAA fallback is never taken in these tests unless we patch it
    monkeypatch.setattr(mxmod, "_a_or_aaaa_exists", lambda d: False)
    return calls


def _patch_mx_raises(monkeypatch, exc: Exception | None = None, a_exists: bool = False):
    """
    Force the MX path to raise (NoAnswer/NXDOMAIN-like) and control A/AAAA fallback.
    """
    import src.resolve.mx as mxmod

    def raiser(_domain: str):
        raise exc or Exception("forced_mx_failure")

    monkeypatch.setattr(mxmod, "_DNSPY_AVAILABLE", True)
    monkeypatch.setattr(mxmod, "_mx_lookup_with_dnspython", raiser)
    monkeypatch.setattr(mxmod, "_a_or_aaaa_exists", lambda d: bool(a_exists))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_mx_success_sorted_preference(db_path: str, monkeypatch):
    """
    MX lookup success: ensure sorting by preference asc (then by hostname),
    preference_map correctness, and lowest_mx selection.
    """
    # Unsorted input; same pref tie to test secondary sort by host
    pairs = [
        (20, "mx20b.domain.test"),
        (10, "mx10.domain.test"),
        (20, "mx20a.domain.test"),
        (5, "mx05.domain.test"),
    ]
    counter = _patch_mx_pairs(monkeypatch, pairs)

    res = resolve_mx(company_id=1, domain="Example.COM", db_path=db_path)
    assert res.failure is None
    assert res.lowest_mx == "mx05.domain.test"
    assert res.mx_hosts == [
        "mx05.domain.test",
        "mx10.domain.test",
        "mx20a.domain.test",
        "mx20b.domain.test",
    ]
    assert res.preference_map == {
        "mx05.domain.test": 5,
        "mx10.domain.test": 10,
        "mx20a.domain.test": 20,
        "mx20b.domain.test": 20,
    }
    # One DNS call made
    assert counter["count"] == 1

    # JSON-serializable fields
    json.dumps(res.mx_hosts)
    json.dumps(res.preference_map)


def test_cache_reuse_within_ttl(db_path: str, monkeypatch):
    """
    Calling resolve_mx twice should reuse the cached row (cached=True on second call)
    and not perform a second DNS query within TTL.
    """
    pairs = [(1, "mx1.test"), (10, "mx10.test")]
    counter = _patch_mx_pairs(monkeypatch, pairs)

    res1 = resolve_mx(company_id=2, domain="cache.test", db_path=db_path)
    assert res1.cached is False
    assert counter["count"] == 1

    res2 = resolve_mx(company_id=2, domain="cache.test", db_path=db_path)
    assert res2.cached is True
    # No additional DNS call
    assert counter["count"] == 1
    # Cached values identical
    assert res2.lowest_mx == "mx1.test"
    assert res2.preference_map["mx1.test"] == 1


def test_cache_invalidation_after_ttl_expiry(db_path: str, monkeypatch):
    """
    If cache is older than TTL, resolver must refresh and mark cached=False.
    """
    pairs = [(5, "mx5.old"), (0, "mx0.new")]
    counter = _patch_mx_pairs(monkeypatch, pairs)

    # First write with short TTL
    res1 = resolve_mx(company_id=3, domain="ttl.example", db_path=db_path, ttl_seconds=1)
    assert res1.cached is False
    assert counter["count"] == 1

    # Age the row far in the past to force refresh regardless of 1s TTL
    canon = norm_domain("ttl.example")
    _set_row_resolved_at(db_path, 3, canon, "1970-01-01T00:00:00Z")

    # Second call should refresh (another DNS call)
    res2 = resolve_mx(company_id=3, domain="ttl.example", db_path=db_path, ttl_seconds=1)
    assert res2.cached is False
    assert counter["count"] == 2
    assert res2.lowest_mx == "mx0.new"


def test_failure_mode_no_a_fallback(db_path: str, monkeypatch):
    """
    When MX lookup fails and there is no A/AAAA fallback, we return empty hosts,
    lowest_mx=None, and a non-empty failure string.
    """
    _patch_mx_raises(monkeypatch, exc=Exception("NXDOMAIN"), a_exists=False)

    res = resolve_mx(company_id=4, domain="no-such-tld.invalid", db_path=db_path)
    assert res.mx_hosts == []
    assert res.lowest_mx is None
    assert isinstance(res.failure, str) and res.failure  # non-empty
    assert res.cached is False


def test_a_record_fallback_when_mx_missing(db_path: str, monkeypatch):
    """
    On MX failure but A/AAAA presence, treat domain itself as MX with pref=0.
    """
    _patch_mx_raises(monkeypatch, exc=Exception("NoAnswer"), a_exists=True)

    dom = "fallback.test"
    canon = norm_domain(dom)
    res = resolve_mx(company_id=5, domain=dom, db_path=db_path)
    assert res.failure is None
    assert res.mx_hosts == [canon]
    assert res.preference_map == {canon: 0}
    assert res.lowest_mx == canon


def test_json_fields_well_formed_types(db_path: str, monkeypatch):
    """
    Ensure types are JSON-friendly and values are integers in preference_map.
    """
    pairs = [(1, "mx.a"), (2, "mx.b")]
    _patch_mx_pairs(monkeypatch, pairs)

    res = resolve_mx(company_id=6, domain="types.test", db_path=db_path)
    assert isinstance(res.mx_hosts, list)
    assert isinstance(res.preference_map, dict)
    # integer values
    assert all(isinstance(v, int) for v in res.preference_map.values())
    # JSON dumps must succeed
    json.dumps(res.mx_hosts)
    json.dumps(res.preference_map)


def test_idn_domains_no_crash_and_punycode(db_path: str, monkeypatch):
    """
    Unicode/IDN domains should normalize to IDNA ASCII and work with A-fallback.
    """
    # Force MX path to fail and A/AAAA to succeed
    _patch_mx_raises(monkeypatch, exc=Exception("NoNameservers"), a_exists=True)

    unicode_dom = "пример.рф"  # example in Russian
    canon = norm_domain(unicode_dom)
    assert canon.startswith("xn--")  # confirm punycode

    res = resolve_mx(company_id=7, domain=unicode_dom, db_path=db_path)
    assert res.failure is None
    assert res.mx_hosts == [canon]
    assert res.preference_map == {canon: 0}
    assert res.lowest_mx == canon


def test_deterministic_tie_break_on_hostname(db_path: str, monkeypatch):
    """
    When MX preferences are equal, ordering must be by hostname (lexicographic),
    and lowest_mx must be the lexicographically smallest.
    """
    pairs = [
        (10, "mx-b.test"),
        (10, "mx-a.test"),
        (10, "mx-c.test"),
    ]
    _patch_mx_pairs(monkeypatch, pairs)

    res = resolve_mx(company_id=8, domain="tie.test", db_path=db_path)
    assert res.mx_hosts == ["mx-a.test", "mx-b.test", "mx-c.test"]
    assert res.lowest_mx == "mx-a.test"


def test_failure_rows_do_not_cache(db_path: str, monkeypatch):
    """
    A prior row with failure should not be treated as valid cache; next call should refresh.
    """
    # First, create a failure row manually
    con = sqlite3.connect(db_path)
    canon = norm_domain("will-refresh.test")
    try:
        con.execute(
            """
            INSERT INTO domain_resolutions (company_id, domain, mx_hosts, preference_map,
                                            lowest_mx, resolved_at, ttl, failure)
            VALUES (?, ?, '[]', '{}', NULL, '1970-01-01T00:00:00Z', 86400, 'timeout')
            """,
            (9, canon),
        )
        con.commit()
    finally:
        con.close()

    # Now a successful MX lookup should occur (not cached), and row should be updated.
    pairs = [(1, "ok.mx")]
    counter = _patch_mx_pairs(monkeypatch, pairs)

    res = resolve_mx(company_id=9, domain="will-refresh.test", db_path=db_path)
    assert res.cached is False
    assert counter["count"] == 1
    assert res.lowest_mx == "ok.mx"
