# tests/test_candidate_ranking.py
"""Evidence-ranked probe planning (src/verify/candidates.py).

Mostly pure functions. The evidence rows are shaped exactly as
src.generate.patterns.get_domain_patterns() returns them; the last section
drives the two DB readers with a fake psycopg connection.
"""

from __future__ import annotations

import pytest

from src.generate.patterns import pattern_confidence
from src.verify.candidates import (
    DEFAULT_MAX_PROBES_PER_PERSON,
    GRADE_HIGH,
    GRADE_LOW,
    GRADE_MEDIUM,
    LARGE_COMPANY_ORDER,
    SMALL_COMPANY_ORDER,
    base_rate_order,
    catchall_confidence,
    company_employee_count,
    company_pattern_evidence,
    has_trusted_pattern,
    max_probes_per_person,
    rank_candidates,
    rank_patterns,
)


def row(pattern: str, confirmed: int, failed: int = 0) -> dict:
    confidence, status = pattern_confidence(confirmed, failed)
    return {
        "pattern": pattern,
        "domain": "acme.com",
        "confirmed_count": confirmed,
        "failed_count": failed,
        "confidence": confidence,
        "status": status,
        "renderable": True,
    }


def ranked(*rows: dict) -> list[dict]:
    """Mirror get_domain_patterns() ordering: confirmed DESC, failed ASC, name ASC."""
    return sorted(rows, key=lambda r: (-r["confirmed_count"], r["failed_count"], r["pattern"]))


# --------------------------------------------------------------------------
# Base rate
# --------------------------------------------------------------------------


def test_unknown_size_defaults_to_small_company_order():
    assert base_rate_order() == SMALL_COMPANY_ORDER
    assert base_rate_order(14) == SMALL_COMPANY_ORDER
    assert rank_patterns([]) == list(SMALL_COMPANY_ORDER)


def test_large_company_reorders_toward_first_last():
    order = base_rate_order(25_000)
    assert order == LARGE_COMPANY_ORDER
    assert order[0] == "first.last"
    assert order.index("first") > order.index("first.last")


def test_first_last_is_never_generated():
    for evidence in ([], ranked(row("first_last", 5)), ranked(row("first_last", 1))):
        assert "first_last" not in rank_patterns(evidence)
    emails = rank_candidates(ranked(row("first_last", 5)), "Ada", "Lovelace", "acme.com")
    assert all("_" not in email.split("@")[0] for email, _ in emails)


# --------------------------------------------------------------------------
# Evidence ranking
# --------------------------------------------------------------------------


def test_two_trusted_patterns_come_first_in_confirmed_order():
    evidence = ranked(row("first.last", 3), row("flast", 7, 1), row("first", 1))
    patterns = rank_patterns(evidence)
    assert patterns[:3] == ["flast", "first.last", "first"]
    # ...and the base rate still supplies the tail.
    assert patterns[3:] == ["firstlast", "f.last", "firstl"]


def test_candidate_pattern_ranks_below_trusted_but_above_base_rate():
    # "f.last" is 5th in the small-company base rate; one confirmation lifts it.
    evidence = ranked(row("firstlast", 4), row("f.last", 1))
    assert rank_patterns(evidence)[:2] == ["firstlast", "f.last"]


def test_refuted_pattern_is_skipped_entirely():
    evidence = ranked(row("flast", 0, 4), row("first.last", 2))
    patterns = rank_patterns(evidence)
    assert "flast" not in patterns
    assert patterns[0] == "first.last"
    assert [e for e, _ in rank_candidates(evidence, "Ada", "Lovelace", "acme.com")][0] == (
        "ada.lovelace@acme.com"
    )


def test_operator_override_outranks_base_rate_but_not_trusted():
    assert rank_patterns([], override="firstl")[0] == "firstl"
    evidence = ranked(row("first.last", 3))
    assert rank_patterns(evidence, override="firstl")[:2] == ["first.last", "firstl"]


def test_override_cannot_resurrect_a_refuted_or_blocked_pattern():
    evidence = ranked(row("flast", 0, 2))
    assert "flast" not in rank_patterns(evidence, override="flast")
    assert "first_last" not in rank_patterns([], override="first_last")


def test_has_trusted_pattern():
    assert has_trusted_pattern(ranked(row("flast", 2))) is True
    assert has_trusted_pattern(ranked(row("flast", 1))) is False
    assert has_trusted_pattern([]) is False


# --------------------------------------------------------------------------
# Candidate rendering
# --------------------------------------------------------------------------


def test_candidates_are_ordered_addresses_with_their_pattern():
    assert rank_candidates([], "Ada", "Lovelace", "acme.com", limit=3) == [
        ("alovelace@acme.com", "flast"),
        ("ada.lovelace@acme.com", "first.last"),
        ("ada@acme.com", "first"),
    ]


def test_limit_caps_the_probe_budget():
    assert rank_candidates([], "Ada", "Lovelace", "acme.com", limit=1) == [
        ("alovelace@acme.com", "flast")
    ]
    assert rank_candidates([], "Ada", "Lovelace", "acme.com", limit=0) == []


def test_compound_names_spend_the_budget_on_formats_not_spellings():
    candidates = rank_candidates([], "José", "García-López", "acme.com", limit=3)
    assert [p for _, p in candidates] == ["flast", "first.last", "first"]
    # The single-token spelling exists, but only past the canonical renderings.
    everything = [e for e, _ in rank_candidates([], "José", "García-López", "acme.com")]
    assert "jgarcialopez@acme.com" in everything
    assert "jlopez@acme.com" in everything
    assert everything.index("jgarcialopez@acme.com") < everything.index("jlopez@acme.com")


def test_role_addresses_are_never_generated():
    # "first" would render sales@; it must be dropped, not offered.
    emails = [e for e, _ in rank_candidates([], "Sales", "Smith", "acme.com")]
    assert "sales@acme.com" not in emails
    assert "ssmith@acme.com" in emails


def test_missing_name_part_drops_only_the_patterns_that_need_it():
    candidates = rank_candidates([], "Ada", "", "acme.com")
    assert candidates == [("ada@acme.com", "first")]


# --------------------------------------------------------------------------
# Catch-all grading
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "confirmed,failed,expected",
    [(7, 1, GRADE_HIGH), (2, 0, GRADE_HIGH), (1, 0, GRADE_MEDIUM), (0, 3, GRADE_LOW)],
)
def test_catchall_grade_follows_evidence_status(confirmed, failed, expected):
    evidence = ranked(row("flast", confirmed, failed))
    confidence, grade = catchall_confidence(evidence, "flast")
    assert grade == expected
    assert confidence == pattern_confidence(confirmed, failed)[0]


def test_catchall_grade_is_low_without_company_evidence():
    assert catchall_confidence([], "flast") == (0.5, GRADE_LOW)
    assert catchall_confidence(ranked(row("first", 9)), "flast") == (0.5, GRADE_LOW)


# --------------------------------------------------------------------------
# Probe budget
# --------------------------------------------------------------------------


def test_probe_budget_defaults_to_three(monkeypatch):
    monkeypatch.delenv("MAX_PROBES_PER_PERSON", raising=False)
    assert max_probes_per_person() == DEFAULT_MAX_PROBES_PER_PERSON == 3


def test_probe_budget_reads_env_and_survives_garbage(monkeypatch):
    monkeypatch.setenv("MAX_PROBES_PER_PERSON", "5")
    assert max_probes_per_person() == 5
    monkeypatch.setenv("MAX_PROBES_PER_PERSON", "-2")
    assert max_probes_per_person() == 0
    monkeypatch.setenv("MAX_PROBES_PER_PERSON", "not-a-number")
    assert max_probes_per_person() == DEFAULT_MAX_PROBES_PER_PERSON


# --------------------------------------------------------------------------
# Transaction hygiene: a swallowed DB error must not poison the connection
#
# On PostgreSQL any failed statement aborts the WHOLE transaction -- every
# later statement raises InFailedSqlTransaction until rollback(). The pattern
# tables are not migrated in production, so the evidence SELECT fails on every
# company; without a rollback the probe verdicts written afterwards on the same
# connection are silently discarded.
# --------------------------------------------------------------------------


class InFailedSqlTransaction(Exception):
    """psycopg raises this for every statement after an aborted one."""


class UndefinedTable(Exception):
    """psycopg raises this for an unmigrated table."""


class FakePgCursor:
    def __init__(self, conn, rows):
        self._conn = conn
        self._rows = rows

    def execute(self, sql, params=None):
        self._rows = self._conn._run(sql)
        return self

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self):
        pass


class FakePgConn:
    """psycopg-shaped connection that reproduces Postgres abort semantics."""

    def __init__(self, failing_table: str):
        self.failing_table = failing_table
        self.aborted = False
        self.applied: list[str] = []

    def _run(self, sql: str):
        if self.aborted:
            raise InFailedSqlTransaction(
                "current transaction is aborted, "
                "commands ignored until end of transaction block"
            )
        if self.failing_table in sql:
            self.aborted = True
            raise UndefinedTable(f'relation "{self.failing_table}" does not exist')
        self.applied.append(sql.strip().split()[0].upper())
        return [("row",)]

    def cursor(self):
        return FakePgCursor(self, [])

    def execute(self, sql, params=None):
        return FakePgCursor(self, self._run(sql))

    def rollback(self):
        self.aborted = False


WRITE_VERDICT = "INSERT INTO verification_results (email_id, verify_status) VALUES (1, 'valid')"


def test_missing_pattern_table_does_not_discard_later_writes():
    con = FakePgConn("company_email_pattern")
    assert company_pattern_evidence(con, 7) == []  # degrades to base-rate order
    assert con.aborted is False  # rolled back
    con.execute(WRITE_VERDICT)  # the probe verdict still lands
    assert con.applied == ["INSERT"]


def test_without_rollback_the_next_write_is_lost(monkeypatch):
    """Same run with the fix disabled: proves the rollback is what saves it."""
    monkeypatch.setattr("src.verify.candidates._rollback", lambda conn: None)
    con = FakePgConn("company_email_pattern")
    assert company_pattern_evidence(con, 7) == []  # caller sees no error at all
    assert con.aborted is True
    with pytest.raises(InFailedSqlTransaction):
        con.execute(WRITE_VERDICT)
    assert con.applied == []  # verdict silently discarded


def test_unreadable_companies_row_does_not_discard_later_writes():
    con = FakePgConn("FROM companies")
    assert company_employee_count(con, 7) is None
    assert con.aborted is False
    con.execute(WRITE_VERDICT)
    assert con.applied == ["INSERT"]
