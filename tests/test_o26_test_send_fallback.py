from __future__ import annotations

import inspect
import sqlite3
from typing import Any

from src.verify.test_send import choose_next_test_send_candidate


def _make_test_db() -> sqlite3.Connection:
    """
    Create a minimal in-memory DB with just the tables/columns needed for
    choose_next_test_send_candidate().

    Note:
      - Many project DB call sites assume row access by name (sqlite3.Row).
      - Recent multi-tenant changes often require tenant_id columns.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.executescript(
        """
        CREATE TABLE people (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id   TEXT NOT NULL DEFAULT 'dev',
            first_name  TEXT,
            last_name   TEXT
        );

        CREATE TABLE emails (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id  TEXT NOT NULL DEFAULT 'dev',
            person_id  INTEGER,
            email      TEXT
        );

        CREATE TABLE verification_results (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id        TEXT NOT NULL DEFAULT 'dev',
            email_id         INTEGER NOT NULL,
            verify_status    TEXT,
            verify_reason    TEXT,
            test_send_status TEXT,
            test_send_token  TEXT,
            test_send_at     TEXT,
            bounce_code      TEXT,
            bounce_reason    TEXT
        );
        """
    )
    return conn


def _seed_person_with_permutations(
    conn: sqlite3.Connection, tenant_id: str = "dev"
) -> dict[str, int]:
    """
    Insert a single person "Brett Anderson" with three permutations for the
    same domain, all starting as risky_catch_all + not_requested.

    Returns a mapping {email: email_id}.
    """
    cur = conn.execute(
        "INSERT INTO people (tenant_id, first_name, last_name) VALUES (?, ?, ?)",
        (tenant_id, "Brett", "Anderson"),
    )
    person_id = int(cur.lastrowid)

    emails = [
        "banderson@crestwellpartners.com",  # flast
        "brett.anderson@crestwellpartners.com",  # first.last
        "brett@crestwellpartners.com",  # first
    ]

    email_ids: dict[str, int] = {}
    for em in emails:
        cur = conn.execute(
            "INSERT INTO emails (tenant_id, person_id, email) VALUES (?, ?, ?)",
            (tenant_id, person_id, em),
        )
        email_id = int(cur.lastrowid)
        email_ids[em] = email_id

        conn.execute(
            """
            INSERT INTO verification_results (
                tenant_id,
                email_id,
                verify_status,
                verify_reason,
                test_send_status,
                test_send_token,
                test_send_at,
                bounce_code,
                bounce_reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tenant_id,
                email_id,
                "risky_catch_all",
                "rcpt_2xx_unknown_catchall",
                "not_requested",
                None,
                None,
                None,
                None,
            ),
        )

    conn.commit()
    return email_ids


def _choose(conn: sqlite3.Connection, *, email_id: int, tenant_id: str = "dev") -> Any:
    """
    Call choose_next_test_send_candidate with or without tenant_id depending
    on the function signature (supports refactors without breaking tests).
    """
    params = inspect.signature(choose_next_test_send_candidate).parameters
    if "tenant_id" in params:
        return choose_next_test_send_candidate(conn, tenant_id=tenant_id, email_id=email_id)
    return choose_next_test_send_candidate(conn, email_id=email_id)


def test_choose_next_test_send_candidate_respects_pattern_priority() -> None:
    """
    Initial selection should pick the highest-priority pattern for the person.

    Observed current expected priority:

        first.last ("brett.anderson") > first ("brett") > flast ("banderson")
    """
    conn = _make_test_db()
    try:
        email_ids = _seed_person_with_permutations(conn, tenant_id="dev")

        # We can pass any email_id for this person; the helper uses it only
        # to resolve (person, domain) and then ranks all candidates.
        some_email_id = next(iter(email_ids.values()))

        cand = _choose(conn, email_id=some_email_id, tenant_id="dev")
        assert cand is not None
        assert cand.email == "brett.anderson@crestwellpartners.com"

        # Pattern inference should also recognize 'first.last' if present
        # (tolerate None if the implementation returns no pattern field).
        pattern = getattr(cand, "pattern", None)
        assert pattern in {"first.last", "first_last", None}
    finally:
        conn.close()


def test_choose_next_test_send_candidate_skips_bounced_and_walks_down_order() -> None:
    """
    After a hard bounce has been applied (test_send_status != not_requested),
    the helper should skip that email and move to the next-best permutation.
    """
    conn = _make_test_db()
    try:
        email_ids = _seed_person_with_permutations(conn, tenant_id="dev")

        eid_flast = email_ids["banderson@crestwellpartners.com"]
        eid_first_last = email_ids["brett.anderson@crestwellpartners.com"]
        eid_first = email_ids["brett@crestwellpartners.com"]

        # Simulate that the top-priority candidate (first.last) already bounced.
        conn.execute(
            """
            UPDATE verification_results
            SET test_send_status = 'bounce_hard'
            WHERE email_id = ?
            """,
            (eid_first_last,),
        )
        conn.commit()

        # Now the next candidate should be first.
        cand2 = _choose(conn, email_id=eid_first_last, tenant_id="dev")
        assert cand2 is not None
        assert cand2.email == "brett@crestwellpartners.com"

        # Simulate a bounce for the second candidate as well.
        conn.execute(
            """
            UPDATE verification_results
            SET test_send_status = 'bounce_hard'
            WHERE email_id = ?
            """,
            (eid_first,),
        )
        conn.commit()

        # Now only the 'flast' candidate should remain.
        cand3 = _choose(conn, email_id=eid_first, tenant_id="dev")
        assert cand3 is not None
        assert cand3.email == "banderson@crestwellpartners.com"

        # If we mark all as bounced, there should be nothing left to try.
        conn.execute(
            """
            UPDATE verification_results
            SET test_send_status = 'bounce_hard'
            WHERE email_id = ?
            """,
            (eid_flast,),
        )
        conn.commit()

        cand4 = _choose(conn, email_id=eid_flast, tenant_id="dev")
        assert cand4 is None
    finally:
        conn.close()
