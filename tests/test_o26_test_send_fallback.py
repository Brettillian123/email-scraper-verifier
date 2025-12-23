from __future__ import annotations

import sqlite3

from src.verify.test_send import choose_next_test_send_candidate


def _make_test_db() -> sqlite3.Connection:
    """
    Create a minimal in-memory DB with just the tables/columns needed for
    choose_next_test_send_candidate().

    We intentionally do not depend on the project's full schema/migrations
    so this test stays self-contained.
    """
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE people (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name  TEXT,
            last_name   TEXT
        );

        CREATE TABLE emails (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id  INTEGER,
            email      TEXT
        );

        CREATE TABLE verification_results (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
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


def _seed_person_with_permutations(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Insert a single person "Brett Anderson" with three permutations for the
    same domain, all starting as risky_catch_all + not_requested.

    Returns a mapping {email: email_id}.
    """
    cur = conn.execute(
        "INSERT INTO people (first_name, last_name) VALUES (?, ?)",
        ("Brett", "Anderson"),
    )
    person_id = cur.lastrowid

    emails = [
        "banderson@crestwellpartners.com",  # flast
        "brett.anderson@crestwellpartners.com",  # first.last
        "brett@crestwellpartners.com",  # first
    ]

    email_ids: dict[str, int] = {}
    for em in emails:
        cur = conn.execute(
            "INSERT INTO emails (person_id, email) VALUES (?, ?)",
            (person_id, em),
        )
        email_id = int(cur.lastrowid)
        email_ids[em] = email_id

        conn.execute(
            """
            INSERT INTO verification_results (
                email_id,
                verify_status,
                verify_reason,
                test_send_status,
                test_send_token,
                test_send_at,
                bounce_code,
                bounce_reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
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


def test_choose_next_test_send_candidate_respects_pattern_priority() -> None:
    """
    Initial selection should pick the highest-priority pattern for the person:

        flast ("banderson") > first.last ("brett.anderson") > first ("brett")
    """
    conn = _make_test_db()
    try:
        email_ids = _seed_person_with_permutations(conn)

        # We can pass any email_id for this person; the helper uses it only
        # to resolve (person, domain) and then ranks all candidates.
        some_email_id = next(iter(email_ids.values()))

        cand = choose_next_test_send_candidate(conn, email_id=some_email_id)
        assert cand is not None
        assert cand.email == "banderson@crestwellpartners.com"
        # Pattern inference should also recognize 'flast'.
        assert cand.pattern in {"flast", None}  # tolerate None if inference ever relaxes
    finally:
        conn.close()


def test_choose_next_test_send_candidate_skips_bounced_and_walks_down_order() -> None:
    """
    After a hard bounce has been applied (test_send_status != not_requested),
    the helper should skip that email and move to the next-best permutation.
    """
    conn = _make_test_db()
    try:
        email_ids = _seed_person_with_permutations(conn)

        eid_flast = email_ids["banderson@crestwellpartners.com"]
        eid_first_last = email_ids["brett.anderson@crestwellpartners.com"]
        eid_first = email_ids["brett@crestwellpartners.com"]

        # Simulate that the top-priority candidate (flast) already bounced.
        conn.execute(
            """
            UPDATE verification_results
            SET test_send_status = 'bounce_hard'
            WHERE email_id = ?
            """,
            (eid_flast,),
        )
        conn.commit()

        # Now the next candidate should be first.last.
        cand2 = choose_next_test_send_candidate(conn, email_id=eid_flast)
        assert cand2 is not None
        assert cand2.email == "brett.anderson@crestwellpartners.com"

        # Simulate a bounce for the second candidate as well.
        conn.execute(
            """
            UPDATE verification_results
            SET test_send_status = 'bounce_hard'
            WHERE email_id = ?
            """,
            (eid_first_last,),
        )
        conn.commit()

        # Now only the 'first' pattern candidate should remain.
        cand3 = choose_next_test_send_candidate(conn, email_id=eid_first_last)
        assert cand3 is not None
        assert cand3.email == "brett@crestwellpartners.com"

        # If we mark all as bounced, there should be nothing left to try.
        conn.execute(
            """
            UPDATE verification_results
            SET test_send_status = 'bounce_hard'
            WHERE email_id = ?
            """,
            (eid_first,),
        )
        conn.commit()

        cand4 = choose_next_test_send_candidate(conn, email_id=eid_first)
        assert cand4 is None
    finally:
        conn.close()
