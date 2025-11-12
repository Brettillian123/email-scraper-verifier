# tests/test_ingest_csv_jsonl.py
import csv
import importlib
import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest


def _apply_schema(db_path: Path) -> None:
    schema_sql = Path("db/schema.sql").read_text(encoding="utf-8")
    with sqlite3.connect(db_path) as conn:
        conn.executescript(schema_sql)
        conn.commit()


@pytest.fixture()
def temp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "test.db"
    db_path.touch()
    _apply_schema(db_path)
    # Ensure a valid sqlite URL on Windows (use forward slashes)
    db_url = "sqlite:///" + db_path.as_posix()
    monkeypatch.setenv("DATABASE_URL", db_url)
    return db_path


@pytest.fixture()
def enqueue_spy(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, Any]]]:
    """
    Capture job intents.

    Supports both call styles:
      - enqueue(job_name, payload_dict)
      - enqueue(job_name, **payload_fields)

    Patches common targets used by ingest layers:
      - src.ingest.enqueue
      - src.queue.enqueue
      - src.jobs.enqueue
    """
    calls: list[tuple[str, dict[str, Any]]] = []

    def fake_enqueue(job_name: str, payload: dict[str, Any] | None = None, **kwargs: Any) -> None:
        if payload is None:
            payload = kwargs
        calls.append((job_name, payload))

    for target in ("src.ingest", "src.queue", "src.jobs"):
        try:
            mod = importlib.import_module(target)
            if hasattr(mod, "enqueue"):
                monkeypatch.setattr(mod, "enqueue", fake_enqueue, raising=True)
        except Exception:
            # Module might not exist in this project; that's fine.
            pass

    return calls


def _rows_from_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _rows_from_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _count_people(conn: sqlite3.Connection) -> int:
    """
    Return a persisted row count for this test.

    Preference order:
      1) people  (primary app table)
      2) emails  (fallback if people not used directly)
      3) ingest_items (staging used by R07 ingest)
    """
    for table in ("people", "emails", "ingest_items"):
        try:
            return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # type: ignore[index]
        except sqlite3.OperationalError:
            continue
    return 0


def _ingest_rows(rows: list[dict[str, Any]]) -> tuple[int, int]:
    """Call src.ingest.ingest_row on each row and return (accepted, rejected).
    ingest_row returns True (accepted) or False (rejected) and should not raise for normal rejects.
    """
    # TEMP: drop this at the top of whatever function does the gating (often _ingest_rows)
    import pprint

    from src import ingest as I  # import here to honor DATABASE_URL monkeypatch

    print("FIRST ROW:", pprint.pformat(rows[0] if rows else None))

    accepted = rejected = 0
    for _i, r in enumerate(rows):
        ok = I.ingest_row(r)
        if ok:
            accepted += 1
        else:
            rejected += 1
    return accepted, rejected


def test_csv_and_jsonl_persist_and_enqueue(
    temp_db: Path, enqueue_spy: list[tuple[str, dict[str, Any]]]
):
    csv_path = Path("tests/fixtures/leads_small.csv")
    jsonl_path = Path("tests/fixtures/leads_small.jsonl")

    csv_rows = _rows_from_csv(csv_path)
    jsonl_rows = _rows_from_jsonl(jsonl_path)

    a1, r1 = _ingest_rows(csv_rows)
    a2, r2 = _ingest_rows(jsonl_rows)

    # From fixtures: 1 bad row in each file → 2 rejects total, 4 accepted total
    assert (a1, r1) == (2, 1)
    assert (a2, r2) == (2, 1)

    # Enqueue should be called once per accepted row
    assert len(enqueue_spy) == a1 + a2

    # Enqueue payloads should include normalized role & user_supplied_domain
    normalized_domains = {p.get("user_supplied_domain", "") for (_job, p) in enqueue_spy}
    assert "acme.com" in normalized_domains
    assert "xn--bcher-kva.de" in normalized_domains  # IDN normalized

    # Verify something actually persisted
    with sqlite3.connect(temp_db) as conn:
        total = _count_people(conn)
    assert total >= a1 + a2  # at least one record per accepted row
