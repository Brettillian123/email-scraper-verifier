# tests/test_o17_analytics_diagnostics.py
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from src.admin.metrics import get_verification_time_series


def _get_dev_connection() -> sqlite3.Connection:
    """
    Open the *real* dev DB, bypassing pytest's temporary fallback DB wiring.

    Priority:
      1. DIAG_DB_PATH env var, if set.
      2. default: <repo_root>/data/dev.db

    This is for local diagnostics only; in CI where the file does not exist,
    the test will skip.
    """
    # Allow override via env if you want to point at a different DB.
    diag_path = os.getenv("DIAG_DB_PATH")
    if diag_path:
        db_path = Path(diag_path)
    else:
        # tests/ -> repo root -> data/dev.db
        db_path = Path(__file__).resolve().parents[1] / "data" / "dev.db"

    if not db_path.exists():
        pytest.skip(f"Diagnostics DB not found at {db_path}; run locally against data/dev.db.")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row  # type: ignore[attr-defined]
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (name,),
        )
        return cur.fetchone() is not None
    except sqlite3.Error:
        return False


def _row_to_dict(row: Any) -> dict[str, Any]:
    """
    Convert a sqlite3.Row or tuple-like row into a plain dict for printing.
    """
    try:
        if hasattr(row, "keys"):
            return {key: row[key] for key in row.keys()}
        return dict(row)  # type: ignore[arg-type]
    except Exception:
        return {"row": row}


def _print_row_dicts(rows: list[Any]) -> None:
    for row in rows:
        print(_row_to_dict(row))


def test_o17_verification_time_series_diagnostics() -> None:
    """
    Diagnostics for O17 verification time series discrepancies.

    This does NOT assert on specific values. Instead, it:

      1. Prints raw daily totals from verification_results.
      2. Prints daily totals with only the rolling window filter applied.
      3. Prints daily totals with window + verify_status IS NOT NULL, which
         matches the WHERE clause in get_verification_time_series().
      4. Prints the get_verification_time_series(...) output.
      5. Shows a combined comparison table per day.
      6. For any day where totals diverge, dumps the underlying rows with
         flags indicating whether each row is in-window and has verify_status.

    Run locally against your dev DB with:

        pytest tests/test_o17_analytics_diagnostics.py -s

    and inspect stdout to see exactly which rows are being dropped.
    """
    conn = _get_dev_connection()

    if not _table_exists(conn, "verification_results"):
        pytest.skip(
            "verification_results table not found in diagnostics DB; "
            "ensure data/dev.db is migrated and populated."
        )

    window_days = 30

    # ------------------------------------------------------------------
    # 1) Raw totals by day, no filters.
    # ------------------------------------------------------------------
    raw_all_rows = conn.execute(
        """
        SELECT
          date(COALESCE(verified_at, checked_at)) AS day,
          COUNT(*) AS total_all
        FROM verification_results
        GROUP BY day
        ORDER BY day ASC
        """
    ).fetchall()

    print("\n=== Raw verification_results daily totals (no filters) ===")
    _print_row_dicts(raw_all_rows)

    raw_all_by_day: dict[str, int] = {
        str(row["day"]): int(row["total_all"]) for row in raw_all_rows
    }

    # ------------------------------------------------------------------
    # 2) Raw totals by day WITH the rolling window filter only.
    # ------------------------------------------------------------------
    raw_window_rows = conn.execute(
        """
        SELECT
          date(COALESCE(verified_at, checked_at)) AS day,
          COUNT(*) AS total_window
        FROM verification_results
        WHERE COALESCE(verified_at, checked_at) >= datetime('now', ?)
        GROUP BY day
        ORDER BY day ASC
        """,
        (f"-{int(window_days)} days",),
    ).fetchall()

    print("\n=== Raw verification_results daily totals (in window) ===")
    _print_row_dicts(raw_window_rows)

    raw_window_by_day: dict[str, int] = {
        str(row["day"]): int(row["total_window"]) for row in raw_window_rows
    }

    # ------------------------------------------------------------------
    # 3) Raw totals by day WITH window + verify_status IS NOT NULL.
    #    This matches the WHERE clause in get_verification_time_series().
    # ------------------------------------------------------------------
    raw_window_status_rows = conn.execute(
        """
        SELECT
          date(COALESCE(verified_at, checked_at)) AS day,
          COUNT(*) AS total_window_with_status
        FROM verification_results
        WHERE COALESCE(verified_at, checked_at) >= datetime('now', ?)
          AND verify_status IS NOT NULL
        GROUP BY day
        ORDER BY day ASC
        """,
        (f"-{int(window_days)} days",),
    ).fetchall()

    print("\n=== Raw verification_results daily totals (in window + verify_status IS NOT NULL) ===")
    _print_row_dicts(raw_window_status_rows)

    raw_window_status_by_day: dict[str, int] = {
        str(row["day"]): int(row["total_window_with_status"]) for row in raw_window_status_rows
    }

    # ------------------------------------------------------------------
    # 4) Analytics helper output.
    # ------------------------------------------------------------------
    ts_points = get_verification_time_series(conn, window_days=window_days)

    print("\n=== get_verification_time_series(...) output ===")
    for point in ts_points:
        print(point)

    ts_total_by_day: dict[str, int] = {
        str(point["date"]): int(point["total"]) for point in ts_points
    }

    # ------------------------------------------------------------------
    # 5) Combined comparison table.
    # ------------------------------------------------------------------
    print("\n=== Daily comparison table ===")
    days = sorted(
        set(raw_all_by_day)
        | set(raw_window_by_day)
        | set(raw_window_status_by_day)
        | set(ts_total_by_day)
    )
    if not days:
        print("(no days found)")
        return

    header = (
        "day",
        "total_all",
        "total_in_window",
        "total_in_window_with_status",
        "ts_total",
    )
    print(" | ".join(f"{h:>28}" for h in header))

    for day in days:
        row = (
            day,
            str(raw_all_by_day.get(day, 0)),
            str(raw_window_by_day.get(day, 0)),
            str(raw_window_status_by_day.get(day, 0)),
            str(ts_total_by_day.get(day, 0)),
        )
        print(" | ".join(f"{val:>28}" for val in row))

    # ------------------------------------------------------------------
    # 6) For days where totals diverge, dump the underlying rows with flags.
    # ------------------------------------------------------------------
    for day in days:
        base = raw_all_by_day.get(day, 0)
        ts_total = ts_total_by_day.get(day, 0)
        if base == ts_total:
            continue

        print(f"\n=== Detailed rows for day {day} (raw total_all={base}, ts_total={ts_total}) ===")

        suspect_rows = conn.execute(
            """
            SELECT
              id,
              email_id,
              verify_status,
              verify_reason,
              status,
              COALESCE(verified_at, checked_at) AS ts,
              date(COALESCE(verified_at, checked_at)) AS day,
              CASE
                WHEN COALESCE(verified_at, checked_at) >= datetime('now', ?) THEN 1
                ELSE 0
              END AS in_window,
              CASE
                WHEN verify_status IS NOT NULL THEN 1
                ELSE 0
              END AS has_status
            FROM verification_results
            WHERE date(COALESCE(verified_at, checked_at)) = ?
            ORDER BY id
            """,
            (f"-{int(window_days)} days", day),
        ).fetchall()

        _print_row_dicts(suspect_rows)

    # Diagnostics only: always "pass" so it does not break CI.
    assert True
