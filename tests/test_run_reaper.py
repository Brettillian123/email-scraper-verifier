# tests/test_run_reaper.py
"""Reaper behavior, on a hermetic sqlite file via the compat layer.

Deliberately does not use the shared test DB: the reaper writes to every stale
run it can see, so it gets its own throwaway `runs` table regardless of whether
the suite is pointed at Postgres or SQLite.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from src.db import CompatConnection
from src.queueing import reaper

RUNS_DDL = """
CREATE TABLE runs (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'dev',
  status TEXT NOT NULL DEFAULT 'queued',
  domains_json TEXT NOT NULL,
  options_json TEXT NOT NULL,
  progress_json TEXT,
  error TEXT,
  created_at TEXT,
  updated_at TEXT,
  started_at TEXT,
  finished_at TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0,
  CHECK (status IN ('queued','running','succeeded','failed','cancelled'))
);
"""


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = str(tmp_path / "reaper.db")
    sqlite3.connect(path).executescript(RUNS_DDL)
    monkeypatch.setattr(
        reaper, "get_conn", lambda: CompatConnection(sqlite3.connect(path), is_pg=False)
    )
    monkeypatch.setattr(reaper, "_enqueue_run", lambda *a, **k: None)
    return path


def _seed(path, run_id, status, age_s, retry_count=0):
    beat = _iso(datetime.now(UTC) - timedelta(seconds=age_s))
    con = sqlite3.connect(path)
    con.execute(
        "INSERT INTO runs (id, tenant_id, status, domains_json, options_json,"
        " created_at, updated_at, started_at, retry_count) VALUES (?,'dev',?,'[]','{}',?,?,?,?)",
        (run_id, status, beat, beat, beat, retry_count),
    )
    con.commit()


def _status(path, run_id):
    return sqlite3.connect(path).execute(
        "SELECT status, retry_count FROM runs WHERE id = ?", (run_id,)
    ).fetchone()


def test_reaps_stale_and_spares_live_runs(db):
    _seed(db, "stale", "running", 7200, retry_count=99)  # over any retry budget
    _seed(db, "live", "running", 30)
    _seed(db, "done", "succeeded", 999999)

    out = reaper.reap_stale_runs(timeout_s=3600)

    assert out["failed"] == ["stale"]
    assert _status(db, "stale")[0] == "failed"
    assert _status(db, "live")[0] == "running"
    assert _status(db, "done")[0] == "succeeded"


def test_reap_is_idempotent(db):
    _seed(db, "stale", "running", 7200, retry_count=99)
    reaper.reap_stale_runs(timeout_s=3600)
    second = reaper.reap_stale_runs(timeout_s=3600)

    assert second["stale"] == 0
    assert second["failed"] == []
    assert _status(db, "stale")[0] == "failed"


def test_retry_budget_requeues_then_fails(db):
    _seed(db, "flaky", "running", 7200)

    first = reaper.reap_stale_runs(timeout_s=3600, max_retries=1, requeue=True)
    assert first["requeued"] == ["flaky"]
    assert _status(db, "flaky") == ("queued", 1)

    # Goes stale again with the budget spent.
    sqlite3.connect(db).execute(
        "UPDATE runs SET updated_at = ? WHERE id = 'flaky'",
        (_iso(datetime.now(UTC) - timedelta(seconds=7200)),),
    ).connection.commit()

    second = reaper.reap_stale_runs(timeout_s=3600, max_retries=1, requeue=True)
    assert second["failed"] == ["flaky"]
    assert _status(db, "flaky")[0] == "failed"
