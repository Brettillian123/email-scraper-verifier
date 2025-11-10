# tests/debug_sqlite_spy.py
import inspect
import sqlite3

import pytest

_ORIG_CONNECT = sqlite3.connect


def _spy_connect(*args, **kwargs):
    path = args[0] if args else kwargs.get("database")
    # Show who opened the connection and where it points
    stack = inspect.stack()[1:4]  # top few frames for signal, not noise
    caller = " -> ".join(f"{f.filename}:{f.lineno}" for f in stack)
    print(f"[SQLITE-SPY] connect({path!r})  caller={caller}")

    conn = _ORIG_CONNECT(*args, **kwargs)
    try:
        # Show every SQL statement executed on this connection
        conn.set_trace_callback(lambda sql: print(f"[SQLITE-SQL] {sql}"))
    except Exception:
        pass
    return conn


@pytest.fixture(autouse=True)
def _patch_sqlite_connect(monkeypatch):
    monkeypatch.setattr(sqlite3, "connect", _spy_connect)
    yield
