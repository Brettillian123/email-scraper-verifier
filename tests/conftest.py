# ruff: noqa: E402
# tests/conftest.py
# tests/conftest.py (top of file)
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))  # allow `import src` in CI

import inspect

import pytest

import src.ingest as I


@pytest.fixture(autouse=True, scope="session")
def _debug_import():
    print("PYTEST USING:", I.__file__)
    print("PYTEST ingest_row source:\n", inspect.getsource(I.ingest_row))
    print("PYTEST sys.path[0:3]:", sys.path[0:3])
