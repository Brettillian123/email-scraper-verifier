# tests/conftest.py
import inspect
import sys

import pytest

import src.ingest as I


@pytest.fixture(autouse=True, scope="session")
def _debug_import():
    print("PYTEST USING:", I.__file__)
    print("PYTEST ingest_row source:\n", inspect.getsource(I.ingest_row))
    print("PYTEST sys.path[0:3]:", sys.path[0:3])
