# tests/test_ingest_diag_flow.py
from __future__ import annotations

import csv
import importlib
import inspect
import json
import os
import pprint
import sqlite3
from collections.abc import Callable
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
    db_path = tmp_path / "diag.db"
    db_path.touch()
    _apply_schema(db_path)
    db_url = "sqlite:///" + db_path.as_posix()
    monkeypatch.setenv("DATABASE_URL", db_url)
    print(f"PYTEST DB WIRED → {db_path}")
    return db_path


@pytest.fixture()
def enqueue_spy(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, Any]]]:
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
            pass
    return calls


def _rows_from_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _rows_from_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _count_people(conn: sqlite3.Connection) -> int:
    for table in ("people", "emails", "engage_people", "ingest_items"):
        try:
            return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # type: ignore[index]
        except sqlite3.OperationalError:
            continue
    return 0


def _safe_repr(x: Any, maxlen: int = 600) -> str:
    s = pprint.pformat(x, width=100, compact=False)
    return s if len(s) <= maxlen else (s[: maxlen - 3] + "...")


def _wrap_if_exists(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    func_names: list[str],
    sink: list[dict[str, Any]],
) -> None:
    try:
        mod = importlib.import_module(module_name)
    except Exception:
        return

    for fn in func_names:
        if hasattr(mod, fn):
            orig = getattr(mod, fn)

            def _mk(_orig: Callable[..., Any], _mod: str, _fn: str) -> Callable[..., Any]:
                def _wrapper(*args: Any, **kwargs: Any) -> Any:
                    try:
                        out = _orig(*args, **kwargs)
                    except Exception as e:
                        sink.append(
                            {
                                "stage": f"{_mod}.{_fn}",
                                "args": _safe_repr(args),
                                "kwargs": _safe_repr(kwargs),
                                "raised": repr(e),
                            }
                        )
                        raise
                    else:
                        sink.append(
                            {
                                "stage": f"{_mod}.{_fn}",
                                "args": _safe_repr(args),
                                "kwargs": _safe_repr(kwargs),
                                "return": _safe_repr(out),
                            }
                        )
                        return out

                return _wrapper

            monkeypatch.setattr(mod, fn, _mk(orig, module_name, fn), raising=True)


def _discover_required_fields() -> list[str]:
    for modname in ("src.ingest.validators", "src.validators", "validators"):
        try:
            v = importlib.import_module(modname)
        except Exception:
            continue
        for name in (
            "REQUIRED_FIELDS",
            "MIN_REQUIRED_FIELDS",
            "MIN_REQUIRED",
            "REQUIRED_INPUT_FIELDS",
        ):
            if hasattr(v, name):
                try:
                    return list(getattr(v, name))
                except Exception:
                    pass
        for name in ("required_fields", "min_required_fields"):
            fn = getattr(v, name, None)
            if callable(fn):
                try:
                    return list(fn())
                except Exception:
                    pass

    try:
        ingest_mod = importlib.import_module("src.ingest")
        for name in (
            "REQUIRED_FIELDS",
            "MIN_REQUIRED_FIELDS",
            "MIN_REQUIRED",
            "REQUIRED_INPUT_FIELDS",
        ):
            if hasattr(ingest_mod, name):
                try:
                    return list(getattr(ingest_mod, name))
                except Exception:
                    pass
    except Exception:
        pass
    return []


def _read_fixture(kind: str) -> list[dict[str, Any]]:
    if kind == "csv":
        return _rows_from_csv(Path("tests/fixtures/leads_small.csv"))
    elif kind == "jsonl":
        return _rows_from_jsonl(Path("tests/fixtures/leads_small.jsonl"))
    raise AssertionError(kind)


def _split_name(full: str) -> tuple[str, str] | tuple[str, None]:
    parts = [p for p in (full or "").strip().split() if p]
    if not parts:
        return ("", None)
    if len(parts) == 1:
        return (parts[0], None)
    return (" ".join(parts[:-1]), parts[-1])


def _idna(s: str | None) -> str | None:
    if not s:
        return s
    try:
        return s.encode("idna").decode("ascii")
    except Exception:
        return s


def _variants_for_row(row: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """
    Matrix of realistic fixes:
      - alias user_supplied_domain -> domain
      - split full_name -> first_name/last_name
      - add 'role' placeholder
      - add synthetic email first.last@domain
      - IDNA-punycode domain/email if non-ASCII
      - add minimal source_url
    """
    base = dict(row)
    out: dict[str, dict[str, Any]] = {"identity": dict(base)}

    # alias domain
    if "user_supplied_domain" in base and "domain" not in base:
        out["alias_domain"] = {**base, "domain": base["user_supplied_domain"]}

    # split full_name
    if "full_name" in base:
        fn, ln = _split_name(base.get("full_name") or "")
        out["split_name"] = {**base, "first_name": fn, **({"last_name": ln} if ln else {})}

    # role placeholder
    def with_role(d: dict[str, Any]) -> dict[str, Any]:
        return d if "role" in d else {**d, "role": "Unknown"}

    # add source_url
    def with_source(d: dict[str, Any]) -> dict[str, Any]:
        return d if d.get("source_url") else {**d, "source_url": "https://fixture.local/about"}

    # synthetic email
    def with_email(d: dict[str, Any], idna_mode: bool = False) -> dict[str, Any]:
        dom = d.get("domain") or d.get("user_supplied_domain")
        fn = (d.get("first_name") or "").strip().replace(" ", ".").lower()
        ln = (d.get("last_name") or "").strip().replace(" ", ".").lower()
        if dom and fn and ln:
            dom2 = _idna(dom) if idna_mode else dom
            return {**d, "email": f"{fn}.{ln}@{dom2}".lower()}
        return d

    # idna domain
    def with_idna_domain(d: dict[str, Any]) -> dict[str, Any]:
        dom = d.get("domain")
        if dom:
            return {**d, "domain": _idna(dom)}
        return d

    # seed combos
    seeds = {
        "identity": out["identity"],
        **({"alias_domain": out["alias_domain"]} if "alias_domain" in out else {}),
        **({"split_name": out["split_name"]} if "split_name" in out else {}),
    }

    variants: dict[str, dict[str, Any]] = {}
    for name, d in seeds.items():
        variants[name] = d
        variants[name + "+role"] = with_role(d)
        variants[name + "+source"] = with_source(d)
        variants[name + "+email"] = with_email(d, idna_mode=False)
        variants[name + "+email_idna"] = with_email(d, idna_mode=True)
        # domain punycoding if present
        d2 = d
        if d.get("domain"):
            d2 = with_idna_domain(d)
            variants[name + "+idna_domain"] = d2
            variants[name + "+idna_domain+email"] = with_email(d2, idna_mode=False)
            variants[name + "+idna_domain+email_idna"] = with_email(d2, idna_mode=True)
        # full combo
        v = with_source(with_role(with_email(d2, idna_mode=True)))
        variants[name + "+email_idna+role+source"] = v

    # cross: alias+split together if both exist
    if "alias_domain" in out and "split_name" in out:
        both = {**out["alias_domain"], **out["split_name"]}
        variants["alias_domain+split_name"] = both
        variants["alias_domain+split_name+email"] = with_email(both, idna_mode=False)
        variants["alias_domain+split_name+email_idna"] = with_email(both, idna_mode=True)
        variants["alias_domain+split_name+email_idna+role+source"] = with_source(
            with_role(with_email(both, True))
        )

    return variants


def _ingest_and_measure(
    ingest_mod,
    rows: list[dict[str, Any]],
    enqueue_spy,
    conn: sqlite3.Connection,
) -> tuple[int, int, list[dict[str, Any]]]:
    accepted = rejected = 0
    per_row: list[dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        before = _count_people(conn)
        try:
            ok = ingest_mod.ingest_row(row)
        except Exception as e:
            ok = False
            delta = 0
            per_row.append(
                {
                    "row_index": idx,
                    "accepted": False,
                    "raised": repr(e),
                    "db_count_delta": delta,
                    "present_keys": sorted(row.keys()),
                }
            )
        else:
            after = _count_people(conn)
            delta = after - before
            if ok:
                accepted += 1
            else:
                rejected += 1
            per_row.append(
                {
                    "row_index": idx,
                    "accepted": ok,
                    "db_count_delta": delta,
                    "present_keys": sorted(row.keys()),
                }
            )
    return accepted, rejected, per_row


def _gate_results(ingest_mod, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    fn = getattr(ingest_mod, "is_minimum_viable", None)
    if not callable(fn):
        return out
    for idx, r in enumerate(rows, start=1):
        try:
            ok, reasons = fn(r)
        except Exception as e:
            out.append({"row_index": idx, "ok": False, "raised": repr(e)})
        else:
            out.append({"row_index": idx, "ok": bool(ok), "reasons": reasons})
    return out


@pytest.mark.parametrize("kind, expected", [("csv", (2, 1)), ("jsonl", (2, 1))])
def test_ingest_row_diagnostic_flow(
    kind: str,
    expected: tuple[int, int],
    temp_db: Path,
    enqueue_spy: list,
    monkeypatch: pytest.MonkeyPatch,
):
    ingest_mod = importlib.import_module("src.ingest")

    # Wrap helpers including the gate
    call_trace: list[dict[str, Any]] = []
    _wrap_if_exists(
        monkeypatch,
        "src.ingest",
        ["is_minimum_viable", "normalize_row", "validate", "persist", "persist_row"],
        call_trace,
    )
    _wrap_if_exists(
        monkeypatch,
        "src.ingest.normalize",
        ["normalize", "normalize_row", "norm_row", "normalize_ingest"],
        call_trace,
    )
    _wrap_if_exists(
        monkeypatch,
        "src.ingest.validators",
        ["validate", "validate_row", "validate_ingest", "validate_ingest_row"],
        call_trace,
    )
    _wrap_if_exists(
        monkeypatch,
        "src.ingest.persist",
        [
            "persist",
            "persist_row",
            "upsert_row",
            "upsert_person",
            "upsert_email",
            "write_person",
            "write_email",
        ],
        call_trace,
    )

    rows = _read_fixture(kind)
    required = _discover_required_fields()

    # ---- Baseline — as-is (Option A: measure baseline-only enqueue delta) ----
    pre_enq = len(enqueue_spy)
    with sqlite3.connect(temp_db) as conn:
        base_counts = _ingest_and_measure(ingest_mod, rows, enqueue_spy, conn)
        base_gate = _gate_results(ingest_mod, rows)
    baseline_enqueues = len(enqueue_spy) - pre_enq  # enqueues produced by the baseline run

    # Variant matrix
    variant_results: dict[str, dict[str, Any]] = {}
    variants = _aggregate_variants(rows)
    for vname, vrows in variants.items():
        with sqlite3.connect(temp_db) as conn:
            acc, rej, _ = _ingest_and_measure(ingest_mod, vrows, enqueue_spy, conn)
        variant_results[vname] = {
            "accepted": acc,
            "rejected": rej,
            "gate": _gate_results(ingest_mod, vrows),
        }

    # Gate-bypass proving run — ensures downstream path is healthy
    def _gate_bypass(_row: dict[str, Any]) -> tuple[bool, list[str]]:
        return True, []

    with sqlite3.connect(temp_db) as conn:
        monkeypatch.setattr(ingest_mod, "is_minimum_viable", _gate_bypass, raising=True)
        bypass_counts = _ingest_and_measure(ingest_mod, rows, enqueue_spy, conn)
    # restore (monkeypatch cleanup happens automatically after test)

    # Source snippets
    try:
        ingest_src = inspect.getsource(ingest_mod.ingest_row)
        ingest_src_head = "\n".join(ingest_src.splitlines()[:120])
    except Exception:
        ingest_src_head = "<unavailable>"

    try:
        gate_src = inspect.getsource(ingest_mod.is_minimum_viable)
        gate_src_head = "\n".join(gate_src.splitlines()[:120])
    except Exception:
        gate_src_head = "<unavailable>"

    # Summarize & assert
    got = base_counts[:2]
    if got != expected:
        debug = {
            "fixture": kind,
            "DATABASE_URL": os.environ.get("DATABASE_URL"),
            "ENV_FLAGS": {
                k: os.environ.get(k)
                for k in (
                    "INGEST_STRICT",
                    "INGEST_REQUIRE_ROLE",
                    "INGEST_REQUIRE_EMAIL",
                    "REDIS_URL",
                    "RQ_REDIS_URL",
                    "QUEUE_URL",
                )
            },
            "ingest_module_file": getattr(ingest_mod, "__file__", None),
            "ingest_row_signature": str(
                getattr(ingest_mod.ingest_row, "__signature__", None)
                or inspect.signature(ingest_mod.ingest_row)
            ),
            "ingest_row_src_head": ingest_src_head,
            "gate_src_head": gate_src_head,
            "required_fields_discovered": required,
            "baseline": {
                "expected": expected,
                "got": got,
                "per_row": base_counts[2],
                "gate_results": base_gate,
                "enqueue_delta": baseline_enqueues,
            },
            "call_trace_tail": call_trace[-12:],
            "variant_best": _best_variants(
                {k: (v["accepted"], v["rejected"]) for k, v in variant_results.items()}
            ),
            "variant_results_sample": {
                k: {
                    "accepted": v["accepted"],
                    "rejected": v["rejected"],
                    "gate_sample": v["gate"][:3],
                }
                for k, v in list(variant_results.items())[:10]
            },
            "bypass_counts": {"accepted": bypass_counts[0], "rejected": bypass_counts[1]},
            "first_row": rows[0] if rows else None,
        }
        pretty = pprint.pformat(debug, width=100, compact=False)
        pytest.fail(
            "Ingest diagnostic mismatch — enriched dump below.\n\n"
            + pretty
            + "\n\nHow to read this:\n"
            + "1) 'gate_src_head' shows the first ~120 lines of "
            + "is_minimum_viable; find the exact required keys/env.\n"
            + "2) 'baseline.gate_results' gives (ok, reasons) per row.\n"
            + "3) 'variant_results_*' show which small tweaks flip the gate. "
            + "Try the top entries in 'variant_best'.\n"
            + "4) 'bypass_counts' proves whether persist/enqueue work when the "
            + "gate is forced open.\n"
        )

    # If baseline counts match, still check enqueue parity (Option A)
    accepted = got[0]
    assert baseline_enqueues >= accepted, (
        "Expected at least one enqueue per accepted row; "
        f"accepted={accepted}, baseline_enqueues={baseline_enqueues}"
    )


# ---- helpers to build/score row variants ------------------------------------


def _aggregate_variants(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    per_variant: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        vmap = _variants_for_row(r)
        for name, vrow in vmap.items():
            per_variant.setdefault(name, []).append(vrow)
    return per_variant


def _best_variants(
    variant_results_simple: dict[str, tuple[int, int]],
) -> list[tuple[str, tuple[int, int]]]:
    return sorted(
        variant_results_simple.items(),
        key=lambda kv: (-kv[1][0], kv[1][1], kv[0]),
    )[:8]
