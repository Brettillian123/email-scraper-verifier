# scripts/r06_selftest.py
from __future__ import annotations

# --- Import-path bootstrap so "import src" works when run from scripts/ ---
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# --- Standard imports (now safe to import project modules later) ---
import json
import os
import threading
import time

import rq.worker
from redis import Redis
from rq import Queue, SimpleWorker, Worker
from rq.registry import FailedJobRegistry

# RQ installs SIGINT/SIGTERM handlers in .work(); Python forbids that outside main thread.
# Monkeypatch the install step to a no-op so threaded workers don’t crash (tests only).
rq.worker.Worker._install_signal_handlers = lambda self: None

# --- Test configuration (safe, low limits) ---
os.environ.setdefault("RQ_REDIS_URL", "redis://127.0.0.1:6379/0")
os.environ.setdefault("GLOBAL_MAX_CONCURRENCY", "3")
os.environ.setdefault("PER_MX_MAX_CONCURRENCY_DEFAULT", "2")
# Disable RPS inside the integration run (we test the primitive separately)
os.environ.setdefault("GLOBAL_RPS", "0")
os.environ.setdefault("PER_MX_RPS_DEFAULT", "0")
os.environ.setdefault("VERIFY_MAX_ATTEMPTS", "3")
os.environ.setdefault("VERIFY_BASE_BACKOFF_SECONDS", "1")
os.environ.setdefault("VERIFY_MAX_BACKOFF_SECONDS", "3")
os.environ.setdefault("QUEUE_NAME", "verify_selftest")

TEST_NS = "selftest:r06"
TEST_QUEUE = os.environ["QUEUE_NAME"]

# Import after env is set so the code reads these values
from src.queueing import tasks  # noqa: E402
from src.queueing.rate_limit import RPS_KEY_GLOBAL, can_consume_rps  # noqa: E402
from src.queueing.redis_conn import get_redis  # noqa: E402

r: Redis = get_redis()


def k(s: str) -> str:
    return f"{TEST_NS}:{s}"


def clear_ns() -> None:
    for key in r.scan_iter(f"{TEST_NS}:*"):
        r.delete(key)


def patch_tasks_for_selftest():
    # Force a deterministic MX so per-MX semaphore is exercised
    tasks.lookup_mx = lambda domain: ("mx.selftest.local", 0)  # type: ignore

    # Concurrency/rps counters live in Redis so threads/workers can share them
    LUA_SET_MAX = """
    local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
    local max = tonumber(redis.call('GET', KEYS[2]) or '0')
    if cur > max then
      redis.call('SET', KEYS[2], cur)
    end
    return max
    """

    def smtp_stub(email: str, helo_domain: str):
        # record that we passed RPS (smtp_probe is only called after RPS gates)
        r.incr(k("rps_pass_total"))

        # "enter critical section"
        r.incr(k("conc:cur"))
        r.eval(LUA_SET_MAX, 2, k("conc:cur"), k("conc:max"))

        # simulate network time
        time.sleep(0.35)

        # leave
        r.decr(k("conc:cur"))
        return ("valid", "selftest-ok")

    # One job that always fails *unhandled* to test DLQ/failed registry path.
    class _UnhandledError(Exception):
        pass

    def smtp_fail_unhandled(email: str, helo_domain: str):
        raise _UnhandledError("selftest-unhandled-fail")

    # Swap in our success stub
    tasks.smtp_probe = smtp_stub  # type: ignore

    # Idempotent DB write hook: stub to a Redis hash (accepts both positional & kw)
    def upsert_stub(*args, **kwargs):
        if args:
            email, verify_status, reason, mx_host = args
        else:
            email = kwargs.get("email")
            verify_status = kwargs.get("verify_status")
            reason = kwargs.get("reason")
            mx_host = kwargs.get("mx_host")

        payload = {
            "email": email,
            "verify_status": verify_status,
            "reason": reason,
            "mx_host": mx_host,
        }
        r.hset(k("db"), email, json.dumps(payload, sort_keys=True))
        r.hincrby(k("db_writes"), email, 1)

    tasks.upsert_verification_result = upsert_stub  # type: ignore

    # Expose the unhandled-fail stub so we can enqueue one failing job
    return smtp_fail_unhandled


def start_workers(n: int) -> None:
    WorkerClass = SimpleWorker if os.name == "nt" else Worker
    threads: list[threading.Thread] = []

    def run():
        q = Queue(TEST_QUEUE, connection=r)
        w = WorkerClass([q], connection=r)
        # Burst processes everything currently enqueued and exits
        w.work(burst=True, with_scheduler=False)

    for _ in range(n):
        t = threading.Thread(target=run, daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join(timeout=30)


def main() -> None:
    clear_ns()
    smtp_fail_unhandled = patch_tasks_for_selftest()

    q = Queue(TEST_QUEUE, connection=r)
    failed_reg = FailedJobRegistry(queue=q)

    # Clean slate
    for jid in failed_reg.get_job_ids():
        failed_reg.remove(jid)

    # -------- Phase A: Concurrency + Idempotency + DLQ ----------
    # Enqueue 6 normal jobs (same MX), workers=3; per-MX cap = 2 → max conc ≤ 2
    emails = [f"ok{i}@crestwellpartners.com" for i in range(6)]
    for e in emails:
        q.enqueue(tasks.verify_email_task, e, job_timeout=60)

    # Enqueue the same email twice → should "upsert" to the same logical row
    dup = "dup@crestwellpartners.com"
    q.enqueue(tasks.verify_email_task, dup, job_timeout=60)
    q.enqueue(tasks.verify_email_task, dup, job_timeout=60)

    # Enqueue one *unhandled* failing job → goes to Failed registry
    orig = tasks.smtp_probe
    tasks.smtp_probe = smtp_fail_unhandled  # type: ignore
    q.enqueue(tasks.verify_email_task, "willfail@crestwellpartners.com", job_timeout=30)
    tasks.smtp_probe = orig  # restore

    start_workers(n=3)

    # Assertions (Phase A)
    per_mx_limit = int(os.environ["PER_MX_MAX_CONCURRENCY_DEFAULT"])
    max_conc = int(r.get(k("conc:max")) or 0)

    db_map_raw: dict[bytes, bytes] = r.hgetall(k("db"))
    db_map: dict[str, str] = {kk.decode(): vv.decode() for kk, vv in db_map_raw.items()}

    failed_count = failed_reg.count

    a_ok = True
    problems: list[str] = []

    if max_conc > per_mx_limit:
        a_ok = False
        problems.append(f"Per-MX concurrency exceeded: observed={max_conc} limit={per_mx_limit}")

    dup_writes = int(r.hget(k("db_writes"), dup) or 0)
    if dup not in db_map:
        a_ok = False
        problems.append("Idempotency check failed: no upsert record for duplicate email.")
    elif dup_writes < 2:
        a_ok = False
        problems.append("Idempotency/finally path not exercised: duplicate email wrote < 2 times.")

    if failed_count < 1:
        a_ok = False
        problems.append("DLQ/Failed registry is empty; expected at least 1 failed job.")

    # -------- Phase B: RPS primitive (unit) ----------
    # Enable a tiny window and call the primitive multiple times within one second
    os.environ["GLOBAL_RPS"] = "2"

    samples: list[tuple[bool, int]] = []
    # Align to the next whole second for determinism
    edge = int(time.time())
    while int(time.time()) == edge:
        pass
    start_sec = int(time.time())

    for _ in range(5):
        # allow at most 2 in the current second
        ok = can_consume_rps(r, RPS_KEY_GLOBAL, 2)
        samples.append((ok, int(time.time())))
        # tight loop within same second

    rps_pass = sum(1 for ok, sec in samples if ok and sec == start_sec)
    b_ok = rps_pass <= 2

    # -------- Report ----------
    final_ok = a_ok and b_ok
    print("\n=== R06 SELFTEST RESULTS ===")
    print(
        json.dumps(
            {
                "queue": TEST_QUEUE,
                "per_mx_limit": per_mx_limit,
                "observed_max_concurrency": max_conc,
                "db_upsert_keys": sorted(list(db_map.keys())),
                "duplicate_writes_for_dup": dup_writes,
                "failed_jobs": failed_count,
                "rps_primitive_pass_within_1s": rps_pass,
                "all_checks_passed": final_ok,
                "problems": problems,
            },
            indent=2,
        )
    )

    if not final_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
