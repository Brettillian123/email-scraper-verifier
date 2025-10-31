# src/queueing/rate_limit.py
import os
import random
import time
from contextlib import contextmanager

from redis import Redis
from redis.exceptions import WatchError

# ---- Keys / constants ----
GLOBAL_SEM = "sem:global"
MX_SEM = "sem:mx:{mx}"
SEM_TTL = 120  # seconds; prevents deadlocks if a worker dies mid-lease

RPS_KEY_GLOBAL = "rps:global:{sec}"
RPS_KEY_MX = "rps:mx:{mx}:{sec}"

# Configurable default via env
PER_MX_MAX_CONCURRENCY_DEFAULT = int(os.getenv("PER_MX_MAX_CONCURRENCY_DEFAULT", "3"))


# ---- Time helper ----
def _now_sec() -> int:
    return int(time.time())


# ---- Semaphore primitives ----
def try_acquire(redis: Redis, key: str, limit: int) -> bool:
    """
    Attempt to acquire a semaphore slot under `key`.
    Uses WATCH/MULTI to ensure we don't exceed `limit`.
    """
    while True:
        with redis.pipeline() as p:
            try:
                p.watch(key)
                cur_raw = p.get(key)
                cur = int(cur_raw) if cur_raw is not None else 0
                if cur >= limit:
                    p.unwatch()
                    return False
                p.multi()
                p.incr(key, 1)
                p.expire(key, SEM_TTL)
                p.execute()
                return True
            except WatchError:  # race; retry
                continue


def release(redis: Redis, key: str):
    """
    Best-effort release that never goes negative.
    CAS loop: read -> compute -> write/delete.
    """
    while True:
        with redis.pipeline() as p:
            try:
                p.watch(key)
                cur = int(p.get(key) or 0)
                new_val = max(cur - 1, 0)
                p.multi()
                if new_val == 0:
                    p.delete(key)
                else:
                    # set exact value and refresh TTL
                    p.set(key, new_val)
                    p.expire(key, SEM_TTL)
                p.execute()
                return
            except WatchError:
                continue
            except Exception:
                # don't let release crash the worker
                return


# ---- Simple RPS window (1s tumbling window) ----
def can_consume_rps(redis: Redis, key: str, limit: int) -> bool:
    now = _now_sec()
    window_key = f"{key.format(sec=now)}"
    cnt = redis.incr(window_key, 1)
    if cnt == 1:
        redis.expire(window_key, 2)  # 1s window + slack
    return cnt <= limit


# ---- Backoff helpers ----
def full_jitter_delay(base: float, attempt: int, cap: float) -> float:
    exp = min(cap, base * (2**attempt))
    return random.uniform(0.0, exp)


def compute_backoff(
    attempt: int, *, base: float = 1.0, cap: float = 60.0, jitter: str = "full"
) -> float:
    """
    Exponential backoff with jitter.
    - 'full':  uniform(0, min(cap, base * 2**attempt))
    - 'equal': uniform(min(cap, base * 2**attempt)/2, min(cap, base * 2**attempt))
    """
    if attempt < 0:
        attempt = 0
    hi = min(cap, base * (2**attempt))
    if jitter == "equal":
        lo = hi / 2.0
        return random.uniform(lo, hi)
    return random.uniform(0.0, hi)


# ---- Per-MX slot context manager (used by tests and worker) ----
@contextmanager
def per_mx_slot(
    mx_host: str,
    *,
    redis: Redis,
    max_concurrency: int | None = None,
    acquire_timeout_s: float = 10.0,
    poll_ms: int = 50,
):
    """
    Acquire a per-MX semaphore slot. Blocks until acquired or timeout.
    Counter key: MX_SEM.format(mx=mx_host)
    """
    limit = int(max_concurrency or PER_MX_MAX_CONCURRENCY_DEFAULT)
    key = MX_SEM.format(mx=mx_host)
    deadline = time.monotonic() + acquire_timeout_s

    # Acquire loop
    while True:
        if try_acquire(redis, key, limit):
            break
        if time.monotonic() > deadline:
            raise TimeoutError(f"per_mx_slot acquire timed out for {mx_host}")
        time.sleep(poll_ms / 1000.0)

    try:
        yield
    finally:
        release(redis, key)
