import random
import time

from redis import Redis

GLOBAL_SEM = "sem:global"
MX_SEM = "sem:mx:{mx}"
SEM_TTL = 120  # seconds; prevents deadlocks if a worker dies mid-lease

RPS_KEY_GLOBAL = "rps:global:{sec}"
RPS_KEY_MX = "rps:mx:{mx}:{sec}"


def _now_sec() -> int:
    return int(time.time())


def try_acquire(redis: Redis, key: str, limit: int) -> bool:
    # Atomic INCR with TTL only when creating
    pipe = redis.pipeline()
    while True:
        try:
            pipe.watch(key)
            cur = pipe.get(key)
            cur = int(cur) if cur is not None else 0
            if cur >= limit:
                pipe.unwatch()
                return False
            pipe.multi()
            pipe.incr(key, 1)
            pipe.expire(key, SEM_TTL)
            pipe.execute()
            return True
        except redis.WatchError:  # race; retry
            continue


def release(redis: Redis, key: str):
    # Best-effort; ensure it never goes negative
    with redis.pipeline() as p:
        p.decr(key, 1)
        p.expire(key, SEM_TTL)
        try:
            p.execute()
        except Exception:
            pass


def can_consume_rps(redis: Redis, key: str, limit: int) -> bool:
    now = _now_sec()
    window_key = f"{key.format(sec=now)}"
    cnt = redis.incr(window_key, 1)
    if cnt == 1:
        redis.expire(window_key, 2)  # 1s window + slack
    return cnt <= limit


def full_jitter_delay(base: float, attempt: int, cap: float) -> float:
    exp = min(cap, base * (2**attempt))
    return random.uniform(0, exp)
