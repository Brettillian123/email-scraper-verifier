import random
import time
from concurrent.futures import ThreadPoolExecutor

import fakeredis
import redis

from src.queueing import rate_limit


def test_per_mx_concurrency_cap():
    r = fakeredis.FakeRedis()
    mx = "mx.example.net"
    limit = rate_limit.PER_MX_MAX_CONCURRENCY_DEFAULT
    total_jobs = max(20, limit * 8)

    k_cur = f"test:mx:{mx}:cur"
    k_max = f"test:mx:{mx}:max"
    r.delete(k_cur, k_max)

    def task(i: int):
        with rate_limit.per_mx_slot(
            mx, redis=r, max_concurrency=limit, acquire_timeout_s=5.0, poll_ms=10
        ):
            cur = r.incr(k_cur)

            # update 'max seen' atomically
            while True:
                with r.pipeline() as p:
                    try:
                        p.watch(k_max)
                        current_max = int(p.get(k_max) or 0)
                        if cur <= current_max:
                            p.unwatch()
                            break
                        p.multi()
                        p.set(k_max, cur)
                        p.execute()
                        break
                    except redis.WatchError:
                        continue

            time.sleep(0.05 + random.random() * 0.02)
            r.decr(k_cur)

    with ThreadPoolExecutor(max_workers=total_jobs) as pool:
        list(pool.map(task, range(total_jobs)))

    max_seen = int(r.get(k_max) or 0)
    assert max_seen == limit, f"Observed {max_seen}, expected {limit}"
    assert max_seen <= limit


def test_slot_released_on_exception():
    import pytest

    r = fakeredis.FakeRedis()
    mx = "mx.release"
    limit = 1

    with pytest.raises(RuntimeError):
        with rate_limit.per_mx_slot(mx, redis=r, max_concurrency=limit):
            raise RuntimeError("boom")

    # should immediately reacquire
    acquired = False
    with rate_limit.per_mx_slot(mx, redis=r, max_concurrency=limit):
        acquired = True
    assert acquired
