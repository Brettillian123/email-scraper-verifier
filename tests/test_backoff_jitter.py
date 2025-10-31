from src.queueing import rate_limit


def test_full_jitter_ranges():
    base = 0.2
    cap = 2.5
    for attempt in range(0, 7):
        hi = min(cap, base * (2**attempt))
        for _ in range(200):
            d = rate_limit.compute_backoff(attempt, base=base, cap=cap, jitter="full")
            assert 0.0 <= d <= hi


def test_equal_jitter_ranges():
    base = 0.2
    cap = 2.5
    for attempt in range(0, 7):
        hi = min(cap, base * (2**attempt))
        lo = hi / 2.0
        for _ in range(200):
            d = rate_limit.compute_backoff(attempt, base=base, cap=cap, jitter="equal")
            assert lo <= d <= hi
