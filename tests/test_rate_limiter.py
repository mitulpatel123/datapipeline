import time

from connectors.rate_limiter import MinIntervalLimiter, TokenBucketRateLimiter


def test_token_bucket_allows_burst_up_to_capacity():
    limiter = TokenBucketRateLimiter(rate_per_second=10, capacity=3)
    start = time.monotonic()
    for _ in range(3):
        limiter.acquire()
    elapsed = time.monotonic() - start
    assert elapsed < 0.05, "first `capacity` acquires should not block"


def test_token_bucket_throttles_beyond_capacity():
    limiter = TokenBucketRateLimiter(rate_per_second=10, capacity=1)
    start = time.monotonic()
    for _ in range(3):
        limiter.acquire()
    elapsed = time.monotonic() - start
    # 3 acquires at capacity=1, rate=10/s -> ~2 waits of ~0.1s each
    assert elapsed >= 0.15


def test_min_interval_limiter_enforces_spacing_per_key():
    limiter = MinIntervalLimiter(min_interval_seconds=0.2)
    start = time.monotonic()
    limiter.acquire("key-a")
    limiter.acquire("key-a")
    elapsed = time.monotonic() - start
    assert elapsed >= 0.2


def test_min_interval_limiter_keys_are_independent():
    limiter = MinIntervalLimiter(min_interval_seconds=0.3)
    start = time.monotonic()
    limiter.acquire("key-a")
    limiter.acquire("key-b")  # different key, should not wait for key-a's interval
    elapsed = time.monotonic() - start
    assert elapsed < 0.1
