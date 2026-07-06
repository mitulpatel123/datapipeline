"""Token-bucket rate limiter enforcing Dhan's per-account global cap.

Dhan's 5 req/sec limit is GLOBAL per account -- it counts every endpoint together
(option chain, market quote, historical, expiry list, everything), not 5/sec per
endpoint. One TokenBucketRateLimiter instance must be shared across every call an
account's connector makes, or the account risks a 429 / soft ban.
"""
import threading
import time


class TokenBucketRateLimiter:
    def __init__(self, rate_per_second: float, capacity: float | None = None):
        self.rate = rate_per_second
        self.capacity = capacity or rate_per_second
        self.tokens = self.capacity
        self.last_refill = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self):
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.last_refill
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last_refill = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = (1 - self.tokens) / self.rate
            time.sleep(wait)


class MinIntervalLimiter:
    """Enforces a minimum interval between calls sharing the same key.

    Used for the option-chain-specific rule: max 1 unique request per 3 seconds.
    """

    def __init__(self, min_interval_seconds: float):
        self.min_interval = min_interval_seconds
        self.last_call = {}
        self.lock = threading.Lock()

    def acquire(self, key: str):
        while True:
            with self.lock:
                now = time.monotonic()
                last = self.last_call.get(key, 0)
                elapsed = now - last
                if elapsed >= self.min_interval:
                    self.last_call[key] = now
                    return
                wait = self.min_interval - elapsed
            time.sleep(wait)
