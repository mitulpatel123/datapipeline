import json

import redis

from config import settings

client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)

TICK_TTL_SECONDS = 2 * 60 * 60  # 2 hour rolling window for tick data
DEDUPE_TTL_SECONDS = 60  # idempotency window for duplicate timestamp+key rejects


def set_latest(key: str, payload: dict, ttl: int | None = None):
    client.set(key, json.dumps(payload, default=str), ex=ttl)


def get_latest(key: str) -> dict | None:
    raw = client.get(key)
    return json.loads(raw) if raw else None


def mark_write(data_type: str):
    """Records the last successful write time for a data type, read by the gap watchdog."""
    client.set(f"nifty:last_successful_write:{data_type}", str(__import__("time").time()))


def is_duplicate(dedupe_key: str) -> bool:
    """Idempotency check: returns True (and marks seen) if this key was already processed
    within DEDUPE_TTL_SECONDS -- used to reject duplicate timestamp+key combos before write."""
    key = f"nifty:seen:{dedupe_key}"
    was_set = client.set(key, "1", ex=DEDUPE_TTL_SECONDS, nx=True)
    return not was_set
