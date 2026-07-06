"""Phase 1a checkpoint: verify Redis and PostgreSQL are reachable before any pipeline code runs."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings


def test_redis():
    import redis

    client = redis.Redis.from_url(settings.REDIS_URL)
    client.ping()
    client.set("nifty:connection_test", "ok", ex=10)
    assert client.get("nifty:connection_test") == b"ok"
    print(f"[OK] Redis reachable at {settings.REDIS_URL}")


def test_postgres():
    from sqlalchemy import create_engine, text

    engine = create_engine(settings.POSTGRES_URL)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).scalar()
        assert result == 1
    print(f"[OK] PostgreSQL reachable at {settings.POSTGRES_URL}")


def test_env():
    missing = []
    if not settings.DHAN_CLIENT_ID_1 or not settings.DHAN_ACCESS_TOKEN_1:
        missing.append("DHAN account 1 credentials")
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID:
        missing.append("Telegram credentials")
    if missing:
        print(f"[WARN] Missing: {', '.join(missing)}")
    else:
        print("[OK] Required .env credentials present")
    print(
        f"[INFO] Dhan account 2 {'enabled' if settings.DHAN_ACCOUNT_2_ENABLED else 'NOT configured yet'}"
    )
    print(
        f"[INFO] Marketaux key {'present' if settings.MARKETAUX_API_KEY else 'NOT configured'}"
    )


if __name__ == "__main__":
    failures = []
    for name, fn in [("redis", test_redis), ("postgres", test_postgres), ("env", test_env)]:
        try:
            fn()
        except Exception as exc:
            failures.append(name)
            print(f"[FAIL] {name}: {exc}")

    if failures:
        print(f"\nFAILED: {failures}")
        sys.exit(1)
    print("\nAll connection checks passed.")
