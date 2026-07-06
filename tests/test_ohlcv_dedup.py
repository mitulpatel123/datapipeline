from datetime import datetime, timezone

import pytest
from sqlalchemy import delete, select

from config import settings
from storage.postgres_client import get_session
from storage.postgres_models import OhlcvIntraday


@pytest.fixture
def account2(monkeypatch):
    """A DhanAccount2 instance whose __init__ requirements are satisfied without real
    credentials -- we only need its _store_ohlcv method, no Dhan calls happen here."""
    monkeypatch.setattr(settings, "DHAN_ACCOUNT_2_ENABLED", True)
    monkeypatch.setattr(settings, "DHAN_CLIENT_ID_2", "test_client")
    monkeypatch.setattr(settings, "DHAN_ACCESS_TOKEN_2", "test_token")
    from connectors.dhan_account2 import DhanAccount2

    return DhanAccount2()


@pytest.fixture(autouse=True)
def _cleanup():
    yield
    with get_session() as session:
        session.execute(delete(OhlcvIntraday).where(OhlcvIntraday.symbol == "TEST_DEDUP"))


def _candles():
    ts = int(datetime(2026, 7, 6, 9, 15, tzinfo=timezone.utc).timestamp())
    return {
        "timestamp": [ts, ts + 300],
        "open": [100.0, 101.0],
        "high": [102.0, 103.0],
        "low": [99.0, 100.0],
        "close": [101.0, 102.0],
        "volume": [1000, 1100],
        "open_interest": [0, 0],
    }


def test_rerun_does_not_insert_duplicate_bars(account2):
    fetched_at = datetime.now(timezone.utc)
    candles = _candles()

    first_count = account2._store_ohlcv("TEST_DEDUP", "99999", "5min", candles, fetched_at)
    assert first_count == 2

    # Simulate a rerun (e.g. EOD backfill retried) with the identical bars.
    second_count = account2._store_ohlcv("TEST_DEDUP", "99999", "5min", candles, fetched_at)
    assert second_count == 0, "rerunning with identical bars must not insert duplicates"

    with get_session() as session:
        rows = session.execute(
            select(OhlcvIntraday).where(OhlcvIntraday.symbol == "TEST_DEDUP")
        ).scalars().all()
    assert len(rows) == 2
