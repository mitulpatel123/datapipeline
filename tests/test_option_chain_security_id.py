from datetime import date, datetime, timezone

import pytest
from sqlalchemy import delete, select

from storage import redis_client
from storage.ingest import store_option_chain_snapshot
from storage.postgres_client import get_session
from storage.postgres_models import OptionChainSnapshot

TEST_EXPIRY = date(2026, 7, 7)


@pytest.fixture(autouse=True)
def _cleanup():
    yield
    with get_session() as session:
        session.execute(delete(OptionChainSnapshot).where(OptionChainSnapshot.expiry == TEST_EXPIRY))


def test_security_id_persists_to_postgres():
    fetched_at = datetime.now(timezone.utc)
    row = {
        "fetched_at": fetched_at, "expiry": TEST_EXPIRY, "strike": 24000.0, "option_type": "CE",
        "ltp": 10.0, "oi": 100, "prev_oi": 90, "volume": 5, "iv": 15.0,
        "delta": 0.5, "theta": -1, "gamma": 0.01, "vega": 2, "bid": 9.5, "ask": 10.5,
        "underlying_ltp": 24000, "security_id": "42499",
    }

    with get_session() as session:
        stored, rejected = store_option_chain_snapshot(session, [row], "test_acct")
    assert stored == 1
    assert rejected == 0

    with get_session() as session:
        db_row = session.execute(
            select(OptionChainSnapshot).where(OptionChainSnapshot.expiry == TEST_EXPIRY)
        ).scalar_one()
    assert db_row.security_id == "42499"


def test_security_id_absent_is_stored_as_none():
    fetched_at = datetime.now(timezone.utc)
    row = {
        "fetched_at": fetched_at, "expiry": TEST_EXPIRY, "strike": 24050.0, "option_type": "PE",
        "ltp": 10.0, "oi": 100, "volume": 5, "iv": 15.0, "bid": 9.5, "ask": 10.5,
        "underlying_ltp": 24000,
    }

    with get_session() as session:
        stored, _ = store_option_chain_snapshot(session, [row], "test_acct")
    assert stored == 1

    with get_session() as session:
        db_row = session.execute(
            select(OptionChainSnapshot).where(
                OptionChainSnapshot.expiry == TEST_EXPIRY, OptionChainSnapshot.strike == 24050.0
            )
        ).scalar_one()
    assert db_row.security_id is None


def test_redis_latest_snapshot_cache_contains_security_id():
    """The websocket option-universe refresh reads security_id from this exact cache
    (connectors/dhan_websocket.py refresh_option_universe) -- it must round-trip."""
    fetched_at = datetime.now(timezone.utc)
    rows = [
        {
            "fetched_at": fetched_at, "expiry": "2026-07-07", "strike": 24000.0, "option_type": "CE",
            "security_id": "42499", "ltp": 10.0, "oi": 100,
        }
    ]
    cache_key = "nifty:optionchain:2026-07-07:latest"
    redis_client.set_latest(cache_key, {"fetched_at": fetched_at.isoformat(), "underlying_ltp": 24000, "rows": rows})

    cached = redis_client.get_latest(cache_key)
    assert cached["rows"][0]["security_id"] == "42499"
    redis_client.client.delete(cache_key)
