import time

import pytest
from sqlalchemy import delete

import storage.gap_watchdog as gw
from storage import redis_client
from storage.postgres_client import get_session
from storage.postgres_models import DataGapLog


@pytest.fixture(autouse=True)
def _mock_telegram(monkeypatch):
    # These tests deliberately trigger gap alerts -- don't spam the user's phone.
    monkeypatch.setattr("storage.gap_watchdog.send_telegram_alert", lambda *a, **k: None)


@pytest.fixture(autouse=True)
def _clean_state():
    for data_type, _, _ in gw.WATCHED_KEYS:
        redis_client.client.delete(f"nifty:last_successful_write:{data_type}")
        redis_client.client.delete(gw._cooldown_key(data_type))
    yield
    for data_type, _, _ in gw.WATCHED_KEYS:
        redis_client.client.delete(f"nifty:last_successful_write:{data_type}")
        redis_client.client.delete(gw._cooldown_key(data_type))
    with get_session() as session:
        session.execute(delete(DataGapLog).where(DataGapLog.data_type.in_([k for k, _, _ in gw.WATCHED_KEYS])))


def test_within_grace_period_fires_nothing():
    gw._process_start_monotonic = time.monotonic()
    assert gw.check_gaps() == []


def test_never_started_past_grace_period_fires_for_all_watched_keys():
    gw._process_start_monotonic = time.monotonic() - 700  # past every grace period
    fired = gw.check_gaps()
    fired_types = {msg.split("gap detected: ")[1].split(" has")[0] for msg in fired}
    assert fired_types == {k for k, _, _ in gw.WATCHED_KEYS}


def test_cooldown_suppresses_immediate_repeat_alert():
    gw._process_start_monotonic = time.monotonic() - 700
    first = gw.check_gaps()
    assert len(first) > 0
    second = gw.check_gaps()
    assert second == []


def test_recent_write_does_not_fire():
    gw._process_start_monotonic = time.monotonic() - 700
    for data_type, _, _ in gw.WATCHED_KEYS:
        redis_client.mark_write(data_type)
    assert gw.check_gaps() == []


def test_stale_write_past_threshold_fires():
    gw._process_start_monotonic = time.monotonic() - 700
    # Must exceed expected_interval * STALE_MULTIPLIER for EVERY watched key, including
    # instrument_master's 86400s interval (threshold 259200s) -- not just the short ones.
    stale_time = time.time() - 300_000
    for data_type, _, _ in gw.WATCHED_KEYS:
        redis_client.client.set(f"nifty:last_successful_write:{data_type}", str(stale_time))
    fired = gw.check_gaps()
    assert len(fired) == len(gw.WATCHED_KEYS)
