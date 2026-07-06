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
def _marketaux_enabled(monkeypatch):
    # Pin this regardless of the real .env so watched-key behavior is deterministic.
    monkeypatch.setattr("storage.gap_watchdog.settings.MARKETAUX_API_KEY", "test-key")


def _all_keys():
    return gw.get_watched_keys()


@pytest.fixture(autouse=True)
def _clean_state():
    def _clear():
        for data_type, _, _ in _all_keys():
            redis_client.client.delete(f"nifty:last_successful_write:{data_type}")
            redis_client.client.delete(gw._cooldown_key(data_type))

    _clear()
    yield
    _clear()
    with get_session() as session:
        session.execute(delete(DataGapLog).where(DataGapLog.data_type.in_([k for k, _, _ in _all_keys()])))


def test_within_grace_period_fires_nothing():
    gw._process_start_monotonic = time.monotonic()
    assert gw.check_gaps() == []


def test_never_started_past_grace_period_fires_for_all_watched_keys():
    gw._process_start_monotonic = time.monotonic() - 700  # past every grace period
    fired = gw.check_gaps()
    fired_types = {msg.split("gap detected: ")[1].split(" has")[0] for msg in fired}
    assert fired_types == {k for k, _, _ in _all_keys()}


def test_cooldown_suppresses_immediate_repeat_alert():
    gw._process_start_monotonic = time.monotonic() - 700
    first = gw.check_gaps()
    assert len(first) > 0
    second = gw.check_gaps()
    assert second == []


def test_recent_write_does_not_fire():
    gw._process_start_monotonic = time.monotonic() - 700
    for data_type, _, _ in _all_keys():
        redis_client.mark_write(data_type)
    assert gw.check_gaps() == []


def test_stale_write_past_threshold_fires():
    gw._process_start_monotonic = time.monotonic() - 700
    # Must exceed expected_interval * STALE_MULTIPLIER for EVERY watched key, including
    # instrument_master's 86400s interval (threshold 259200s) -- not just the short ones.
    stale_time = time.time() - 300_000
    for data_type, _, _ in _all_keys():
        redis_client.client.set(f"nifty:last_successful_write:{data_type}", str(stale_time))
    fired = gw.check_gaps()
    assert len(fired) == len(_all_keys())


def test_futures_and_heavyweight_keys_are_watched():
    watched_names = {k for k, _, _ in _all_keys()}
    assert "tick_data:acct1_ws:NIFTY_FUT" in watched_names
    assert "tick_data:acct1_quote:NIFTY_FUT" in watched_names
    for symbol in gw.HEAVYWEIGHT_SYMBOLS:
        assert f"tick_data:acct1_ws:{symbol}" in watched_names


class TestOptionalNewsStream:
    def test_news_watched_when_marketaux_key_present(self, monkeypatch):
        monkeypatch.setattr("storage.gap_watchdog.settings.MARKETAUX_API_KEY", "a-real-key")
        names = {k for k, _, _ in gw.get_watched_keys()}
        assert "news_sentiment" in names

    def test_news_not_watched_when_marketaux_key_missing(self, monkeypatch):
        monkeypatch.setattr("storage.gap_watchdog.settings.MARKETAUX_API_KEY", None)
        names = {k for k, _, _ in gw.get_watched_keys()}
        assert "news_sentiment" not in names

    def test_no_never_started_alert_for_news_when_key_missing(self, monkeypatch):
        monkeypatch.setattr("storage.gap_watchdog.settings.MARKETAUX_API_KEY", None)
        gw._process_start_monotonic = time.monotonic() - 700
        fired = gw.check_gaps()
        assert not any("news_sentiment" in msg for msg in fired)

    def test_disabled_streams_reports_news_when_key_missing(self, monkeypatch):
        monkeypatch.setattr("storage.gap_watchdog.settings.MARKETAUX_API_KEY", None)
        disabled = gw.disabled_streams()
        assert "news_sentiment" in disabled
        assert "MARKETAUX_API_KEY" in disabled["news_sentiment"]

    def test_disabled_streams_empty_when_key_present(self, monkeypatch):
        monkeypatch.setattr("storage.gap_watchdog.settings.MARKETAUX_API_KEY", "a-real-key")
        assert gw.disabled_streams() == {}
