from datetime import datetime

from utils.time_utils import IST, is_market_day_ist, is_market_hours_ist, market_session_bounds_ist


def test_market_hours_during_session():
    monday_1000 = datetime(2026, 7, 6, 10, 0, tzinfo=IST)  # Monday
    assert is_market_hours_ist(monday_1000) is True


def test_before_market_open():
    monday_0800 = datetime(2026, 7, 6, 8, 0, tzinfo=IST)
    assert is_market_hours_ist(monday_0800) is False


def test_after_market_close():
    monday_1600 = datetime(2026, 7, 6, 16, 0, tzinfo=IST)
    assert is_market_hours_ist(monday_1600) is False


def test_weekend_is_never_market_hours():
    saturday_1000 = datetime(2026, 7, 4, 10, 0, tzinfo=IST)  # Saturday
    assert is_market_hours_ist(saturday_1000) is False
    assert is_market_day_ist(saturday_1000.date()) is False


def test_weekday_is_a_market_day_absent_holiday_file():
    monday = datetime(2026, 7, 6, 10, 0, tzinfo=IST).date()
    assert is_market_day_ist(monday) is True


def test_market_session_bounds():
    day = datetime(2026, 7, 6, tzinfo=IST).date()
    open_dt, close_dt = market_session_bounds_ist(day)
    assert open_dt.hour == 9 and open_dt.minute == 0
    assert close_dt.hour == 15 and close_dt.minute == 35
    assert open_dt.tzinfo is not None and close_dt.tzinfo is not None
