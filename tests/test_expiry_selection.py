from datetime import date

from connectors.dhan_account1 import select_nearest_valid_expiry


def test_picks_nearest_future_expiry_regardless_of_input_order():
    expiries = ["2026-08-04", "2026-07-07", "2026-12-29", "2026-07-14"]
    assert select_nearest_valid_expiry(expiries, today=date(2026, 7, 6)) == "2026-07-07"


def test_ignores_expiries_before_today():
    expiries = ["2026-07-01", "2026-07-03", "2026-07-14"]
    assert select_nearest_valid_expiry(expiries, today=date(2026, 7, 6)) == "2026-07-14"


def test_today_itself_is_valid():
    expiries = ["2026-07-06", "2026-07-14"]
    assert select_nearest_valid_expiry(expiries, today=date(2026, 7, 6)) == "2026-07-06"


def test_returns_none_when_nothing_valid():
    expiries = ["2026-01-01", "2026-06-30"]
    assert select_nearest_valid_expiry(expiries, today=date(2026, 7, 6)) is None


def test_ignores_unparseable_entries():
    expiries = ["not-a-date", "2026-07-14"]
    assert select_nearest_valid_expiry(expiries, today=date(2026, 7, 6)) == "2026-07-14"


def test_empty_list_returns_none():
    assert select_nearest_valid_expiry([], today=date(2026, 7, 6)) is None
