from datetime import date

import pandas as pd
import pytest

import connectors.instrument_master as im

FAKE_FUTURES_DF = pd.DataFrame(
    [
        {"EXCH_ID": "NSE", "SEGMENT": "D", "UNDERLYING_SYMBOL": "NIFTY", "INSTRUMENT_TYPE": "FUT",
         "SECURITY_ID": 111, "SM_EXPIRY_DATE": "2026-07-31"},
        {"EXCH_ID": "NSE", "SEGMENT": "D", "UNDERLYING_SYMBOL": "NIFTY", "INSTRUMENT_TYPE": "FUT",
         "SECURITY_ID": 222, "SM_EXPIRY_DATE": "2026-08-28"},
        {"EXCH_ID": "NSE", "SEGMENT": "D", "UNDERLYING_SYMBOL": "NIFTY", "INSTRUMENT_TYPE": "FUT",
         "SECURITY_ID": 333, "SM_EXPIRY_DATE": "2026-09-25"},
    ]
)


@pytest.fixture(autouse=True)
def _fake_instrument_master(monkeypatch):
    monkeypatch.setattr(im, "load_instrument_master", lambda *a, **k: FAKE_FUTURES_DF)


def test_explicit_today_argument_still_works():
    """Explicit `today` must bypass any IST-lookup entirely -- needed for deterministic tests."""
    result = im.resolve_nearest_future("NIFTY", today=date(2026, 8, 1))
    assert result == "222"  # first contract expiring on/after 2026-08-01


def test_default_uses_today_ist_date_not_local_date_today(monkeypatch):
    """Without an explicit `today`, the function must call today_ist_date() -- not the
    stdlib date.today() (an immutable C type, can't be monkeypatched directly, which is
    itself a good reason resolve_nearest_future must not depend on it). We instead pick
    a mocked "IST today" far from the real current date and assert the result reflects
    THAT date's nearest contract -- if the code regressed to date.today(), this would
    pick a different (real-date-based) contract and fail."""
    monkeypatch.setattr(im, "today_ist_date", lambda: date(2026, 9, 1))

    result = im.resolve_nearest_future("NIFTY")

    # Only contract expiring on/after 2026-09-01 is 333 (07-31 and 08-28 are both past).
    assert result == "333"


def test_no_valid_contract_returns_none_not_error():
    result = im.resolve_nearest_future("NIFTY", today=date(2030, 1, 1))
    assert result is None


def test_unknown_symbol_returns_none():
    result = im.resolve_nearest_future("SOME_UNKNOWN_SYMBOL", today=date(2026, 7, 6))
    assert result is None
