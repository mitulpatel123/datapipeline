from datetime import date, datetime, timezone

import pytest
from sqlalchemy import delete

from scripts.generate_daily_report import _apply_verdict, collect_report_data
from storage.postgres_client import get_session
from storage.postgres_models import ApiRequestLog, OptionChainSnapshot

TEST_DAY = date(2020, 1, 6)  # a Monday, far from any real data


def _bounds():
    from zoneinfo import ZoneInfo

    ist = ZoneInfo("Asia/Kolkata")
    start = datetime.combine(TEST_DAY, datetime.min.time(), tzinfo=ist)
    return start, start


@pytest.fixture(autouse=True)
def _cleanup():
    yield
    start, _ = _bounds()
    end = start.replace(hour=23, minute=59, second=59)
    with get_session() as session:
        session.execute(
            delete(OptionChainSnapshot).where(
                OptionChainSnapshot.fetched_at >= start, OptionChainSnapshot.fetched_at < end
            )
        )
        session.execute(
            delete(ApiRequestLog).where(ApiRequestLog.fetched_at >= start, ApiRequestLog.fetched_at < end)
        )


def test_empty_day_reports_zero_counts_and_warns():
    report = collect_report_data(TEST_DAY)
    assert report.option_chain_rows == 0
    assert report.option_chain_cycles == 0
    assert report.verdict in ("WARN", "FAIL")  # 100% missing on an empty day is at least a WARN


def test_429_in_production_mode_fails(monkeypatch):
    monkeypatch.setattr("scripts.generate_daily_report.settings.SOAK_MODE", "production")
    start, _ = _bounds()
    with get_session() as session:
        session.add(
            ApiRequestLog(
                fetched_at=start.astimezone(timezone.utc),
                source_account="acct1",
                endpoint_family="optionchain",
                endpoint_name="option_chain",
                success=False,
                rate_limited=True,
                circuit_state="cooldown",
            )
        )

    report = collect_report_data(TEST_DAY)
    assert report.verdict == "FAIL"
    assert any("429" in r or "rate-limit" in r for r in report.verdict_reasons)


def test_429_in_safe_mode_only_warns(monkeypatch):
    monkeypatch.setattr("scripts.generate_daily_report.settings.SOAK_MODE", "safe")
    start, _ = _bounds()
    with get_session() as session:
        session.add(
            ApiRequestLog(
                fetched_at=start.astimezone(timezone.utc),
                source_account="acct1",
                endpoint_family="optionchain",
                endpoint_name="option_chain",
                success=False,
                rate_limited=True,
                circuit_state="cooldown",
            )
        )

    report = collect_report_data(TEST_DAY)
    assert report.verdict != "FAIL", "safe mode 429s should warn, not fail, per spec"
