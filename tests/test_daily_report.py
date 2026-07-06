from datetime import date, datetime, timezone

import pytest
from sqlalchemy import delete

from scripts.generate_daily_report import _apply_verdict, collect_report_data, render_markdown
from storage import redis_client
from storage.postgres_client import get_session
from storage.postgres_models import ApiRequestLog, DataGapLog, OptionChainSnapshot, SystemError

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
        session.execute(
            delete(DataGapLog).where(DataGapLog.fetched_at >= start, DataGapLog.fetched_at < end)
        )
        session.execute(
            delete(SystemError).where(SystemError.fetched_at >= start, SystemError.fetched_at < end)
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


def test_markdown_contains_all_required_sections():
    report = collect_report_data(TEST_DAY)
    markdown = render_markdown(report)

    required_sections = [
        "Option chain", "WebSocket", "Quote reconciliation", "Tick data by source/symbol",
        "Derived analytics", "Global indices", "News sentiment",
        "Dhan API requests", "Validation rejects", "Duplicate/skipped news",
        "Disabled optional streams", "Refresh/backfill status", "Data gaps",
        "Longest gap per stream",
    ]
    for section in required_sections:
        assert section in markdown, f"missing required section: {section}"


def test_validation_rejects_grouped_by_component():
    start, _ = _bounds()
    with get_session() as session:
        session.add(SystemError(
            fetched_at=start.astimezone(timezone.utc), component="option_chain_snapshot",
            error_message="2 record(s) rejected on validation. First error: ...", severity="warning", resolved=False,
        ))
        session.add(SystemError(
            fetched_at=start.astimezone(timezone.utc), component="tick_data",
            error_message="1 record(s) rejected on validation. First error: ...", severity="warning", resolved=False,
        ))

    report = collect_report_data(TEST_DAY)
    assert report.validation_rejects_by_component == {"option_chain_snapshot": 1, "tick_data": 1}


def test_duplicate_news_skipped_count_parsed_from_system_errors():
    start, _ = _bounds()
    with get_session() as session:
        session.add(SystemError(
            fetched_at=start.astimezone(timezone.utc), component="news_connector",
            error_message="skipped 3 duplicate-url article(s) this cycle", severity="info", resolved=False,
        ))
        session.add(SystemError(
            fetched_at=start.astimezone(timezone.utc), component="news_connector",
            error_message="skipped 2 duplicate-url article(s) this cycle", severity="info", resolved=False,
        ))

    report = collect_report_data(TEST_DAY)
    assert report.duplicate_news_skipped == 5


def test_disabled_optional_streams_reflects_missing_marketaux_key(monkeypatch):
    monkeypatch.setattr("storage.gap_watchdog.settings.MARKETAUX_API_KEY", None)
    report = collect_report_data(TEST_DAY)
    assert "news_sentiment" in report.disabled_optional_streams


def test_longest_gap_per_stream_takes_max_not_sum():
    start, _ = _bounds()
    fetched_at = start.astimezone(timezone.utc)
    with get_session() as session:
        session.add(DataGapLog(
            fetched_at=fetched_at, data_type="tick_data:acct1_ws:NIFTY",
            expected_fetch_time=fetched_at, actual_fetch_time=fetched_at, gap_seconds=50, severity="warning",
        ))
        session.add(DataGapLog(
            fetched_at=fetched_at, data_type="tick_data:acct1_ws:NIFTY",
            expected_fetch_time=fetched_at, actual_fetch_time=fetched_at, gap_seconds=120, severity="warning",
        ))

    report = collect_report_data(TEST_DAY)
    assert report.longest_gap_seconds_by_stream["tick_data:acct1_ws:NIFTY"] == 120


def test_refresh_status_reports_never_written_when_no_marker():
    redis_client.client.delete("nifty:last_successful_write:expiry_list_refresh")
    report = collect_report_data(TEST_DAY)
    assert report.expiry_list_refresh_status == "never written this run"


def test_refresh_status_reports_last_refreshed_time():
    import time

    redis_client.client.set("nifty:last_successful_write:instrument_master", str(time.time()))
    report = collect_report_data(TEST_DAY)
    assert "last refreshed" in report.instrument_master_status
