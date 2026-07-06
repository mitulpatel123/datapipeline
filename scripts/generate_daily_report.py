"""Phase 1j daily validation report: run once/day after market close.

Reports real counts (not just expected-vs-errors) for a given IST trading day:
option chain snapshots (cycles + rows), tick data by source/symbol, quote
reconciliation cycles, websocket rows by symbol, derived analytics, global
indices, news, api_request_log request/429 counts per account/endpoint,
validation rejects, data gaps, and API latency -- then a PASS/WARN/FAIL verdict.

Outputs both a human-readable Markdown report and a machine-readable JSON summary.
"""
import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import func, select

from config import settings
from config.settings import LOG_DIR
from orchestrator import SCHEDULER_PROFILES
from storage import redis_client
from storage.gap_watchdog import disabled_streams
from storage.postgres_client import get_session
from storage.postgres_models import (
    ApiRequestLog,
    DataGapLog,
    DerivedAnalytics,
    GlobalIndex,
    NewsSentiment,
    OptionChainSnapshot,
    SystemError,
    TickData,
)

DUPLICATE_NEWS_PATTERN = re.compile(r"skipped (\d+) duplicate")

IST = ZoneInfo("Asia/Kolkata")
MARKET_SECONDS_PER_DAY = int((15 * 3600 + 35 * 60) - (9 * 3600))  # 09:00-15:35 IST
WEBSOCKET_HEURISTIC_INTERVAL_SECONDS = 5  # loose floor: at least 1 tick/5s during market hours
CRITICAL_STREAM_WARN_THRESHOLD = 0.01  # spec: warn if >1% missing cycles


def _day_bounds(day) -> tuple[datetime, datetime]:
    start = datetime.combine(day, datetime.min.time(), tzinfo=IST)
    end = start + timedelta(days=1)
    return start, end


@dataclass
class ReportData:
    day: str
    soak_mode: str
    option_chain_cycles: int = 0
    option_chain_rows: int = 0
    option_chain_expected_cycles: int = 0
    quote_reconciliation_cycles: int = 0
    quote_reconciliation_expected_cycles: int = 0
    ws_nifty_rows: int = 0
    ws_nifty_expected_rows: int = 0
    tick_by_source_symbol: dict = field(default_factory=dict)
    derived_analytics_rows: int = 0
    derived_analytics_cycles: int = 0
    global_indices_by_symbol: dict = field(default_factory=dict)
    news_rows: int = 0
    api_requests_by_account_family: dict = field(default_factory=dict)
    api_429_by_account_family: dict = field(default_factory=dict)
    api_latency_ms_avg: float | None = None
    api_latency_ms_max: float | None = None
    validation_rejects: int = 0
    validation_rejects_by_component: dict = field(default_factory=dict)
    data_gaps: list = field(default_factory=list)
    longest_gap_seconds_by_stream: dict = field(default_factory=dict)
    system_error_count: int = 0
    duplicate_news_skipped: int = 0
    expiry_list_refresh_status: str = "unknown"
    instrument_master_status: str = "unknown"
    eod_backfill_status: str = "unknown"
    disabled_optional_streams: dict = field(default_factory=dict)
    verdict: str = "PASS"
    verdict_reasons: list = field(default_factory=list)


def _pct_missing(actual: int, expected: int) -> float:
    if expected <= 0:
        return 0.0
    return max(0.0, (expected - actual) / expected)


def _stream_status(data_type: str) -> str:
    """Current status (not day-scoped -- these keys hold only the latest write time,
    there's no per-day history to derive an exact count from without a dedicated
    counter). Good enough to answer "did this actually run, and how recently"."""
    raw = redis_client.client.get(f"nifty:last_successful_write:{data_type}")
    if raw is None:
        return "never written this run"
    last_write = datetime.fromtimestamp(float(raw), tz=IST)
    return f"last refreshed {last_write.isoformat()}"


def collect_report_data(day) -> ReportData:
    start, end = _day_bounds(day)
    profile = SCHEDULER_PROFILES.get(settings.SOAK_MODE, SCHEDULER_PROFILES["safe"])
    r = ReportData(day=day.isoformat(), soak_mode=settings.SOAK_MODE)

    r.option_chain_expected_cycles = int(MARKET_SECONDS_PER_DAY / profile["option_chain_seconds"])
    r.quote_reconciliation_expected_cycles = int(MARKET_SECONDS_PER_DAY / profile["quote_reconciliation_seconds"])
    r.ws_nifty_expected_rows = int(MARKET_SECONDS_PER_DAY / WEBSOCKET_HEURISTIC_INTERVAL_SECONDS)

    with get_session() as session:
        r.option_chain_cycles = session.execute(
            select(func.count(func.distinct(OptionChainSnapshot.fetched_at))).where(
                OptionChainSnapshot.fetched_at >= start, OptionChainSnapshot.fetched_at < end
            )
        ).scalar() or 0
        r.option_chain_rows = session.execute(
            select(func.count()).select_from(OptionChainSnapshot).where(
                OptionChainSnapshot.fetched_at >= start, OptionChainSnapshot.fetched_at < end
            )
        ).scalar() or 0

        r.quote_reconciliation_cycles = session.execute(
            select(func.count(func.distinct(TickData.fetched_at))).where(
                TickData.source_account == "acct1_quote", TickData.fetched_at >= start, TickData.fetched_at < end
            )
        ).scalar() or 0

        r.ws_nifty_rows = session.execute(
            select(func.count()).select_from(TickData).where(
                TickData.source_account == "acct1_ws", TickData.symbol == "NIFTY",
                TickData.fetched_at >= start, TickData.fetched_at < end,
            )
        ).scalar() or 0

        tick_rows = session.execute(
            select(TickData.source_account, TickData.symbol, func.count()).where(
                TickData.fetched_at >= start, TickData.fetched_at < end
            ).group_by(TickData.source_account, TickData.symbol)
        ).all()
        for source_account, symbol, count in tick_rows:
            r.tick_by_source_symbol[f"{source_account}:{symbol}"] = count

        r.derived_analytics_rows = session.execute(
            select(func.count()).select_from(DerivedAnalytics).where(
                DerivedAnalytics.fetched_at >= start, DerivedAnalytics.fetched_at < end
            )
        ).scalar() or 0
        r.derived_analytics_cycles = session.execute(
            select(func.count(func.distinct(DerivedAnalytics.fetched_at))).where(
                DerivedAnalytics.fetched_at >= start, DerivedAnalytics.fetched_at < end
            )
        ).scalar() or 0

        global_rows = session.execute(
            select(GlobalIndex.symbol, func.count()).where(
                GlobalIndex.fetched_at >= start, GlobalIndex.fetched_at < end
            ).group_by(GlobalIndex.symbol)
        ).all()
        for symbol, count in global_rows:
            r.global_indices_by_symbol[symbol] = count

        r.news_rows = session.execute(
            select(func.count()).select_from(NewsSentiment).where(
                NewsSentiment.fetched_at >= start, NewsSentiment.fetched_at < end
            )
        ).scalar() or 0

        api_rows = session.execute(
            select(ApiRequestLog.source_account, ApiRequestLog.endpoint_family, ApiRequestLog.rate_limited).where(
                ApiRequestLog.fetched_at >= start, ApiRequestLog.fetched_at < end
            )
        ).all()
        for source_account, endpoint_family, rate_limited in api_rows:
            key = f"{source_account}/{endpoint_family}"
            r.api_requests_by_account_family[key] = r.api_requests_by_account_family.get(key, 0) + 1
            if rate_limited:
                r.api_429_by_account_family[key] = r.api_429_by_account_family.get(key, 0) + 1

        latency_stats = session.execute(
            select(func.avg(ApiRequestLog.latency_ms), func.max(ApiRequestLog.latency_ms)).where(
                ApiRequestLog.fetched_at >= start, ApiRequestLog.fetched_at < end
            )
        ).one_or_none()
        if latency_stats and latency_stats[0] is not None:
            r.api_latency_ms_avg = round(float(latency_stats[0]), 1)
            r.api_latency_ms_max = int(latency_stats[1]) if latency_stats[1] is not None else None

        r.validation_rejects = session.execute(
            select(func.count()).select_from(SystemError).where(
                SystemError.fetched_at >= start, SystemError.fetched_at < end,
                SystemError.error_message.ilike("%rejected on validation%"),
            )
        ).scalar() or 0

        reject_rows = session.execute(
            select(SystemError.component, func.count()).where(
                SystemError.fetched_at >= start, SystemError.fetched_at < end,
                SystemError.error_message.ilike("%rejected on validation%"),
            ).group_by(SystemError.component)
        ).all()
        for component, count in reject_rows:
            r.validation_rejects_by_component[component] = count

        news_dup_rows = session.execute(
            select(SystemError.error_message).where(
                SystemError.fetched_at >= start, SystemError.fetched_at < end,
                SystemError.component == "news_connector",
                SystemError.error_message.ilike("%skipped%duplicate%"),
            )
        ).scalars().all()
        for message in news_dup_rows:
            match = DUPLICATE_NEWS_PATTERN.search(message)
            if match:
                r.duplicate_news_skipped += int(match.group(1))

        r.system_error_count = session.execute(
            select(func.count()).select_from(SystemError).where(
                SystemError.fetched_at >= start, SystemError.fetched_at < end
            )
        ).scalar() or 0

        gaps = session.execute(
            select(DataGapLog).where(DataGapLog.fetched_at >= start, DataGapLog.fetched_at < end)
        ).scalars().all()
        r.data_gaps = [
            {"data_type": g.data_type, "gap_seconds": float(g.gap_seconds) if g.gap_seconds else None, "severity": g.severity}
            for g in gaps
        ]
        for gap in r.data_gaps:
            if gap["gap_seconds"] is None:
                continue
            current = r.longest_gap_seconds_by_stream.get(gap["data_type"], 0.0)
            r.longest_gap_seconds_by_stream[gap["data_type"]] = max(current, gap["gap_seconds"])

    r.expiry_list_refresh_status = _stream_status("expiry_list_refresh")
    r.instrument_master_status = _stream_status("instrument_master")
    r.eod_backfill_status = _stream_status("eod_historical_backfill")
    r.disabled_optional_streams = disabled_streams()

    _apply_verdict(r)
    return r


def _apply_verdict(r: ReportData):
    reasons = []
    total_429 = sum(r.api_429_by_account_family.values())
    if total_429 > 0 and r.soak_mode in ("normal", "production"):
        reasons.append(f"FAIL: {total_429} Dhan rate-limit (429) incident(s) occurred while running in '{r.soak_mode}' mode")

    oc_missing_pct = _pct_missing(r.option_chain_cycles, r.option_chain_expected_cycles)
    if oc_missing_pct > CRITICAL_STREAM_WARN_THRESHOLD:
        reasons.append(f"WARN: option chain cycles missing {oc_missing_pct:.1%} (actual={r.option_chain_cycles}, expected~={r.option_chain_expected_cycles})")

    qr_missing_pct = _pct_missing(r.quote_reconciliation_cycles, r.quote_reconciliation_expected_cycles)
    if qr_missing_pct > CRITICAL_STREAM_WARN_THRESHOLD:
        reasons.append(f"WARN: quote reconciliation cycles missing {qr_missing_pct:.1%} (actual={r.quote_reconciliation_cycles}, expected~={r.quote_reconciliation_expected_cycles})")

    ws_missing_pct = _pct_missing(r.ws_nifty_rows, r.ws_nifty_expected_rows)
    if ws_missing_pct > CRITICAL_STREAM_WARN_THRESHOLD:
        reasons.append(f"WARN: websocket NIFTY rows missing {ws_missing_pct:.1%} (actual={r.ws_nifty_rows}, expected(heuristic)~={r.ws_nifty_expected_rows})")

    if any(g["severity"] == "critical" for g in r.data_gaps):
        reasons.append("WARN: at least one critical-severity data gap logged")

    if total_429 > 0 and r.soak_mode == "safe":
        reasons.append(f"WARN: {total_429} Dhan rate-limit (429) incident(s) occurred (soak mode is 'safe', not yet FAIL-graded)")

    if any(msg.startswith("FAIL") for msg in reasons):
        r.verdict = "FAIL"
    elif reasons:
        r.verdict = "WARN"
    else:
        r.verdict = "PASS"
    r.verdict_reasons = reasons


def render_markdown(r: ReportData) -> str:
    lines = [
        f"# Daily Validation Report: {r.day} (IST)",
        "",
        f"**SOAK_MODE:** {r.soak_mode}",
        f"**Verdict: {r.verdict}**",
        "",
    ]
    if r.verdict_reasons:
        lines.append("Reasons:")
        for reason in r.verdict_reasons:
            lines.append(f"- {reason}")
    else:
        lines.append("No issues found.")
    lines.append("")

    lines += [
        "## Option chain",
        f"- Cycles: {r.option_chain_cycles} / ~{r.option_chain_expected_cycles} expected",
        f"- Rows: {r.option_chain_rows}",
        "",
        "## Quote reconciliation",
        f"- Cycles: {r.quote_reconciliation_cycles} / ~{r.quote_reconciliation_expected_cycles} expected",
        "",
        "## WebSocket (NIFTY)",
        f"- Rows: {r.ws_nifty_rows} / ~{r.ws_nifty_expected_rows} expected (loose heuristic, push-based feed)",
        "",
        "## Tick data by source/symbol",
    ]
    for key, count in sorted(r.tick_by_source_symbol.items()):
        lines.append(f"- {key}: {count}")

    lines += [
        "",
        "## Derived analytics",
        f"- Rows: {r.derived_analytics_rows}, cycles: {r.derived_analytics_cycles}",
        "",
        "## Global indices",
    ]
    for symbol, count in sorted(r.global_indices_by_symbol.items()):
        lines.append(f"- {symbol}: {count}")

    lines += [
        "",
        f"## News sentiment: {r.news_rows} rows",
        "",
        "## Dhan API requests (api_request_log)",
    ]
    for key, count in sorted(r.api_requests_by_account_family.items()):
        rl = r.api_429_by_account_family.get(key, 0)
        lines.append(f"- {key}: {count} requests, {rl} rate-limited")
    if r.api_latency_ms_avg is not None:
        lines.append(f"- Latency: avg {r.api_latency_ms_avg}ms, max {r.api_latency_ms_max}ms")

    lines += [
        "",
        f"## Validation rejects: {r.validation_rejects}",
    ]
    for component, count in sorted(r.validation_rejects_by_component.items()):
        lines.append(f"- {component}: {count}")

    lines += [
        "",
        f"## Duplicate/skipped news articles: {r.duplicate_news_skipped}",
        "",
        "## Refresh/backfill status",
        f"- Expiry list refresh: {r.expiry_list_refresh_status}",
        f"- Instrument master: {r.instrument_master_status}",
        f"- EOD historical backfill: {r.eod_backfill_status}",
        "",
        "## Disabled optional streams",
    ]
    if not r.disabled_optional_streams:
        lines.append("- none")
    for name, reason in sorted(r.disabled_optional_streams.items()):
        lines.append(f"- {name}: {reason}")

    lines += [
        "",
        f"## System errors (all alerts fired): {r.system_error_count}",
        "",
        "## Data gaps",
    ]
    if not r.data_gaps:
        lines.append("- none")
    for g in r.data_gaps:
        lines.append(f"- [{g['severity']}] {g['data_type']}: {g['gap_seconds']}s")

    lines += ["", "## Longest gap per stream"]
    if not r.longest_gap_seconds_by_stream:
        lines.append("- none")
    for data_type, seconds in sorted(r.longest_gap_seconds_by_stream.items(), key=lambda kv: -kv[1]):
        lines.append(f"- {data_type}: {seconds:.0f}s")

    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="YYYY-MM-DD (IST), defaults to today", default=None)
    args = parser.parse_args()

    day = datetime.now(IST).date() if not args.date else datetime.fromisoformat(args.date).date()
    report = collect_report_data(day)

    markdown = render_markdown(report)
    print(markdown)
    print(f"\nVERDICT: {report.verdict}")

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    md_path = LOG_DIR / f"daily_report_{day.isoformat()}.md"
    json_path = LOG_DIR / f"daily_report_{day.isoformat()}.json"
    md_path.write_text(markdown)
    json_path.write_text(json.dumps(report.__dict__, indent=2, default=str))
    print(f"\nWritten to {md_path} and {json_path}")
