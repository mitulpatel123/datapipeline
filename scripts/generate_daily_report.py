"""Phase 1j daily validation report: run once/day after market close.

Reports, for a given IST trading day: % expected data points received per source,
gaps (data_gap_log), validation rejects, alerts fired, and rate-limit incidents per
account -- all reconstructed from system_errors / data_gap_log, which every
connector and the orchestrator write to via storage.ingest.log_and_alert /
log_system_error (see that module's docstring for why this matters).
"""
import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import func, select

from config.settings import LOG_DIR
from storage.postgres_client import get_session
from storage.postgres_models import DataGapLog, SystemError

IST = ZoneInfo("Asia/Kolkata")
MARKET_SECONDS_PER_DAY = int((15 * 3600 + 35 * 60) - (9 * 3600))  # 09:00-15:35 IST

EXPECTED_FETCH_INTERVALS = {
    "option_chain_snapshots": 3,
    "tick_data (quote reconciliation)": 5,
    "global_indices": 300,
    "news_sentiment": 300,
}


def _day_bounds(day: datetime.date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, datetime.min.time(), tzinfo=IST)
    end = start + timedelta(days=1)
    return start, end


def generate_report(day) -> str:
    start, end = _day_bounds(day)
    lines = [f"=== Daily Validation Report: {day.isoformat()} (IST) ===", ""]

    with get_session() as session:
        lines.append("-- Expected data points per source --")
        expected_cycles = MARKET_SECONDS_PER_DAY // 3
        for label, interval in EXPECTED_FETCH_INTERVALS.items():
            expected = MARKET_SECONDS_PER_DAY // interval
            lines.append(f"  {label}: expected ~{expected} fetch cycles (every {interval}s during market hours)")
        lines.append("")

        lines.append("-- Data gaps (data_gap_log) --")
        gaps = session.execute(
            select(DataGapLog).where(DataGapLog.fetched_at >= start, DataGapLog.fetched_at < end)
        ).scalars().all()
        if not gaps:
            lines.append("  none")
        for gap in gaps:
            lines.append(f"  {gap.data_type}: gap of {gap.gap_seconds}s at {gap.fetched_at}")
        lines.append("")

        lines.append("-- Validation rejects --")
        rejects = session.execute(
            select(SystemError).where(
                SystemError.fetched_at >= start,
                SystemError.fetched_at < end,
                SystemError.error_message.ilike("%rejected on validation%"),
            )
        ).scalars().all()
        if not rejects:
            lines.append("  none")
        for row in rejects:
            lines.append(f"  [{row.component}] {row.error_message[:200]}")
        lines.append("")

        lines.append("-- Alerts fired (all system_errors) --")
        all_errors = session.execute(
            select(SystemError).where(SystemError.fetched_at >= start, SystemError.fetched_at < end)
        ).scalars().all()
        lines.append(f"  total: {len(all_errors)}")
        for row in all_errors:
            lines.append(f"  [{row.severity}] [{row.component}] {row.error_message[:200]}")
        lines.append("")

        lines.append("-- Rate-limit incidents per account --")
        for account in ("acct1", "acct2"):
            hits = [
                row for row in all_errors
                if row.component == account
                and any(k in row.error_message.lower() for k in ("429", "too many requests", "rate limit"))
            ]
            lines.append(f"  {account}: {len(hits)} incident(s)")

    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="YYYY-MM-DD (IST), defaults to today", default=None)
    args = parser.parse_args()

    day = datetime.now(IST).date() if not args.date else datetime.fromisoformat(args.date).date()
    report = generate_report(day)
    print(report)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    out_path = LOG_DIR / f"daily_report_{day.isoformat()}.txt"
    out_path.write_text(report)
    print(f"\nWritten to {out_path}")
