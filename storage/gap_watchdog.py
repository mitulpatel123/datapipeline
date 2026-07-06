"""Data gap watchdog: alerts when a data type hasn't had a successful write in
longer than its expected interval x 3 (spec section 6.2). Every ingest path calls
redis_client.mark_write(data_type) on success, so this only has to read that.
"""
import logging
from datetime import datetime, timezone

from alerts.telegram_alert import send_telegram_alert
from storage import redis_client
from storage.postgres_client import get_session
from storage.postgres_models import DataGapLog

logger = logging.getLogger(__name__)

EXPECTED_INTERVALS_SECONDS = {
    "option_chain_snapshots": 3,
    "tick_data": 5,
    "global_indices": 300,
    "derived_analytics": 3,
    "news_sentiment": 300,
    "instrument_master": 86400,
}
STALE_MULTIPLIER = 3


def check_gaps() -> list[str]:
    now = datetime.now(timezone.utc)
    fired = []
    with get_session() as session:
        for data_type, expected_interval in EXPECTED_INTERVALS_SECONDS.items():
            raw = redis_client.client.get(f"nifty:last_successful_write:{data_type}")
            if raw is None:
                continue  # never written yet this run -- not a gap, just not started
            last_write = datetime.fromtimestamp(float(raw), tz=timezone.utc)
            gap_seconds = (now - last_write).total_seconds()
            threshold = expected_interval * STALE_MULTIPLIER
            if gap_seconds > threshold:
                session.add(
                    DataGapLog(
                        fetched_at=now,
                        expected_fetch_time=last_write,
                        actual_fetch_time=None,
                        data_type=data_type,
                        gap_seconds=gap_seconds,
                    )
                )
                fired.append(
                    f"[data-pipeline] gap detected: {data_type} stale for {int(gap_seconds)}s "
                    f"(expected every {expected_interval}s)"
                )
    for message in fired:
        send_telegram_alert(message)
    return fired
