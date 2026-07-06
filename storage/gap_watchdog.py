"""Data gap watchdog: alerts when a data type hasn't had a successful write in
longer than its expected interval x 3, OR has never written anything at all past a
startup grace period (spec items 14-17). Every ingest path calls
redis_client.mark_write(data_type) on success -- this only reads that.

Per-source/instrument keys (e.g. tick_data:acct1_ws:NIFTY vs
tick_data:acct1_quote:NIFTY) let this tell a dead websocket apart from a still-
healthy quote-reconciliation feed even though both write to the same tick_data
table. VIX/GIFT/FII-DII are intentionally absent from WATCHED_KEYS -- they're
blocked stubs (see connectors/scraper_*.py) and were never scheduled, so there's
nothing to falsely flag as a gap (spec item 37).

Optional integrations (currently just Marketaux news) are tracked in
OPTIONAL_STREAMS: a stream only enters the watch list if its enabled-check passes,
so a deliberately-unconfigured optional API never produces a false "never started"
alert. The daily report uses the same registry to list disabled streams explicitly
rather than silently omitting them.

Dynamic option-chain strikes are intentionally NOT watched here -- the ATM window
shifts through the day (see connectors/dhan_websocket.py refresh_option_universe),
so a fixed per-strike gap key would misfire constantly as strikes roll in and out.
"""
import logging
import time
from datetime import datetime, timedelta, timezone

from alerts.telegram_alert import send_telegram_alert
from config import settings
from storage import redis_client
from storage.postgres_client import get_session
from storage.postgres_models import DataGapLog

logger = logging.getLogger(__name__)

STALE_MULTIPLIER = 3
_process_start_monotonic = time.monotonic()

HEAVYWEIGHT_SYMBOLS = ("RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS")

# name -> callable returning whether this optional stream is currently enabled.
OPTIONAL_STREAMS = {
    "news_sentiment": lambda: bool(settings.MARKETAUX_API_KEY),
}

# (redis_key, expected_interval_seconds, startup_grace_seconds)
_ALWAYS_WATCHED_KEYS = [
    ("option_chain_snapshots:acct1", 3, 15),
    ("tick_data:acct1_ws:NIFTY", 5, 30),
    ("tick_data:acct1_ws:NIFTY_FUT", 5, 30),
    ("tick_data:acct1_quote:NIFTY", 5, 30),
    ("tick_data:acct1_quote:NIFTY_FUT", 5, 30),
    ("derived_analytics", 3, 15),
    ("global_indices", 300, 600),
    ("instrument_master", 86400, 600),
]
for _symbol in HEAVYWEIGHT_SYMBOLS:
    _ALWAYS_WATCHED_KEYS.append((f"tick_data:acct1_ws:{_symbol}", 5, 30))

# Optional-stream watch definitions, keyed by the same name used in OPTIONAL_STREAMS.
_OPTIONAL_WATCHED_KEYS = {
    "news_sentiment": ("news_sentiment", 300, 600),
}


def get_watched_keys() -> list[tuple[str, int, int]]:
    """Built fresh on every call (not a frozen module constant) so tests can
    monkeypatch settings.MARKETAUX_API_KEY (or any future optional-stream flag) and
    see the watch list react without reimporting this module."""
    keys = list(_ALWAYS_WATCHED_KEYS)
    for name, is_enabled in OPTIONAL_STREAMS.items():
        if is_enabled():
            keys.append(_OPTIONAL_WATCHED_KEYS[name])
    return keys


# Backward-compatible module attribute -- tests/callers that iterate WATCHED_KEYS
# directly still see the always-on set; use get_watched_keys() for the live list.
WATCHED_KEYS = _ALWAYS_WATCHED_KEYS


def disabled_streams() -> dict[str, str]:
    """name -> human reason, for streams whose enabled-check currently fails."""
    reasons = {
        "news_sentiment": "MARKETAUX_API_KEY not configured",
    }
    return {name: reasons[name] for name, is_enabled in OPTIONAL_STREAMS.items() if not is_enabled()}


def _cooldown_key(data_type: str) -> str:
    return f"nifty:gap_alert_cooldown:{data_type}"


def _already_alerted_recently(data_type: str) -> bool:
    """Returns True (and does NOT reset the cooldown) if we alerted on this data_type
    within the last GAP_ALERT_COOLDOWN_SECONDS -- prevents the once-a-minute watchdog
    job from re-alerting forever on the same ongoing gap."""
    key = _cooldown_key(data_type)
    acquired = redis_client.client.set(key, "1", ex=settings.GAP_ALERT_COOLDOWN_SECONDS, nx=True)
    return not acquired


def _parse_data_type(data_type: str) -> dict:
    parts = data_type.split(":")
    return {
        "source_account": parts[1] if len(parts) >= 2 else None,
        "symbol": parts[2] if len(parts) >= 3 else None,
    }


def check_gaps() -> list[str]:
    now = datetime.now(timezone.utc)
    uptime_seconds = time.monotonic() - _process_start_monotonic
    fired = []

    with get_session() as session:
        for data_type, expected_interval, grace_seconds in get_watched_keys():
            raw = redis_client.client.get(f"nifty:last_successful_write:{data_type}")
            meta = _parse_data_type(data_type)

            if raw is None:
                if uptime_seconds < grace_seconds:
                    continue  # still within startup grace period, not a gap yet
                if _already_alerted_recently(data_type):
                    continue
                gap_seconds = uptime_seconds
                expected_fetch_time = now
                severity = "critical"
                message = (
                    f"gap detected: {data_type} has NEVER written data "
                    f"{int(uptime_seconds)}s into this run (grace period was {grace_seconds}s)"
                )
            else:
                last_write = datetime.fromtimestamp(float(raw), tz=timezone.utc)
                gap_seconds = (now - last_write).total_seconds()
                threshold = expected_interval * STALE_MULTIPLIER
                if gap_seconds <= threshold:
                    continue
                if _already_alerted_recently(data_type):
                    continue
                expected_fetch_time = last_write + timedelta(seconds=expected_interval)
                severity = "warning"
                message = (
                    f"gap detected: {data_type} stale for {int(gap_seconds)}s "
                    f"(expected every {expected_interval}s)"
                )

            session.add(
                DataGapLog(
                    fetched_at=now,
                    source_account=meta["source_account"],
                    symbol=meta["symbol"],
                    expected_fetch_time=expected_fetch_time,
                    actual_fetch_time=now,
                    data_type=data_type,
                    gap_seconds=gap_seconds,
                    severity=severity,
                )
            )
            fired.append(f"[data-pipeline] {message}")

    for message in fired:
        send_telegram_alert(message)
    return fired
