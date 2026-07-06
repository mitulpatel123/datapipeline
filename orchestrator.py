"""Single entry point tying every Phase 1 job into one scheduler process.

Why apscheduler over separate cron jobs (per spec section 5, justification required):
this is one long-running Python process holding live connector state (websocket
connection, rate limiters, cached instrument master, current expiry) that every job
shares. Separate cron jobs would each need their own process startup, couldn't share
the websocket connection or in-process rate limiter state, and would scatter logging/
error handling across N processes instead of one place. BlockingScheduler runs the
scheduler in the main thread (this process's only job is to be the orchestrator), with
job callbacks executed in its internal thread pool.

All schedules are pinned to Asia/Kolkata regardless of the host machine's timezone --
this runs from a US-based machine against an Indian market, and every timestamp bug
found during development (REST last_trade_time, websocket LTT epoch) came from exactly
this kind of timezone mismatch.

Only ONE orchestrator may run at a time (Redis lock, see utils/process_lock.py) --
two instances would double-poll Dhan and risk tripping the circuit breaker for no
reason. Scheduler cadence is controlled by SOAK_MODE (safe/normal/production): start
every soak test on "safe" and only move faster once a session has proven stable
(see README).
"""
import logging
import os
import socket
import sys
import uuid
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler

sys.path.insert(0, str(Path(__file__).resolve().parent))

from analytics.derived_metrics import compute_and_store as compute_derived_analytics
from config import settings
from connectors.dhan_account1 import DhanAccount1, select_nearest_valid_expiry
from connectors.dhan_account2 import DhanAccount2
from connectors.dhan_websocket import DhanWebSocketClient
from connectors.news_connector import fetch_and_store_news
from connectors.yfinance_connector import fetch_and_store_global_indices
from storage import redis_client
from storage.gap_watchdog import check_gaps
from storage.ingest import log_and_alert
from utils.process_lock import OrchestratorLock
from utils.time_utils import IST, is_market_hours_ist, today_ist_iso

LOG_DIR = settings.LOG_DIR
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "orchestrator.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("orchestrator")

SCHEDULER_PROFILES = {
    "safe": {"option_chain_seconds": 6, "quote_reconciliation_seconds": 15},
    "normal": {"option_chain_seconds": 4, "quote_reconciliation_seconds": 10},
    "production": {"option_chain_seconds": 3.3, "quote_reconciliation_seconds": 7},
}
ORCHESTRATOR_LOCK_KEY = "nifty:orchestrator:lock"

acct1 = DhanAccount1()
acct2 = DhanAccount2() if settings.DHAN_ACCOUNT_2_ENABLED else None
ws_client = DhanWebSocketClient()

_state = {"current_expiry": None}


def guarded(job_name: str):
    """Every job is isolated: an exception here must not crash the scheduler thread,
    but must be logged to system_errors and alerted (spec section 6.2)."""

    def decorator(fn):
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                logger.exception("Job %s failed", job_name)
                log_and_alert(job_name, f"job '{job_name}' raised: {exc}")

        wrapper.__name__ = fn.__name__
        return wrapper

    return decorator


@guarded("refresh_expiry_list")
def refresh_expiry_list():
    expiries = acct1.get_expiry_list()
    nearest = select_nearest_valid_expiry(expiries)
    if nearest is None:
        log_and_alert("refresh_expiry_list", f"No valid expiry >= today found in {expiries!r} -- option chain job will be skipped")
        _state["current_expiry"] = None
        return
    _state["current_expiry"] = nearest
    logger.info("Nearest expiry set to %s", nearest)


@guarded("option_chain_fetch")
def option_chain_job():
    if not is_market_hours_ist():
        return
    expiry = _state["current_expiry"]
    if not expiry:
        return
    stored, rejected = acct1.fetch_and_store_option_chain(expiry)
    compute_derived_analytics(expiry)

    snapshot = redis_client.get_latest(f"nifty:optionchain:{expiry}:latest")
    if snapshot:
        ws_client.refresh_option_universe(snapshot["rows"], snapshot["underlying_ltp"], expiry)

    logger.debug("option_chain_job: stored=%d rejected=%d", stored, rejected)


@guarded("quote_reconciliation")
def quote_reconciliation_job():
    if not is_market_hours_ist():
        return
    acct1.fetch_market_quote_reconciliation()


@guarded("global_indices")
def global_indices_job():
    fetch_and_store_global_indices(dhan_account1=acct1)


@guarded("news_sentiment")
def news_job():
    if not settings.MARKETAUX_API_KEY:
        return
    fetch_and_store_news()


@guarded("gap_watchdog")
def gap_watchdog_job():
    check_gaps()


@guarded("websocket_start")
def websocket_start_job():
    if not is_market_hours_ist():
        return
    ws_client.start()
    logger.info("Websocket started (state=%s)", ws_client.state)


@guarded("websocket_stop")
def websocket_stop_job():
    import threading

    def _stop():
        try:
            ws_client.stop()
        except Exception:
            logger.warning("websocket stop() did not complete cleanly", exc_info=True)

    threading.Thread(target=_stop, daemon=True).start()
    logger.info("Websocket stop requested")


@guarded("instrument_master_refresh")
def instrument_master_refresh_job():
    if acct2:
        acct2.download_and_cache_instrument_master()
    else:
        from connectors.instrument_master import download_instrument_master

        download_instrument_master()


@guarded("eod_historical_backfill")
def eod_backfill_job():
    if not acct2:
        logger.warning("Account 2 not configured, skipping EOD backfill")
        return
    today = today_ist_iso()
    acct2.backfill_nifty_daily(today, today)
    acct2.backfill_nifty_intraday(today, today, interval=5)
    for symbol in ("RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS"):
        acct2.backfill_stock_daily(symbol, today, today)


def build_scheduler() -> BlockingScheduler:
    scheduler = BlockingScheduler(timezone=IST)
    profile = SCHEDULER_PROFILES.get(settings.SOAK_MODE, SCHEDULER_PROFILES["safe"])
    logger.info("Scheduler profile: SOAK_MODE=%s -> %s", settings.SOAK_MODE, profile)

    common = dict(max_instances=1, coalesce=True)

    scheduler.add_job(
        option_chain_job, "interval", seconds=profile["option_chain_seconds"],
        id="option_chain", misfire_grace_time=5, **common,
    )
    scheduler.add_job(
        quote_reconciliation_job, "interval", seconds=profile["quote_reconciliation_seconds"],
        id="quote_reconciliation", misfire_grace_time=10, **common,
    )
    scheduler.add_job(global_indices_job, "interval", minutes=5, id="global_indices", misfire_grace_time=60, **common)
    scheduler.add_job(news_job, "interval", minutes=5, id="news_sentiment", misfire_grace_time=60, **common)
    scheduler.add_job(gap_watchdog_job, "interval", minutes=1, id="gap_watchdog", misfire_grace_time=30, **common)
    scheduler.add_job(refresh_expiry_list, "interval", hours=1, id="expiry_list_refresh", misfire_grace_time=300, **common)

    scheduler.add_job(
        websocket_start_job, "cron", day_of_week="mon-fri", hour=9, minute=0,
        id="ws_start", misfire_grace_time=300, **common,
    )
    scheduler.add_job(
        websocket_stop_job, "cron", day_of_week="mon-fri", hour=15, minute=35,
        id="ws_stop", misfire_grace_time=300, **common,
    )
    scheduler.add_job(
        instrument_master_refresh_job, "cron", day_of_week="mon-fri", hour=8, minute=0,
        id="instrument_master", misfire_grace_time=1800, **common,
    )
    scheduler.add_job(
        eod_backfill_job, "cron", day_of_week="mon-fri", hour=16, minute=0,
        id="eod_backfill", misfire_grace_time=1800, **common,
    )

    # VIX / GIFT Nifty / FII-DII scrapers intentionally NOT scheduled -- blocked on
    # Section 2 manually-sourced endpoints (see connectors/scraper_*.py stubs).

    return scheduler


def main():
    logger.info("Orchestrator starting (SOAK_MODE=%s)", settings.SOAK_MODE)

    owner_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    lock = OrchestratorLock(redis_client.client, ORCHESTRATOR_LOCK_KEY, settings.ORCHESTRATOR_LOCK_TTL_SECONDS, owner_id)
    if not lock.acquire():
        msg = (
            "Another orchestrator instance appears to already be running (Redis lock "
            f"'{ORCHESTRATOR_LOCK_KEY}' is held) -- exiting to avoid double-polling Dhan."
        )
        logger.error(msg)
        log_and_alert("orchestrator_lock", msg, severity="critical")
        sys.exit(1)
    lock.start_heartbeat()

    try:
        refresh_expiry_list()
        if is_market_hours_ist():
            websocket_start_job()

        scheduler = build_scheduler()
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Orchestrator shutting down")
        except Exception as exc:
            logger.exception("Orchestrator crashed")
            log_and_alert("orchestrator", f"ORCHESTRATOR CRASHED: {exc}", severity="critical")
            raise
    finally:
        lock.release()


if __name__ == "__main__":
    main()
