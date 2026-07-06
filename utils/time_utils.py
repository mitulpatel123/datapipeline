"""Single source of truth for IST time -- every market-data timestamp/date decision
in this pipeline must go through here. Never call date.today() or datetime.now()
directly for anything market-related: the host machine's local timezone is
irrelevant (this runs from the US against an Indian market) and has already caused
real bugs (see connectors/dhan_account1.py, dhan_websocket.py docstrings).
"""
import json
import logging
from datetime import date, datetime
from datetime import time as dtime

from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")

MARKET_OPEN = dtime(9, 0)
MARKET_CLOSE = dtime(15, 35)

_holidays_cache: set[str] | None = None


def now_ist() -> datetime:
    return datetime.now(IST)


def today_ist_date() -> date:
    return now_ist().date()


def today_ist_iso() -> str:
    return today_ist_date().isoformat()


def market_session_bounds_ist(day: date) -> tuple[datetime, datetime]:
    """Returns (market_open, market_close) as tz-aware IST datetimes for the given date."""
    open_dt = datetime.combine(day, MARKET_OPEN, tzinfo=IST)
    close_dt = datetime.combine(day, MARKET_CLOSE, tzinfo=IST)
    return open_dt, close_dt


def _load_holidays() -> set[str]:
    global _holidays_cache
    if _holidays_cache is not None:
        return _holidays_cache
    from config import settings

    path = settings.BASE_DIR / settings.NSE_HOLIDAY_FILE
    if not path.exists():
        logger.warning("NSE holiday file not found at %s -- only weekends will be skipped", path)
        _holidays_cache = set()
        return _holidays_cache
    try:
        data = json.loads(path.read_text())
        _holidays_cache = set(data.get("holidays", []))
    except Exception:
        logger.exception("Failed to parse NSE holiday file %s -- only weekends will be skipped", path)
        _holidays_cache = set()
    return _holidays_cache


def is_market_day_ist(day: date | None = None) -> bool:
    day = day or today_ist_date()
    if day.weekday() >= 5:  # Saturday/Sunday
        return False
    if day.isoformat() in _load_holidays():
        return False
    return True


def is_market_hours_ist(now: datetime | None = None) -> bool:
    now = now or now_ist()
    if not is_market_day_ist(now.date()):
        return False
    return MARKET_OPEN <= now.time() <= MARKET_CLOSE
