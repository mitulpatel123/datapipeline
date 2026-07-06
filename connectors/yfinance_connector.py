"""Global indices, commodities, and USD/INR via yfinance.

USD/INR prefers Dhan's nearest-expiry USDINR futures contract (per spec: keep it in
one system), but ONLY outside market hours -- during market hours Dhan's REST quota
is already spent on option chain + quote reconciliation, and this job runs on its
own 5-minute schedule, so calling Dhan here too would be an uncoordinated collision
the circuit breaker can't see coming. yfinance USDINR=X is used unconditionally
during market hours, and as the fallback any time the Dhan path fails (NSE has no
standalone USD/INR "spot" instrument, only options and futures, so there's a real
roll-over dependency there that yfinance sidesteps entirely).
"""
import logging
from datetime import datetime, timezone

import yfinance as yf

from storage import redis_client
from storage.postgres_client import get_session
from storage.postgres_models import GlobalIndex
from utils.time_utils import is_market_hours_ist

logger = logging.getLogger(__name__)

YFINANCE_SYMBOLS = {
    "SPX": "^GSPC",
    "DOW_FUTURES": "YM=F",
    "NASDAQ": "^IXIC",
    "US_VIX": "^VIX",
    "NIKKEI": "^N225",
    "HANG_SENG": "^HSI",
    "BRENT_CRUDE": "BZ=F",
    "WTI_CRUDE": "CL=F",
    "GOLD": "GC=F",
}

USDINR_YFINANCE_TICKER = "USDINR=X"


def _fetch_usdinr_via_dhan(dhan_account1) -> tuple[float, float | None] | None:
    """Best-effort primary path, only ever called outside market hours (see module
    docstring). Returns (value, prev_close) or None to trigger the yfinance fallback."""
    try:
        from connectors.instrument_master import resolve_nearest_future

        security_id = resolve_nearest_future("USDINR", exch_id="NSE", segment="C")
        if not security_id:
            return None

        data = dhan_account1.fetch_quote_for("NSE_CURRENCY", [int(security_id)])
        quote = data["NSE_CURRENCY"][str(security_id)]
        value = quote.get("last_price")
        prev_close = quote.get("ohlc", {}).get("close")
        if not value:
            return None
        return float(value), (float(prev_close) if prev_close else None)
    except Exception:
        logger.warning("Dhan USD/INR futures lookup failed, falling back to yfinance", exc_info=True)
        return None


def _fetch_usdinr_via_yfinance() -> tuple[float, float | None] | None:
    try:
        info = yf.Ticker(USDINR_YFINANCE_TICKER).fast_info
        value = info.get("lastPrice")
        prev_close = info.get("previousClose")
        if value is None:
            return None
        return float(value), (float(prev_close) if prev_close else None)
    except Exception:
        logger.exception("yfinance USD/INR fetch failed")
        return None


def fetch_and_store_global_indices(dhan_account1=None) -> int:
    fetched_at = datetime.now(timezone.utc)
    stored = 0

    with get_session() as session:
        for name, ticker in YFINANCE_SYMBOLS.items():
            try:
                info = yf.Ticker(ticker).fast_info
                value = info.get("lastPrice")
                prev_close = info.get("previousClose")
            except Exception:
                logger.exception("yfinance fetch failed for %s (%s)", name, ticker)
                continue
            if value is None:
                continue
            _store_index_row(session, name, float(value), prev_close, fetched_at, "external")
            stored += 1

        usdinr = None
        source = "external"
        if dhan_account1 is not None and not is_market_hours_ist():
            usdinr = _fetch_usdinr_via_dhan(dhan_account1)
            source = "acct1"
        if usdinr is None:
            usdinr = _fetch_usdinr_via_yfinance()
            source = "external"
        if usdinr is not None:
            value, prev_close = usdinr
            _store_index_row(session, "USDINR", value, prev_close, fetched_at, source)
            stored += 1

    if stored:
        redis_client.mark_write("global_indices")
    return stored


def _store_index_row(session, symbol: str, value: float, prev_close, fetched_at, source_account: str):
    change_pct = ((value - prev_close) / prev_close * 100) if prev_close else None
    dedupe_key = f"globalindex:{symbol}:{fetched_at.strftime('%Y%m%d%H%M')}"
    if redis_client.is_duplicate(dedupe_key):
        return
    session.add(
        GlobalIndex(
            fetched_at=fetched_at,
            source_account=source_account,
            symbol=symbol,
            value=value,
            change_pct=change_pct,
        )
    )
    redis_client.set_latest(
        f"nifty:global:{symbol}:latest",
        {"value": value, "change_pct": change_pct, "fetched_at": fetched_at.isoformat()},
    )
