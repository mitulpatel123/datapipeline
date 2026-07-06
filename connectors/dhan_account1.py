"""Dhan Account 1: real-time / high-frequency connector.

Every single call (option chain, expiry list, market quote) is routed through one
shared TokenBucketRateLimiter -- Dhan's 5 req/sec limit is global per account across
every endpoint, not per endpoint. Option chain additionally respects the documented
"1 unique request per 3 seconds" rule via MinIntervalLimiter.
"""
import logging
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from dhanhq import DhanContext, dhanhq

from config import settings
from connectors.instrument_master import (
    HEAVYWEIGHT_STOCKS,
    NIFTY50_EXCHANGE_SEGMENT,
    NIFTY50_SECURITY_ID,
    resolve_security_id,
)
from connectors.rate_limiter import MinIntervalLimiter, TokenBucketRateLimiter
from storage import redis_client
from storage.ingest import log_and_alert, store_option_chain_snapshot, store_tick_rows
from storage.postgres_client import get_session

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
BACKOFF_CAP_SECONDS = 4
IST = ZoneInfo("Asia/Kolkata")


def _parse_ltt(value):
    """Dhan's marketfeed/quote returns last_trade_time as 'DD/MM/YYYY HH:MM:SS' IST wall-clock,
    not ISO and not tz-aware. Must tag it IST explicitly -- otherwise Postgres/psycopg2 stores
    the naive value using the connecting machine's local timezone, silently corrupting it (this
    pipeline runs from a US-based machine while all Dhan timestamps are IST)."""
    if not value:
        return None
    try:
        naive = datetime.strptime(value, "%d/%m/%Y %H:%M:%S")
        return naive.replace(tzinfo=IST)
    except ValueError:
        return None


class DhanAccount1:
    def __init__(self):
        self.client_id = settings.DHAN_CLIENT_ID_1
        self.access_token = settings.DHAN_ACCESS_TOKEN_1
        self.context = DhanContext(self.client_id, self.access_token)
        self.client = dhanhq(self.context)
        self.limiter = TokenBucketRateLimiter(rate_per_second=settings.DHAN_MAX_REQUESTS_PER_SECOND)
        self.optionchain_interval = MinIntervalLimiter(settings.DHAN_OPTIONCHAIN_MIN_INTERVAL_SECONDS)
        self.source_account = "acct1"
        self._stock_ids = None

    @property
    def stock_security_ids(self) -> dict:
        if self._stock_ids is None:
            self._stock_ids = {
                symbol: resolve_security_id(symbol, exch_id="NSE", segment="E", instrument_type="ES")
                for symbol in HEAVYWEIGHT_STOCKS
            }
        return self._stock_ids

    def _call(self, fn, *args, **kwargs):
        delay = 1
        last_response = None
        for attempt in range(MAX_RETRIES + 1):
            self.limiter.acquire()
            last_response = fn(*args, **kwargs)
            if last_response.get("status") == "success":
                return last_response
            logger.warning(
                "%s Dhan call failed (attempt %d/%d): %s",
                self.source_account, attempt + 1, MAX_RETRIES + 1, last_response.get("remarks"),
            )
            if attempt < MAX_RETRIES:
                time.sleep(delay)
                delay = min(delay * 2, BACKOFF_CAP_SECONDS)

        remarks = last_response.get("remarks") if last_response else "no response"
        log_and_alert(
            self.source_account,
            f"{self.source_account}: Dhan API call failed after {MAX_RETRIES + 1} attempts. "
            f"Details: {remarks}",
        )
        raise RuntimeError(f"Dhan API call failed on {self.source_account}: {remarks}")

    def get_expiry_list(self) -> list[str]:
        # dhanhq SDK nests Dhan's raw JSON body (itself {"data": ..., "status": ...}) under
        # response["data"] -- so the actual payload is response["data"]["data"].
        response = self._call(self.client.expiry_list, int(NIFTY50_SECURITY_ID), NIFTY50_EXCHANGE_SEGMENT)
        return response["data"]["data"]

    def fetch_option_chain(self, expiry: str) -> dict:
        self.optionchain_interval.acquire(f"optionchain:{expiry}")
        response = self._call(self.client.option_chain, int(NIFTY50_SECURITY_ID), NIFTY50_EXCHANGE_SEGMENT, expiry)
        return response["data"]["data"]

    def fetch_and_store_option_chain(self, expiry: str) -> tuple[int, int]:
        data = self.fetch_option_chain(expiry)
        fetched_at = datetime.now(timezone.utc)
        underlying_ltp = data.get("last_price")
        rows = []
        for strike_str, sides in data.get("oc", {}).items():
            for option_type, key in (("CE", "ce"), ("PE", "pe")):
                leg = sides.get(key)
                if not leg:
                    continue
                greeks = leg.get("greeks", {})
                rows.append(
                    {
                        "fetched_at": fetched_at,
                        "expiry": expiry,
                        "strike": float(strike_str),
                        "option_type": option_type,
                        "ltp": leg.get("last_price"),
                        "oi": leg.get("oi"),
                        "prev_oi": leg.get("previous_oi"),
                        "volume": leg.get("volume"),
                        "iv": leg.get("implied_volatility"),
                        "delta": greeks.get("delta"),
                        "theta": greeks.get("theta"),
                        "gamma": greeks.get("gamma"),
                        "vega": greeks.get("vega"),
                        "bid": leg.get("top_bid_price"),
                        "ask": leg.get("top_ask_price"),
                        "underlying_ltp": underlying_ltp,
                    }
                )

        with get_session() as session:
            stored, rejected = store_option_chain_snapshot(session, rows, self.source_account)

        redis_client.set_latest(
            f"nifty:optionchain:{expiry}:latest",
            {"fetched_at": fetched_at.isoformat(), "underlying_ltp": underlying_ltp, "rows": rows},
        )
        return stored, rejected

    def fetch_market_quote_reconciliation(self) -> tuple[int, int]:
        """NIFTY + 5 heavyweight stocks in one marketfeed/quote call, stored to tick_data
        tagged 'acct1_quote' so it can be cross-checked against the websocket ticks."""
        securities = {
            NIFTY50_EXCHANGE_SEGMENT: [int(NIFTY50_SECURITY_ID)],
            "NSE_EQ": [int(sid) for sid in self.stock_security_ids.values()],
        }
        response = self._call(self.client.quote_data, securities)

        fetched_at = datetime.now(timezone.utc)
        id_to_symbol = {v: k for k, v in self.stock_security_ids.items()}
        id_to_symbol[NIFTY50_SECURITY_ID] = "NIFTY"

        rows = []
        for _segment, by_id in response["data"]["data"].items():
            for security_id, quote in by_id.items():
                symbol = id_to_symbol.get(str(security_id), str(security_id))
                depth = quote.get("depth", {})
                rows.append(
                    {
                        "fetched_at": fetched_at,
                        "security_id": str(security_id),
                        "symbol": symbol,
                        "ltp": quote.get("last_price"),
                        "ltt": _parse_ltt(quote.get("last_trade_time")),
                        "volume": quote.get("volume"),
                        "oi": quote.get("oi"),
                        "bid_depth": depth.get("buy"),
                        "ask_depth": depth.get("sell"),
                    }
                )

        with get_session() as session:
            stored, rejected = store_tick_rows(session, rows, f"{self.source_account}_quote")
        return stored, rejected
