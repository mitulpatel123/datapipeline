"""Dhan Account 1: real-time / high-frequency connector.

Every Dhan REST call goes through connectors.dhan_request_manager -- no direct SDK
calls here. That manager owns the account-level token bucket, the option-chain-family
and marketquote-family minimum intervals, and the circuit breaker that trips on 429 /
soft-ban signals. See that module's docstring for the full rationale.
"""
import logging
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from dhanhq import DhanContext, dhanhq

from config import settings
from connectors.instrument_master import (
    HEAVYWEIGHT_STOCKS,
    NIFTY50_EXCHANGE_SEGMENT,
    NIFTY50_SECURITY_ID,
    resolve_security_id,
)
from connectors.dhan_request_manager import get_request_manager
from storage import redis_client
from storage.ingest import log_and_alert, store_option_chain_snapshot, store_tick_rows
from storage.postgres_client import get_session

logger = logging.getLogger(__name__)

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


def select_nearest_valid_expiry(expiries: list[str], today: date | None = None) -> str | None:
    """Never blindly trust expiries[0] -- parse, sort, and pick the nearest expiry
    that is today or later in IST. Returns None (caller must alert + skip) if nothing
    valid is found."""
    today = today or datetime.now(IST).date()
    parsed = []
    for raw in expiries:
        try:
            parsed.append(datetime.strptime(raw, "%Y-%m-%d").date())
        except ValueError:
            continue
    valid = sorted(d for d in parsed if d >= today)
    return valid[0].isoformat() if valid else None


class DhanAccount1:
    def __init__(self):
        self.client_id = settings.DHAN_CLIENT_ID_1
        self.access_token = settings.DHAN_ACCESS_TOKEN_1
        self.context = DhanContext(self.client_id, self.access_token)
        self.client = dhanhq(self.context)
        self.source_account = "acct1"
        self.request_manager = get_request_manager(self.source_account)
        self._stock_ids = None

    @property
    def stock_security_ids(self) -> dict:
        if self._stock_ids is None:
            self._stock_ids = {
                symbol: resolve_security_id(symbol, exch_id="NSE", segment="E", instrument_type="ES")
                for symbol in HEAVYWEIGHT_STOCKS
            }
        return self._stock_ids

    def get_expiry_list(self) -> list[str]:
        # dhanhq SDK nests Dhan's raw JSON body (itself {"data": ..., "status": ...}) under
        # response["data"] -- so the actual payload is response["data"]["data"]. Expiry list
        # is part of the option-chain endpoint family and shares its limiter/breaker so a
        # startup/hourly refresh can't collide with an active option-chain fetch.
        response = self.request_manager.call(
            "optionchain", "expiry_list", self.client.expiry_list, int(NIFTY50_SECURITY_ID), NIFTY50_EXCHANGE_SEGMENT
        )
        return response["data"]["data"]

    def fetch_option_chain(self, expiry: str) -> dict:
        response = self.request_manager.call(
            "optionchain", "option_chain", self.client.option_chain,
            int(NIFTY50_SECURITY_ID), NIFTY50_EXCHANGE_SEGMENT, expiry,
        )
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
                        "security_id": leg.get("security_id"),
                    }
                )

        with get_session() as session:
            stored, rejected = store_option_chain_snapshot(session, rows, self.source_account)

        redis_client.set_latest(
            f"nifty:optionchain:{expiry}:latest",
            {"fetched_at": fetched_at.isoformat(), "underlying_ltp": underlying_ltp, "rows": rows},
        )
        redis_client.mark_write(f"option_chain_snapshots:{self.source_account}")
        return stored, rejected

    def fetch_market_quote_reconciliation(self) -> tuple[int, int]:
        """NIFTY + 5 heavyweight stocks + (if resolvable) nearest NIFTY futures contract,
        all in ONE marketfeed/quote call, stored to tick_data tagged 'acct1_quote' so it
        can be cross-checked against the websocket ticks. This is also the single
        centralized place that fetches the futures quote -- derived_metrics.compute_futures_basis
        reads the Redis cache populated here instead of calling Dhan itself, so that job
        can never create a second, colliding /marketfeed/quote call (see spec item 2)."""
        securities = {
            NIFTY50_EXCHANGE_SEGMENT: [int(NIFTY50_SECURITY_ID)],
            "NSE_EQ": [int(sid) for sid in self.stock_security_ids.values()],
        }
        id_to_symbol = {v: k for k, v in self.stock_security_ids.items()}
        id_to_symbol[NIFTY50_SECURITY_ID] = "NIFTY"

        futures_security_id = None
        try:
            from connectors.instrument_master import resolve_nearest_future

            futures_security_id = resolve_nearest_future("NIFTY", exch_id="NSE", segment="D")
        except Exception:
            logger.warning("Could not resolve nearest NIFTY futures contract", exc_info=True)
        if futures_security_id:
            securities["NSE_FNO"] = [int(futures_security_id)]
            id_to_symbol[futures_security_id] = "NIFTY_FUT"

        response = self.request_manager.call("marketquote", "quote_data", self.client.quote_data, securities)

        fetched_at = datetime.now(timezone.utc)
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
                if symbol == "NIFTY_FUT" and quote.get("last_price"):
                    redis_client.set_latest(
                        "nifty:futures:nearest:latest",
                        {
                            "security_id": str(security_id),
                            "ltp": float(quote["last_price"]),
                            "fetched_at": fetched_at.isoformat(),
                        },
                        ttl=300,
                    )

        with get_session() as session:
            stored, rejected = store_tick_rows(session, rows, f"{self.source_account}_quote")
        return stored, rejected

    def fetch_quote_for(self, exchange_segment: str, security_ids: list[int]) -> dict:
        """Generic single-call quote fetch (e.g. nearest futures contract) routed through
        the same marketquote family limiter/breaker as reconciliation -- callers must NOT
        call self.client.quote_data directly."""
        response = self.request_manager.call(
            "marketquote", "quote_data", self.client.quote_data, {exchange_segment: security_ids}
        )
        return response["data"]["data"]
