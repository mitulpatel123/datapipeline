"""Dhan Account 2: historical / reference connector (lower frequency by design).

Uses its own DhanRequestManager instance (get_request_manager("acct2")) -- never
shared with Account 1's -- so if Account 1 gets throttled, this account keeps
historical backfill running independently.
"""
import logging
from datetime import datetime, timezone

from dhanhq import DhanContext, dhanhq
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import settings
from connectors.dhan_request_manager import get_request_manager
from connectors.instrument_master import (
    HEAVYWEIGHT_STOCKS,
    NIFTY50_EXCHANGE_SEGMENT,
    NIFTY50_SECURITY_ID,
    download_instrument_master,
    resolve_security_id,
)
from storage import redis_client
from storage.postgres_client import get_session
from storage.postgres_models import OhlcvIntraday

logger = logging.getLogger(__name__)


class DhanAccount2:
    def __init__(self):
        self.client_id = settings.DHAN_CLIENT_ID_2
        self.access_token = settings.DHAN_ACCESS_TOKEN_2
        if not settings.DHAN_ACCOUNT_2_ENABLED:
            raise RuntimeError("DhanAccount2 requires DHAN_CLIENT_ID_2 / DHAN_ACCESS_TOKEN_2 in .env")
        self.context = DhanContext(self.client_id, self.access_token)
        self.client = dhanhq(self.context)
        self.source_account = "acct2"
        self.request_manager = get_request_manager(self.source_account)

    def download_and_cache_instrument_master(self):
        path = download_instrument_master()
        redis_client.mark_write("instrument_master")
        return path

    def fetch_historical_daily(
        self, security_id: str, exchange_segment: str, instrument_type: str,
        from_date: str, to_date: str, expiry_code: int = 0,
    ) -> dict:
        # Unlike optionchain/expirylist/quote, Dhan's raw historical response body IS the
        # candle dict directly (no {"data":..., "status":...} wrapper), so the SDK's
        # response["data"] is already the payload -- no double-nesting here.
        response = self.request_manager.call(
            "historical", "historical_daily_data", self.client.historical_daily_data,
            security_id, exchange_segment, instrument_type, from_date, to_date, expiry_code, True,
        )
        return response["data"]

    def fetch_intraday(
        self, security_id: str, exchange_segment: str, instrument_type: str,
        from_date: str, to_date: str, interval: int = 1,
    ) -> dict:
        response = self.request_manager.call(
            "historical", "intraday_minute_data", self.client.intraday_minute_data,
            security_id, exchange_segment, instrument_type, from_date, to_date, interval, True,
        )
        return response["data"]

    def _store_ohlcv(self, symbol: str, security_id: str, interval: str, candles: dict, fetched_at: datetime) -> int:
        timestamps = candles.get("timestamp", [])
        opens = candles.get("open", [])
        highs = candles.get("high", [])
        lows = candles.get("low", [])
        closes = candles.get("close", [])
        volumes = candles.get("volume", [])
        ois = candles.get("open_interest", [None] * len(timestamps))

        rows = []
        for i, ts in enumerate(timestamps):
            bar_time = datetime.fromtimestamp(ts, tz=timezone.utc)
            rows.append(
                {
                    "fetched_at": fetched_at,
                    "source_account": self.source_account,
                    "security_id": security_id,
                    "symbol": symbol,
                    "interval": interval,
                    "open": opens[i] if i < len(opens) else None,
                    "high": highs[i] if i < len(highs) else None,
                    "low": lows[i] if i < len(lows) else None,
                    "close": closes[i] if i < len(closes) else None,
                    "volume": volumes[i] if i < len(volumes) else None,
                    "oi": ois[i] if i < len(ois) else None,
                    "bar_timestamp": bar_time,
                }
            )
        if not rows:
            return 0

        # Postgres unique constraint (source_account, symbol, interval, bar_timestamp) is the
        # real durability guard here -- Redis dedupe alone expires after 60s, so a rerun of
        # EOD backfill (or any retry) would otherwise insert duplicate bars.
        with get_session() as session:
            stmt = pg_insert(OhlcvIntraday).values(rows)
            stmt = stmt.on_conflict_do_nothing(index_elements=["source_account", "symbol", "interval", "bar_timestamp"])
            result = session.execute(stmt)
            stored = result.rowcount or 0

        if stored:
            redis_client.mark_write(f"ohlcv_intraday:{self.source_account}:{symbol}:{interval}")
        return stored

    def backfill_nifty_daily(self, from_date: str, to_date: str) -> int:
        candles = self.fetch_historical_daily(
            str(int(NIFTY50_SECURITY_ID)), NIFTY50_EXCHANGE_SEGMENT, "INDEX", from_date, to_date,
        )
        return self._store_ohlcv("NIFTY", NIFTY50_SECURITY_ID, "day", candles, datetime.now(timezone.utc))

    def backfill_nifty_intraday(self, from_date: str, to_date: str, interval: int = 1) -> int:
        candles = self.fetch_intraday(
            str(int(NIFTY50_SECURITY_ID)), NIFTY50_EXCHANGE_SEGMENT, "INDEX", from_date, to_date, interval,
        )
        return self._store_ohlcv("NIFTY", NIFTY50_SECURITY_ID, f"{interval}min", candles, datetime.now(timezone.utc))

    def backfill_stock_daily(self, symbol: str, from_date: str, to_date: str) -> int:
        security_id = resolve_security_id(symbol, exch_id="NSE", segment="E", instrument_type="ES")
        candles = self.fetch_historical_daily(security_id, "NSE_EQ", "EQUITY", from_date, to_date)
        return self._store_ohlcv(symbol, security_id, "day", candles, datetime.now(timezone.utc))
