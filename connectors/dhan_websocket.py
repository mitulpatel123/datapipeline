"""Dhan Account 1 live tick feed: NIFTY 50 + 5 heavyweight stocks, Full packet mode.

Uses the official dhanhq MarketFeed websocket client. The SDK's own reconnect loop
(in MarketFeed._run_async) already retries every ~1s when the socket drops, so we
don't reimplement reconnection -- we only track downtime for alerting purposes.
"""
import logging
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from dhanhq import DhanContext, MarketFeed

from alerts.telegram_alert import send_telegram_alert
from config import settings
from connectors.instrument_master import HEAVYWEIGHT_STOCKS, NIFTY50_SECURITY_ID, resolve_security_id
from storage import redis_client
from storage.ingest import store_tick_rows
from storage.postgres_client import get_session

logger = logging.getLogger(__name__)

EXCHANGE_IDX = 0
EXCHANGE_NSE_EQ = 1
PACKET_QUOTE = 17
PACKET_FULL = 21
DISCONNECT_ALERT_THRESHOLD_SECONDS = 120
IST = ZoneInfo("Asia/Kolkata")


class _PatchedMarketFeed(MarketFeed):
    """The stock SDK's utc_time() does utcfromtimestamp(...).strftime('%H:%M:%S'), which
    both drops the date AND mislabels the value as UTC. Verified live: Dhan's LTT epoch is
    offset from true UTC epoch by exactly +5:30 -- it's IST wall-clock time computed as if
    it were a UTC epoch (a vendor bug), not a real Unix timestamp. So interpreting the raw
    epoch as UTC and reading off its HH:MM:SS actually gives the correct IST wall-clock
    reading directly; it must be tagged IST, not UTC, or every tick timestamp is off by 5.5h."""

    def utc_time(self, epoch_time):
        return datetime.fromtimestamp(epoch_time, tz=timezone.utc).replace(tzinfo=IST)


class DhanWebSocketClient:
    def __init__(self):
        self.client_id = settings.DHAN_CLIENT_ID_1
        self.access_token = settings.DHAN_ACCESS_TOKEN_1
        self.context = DhanContext(self.client_id, self.access_token)
        self.source_account = "acct1_ws"
        self._security_id_to_symbol = None
        self._down_since = None
        self._alerted_down = False
        self._feed = None
        self._thread = None

    @property
    def security_id_to_symbol(self) -> dict:
        if self._security_id_to_symbol is None:
            mapping = {NIFTY50_SECURITY_ID: "NIFTY"}
            for symbol in HEAVYWEIGHT_STOCKS:
                sid = resolve_security_id(symbol, exch_id="NSE", segment="E", instrument_type="ES")
                mapping[sid] = symbol
            self._security_id_to_symbol = mapping
        return self._security_id_to_symbol

    def _build_instruments(self) -> list[tuple[int, str, int]]:
        # Dhan silently drops Full-mode (21) subscriptions for index instruments (no order
        # book) -- verified live: NIFTY only produces data at Quote mode (17). Equities support
        # Full mode fine, so they get depth data too.
        instruments = [(EXCHANGE_IDX, NIFTY50_SECURITY_ID, PACKET_QUOTE)]
        for sid, symbol in self.security_id_to_symbol.items():
            if sid == NIFTY50_SECURITY_ID:
                continue
            instruments.append((EXCHANGE_NSE_EQ, sid, PACKET_FULL))
        return instruments

    def _on_connect(self, _instance):
        if self._down_since is not None:
            logger.info("Dhan websocket reconnected after %.0fs", time.monotonic() - self._down_since)
        self._down_since = None
        self._alerted_down = False

    def _on_close(self, _instance):
        if self._down_since is None:
            self._down_since = time.monotonic()
        logger.warning("Dhan websocket connection closed")

    def _on_error(self, _instance, error):
        logger.error("Dhan websocket error: %s", error)
        if self._down_since is None:
            self._down_since = time.monotonic()
        downtime = time.monotonic() - self._down_since
        if downtime > DISCONNECT_ALERT_THRESHOLD_SECONDS and not self._alerted_down:
            self._alerted_down = True
            send_telegram_alert(
                f"[data-pipeline] {self.source_account}: websocket down for over "
                f"{int(downtime)}s. Last error: {error}"
            )

    def _on_message(self, _instance, data):
        if not data or "security_id" not in data:
            return
        sid = str(data["security_id"])
        symbol = self.security_id_to_symbol.get(sid, sid)
        depth = data.get("depth")
        bid_depth = ask_depth = None
        if depth:
            bid_depth = [{"price": d["bid_price"], "quantity": d["bid_quantity"], "orders": d["bid_orders"]} for d in depth]
            ask_depth = [{"price": d["ask_price"], "quantity": d["ask_quantity"], "orders": d["ask_orders"]} for d in depth]

        ltt = data.get("LTT")
        row = {
            "fetched_at": datetime.now(timezone.utc),
            "security_id": sid,
            "symbol": symbol,
            "ltp": float(data["LTP"]) if data.get("LTP") not in (None, "") else None,
            "ltt": ltt,
            "volume": data.get("volume"),
            "oi": data.get("OI"),
            "bid_depth": bid_depth,
            "ask_depth": ask_depth,
        }
        with get_session() as session:
            store_tick_rows(session, [row], self.source_account)
        redis_client.set_latest(f"nifty:tick:{sid}:latest", row, ttl=redis_client.TICK_TTL_SECONDS)

    def start(self):
        """Starts the websocket in a background thread. Returns the thread object."""
        instruments = self._build_instruments()
        self._feed = _PatchedMarketFeed(
            self.context,
            instruments,
            version="v2",
            on_connect=self._on_connect,
            on_message=self._on_message,
            on_close=self._on_close,
            on_error=self._on_error,
        )
        self._thread = self._feed.start()
        return self._thread

    def stop(self):
        if self._feed:
            self._feed.close_connection()
