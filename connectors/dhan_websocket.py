"""Dhan Account 1 live tick feed: NIFTY 50, 5 heavyweight stocks, nearest NIFTY
futures contract, and (when enabled) a live option-chain universe around ATM.

Uses the official dhanhq MarketFeed websocket client. The SDK's own reconnect loop
(in MarketFeed._run_async) already retries every ~1s when the socket drops, so we
don't reimplement reconnection -- we only track downtime for alerting purposes.

start()/stop() are idempotent and tracked through an explicit state machine
(STOPPED/STARTING/RUNNING/STOPPING/ERROR) so a duplicate start() call (e.g. from a
misfired scheduler job) can never open a second websocket connection.
"""
import logging
import queue
import threading
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from dhanhq import DhanContext, MarketFeed

from config import settings
from connectors.instrument_master import HEAVYWEIGHT_STOCKS, NIFTY50_SECURITY_ID, resolve_nearest_future, resolve_security_id
from storage import redis_client
from storage.ingest import log_and_alert, store_tick_rows
from storage.postgres_client import get_session

logger = logging.getLogger(__name__)

EXCHANGE_IDX = 0
EXCHANGE_NSE_EQ = 1
EXCHANGE_NSE_FNO = 2
PACKET_QUOTE = 17
PACKET_FULL = 21
DISCONNECT_ALERT_THRESHOLD_SECONDS = 120
IST = ZoneInfo("Asia/Kolkata")
BATCH_FLUSH_INTERVAL_SECONDS = 0.75
BATCH_FLUSH_MAX_ROWS = 100


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
    class State:
        STOPPED = "STOPPED"
        STARTING = "STARTING"
        RUNNING = "RUNNING"
        STOPPING = "STOPPING"
        ERROR = "ERROR"

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

        self._state = self.State.STOPPED
        self._state_lock = threading.Lock()

        self._option_instruments: dict[str, tuple[int, str, int]] = {}
        self._current_atm_strike = None
        self._current_expiry = None
        self._universe_lock = threading.Lock()

        # Batched Postgres writer: opening a new session per tick doesn't scale once the
        # option universe (up to ~2*OPTION_WS_ATM_STRIKES_EACH_SIDE+1 strikes) is
        # subscribed alongside NIFTY/futures/stocks. Redis "latest tick" updates stay
        # immediate (cheap); Postgres writes are batched and flushed on a timer/size
        # threshold, and drained fully on stop().
        self._write_queue: queue.Queue = queue.Queue()
        self._batch_thread: threading.Thread | None = None
        self._batch_stop = threading.Event()

    @property
    def state(self) -> str:
        return self._state

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

        try:
            futures_id = resolve_nearest_future("NIFTY", exch_id="NSE", segment="D")
            if futures_id:
                instruments.append((EXCHANGE_NSE_FNO, futures_id, PACKET_FULL))
                self.security_id_to_symbol[futures_id] = "NIFTY_FUT"
        except Exception:
            logger.warning("Could not resolve nearest NIFTY futures contract for websocket base universe", exc_info=True)

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
            log_and_alert(
                self.source_account,
                f"{self.source_account}: websocket down for over {int(downtime)}s. Last error: {error}",
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
        redis_client.set_latest(f"nifty:tick:{sid}:latest", row, ttl=redis_client.TICK_TTL_SECONDS)
        self._write_queue.put(row)

    def _flush_batch(self, rows: list[dict]):
        if not rows:
            return
        try:
            with get_session() as session:
                store_tick_rows(session, rows, self.source_account)
        except Exception:
            logger.exception("Failed to flush websocket tick batch (%d rows)", len(rows))

    def _batch_writer_loop(self):
        buffer: list[dict] = []
        last_flush = time.monotonic()
        while not self._batch_stop.is_set():
            remaining = BATCH_FLUSH_INTERVAL_SECONDS - (time.monotonic() - last_flush)
            try:
                row = self._write_queue.get(timeout=max(0.05, remaining))
                buffer.append(row)
            except queue.Empty:
                pass

            due_by_time = buffer and (time.monotonic() - last_flush) >= BATCH_FLUSH_INTERVAL_SECONDS
            due_by_size = len(buffer) >= BATCH_FLUSH_MAX_ROWS
            if due_by_time or due_by_size:
                self._flush_batch(buffer)
                buffer = []
                last_flush = time.monotonic()

        # Drain whatever is left in the queue and flush before the thread exits.
        while True:
            try:
                buffer.append(self._write_queue.get_nowait())
            except queue.Empty:
                break
        self._flush_batch(buffer)

    def start(self):
        """Idempotent: returns the existing thread if already starting/running instead
        of opening a second websocket connection (spec item 9)."""
        with self._state_lock:
            if self._state in (self.State.RUNNING, self.State.STARTING):
                logger.info("Websocket already %s, start() is a no-op", self._state)
                return self._thread

            self._state = self.State.STARTING
            try:
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

                self._batch_stop.clear()
                self._batch_thread = threading.Thread(target=self._batch_writer_loop, daemon=True)
                self._batch_thread.start()

                self._state = self.State.RUNNING
                return self._thread
            except Exception:
                self._state = self.State.ERROR
                raise

    def stop(self):
        """Idempotent: a no-op if already stopped/stopping."""
        with self._state_lock:
            if self._state in (self.State.STOPPED, self.State.STOPPING):
                logger.info("Websocket already %s, stop() is a no-op", self._state)
                return
            self._state = self.State.STOPPING
            try:
                if self._feed:
                    self._feed.close_connection()
            finally:
                self._batch_stop.set()
                if self._batch_thread:
                    self._batch_thread.join(timeout=10)
                self._batch_thread = None

                self._feed = None
                self._thread = None
                self._option_instruments = {}
                self._current_atm_strike = None
                self._current_expiry = None
                self._state = self.State.STOPPED

    def refresh_option_universe(self, rows: list[dict], underlying_ltp: float, expiry: str):
        """Called after each option-chain fetch. Subscribes ATM +/- OPTION_WS_ATM_STRIKES_EACH_SIDE
        strikes (both CE/PE) for the current expiry, using each leg's security_id from the
        option chain response. Only resubscribes when the ATM strike has moved by more than
        1 strike step or the expiry has changed (spec item 10) -- not on every 3s cycle."""
        if not settings.ENABLE_OPTION_WEBSOCKET_UNIVERSE:
            return
        if self._state != self.State.RUNNING or not self._feed:
            return
        if not rows or underlying_ltp is None:
            return

        strikes = sorted({r["strike"] for r in rows})
        if not strikes:
            return
        atm_strike = min(strikes, key=lambda s: abs(s - underlying_ltp))
        atm_index = strikes.index(atm_strike)

        with self._universe_lock:
            expiry_changed = expiry != self._current_expiry
            if self._current_atm_strike is not None and self._current_atm_strike in strikes:
                prev_index = strikes.index(self._current_atm_strike)
                atm_shifted = abs(atm_index - prev_index) > 1
            else:
                atm_shifted = True

            if not expiry_changed and not atm_shifted:
                return

            each_side = settings.OPTION_WS_ATM_STRIKES_EACH_SIDE
            window = set(strikes[max(0, atm_index - each_side): atm_index + each_side + 1])

            new_instruments: dict[str, tuple[int, str, int]] = {}
            for r in rows:
                if r["strike"] not in window:
                    continue
                sid = r.get("security_id")
                if not sid:
                    continue
                sid = str(sid)
                new_instruments[sid] = (EXCHANGE_NSE_FNO, sid, PACKET_FULL)
                self.security_id_to_symbol[sid] = f"{r['strike']}{r['option_type']}"

            old_ids = set(self._option_instruments.keys())
            new_ids = set(new_instruments.keys())

            to_unsubscribe = [self._option_instruments[sid] for sid in (old_ids - new_ids)]
            to_subscribe = [new_instruments[sid] for sid in (new_ids - old_ids)]

            try:
                # SDK batches internally in groups of 100 per JSON subscription message.
                if to_unsubscribe:
                    self._feed.unsubscribe_symbols(to_unsubscribe)
                if to_subscribe:
                    self._feed.subscribe_symbols(to_subscribe)
            except Exception:
                logger.exception("Failed to update option websocket universe")
                return

            self._option_instruments = new_instruments
            self._current_atm_strike = atm_strike
            self._current_expiry = expiry
            logger.info(
                "Option websocket universe refreshed: expiry=%s atm=%s strikes_subscribed=%d",
                expiry, atm_strike, len(new_instruments),
            )
