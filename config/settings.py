import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

DHAN_CLIENT_ID_1 = os.getenv("DHAN_CLIENT_ID_1")
DHAN_ACCESS_TOKEN_1 = os.getenv("DHAN_ACCESS_TOKEN_1")
DHAN_CLIENT_ID_2 = os.getenv("DHAN_CLIENT_ID_2") or None
DHAN_ACCESS_TOKEN_2 = os.getenv("DHAN_ACCESS_TOKEN_2") or None
DHAN_ACCOUNT_2_ENABLED = bool(DHAN_CLIENT_ID_2 and DHAN_ACCESS_TOKEN_2)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://localhost:5432/nifty_data")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

MARKETAUX_API_KEY = os.getenv("MARKETAUX_API_KEY") or None

# --- Dhan rate limiting (code-enforced, not just documented) ---
# These are GLOBAL per account across every endpoint (option chain, quotes, historical,
# etc.), not per endpoint. Defaults are intentionally conservative after Account 1 and
# Account 2 both hit Dhan's broader rate limiter (HTTP 429) during one heavy testing
# session -- see connectors/dhan_request_manager.py.
DHAN_GLOBAL_RPS = float(os.getenv("DHAN_GLOBAL_RPS", "4"))
DHAN_GLOBAL_BURST = float(os.getenv("DHAN_GLOBAL_BURST", "1"))
DHAN_OPTIONCHAIN_MIN_INTERVAL_SECONDS = float(os.getenv("DHAN_OPTIONCHAIN_MIN_INTERVAL_SECONDS", "3.3"))
DHAN_MARKETQUOTE_MIN_INTERVAL_SECONDS = float(os.getenv("DHAN_MARKETQUOTE_MIN_INTERVAL_SECONDS", "1.2"))

# Circuit breaker escalation on 429 / soft-ban warnings.
DHAN_429_FIRST_COOLDOWN_SECONDS = int(os.getenv("DHAN_429_FIRST_COOLDOWN_SECONDS", "60"))
DHAN_429_SECOND_COOLDOWN_SECONDS = int(os.getenv("DHAN_429_SECOND_COOLDOWN_SECONDS", "300"))
DHAN_429_REDUCED_SPEED_MINUTES = int(os.getenv("DHAN_429_REDUCED_SPEED_MINUTES", "15"))

# safe -> normal -> production: widen intervals first, tighten only once a session
# has proven stable. See orchestrator.py SCHEDULER_PROFILES.
SOAK_MODE = os.getenv("SOAK_MODE", "safe")

ORCHESTRATOR_LOCK_TTL_SECONDS = int(os.getenv("ORCHESTRATOR_LOCK_TTL_SECONDS", "60"))
GAP_ALERT_COOLDOWN_SECONDS = int(os.getenv("GAP_ALERT_COOLDOWN_SECONDS", "600"))

ENABLE_OPTION_WEBSOCKET_UNIVERSE = os.getenv("ENABLE_OPTION_WEBSOCKET_UNIVERSE", "true").lower() == "true"
OPTION_WS_ATM_STRIKES_EACH_SIDE = int(os.getenv("OPTION_WS_ATM_STRIKES_EACH_SIDE", "10"))

NSE_HOLIDAY_FILE = os.getenv("NSE_HOLIDAY_FILE", "config/nse_holidays.json")

IST = "Asia/Kolkata"
MARKET_OPEN = "09:00"
MARKET_CLOSE = "15:35"

LOG_DIR = BASE_DIR / "logs"
